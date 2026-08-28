import { config } from "../config.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";

// One-shot census seed from HyperTracker (docs.coinmarketman.com).
//
// GET {base}/external/positions/open/coin/{COIN} with Accept: text/csv returns the
// full open-position snapshot for a coin (their docs: a pre-signed CSV URL valid
// ~120s; we also handle direct CSV bodies and plain JSON arrays defensively).
//
// Imported rows are PROVISIONAL: wallets enter `traders` as bootstrap_pending, so
// the existing clearinghouseState bootstrapper progressively re-verifies every one
// against Hyperliquid's official state (seeded wallets sort behind tape-active ones
// in the queue). Nothing already bootstrapped is ever overwritten by a seed.
// Per-coin completion is recorded in seed_progress, making the seed resumable and
// idempotent across restarts.

const SOURCE = "hypertracker";
const HISTORY_SOURCE = "hypertracker-history";
const DEEP_SOURCE = "hypertracker-deep";
const REQUEST_TIMEOUT_MS = 120_000;
const IMPORT_CHUNK = 10_000;
const MAX_COIN_FAILURES = 10;

class AuthError extends Error {}
// Persistent 429s — on the free tier the daily quota resets; we stop cleanly and
// resume on a later boot instead of burning requests.
class QuotaError extends Error {}

interface SeedRow {
  address: string;
  szi: number;
  entryPx: number | null;
}

export function shouldSeed(): boolean {
  return config.hypertrackerApiKey !== "";
}

// True once every live coin has completed all enabled import passes.
export async function seedComplete(): Promise<boolean> {
  const sources = [SOURCE, HISTORY_SOURCE, ...(config.hypertrackerDeepHistory ? [DEEP_SOURCE] : [])];
  const { rows } = await pool.query<{ missing: number }>(
    `select count(*)::int as missing
     from perp_assets a
     cross join unnest($1::text[]) as src(source)
     where a.is_delisted = false
       and not exists (select 1 from seed_progress s where s.source = src.source and s.coin = a.coin)`,
    [sources],
  );
  return (rows[0]?.missing ?? 0) === 0;
}

export async function runSeeder(isStopped: () => boolean): Promise<void> {
  // Wait for the asset registry (populated by the first tick).
  while (!isStopped()) {
    const { rows } = await pool.query<{ n: number }>(
      "select count(*)::int as n from perp_assets where is_delisted = false",
    );
    if ((rows[0]?.n ?? 0) > 0) break;
    await sleepFor(5_000, isStopped);
  }
  if (isStopped()) return;

  try {
    await censusPass(isStopped);
    await historyPass(isStopped);
    if (config.hypertrackerDeepHistory) await deepHistoryPass(isStopped);
  } catch (err) {
    if (err instanceof AuthError) {
      logErr("seed", "hypertracker API key rejected — aborting. Fix HYPERTRACKER_API_KEY and redeploy; imports resume automatically");
      await opsEvent("seed", "error", `API key rejected (${err.message}) — imports paused until key fixed`);
    } else if (err instanceof QuotaError) {
      log("seed", "hypertracker request quota exhausted — pausing. Imports resume at the next retry (free tier resets daily; a paid tier finishes in one pass)");
      await opsEvent("seed", "warn", `quota exhausted (${err.message}) — retrying in ${Math.round(config.hypertrackerRetryMs / 3_600_000)}h`);
    } else {
      throw err;
    }
  }
}

// Pending coins ordered by open interest, so the markets that matter import inside
// the first available quota window rather than after the alphabetical long tail.
async function pendingCoins(source: string): Promise<string[]> {
  const { rows } = await pool.query<{ coin: string }>(
    `select a.coin from perp_assets a
     left join lateral (
       select oi_usd from perp_ticks t where t.coin = a.coin order by ts desc limit 1
     ) l on true
     where a.is_delisted = false
       and not exists (select 1 from seed_progress s where s.source = $1 and s.coin = a.coin)
     order by l.oi_usd desc nulls last, a.coin`,
    [source],
  );
  return rows.map((r) => r.coin);
}

async function censusPass(isStopped: () => boolean): Promise<void> {
  const todo = await pendingCoins(SOURCE);
  if (todo.length === 0) {
    log("seed", "hypertracker census already imported");
    return;
  }
  log("seed", `hypertracker census import starting: ${todo.length} coins`);

  let failures = 0;
  let loggedFields = false;
  for (const coin of todo) {
    if (isStopped()) return;
    try {
      const { rows, sampleFields } = await fetchOpenPositions(coin);
      if (!loggedFields && sampleFields) {
        log("seed", `response fields: ${sampleFields}`);
        loggedFields = true;
      }
      const { wallets, imported } = await importRows(coin, rows);
      await pool.query(
        `insert into seed_progress (source, coin, wallets, rows_imported) values ($1, $2, $3, $4)
         on conflict (source, coin) do update set
           completed_at = now(), wallets = excluded.wallets, rows_imported = excluded.rows_imported`,
        [SOURCE, coin, wallets, imported],
      );
      log("seed", `${coin}: ${imported} provisional positions imported (${wallets} wallets in snapshot)`);
    } catch (err) {
      if (err instanceof AuthError || err instanceof QuotaError) throw err;
      failures++;
      logErr("seed", `${coin} failed (${failures}/${MAX_COIN_FAILURES})`, err);
      await opsEvent("seed-census", "error", `${coin}: ${err instanceof Error ? err.message : String(err)}`);
      if (failures >= MAX_COIN_FAILURES) {
        logErr("seed", "too many census failures — stopping; remaining coins retry later");
        return;
      }
    }
    await sleepFor(config.hypertrackerReqDelayMs, isStopped);
  }
  log("seed", "hypertracker census import finished");
  await opsEvent("seed-census", "info", "census import finished");
}

// Import HyperTracker's 2h-sampled long/short count history into
// positioning_snapshots (source='hypertracker') — the change-over-time series
// from before this service existed. Live rows are never overwritten.
async function historyPass(isStopped: () => boolean): Promise<void> {
  const todo = await pendingCoins(HISTORY_SOURCE);
  if (todo.length === 0) {
    log("seed", "hypertracker count history already imported");
    return;
  }
  log("seed", `hypertracker count-history import starting: ${todo.length} coins`);

  let failures = 0;
  for (const coin of todo) {
    if (isStopped()) return;
    try {
      const rows = await fetchMetricsHistory(coin);
      const imported = await importHistory(coin, rows);
      await pool.query(
        `insert into seed_progress (source, coin, wallets, rows_imported) values ($1, $2, 0, $3)
         on conflict (source, coin) do update set
           completed_at = now(), rows_imported = excluded.rows_imported`,
        [HISTORY_SOURCE, coin, imported],
      );
      const oldest = rows.length > 0 ? rows.reduce((m, r) => (r.ts < m ? r.ts : m), rows[0]!.ts) : null;
      log("seed", `${coin}: ${imported} historical snapshots imported${oldest ? ` (back to ${oldest.toISOString().slice(0, 10)})` : ""}`);
    } catch (err) {
      if (err instanceof AuthError || err instanceof QuotaError) throw err;
      failures++;
      logErr("seed", `${coin} history failed (${failures}/${MAX_COIN_FAILURES})`, err);
      await opsEvent("seed-history", "error", `${coin}: ${err instanceof Error ? err.message : String(err)}`);
      if (failures >= MAX_COIN_FAILURES) {
        logErr("seed", "too many history failures — stopping; remaining coins retry later");
        return;
      }
    }
    await sleepFor(config.hypertrackerReqDelayMs, isStopped);
  }
  log("seed", "hypertracker count-history import finished");
  await opsEvent("seed-history", "info", "count-history import finished");
}

async function fetchOpenPositions(coin: string): Promise<{ rows: SeedRow[]; sampleFields: string | null }> {
  const url = `${config.hypertrackerBaseUrl}/external/positions/open/coin/${encodeURIComponent(coin)}`;
  const res = await httpGet(url, {
    Authorization: `Bearer ${config.hypertrackerApiKey}`,
    Accept: "text/csv",
  });
  return parseResponse(coin, res, true);
}

async function parseResponse(
  coin: string,
  res: Response,
  allowPresigned: boolean,
): Promise<{ rows: SeedRow[]; sampleFields: string | null }> {
  const ctype = res.headers.get("content-type") ?? "";
  const body = await res.text();
  const trimmed = body.trimStart();
  if (ctype.includes("json") || trimmed.startsWith("{") || trimmed.startsWith("[")) {
    const parsed: unknown = JSON.parse(body);
    if (allowPresigned) {
      const presigned = findPresignedUrl(parsed);
      if (presigned) {
        const fileRes = await httpGet(presigned, {});
        if (!fileRes.ok) throw new Error(`presigned fetch: HTTP ${fileRes.status}`);
        return parseResponse(coin, fileRes, false);
      }
    }
    const list = extractJsonList(parsed);
    return mapRawRows(coin, list);
  }
  return mapRawRows(coin, parseCsv(body));
}

function findPresignedUrl(v: unknown): string | null {
  if (typeof v !== "object" || v === null || Array.isArray(v)) return null;
  const obj = v as Record<string, unknown>;
  for (const key of ["url", "downloadUrl", "download_url", "presignedUrl", "presigned_url", "csvUrl", "csv_url", "link", "href", "file"]) {
    const val = obj[key];
    if (typeof val === "string" && /^https?:\/\//.test(val)) return val;
  }
  // one level of nesting, e.g. {data: {url}}
  for (const val of Object.values(obj)) {
    if (typeof val === "object" && val !== null && !Array.isArray(val)) {
      const nested = findPresignedUrl(val);
      if (nested) return nested;
    }
  }
  return null;
}

function extractJsonList(parsed: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(parsed)) return parsed.filter(isRecord);
  if (isRecord(parsed)) {
    for (const key of ["data", "positions", "results", "rows", "items", "metrics"]) {
      const val = parsed[key];
      if (Array.isArray(val)) return val.filter(isRecord);
    }
  }
  return [];
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

// Minimal RFC4180 CSV parser (quoted fields, embedded commas/newlines).
function parseCsv(text: string): Array<Record<string, unknown>> {
  const rows: string[][] = [];
  let field = "";
  let row: string[] = [];
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else inQuotes = false;
      } else field += c;
    } else if (c === '"') inQuotes = true;
    else if (c === ",") {
      row.push(field);
      field = "";
    } else if (c === "\n") {
      row.push(field.endsWith("\r") ? field.slice(0, -1) : field);
      rows.push(row);
      row = [];
      field = "";
    } else field += c;
  }
  if (field !== "" || row.length > 0) {
    row.push(field.endsWith("\r") ? field.slice(0, -1) : field);
    rows.push(row);
  }
  const header = rows.shift();
  if (!header) return [];
  const keys = header.map((h) => h.trim());
  return rows
    .filter((r) => r.length > 1 || (r.length === 1 && r[0] !== ""))
    .map((r) => Object.fromEntries(keys.map((k, i) => [k, r[i] ?? ""])));
}

interface HistRow {
  ts: Date;
  nLong: number;
  nShort: number;
  szLong: number | null;
  szShort: number | null;
  ntlLong: number | null;
  ntlShort: number | null;
}

// Deep archive: walk each coin backward through the paginated position-metrics
// series (snapshots every ~15min back to HYPERTRACKER_DEEP_START). The resume
// point is the database itself — each request asks for rows strictly older than
// the oldest 'hypertracker' row we already hold — so quota walls interrupt
// nothing and no cursor state is stored.
async function deepHistoryPass(isStopped: () => boolean): Promise<void> {
  const todo = await pendingCoins(DEEP_SOURCE);
  if (todo.length === 0) {
    log("seed", "hypertracker deep history already imported");
    return;
  }
  log("seed", `hypertracker deep-history import starting: ${todo.length} coins (target ${config.hypertrackerDeepStart})`);

  let failures = 0;
  for (const coin of todo) {
    if (isStopped()) return;
    try {
      const { imported, oldest, requests } = await deepImportCoin(coin, isStopped);
      if (isStopped()) return; // interrupted mid-coin — resume next cycle, no progress marker
      await pool.query(
        `insert into seed_progress (source, coin, wallets, rows_imported) values ($1, $2, 0, $3)
         on conflict (source, coin) do update set
           completed_at = now(), rows_imported = excluded.rows_imported`,
        [DEEP_SOURCE, coin, imported],
      );
      log("seed", `${coin}: deep history complete — ${imported} rows${oldest ? ` back to ${oldest.slice(0, 10)}` : ""} (${requests} requests)`);
      await opsEvent("seed-deep", "info", `${coin}: complete, ${imported} rows${oldest ? ` back to ${oldest.slice(0, 10)}` : ""}`);
    } catch (err) {
      if (err instanceof AuthError || err instanceof QuotaError) throw err;
      failures++;
      logErr("seed", `${coin} deep history failed (${failures}/${MAX_COIN_FAILURES})`, err);
      await opsEvent("seed-deep", "error", `${coin}: ${err instanceof Error ? err.message : String(err)}`);
      if (failures >= MAX_COIN_FAILURES) {
        logErr("seed", "too many deep-history failures — stopping; remaining coins retry later");
        return;
      }
    }
    await sleepFor(config.hypertrackerReqDelayMs, isStopped);
  }
  log("seed", "hypertracker deep-history import finished");
  await opsEvent("seed-deep", "info", "deep-history import finished");
}

async function deepImportCoin(
  coin: string,
  isStopped: () => boolean,
): Promise<{ imported: number; oldest: string | null; requests: number }> {
  const deepStartMs = Date.parse(config.hypertrackerDeepStart);
  if (!Number.isFinite(deepStartMs)) throw new Error(`invalid HYPERTRACKER_DEEP_START "${config.hypertrackerDeepStart}"`);
  let imported = 0;
  let requests = 0;
  let oldest: string | null = null;
  while (!isStopped()) {
    const { rows } = await pool.query<{ min: Date | null }>(
      "select min(ts) as min from positioning_snapshots where coin = $1 and source = 'hypertracker'",
      [coin],
    );
    const endMs = rows[0]?.min ? rows[0].min.getTime() - 1_000 : Date.now();
    if (endMs <= deepStartMs) break;
    await sleepFor(config.hypertrackerReqDelayMs, isStopped);
    if (isStopped()) break;
    const page = await fetchMetricsPage(coin, new Date(deepStartMs).toISOString(), new Date(endMs).toISOString());
    requests++;
    if (page.length === 0) break; // nothing older — reached the start of their record
    imported += await importHistory(coin, page);
    const oldestInPage = page.reduce((m, r) => (r.ts < m ? r.ts : m), page[0]!.ts);
    oldest = oldestInPage.toISOString();
    if (oldestInPage.getTime() >= endMs + 1_000) {
      // The API ignored our end bound — bail rather than loop forever.
      throw new Error(`position-metrics ignored end filter (oldest returned ${oldest} >= requested end)`);
    }
  }
  return { imported, oldest, requests };
}

// Paginated series rows are objects: {createdAt, positionCount, positionCountLong,
// totalPositionValue(Long), totalPositionSize(Long), ...} — unlike the export's
// column/data format, this one includes the size breakdown.
async function fetchMetricsPage(coin: string, startIso: string, endIso: string): Promise<HistRow[]> {
  const url =
    `${config.hypertrackerBaseUrl}/external/position-metrics/coin/${encodeURIComponent(coin)}` +
    `?limit=1000&start=${encodeURIComponent(startIso)}&end=${encodeURIComponent(endIso)}`;
  const res = await httpGet(url, { Authorization: `Bearer ${config.hypertrackerApiKey}` });
  const text = await res.text();
  try {
    const list = extractJsonList(JSON.parse(text));
    const out: HistRow[] = [];
    for (const raw of list) {
      const tsMs = typeof raw.createdAt === "string" ? Date.parse(raw.createdAt) : NaN;
      const count = toNum(raw.positionCount);
      const nLong = toNum(raw.positionCountLong);
      if (!Number.isFinite(tsMs) || count === null || nLong === null || nLong > count || nLong < 0) continue;
      const val = toNum(raw.totalPositionValue);
      const valLong = toNum(raw.totalPositionValueLong);
      const sz = toNum(raw.totalPositionSize);
      const szLong = toNum(raw.totalPositionSizeLong);
      out.push({
        ts: new Date(tsMs),
        nLong: Math.round(nLong),
        nShort: Math.round(count - nLong),
        szLong,
        szShort: sz !== null && szLong !== null ? sz - szLong : null,
        ntlLong: valLong,
        ntlShort: val !== null && valLong !== null ? val - valLong : null,
      });
    }
    return out;
  } catch (err) {
    const snippet = text.slice(0, 200).replace(/\s+/g, " ");
    const msg = err instanceof Error ? err.message : String(err);
    throw new Error(`${msg} — body starts: ${snippet}`);
  }
}

// GET /external/exports/coins/{coin}/position-metrics → {coin, metrics: {columns, data}}.
// Note the docs' format quirk: `columns` starts with "coin" but data rows omit it
// (the coin lives at the top level), so column mapping detects the offset.
async function fetchMetricsHistory(coin: string): Promise<HistRow[]> {
  const url = `${config.hypertrackerBaseUrl}/external/exports/coins/${encodeURIComponent(coin)}/position-metrics`;
  const res = await httpGet(url, { Authorization: `Bearer ${config.hypertrackerApiKey}` });
  let text = await res.text();
  try {
    let parsed: unknown = JSON.parse(text);
    const presigned = findPresignedUrl(parsed);
    if (presigned) {
      const fileRes = await httpGet(presigned, {});
      if (!fileRes.ok) throw new Error(`presigned fetch: HTTP ${fileRes.status}`);
      text = await fileRes.text();
      parsed = JSON.parse(text);
    }
    return parseMetricsExport(parsed);
  } catch (err) {
    // Surface what actually came back so a single log line diagnoses format drift.
    const snippet = text.slice(0, 200).replace(/\s+/g, " ");
    const msg = err instanceof Error ? err.message : String(err);
    throw new Error(`${msg} — body starts: ${snippet}`);
  }
}

function parseMetricsExport(body: unknown): HistRow[] {
  const root = isRecord(body) ? body : {};
  const metrics = isRecord(root.metrics) ? root.metrics : root;
  const columns = Array.isArray(metrics.columns) ? (metrics.columns as unknown[]).map(String) : null;
  const data = Array.isArray(metrics.data) ? (metrics.data as unknown[]) : null;
  if (!columns || !data) throw new Error("unexpected position-metrics export format");
  const firstRow = data.find((r) => Array.isArray(r)) as unknown[] | undefined;
  let cols = columns;
  if (firstRow && firstRow.length === columns.length - 1 && columns[0]?.toLowerCase() === "coin") {
    cols = columns.slice(1);
  }
  const idx = (name: string): number => cols.findIndex((c) => c.toLowerCase() === name.toLowerCase());
  const iTs = idx("timestamp");
  const iCount = idx("positionCount");
  const iLong = idx("positionCountLong");
  const iVal = idx("totalPositionValue");
  const iValLong = idx("totalPositionValueLong");
  const iSz = idx("totalPositionSize");
  const iSzLong = idx("totalPositionSizeLong");
  if (iTs < 0 || iCount < 0 || iLong < 0) {
    throw new Error(`position-metrics export missing expected columns: ${cols.join(", ")}`);
  }
  const rows: HistRow[] = [];
  for (const raw of data) {
    if (!Array.isArray(raw)) continue;
    const tsMs = typeof raw[iTs] === "string" ? Date.parse(raw[iTs] as string) : NaN;
    const count = toNum(raw[iCount]);
    const nLong = toNum(raw[iLong]);
    if (!Number.isFinite(tsMs) || count === null || nLong === null || nLong > count || nLong < 0) continue;
    const val = iVal >= 0 ? toNum(raw[iVal]) : null;
    const valLong = iValLong >= 0 ? toNum(raw[iValLong]) : null;
    const sz = iSz >= 0 ? toNum(raw[iSz]) : null;
    const szLong = iSzLong >= 0 ? toNum(raw[iSzLong]) : null;
    rows.push({
      ts: new Date(tsMs),
      nLong: Math.round(nLong),
      nShort: Math.round(count - nLong),
      szLong,
      szShort: sz !== null && szLong !== null ? sz - szLong : null,
      ntlLong: valLong,
      ntlShort: val !== null && valLong !== null ? val - valLong : null,
    });
  }
  return rows;
}

async function importHistory(coin: string, rows: HistRow[]): Promise<number> {
  let imported = 0;
  for (let i = 0; i < rows.length; i += IMPORT_CHUNK) {
    const chunk = rows.slice(i, i + IMPORT_CHUNK);
    const res = await pool.query(
      `insert into positioning_snapshots
         (ts, coin, n_long, n_short, sz_long, sz_short, ntl_long, ntl_short, traders_tracked, source)
       select u.ts, $2, u.n_long, u.n_short, u.sz_long, u.sz_short, u.ntl_long, u.ntl_short, null, 'hypertracker'
       from unnest($1::timestamptz[], $3::int[], $4::int[], $5::float8[], $6::float8[], $7::float8[], $8::float8[])
         as u(ts, n_long, n_short, sz_long, sz_short, ntl_long, ntl_short)
       on conflict (coin, ts) do nothing`,
      [
        chunk.map((r) => r.ts),
        coin,
        chunk.map((r) => r.nLong),
        chunk.map((r) => r.nShort),
        chunk.map((r) => r.szLong),
        chunk.map((r) => r.szShort),
        chunk.map((r) => r.ntlLong),
        chunk.map((r) => r.ntlShort),
      ],
    );
    imported += res.rowCount ?? 0;
  }
  return imported;
}

const ADDR_KEYS = ["address", "wallet", "user", "walletAddress", "wallet_address", "owner", "trader", "account"];
const COIN_KEYS = ["coin", "symbol", "asset", "market", "pair"];
const SIGNED_KEYS = ["szi", "signedSize", "signed_size", "netSize", "net_size"];
const SIZE_KEYS = ["size", "sz", "positionSize", "position_size", "amount", "coins", "qty"];
const SIDE_KEYS = ["side", "direction", "positionSide", "position_side", "bias", "longShort", "long_short"];
const ENTRY_KEYS = ["entryPx", "entry_px", "entryPrice", "entry_price", "avgEntryPrice", "avg_entry_price", "avgPrice"];
const LONG_VALUES = new Set(["long", "buy", "b", "bull", "1", "true"]);
const SHORT_VALUES = new Set(["short", "sell", "a", "s", "bear", "-1", "false"]);

function pick(row: Record<string, unknown>, keys: string[]): unknown {
  for (const k of keys) {
    const v = row[k];
    if (v !== undefined && v !== null && v !== "") return v;
  }
  const lower = new Map(Object.keys(row).map((k) => [k.toLowerCase(), k]));
  for (const k of keys) {
    const real = lower.get(k.toLowerCase());
    if (real !== undefined) {
      const v = row[real];
      if (v !== undefined && v !== null && v !== "") return v;
    }
  }
  return undefined;
}

function toNum(v: unknown): number | null {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const n = parseFloat(v.replace(/,/g, ""));
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function mapRawRows(
  reqCoin: string,
  raws: Array<Record<string, unknown>>,
): { rows: SeedRow[]; sampleFields: string | null } {
  const rows: SeedRow[] = [];
  let skipped = 0;
  for (const raw of raws) {
    const parsed = toSeedRow(reqCoin, raw);
    if (parsed) rows.push(parsed);
    else skipped++;
  }
  if (skipped > 0 && skipped > raws.length / 2 && raws.length > 10) {
    log("seed", `${reqCoin}: warning — ${skipped}/${raws.length} rows unparseable (field mapping may need adjustment)`);
  }
  const sampleFields = raws[0] ? Object.keys(raws[0]).slice(0, 25).join(", ") : null;
  return { rows, sampleFields };
}

function toSeedRow(reqCoin: string, raw: Record<string, unknown>): SeedRow | null {
  const addr = pick(raw, ADDR_KEYS);
  if (typeof addr !== "string" || !/^0x[0-9a-fA-F]{40}$/.test(addr.trim())) return null;
  // The snapshot covers HIP-3 builder exchanges too (per their docs, `dex` field);
  // our ledger tracks the main DEX only — same-symbol positions elsewhere are separate markets.
  const dex = pick(raw, ["dex", "exchange", "venue"]);
  if (typeof dex === "string" && dex !== "" && dex.toLowerCase() !== "main") return null;
  const rowCoin = pick(raw, COIN_KEYS);
  // Skip rows for other symbols — we key by main-DEX coins.
  if (typeof rowCoin === "string" && rowCoin !== "" && rowCoin.toUpperCase() !== reqCoin.toUpperCase()) return null;
  let szi = toNum(pick(raw, SIGNED_KEYS));
  if (szi === null) {
    const size = toNum(pick(raw, SIZE_KEYS));
    if (size === null || size === 0) return null;
    if (size < 0) {
      szi = size; // already signed
    } else {
      const sideRaw = pick(raw, SIDE_KEYS);
      const side =
        typeof sideRaw === "string" ? sideRaw.trim().toLowerCase() : typeof sideRaw === "boolean" ? (sideRaw ? "long" : "short") : "";
      if (LONG_VALUES.has(side)) szi = size;
      else if (SHORT_VALUES.has(side)) szi = -size;
      else return null;
    }
  }
  if (szi === 0 || !Number.isFinite(szi)) return null;
  return { address: addr.trim().toLowerCase(), szi, entryPx: toNum(pick(raw, ENTRY_KEYS)) };
}

async function importRows(coin: string, rows: SeedRow[]): Promise<{ wallets: number; imported: number }> {
  if (rows.length === 0) return { wallets: 0, imported: 0 };
  const byAddr = new Map<string, SeedRow>();
  for (const r of rows) if (!byAddr.has(r.address)) byAddr.set(r.address, r);
  const uniq = [...byAddr.values()];
  let imported = 0;
  for (let i = 0; i < uniq.length; i += IMPORT_CHUNK) {
    const chunk = uniq.slice(i, i + IMPORT_CHUNK);
    const addrs = chunk.map((r) => r.address);
    // New wallets enter pending (last_trade_at null → behind tape-active wallets in the
    // bootstrap queue); wallets we already know are left untouched.
    await pool.query(
      `insert into traders (address) select * from unnest($1::text[]) on conflict (address) do nothing`,
      [addrs],
    );
    // Provisional positions only for never-bootstrapped wallets; never overwrite.
    const res = await pool.query(
      `insert into positions (address, coin, szi, entry_px, updated_at)
       select u.address, $2, u.szi, u.entry_px, now()
       from unnest($1::text[], $3::float8[], $4::float8[]) as u(address, szi, entry_px)
       join traders t on t.address = u.address
       where t.bootstrapped_at is null
       on conflict (address, coin) do nothing`,
      [addrs, coin, chunk.map((r) => r.szi), chunk.map((r) => r.entryPx)],
    );
    imported += res.rowCount ?? 0;
  }
  return { wallets: uniq.length, imported };
}

async function httpGet(url: string, headers: Record<string, string>): Promise<Response> {
  let lastErr: unknown;
  let sawRateLimit = false;
  for (let attempt = 0; attempt <= 3; attempt++) {
    if (attempt > 0) await sleep(5_000 * attempt);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const res = await fetch(url, { headers, signal: controller.signal });
      if (res.status === 401 || res.status === 403) throw new AuthError(`HTTP ${res.status}`);
      // Their limiter signals a spent request quota with 402 Payment Required.
      if (res.status === 402) throw new QuotaError("HTTP 402 (request quota spent)");
      if (res.status === 429) {
        sawRateLimit = true;
        // Their limiter returns retry_after (header or JSON body); a short value is a
        // per-minute limit worth waiting out, a long one means the quota is spent.
        let retryAfterSec = parseInt(res.headers.get("retry-after") ?? "", 10);
        if (!Number.isFinite(retryAfterSec)) {
          try {
            const body = (await res.json()) as { retry_after?: number };
            if (typeof body.retry_after === "number") retryAfterSec = body.retry_after;
          } catch {
            /* ignore */
          }
        }
        if (Number.isFinite(retryAfterSec) && retryAfterSec > 300) throw new QuotaError(`retry_after ${retryAfterSec}s`);
        lastErr = new Error("HTTP 429");
        await sleep(Number.isFinite(retryAfterSec) && retryAfterSec > 0 ? retryAfterSec * 1000 : 30_000);
        continue;
      }
      if (res.status >= 500) {
        lastErr = new Error(`HTTP ${res.status}`);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res;
    } catch (err) {
      if (err instanceof AuthError || err instanceof QuotaError) throw err;
      lastErr = err;
    } finally {
      clearTimeout(timer);
    }
  }
  if (sawRateLimit) throw new QuotaError("rate limit persisted through retries");
  throw lastErr instanceof Error ? lastErr : new Error(`request failed: ${url}`);
}

async function sleepFor(ms: number, isStopped: () => boolean): Promise<void> {
  const deadline = Date.now() + ms;
  while (!isStopped() && Date.now() < deadline) {
    await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
  }
}
