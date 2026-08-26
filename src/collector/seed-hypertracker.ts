import { config } from "../config.js";
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
const REQUEST_TIMEOUT_MS = 120_000;
const IMPORT_CHUNK = 10_000;
const MAX_COIN_FAILURES = 10;

class AuthError extends Error {}

interface SeedRow {
  address: string;
  szi: number;
  entryPx: number | null;
}

export function shouldSeed(): boolean {
  return config.hypertrackerApiKey !== "";
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

  const { rows: todo } = await pool.query<{ coin: string }>(
    `select a.coin from perp_assets a
     where a.is_delisted = false
       and not exists (select 1 from seed_progress s where s.source = $1 and s.coin = a.coin)
     order by a.coin`,
    [SOURCE],
  );
  if (todo.length === 0) {
    log("seed", "hypertracker census already imported (seed_progress complete)");
    return;
  }
  log("seed", `hypertracker census import starting: ${todo.length} coins`);

  let failures = 0;
  let loggedFields = false;
  for (const { coin } of todo) {
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
      if (err instanceof AuthError) {
        logErr("seed", "hypertracker API key rejected — aborting seed. Fix HYPERTRACKER_API_KEY and redeploy; the seed resumes automatically");
        return;
      }
      failures++;
      logErr("seed", `${coin} failed (${failures}/${MAX_COIN_FAILURES})`, err);
      if (failures >= MAX_COIN_FAILURES) {
        logErr("seed", "too many failures — stopping; remaining coins retry on next boot");
        return;
      }
    }
    await sleepFor(config.hypertrackerReqDelayMs, isStopped);
  }
  log("seed", "hypertracker census import finished");
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
    for (const key of ["data", "positions", "results", "rows", "items"]) {
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
  const rowCoin = pick(raw, COIN_KEYS);
  // Skip rows for other symbols (e.g. HIP-3 builder exchanges) — we key by main-DEX coins.
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
  for (let attempt = 0; attempt <= 3; attempt++) {
    if (attempt > 0) await sleep(5_000 * attempt);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const res = await fetch(url, { headers, signal: controller.signal });
      if (res.status === 401 || res.status === 403) throw new AuthError(`HTTP ${res.status}`);
      if (res.status === 429) {
        const ra = parseInt(res.headers.get("retry-after") ?? "", 10);
        lastErr = new Error("HTTP 429");
        await sleep(Number.isFinite(ra) && ra > 0 ? ra * 1000 : 30_000);
        continue;
      }
      if (res.status >= 500) {
        lastErr = new Error(`HTTP ${res.status}`);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res;
    } catch (err) {
      if (err instanceof AuthError) throw err;
      lastErr = err;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(`request failed: ${url}`);
}

async function sleepFor(ms: number, isStopped: () => boolean): Promise<void> {
  const deadline = Date.now() + ms;
  while (!isStopped() && Date.now() < deadline) {
    await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
  }
}
