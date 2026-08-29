import { EMA_TF_MS, config } from "../config.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { hl, sleep } from "../hl/client.js";
import { logErr } from "../log.js";
import type { Candle } from "../hl/types.js";

// EMA tracker: maintains EMA(period) per coin per timeframe from HL's official
// candles. Each (coin, timeframe) is seeded once from a full candleSnapshot fetch
// (up to HL's ~5000-candle retention — deep enough that EMA200 is fully converged
// on every timeframe), then advanced incrementally: one hourly-candle fetch per
// coin per sweep covers every timeframe, because a 4h/12h/1d bucket's close IS
// the close of its last traded hour and HL buckets are epoch/UTC-aligned.
//
// Seeding matches TradingView's ta.ema: SMA over the first `period` closes, then
// ema = α·close + (1−α)·ema with α = 2/(period+1). A listing younger than
// `period` candles reports null until enough closes exist (seed_sum/seed_n
// accumulate across sweeps). Hours with zero trades produce no candle on HL and
// are skipped here too, matching how charts compute EMAs over existing bars.

const HOUR_MS = 3_600_000;
const HL_CANDLE_CAP = 5_000; // HL retains ~5000 most recent candles per interval
const MAX_1H_GAP_MS = 4_800 * HOUR_MS; // beyond this the hourly stream can't cover the gap — reseed
const RESEED_BATCH = 8; // coins re-seeded from full history per sweep (drift/self-heal)
export const EMA_SWEEP_LAG_MS = 25_000; // fetch shortly after the hour so closes are final

interface EmaCell {
  period: number;
  ema: number | null; // null while the SMA seed is still accumulating
  seedSum: number;
  seedN: number;
  nCandles: number;
  seededAt: Date;
}

export interface TfState {
  tf: string;
  tfMs: number;
  lastOpenMs: number; // open time of the last closed candle applied
  cells: EmaCell[]; // one per configured period, ascending
  dirty: boolean;
}

interface EmaStateRow {
  coin: string;
  tf: string;
  period: number;
  ema: number | null;
  seed_sum: number;
  seed_n: number;
  n_candles: number;
  last_open_ms: string; // bigint arrives as a string
  seeded_at: Date;
}

function applyClose(cell: EmaCell, close: number): void {
  cell.nCandles++;
  if (cell.ema === null) {
    cell.seedSum += close;
    cell.seedN++;
    if (cell.seedN >= cell.period) cell.ema = cell.seedSum / cell.period;
  } else {
    const alpha = 2 / (cell.period + 1);
    cell.ema = alpha * close + (1 - alpha) * cell.ema;
  }
}

function freshCells(): EmaCell[] {
  return config.emaPeriods.map((period) => ({
    period,
    ema: null,
    seedSum: 0,
    seedN: 0,
    nCandles: 0,
    seededAt: new Date(),
  }));
}

function closedAsc(candles: Candle[], bucketMs: number, now: number): Candle[] {
  // candleSnapshot includes the still-forming bucket — keep only finished ones.
  return candles.filter((c) => c.t + bucketMs <= now).sort((a, b) => a.t - b.t);
}

// Full (re)build of one timeframe from HL's native candles for it.
// (Exported, like advanceTf, for verification harnesses — not used elsewhere.)
export async function bootstrapTf(coin: string, tf: string, now: number): Promise<TfState> {
  const tfMs = EMA_TF_MS[tf]!;
  await sleep(config.emaReqDelayMs);
  const candles = await hl.candleSnapshot(coin, tf, Math.max(0, now - HL_CANDLE_CAP * tfMs), now);
  const state: TfState = {
    tf,
    tfMs,
    // With no closed candles yet, park the cursor on the previous bucket so the
    // incremental pass picks up from the currently-forming one.
    lastOpenMs: Math.floor(now / tfMs) * tfMs - tfMs,
    cells: freshCells(),
    dirty: true,
  };
  for (const c of closedAsc(candles, tfMs, now)) {
    const close = parseFloat(c.c);
    if (!Number.isFinite(close)) continue;
    for (const cell of state.cells) applyClose(cell, close);
    state.lastOpenMs = c.t;
  }
  return state;
}

// Advance one timeframe using closed hourly candles: group them into this
// timeframe's (epoch-aligned) buckets; each finished bucket closes at the close
// of its last traded hour. Buckets at or before the cursor are skipped, so
// overlapping fetches are idempotent.
export function advanceTf(state: TfState, hourlyClosed: Candle[], now: number): void {
  let bucketOpen = -1;
  let bucketClose = NaN;
  const finish = (): void => {
    if (bucketOpen < 0) return;
    for (const cell of state.cells) applyClose(cell, bucketClose);
    state.lastOpenMs = bucketOpen;
    state.dirty = true;
  };
  for (const c of hourlyClosed) {
    const open = Math.floor(c.t / state.tfMs) * state.tfMs;
    if (open <= state.lastOpenMs || open + state.tfMs > now) continue;
    const close = parseFloat(c.c);
    if (!Number.isFinite(close)) continue;
    if (open !== bucketOpen) {
      finish();
      bucketOpen = open;
    }
    bucketClose = close;
  }
  finish();
}

// Load stored state grouped per coin. A timeframe whose rows don't exactly match
// the configured periods on a single shared cursor (config changed, partial
// write) is dropped here and rebuilt by the sweep.
async function loadStates(): Promise<Map<string, TfState[]>> {
  const { rows } = await pool.query<EmaStateRow>(
    "select coin, tf, period, ema, seed_sum, seed_n, n_candles, last_open_ms, seeded_at from ema_state",
  );
  const grouped = new Map<string, Map<string, EmaStateRow[]>>();
  for (const r of rows) {
    const tfMap = grouped.get(r.coin) ?? new Map<string, EmaStateRow[]>();
    const list = tfMap.get(r.tf) ?? [];
    list.push(r);
    tfMap.set(r.tf, list);
    grouped.set(r.coin, tfMap);
  }
  const out = new Map<string, TfState[]>();
  for (const [coin, tfMap] of grouped) {
    const states: TfState[] = [];
    for (const [tf, tfRows] of tfMap) {
      const tfMs = EMA_TF_MS[tf];
      if (!tfMs) continue;
      const periods = new Set(tfRows.map((r) => r.period));
      const oneCursor = new Set(tfRows.map((r) => r.last_open_ms)).size === 1;
      if (!oneCursor || periods.size !== config.emaPeriods.length || !config.emaPeriods.every((p) => periods.has(p))) {
        continue;
      }
      states.push({
        tf,
        tfMs,
        lastOpenMs: Number(tfRows[0]!.last_open_ms),
        dirty: false,
        cells: [...tfRows]
          .sort((a, b) => a.period - b.period)
          .map((r) => ({
            period: r.period,
            ema: r.ema,
            seedSum: r.seed_sum,
            seedN: r.seed_n,
            nCandles: r.n_candles,
            seededAt: r.seeded_at,
          })),
      });
    }
    out.set(coin, states);
  }
  return out;
}

async function saveCoin(coin: string, states: TfState[]): Promise<number> {
  const tf: string[] = [];
  const period: number[] = [];
  const ema: Array<number | null> = [];
  const seedSum: number[] = [];
  const seedN: number[] = [];
  const nCandles: number[] = [];
  const lastOpen: number[] = [];
  const seededAt: Date[] = [];
  for (const s of states) {
    if (!s.dirty) continue;
    for (const cell of s.cells) {
      tf.push(s.tf);
      period.push(cell.period);
      ema.push(cell.ema);
      seedSum.push(cell.seedSum);
      seedN.push(cell.seedN);
      nCandles.push(cell.nCandles);
      lastOpen.push(s.lastOpenMs);
      seededAt.push(cell.seededAt);
    }
  }
  if (tf.length === 0) return 0;
  await pool.query(
    `insert into ema_state (coin, tf, period, ema, seed_sum, seed_n, n_candles, last_open_ms, seeded_at, updated_at)
     select $1, *, now()
     from unnest($2::text[], $3::int[], $4::float8[], $5::float8[], $6::int[], $7::int[], $8::bigint[], $9::timestamptz[])
     on conflict (coin, tf, period) do update set
       ema = excluded.ema, seed_sum = excluded.seed_sum, seed_n = excluded.seed_n,
       n_candles = excluded.n_candles, last_open_ms = excluded.last_open_ms,
       seeded_at = excluded.seeded_at, updated_at = excluded.updated_at`,
    [coin, tf, period, ema, seedSum, seedN, nCandles, lastOpen, seededAt],
  );
  for (const s of states) s.dirty = false;
  return tf.length;
}

// Coins whose state was seeded longest ago, past the reseed horizon — rebuilt
// from full history to shrug off any drift (missed candles, fp error).
async function pickReseedCoins(): Promise<Set<string>> {
  if (config.emaReseedDays <= 0) return new Set();
  const { rows } = await pool.query<{ coin: string }>(
    `select coin from ema_state group by coin
     having min(seeded_at) < now() - make_interval(days => $1)
     order by min(seeded_at) limit $2`,
    [config.emaReseedDays, RESEED_BATCH],
  );
  return new Set(rows.map((r) => r.coin));
}

export interface EmaSweepResult {
  coins: number;
  requests: number;
  seededTfs: number;
  rowsWritten: number;
  failures: number;
}

// One sweep over every live coin. Cheap when there's nothing to do: a coin costs
// zero requests until its next candle has actually closed, one hourly fetch when
// one has, and one fetch per timeframe only when (re)seeding.
export async function syncEmas(isStopped: () => boolean): Promise<EmaSweepResult> {
  const result: EmaSweepResult = { coins: 0, requests: 0, seededTfs: 0, rowsWritten: 0, failures: 0 };
  const { rows: coinRows } = await pool.query<{ coin: string }>(
    "select coin from perp_assets where is_delisted = false order by coin",
  );
  result.coins = coinRows.length;
  if (coinRows.length === 0) return result;

  // Shed state for timeframes/periods no longer configured, and for coins no
  // longer live — frozen EMAs must not linger (a relisting reseeds cleanly).
  await pool.query(
    `delete from ema_state s
     where not (s.tf = any($1) and s.period = any($2::int[]))
        or not exists (select 1 from perp_assets a where a.coin = s.coin and a.is_delisted = false)`,
    [config.emaTimeframes, config.emaPeriods],
  );

  const stored = await loadStates();
  const reseed = await pickReseedCoins();

  for (const { coin } of coinRows) {
    if (isStopped()) break;
    try {
      const now = Date.now();
      let existing = reseed.has(coin) ? [] : (stored.get(coin) ?? []);

      // A gap the hourly stream can't span (long downtime) forces a full reseed.
      if (existing.length > 0) {
        const fetchFromMs = Math.min(...existing.map((s) => s.lastOpenMs + s.tfMs));
        if (now - fetchFromMs > MAX_1H_GAP_MS) existing = [];
      }

      const have = new Map(existing.map((s) => [s.tf, s]));
      const states: TfState[] = [];
      for (const tfName of config.emaTimeframes) {
        const kept = have.get(tfName);
        if (kept) {
          states.push(kept);
        } else {
          if (isStopped()) break;
          const seeded = await bootstrapTf(coin, tfName, Date.now());
          result.requests++;
          result.seededTfs++;
          // Persist each seeded timeframe immediately so a later fetch failing
          // (or a shutdown) doesn't discard the requests already spent.
          result.rowsWritten += await saveCoin(coin, [seeded]);
          states.push(seeded);
        }
      }

      // One hourly fetch advances every timeframe that predates this sweep.
      const incremental = states.filter((s) => have.has(s.tf));
      if (incremental.length > 0 && !isStopped()) {
        const nextCloseAt = Math.min(...incremental.map((s) => s.lastOpenMs + 2 * s.tfMs));
        if (now >= nextCloseAt) {
          const fetchFromMs = Math.min(...incremental.map((s) => s.lastOpenMs + s.tfMs));
          await sleep(config.emaReqDelayMs);
          const candles = await hl.candleSnapshot(coin, "1h", fetchFromMs, now);
          result.requests++;
          const hourly = closedAsc(candles, HOUR_MS, now).filter((c) => c.t >= fetchFromMs);
          for (const s of incremental) advanceTf(s, hourly, now);
        }
      }

      result.rowsWritten += await saveCoin(coin, states);
    } catch (err) {
      result.failures++;
      logErr("emas", `sync failed for ${coin}`, err);
    }
  }

  if (result.seededTfs > 0) {
    void opsEvent("emas", "info", `seeded ${result.seededTfs} coin-timeframes from full candle history`);
  }
  if (result.failures > 0) {
    void opsEvent("emas", "warn", `${result.failures} coins failed this EMA sweep — retrying shortly`);
  }
  return result;
}
