import { pool } from "../db/pool.js";
import { aprPct, cached, pctChange } from "./util.js";

export interface TickRow {
  coin: string;
  ts: Date;
  mark_px: number | null;
  mid_px: number | null;
  oracle_px: number | null;
  prev_day_px: number | null;
  funding_hr: number | null;
  open_interest: number | null;
  oi_usd: number | null;
  premium: number | null;
  day_ntl_vlm: number | null;
  px: number | null; // mid with mark fallback — the price used for change math
}

const TICK_COLS = `coin, ts, mark_px, mid_px, oracle_px, prev_day_px, funding_hr,
                   open_interest, oi_usd, premium, day_ntl_vlm,
                   coalesce(mid_px, mark_px) as px`;

export const LATEST_MAX_AGE_MS = 180_000;

// Newest tick per coin, ignoring anything older than maxAge (stale collector guard).
export async function latestTicks(maxAgeMs: number = LATEST_MAX_AGE_MS): Promise<TickRow[]> {
  const { rows } = await pool.query<TickRow>(
    `select distinct on (coin) ${TICK_COLS}
     from perp_ticks
     where ts >= now() - make_interval(secs => $1)
     order by coin, ts desc`,
    [maxAgeMs / 1000],
  );
  return rows;
}

export async function singleTick(coin: string): Promise<TickRow | null> {
  const { rows } = await pool.query<TickRow>(
    `select ${TICK_COLS} from perp_ticks where coin = $1 order by ts desc limit 1`,
    [coin],
  );
  return rows[0] ?? null;
}

// Per coin, the tick closest to the target time within ± tolerance.
export async function ticksAt(targetMs: number, tolMs: number): Promise<TickRow[]> {
  const { rows } = await pool.query<TickRow>(
    `select distinct on (coin) ${TICK_COLS}
     from perp_ticks
     where ts >= to_timestamp($1 / 1000.0) - make_interval(secs => $2)
       and ts <= to_timestamp($1 / 1000.0) + make_interval(secs => $2)
     order by coin, abs(extract(epoch from (ts - to_timestamp($1 / 1000.0)))), ts desc`,
    [targetMs, tolMs / 1000],
  );
  return rows;
}

// How far from the exact window start we accept a reference tick:
// 5% of the window, clamped to [90s, 15min].
export function toleranceFor(windowMs: number): number {
  return Math.min(Math.max(windowMs * 0.05, 90_000), 15 * 60_000);
}

export interface ChangeRow {
  coin: string;
  px: number | null;
  pxThen: number | null;
  pxChangePct: number | null;
  oiUsd: number | null;
  oiUsdThen: number | null;
  oiUsdChangePct: number | null;
  openInterest: number | null;
  fundingHr: number | null;
  fundingHrThen: number | null;
  fundingAprPct: number | null;
  markPx: number | null;
  dayNtlVlm: number | null;
  hl24hChangePct: number | null; // HL's own 24h change (px vs prevDayPx), for reference
  thenTs: string | null;
}

export interface ChangesBundle {
  asOf: string | null;
  rows: ChangeRow[];
}

export async function changesBundle(windowMs: number): Promise<ChangesBundle> {
  const [nowRows, thenRows] = await Promise.all([
    latestTicks(),
    ticksAt(Date.now() - windowMs, toleranceFor(windowMs)),
  ]);
  const thenBy = new Map(thenRows.map((r) => [r.coin, r]));
  const rows: ChangeRow[] = nowRows.map((n) => {
    const t = thenBy.get(n.coin);
    return {
      coin: n.coin,
      px: n.px,
      pxThen: t?.px ?? null,
      pxChangePct: pctChange(n.px, t?.px ?? null),
      oiUsd: n.oi_usd,
      oiUsdThen: t?.oi_usd ?? null,
      oiUsdChangePct: pctChange(n.oi_usd, t?.oi_usd ?? null),
      openInterest: n.open_interest,
      fundingHr: n.funding_hr,
      fundingHrThen: t?.funding_hr ?? null,
      fundingAprPct: n.funding_hr !== null ? aprPct(n.funding_hr) : null,
      markPx: n.mark_px,
      dayNtlVlm: n.day_ntl_vlm,
      hl24hChangePct: pctChange(n.px, n.prev_day_px),
      thenTs: t ? t.ts.toISOString() : null,
    };
  });
  let asOf: Date | null = null;
  for (const r of nowRows) if (!asOf || r.ts > asOf) asOf = r.ts;
  return { asOf: asOf ? asOf.toISOString() : null, rows };
}

const CHANGES_CACHE_MS = 10_000;

// Shared by every endpoint that needs "now vs N ago" for a window (the board and
// the per-coin snapshot/recap) — one compute per window per cache tick.
export function changesBundleCached(windowMs: number): Promise<ChangesBundle> {
  return cached(`changes:${windowMs}`, CHANGES_CACHE_MS, () => changesBundle(windowMs));
}

export interface AssetRow {
  coin: string;
  sz_decimals: number | null;
  max_leverage: number | null;
  is_delisted: boolean;
  first_seen: Date;
}

// The asset registry is a few hundred rows that change on the order of days —
// resolve coin names against a briefly-cached copy instead of querying per request.
function allAssets(): Promise<AssetRow[]> {
  return cached("assets:all", 60_000, async () => {
    const { rows } = await pool.query<AssetRow>(
      "select coin, sz_decimals, max_leverage, is_delisted, first_seen from perp_assets",
    );
    return rows;
  });
}

// Exact match first, then case-insensitive (HL names are case-sensitive, e.g. "kPEPE").
export async function resolveCoin(raw: string): Promise<AssetRow | null> {
  const assets = await allAssets();
  const exact = assets.find((a) => a.coin === raw);
  if (exact) return exact;
  const upper = raw.toUpperCase();
  return assets.find((a) => a.coin.toUpperCase() === upper) ?? null;
}

export interface PerpListRow extends AssetRow {
  ts: Date | null;
  px: number | null;
  mark_px: number | null;
  oi_usd: number | null;
  open_interest: number | null;
  funding_hr: number | null;
  day_ntl_vlm: number | null;
  prev_day_px: number | null;
}

export async function perpList(): Promise<PerpListRow[]> {
  const { rows } = await pool.query<PerpListRow>(
    `select a.coin, a.sz_decimals, a.max_leverage, a.is_delisted, a.first_seen,
            l.ts, coalesce(l.mid_px, l.mark_px) as px, l.mark_px, l.oi_usd,
            l.open_interest, l.funding_hr, l.day_ntl_vlm, l.prev_day_px
     from perp_assets a
     left join lateral (
       select * from perp_ticks t where t.coin = a.coin order by ts desc limit 1
     ) l on true
     order by l.oi_usd desc nulls last, a.coin`,
  );
  return rows;
}

export interface PerpCandleRow {
  t: Date;
  mid_o: number | null;
  mid_h: number | null;
  mid_l: number | null;
  mid_c: number | null;
  mark_c: number | null;
  oracle_c: number | null;
  oi_o: number | null;
  oi_h: number | null;
  oi_l: number | null;
  oi_c: number | null;
  oi_usd_o: number | null;
  oi_usd_h: number | null;
  oi_usd_l: number | null;
  oi_usd_c: number | null;
  funding_hr_c: number | null;
  premium_a: number | null;
  day_ntl_vlm_c: number | null;
  n_ticks: number;
}

export type CandleInterval = "5m" | "1h" | "1d";

const PERP_CANDLE_TABLES: Record<"5m" | "1h", string> = {
  "5m": "perp_candles_5m",
  "1h": "perp_candles_1h",
};

// Most recent `limit` candles within [from, to), ascending. 1d is aggregated from 1h.
export async function perpCandles(
  coin: string,
  interval: CandleInterval,
  fromMs: number,
  toMs: number,
  limit: number,
): Promise<PerpCandleRow[]> {
  let sql: string;
  if (interval === "1d") {
    sql = `
      select * from (
        select date_trunc('day', t) as t,
          (array_agg(mid_o order by t) filter (where mid_o is not null))[1] as mid_o,
          max(mid_h) as mid_h, min(mid_l) as mid_l,
          (array_agg(mid_c order by t desc) filter (where mid_c is not null))[1] as mid_c,
          (array_agg(mark_c order by t desc) filter (where mark_c is not null))[1] as mark_c,
          (array_agg(oracle_c order by t desc) filter (where oracle_c is not null))[1] as oracle_c,
          (array_agg(oi_o order by t) filter (where oi_o is not null))[1] as oi_o,
          max(oi_h) as oi_h, min(oi_l) as oi_l,
          (array_agg(oi_c order by t desc) filter (where oi_c is not null))[1] as oi_c,
          (array_agg(oi_usd_o order by t) filter (where oi_usd_o is not null))[1] as oi_usd_o,
          max(oi_usd_h) as oi_usd_h, min(oi_usd_l) as oi_usd_l,
          (array_agg(oi_usd_c order by t desc) filter (where oi_usd_c is not null))[1] as oi_usd_c,
          (array_agg(funding_hr_c order by t desc) filter (where funding_hr_c is not null))[1] as funding_hr_c,
          avg(premium_a) as premium_a,
          (array_agg(day_ntl_vlm_c order by t desc) filter (where day_ntl_vlm_c is not null))[1] as day_ntl_vlm_c,
          sum(n_ticks)::int as n_ticks
        from perp_candles_1h
        where coin = $1 and t >= to_timestamp($2 / 1000.0) and t < to_timestamp($3 / 1000.0)
        group by 1
      ) d
      order by t desc limit $4`;
  } else {
    sql = `
      select t, mid_o, mid_h, mid_l, mid_c, mark_c, oracle_c,
             oi_o, oi_h, oi_l, oi_c, oi_usd_o, oi_usd_h, oi_usd_l, oi_usd_c,
             funding_hr_c, premium_a, day_ntl_vlm_c, n_ticks
      from ${PERP_CANDLE_TABLES[interval]}
      where coin = $1 and t >= to_timestamp($2 / 1000.0) and t < to_timestamp($3 / 1000.0)
      order by t desc limit $4`;
  }
  const { rows } = await pool.query<PerpCandleRow>(sql, [coin, fromMs, toMs, limit]);
  return rows.reverse();
}

export interface MarketCandleRow {
  t: Date;
  oi_usd_o: number | null;
  oi_usd_h: number | null;
  oi_usd_l: number | null;
  oi_usd_c: number | null;
  funding_hr_oiw_a: number | null;
  day_ntl_vlm_c: number | null;
  n_coins: number | null;
  n_ticks: number;
}

const MARKET_CANDLE_TABLES: Record<"5m" | "1h", string> = {
  "5m": "market_candles_5m",
  "1h": "market_candles_1h",
};

export async function marketCandles(
  interval: CandleInterval,
  fromMs: number,
  toMs: number,
  limit: number,
): Promise<MarketCandleRow[]> {
  let sql: string;
  if (interval === "1d") {
    sql = `
      select * from (
        select date_trunc('day', t) as t,
          (array_agg(oi_usd_o order by t) filter (where oi_usd_o is not null))[1] as oi_usd_o,
          max(oi_usd_h) as oi_usd_h, min(oi_usd_l) as oi_usd_l,
          (array_agg(oi_usd_c order by t desc) filter (where oi_usd_c is not null))[1] as oi_usd_c,
          avg(funding_hr_oiw_a) as funding_hr_oiw_a,
          (array_agg(day_ntl_vlm_c order by t desc) filter (where day_ntl_vlm_c is not null))[1] as day_ntl_vlm_c,
          max(n_coins) as n_coins,
          sum(n_ticks)::int as n_ticks
        from market_candles_1h
        where t >= to_timestamp($1 / 1000.0) and t < to_timestamp($2 / 1000.0)
        group by 1
      ) d
      order by t desc limit $3`;
  } else {
    sql = `
      select t, oi_usd_o, oi_usd_h, oi_usd_l, oi_usd_c, funding_hr_oiw_a,
             day_ntl_vlm_c, n_coins, n_ticks
      from ${MARKET_CANDLE_TABLES[interval]}
      where t >= to_timestamp($1 / 1000.0) and t < to_timestamp($2 / 1000.0)
      order by t desc limit $3`;
  }
  const { rows } = await pool.query<MarketCandleRow>(sql, [fromMs, toMs, limit]);
  return rows.reverse();
}

export interface FundingRow {
  ts: Date;
  rate_hr: number;
  premium: number | null;
}

export async function fundingRows(
  coin: string,
  fromMs: number,
  toMs: number,
  limit: number,
): Promise<FundingRow[]> {
  const { rows } = await pool.query<FundingRow>(
    `select ts, rate_hr, premium
     from funding_history
     where coin = $1 and ts >= to_timestamp($2 / 1000.0) and ts < to_timestamp($3 / 1000.0)
     order by ts desc limit $4`,
    [coin, fromMs, toMs, limit],
  );
  return rows.reverse();
}

export interface PositioningRow {
  ts: Date;
  coin: string;
  n_long: number;
  n_short: number;
  sz_long: number | null;
  sz_short: number | null;
  ntl_long: number | null;
  ntl_short: number | null;
  traders_tracked: number | null;
  source: string; // 'live' (our ledger) or 'hypertracker' (backfilled history)
  avg_entry_long: number | null;
  avg_entry_short: number | null;
  n_long_entry: number | null;
  n_short_entry: number | null;
  n_long_profit: number | null;
  n_short_profit: number | null;
}

const POSITIONING_COLS = `ts, coin, n_long, n_short, sz_long, sz_short, ntl_long, ntl_short,
  traders_tracked, source, avg_entry_long, avg_entry_short,
  n_long_entry, n_short_entry, n_long_profit, n_short_profit`;

export async function latestPositioning(): Promise<PositioningRow[]> {
  const { rows } = await pool.query<PositioningRow>(
    `select distinct on (coin) ${POSITIONING_COLS}
     from positioning_snapshots order by coin, ts desc`,
  );
  return rows;
}

export async function latestPositioningFor(coin: string): Promise<PositioningRow | null> {
  const { rows } = await pool.query<PositioningRow>(
    `select ${POSITIONING_COLS} from positioning_snapshots
     where coin = $1 order by ts desc limit 1`,
    [coin],
  );
  return rows[0] ?? null;
}

export async function positioningHistory(
  coin: string,
  fromMs: number,
  toMs: number,
  limit: number,
): Promise<PositioningRow[]> {
  const { rows } = await pool.query<PositioningRow>(
    `select ${POSITIONING_COLS} from positioning_snapshots
     where coin = $1 and ts >= to_timestamp($2 / 1000.0) and ts < to_timestamp($3 / 1000.0)
     order by ts desc limit $4`,
    [coin, fromMs, toMs, limit],
  );
  return rows.reverse();
}

// Snapshot at or just before the target time (for change-over-window math).
// Falls back to a wider window so 2h-sampled backfilled history still qualifies.
export async function positioningAt(coin: string, targetMs: number): Promise<PositioningRow | null> {
  for (const tolerance of ["45 minutes", "130 minutes"]) {
    const { rows } = await pool.query<PositioningRow>(
      `select ${POSITIONING_COLS} from positioning_snapshots
       where coin = $1 and ts <= to_timestamp($2 / 1000.0)
         and ts >= to_timestamp($2 / 1000.0) - $3::interval
       order by ts desc limit 1`,
      [coin, targetMs, tolerance],
    );
    if (rows[0]) return rows[0];
  }
  return null;
}

// Coverage counts scan the whole traders and positions tables — cache them so
// per-request positioning endpoints don't repeat two full scans each hit.
export function trackerCoverage(): Promise<{ tracked: number; pending: number; provisional: number }> {
  return cached("coverage:tracker", 60_000, async () => {
    const { rows } = await pool.query<{ tracked: number; pending: number; provisional: number }>(
      `select
         (count(*) filter (where not bootstrap_pending))::int as tracked,
         (count(*) filter (where bootstrap_pending))::int as pending,
         (select count(distinct p.address)
            from positions p join traders t on t.address = p.address
            where t.bootstrapped_at is null)::int as provisional
       from traders`,
    );
    return rows[0] ?? { tracked: 0, pending: 0, provisional: 0 };
  });
}

export interface EmaStateApiRow {
  coin: string;
  tf: string;
  period: number;
  ema: number | null; // null while a young listing accumulates its first `period` closes
  n_candles: number;
  last_open_ms: string; // bigint arrives as a string
  updated_at: Date;
}

const EMA_COLS = "s.coin, s.tf, s.period, s.ema, s.n_candles, s.last_open_ms, s.updated_at";

// Latest EMA per (coin, timeframe, period) for every live coin — the whole
// table is ~coins × timeframes × periods rows, so one scan serves the screener.
export async function emaStates(): Promise<EmaStateApiRow[]> {
  const { rows } = await pool.query<EmaStateApiRow>(
    `select ${EMA_COLS} from ema_state s
     join perp_assets a on a.coin = s.coin
     where a.is_delisted = false`,
  );
  return rows;
}

export async function emaStatesFor(coin: string): Promise<EmaStateApiRow[]> {
  const { rows } = await pool.query<EmaStateApiRow>(`select ${EMA_COLS} from ema_state s where s.coin = $1`, [coin]);
  return rows;
}

// Liquidation series buckets. Sub-hour intervals aggregate the 5m candles, everything
// else the permanent 1h candles; sums are exact because one liquidation event never
// spans buckets (all fills of a forced order share one exchange timestamp).
export const LIQ_BUCKET_MS: Record<string, number> = {
  "5m": 300_000,
  "15m": 900_000,
  "1h": 3_600_000,
  "4h": 14_400_000,
  "12h": 43_200_000,
  "1d": 86_400_000,
};

export interface LiqCandleRow {
  t: Date;
  long_ntl: number;
  short_ntl: number;
  long_events: number;
  short_events: number;
  long_fills: number;
  short_fills: number;
}

const LIQ_CANDLE_SUMS = `
  sum(long_ntl) as long_ntl, sum(short_ntl) as short_ntl,
  sum(long_events)::int as long_events, sum(short_events)::int as short_events,
  sum(long_fills)::int as long_fills, sum(short_fills)::int as short_fills`;

// Most recent `limit` liquidation buckets within [from, to), ascending. Buckets with
// no liquidations are omitted. coin = null aggregates the whole venue.
export async function liqCandles(
  coin: string | null,
  bucketMs: number,
  fromMs: number,
  toMs: number,
  limit: number,
): Promise<LiqCandleRow[]> {
  const table = bucketMs < 3_600_000 ? "liq_candles_5m" : "liq_candles_1h";
  const { rows } = await pool.query<LiqCandleRow>(
    `select * from (
       select to_timestamp(floor(extract(epoch from t) / $1) * $1) as t, ${LIQ_CANDLE_SUMS}
       from ${table}
       where ($2::text is null or coin = $2)
         and t >= to_timestamp($3 / 1000.0) and t < to_timestamp($4 / 1000.0)
       group by 1
     ) b order by t desc limit $5`,
    [bucketMs / 1000, coin, fromMs, toMs, limit],
  );
  return rows.reverse();
}

export interface LiqTotalsRow {
  coin: string;
  long_ntl: number;
  short_ntl: number;
  long_events: number;
  short_events: number;
  long_fills: number;
  short_fills: number;
  traders: number; // distinct wallets liquidated
}

// Per-coin liquidation totals over a trailing window, from raw fills (exact windows,
// bounded by LIQ_RETENTION_DAYS). coin = null returns every coin with liquidations.
export async function liqTotals(windowMs: number, coin: string | null): Promise<LiqTotalsRow[]> {
  const { rows } = await pool.query<LiqTotalsRow>(
    `select coin,
       coalesce(sum(ntl) filter (where side = 'long'), 0) as long_ntl,
       coalesce(sum(ntl) filter (where side = 'short'), 0) as short_ntl,
       (count(distinct (wallet, ts)) filter (where side = 'long'))::int as long_events,
       (count(distinct (wallet, ts)) filter (where side = 'short'))::int as short_events,
       (count(*) filter (where side = 'long'))::int as long_fills,
       (count(*) filter (where side = 'short'))::int as short_fills,
       count(distinct wallet)::int as traders
     from liq_fills
     where ts >= now() - make_interval(secs => $1) and ($2::text is null or coin = $2)
     group by coin`,
    [windowMs / 1000, coin],
  );
  return rows;
}

// Distinct wallets liquidated venue-wide over a trailing window. Not the sum of the
// per-coin counts: one wallet liquidated on BTC and ETH in a cascade is one trader.
export async function liqVenueTraders(windowMs: number): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(
    `select count(distinct wallet)::int as n from liq_fills where ts >= now() - make_interval(secs => $1)`,
    [windowMs / 1000],
  );
  return rows[0]?.n ?? 0;
}

export interface LiqLargestRow {
  coin: string;
  wallet: string;
  ts: Date;
  side: string;
  ntl: number;
  px: number; // notional-weighted fill price
  fills: number;
}

// Per coin, the single largest forced liquidation order in the window — one forced
// order is every fill sharing (wallet, ts). The venue-wide largest is the max of these.
export async function liqLargestOrders(windowMs: number): Promise<LiqLargestRow[]> {
  const { rows } = await pool.query<LiqLargestRow>(
    `select distinct on (coin) coin, wallet, ts, side, ntl, px, fills from (
       select coin, wallet, ts, min(side) as side, sum(ntl) as ntl,
              sum(ntl) / nullif(sum(sz), 0) as px, count(*)::int as fills
       from liq_fills
       where ts >= now() - make_interval(secs => $1)
       group by coin, wallet, ts
     ) o order by coin, ntl desc, ts desc`,
    [windowMs / 1000],
  );
  return rows;
}

export interface LiqFillRow {
  tid: string; // bigint arrives as a string
  ts: Date;
  coin: string;
  side: string;
  px: number;
  sz: number;
  ntl: number;
  wallet: string;
  method: string | null;
}

// Freshness hint for the board: distinguishes "no liquidations lately" (live
// timestamp, quiet market) from a recorder that isn't running yet.
export async function lastLiqAt(): Promise<Date | null> {
  const { rows } = await pool.query<{ max: Date | null }>("select max(ts) as max from liq_fills");
  return rows[0]?.max ?? null;
}

export async function recentLiqFills(coin: string | null, limit: number): Promise<LiqFillRow[]> {
  const { rows } = await pool.query<LiqFillRow>(
    `select tid, ts, coin, side, px, sz, ntl, wallet, method
     from liq_fills
     where ($1::text is null or coin = $1)
     order by ts desc limit $2`,
    [coin, limit],
  );
  return rows;
}

// Venue OI (close) at or just before the target time, for market snapshot change math.
export async function marketOiCloseAt(targetMs: number): Promise<number | null> {
  const { rows } = await pool.query<{ oi_usd_c: number | null }>(
    `select oi_usd_c from market_candles_5m
     where t <= to_timestamp($1 / 1000.0)
       and t >= to_timestamp($1 / 1000.0) - interval '30 minutes'
     order by t desc limit 1`,
    [targetMs],
  );
  return rows[0]?.oi_usd_c ?? null;
}

// ---- Whale discovery (bridge deposits + watched wallets) ----

export interface BridgeStatus {
  last_block: string; // bigint arrives as a string
  updated_at: Date;
  last_deposit_at: Date | null;
}

export async function bridgeStatus(): Promise<BridgeStatus | null> {
  const { rows } = await pool.query<BridgeStatus>(
    `select s.last_block, s.updated_at, (select max(ts) from bridge_deposits) as last_deposit_at
     from bridge_sync s where s.id = 1`,
  );
  return rows[0] ?? null;
}

export interface BridgeDepositRow {
  tx_hash: string;
  log_index: number;
  block_number: string;
  ts: Date;
  address: string;
  usdc: number;
}

export async function recentBridgeDeposits(windowMs: number, minUsd: number, limit: number): Promise<BridgeDepositRow[]> {
  const { rows } = await pool.query<BridgeDepositRow>(
    `select tx_hash, log_index, block_number, ts, address, usdc from bridge_deposits
     where ts > now() - make_interval(secs => $1) and usdc >= $2
     order by ts desc, log_index desc limit $3`,
    [windowMs / 1000, minUsd, limit],
  );
  return rows;
}

export interface WhaleRow {
  address: string;
  deposited_usd: number; // sum over the requested window
  n_deposits: number;
  first_at: Date; // earliest deposit in the requested window
  last_at: Date;
  has_position: boolean; // live: any szi <> 0 in the positions ledger
  flagged_at: Date | null;
  watch_until: Date | null;
  account_value: number | null;
  total_ntl_pos: number | null;
  state_checked_at: Date | null;
  ledger_first_at: Date | null;
  ledger_checked_at: Date | null;
  first_trade_at: Date | null;
  positioned_at: Date | null;
  baseline_positions: Array<{ coin: string; szi: number }> | null;
}

// Addresses whose bridge deposits in the trailing window total at least minUsd,
// joined with whatever the watcher has learned about them on Hyperliquid.
// `positioned` filters on live open positions; `newOnly` keeps accounts whose
// first-ever ledger entry falls inside this deposit episode (brand-new wallets).
export async function whaleWallets(
  windowMs: number,
  minUsd: number,
  positioned: boolean | null,
  newOnly: boolean,
  limit: number,
): Promise<WhaleRow[]> {
  const { rows } = await pool.query<WhaleRow>(
    `with dep as (
       select address, sum(usdc) as usd, count(*)::int as n, min(ts) as first_at, max(ts) as last_at
       from bridge_deposits
       where ts > now() - make_interval(secs => $1)
       group by address having sum(usdc) >= $2
     ), joined as (
       select dep.address, dep.usd as deposited_usd, dep.n as n_deposits, dep.first_at, dep.last_at,
              exists (select 1 from positions p where p.address = dep.address and p.szi <> 0) as has_position,
              w.flagged_at, w.watch_until, w.account_value, w.total_ntl_pos, w.state_checked_at,
              w.ledger_first_at, w.ledger_checked_at, w.first_trade_at, w.positioned_at, w.baseline_positions
       from dep left join whale_wallets w on w.address = dep.address
     )
     select * from joined
     where ($3::boolean is null or has_position = $3)
       and (not $4::boolean or (ledger_first_at is not null and ledger_first_at >= first_at - interval '10 minutes'))
     order by deposited_usd desc, last_at desc
     limit $5`,
    [windowMs / 1000, minUsd, positioned, newOnly, limit],
  );
  return rows;
}

export interface WalletPositionRow {
  address: string;
  coin: string;
  szi: number;
  entry_px: number | null;
  updated_at: Date;
}

export async function openPositionsFor(addresses: string[]): Promise<WalletPositionRow[]> {
  if (addresses.length === 0) return [];
  const { rows } = await pool.query<WalletPositionRow>(
    `select address, coin, szi, entry_px, updated_at from positions
     where address = any($1::text[]) and szi <> 0
     order by address, abs(szi) desc`,
    [addresses],
  );
  return rows;
}

export interface WhaleAlertRow {
  id: string;
  ts: Date;
  kind: string;
  address: string;
  deposited_usd: number;
  account_value: number | null;
  total_ntl_pos: number | null;
  is_new_account: boolean | null;
  ledger_first_at: Date | null;
  positions: Array<{ coin: string; side: string; sz: number; entryPx: number | null }>;
  opened: Array<{ coin: string; side: string; sz: number; entryPx: number | null; ntlUsd: number }>;
  message: string;
  delivered: boolean | null;
  delivery_error: string | null;
}

export async function listWhaleAlerts(f: { kind?: string; address?: string; sinceMs?: number; limit: number }): Promise<WhaleAlertRow[]> {
  const { rows } = await pool.query<WhaleAlertRow>(
    `select id, ts, kind, address, deposited_usd, account_value, total_ntl_pos, is_new_account, ledger_first_at,
            positions, opened, message, delivered, delivery_error
     from whale_alerts
     where ($1::text is null or kind = $1)
       and ($2::text is null or address = $2)
       and ($3::timestamptz is null or ts >= $3)
     order by ts desc, id desc limit $4`,
    [f.kind ?? null, f.address ?? null, f.sinceMs !== undefined ? new Date(f.sinceMs) : null, f.limit],
  );
  return rows;
}
