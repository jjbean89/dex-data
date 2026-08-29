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
