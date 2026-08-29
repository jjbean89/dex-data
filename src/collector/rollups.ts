import { pool } from "../db/pool.js";

// Candle rollups are upserts, so recomputing a bucket (including the live partial one)
// is always safe and idempotent.

const GRANULARITIES = [
  { seconds: 300, perpTable: "perp_candles_5m", marketTable: "market_candles_5m" },
  { seconds: 3600, perpTable: "perp_candles_1h", marketTable: "market_candles_1h" },
] as const;

const BUCKET = "to_timestamp(floor(extract(epoch from ts) / $3) * $3)";

const perpSql = (table: string): string => `
  insert into ${table} (coin, t, mid_o, mid_h, mid_l, mid_c, mark_c, oracle_c,
                        oi_o, oi_h, oi_l, oi_c, oi_usd_o, oi_usd_h, oi_usd_l, oi_usd_c,
                        funding_hr_c, premium_a, day_ntl_vlm_c, n_ticks)
  select
    coin,
    ${BUCKET} as t,
    (array_agg(mid_px order by ts) filter (where mid_px is not null))[1],
    max(mid_px), min(mid_px),
    (array_agg(mid_px order by ts desc) filter (where mid_px is not null))[1],
    (array_agg(mark_px order by ts desc) filter (where mark_px is not null))[1],
    (array_agg(oracle_px order by ts desc) filter (where oracle_px is not null))[1],
    (array_agg(open_interest order by ts) filter (where open_interest is not null))[1],
    max(open_interest), min(open_interest),
    (array_agg(open_interest order by ts desc) filter (where open_interest is not null))[1],
    (array_agg(oi_usd order by ts) filter (where oi_usd is not null))[1],
    max(oi_usd), min(oi_usd),
    (array_agg(oi_usd order by ts desc) filter (where oi_usd is not null))[1],
    (array_agg(funding_hr order by ts desc) filter (where funding_hr is not null))[1],
    avg(premium),
    (array_agg(day_ntl_vlm order by ts desc) filter (where day_ntl_vlm is not null))[1],
    count(*)::int
  from perp_ticks
  where ts >= $1 and ts < $2
  group by coin, ${BUCKET}
  on conflict (coin, t) do update set
    mid_o = excluded.mid_o, mid_h = excluded.mid_h, mid_l = excluded.mid_l, mid_c = excluded.mid_c,
    mark_c = excluded.mark_c, oracle_c = excluded.oracle_c,
    oi_o = excluded.oi_o, oi_h = excluded.oi_h, oi_l = excluded.oi_l, oi_c = excluded.oi_c,
    oi_usd_o = excluded.oi_usd_o, oi_usd_h = excluded.oi_usd_h,
    oi_usd_l = excluded.oi_usd_l, oi_usd_c = excluded.oi_usd_c,
    funding_hr_c = excluded.funding_hr_c, premium_a = excluded.premium_a,
    day_ntl_vlm_c = excluded.day_ntl_vlm_c, n_ticks = excluded.n_ticks`;

// Venue-wide: sum OI across coins per tick (OI-weighted funding likewise), then OHLC
// over the per-tick totals within the bucket — so highs/lows are true venue extremes.
const marketSql = (table: string): string => `
  insert into ${table} (t, oi_usd_o, oi_usd_h, oi_usd_l, oi_usd_c, funding_hr_oiw_a,
                        day_ntl_vlm_c, n_coins, n_ticks)
  select
    to_timestamp(floor(extract(epoch from tick_ts) / $3) * $3) as t,
    (array_agg(total_oi order by tick_ts))[1],
    max(total_oi), min(total_oi),
    (array_agg(total_oi order by tick_ts desc))[1],
    avg(funding_oiw),
    (array_agg(total_vlm order by tick_ts desc))[1],
    max(n_coins), count(*)::int
  from (
    select
      ts as tick_ts,
      sum(oi_usd) as total_oi,
      sum(funding_hr * oi_usd) filter (where funding_hr is not null and oi_usd is not null)
        / nullif(sum(oi_usd) filter (where funding_hr is not null and oi_usd is not null), 0) as funding_oiw,
      sum(day_ntl_vlm) as total_vlm,
      (count(*) filter (where oi_usd is not null))::int as n_coins
    from perp_ticks
    where ts >= $1 and ts < $2
    group by ts
  ) per_tick
  group by 1
  on conflict (t) do update set
    oi_usd_o = excluded.oi_usd_o, oi_usd_h = excluded.oi_usd_h,
    oi_usd_l = excluded.oi_usd_l, oi_usd_c = excluded.oi_usd_c,
    funding_hr_oiw_a = excluded.funding_hr_oiw_a, day_ntl_vlm_c = excluded.day_ntl_vlm_c,
    n_coins = excluded.n_coins, n_ticks = excluded.n_ticks`;

function alignDown(ms: number, seconds: number): Date {
  const b = seconds * 1000;
  return new Date(Math.floor(ms / b) * b);
}

// Everything before this instant is already rolled up. Ticks are stamped at collect
// time and committed moments later, so each run re-covers a short overlap window to
// absorb inserts that were in flight during the previous run.
let rolledUpTo: number | null = null;
const OVERLAP_MS = 90_000;

async function rollupRange(fromMs: number, toMs: number): Promise<void> {
  for (const g of GRANULARITIES) {
    const from = alignDown(fromMs, g.seconds); // widen to the bucket edge so opens are correct
    const to = new Date(toMs);
    await pool.query(perpSql(g.perpTable), [from, to, g.seconds]);
    await pool.query(marketSql(g.marketTable), [from, to, g.seconds]);
  }
}

// Recompute only buckets touched since the last successful run (watermark not
// advanced on failure, so errored ranges are re-covered next time).
export async function runIncrementalRollups(): Promise<void> {
  const now = Date.now();
  const from = rolledUpTo === null ? now - 2 * 3_600_000 : rolledUpTo - OVERLAP_MS;
  await rollupRange(Math.min(from, now), now);
  rolledUpTo = now;
}

// On startup, resume rollups from the oldest live candle bucket (the partial bucket
// being built when the previous process stopped). Only when a candle table is empty
// (fresh schema) does this rebuild over every retained raw tick, in day-sized chunks.
export async function bootstrapRollups(): Promise<number> {
  const now = Date.now();
  const { rows } = await pool.query<{
    tick_min: Date | null;
    p5: Date | null;
    p1h: Date | null;
    m5: Date | null;
    m1h: Date | null;
  }>(
    `select (select min(ts) from perp_ticks) as tick_min,
            (select max(t) from perp_candles_5m) as p5,
            (select max(t) from perp_candles_1h) as p1h,
            (select max(t) from market_candles_5m) as m5,
            (select max(t) from market_candles_1h) as m1h`,
  );
  const r = rows[0];
  if (!r?.tick_min) {
    rolledUpTo = now;
    return 0;
  }
  const maxes = [r.p5, r.p1h, r.m5, r.m1h];
  // Ticks never predate tick_min, so buckets older than it can't have changed.
  const resume = maxes.some((m) => m === null)
    ? r.tick_min.getTime()
    : Math.max(Math.min(...maxes.map((m) => m!.getTime())), r.tick_min.getTime());
  let chunkStart = resume;
  let chunks = 0;
  while (chunkStart < now) {
    const chunkEnd = Math.min(chunkStart + 86_400_000, now);
    await rollupRange(chunkStart, chunkEnd);
    chunkStart = chunkEnd;
    chunks++;
  }
  rolledUpTo = now;
  return chunks;
}
