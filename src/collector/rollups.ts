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

async function rollupRange(fromMs: number, toMs: number): Promise<void> {
  for (const g of GRANULARITIES) {
    const from = alignDown(fromMs, g.seconds); // widen to the bucket edge so opens are correct
    const to = new Date(toMs);
    await pool.query(perpSql(g.perpTable), [from, to, g.seconds]);
    await pool.query(marketSql(g.marketTable), [from, to, g.seconds]);
  }
}

// Recompute the live bucket plus the previous two per granularity (covers late runs).
export async function runIncrementalRollups(): Promise<void> {
  const now = Date.now();
  for (const g of GRANULARITIES) {
    const from = alignDown(now - 2 * g.seconds * 1000, g.seconds);
    await pool.query(perpSql(g.perpTable), [from, new Date(now), g.seconds]);
    await pool.query(marketSql(g.marketTable), [from, new Date(now), g.seconds]);
  }
}

// On startup, rebuild rollups over every raw tick still retained (day-sized chunks).
// Self-heals candle tables after downtime or a schema reset.
export async function bootstrapRollups(): Promise<number> {
  const { rows } = await pool.query<{ min: Date | null }>("select min(ts) as min from perp_ticks");
  const min = rows[0]?.min;
  if (!min) return 0;
  const now = Date.now();
  let chunkStart = alignDown(min.getTime(), 86_400).getTime();
  let chunks = 0;
  while (chunkStart < now) {
    const chunkEnd = Math.min(chunkStart + 86_400_000, now);
    await rollupRange(chunkStart, chunkEnd);
    chunkStart = chunkEnd;
    chunks++;
  }
  return chunks;
}
