import { pool } from "./pool.js";

// Price of one coin at (or just before) a point in time: the nearest raw tick within
// the tolerance, falling back to the 5m candle close once ticks have been pruned
// (RAW_RETENTION_DAYS). Used to put "as BTC moved −3.2%" next to a liquidation.
export async function pxAt(coin: string, atMs: number, tolMs: number = 180_000): Promise<number | null> {
  const { rows } = await pool.query<{ px: number | null }>(
    `select coalesce(mid_px, mark_px) as px from perp_ticks
     where coin = $1
       and ts >= to_timestamp($2 / 1000.0) - make_interval(secs => $3)
       and ts <= to_timestamp($2 / 1000.0) + make_interval(secs => $3)
     order by abs(extract(epoch from (ts - to_timestamp($2 / 1000.0)))) limit 1`,
    [coin, atMs, tolMs / 1000],
  );
  if (rows[0]?.px != null) return rows[0].px;
  const { rows: candles } = await pool.query<{ px: number | null }>(
    `select coalesce(mid_c, mark_c) as px from perp_candles_5m
     where coin = $1 and t <= to_timestamp($2 / 1000.0) and t >= to_timestamp($2 / 1000.0) - interval '1 hour'
       and coalesce(mid_c, mark_c) is not null
     order by t desc limit 1`,
    [coin, atMs],
  );
  return candles[0]?.px ?? null;
}
