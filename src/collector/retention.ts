import { config } from "../config.js";
import { pool } from "../db/pool.js";

// Raw ticks, liquidation fills, and 5m candles are bounded; 1h candles (per-coin
// and venue-wide) are the permanent record and are never pruned.
export async function pruneOldData(): Promise<{ ticks: number; candles5m: number; flatPositions: number }> {
  const t = await pool.query("delete from perp_ticks where ts < now() - make_interval(days => $1)", [
    config.rawRetentionDays,
  ]);
  const c1 = await pool.query("delete from perp_candles_5m where t < now() - make_interval(days => $1)", [
    config.candles5mRetentionDays,
  ]);
  const c2 = await pool.query("delete from market_candles_5m where t < now() - make_interval(days => $1)", [
    config.candles5mRetentionDays,
  ]);
  await pool.query("delete from liq_fills where ts < now() - make_interval(days => $1)", [config.liqRetentionDays]);
  await pool.query("delete from liq_candles_5m where t < now() - make_interval(days => $1)", [
    config.candles5mRetentionDays,
  ]);
  await pool.query("delete from vol_candles_1m where t < now() - make_interval(days => $1)", [config.vol1mRetentionDays]);
  await pool.query("delete from vol_candles_5m where t < now() - make_interval(days => $1)", [config.candles5mRetentionDays]);
  await pool.query("delete from ops_events where ts < now() - interval '14 days'");
  await pool.query("delete from bridge_deposits where ts < now() - make_interval(days => $1)", [
    config.bridgeRetentionDays,
  ]);
  await pool.query("delete from whale_wallets where flagged_at < now() - make_interval(days => $1)", [
    config.bridgeRetentionDays,
  ]);
  await pool.query("delete from whale_alerts where ts < now() - make_interval(days => $1)", [config.bridgeRetentionDays]);
  // Fully closed positions are dead weight: snapshots filter szi <> 0, and a later
  // fill simply re-inserts the row. Keep the ledger to open positions only.
  const p = await pool.query(
    "delete from positions where szi = 0 and updated_at < now() - interval '1 hour'",
  );
  return {
    ticks: t.rowCount ?? 0,
    candles5m: (c1.rowCount ?? 0) + (c2.rowCount ?? 0),
    flatPositions: p.rowCount ?? 0,
  };
}
