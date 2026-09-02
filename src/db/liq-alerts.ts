import type { LiqAlertRule, LiqAlertSide } from "../config.js";
import { pool } from "./pool.js";

// Shared between the collector (evaluator) and the API (status + history), which
// run as separate processes on Railway: the collector mirrors its configured
// rules into liq_alert_rules so the API needs no copy of LIQ_ALERT_RULES.

export interface LiqWindowTotals {
  windowMs: number;
  longNtl: number;
  shortNtl: number;
  longEvents: number;
  shortEvents: number;
  longFills: number;
  shortFills: number;
}

// Exact trailing-window liquidation totals for one coin over several windows in
// a single indexed query (liq_fills (coin, ts)).
export async function liqWindowTotals(coin: string, windowsMs: number[]): Promise<Map<number, LiqWindowTotals>> {
  const { rows } = await pool.query<{
    window_ms: string;
    long_ntl: number;
    short_ntl: number;
    long_events: number;
    short_events: number;
    long_fills: number;
    short_fills: number;
  }>(
    `select w.window_ms::text as window_ms,
       coalesce(sum(f.ntl) filter (where f.side = 'long'), 0) as long_ntl,
       coalesce(sum(f.ntl) filter (where f.side = 'short'), 0) as short_ntl,
       (count(distinct (f.wallet, f.ts)) filter (where f.side = 'long'))::int as long_events,
       (count(distinct (f.wallet, f.ts)) filter (where f.side = 'short'))::int as short_events,
       (count(f.tid) filter (where f.side = 'long'))::int as long_fills,
       (count(f.tid) filter (where f.side = 'short'))::int as short_fills
     from unnest($2::bigint[]) as w(window_ms)
     left join liq_fills f
       on f.coin = $1 and f.ts >= now() - make_interval(secs => w.window_ms / 1000.0)
     group by w.window_ms`,
    [coin, [...new Set(windowsMs)]],
  );
  const out = new Map<number, LiqWindowTotals>();
  for (const r of rows) {
    out.set(Number(r.window_ms), {
      windowMs: Number(r.window_ms),
      longNtl: r.long_ntl,
      shortNtl: r.short_ntl,
      longEvents: r.long_events,
      shortEvents: r.short_events,
      longFills: r.long_fills,
      shortFills: r.short_fills,
    });
  }
  return out;
}

// The side's own numbers out of a window total: 'total' folds both sides together.
export function sideSlice(t: LiqWindowTotals, side: LiqAlertSide): { ntl: number; events: number; fills: number } {
  if (side === "long") return { ntl: t.longNtl, events: t.longEvents, fills: t.longFills };
  if (side === "short") return { ntl: t.shortNtl, events: t.shortEvents, fills: t.shortFills };
  return { ntl: t.longNtl + t.shortNtl, events: t.longEvents + t.shortEvents, fills: t.longFills + t.shortFills };
}

export interface LiqAlertRuleRow {
  coin: string;
  win: string;
  side: LiqAlertSide;
  window_ms: string; // bigint arrives as a string
  threshold_usd: number;
  active: boolean;
  last_value: number | null;
  last_eval_at: Date | null;
  last_fired_at: Date | null;
  updated_at: Date;
}

export async function loadLiqAlertRules(): Promise<LiqAlertRuleRow[]> {
  const { rows } = await pool.query<LiqAlertRuleRow>(
    "select * from liq_alert_rules order by coin, window_ms, side",
  );
  return rows;
}

// Mirror the configured rule set: upsert thresholds (state columns survive), drop
// rules no longer configured. Returns the current state for every rule.
export async function syncLiqAlertRules(rules: LiqAlertRule[]): Promise<LiqAlertRuleRow[]> {
  const client = await pool.connect();
  try {
    await client.query("begin");
    if (rules.length > 0) {
      await client.query(
        `insert into liq_alert_rules (coin, win, side, window_ms, threshold_usd)
         select * from unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::float8[])
         on conflict (coin, win, side) do update set
           window_ms = excluded.window_ms, threshold_usd = excluded.threshold_usd, updated_at = now()`,
        [
          rules.map((r) => r.coin),
          rules.map((r) => r.window),
          rules.map((r) => r.side),
          rules.map((r) => r.windowMs),
          rules.map((r) => r.thresholdUsd),
        ],
      );
    }
    await client.query(
      `delete from liq_alert_rules r
       where not exists (
         select 1 from unnest($1::text[], $2::text[], $3::text[]) as k(coin, win, side)
         where k.coin = r.coin and k.win = r.win and k.side = r.side
       )`,
      [rules.map((r) => r.coin), rules.map((r) => r.window), rules.map((r) => r.side)],
    );
    const { rows } = await client.query<LiqAlertRuleRow>("select * from liq_alert_rules order by coin, window_ms, side");
    await client.query("commit");
    return rows;
  } catch (err) {
    await client.query("rollback");
    throw err;
  } finally {
    client.release();
  }
}

export interface LiqAlertRow {
  id: string; // bigserial arrives as a string
  ts: Date;
  coin: string;
  win: string;
  window_ms: string;
  side: LiqAlertSide;
  ntl_usd: number;
  threshold_usd: number;
  events: number;
  fills: number;
  long_ntl: number;
  short_ntl: number;
  long_events: number;
  short_events: number;
  message: string;
  delivered: boolean | null;
  delivery_error: string | null;
}

export interface LiqAlertFilter {
  coin?: string;
  win?: string;
  side?: LiqAlertSide;
  sinceMs?: number;
  limit: number;
}

// Alert history, newest first.
export async function listLiqAlerts(f: LiqAlertFilter): Promise<LiqAlertRow[]> {
  const { rows } = await pool.query<LiqAlertRow>(
    `select * from liq_alerts
     where ($1::text is null or coin = $1)
       and ($2::text is null or win = $2)
       and ($3::text is null or side = $3)
       and ($4::float8 is null or ts >= to_timestamp($4 / 1000.0))
     order by ts desc, id desc limit $5`,
    [f.coin ?? null, f.win ?? null, f.side ?? null, f.sinceMs ?? null, f.limit],
  );
  return rows;
}
