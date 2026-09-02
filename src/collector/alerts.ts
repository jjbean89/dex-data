import { config, type LiqAlertRule } from "../config.js";
import { liqWindowTotals, sideSlice, syncLiqAlertRules, type LiqWindowTotals } from "../db/liq-alerts.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";

// Liquidation threshold alerts.
//
// Every LIQ_ALERT_INTERVAL_MS the evaluator computes, per rule coin, the exact
// trailing-window liquidated notional (long / short / total) straight from
// liq_fills — the same numbers GET /v1/perps/liquidations reports — and compares
// each rule's side against its threshold.
//
// Firing is edge-triggered with hysteresis: a rule fires once when its value
// crosses the threshold, then stays "active" until the value drops back under
// LIQ_ALERT_REARM_PCT% of the threshold (fills aging out of the trailing window).
// A cascade therefore yields one alert per (coin, window, side), not one per tick,
// and a value hovering at the threshold doesn't flap. State lives in
// liq_alert_rules so restarts neither re-fire an alert already sent nor miss a
// crossing that happened while the collector was down (that one fires on boot,
// stamped with the current value).
//
// Late-verified fills (the recorder classifies trades a few seconds to minutes
// after they print, and backfills after downtime) simply raise the window value
// on a later evaluation — nothing here depends on fills arriving in order.

const WEBHOOK_TIMEOUT_MS = 10_000;
const WEBHOOK_ATTEMPTS = 3;

interface RuleState {
  active: boolean;
}

interface FiredAlert {
  rule: LiqAlertRule;
  ts: Date;
  value: number;
  events: number;
  fills: number;
  totals: LiqWindowTotals;
  message: string;
}

export function formatUsd(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `$${(n / 1e3).toFixed(1)}k`;
  return `$${n.toFixed(0)}`;
}

function sideLabel(rule: LiqAlertRule): string {
  return rule.side === "long" ? "longs" : rule.side === "short" ? "shorts" : "positions";
}

function ruleKey(r: { coin: string; window: string; side: string }): string {
  return `${r.coin}|${r.window}|${r.side}`;
}

export function startLiqAlerts(isStopped: () => boolean): () => Promise<void> {
  const rules = config.liqAlertRules;
  const state = new Map<string, RuleState>();
  const warnedCoins = new Set<string>();
  let fired = 0;
  let evaluations = 0;

  async function init(): Promise<boolean> {
    try {
      const rows = await syncLiqAlertRules(rules);
      for (const r of rows) state.set(ruleKey({ coin: r.coin, window: r.win, side: r.side }), { active: r.active });
      return true;
    } catch (err) {
      logErr("alerts", "rule sync failed — retrying", err);
      return false;
    }
  }

  // A rule for a coin Hyperliquid has never listed is almost certainly a typo
  // (coin names are case-sensitive on the tape: BTC, ETH, kPEPE). Warn once.
  async function checkCoins(): Promise<void> {
    const coins = [...new Set(rules.map((r) => r.coin))].filter((c) => !warnedCoins.has(c));
    if (coins.length === 0) return;
    const { rows } = await pool.query<{ coin: string }>("select coin from perp_assets where coin = any($1::text[])", [coins]);
    const known = new Set(rows.map((r) => r.coin));
    const { rows: anyRows } = await pool.query<{ n: number }>("select count(*)::int as n from perp_assets");
    if ((anyRows[0]?.n ?? 0) === 0) return; // registry not filled yet (first tick pending)
    for (const c of coins) {
      warnedCoins.add(c);
      if (!known.has(c)) {
        const msg = `rule coin "${c}" is not a known Hyperliquid perp — check spelling/case in LIQ_ALERT_RULES`;
        log("alerts", `WARNING: ${msg}`);
        await opsEvent("alerts", "warn", msg);
      }
    }
  }

  async function evaluate(): Promise<void> {
    const now = new Date();
    const byCoin = new Map<string, LiqAlertRule[]>();
    for (const r of rules) {
      let list = byCoin.get(r.coin);
      if (!list) byCoin.set(r.coin, (list = []));
      list.push(r);
    }
    const toFire: FiredAlert[] = [];
    const updates: Array<{ rule: LiqAlertRule; value: number; active: boolean; firedAt: Date | null }> = [];
    for (const [coin, coinRules] of byCoin) {
      const totals = await liqWindowTotals(coin, coinRules.map((r) => r.windowMs));
      for (const rule of coinRules) {
        const t = totals.get(rule.windowMs);
        if (!t) continue;
        const slice = sideSlice(t, rule.side);
        const key = ruleKey(rule);
        const st = state.get(key) ?? { active: false };
        const rearmLevel = rule.thresholdUsd * (config.liqAlertRearmPct / 100);
        let active = st.active;
        let firedAt: Date | null = null;
        if (!st.active && slice.ntl >= rule.thresholdUsd) {
          active = true;
          firedAt = now;
          toFire.push({
            rule,
            ts: now,
            value: slice.ntl,
            events: slice.events,
            fills: slice.fills,
            totals: t,
            message:
              `${rule.coin}: ${formatUsd(slice.ntl)} of ${sideLabel(rule)} liquidated in the last ${rule.window} ` +
              `(${slice.events} forced order${slice.events === 1 ? "" : "s"}; threshold ${formatUsd(rule.thresholdUsd)})`,
          });
        } else if (st.active && slice.ntl < rearmLevel) {
          active = false;
        }
        state.set(key, { active });
        updates.push({ rule, value: slice.ntl, active, firedAt });
      }
    }
    await persist(updates, toFire);
    evaluations++;
    for (const a of toFire) {
      fired++;
      log("alerts", `ALERT ${a.message}`);
    }
  }

  async function persist(
    updates: Array<{ rule: LiqAlertRule; value: number; active: boolean; firedAt: Date | null }>,
    toFire: FiredAlert[],
  ): Promise<void> {
    const client = await pool.connect();
    try {
      await client.query("begin");
      if (updates.length > 0) {
        await client.query(
          `update liq_alert_rules r set
             active = u.active, last_value = u.value, last_eval_at = now(),
             last_fired_at = coalesce(u.fired_at, r.last_fired_at), updated_at = now()
           from unnest($1::text[], $2::text[], $3::text[], $4::float8[], $5::boolean[], $6::timestamptz[])
             as u(coin, win, side, value, active, fired_at)
           where r.coin = u.coin and r.win = u.win and r.side = u.side`,
          [
            updates.map((u) => u.rule.coin),
            updates.map((u) => u.rule.window),
            updates.map((u) => u.rule.side),
            updates.map((u) => u.value),
            updates.map((u) => u.active),
            updates.map((u) => u.firedAt),
          ],
        );
      }
      const deliveries: Array<{ id: string; alert: FiredAlert }> = [];
      for (const a of toFire) {
        const { rows } = await client.query<{ id: string }>(
          `insert into liq_alerts (ts, coin, win, window_ms, side, ntl_usd, threshold_usd, events, fills,
                                   long_ntl, short_ntl, long_events, short_events, message, delivered)
           values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
           returning id`,
          [
            a.ts,
            a.rule.coin,
            a.rule.window,
            a.rule.windowMs,
            a.rule.side,
            a.value,
            a.rule.thresholdUsd,
            a.events,
            a.fills,
            a.totals.longNtl,
            a.totals.shortNtl,
            a.totals.longEvents,
            a.totals.shortEvents,
            a.message,
            config.liqAlertWebhookUrl ? false : null,
          ],
        );
        if (config.liqAlertWebhookUrl && rows[0]) deliveries.push({ id: rows[0].id, alert: a });
      }
      await client.query("commit");
      // Delivery runs after commit, off the evaluation path; the row records the outcome.
      for (const d of deliveries) void deliver(d.id, d.alert);
    } catch (err) {
      await client.query("rollback");
      throw err;
    } finally {
      client.release();
    }
  }

  function webhookFormat(): "json" | "discord" | "slack" {
    if (config.liqAlertWebhookFormat !== "auto") return config.liqAlertWebhookFormat;
    try {
      const host = new URL(config.liqAlertWebhookUrl).hostname.toLowerCase();
      if (/(^|\.)discord(app)?\.com$/.test(host)) return "discord";
      if (/(^|\.)slack\.com$/.test(host)) return "slack";
    } catch {
      /* fall through */
    }
    return "json";
  }

  function webhookBody(id: string, a: FiredAlert): Record<string, unknown> {
    const fmt = webhookFormat();
    const text = `🚨 ${a.message}`;
    if (fmt === "discord") return { content: text };
    if (fmt === "slack") return { text };
    return { type: "liquidation_threshold", alert: serializeAlert(id, a) };
  }

  async function deliver(id: string, a: FiredAlert): Promise<void> {
    const body = JSON.stringify(webhookBody(id, a));
    let lastErr = "";
    for (let attempt = 0; attempt < WEBHOOK_ATTEMPTS; attempt++) {
      if (attempt > 0) await sleep(1_000 * 2 ** (attempt - 1));
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);
      try {
        const res = await fetch(config.liqAlertWebhookUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body,
          signal: controller.signal,
        });
        if (res.ok) {
          await pool.query("update liq_alerts set delivered = true, delivery_error = null where id = $1", [id]).catch(() => undefined);
          return;
        }
        lastErr = `HTTP ${res.status} ${(await res.text().catch(() => "")).slice(0, 200)}`.trim();
        if (res.status >= 400 && res.status < 500 && res.status !== 429) break; // not retryable
      } catch (err) {
        lastErr = err instanceof Error ? (err.name === "AbortError" ? "timeout" : err.message) : String(err);
      } finally {
        clearTimeout(timer);
      }
    }
    logErr("alerts", `webhook delivery failed for alert #${id}`, lastErr);
    await opsEvent("alerts", "error", `webhook delivery failed for alert #${id}: ${lastErr}`);
    await pool
      .query("update liq_alerts set delivered = false, delivery_error = $2 where id = $1", [id, lastErr.slice(0, 500)])
      .catch(() => undefined);
  }

  async function loop(): Promise<void> {
    while (!isStopped() && !(await init())) {
      await sleepStop(15_000);
    }
    let lastStats = Date.now();
    while (!isStopped()) {
      try {
        await evaluate();
        await checkCoins();
      } catch (err) {
        logErr("alerts", "evaluation failed", err);
      }
      if (Date.now() - lastStats >= 3_600_000) {
        const active = [...state.values()].filter((s) => s.active).length;
        log("alerts", `${evaluations} evaluations, ${fired} alerts fired this run, ${active} rule(s) currently over threshold`);
        lastStats = Date.now();
      }
      await sleepStop(config.liqAlertIntervalMs);
    }
  }

  async function sleepStop(ms: number): Promise<void> {
    const deadline = Date.now() + ms;
    while (!isStopped() && Date.now() < deadline) {
      await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
    }
  }

  const run = loop();
  log(
    "alerts",
    `liquidation alerts started: ${rules.length} rules over ${new Set(rules.map((r) => r.coin)).size} coin(s) ` +
      `(${rules.map((r) => `${r.coin} ${r.window} ${r.side} ≥ ${formatUsd(r.thresholdUsd)}`).join(", ")}), ` +
      `every ${Math.round(config.liqAlertIntervalMs / 1000)}s, re-arm below ${config.liqAlertRearmPct}%, ` +
      `webhook ${config.liqAlertWebhookUrl ? webhookFormat() : "off"}`,
  );

  return async () => {
    await run;
    log("alerts", "liquidation alerts stopped");
  };
}

function serializeAlert(id: string, a: FiredAlert): Record<string, unknown> {
  return {
    id,
    t: a.ts.toISOString(),
    tMs: a.ts.getTime(),
    coin: a.rule.coin,
    window: a.rule.window,
    side: a.rule.side,
    ntlUsd: a.value,
    thresholdUsd: a.rule.thresholdUsd,
    pctOfThreshold: (a.value / a.rule.thresholdUsd) * 100,
    events: a.events,
    fills: a.fills,
    longs: { ntlUsd: a.totals.longNtl, events: a.totals.longEvents, fills: a.totals.longFills },
    shorts: { ntlUsd: a.totals.shortNtl, events: a.totals.shortEvents, fills: a.totals.shortFills },
    totalNtlUsd: a.totals.longNtl + a.totals.shortNtl,
    message: a.message,
  };
}
