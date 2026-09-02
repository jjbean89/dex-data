import { config } from "../config.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { oiUsdAt, volumeContext, volumeMetrics, windowBuckets, type VolMetrics } from "../db/volume.js";
import { sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";
import { formatUsd, sendWebhook, webhookConfigured } from "./webhook.js";

// "Volume leading price" detector.
//
// Every minute, for every live coin, the last VOL_SIGNAL_BARS 5m bars are
// compared with the coin's own baseline (median 5m notional over the trailing
// 24h blended with the same time-of-day slots over the prior week). A buildup
// signal fires when all of these hold at once:
//
//   volume     window notional ≥ VOL_SIGNAL_RVOL × expected, and every bar in the
//              window ≥ VOL_SIGNAL_MIN_BAR_RVOL × its expectation (sustained, not
//              one print), with at least VOL_SIGNAL_MIN_BAR_USD per bar;
//   flat price |open→close move| ≤ VOL_SIGNAL_MAX_MOVE_ATR × the coin's typical
//              5m range — the volume hasn't been "spent" on price yet;
//   positioning open interest moved ≥ VOL_SIGNAL_MIN_OI_PCT over the window, or
//              taker flow is one-sided (≥ VOL_SIGNAL_MIN_IMBALANCE_PCT one way).
//              On a perp, accumulation shows up as OI growth; churn doesn't.
//
// One open signal per coin. It is confirmed when a subsequent bar (or the move
// since the signal) exceeds VOL_SIGNAL_BREAKOUT_ATR × range on abnormal volume,
// or expires after VOL_SIGNAL_EXPIRE_MIN. When more than VOL_SIGNAL_MAX_COINS
// coins trigger in the same pass it's a venue-wide event, recorded as such
// (market_wide) and notified once, not per coin.

const EVAL_INTERVAL_MS = 60_000;

interface OpenSignal {
  id: string;
  firedAtMs: number;
  atrPct: number;
  pxTo: number;
  tToMs: number;
}

export function startVolumeSignals(isStopped: () => boolean): () => Promise<void> {
  const open = new Map<string, OpenSignal>();
  let fired = 0;
  let confirmed = 0;

  async function init(): Promise<boolean> {
    try {
      const { rows } = await pool.query<{ id: string; coin: string; fired_at: Date; atr_pct: number; px_to: number; t_to: Date }>(
        "select id, coin, fired_at, atr_pct, px_to, t_to from vol_signals where status = 'open'",
      );
      for (const r of rows) open.set(r.coin, { id: r.id, firedAtMs: r.fired_at.getTime(), atrPct: r.atr_pct, pxTo: r.px_to, tToMs: r.t_to.getTime() });
      return true;
    } catch (err) {
      logErr("vol-signals", "init failed — retrying", err);
      return false;
    }
  }

  async function coinsToWatch(): Promise<string[] | null> {
    if (config.volSignalCoins.length > 0) return config.volSignalCoins;
    return null; // every coin with bars
  }

  function qualifies(m: VolMetrics): boolean {
    if (m.rvol === null || m.minBarRvol === null || m.moveAtr === null || m.atrPct === null || m.atrPct <= 0) return false;
    if (m.historyHours < config.volSignalMinHistoryHours || m.n24 < 12) return false;
    if (m.bars < config.volSignalBars) return false;
    if (m.rvol < config.volSignalRvol || m.minBarRvol < config.volSignalMinBarRvol) return false;
    if (m.avgBarUsd < config.volSignalMinBarUsd) return false;
    if (m.moveAtr > config.volSignalMaxMoveAtr) return false;
    const oiConfirms = m.oiChangePct !== null && Math.abs(m.oiChangePct) >= config.volSignalMinOiPct;
    const imb = config.volSignalMinImbalancePct;
    const flowConfirms = m.buySharePct >= imb || m.buySharePct <= 100 - imb;
    return oiConfirms || flowConfirms;
  }

  function biasOf(m: VolMetrics): "long" | "short" | "mixed" {
    if (m.buySharePct >= 55) return "long";
    if (m.buySharePct <= 45) return "short";
    if (m.oiChangePct !== null && Math.abs(m.oiChangePct) >= config.volSignalMinOiPct) return m.buySharePct > 50 ? "long" : "short";
    return "mixed";
  }

  function describe(m: VolMetrics, bias: string): string {
    const span = Math.round((m.tToMs + 300_000 - m.tFromMs) / 60_000);
    const parts = [
      `${m.rvol!.toFixed(1)}x volume over ${span}min (${formatUsd(m.volNtl)} vs ${formatUsd(m.expectedNtl ?? 0)} expected)`,
      `price ${m.moveP >= 0 ? "+" : ""}${m.moveP.toFixed(2)}% (typical 5m range ${m.atrPct!.toFixed(2)}%)`,
      m.oiChangePct === null ? "OI n/a" : `OI ${m.oiChangePct >= 0 ? "+" : ""}${m.oiChangePct.toFixed(2)}%`,
      `taker buys ${m.buySharePct.toFixed(0)}%`,
      ...(m.twapSharePct >= 20 ? [`TWAP ${m.twapSharePct.toFixed(0)}% of flow`] : []),
    ];
    return `${m.coin}: ${parts.join(", ")} → ${bias === "mixed" ? "positioning building, direction unclear" : `${bias} buildup`}`;
  }

  async function evaluate(): Promise<void> {
    const nowMs = Date.now();
    const coins = await coinsToWatch();
    const w = windowBuckets(config.volSignalBars, nowMs);
    const [ctx, oiStart, oiNow] = await Promise.all([
      volumeContext(coins, config.volSignalBars, nowMs),
      oiUsdAt(coins, w.startMs),
      oiUsdAt(coins, nowMs),
    ]);
    const candidates: Array<{ m: VolMetrics; bias: "long" | "short" | "mixed" }> = [];
    for (const [coin, c] of ctx) {
      const m = volumeMetrics(c, oiStart.get(coin) ?? null, oiNow.get(coin) ?? null, nowMs);
      if (!m) continue;
      const cur = open.get(coin);
      if (cur) {
        await followUp(coin, cur, m, c.bars, nowMs);
        continue;
      }
      if (qualifies(m)) candidates.push({ m, bias: biasOf(m) });
    }
    if (candidates.length === 0) return;
    const marketWide = candidates.length > config.volSignalMaxCoins;
    for (const cand of candidates) await fire(cand.m, cand.bias, marketWide, nowMs);
    if (marketWide) {
      const msg = `market-wide volume surge: ${candidates.length} coins on abnormal volume with flat price at once (${candidates
        .slice(0, 8)
        .map((c) => c.m.coin)
        .join(", ")}${candidates.length > 8 ? ", …" : ""}) — recorded, not alerted individually`;
      log("vol-signals", msg);
      await opsEvent("vol-signals", "info", msg);
      if (notifyOn()) void sendWebhook(`📊 ${msg}`, { type: "volume_market_wide", coins: candidates.map((c) => c.m.coin), t: new Date(nowMs).toISOString() });
    }
  }

  async function fire(m: VolMetrics, bias: "long" | "short" | "mixed", marketWide: boolean, nowMs: number): Promise<void> {
    const message = describe(m, bias);
    const { rows } = await pool.query<{ id: string }>(
      `insert into vol_signals (coin, fired_at, t_from, t_to, bars, vol_ntl, baseline_ntl, rvol, min_bar_rvol,
         px_from, px_to, px_move_pct, atr_pct, oi_from, oi_to, oi_change_pct, buy_share_pct, twap_share_pct,
         bias, market_wide, message, delivered)
       values ($1, $2, to_timestamp($3 / 1000.0), to_timestamp($4 / 1000.0), $5, $6, $7, $8, $9,
         $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
       returning id`,
      [
        m.coin,
        new Date(nowMs),
        m.tFromMs,
        m.tToMs,
        m.bars,
        m.volNtl,
        m.expectedNtl,
        m.rvol,
        m.minBarRvol,
        m.pxFrom,
        m.pxTo,
        m.moveP,
        m.atrPct,
        m.oiFrom,
        m.oiTo,
        m.oiChangePct,
        m.buySharePct,
        m.twapSharePct,
        bias,
        marketWide,
        message,
        notifyOn() && !marketWide ? false : null,
      ],
    );
    const id = rows[0]!.id;
    open.set(m.coin, { id, firedAtMs: nowMs, atrPct: m.atrPct!, pxTo: m.pxTo, tToMs: m.tToMs });
    fired++;
    log("vol-signals", `${marketWide ? "SIGNAL (market-wide)" : "SIGNAL"} ${message}`);
    if (notifyOn() && !marketWide) void deliver(id, "vol_signals", `📈 ${message}`, { type: "volume_buildup", signal: envelope(id, m, bias, message) });
  }

  // Open signal: keep its window stats current while the buildup persists, then
  // confirm on a breakout bar / cumulative move, or expire.
  async function followUp(coin: string, cur: OpenSignal, m: VolMetrics, bars: Array<{ tMs: number; o: number; c: number; ntl: number; fraction: number }>, nowMs: number): Promise<void> {
    const breakoutPct = config.volSignalBreakoutAtr * cur.atrPct;
    let breakout: { movePct: number; rvol: number | null } | null = null;
    for (const b of bars) {
      if (b.tMs <= cur.tToMs) continue; // bars that were part of the buildup window don't count
      const move = b.o > 0 ? ((b.c - b.o) / b.o) * 100 : 0;
      if (Math.abs(move) >= breakoutPct) {
        breakout = { movePct: move, rvol: m.baselineBar ? b.ntl / (m.baselineBar * b.fraction) : null };
        break;
      }
    }
    if (!breakout && cur.pxTo > 0) {
      const move = ((m.pxTo - cur.pxTo) / cur.pxTo) * 100;
      if (Math.abs(move) >= breakoutPct) breakout = { movePct: move, rvol: m.rvol };
    }
    if (breakout) {
      await pool.query(
        `update vol_signals set status = 'confirmed', confirmed_at = now(), closed_at = now(),
           breakout_move_pct = $2, breakout_rvol = $3, updated_at = now() where id = $1`,
        [cur.id, breakout.movePct, breakout.rvol],
      );
      open.delete(coin);
      confirmed++;
      const mins = Math.round((nowMs - cur.firedAtMs) / 60_000);
      const message = `${coin} breakout confirmed: ${breakout.movePct >= 0 ? "+" : ""}${breakout.movePct.toFixed(2)}%${breakout.rvol ? ` on ${breakout.rvol.toFixed(1)}x volume` : ""}, ${mins}min after the buildup signal`;
      log("vol-signals", `CONFIRMED ${message}`);
      if (notifyOn()) void sendWebhook(`✅ ${message}`, { type: "volume_breakout", signalId: cur.id, coin, movePct: breakout.movePct, rvol: breakout.rvol, minutesAfterSignal: mins, message });
      return;
    }
    if (nowMs - cur.firedAtMs > config.volSignalExpireMin * 60_000) {
      await pool.query("update vol_signals set status = 'expired', closed_at = now(), updated_at = now() where id = $1", [cur.id]);
      open.delete(coin);
      log("vol-signals", `expired: ${coin} signal #${cur.id} — no breakout within ${config.volSignalExpireMin}min`);
      return;
    }
    if (qualifies(m)) {
      // Buildup still running: extend the window so the row reflects the whole accumulation.
      await pool.query(
        `update vol_signals set t_to = to_timestamp($2 / 1000.0), vol_ntl = vol_ntl + greatest(0, $3 - vol_ntl), rvol = greatest(rvol, $4),
           px_to = $5, px_move_pct = ($5 - px_from) / nullif(px_from, 0) * 100, oi_to = coalesce($6, oi_to),
           oi_change_pct = case when oi_from > 0 and $6 is not null then ($6 - oi_from) / oi_from * 100 else oi_change_pct end,
           updated_at = now()
         where id = $1`,
        [cur.id, m.tToMs, m.volNtl, m.rvol, m.pxTo, m.oiTo],
      );
      cur.tToMs = Math.max(cur.tToMs, m.tToMs);
    }
  }

  function envelope(id: string, m: VolMetrics, bias: string, message: string): Record<string, unknown> {
    return {
      id,
      coin: m.coin,
      from: new Date(m.tFromMs).toISOString(),
      to: new Date(m.tToMs).toISOString(),
      bars: m.bars,
      volNtlUsd: m.volNtl,
      expectedNtlUsd: m.expectedNtl,
      rvol: m.rvol,
      minBarRvol: m.minBarRvol,
      pxFrom: m.pxFrom,
      pxTo: m.pxTo,
      pxMovePct: m.moveP,
      atrPct: m.atrPct,
      oiChangePct: m.oiChangePct,
      buySharePct: m.buySharePct,
      twapSharePct: m.twapSharePct,
      bias,
      message,
    };
  }

  function notifyOn(): boolean {
    return config.volSignalNotify && webhookConfigured();
  }

  async function deliver(id: string, table: string, text: string, envelopeBody: Record<string, unknown>): Promise<void> {
    const r = await sendWebhook(text, envelopeBody);
    if (r.ok) {
      await pool.query(`update ${table} set delivered = true, delivery_error = null where id = $1`, [id]).catch(() => undefined);
      return;
    }
    logErr("vol-signals", `webhook delivery failed for signal #${id}`, r.error);
    await opsEvent("vol-signals", "error", `webhook delivery failed for signal #${id}: ${r.error}`);
    await pool.query(`update ${table} set delivered = false, delivery_error = $2 where id = $1`, [id, r.error]).catch(() => undefined);
  }

  async function loop(): Promise<void> {
    while (!isStopped() && !(await init())) await sleepStop(15_000);
    let lastStats = Date.now();
    let first = true;
    while (!isStopped()) {
      if (!first) {
        // Align to just after each minute boundary so 5m closes are seen promptly.
        const next = (Math.floor(Date.now() / EVAL_INTERVAL_MS) + 1) * EVAL_INTERVAL_MS + 2_000;
        await sleepStop(next - Date.now());
        if (isStopped()) break;
      }
      first = false;
      try {
        await evaluate();
      } catch (err) {
        logErr("vol-signals", "evaluation failed", err);
      }
      if (Date.now() - lastStats >= 3_600_000) {
        log("vol-signals", `${fired} buildup signals fired, ${confirmed} confirmed this run, ${open.size} open`);
        lastStats = Date.now();
      }
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
    "vol-signals",
    `detector started: ${config.volSignalBars}×5m window, rvol ≥ ${config.volSignalRvol} (each bar ≥ ${config.volSignalMinBarRvol}), ` +
      `≥ ${formatUsd(config.volSignalMinBarUsd)}/bar, move ≤ ${config.volSignalMaxMoveAtr}×range, OI ≥ ${config.volSignalMinOiPct}% or flow ≥ ${config.volSignalMinImbalancePct}%, ` +
      `breakout ${config.volSignalBreakoutAtr}×range, coins ${config.volSignalCoins.length > 0 ? config.volSignalCoins.join("/") : "all"}, notify ${notifyOn() ? "on" : "off"}`,
  );
  return async () => {
    await run;
    log("vol-signals", "detector stopped");
  };
}
