import { config } from "../config.js";
import type { WhaleCoinBreakdown, WhaleStateAfter } from "../db/liq-whales.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { pxAt } from "../db/px.js";
import { sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";
import { refreshWalletState } from "./wallet-state.js";
import { formatUsd, sendWebhook, webhookConfigured } from "./webhook.js";

// Large liquidated accounts.
//
// Collects every wallet whose liquidation burst crosses LIQ_WHALE_THRESHOLD. A
// burst ("episode") is the run of one wallet's liquidation fills — across every
// coin — with no gap longer than LIQ_WHALE_WINDOW: a position that gets taken
// down in several partial liquidations over a few minutes is one episode, and
// so is a wallet liquidated on BTC and ETH in the same cascade.
//
// The tracker keys on when fills were *recorded* (liq_fills.recorded_at), not
// when they traded: the recorder classifies fills with a lag and backfills
// after downtime, and a $10M liquidation discovered an hour late still has to
// be collected. Each tick it re-examines every wallet with newly recorded fills
// plus every open episode, so late fills also grow an episode that's still
// open. Episodes close once the window has passed since their last fill; a
// closed episode is never reopened — later fills start a new one.
//
// Idempotent across restarts: an episode is only created if no existing row for
// that wallet already reaches into the candidate burst.
//
// Each episode also carries context for the narrative ("whale 0x… had its entire
// $6M BTC long liquidated as BTC moved −3.2%"): the coin's price 1h before the
// burst, at its first and latest fill (from our own ticks), and what the wallet
// still holds afterwards (one clearinghouseState fetch — weight 2 — on detection
// and again when the episode closes).

interface Episode {
  id: string;
  fromMs: number;
}

interface EpisodeStats {
  fromMs: number;
  toMs: number;
  ntl: number;
  events: number;
  fills: number;
  coins: WhaleCoinBreakdown[];
  stateAfter: WhaleStateAfter | null;
}

export function fmtPct(pct: number): string {
  return `${pct >= 0 ? "+" : "−"}${Math.abs(pct).toFixed(2)}%`;
}

function pctChange(now: number | null | undefined, then: number | null | undefined): number | null {
  if (now == null || then == null || then === 0) return null;
  return ((now - then) / Math.abs(then)) * 100;
}

export function explorerUrl(wallet: string): string {
  return `https://app.hyperliquid.xyz/explorer/address/${wallet}`;
}

// Wire shape of one coin of a whale episode, shared by the webhook and the API.
export function serializeWhaleCoin(c: WhaleCoinBreakdown): Record<string, unknown> {
  const pxBefore = c.pxBefore ?? null;
  const pxStart = c.pxStart ?? null;
  const pxEnd = c.pxEnd ?? null;
  return {
    coin: c.coin,
    side: c.side,
    ntlUsd: c.ntl,
    events: c.events,
    fills: c.fills,
    px: {
      before1h: pxBefore,
      atStart: pxStart,
      atEnd: pxEnd,
      movePct: pctChange(pxEnd, pxBefore), // 1h before the burst → latest fill
      burstMovePct: pctChange(pxEnd, pxStart), // first fill → latest fill
    },
    remainingSz: c.remainingSz ?? null,
    remainingNtlUsd: c.remainingNtlUsd ?? null,
    fullyLiquidated: c.fullyLiquidated ?? null,
  };
}

export function shortAddr(wallet: string): string {
  return `${wallet.slice(0, 6)}…${wallet.slice(-4)}`;
}

// The one-liner for a whale liquidation, from the largest coin in the episode:
//   "Whale 0x1234…abcd's entire $6.00M BTC long was liquidated as BTC moved −3.21% into the liquidation"
//   "Whale 0x1234…abcd had $6.00M of its BTC long liquidated ($2.10M still open) as BTC moved −3.21% …"
// Price move = 1h before the burst → its latest fill; "entire" only when the wallet
// was checked and holds nothing on that side any more. Other coins are counted.
export function whaleHeadline(wallet: string, coins: WhaleCoinBreakdown[]): string {
  const top = coins[0];
  if (!top) return `Whale ${shortAddr(wallet)} liquidated`;
  const side = top.side;
  const who = `Whale ${shortAddr(wallet)}`;
  let text: string;
  if (top.fullyLiquidated === true) text = `${who}'s entire ${formatUsd(top.ntl)} ${top.coin} ${side} was liquidated`;
  else if (top.fullyLiquidated === false && top.remainingNtlUsd != null) {
    text = `${who} had ${formatUsd(top.ntl)} of its ${top.coin} ${side} liquidated (${formatUsd(top.remainingNtlUsd)} still open)`;
  } else text = `${who} had ${formatUsd(top.ntl)} of ${top.coin} ${side}s liquidated`;
  const move = pctChange(top.pxEnd, top.pxBefore);
  if (move !== null) text += ` as ${top.coin} moved ${fmtPct(move)} into the liquidation`;
  if (coins.length > 1) {
    const rest = coins.slice(1).reduce((a, c) => a + c.ntl, 0);
    text += `, plus ${formatUsd(rest)} across ${coins.length - 1} other coin${coins.length === 2 ? "" : "s"}`;
  }
  return text;
}

export function startLiqWhaleTracker(isStopped: () => boolean): () => Promise<void> {
  const threshold = config.liqWhaleThresholdUsd;
  const windowMs = config.liqWhaleWindow.ms;
  const open = new Map<string, Episode>(); // wallet → open episode
  let watermark = new Date(0);
  let detected = 0;

  async function init(): Promise<boolean> {
    try {
      const { rows } = await pool.query<{ id: string; wallet: string; from_ts: Date }>(
        "select id, wallet, from_ts from liq_whales where active = true",
      );
      for (const r of rows) open.set(r.wallet, { id: r.id, fromMs: r.from_ts.getTime() });
      // Re-examine everything recorded over the backfill horizon: creation is
      // idempotent, so a generous first pass only costs a few queries.
      watermark = new Date(Date.now() - Math.max(config.liqBackfillHours * 3_600_000, windowMs));
      return true;
    } catch (err) {
      logErr("whales", "init failed — retrying", err);
      return false;
    }
  }

  // Aggregate one wallet's fills in [fromMs, ∞) by coin and side.
  async function stats(wallet: string, fromMs: number): Promise<EpisodeStats | null> {
    const { rows } = await pool.query<{
      coin: string;
      side: "long" | "short";
      ntl: number;
      events: number;
      fills: number;
      min_ts: Date;
      max_ts: Date;
    }>(
      `select coin, side, sum(ntl) as ntl, count(distinct ts)::int as events, count(*)::int as fills,
              min(ts) as min_ts, max(ts) as max_ts
       from liq_fills where wallet = $1 and ts >= to_timestamp($2 / 1000.0)
       group by coin, side order by ntl desc`,
      [wallet, fromMs],
    );
    if (rows.length === 0) return null;
    const out: EpisodeStats = { fromMs: Infinity, toMs: 0, ntl: 0, events: 0, fills: 0, coins: [], stateAfter: null };
    for (const r of rows) {
      out.ntl += r.ntl;
      out.events += r.events;
      out.events += r.events;
      out.fills += r.fills;
      out.fromMs = Math.min(out.fromMs, r.min_ts.getTime());
      out.toMs = Math.max(out.toMs, r.max_ts.getTime());
      out.coins.push({ coin: r.coin, side: r.side, ntl: r.ntl, events: r.events, fills: r.fills });
    }
    return out;
  }

  // Price context per coin from our own ticks/candles (cheap, local). Silently null
  // where no price is recorded (e.g. the collector was down over the burst).
  async function addPriceContext(s: EpisodeStats): Promise<void> {
    await Promise.all(
      s.coins.map(async (c) => {
        const [before, start, end] = await Promise.all([
          pxAt(c.coin, s.fromMs - 3_600_000, 600_000),
          pxAt(c.coin, s.fromMs, 120_000),
          pxAt(c.coin, s.toMs, 120_000),
        ]);
        c.pxBefore = before;
        c.pxStart = start;
        c.pxEnd = end;
      }),
    );
  }

  // What the wallet still holds after the burst: one clearinghouseState fetch.
  // Kept on failure as null (never blocks the record — the notional is the point).
  async function addRemaining(wallet: string, s: EpisodeStats, previous: WhaleStateAfter | null): Promise<void> {
    let state = previous;
    try {
      const w = await refreshWalletState(wallet);
      state = {
        checkedAt: new Date().toISOString(),
        accountValue: w.accountValue,
        totalNtlPos: w.totalNtlPos,
        positions: w.positions.map((p) => ({ coin: p.coin, szi: p.szi, entryPx: p.entryPx, positionValue: p.positionValue })),
      };
    } catch (err) {
      logErr("whales", `clearinghouseState failed for ${wallet} — remaining position unknown`, err);
    }
    s.stateAfter = state;
    await applyRemaining(s, state);
  }

  async function applyRemaining(s: EpisodeStats, state: WhaleStateAfter | null): Promise<void> {
    if (!state) return;
    const held = new Map(state.positions.map((p) => [p.coin, p]));
    for (const c of s.coins) {
      const p = held.get(c.coin);
      // Still on the liquidated side = partial liquidation; flat, or flipped, = fully taken down.
      const sameSide = p ? (p.szi > 0 ? "long" : "short") === c.side : false;
      c.remainingSz = sameSide && p ? p.szi : 0;
      c.remainingNtlUsd = sameSide && p ? p.positionValue : 0;
      c.fullyLiquidated = !sameSide;
    }
  }

  // Start of the burst that ends at the wallet's latest fill: walk back through
  // fills while consecutive gaps stay within the window.
  async function burstStart(wallet: string): Promise<number | null> {
    const { rows } = await pool.query<{ ts: Date }>(
      `select distinct ts from liq_fills where wallet = $1 and ts >= now() - make_interval(days => $2)
       order by ts desc limit 500`,
      [wallet, config.liqRetentionDays],
    );
    if (rows.length === 0) return null;
    let start = rows[0]!.ts.getTime();
    for (let i = 1; i < rows.length; i++) {
      const t = rows[i]!.ts.getTime();
      if (start - t > windowMs) break;
      start = t;
    }
    return start;
  }

  async function evaluate(): Promise<void> {
    const tickStart = new Date();
    const { rows } = await pool.query<{ wallet: string }>(
      "select distinct wallet from liq_fills where recorded_at > $1",
      [watermark],
    );
    const wallets = new Set<string>(rows.map((r) => r.wallet));
    for (const w of open.keys()) wallets.add(w);
    for (const wallet of wallets) {
      if (isStopped()) return;
      const ep = open.get(wallet);
      if (ep) {
        await refresh(wallet, ep);
      } else {
        await consider(wallet);
      }
    }
    watermark = tickStart;
  }

  // Wallet without an open episode: does its latest burst cross the threshold?
  async function consider(wallet: string): Promise<void> {
    const startMs = await burstStart(wallet);
    if (startMs === null) return;
    const s = await stats(wallet, startMs);
    if (!s || s.ntl < threshold) return;
    // Already collected (restart / re-examination)? Any row that reaches into this burst covers it.
    const { rows: existing } = await pool.query<{ id: string; active: boolean }>(
      "select id, active from liq_whales where wallet = $1 and to_ts >= to_timestamp($2 / 1000.0) order by to_ts desc limit 1",
      [wallet, startMs],
    );
    if (existing[0]) {
      if (existing[0].active) open.set(wallet, { id: existing[0].id, fromMs: startMs });
      return;
    }
    const active = Date.now() - s.toMs <= windowMs;
    await addPriceContext(s);
    await addRemaining(wallet, s, null);
    const { rows } = await pool.query<{ id: string }>(
      `insert into liq_whales (wallet, from_ts, to_ts, ntl, events, fills, coins, active, threshold_usd, delivered, state_after, state_checked_at)
       values ($1, to_timestamp($2 / 1000.0), to_timestamp($3 / 1000.0), $4, $5, $6, $7::jsonb, $8, $9, $10, $11::jsonb, $12)
       returning id`,
      [
        wallet, s.fromMs, s.toMs, s.ntl, s.events, s.fills, JSON.stringify(s.coins), active, threshold, notifyOn() ? false : null,
        s.stateAfter ? JSON.stringify(s.stateAfter) : null, s.stateAfter ? s.stateAfter.checkedAt : null,
      ],
    );
    const id = rows[0]!.id;
    if (active) open.set(wallet, { id, fromMs: s.fromMs });
    detected++;
    const message = describe(wallet, s);
    log("whales", `WHALE ${message}`);
    if (notifyOn()) void deliver(id, wallet, s, message);
  }

  // Open episode: fold in new fills, close it once the window has passed.
  async function refresh(wallet: string, ep: Episode): Promise<void> {
    const s = await stats(wallet, ep.fromMs);
    if (!s) {
      // Fills pruned or removed from under the episode — nothing to update; close it.
      await pool.query("update liq_whales set active = false, updated_at = now() where id = $1", [ep.id]);
      open.delete(wallet);
      return;
    }
    const active = Date.now() - s.toMs <= windowMs;
    await addPriceContext(s);
    const { rows: prev } = await pool.query<{ state_after: WhaleStateAfter | null }>(
      "select state_after from liq_whales where id = $1",
      [ep.id],
    );
    const previous = prev[0]?.state_after ?? null;
    // Re-check the wallet when the episode closes (the final answer) or when new
    // fills landed since the last check; otherwise carry the previous snapshot.
    const newFills = previous ? s.toMs > Date.parse(previous.checkedAt) : true;
    if (!active || newFills) await addRemaining(wallet, s, previous);
    else {
      s.stateAfter = previous;
      await applyRemaining(s, previous);
    }
    await pool.query(
      `update liq_whales set to_ts = to_timestamp($2 / 1000.0), ntl = $3, events = $4, fills = $5, coins = $6::jsonb,
         active = $7, state_after = $8::jsonb, state_checked_at = $9, updated_at = now()
       where id = $1`,
      [
        ep.id, s.toMs, s.ntl, s.events, s.fills, JSON.stringify(s.coins), active,
        s.stateAfter ? JSON.stringify(s.stateAfter) : null, s.stateAfter ? s.stateAfter.checkedAt : null,
      ],
    );
    if (!active) {
      open.delete(wallet);
      log("whales", `episode closed: ${wallet} ${formatUsd(s.ntl)} over ${Math.round((s.toMs - s.fromMs) / 60_000)}min`);
    }
  }

  function notifyOn(): boolean {
    return config.liqWhaleNotify && webhookConfigured();
  }

  function describe(wallet: string, s: EpisodeStats): string {
    const parts = s.coins.map((c) => `${c.coin} ${c.side === "long" ? "longs" : "shorts"} ${formatUsd(c.ntl)}`);
    const span = Math.max(1, Math.round((s.toMs - s.fromMs) / 60_000));
    return (
      `${whaleHeadline(wallet, s.coins)} (${parts.join(", ")}; ${s.events} forced order${s.events === 1 ? "" : "s"}` +
      `${s.events > 1 ? ` over ${span}min` : ""}) — ${explorerUrl(wallet)}`
    );
  }

  async function deliver(id: string, wallet: string, s: EpisodeStats, message: string): Promise<void> {
    const r = await sendWebhook(`🐋 ${message}`, {
      type: "whale_liquidation",
      whale: {
        id,
        wallet,
        explorer: explorerUrl(wallet),
        from: new Date(s.fromMs).toISOString(),
        to: new Date(s.toMs).toISOString(),
        ntlUsd: s.ntl,
        thresholdUsd: threshold,
        events: s.events,
        fills: s.fills,
        coins: s.coins.map(serializeWhaleCoin),
        stateAfter: s.stateAfter,
        headline: whaleHeadline(wallet, s.coins),
        message,
      },
    });
    if (r.ok) {
      await pool.query("update liq_whales set delivered = true, delivery_error = null where id = $1", [id]).catch(() => undefined);
      return;
    }
    logErr("whales", `webhook delivery failed for whale #${id}`, r.error);
    await opsEvent("whales", "error", `webhook delivery failed for whale #${id}: ${r.error}`);
    await pool
      .query("update liq_whales set delivered = false, delivery_error = $2 where id = $1", [id, r.error])
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
      } catch (err) {
        logErr("whales", "evaluation failed", err);
      }
      if (Date.now() - lastStats >= 3_600_000) {
        log("whales", `${detected} whale liquidations collected this run, ${open.size} episode(s) open`);
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
    "whales",
    `whale tracker started: wallets liquidated ≥ ${formatUsd(threshold)} within a ${config.liqWhaleWindow.name} burst, ` +
      `every ${Math.round(config.liqAlertIntervalMs / 1000)}s, notify ${notifyOn() ? "on" : "off"}`,
  );

  return async () => {
    await run;
    log("whales", "whale tracker stopped");
  };
}
