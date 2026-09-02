import { config } from "../config.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { hl, sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";
import { logBridgeError, pollBridge } from "./bridge.js";
import type { TradeTape } from "./tape.js";
import { refreshWalletState } from "./wallet-state.js";

// Whale discovery.
//
// 1. Bridge loop: poll Arbitrum for USDC deposits into the Hyperliquid bridge
//    (see bridge.ts). Any address whose deposits over the trailing
//    WHALE_WINDOW_HOURS sum to WHALE_MIN_USD or more becomes a whale_wallets row.
// 2. Watch loop: flagged wallets are polled on Hyperliquid (clearinghouseState,
//    weight 2) until they show an open position or WHALE_WATCH_HOURS elapse,
//    then slowly afterwards. The first poll also reads the wallet's ledger from
//    time zero (one userNonFundingLedgerUpdates call) — its first entry is the
//    account's age, which separates a brand-new whale from a known one topping up.
// 3. Tape hook: the trades WebSocket names both parties of every fill, so a
//    watched wallet's first fill is caught the moment it happens, at zero API
//    cost, and triggers an immediate state refresh.
//
// Budget: $1M+ depositors are a handful per day, so the watch loop's requests
// are negligible next to the positions bootstrapper.

const POSITIONED_RECHECK_MS = 900_000; // positioned wallets: refresh account value every 15 min
const WATCH_BATCH = 100;
const STATS_INTERVAL_MS = 300_000;

interface WatchRow {
  address: string;
  ledger_checked_at: Date | null;
  positioned_at: Date | null;
  first_trade_at: Date | null;
}

export function startWhaleTracker(isStopped: () => boolean, tape: TradeTape | null): () => Promise<void> {
  const watched = new Set<string>(); // addresses under watch (refreshed from DB each loop)
  const refreshNow = new Set<string>(); // wallets that just traded — check on the next tick
  const pendingFirstTrade = new Map<string, number>(); // address → earliest fill time seen
  let wake = false;
  let polls = 0;
  let depositsSeen = 0;
  let flagged = 0;
  let checks = 0;

  if (tape) {
    tape.onTrades((trades) => {
      if (watched.size === 0) return;
      for (const t of trades) {
        if (!Array.isArray(t.users)) continue;
        for (const u of t.users) {
          const a = u.toLowerCase();
          if (!watched.has(a)) continue;
          const prev = pendingFirstTrade.get(a);
          if (prev === undefined || t.time < prev) pendingFirstTrade.set(a, t.time);
          refreshNow.add(a);
          wake = true;
        }
      }
    });
  }

  // Flag any of the given addresses whose deposits in the window ending at their
  // latest deposit reach the threshold. A wallet re-funded after its watch expired
  // starts a fresh episode (position/trade markers reset); a top-up during an
  // active watch just extends it.
  async function flagCandidates(addresses: string[]): Promise<void> {
    if (addresses.length === 0) return;
    const { rows } = await pool.query<{ address: string; deposited_usd: number; inserted: boolean }>(
      `with latest as (
         select address, max(ts) as last_at from bridge_deposits where address = any($1::text[]) group by address
       ), agg as (
         select d.address, sum(d.usdc) as usd, min(d.ts) as first_at, max(d.ts) as last_at
         from bridge_deposits d join latest l on l.address = d.address
         where d.ts > l.last_at - make_interval(hours => $2)
         group by d.address having sum(d.usdc) >= $3
       )
       insert into whale_wallets (address, flagged_at, deposited_usd, first_deposit_at, last_deposit_at, watch_until)
       select address, now(), usd, first_at, last_at, last_at + make_interval(hours => $4) from agg
       on conflict (address) do update set
         deposited_usd    = case when whale_wallets.watch_until < now() then excluded.deposited_usd
                                 else greatest(whale_wallets.deposited_usd, excluded.deposited_usd) end,
         flagged_at       = case when whale_wallets.watch_until < now() then now() else whale_wallets.flagged_at end,
         first_deposit_at = case when whale_wallets.watch_until < now() then excluded.first_deposit_at else whale_wallets.first_deposit_at end,
         first_trade_at   = case when whale_wallets.watch_until < now() then null else whale_wallets.first_trade_at end,
         positioned_at    = case when whale_wallets.watch_until < now() then null else whale_wallets.positioned_at end,
         last_deposit_at  = excluded.last_deposit_at,
         watch_until      = excluded.watch_until,
         state_checked_at = null
       returning address, deposited_usd, (xmax = 0) as inserted`,
      [addresses, config.whaleWindowHours, config.whaleMinUsd, config.whaleWatchHours],
    );
    for (const r of rows) {
      watched.add(r.address);
      refreshNow.add(r.address);
      wake = true;
      if (r.inserted) {
        flagged++;
        const msg = `whale flagged: ${r.address} deposited $${Math.round(r.deposited_usd).toLocaleString("en-US")} within ${config.whaleWindowHours}h`;
        log("whales", msg);
        await opsEvent("whales", "info", msg);
      }
    }
  }

  async function bridgeLoop(): Promise<void> {
    while (!isStopped()) {
      const started = Date.now();
      try {
        const r = await pollBridge(isStopped);
        polls++;
        depositsSeen += r.deposits.length;
        if (r.deposits.length > 0) {
          await flagCandidates([...new Set(r.deposits.map((d) => d.address))]);
        }
      } catch (err) {
        logBridgeError("poll failed", err);
        await sleepStop(5_000);
      }
      await sleepUntil(started + config.bridgePollMs);
    }
  }

  async function loadWatched(): Promise<void> {
    const { rows } = await pool.query<{ address: string }>("select address from whale_wallets where watch_until > now()");
    watched.clear();
    for (const r of rows) watched.add(r.address);
  }

  async function flushFirstTrades(): Promise<void> {
    if (pendingFirstTrade.size === 0) return;
    const entries = [...pendingFirstTrade.entries()];
    pendingFirstTrade.clear();
    await pool.query(
      `update whale_wallets w set first_trade_at = least(coalesce(w.first_trade_at, u.t), u.t)
       from unnest($1::text[], $2::timestamptz[]) as u(address, t)
       where w.address = u.address`,
      [entries.map((e) => e[0]), entries.map((e) => new Date(e[1]))],
    );
  }

  async function checkWallet(w: WatchRow): Promise<void> {
    const state = await refreshWalletState(w.address);
    checks++;
    const hasPosition = state.positions.length > 0;
    let ledgerFirst: Date | null = null;
    let ledgerChecked = false;
    if (w.ledger_checked_at === null) {
      try {
        const ledger = await hl.userNonFundingLedgerUpdates(w.address, 0);
        ledgerChecked = true;
        if (ledger.length > 0) ledgerFirst = new Date(Math.min(...ledger.map((e) => e.time)));
      } catch (err) {
        logErr("whales", `ledger check failed for ${w.address}`, err);
      }
    }
    await pool.query(
      `update whale_wallets set
         account_value = $2, total_ntl_pos = $3, state_checked_at = now(),
         positioned_at = case when $4::boolean then coalesce(positioned_at, first_trade_at, now()) else positioned_at end,
         ledger_first_at = case when $5::boolean then $6 else ledger_first_at end,
         ledger_checked_at = case when $5::boolean then now() else ledger_checked_at end
       where address = $1`,
      [w.address, state.accountValue, state.totalNtlPos, hasPosition, ledgerChecked, ledgerFirst],
    );
    if (hasPosition && w.positioned_at === null) {
      const ntl = state.totalNtlPos ?? 0;
      const coins = state.positions.map((p) => `${p.szi > 0 ? "long" : "short"} ${p.coin}`).join(", ");
      const msg = `whale positioned: ${w.address} ${coins} ($${Math.round(ntl).toLocaleString("en-US")} notional, account $${Math.round(state.accountValue ?? 0).toLocaleString("en-US")})`;
      log("whales", msg);
      await opsEvent("whales", "info", msg);
    }
  }

  async function watchLoop(): Promise<void> {
    while (!isStopped()) {
      const deadline = Date.now() + config.whaleWatchPollMs;
      try {
        await flushFirstTrades();
        await loadWatched();
        const urgent = [...refreshNow];
        refreshNow.clear();
        wake = false; // anything flagged from here on wakes the next pass early
        const { rows } = await pool.query<WatchRow>(
          `select address, ledger_checked_at, positioned_at, first_trade_at from whale_wallets
           where address = any($1::text[])
              or (watch_until > now() and (
                    state_checked_at is null
                 or (positioned_at is null and state_checked_at < now() - make_interval(secs => $2))
                 or (positioned_at is not null and state_checked_at < now() - make_interval(secs => $3))))
           order by state_checked_at asc nulls first limit $4`,
          [urgent, Math.floor(config.whaleWatchPollMs / 1000) - 5, POSITIONED_RECHECK_MS / 1000, WATCH_BATCH],
        );
        for (const w of rows) {
          if (isStopped()) return;
          try {
            await checkWallet(w);
          } catch (err) {
            logErr("whales", `state check failed for ${w.address}`, err);
          }
          await sleepStop(config.bootstrapDelayMs);
        }
      } catch (err) {
        logErr("whales", "watch pass failed", err);
      }
      await sleepUntilOrWake(deadline);
    }
  }

  async function statsLoop(): Promise<void> {
    while (!isStopped()) {
      await sleepStop(STATS_INTERVAL_MS);
      if (isStopped()) break;
      log("whales", `${polls} bridge polls, ${depositsSeen} deposits recorded, ${flagged} wallets flagged, ${checks} state checks, watching ${watched.size}`);
    }
  }

  async function sleepStop(ms: number): Promise<void> {
    await sleepUntil(Date.now() + ms);
  }

  async function sleepUntil(deadline: number): Promise<void> {
    while (!isStopped() && Date.now() < deadline) {
      await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
    }
  }

  async function sleepUntilOrWake(deadline: number): Promise<void> {
    while (!isStopped() && !wake && Date.now() < deadline) {
      await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
    }
    if (wake) await sleep(2_000); // let HL index the fill before we read the state
  }

  const loops = [bridgeLoop(), watchLoop(), statsLoop()];
  log(
    "whales",
    `tracker started (bridge poll ${config.bridgePollMs}ms via ${new URL(config.arbitrumRpcUrl).host}, threshold $${config.whaleMinUsd.toLocaleString("en-US")} per ${config.whaleWindowHours}h, watch ${config.whaleWatchHours}h${tape ? ", tape hook on" : ""})`,
  );

  return async () => {
    await Promise.allSettled(loops);
    log("whales", "tracker stopped");
  };
}
