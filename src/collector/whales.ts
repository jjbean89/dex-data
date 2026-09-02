import { config } from "../config.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { hl, sleep } from "../hl/client.js";
import type { UserFill } from "../hl/types.js";
import { log, logErr } from "../log.js";
import { logBridgeError, pollBridge } from "./bridge.js";
import type { TradeTape } from "./tape.js";
import { refreshWalletState, type WalletPosition, type WalletState } from "./wallet-state.js";
import { formatUsd, sendWebhook, webhookConfigured, webhookFormat } from "./webhook.js";

// Whale discovery.
//
// 1. Bridge loop: poll Arbitrum for USDC deposits into the Hyperliquid bridge
//    (see bridge.ts). Any address whose deposits over the trailing
//    WHALE_WINDOW_HOURS sum to WHALE_MIN_USD or more becomes a whale_wallets row.
// 2. Watch loop: flagged wallets are polled on Hyperliquid (clearinghouseState,
//    weight 2) until they open a large position or WHALE_WATCH_HOURS elapse,
//    then slowly afterwards. The first poll also reads the wallet's ledger from
//    time zero (one userNonFundingLedgerUpdates call) — its first entry is the
//    account's age, which separates a brand-new whale from a known one topping up
//    — and reconstructs what the wallet held BEFORE the deposit: current positions
//    minus the net of its fills since the first deposit (one userFillsByTime call).
//    "Opened a position" means a position that is not in that baseline — a new
//    coin, or a side flip — so a known account adding to thirty existing shorts
//    is not an event, but the same account opening a fresh one is. The alert
//    fires once per episode, when the newly opened positions reach
//    WHALE_POSITION_MIN_USD of notional.
// 3. Tape hook: the trades WebSocket names both parties of every fill, so a
//    watched wallet's first fill is caught the moment it happens, at zero API
//    cost, and triggers an immediate state refresh.
//
// Budget: $1M+ depositors are a handful per day, so the watch loop's requests
// are negligible next to the positions bootstrapper.

const ALERTED_RECHECK_MS = 900_000; // after the position alert: refresh account value every 15 min
const FILL_CAP = 2000; // userFillsByTime returns at most this many fills
const SZ_EPS = 1e-6; // float residue from summing fill sizes
const WATCH_BATCH = 100;
const STATS_INTERVAL_MS = 300_000;

interface WatchRow {
  address: string;
  deposited_usd: number;
  first_deposit_at: Date;
  last_deposit_at: Date;
  ledger_checked_at: Date | null;
  ledger_first_at: Date | null;
  positioned_at: Date | null;
  first_trade_at: Date | null;
  funded_alerted_at: Date | null;
  position_alerted_at: Date | null;
  baseline_positions: BaselinePosition[] | null;
}

// A position the wallet held as of the episode's first deposit.
interface BaselinePosition {
  coin: string;
  szi: number;
}

function notional(p: WalletPosition): number {
  if (p.positionValue !== null) return Math.abs(p.positionValue);
  return p.entryPx !== null ? Math.abs(p.szi) * p.entryPx : 0;
}

// Positions opened after funding: a coin the wallet did not hold at its first
// deposit, or one whose side has flipped since. Adding to an existing position
// is not an opening.
export function openedSince(baseline: BaselinePosition[], positions: WalletPosition[]): WalletPosition[] {
  const before = new Map(baseline.map((b) => [b.coin, b.szi]));
  return positions.filter((p) => {
    const prev = before.get(p.coin);
    return prev === undefined || Math.abs(prev) < SZ_EPS || Math.sign(prev) !== Math.sign(p.szi);
  });
}

// Reconstructs the wallet's perp positions at the start of `fills` from its
// current positions: unwind each fill (a buy added sz, a sell removed it). Spot
// fills ride the same feed ("PURR/USDC", "@107") and are skipped.
export function rewindPositions(positions: WalletPosition[], fills: UserFill[]): BaselinePosition[] {
  const szi = new Map(positions.map((p) => [p.coin, p.szi]));
  for (const f of fills) {
    if (f.coin.includes("/") || f.coin.startsWith("@")) continue;
    const sz = parseFloat(f.sz);
    if (!Number.isFinite(sz)) continue;
    szi.set(f.coin, (szi.get(f.coin) ?? 0) - (f.side === "B" ? sz : -sz));
  }
  const out: BaselinePosition[] = [];
  for (const [coin, v] of szi) if (Math.abs(v) >= SZ_EPS) out.push({ coin, szi: v });
  return out;
}

type WhaleAlertKind = "funded" | "positioned";

const EXPLORER_URL = "https://app.hyperliquid.xyz/explorer/address/";

function shortAddr(a: string): string {
  return `${a.slice(0, 6)}…${a.slice(-4)}`;
}

function ago(d: Date): string {
  const s = Math.max(0, Math.round((Date.now() - d.getTime()) / 1000));
  if (s < 90) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 90) return `${m} min ago`;
  const h = Math.round(m / 60);
  if (h < 36) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

// "New" = the wallet's first-ever Hyperliquid ledger entry is within
// WHALE_FUNDED_MAX_AGE_HOURS; an empty ledger (deposit not yet indexed) counts as new.
function accountAge(ledgerFirst: Date | null, checked: boolean): { isNew: boolean | null; label: string } {
  if (!checked) return { isNew: null, label: "account age unknown" };
  if (!ledgerFirst) return { isNew: true, label: "brand-new account" };
  const ageMs = Date.now() - ledgerFirst.getTime();
  const isNew = ageMs <= config.whaleFundedMaxAgeHours * 3_600_000;
  const days = ageMs / 86_400_000;
  if (isNew) return { isNew, label: `brand-new account (first activity ${ago(ledgerFirst)})` };
  if (days < 30) return { isNew, label: `account ${Math.round(days)}d old` };
  return { isNew, label: `account ${Math.round(days / 30)}mo old` };
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
  let alerts = 0;

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
         funded_alerted_at   = case when whale_wallets.watch_until < now() then null else whale_wallets.funded_alerted_at end,
         position_alerted_at = case when whale_wallets.watch_until < now() then null else whale_wallets.position_alerted_at end,
         baseline_positions  = case when whale_wallets.watch_until < now() then null else whale_wallets.baseline_positions end,
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

  const webhookOn = webhookConfigured(); // LIQ_ALERT_WEBHOOK_URL, shared with the other alerts
  const alertKinds = new Set(config.whaleAlertEvents as WhaleAlertKind[]);

  // Records the alert, then pushes it to the webhook off the watch path; the row
  // keeps the delivery outcome so /v1/whales/alerts shows what actually went out.
  async function fireAlert(
    kind: WhaleAlertKind,
    w: WatchRow,
    state: WalletState,
    isNew: boolean | null,
    ledgerFirst: Date | null,
    opened: WalletPosition[],
    message: string,
  ): Promise<void> {
    const positions = state.positions.map((p) => ({ coin: p.coin, side: p.szi > 0 ? "long" : "short", sz: Math.abs(p.szi), entryPx: p.entryPx }));
    const openedRows = opened.map((p) => ({ coin: p.coin, side: p.szi > 0 ? "long" : "short", sz: Math.abs(p.szi), entryPx: p.entryPx, ntlUsd: notional(p) }));
    const openedNtl = opened.reduce((acc, p) => acc + notional(p), 0);
    const { rows } = await pool.query<{ id: string; ts: Date }>(
      `insert into whale_alerts (kind, address, deposited_usd, account_value, total_ntl_pos, is_new_account, ledger_first_at, positions, opened, message, delivered)
       values ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11) returning id, ts`,
      [kind, w.address, w.deposited_usd, state.accountValue, state.totalNtlPos, isNew, ledgerFirst, JSON.stringify(positions), JSON.stringify(openedRows), message, webhookOn ? false : null],
    );
    alerts++;
    log("whales", `ALERT ${message}`);
    await opsEvent("whales", "info", `alert: ${message}`);
    const row = rows[0];
    if (!webhookOn || !row) return;
    const envelope = {
      type: `whale_${kind}`,
      alert: {
        id: row.id,
        t: row.ts.toISOString(),
        kind,
        address: w.address,
        depositedUsd: w.deposited_usd,
        firstDepositAt: w.first_deposit_at.toISOString(),
        lastDepositAt: w.last_deposit_at.toISOString(),
        accountValueUsd: state.accountValue,
        totalNtlPosUsd: state.totalNtlPos,
        isNewAccount: isNew,
        ledgerFirstAt: ledgerFirst ? ledgerFirst.toISOString() : null,
        positions,
        opened: openedRows,
        openedNtlUsd: openedNtl,
        explorer: `${EXPLORER_URL}${w.address}`,
        message,
      },
    };
    void sendWebhook(`🐋 ${message}`, envelope).then(async (r) => {
      if (r.ok) {
        await pool.query("update whale_alerts set delivered = true, delivery_error = null where id = $1", [row.id]).catch(() => undefined);
        return;
      }
      logErr("whales", `webhook delivery failed for alert #${row.id}`, r.error);
      await opsEvent("whales", "error", `webhook delivery failed for alert #${row.id}: ${r.error}`);
      await pool.query("update whale_alerts set delivered = false, delivery_error = $2 where id = $1", [row.id, r.error]).catch(() => undefined);
    });
  }

  function fundedMessage(w: WatchRow, state: WalletState, ageLabel: string): string {
    const n = w.last_deposit_at.getTime() === w.first_deposit_at.getTime() ? "" : " in several deposits";
    const acct = state.accountValue !== null ? `, perps account ${formatUsd(state.accountValue)}` : "";
    return (
      `New whale funded: ${shortAddr(w.address)} bridged ${formatUsd(w.deposited_usd)} into Hyperliquid${n} ` +
      `(latest ${ago(w.last_deposit_at)}) — ${ageLabel}${acct}. ${EXPLORER_URL}${w.address}`
    );
  }

  // Leads with what was newly opened; the wallet's overall book is context.
  function positionedMessage(w: WatchRow, state: WalletState, opened: WalletPosition[], heldBefore: number, ageLabel: string): string {
    const parts = opened
      .slice()
      .sort((a, b) => notional(b) - notional(a))
      .slice(0, 4)
      .map((p) => {
        const ntl = notional(p) > 0 ? ` ${formatUsd(notional(p))}` : "";
        const px = p.entryPx !== null ? ` @ ${p.entryPx}` : "";
        return `${p.szi > 0 ? "long" : "short"} ${p.coin}${ntl}${px}`;
      });
    const more = opened.length > 4 ? ` (+${opened.length - 4} more)` : "";
    const openedNtl = opened.reduce((acc, p) => acc + notional(p), 0);
    const held = heldBefore > 0 ? ` on top of ${heldBefore} position${heldBefore === 1 ? "" : "s"} held before funding` : "";
    const total = state.totalNtlPos !== null ? `, ${formatUsd(state.totalNtlPos)} total notional` : "";
    const acct = state.accountValue !== null ? `, account ${formatUsd(state.accountValue)}` : "";
    return (
      `Whale opened a new position: ${shortAddr(w.address)} ${parts.join(", ")}${more} — ${formatUsd(openedNtl)} newly opened${held}${total}${acct}. ` +
      `Bridged ${formatUsd(w.deposited_usd)} ${ago(w.last_deposit_at)}, ${ageLabel}. ${EXPLORER_URL}${w.address}`
    );
  }

  // What the wallet held at its first deposit of this episode: the current
  // positions with every fill since then unwound. A wallet too busy for one
  // fills page (2000+ fills since the deposit) is a market maker, not a whale
  // taking a view — its current book stands in as the baseline, so nothing it
  // already trades can alert; a new coin still can.
  async function baselineFor(w: WatchRow, state: WalletState): Promise<BaselinePosition[]> {
    const fills = await hl.userFillsByTime(w.address, w.first_deposit_at.getTime(), state.time);
    if (fills.length >= FILL_CAP) {
      const msg = `fills since deposit hit the ${FILL_CAP}-fill cap for ${w.address} — using its current positions as the pre-funding baseline`;
      log("whales", msg);
      await opsEvent("whales", "warn", msg);
      return state.positions.map((p) => ({ coin: p.coin, szi: p.szi }));
    }
    return rewindPositions(state.positions, fills);
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
    let baseline = w.baseline_positions;
    let baselineTaken = false;
    if (baseline === null) {
      try {
        baseline = await baselineFor(w, state);
        baselineTaken = true;
      } catch (err) {
        logErr("whales", `fills check failed for ${w.address} — pre-funding baseline deferred to the next check`, err);
      }
    }
    const opened = baseline ? openedSince(baseline, state.positions) : [];
    const openedNtl = opened.reduce((acc, p) => acc + notional(p), 0);
    // For a wallet that held nothing before funding, its first fill on the tape IS the opening.
    const openedAt = opened.length > 0 ? (baseline?.length === 0 && w.first_trade_at ? w.first_trade_at : new Date()) : null;
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
         positioned_at = coalesce(positioned_at, $4::timestamptz),
         ledger_first_at = case when $5::boolean then $6 else ledger_first_at end,
         ledger_checked_at = case when $5::boolean then now() else ledger_checked_at end,
         baseline_positions = case when $7::boolean then $8::jsonb else baseline_positions end
       where address = $1`,
      [w.address, state.accountValue, state.totalNtlPos, openedAt, ledgerChecked, ledgerFirst, baselineTaken, baselineTaken ? JSON.stringify(baseline) : null],
    );
    if (baselineTaken && baseline) {
      log("whales", `${w.address} held ${baseline.length} position${baseline.length === 1 ? "" : "s"} at its first deposit${opened.length > 0 ? `, ${opened.length} opened since` : ""}`);
    }
    const ledgerKnown = ledgerChecked || w.ledger_checked_at !== null;
    const ledgerFirstAt = ledgerChecked ? ledgerFirst : w.ledger_first_at;
    const age = accountAge(ledgerFirstAt, ledgerKnown);
    // The funded alert is for fresh wallets only, so it waits for the ledger check
    // (a failed check leaves the marker unset and the next pass retries) and is
    // skipped — once, silently to the webhook — for accounts older than the cutoff.
    if (w.funded_alerted_at === null && ledgerKnown) {
      if (age.isNew) {
        if (alertKinds.has("funded")) await fireAlert("funded", w, state, age.isNew, ledgerFirstAt, [], fundedMessage(w, state, age.label));
      } else {
        log("whales", `funded alert skipped for ${w.address}: ${age.label} (cutoff ${config.whaleFundedMaxAgeHours}h)`);
      }
      await pool.query("update whale_wallets set funded_alerted_at = now() where address = $1", [w.address]);
    }
    if (baseline && opened.length > 0 && openedNtl >= config.whalePositionMinUsd && w.position_alerted_at === null) {
      if (alertKinds.has("positioned")) {
        await fireAlert("positioned", w, state, age.isNew, ledgerFirstAt, opened, positionedMessage(w, state, opened, baseline.length, age.label));
      }
      await pool.query("update whale_wallets set position_alerted_at = now() where address = $1", [w.address]);
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
          `select address, deposited_usd, first_deposit_at, last_deposit_at, ledger_checked_at, ledger_first_at,
                  positioned_at, first_trade_at, funded_alerted_at, position_alerted_at, baseline_positions
           from whale_wallets
           where address = any($1::text[])
              or (watch_until > now() and (
                    state_checked_at is null
                 or (position_alerted_at is null and state_checked_at < now() - make_interval(secs => $2))
                 or (position_alerted_at is not null and state_checked_at < now() - make_interval(secs => $3))))
           order by state_checked_at asc nulls first limit $4`,
          [urgent, Math.floor(config.whaleWatchPollMs / 1000) - 5, ALERTED_RECHECK_MS / 1000, WATCH_BATCH],
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
      log("whales", `${polls} bridge polls, ${depositsSeen} deposits recorded, ${flagged} wallets flagged, ${checks} state checks, ${alerts} alerts, watching ${watched.size}`);
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
    `tracker started (bridge poll ${config.bridgePollMs}ms via ${new URL(config.arbitrumRpcUrl).host}, threshold $${config.whaleMinUsd.toLocaleString("en-US")} per ${config.whaleWindowHours}h, watch ${config.whaleWatchHours}h, position alert ≥ $${config.whalePositionMinUsd.toLocaleString("en-US")}${tape ? ", tape hook on" : ""}, alerts ${[...alertKinds].join("+") || "off"} → ${webhookOn ? webhookFormat() : "log only"})`,
  );

  return async () => {
    await Promise.allSettled(loops);
    log("whales", "tracker stopped");
  };
}
