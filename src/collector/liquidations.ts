import { config } from "../config.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { hl, sleep } from "../hl/client.js";
import type { UserFill } from "../hl/types.js";
import { log, logErr } from "../log.js";
import type { TradeTape } from "./tape.js";

// Liquidation recording.
//
// Hyperliquid has no public liquidation feed: forced closes print on the trades
// WebSocket looking exactly like normal trades. But userFillsByTime marks
// liquidation fills on BOTH parties of the print (liquidation.liquidatedUser names
// the forced wallet) — so verifying EITHER side of a trade classifies it
// definitively, and one call classifies every trade that wallet took part in.
//
// The recorder queues both counterparties of every tape trade and spends a paced
// userFillsByTime budget on the wallet covering the most unclassified trades.
// Active market makers sit on one side of most flow (top ~30 wallets ≈ 2/3 of all
// trades, measured), and a liquidated wallet's forced order usually splinters into
// several prints, pushing the victim itself up the queue — so coverage concentrates
// exactly where liquidations are. Trades still unclassified after PENDING_MAX_AGE_MS
// are dropped and counted (the stats line reports live coverage).
//
// Verified fills land in liq_fills keyed by HL's trade id; per-coin 5m/1h candle
// buckets are recomputed from liq_fills in the same transaction, so late
// verification and re-discovery stay idempotent. Fills of one forced order share
// the same (wallet, ts) — events are counted as distinct (wallet, ts) pairs.
//
// On boot (and after WS gaps) recently-active wallets from the traders table are
// swept over the downtime window: their fills replay any liquidations that printed
// while the collector was down — the one series in this service that can be healed
// retroactively, since userFillsByTime reaches back in time.

const PENDING_MAX_AGE_MS = 900_000;
const SWEEP_INTERVAL_MS = 30_000;
const STATS_INTERVAL_MS = 300_000;
const BACKFILL_CHUNK_MS = 5_400_000; // 90min windows keep responses far from the 2000-fill cap
const FAIL_RETRY_COOLDOWN_MS = 15_000;

const GRANULARITIES = [
  { seconds: 300, table: "liq_candles_5m" },
  { seconds: 3600, table: "liq_candles_1h" },
] as const;

interface PendingTrade {
  timeMs: number;
  wallets: [string, string];
}

interface LiqRow {
  tid: number;
  tsMs: number;
  coin: string;
  side: "long" | "short";
  px: number;
  sz: number;
  ntl: number;
  wallet: string;
  method: string | null;
}

export function startLiquidationsRecorder(isStopped: () => boolean, tape: TradeTape): () => Promise<void> {
  const pending = new Map<number, PendingTrade>(); // tid → unclassified tape trade
  const byWallet = new Map<string, Set<number>>(); // wallet → its pending tids
  const cooldownUntil = new Map<string, number>();
  const backfillQueue: Array<{ wallet: string; fromMs: number; toMs: number; fails?: number }> = [];

  let seen = 0;
  let covered = 0;
  let expired = 0;
  let verifies = 0;
  let verifyFails = 0;
  let fillsRecorded = 0;

  tape.onTrades((trades) => {
    const minTime = Date.now() - config.liqBackfillHours * 3_600_000;
    for (const t of trades) {
      if (!Array.isArray(t.users) || t.users.length < 2) continue;
      if (t.time < minTime || pending.has(t.tid)) continue; // resubscribe snapshots replay deep history
      seen++;
      pending.set(t.tid, { timeMs: t.time, wallets: [t.users[0], t.users[1]] });
      for (const w of t.users) {
        let set = byWallet.get(w);
        if (!set) byWallet.set(w, (set = new Set()));
        set.add(t.tid);
      }
    }
  });

  tape.onGap((gapMs) => {
    // Trades during the outage never reached the tape; sweep wallets that were
    // active going into it so their fills replay what was missed.
    void seedBackfill(Date.now() - gapMs - 300_000, `ws gap ${Math.round(gapMs / 1000)}s`);
  });

  function removeTrade(tid: number, trade: PendingTrade): void {
    pending.delete(tid);
    for (const w of trade.wallets) {
      const set = byWallet.get(w);
      if (set) {
        set.delete(tid);
        if (set.size === 0) byWallet.delete(w);
      }
    }
  }

  // The wallet whose verification would classify the most pending trades, among
  // wallets off cooldown whose oldest pending trade has aged past the verify lag
  // (so bursts batch into one call and HL has indexed the fills).
  function pickLive(): string | null {
    const now = Date.now();
    let best: string | null = null;
    let bestN = 0;
    let bestOldest = Infinity;
    for (const [wallet, tids] of byWallet) {
      if (tids.size < bestN) continue;
      if ((cooldownUntil.get(wallet) ?? 0) > now) continue;
      let oldest = Infinity;
      for (const tid of tids) {
        const p = pending.get(tid);
        if (p && p.timeMs < oldest) oldest = p.timeMs;
      }
      if (now - oldest < config.liqVerifyLagMs) continue;
      if (tids.size > bestN || oldest < bestOldest) {
        best = wallet;
        bestN = tids.size;
        bestOldest = oldest;
      }
    }
    return best;
  }

  async function verifyLive(wallet: string): Promise<void> {
    const tids = byWallet.get(wallet);
    if (!tids || tids.size === 0) return;
    let fromMs = Infinity;
    for (const tid of tids) {
      const p = pending.get(tid);
      if (p && p.timeMs < fromMs) fromMs = p.timeMs;
    }
    if (!Number.isFinite(fromMs)) {
      byWallet.delete(wallet);
      return;
    }
    const fetchStartMs = Date.now();
    const fills = await hl.userFillsByTime(wallet, fromMs - 10_000);
    verifies++;
    if (fills.length >= 2000) {
      await opsEvent("liq", "warn", `userFillsByTime hit the 2000-fill cap for ${wallet} — window too dense, some trades may go unclassified`);
    }
    await ingest(fills, wallet);
    // Anything traded while the request was in flight isn't covered by its window.
    const coveredThroughMs = fetchStartMs - 5_000;
    for (const tid of [...tids]) {
      const p = pending.get(tid);
      if (!p) {
        tids.delete(tid);
        continue;
      }
      if (p.timeMs <= coveredThroughMs) {
        covered++;
        removeTrade(tid, p);
      }
    }
    if (tids.size === 0) byWallet.delete(wallet);
    cooldownUntil.set(wallet, Date.now() + config.liqWalletCooldownMs);
  }

  async function verifyBackfill(entry: { wallet: string; fromMs: number; toMs: number }): Promise<void> {
    const chunkEnd = Math.min(entry.fromMs + BACKFILL_CHUNK_MS, entry.toMs);
    const fills = await hl.userFillsByTime(entry.wallet, entry.fromMs, chunkEnd);
    verifies++;
    if (fills.length >= 2000) {
      await opsEvent("liq", "warn", `backfill hit the 2000-fill cap for ${entry.wallet} (${new Date(entry.fromMs).toISOString()}) — chunk not exhaustive`);
    }
    await ingest(fills, entry.wallet);
    if (chunkEnd < entry.toMs) backfillQueue.push({ ...entry, fromMs: chunkEnd });
  }

  // Store every liquidation-marked fill. The queried wallet may be the victim or the
  // counterparty; liquidatedUser settles who was forced, and the victim's direction
  // follows from which side of the print they were on.
  async function ingest(fills: UserFill[], viaWallet: string): Promise<void> {
    const rows: LiqRow[] = [];
    const via = viaWallet.toLowerCase();
    for (const f of fills) {
      if (!f.liquidation) continue;
      const px = parseFloat(f.px);
      const sz = parseFloat(f.sz);
      if (!Number.isFinite(px) || !Number.isFinite(sz) || sz <= 0) continue;
      const victim = (f.liquidation.liquidatedUser ?? viaWallet).toLowerCase();
      const victimSold = victim === via ? f.side === "A" : f.side === "B";
      rows.push({
        tid: f.tid,
        tsMs: f.time,
        coin: f.coin,
        side: victimSold ? "long" : "short",
        px,
        sz,
        ntl: px * sz,
        wallet: victim,
        method: f.liquidation.method ?? null,
      });
    }
    if (rows.length === 0) return;
    await writeRows(rows);
    fillsRecorded += rows.length;
  }

  async function writeRows(rows: LiqRow[]): Promise<void> {
    const client = await pool.connect();
    try {
      await client.query("begin");
      await client.query(
        `insert into liq_fills (tid, ts, coin, side, px, sz, ntl, wallet, method)
         select * from unnest($1::bigint[], $2::timestamptz[], $3::text[], $4::text[],
                              $5::float8[], $6::float8[], $7::float8[], $8::text[], $9::text[])
         on conflict (tid) do nothing`,
        [
          rows.map((r) => r.tid),
          rows.map((r) => new Date(r.tsMs)),
          rows.map((r) => r.coin),
          rows.map((r) => r.side),
          rows.map((r) => r.px),
          rows.map((r) => r.sz),
          rows.map((r) => r.ntl),
          rows.map((r) => r.wallet),
          rows.map((r) => r.method),
        ],
      );
      for (const g of GRANULARITIES) {
        const bucketMs = g.seconds * 1000;
        const touched = new Map<string, { coin: string; t: number }>();
        for (const r of rows) {
          const t = Math.floor(r.tsMs / bucketMs) * bucketMs;
          touched.set(`${r.coin}|${t}`, { coin: r.coin, t });
        }
        for (const b of touched.values()) {
          await client.query(
            `insert into ${g.table} (coin, t, long_ntl, short_ntl, long_events, short_events, long_fills, short_fills)
             select $1, to_timestamp($2 / 1000.0),
               coalesce(sum(ntl) filter (where side = 'long'), 0),
               coalesce(sum(ntl) filter (where side = 'short'), 0),
               (count(distinct (wallet, ts)) filter (where side = 'long'))::int,
               (count(distinct (wallet, ts)) filter (where side = 'short'))::int,
               (count(*) filter (where side = 'long'))::int,
               (count(*) filter (where side = 'short'))::int
             from liq_fills
             where coin = $1 and ts >= to_timestamp($2 / 1000.0) and ts < to_timestamp($3 / 1000.0)
             on conflict (coin, t) do update set
               long_ntl = excluded.long_ntl, short_ntl = excluded.short_ntl,
               long_events = excluded.long_events, short_events = excluded.short_events,
               long_fills = excluded.long_fills, short_fills = excluded.short_fills`,
            [b.coin, b.t, b.t + bucketMs],
          );
        }
      }
      await client.query("commit");
    } catch (err) {
      await client.query("rollback");
      throw err;
    } finally {
      client.release();
    }
  }

  async function seedBackfill(fromMsRaw: number, reason: string): Promise<void> {
    if (config.liqBackfillWallets <= 0) return;
    try {
      const nowMs = Date.now();
      const fromMs = Math.max(fromMsRaw, nowMs - config.liqBackfillHours * 3_600_000);
      if (nowMs - fromMs < 120_000) return; // nothing meaningful to heal
      const { rows } = await pool.query<{ address: string }>(
        `select address from traders
         where last_trade_at > to_timestamp($1 / 1000.0) - interval '15 minutes'
         order by last_trade_at desc limit $2`,
        [fromMs, config.liqBackfillWallets],
      );
      const queued = new Set(backfillQueue.map((e) => e.wallet));
      let added = 0;
      for (const r of rows) {
        if (queued.has(r.address)) continue;
        backfillQueue.push({ wallet: r.address, fromMs, toMs: nowMs });
        added++;
      }
      if (added > 0) {
        log("liq", `${reason} — backfill sweep queued ${added} wallets over ${Math.round((nowMs - fromMs) / 60_000)}min`);
      }
    } catch (err) {
      logErr("liq", "backfill seed failed", err);
    }
  }

  async function bootBackfill(): Promise<void> {
    try {
      const { rows } = await pool.query<{ max: Date | null }>("select max(ts) as max from liq_fills");
      const watermark = rows[0]?.max ? rows[0].max.getTime() : 0;
      await seedBackfill(watermark - 300_000, watermark > 0 ? "boot" : "first boot");
    } catch (err) {
      logErr("liq", "boot backfill failed", err);
    }
  }

  // One paced verification per tick — the whole recorder's API budget in one knob.
  // Backfill interleaves with live picks so a boot sweep can't starve fresh trades.
  async function verifyLoop(): Promise<void> {
    await bootBackfill();
    let flip = false;
    while (!isStopped()) {
      await sleepStop(config.liqVerifyDelayMs);
      if (isStopped()) break;
      flip = !flip;
      const entry = backfillQueue.length > 0 && flip ? backfillQueue.shift()! : null;
      try {
        if (entry) {
          await verifyBackfill(entry);
        } else {
          const wallet = pickLive();
          if (wallet) {
            try {
              await verifyLive(wallet);
            } catch (err) {
              cooldownUntil.set(wallet, Date.now() + FAIL_RETRY_COOLDOWN_MS);
              throw err;
            }
          } else if (backfillQueue.length > 0) {
            await verifyBackfill(backfillQueue.shift()!);
          }
        }
      } catch (err) {
        verifyFails++;
        if (entry) {
          const fails = (entry.fails ?? 0) + 1;
          if (fails < 3) backfillQueue.push({ ...entry, fails }); // retry the chunk later
          else logErr("liq", `backfill chunk dropped after ${fails} failures (${entry.wallet})`);
        }
        logErr("liq", "verify failed", err);
        await sleepStop(5_000);
      }
    }
  }

  async function sweepLoop(): Promise<void> {
    while (!isStopped()) {
      await sleepStop(SWEEP_INTERVAL_MS);
      if (isStopped()) break;
      const cutoff = Date.now() - PENDING_MAX_AGE_MS;
      for (const [tid, trade] of pending) {
        if (trade.timeMs < cutoff) {
          expired++;
          removeTrade(tid, trade);
        }
      }
      const now = Date.now();
      for (const [wallet, until] of cooldownUntil) {
        if (until <= now) cooldownUntil.delete(wallet);
      }
    }
  }

  async function statsLoop(): Promise<void> {
    while (!isStopped()) {
      await sleepStop(STATS_INTERVAL_MS);
      if (isStopped()) break;
      const classified = covered + expired;
      const pct = classified > 0 ? ((covered / classified) * 100).toFixed(1) : "—";
      log(
        "liq",
        `${fillsRecorded} liq fills recorded, ${verifies} verifies (${verifyFails} failed), ` +
          `trade coverage ${pct}% (${covered}/${classified}), pending ${pending.size}, backfill queue ${backfillQueue.length}`,
      );
    }
  }

  async function sleepStop(ms: number): Promise<void> {
    const deadline = Date.now() + ms;
    while (!isStopped() && Date.now() < deadline) {
      await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
    }
  }

  const loops = [verifyLoop(), sweepLoop(), statsLoop()];
  log(
    "liq",
    `recorder started (verify pace ${config.liqVerifyDelayMs}ms, wallet cooldown ${Math.round(config.liqWalletCooldownMs / 1000)}s, backfill ${config.liqBackfillWallets} wallets/${config.liqBackfillHours}h)`,
  );

  return async () => {
    await Promise.allSettled(loops);
    log("liq", "recorder stopped");
  };
}
