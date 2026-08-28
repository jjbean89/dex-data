import { config } from "../config.js";
import { pool } from "../db/pool.js";
import { hl, sleep } from "../hl/client.js";
import { TradesFeed } from "../hl/ws.js";
import { log, logErr } from "../log.js";

// Long/short trader tracking.
//
// Discovery: every fill on the trades WebSocket names [buyer, seller]. New wallets
// enter `traders` as bootstrap_pending; the bootstrapper fetches their true positions
// via clearinghouseState. Once bootstrapped, fill deltas (+sz buyer / -sz seller,
// verified empirically) maintain `positions`. Deltas seen while a wallet is still
// pending are intentionally dropped — the bootstrap snapshot already includes them.
// Residual drift (WS gaps, the pending→live handoff window) is healed by re-queuing:
// on reconnect gaps, and by the slow re-verify sweep.

export function startPositionsTracker(isStopped: () => boolean): () => Promise<void> {
  interface PendingDelta {
    address: string;
    coin: string;
    delta: number;
    lastTimeMs: number;
  }

  const buffer = new Map<string, PendingDelta>();
  let tradeCount = 0;

  const feed = new TradesFeed({
    url: config.hlWsUrl,
    getCoins: async () => {
      const { rows } = await pool.query<{ coin: string }>(
        "select coin from perp_assets where is_delisted = false",
      );
      return rows.map((r) => r.coin);
    },
    onTrades: (trades) => {
      for (const t of trades) {
        const sz = parseFloat(t.sz);
        if (!Number.isFinite(sz) || sz === 0 || !Array.isArray(t.users)) continue;
        tradeCount++;
        addDelta(t.users[0], t.coin, sz, t.time);
        addDelta(t.users[1], t.coin, -sz, t.time);
      }
    },
    onGap: (gapMs) => void handleGap(gapMs),
  });

  function addDelta(address: string, coin: string, delta: number, timeMs: number): void {
    const key = `${address}|${coin}`;
    const cur = buffer.get(key);
    if (cur) {
      cur.delta += delta;
      if (timeMs > cur.lastTimeMs) cur.lastTimeMs = timeMs;
    } else {
      buffer.set(key, { address, coin, delta, lastTimeMs: timeMs });
    }
  }

  // Positions of anyone who traded during a WS outage have drifted; requeue wallets
  // active around the gap for a fresh baseline. The re-verify sweep catches stragglers.
  async function handleGap(gapMs: number): Promise<void> {
    try {
      const lookbackSec = Math.ceil(gapMs / 1000) + 3_600;
      const res = await pool.query(
        `update traders set bootstrap_pending = true
         where not bootstrap_pending and last_trade_at > now() - make_interval(secs => $1)`,
        [lookbackSec],
      );
      log("positions", `ws gap ${Math.round(gapMs / 1000)}s — requeued ${res.rowCount ?? 0} wallets for re-baseline`);
    } catch (err) {
      logErr("positions", "gap requeue failed", err);
    }
  }

  async function flush(): Promise<void> {
    if (buffer.size === 0) return;
    const entries = [...buffer.values()];
    buffer.clear();

    const addrs: string[] = [];
    const coins: string[] = [];
    const deltas: number[] = [];
    const lastByTrader = new Map<string, number>();
    for (const e of entries) {
      addrs.push(e.address);
      coins.push(e.coin);
      deltas.push(e.delta);
      const prev = lastByTrader.get(e.address);
      if (prev === undefined || e.lastTimeMs > prev) lastByTrader.set(e.address, e.lastTimeMs);
    }
    const tAddrs = [...lastByTrader.keys()];
    const tTimes = tAddrs.map((a) => new Date(lastByTrader.get(a)!));

    await pool.query(
      `insert into traders (address, last_trade_at)
       select * from unnest($1::text[], $2::timestamptz[])
       on conflict (address) do update set
         last_trade_at = greatest(traders.last_trade_at, excluded.last_trade_at)`,
      [tAddrs, tTimes],
    );
    await pool.query(
      `insert into positions (address, coin, szi, updated_at)
       select u.address, u.coin, u.delta, now()
       from unnest($1::text[], $2::text[], $3::float8[]) as u(address, coin, delta)
       join traders t on t.address = u.address
       where t.bootstrap_pending = false and u.delta <> 0
       on conflict (address, coin) do update set
         szi = positions.szi + excluded.szi, updated_at = now()`,
      [addrs, coins, deltas],
    );
  }

  async function flushLoop(): Promise<void> {
    while (!isStopped()) {
      await sleepStop(config.positionsFlushMs);
      try {
        await flush();
      } catch (err) {
        logErr("positions", "flush failed", err);
      }
    }
    await flush().catch(() => undefined); // best-effort drain on shutdown
  }

  async function bootstrapLoop(): Promise<void> {
    while (!isStopped()) {
      try {
        const { rows } = await pool.query<{ address: string }>(
          `select address from traders where bootstrap_pending
           order by last_trade_at desc nulls last limit 1`,
        );
        const addr = rows[0]?.address;
        if (!addr) {
          await sleepStop(2_000);
          continue;
        }
        const state = await hl.clearinghouseState(addr);
        const pCoins: string[] = [];
        const pSzi: number[] = [];
        const pEntry: Array<number | null> = [];
        for (const ap of state.assetPositions) {
          const szi = parseFloat(ap.position.szi);
          if (!Number.isFinite(szi) || szi === 0) continue;
          pCoins.push(ap.position.coin);
          pSzi.push(szi);
          const entry = parseFloat(ap.position.entryPx ?? "");
          pEntry.push(Number.isFinite(entry) ? entry : null);
        }
        const client = await pool.connect();
        try {
          await client.query("begin");
          await client.query("delete from positions where address = $1", [addr]);
          if (pCoins.length > 0) {
            await client.query(
              `insert into positions (address, coin, szi, entry_px, updated_at)
               select $1::text, u.*, now() from unnest($2::text[], $3::float8[], $4::float8[]) as u`,
              [addr, pCoins, pSzi, pEntry],
            );
          }
          await client.query(
            "update traders set bootstrap_pending = false, bootstrapped_at = now() where address = $1",
            [addr],
          );
          await client.query("commit");
        } catch (err) {
          await client.query("rollback");
          throw err;
        } finally {
          client.release();
        }
      } catch (err) {
        logErr("positions", "bootstrap failed", err);
        await sleepStop(2_000);
      }
      await sleepStop(config.bootstrapDelayMs);
    }
  }

  async function takeSnapshot(): Promise<void> {
    const intervalSec = Math.max(1, Math.floor(config.positionsSnapshotMs / 1000));
    await pool.query(
      `with marks as (
         select distinct on (coin) coin, mark_px from perp_ticks
         where ts >= now() - interval '5 minutes'
         order by coin, ts desc
       )
       insert into positioning_snapshots
         (ts, coin, n_long, n_short, sz_long, sz_short, ntl_long, ntl_short, traders_tracked,
          avg_entry_long, avg_entry_short, n_long_entry, n_short_entry, n_long_profit, n_short_profit)
       select
         to_timestamp(floor(extract(epoch from now()) / $1) * $1),
         p.coin,
         (count(*) filter (where p.szi > 0))::int,
         (count(*) filter (where p.szi < 0))::int,
         coalesce(sum(p.szi) filter (where p.szi > 0), 0),
         coalesce(-sum(p.szi) filter (where p.szi < 0), 0),
         sum(p.szi * m.mark_px) filter (where p.szi > 0),
         -sum(p.szi * m.mark_px) filter (where p.szi < 0),
         (select count(*) from traders where not bootstrap_pending)::int,
         sum(p.szi * p.entry_px) filter (where p.szi > 0 and p.entry_px is not null)
           / nullif(sum(p.szi) filter (where p.szi > 0 and p.entry_px is not null), 0),
         sum(-p.szi * p.entry_px) filter (where p.szi < 0 and p.entry_px is not null)
           / nullif(sum(-p.szi) filter (where p.szi < 0 and p.entry_px is not null), 0),
         (count(*) filter (where p.szi > 0 and p.entry_px is not null and m.mark_px is not null))::int,
         (count(*) filter (where p.szi < 0 and p.entry_px is not null and m.mark_px is not null))::int,
         (count(*) filter (where p.szi > 0 and p.entry_px is not null and m.mark_px > p.entry_px))::int,
         (count(*) filter (where p.szi < 0 and p.entry_px is not null and m.mark_px < p.entry_px))::int
       from positions p
       left join marks m on m.coin = p.coin
       where p.szi <> 0
       group by p.coin
       on conflict (coin, ts) do update set
         n_long = excluded.n_long, n_short = excluded.n_short,
         sz_long = excluded.sz_long, sz_short = excluded.sz_short,
         ntl_long = excluded.ntl_long, ntl_short = excluded.ntl_short,
         traders_tracked = excluded.traders_tracked,
         avg_entry_long = excluded.avg_entry_long, avg_entry_short = excluded.avg_entry_short,
         n_long_entry = excluded.n_long_entry, n_short_entry = excluded.n_short_entry,
         n_long_profit = excluded.n_long_profit, n_short_profit = excluded.n_short_profit`,
      [intervalSec],
    );
  }

  async function snapshotLoop(): Promise<void> {
    while (!isStopped()) {
      // Align to the interval grid so snapshot timestamps are clean.
      const next = (Math.floor(Date.now() / config.positionsSnapshotMs) + 1) * config.positionsSnapshotMs;
      await sleepUntil(next);
      if (isStopped()) break;
      try {
        await takeSnapshot();
      } catch (err) {
        logErr("positions", "snapshot failed", err);
      }
    }
  }

  // Slow self-heal: periodically re-baseline the longest-unverified active wallets.
  async function reverifyLoop(): Promise<void> {
    while (!isStopped()) {
      await sleepStop(config.reverifyIntervalMs);
      if (isStopped()) break;
      try {
        const res = await pool.query(
          `update traders set bootstrap_pending = true where address in (
             select address from traders
             where not bootstrap_pending and last_trade_at > now() - interval '7 days'
             order by bootstrapped_at asc nulls first limit $1
           )`,
          [config.reverifyBatch],
        );
        log("positions", `re-verify sweep queued ${res.rowCount ?? 0} wallets`);
      } catch (err) {
        logErr("positions", "re-verify failed", err);
      }
    }
  }

  async function statsLoop(): Promise<void> {
    let lastCount = 0;
    while (!isStopped()) {
      await sleepStop(60_000);
      if (isStopped()) break;
      try {
        const { rows } = await pool.query<{ pending: number; tracked: number }>(
          `select (count(*) filter (where bootstrap_pending))::int as pending,
                  (count(*) filter (where not bootstrap_pending))::int as tracked
           from traders`,
        );
        const rate = Math.round((tradeCount - lastCount) / 60);
        lastCount = tradeCount;
        log("positions", `~${rate} trades/s, wallets tracked ${rows[0]?.tracked ?? 0}, bootstrap queue ${rows[0]?.pending ?? 0}`);
      } catch (err) {
        logErr("positions", "stats failed", err);
      }
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

  feed.start();
  const loops = [flushLoop(), bootstrapLoop(), snapshotLoop(), reverifyLoop(), statsLoop()];
  log("positions", `tracker started (flush ${config.positionsFlushMs}ms, bootstrap pace ${config.bootstrapDelayMs}ms, snapshots every ${Math.round(config.positionsSnapshotMs / 60_000)}min)`);

  return async () => {
    feed.stop();
    await Promise.allSettled(loops);
    log("positions", "tracker stopped");
  };
}
