import { config } from "../config.js";
import { pool } from "../db/pool.js";
import { sleep } from "../hl/client.js";
import type { WsTrade } from "../hl/ws.js";
import { log, logErr } from "../log.js";
import type { TradeTape } from "./tape.js";

// Volume bars from the trade tape.
//
// Every print on the trades WebSocket is folded into an in-memory 1m bucket per
// coin (taker-buy / taker-sell notional and size, print counts, OHLC of trade
// prices, TWAP-print notional — TWAP fills carry an all-zero hash), flushed
// every VOL_FLUSH_MS as additive upserts into vol_candles_1m; the touched 5m and
// 1h buckets are recomputed from the 1m rows in the same transaction.
//
// Additive upserts need each print counted exactly once. Two replay paths exist:
// HL replays recent trades on (re)subscription, so tids seen in the last few
// minutes are remembered and skipped; and after a process restart the previous
// process may already have flushed some of the replayed prints, so trades older
// than this process's start are dropped rather than risk double counting (a
// restart loses at most the few seconds between the last flush and the boot).
//
// This is the one series here that no API can backfill: like open interest,
// per-bar volume exists only while the collector is running.

const ZERO_HASH = /^0x0+$/;
const SEEN_TTL_MS = 600_000;
const BUCKET_1M = 60_000;
const ROLLUPS = [
  { table: "vol_candles_5m", ms: 300_000 },
  { table: "vol_candles_1h", ms: 3_600_000 },
] as const;

interface Bar {
  coin: string;
  t: number; // bucket start ms
  o: number;
  h: number;
  l: number;
  c: number;
  firstMs: number;
  lastMs: number;
  buyNtl: number;
  sellNtl: number;
  buySz: number;
  sellSz: number;
  buyN: number;
  sellN: number;
  twapNtl: number;
}

export function startVolumeRecorder(isStopped: () => boolean, tape: TradeTape): () => Promise<void> {
  const startedAt = Date.now();
  const bars = new Map<string, Bar>(); // `${coin}|${t}` → pending bar
  const seen = new Map<number, number>(); // tid → seen-at ms
  let prints = 0;
  let dropped = 0;
  let flushed = 0;

  tape.onTrades((trades) => {
    const now = Date.now();
    for (const tr of trades) {
      if (tr.time < startedAt || seen.has(tr.tid)) {
        dropped++;
        continue;
      }
      seen.set(tr.tid, now);
      fold(tr);
    }
  });

  function fold(tr: WsTrade): void {
    const px = parseFloat(tr.px);
    const sz = parseFloat(tr.sz);
    if (!Number.isFinite(px) || !Number.isFinite(sz) || sz <= 0 || px <= 0) return;
    prints++;
    const t = Math.floor(tr.time / BUCKET_1M) * BUCKET_1M;
    const key = `${tr.coin}|${t}`;
    let b = bars.get(key);
    if (!b) {
      b = { coin: tr.coin, t, o: px, h: px, l: px, c: px, firstMs: tr.time, lastMs: tr.time, buyNtl: 0, sellNtl: 0, buySz: 0, sellSz: 0, buyN: 0, sellN: 0, twapNtl: 0 };
      bars.set(key, b);
    }
    const ntl = px * sz;
    if (tr.time < b.firstMs) {
      b.firstMs = tr.time;
      b.o = px;
    }
    if (tr.time >= b.lastMs) {
      b.lastMs = tr.time;
      b.c = px;
    }
    if (px > b.h) b.h = px;
    if (px < b.l) b.l = px;
    if (tr.side === "B") {
      b.buyNtl += ntl;
      b.buySz += sz;
      b.buyN++;
    } else {
      b.sellNtl += ntl;
      b.sellSz += sz;
      b.sellN++;
    }
    if (ZERO_HASH.test(tr.hash)) b.twapNtl += ntl;
  }

  async function flush(): Promise<void> {
    if (bars.size === 0) return;
    const batch = [...bars.values()];
    bars.clear();
    const client = await pool.connect();
    try {
      await client.query("begin");
      // Additive merge. Open/close follow exchange time so a late-arriving earlier
      // print doesn't overwrite the close (and vice versa).
      await client.query(
        `insert into vol_candles_1m (coin, t, o, h, l, c, buy_ntl, sell_ntl, buy_sz, sell_sz, buy_n, sell_n, twap_ntl, last_ms, slot)
         select u.*, (extract(epoch from u.t)::bigint % 86400)::int
         from unnest($1::text[], $2::timestamptz[], $3::float8[], $4::float8[], $5::float8[], $6::float8[],
                     $7::float8[], $8::float8[], $9::float8[], $10::float8[], $11::int[], $12::int[], $13::float8[], $14::bigint[])
           as u(coin, t, o, h, l, c, buy_ntl, sell_ntl, buy_sz, sell_sz, buy_n, sell_n, twap_ntl, last_ms)
         on conflict (coin, t) do update set
           o = vol_candles_1m.o,
           h = greatest(vol_candles_1m.h, excluded.h),
           l = least(vol_candles_1m.l, excluded.l),
           c = case when excluded.last_ms >= vol_candles_1m.last_ms then excluded.c else vol_candles_1m.c end,
           last_ms = greatest(vol_candles_1m.last_ms, excluded.last_ms),
           buy_ntl = vol_candles_1m.buy_ntl + excluded.buy_ntl,
           sell_ntl = vol_candles_1m.sell_ntl + excluded.sell_ntl,
           buy_sz = vol_candles_1m.buy_sz + excluded.buy_sz,
           sell_sz = vol_candles_1m.sell_sz + excluded.sell_sz,
           buy_n = vol_candles_1m.buy_n + excluded.buy_n,
           sell_n = vol_candles_1m.sell_n + excluded.sell_n,
           twap_ntl = vol_candles_1m.twap_ntl + excluded.twap_ntl`,
        [
          batch.map((b) => b.coin),
          batch.map((b) => new Date(b.t)),
          batch.map((b) => b.o),
          batch.map((b) => b.h),
          batch.map((b) => b.l),
          batch.map((b) => b.c),
          batch.map((b) => b.buyNtl),
          batch.map((b) => b.sellNtl),
          batch.map((b) => b.buySz),
          batch.map((b) => b.sellSz),
          batch.map((b) => b.buyN),
          batch.map((b) => b.sellN),
          batch.map((b) => b.twapNtl),
          batch.map((b) => b.lastMs),
        ],
      );
      for (const r of ROLLUPS) {
        const touched = new Map<string, { coin: string; t: number }>();
        for (const b of batch) {
          const t = Math.floor(b.t / r.ms) * r.ms;
          touched.set(`${b.coin}|${t}`, { coin: b.coin, t });
        }
        const list = [...touched.values()];
        await client.query(
          `insert into ${r.table} (coin, t, o, h, l, c, buy_ntl, sell_ntl, buy_sz, sell_sz, buy_n, sell_n, twap_ntl, last_ms, slot)
           select k.coin, k.t,
             (array_agg(m.o order by m.t))[1], max(m.h), min(m.l), (array_agg(m.c order by m.t desc))[1],
             sum(m.buy_ntl), sum(m.sell_ntl), sum(m.buy_sz), sum(m.sell_sz), sum(m.buy_n)::int, sum(m.sell_n)::int,
             sum(m.twap_ntl), max(m.last_ms), (extract(epoch from k.t)::bigint % 86400)::int
           from unnest($1::text[], $2::timestamptz[]) as k(coin, t)
           join vol_candles_1m m on m.coin = k.coin and m.t >= k.t and m.t < k.t + make_interval(secs => $3)
           group by k.coin, k.t
           on conflict (coin, t) do update set
             o = excluded.o, h = excluded.h, l = excluded.l, c = excluded.c, last_ms = excluded.last_ms,
             buy_ntl = excluded.buy_ntl, sell_ntl = excluded.sell_ntl, buy_sz = excluded.buy_sz, sell_sz = excluded.sell_sz,
             buy_n = excluded.buy_n, sell_n = excluded.sell_n, twap_ntl = excluded.twap_ntl`,
          [list.map((k) => k.coin), list.map((k) => new Date(k.t)), r.ms / 1000],
        );
      }
      await client.query("commit");
      flushed += batch.length;
    } catch (err) {
      await client.query("rollback");
      // Put the batch back so the prints aren't lost; the next flush retries.
      for (const b of batch) {
        const key = `${b.coin}|${b.t}`;
        const cur = bars.get(key);
        if (!cur) {
          bars.set(key, b);
          continue;
        }
        cur.o = b.firstMs <= cur.firstMs ? b.o : cur.o;
        cur.firstMs = Math.min(cur.firstMs, b.firstMs);
        if (b.lastMs >= cur.lastMs) {
          cur.c = b.c;
          cur.lastMs = b.lastMs;
        }
        cur.h = Math.max(cur.h, b.h);
        cur.l = Math.min(cur.l, b.l);
        cur.buyNtl += b.buyNtl;
        cur.sellNtl += b.sellNtl;
        cur.buySz += b.buySz;
        cur.sellSz += b.sellSz;
        cur.buyN += b.buyN;
        cur.sellN += b.sellN;
        cur.twapNtl += b.twapNtl;
      }
      throw err;
    } finally {
      client.release();
    }
  }

  async function flushLoop(): Promise<void> {
    let lastStats = Date.now();
    while (!isStopped()) {
      await sleepStop(config.volFlushMs);
      try {
        await flush();
      } catch (err) {
        logErr("volume", "flush failed", err);
      }
      const now = Date.now();
      for (const [tid, at] of seen) if (now - at > SEEN_TTL_MS) seen.delete(tid);
      if (now - lastStats >= 300_000) {
        log("volume", `${prints} prints folded, ${flushed} bar-updates flushed, ${dropped} replayed/pre-boot prints skipped`);
        lastStats = now;
      }
    }
    // Final flush on shutdown so the last seconds of prints are kept.
    try {
      await flush();
    } catch (err) {
      logErr("volume", "final flush failed", err);
    }
  }

  async function sleepStop(ms: number): Promise<void> {
    const deadline = Date.now() + ms;
    while (!isStopped() && Date.now() < deadline) {
      await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
    }
  }

  const run = flushLoop();
  log("volume", `recorder started (flush every ${config.volFlushMs}ms, 1m bars kept ${config.vol1mRetentionDays}d)`);
  return async () => {
    await run;
    log("volume", "recorder stopped");
  };
}
