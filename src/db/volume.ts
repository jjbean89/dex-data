import { pool } from "./pool.js";

// Volume bars and the "volume vs. baseline" context shared by the detector
// (collector) and the volume board / signals endpoints (API).

export const VOL_TABLES: Record<string, { table: string; ms: number }> = {
  "1m": { table: "vol_candles_1m", ms: 60_000 },
  "5m": { table: "vol_candles_5m", ms: 300_000 },
  "1h": { table: "vol_candles_1h", ms: 3_600_000 },
};

const BAR_MS = 300_000; // detector/board work on 5m bars

export interface VolBarRow {
  t: Date;
  o: number;
  h: number;
  l: number;
  c: number;
  buy_ntl: number;
  sell_ntl: number;
  buy_sz: number;
  sell_sz: number;
  buy_n: number;
  sell_n: number;
  twap_ntl: number;
}

// Most recent `limit` bars within [from, to), ascending.
export async function volCandles(coin: string, table: string, fromMs: number, toMs: number, limit: number): Promise<VolBarRow[]> {
  const { rows } = await pool.query<VolBarRow>(
    `select t, o, h, l, c, buy_ntl, sell_ntl, buy_sz, sell_sz, buy_n, sell_n, twap_ntl
     from ${table}
     where coin = $1 and t >= to_timestamp($2 / 1000.0) and t < to_timestamp($3 / 1000.0)
     order by t desc limit $4`,
    [coin, fromMs, toMs, limit],
  );
  return rows.reverse();
}

export interface WindowBar {
  tMs: number;
  o: number;
  h: number;
  l: number;
  c: number;
  ntl: number;
  buyNtl: number;
  sellNtl: number;
  twapNtl: number;
  partial: boolean;
  fraction: number; // share of the bar elapsed (1 for closed bars)
}

export interface VolContext {
  coin: string;
  bars: WindowBar[]; // oldest first; may be shorter than requested when bars had no prints
  med24: number | null; // median 5m notional over the trailing 24h
  medSlot: number | null; // median notional of the same time-of-day slots over the prior week
  n24: number; // bars in the trailing 24h
  atrPct: number | null; // median 5m (high-low)/close over 24h, in %
  firstBarMs: number | null; // oldest bar within the 7d lookback (history depth)
}

// Window geometry: the last `bars` 5m buckets ending at `nowMs`. The live bucket
// counts once at least a minute of it has elapsed (its expectation is pro-rated);
// before that the window is the `bars` most recent closed buckets.
export function windowBuckets(bars: number, nowMs: number): { startMs: number; ends: number; partialFraction: number } {
  const live = Math.floor(nowMs / BAR_MS) * BAR_MS;
  const elapsed = nowMs - live;
  if (elapsed >= 60_000) {
    return { startMs: live - (bars - 1) * BAR_MS, ends: live + BAR_MS, partialFraction: elapsed / BAR_MS };
  }
  return { startMs: live - bars * BAR_MS, ends: live, partialFraction: 1 };
}

// Per-coin window bars + baselines in three indexed queries (coins = null → every coin with bars).
export async function volumeContext(coins: string[] | null, bars: number, nowMs: number): Promise<Map<string, VolContext>> {
  const w = windowBuckets(bars, nowMs);
  const liveStart = Math.floor(nowMs / BAR_MS) * BAR_MS;
  const slots: number[] = [];
  for (let t = w.startMs; t < w.ends; t += BAR_MS) slots.push(Math.floor(t / 1000) % 86_400);

  const [win, base, slot] = await Promise.all([
    pool.query<{ coin: string; t: Date; o: number; h: number; l: number; c: number; buy_ntl: number; sell_ntl: number; twap_ntl: number }>(
      `select coin, t, o, h, l, c, buy_ntl, sell_ntl, twap_ntl from vol_candles_5m
       where t >= to_timestamp($1 / 1000.0) and t < to_timestamp($2 / 1000.0)
         and ($3::text[] is null or coin = any($3))
       order by coin, t`,
      [w.startMs, w.ends, coins],
    ),
    pool.query<{ coin: string; med24: number | null; n24: number; atr: number | null; first_t: Date | null }>(
      `select coin,
         percentile_cont(0.5) within group (order by buy_ntl + sell_ntl) filter (where t >= to_timestamp($1 / 1000.0) - interval '24 hours') as med24,
         (count(*) filter (where t >= to_timestamp($1 / 1000.0) - interval '24 hours'))::int as n24,
         percentile_cont(0.5) within group (order by (h - l) / nullif(c, 0)) filter (where t >= to_timestamp($1 / 1000.0) - interval '24 hours') as atr,
         min(t) as first_t
       from vol_candles_5m
       where t >= to_timestamp($1 / 1000.0) - interval '7 days' and t < to_timestamp($1 / 1000.0)
         and ($2::text[] is null or coin = any($2))
       group by coin`,
      [w.startMs, coins],
    ),
    pool.query<{ coin: string; med_slot: number | null }>(
      `select coin, percentile_cont(0.5) within group (order by buy_ntl + sell_ntl) as med_slot
       from vol_candles_5m
       where slot = any($3::int[])
         and t >= to_timestamp($1 / 1000.0) - interval '7 days' and t < to_timestamp($1 / 1000.0) - interval '24 hours'
         and ($2::text[] is null or coin = any($2))
       group by coin`,
      [w.startMs, coins, slots],
    ),
  ]);

  const out = new Map<string, VolContext>();
  const ctxFor = (coin: string): VolContext => {
    let c = out.get(coin);
    if (!c) out.set(coin, (c = { coin, bars: [], med24: null, medSlot: null, n24: 0, atrPct: null, firstBarMs: null }));
    return c;
  };
  for (const r of base.rows) {
    const c = ctxFor(r.coin);
    c.med24 = r.med24;
    c.n24 = r.n24;
    c.atrPct = r.atr === null ? null : r.atr * 100;
    c.firstBarMs = r.first_t ? r.first_t.getTime() : null;
  }
  for (const r of slot.rows) ctxFor(r.coin).medSlot = r.med_slot;
  for (const r of win.rows) {
    const tMs = r.t.getTime();
    const partial = tMs === liveStart && w.partialFraction < 1;
    ctxFor(r.coin).bars.push({
      tMs,
      o: r.o,
      h: r.h,
      l: r.l,
      c: r.c,
      ntl: r.buy_ntl + r.sell_ntl,
      buyNtl: r.buy_ntl,
      sellNtl: r.sell_ntl,
      twapNtl: r.twap_ntl,
      partial,
      fraction: partial ? w.partialFraction : 1,
    });
  }
  return out;
}

// Open interest (USD) per coin at or just before `atMs` (within 3 minutes).
export async function oiUsdAt(coins: string[] | null, atMs: number): Promise<Map<string, number>> {
  const { rows } = await pool.query<{ coin: string; oi_usd: number | null }>(
    `select distinct on (coin) coin, oi_usd from perp_ticks
     where ts > to_timestamp($1 / 1000.0) - interval '3 minutes' and ts <= to_timestamp($1 / 1000.0)
       and ($2::text[] is null or coin = any($2))
     order by coin, ts desc`,
    [atMs, coins],
  );
  const out = new Map<string, number>();
  for (const r of rows) if (r.oi_usd !== null) out.set(r.coin, r.oi_usd);
  return out;
}

export interface VolMetrics {
  coin: string;
  bars: number; // bars counted (closed + live)
  tFromMs: number;
  tToMs: number;
  volNtl: number;
  baselineBar: number | null; // expected notional per full bar
  expectedNtl: number | null; // expected notional for the window (live bar pro-rated)
  rvol: number | null;
  minBarRvol: number | null;
  avgBarUsd: number;
  pxFrom: number;
  pxTo: number;
  moveP: number; // % move open of first bar → close of last
  atrPct: number | null;
  moveAtr: number | null; // |move| / atr
  buySharePct: number;
  twapSharePct: number;
  oiFrom: number | null;
  oiTo: number | null;
  oiChangePct: number | null;
  historyHours: number;
  n24: number;
}

// Pure: turns a coin's context into the numbers the detector and board work with.
export function volumeMetrics(ctx: VolContext, oiFrom: number | null, oiTo: number | null, nowMs: number): VolMetrics | null {
  if (ctx.bars.length === 0) return null;
  const first = ctx.bars[0]!;
  const last = ctx.bars[ctx.bars.length - 1]!;
  const volNtl = ctx.bars.reduce((s, b) => s + b.ntl, 0);
  const buyNtl = ctx.bars.reduce((s, b) => s + b.buyNtl, 0);
  const twapNtl = ctx.bars.reduce((s, b) => s + b.twapNtl, 0);
  const barEquiv = ctx.bars.reduce((s, b) => s + b.fraction, 0);
  // Blend of the trailing-24h median and the same-slot median over the prior week
  // (intraday seasonality), the former alone while the coin has no week of history.
  const baselineBar =
    ctx.med24 === null ? null : ctx.medSlot === null ? ctx.med24 : 0.5 * ctx.med24 + 0.5 * ctx.medSlot;
  const expected = baselineBar === null ? null : baselineBar * barEquiv;
  const rvol = expected && expected > 0 ? volNtl / expected : null;
  let minBarRvol: number | null = null;
  if (baselineBar && baselineBar > 0) {
    minBarRvol = Math.min(...ctx.bars.map((b) => b.ntl / (baselineBar * b.fraction)));
  }
  const moveP = first.o > 0 ? ((last.c - first.o) / first.o) * 100 : 0;
  const atrPct = ctx.atrPct;
  return {
    coin: ctx.coin,
    bars: ctx.bars.length,
    tFromMs: first.tMs,
    tToMs: last.tMs,
    volNtl,
    baselineBar,
    expectedNtl: expected,
    rvol,
    minBarRvol,
    avgBarUsd: barEquiv > 0 ? volNtl / barEquiv : 0,
    pxFrom: first.o,
    pxTo: last.c,
    moveP,
    atrPct,
    moveAtr: atrPct && atrPct > 0 ? Math.abs(moveP) / atrPct : null,
    buySharePct: volNtl > 0 ? (buyNtl / volNtl) * 100 : 50,
    twapSharePct: volNtl > 0 ? (twapNtl / volNtl) * 100 : 0,
    oiFrom,
    oiTo,
    oiChangePct: oiFrom && oiTo && oiFrom > 0 ? ((oiTo - oiFrom) / oiFrom) * 100 : null,
    historyHours: ctx.firstBarMs === null ? 0 : (nowMs - ctx.firstBarMs) / 3_600_000,
    n24: ctx.n24,
  };
}

export interface VolSignalRow {
  id: string;
  coin: string;
  fired_at: Date;
  t_from: Date;
  t_to: Date;
  bars: number;
  vol_ntl: number;
  baseline_ntl: number;
  rvol: number;
  min_bar_rvol: number;
  px_from: number;
  px_to: number;
  px_move_pct: number;
  atr_pct: number;
  oi_from: number | null;
  oi_to: number | null;
  oi_change_pct: number | null;
  buy_share_pct: number;
  twap_share_pct: number;
  bias: string;
  market_wide: boolean;
  status: string;
  confirmed_at: Date | null;
  breakout_move_pct: number | null;
  breakout_rvol: number | null;
  closed_at: Date | null;
  message: string;
  delivered: boolean | null;
  delivery_error: string | null;
  updated_at: Date;
}

export async function listVolSignals(f: {
  coin?: string;
  status?: string;
  sinceMs?: number;
  marketWide?: boolean;
  limit: number;
}): Promise<VolSignalRow[]> {
  const { rows } = await pool.query<VolSignalRow>(
    `select * from vol_signals
     where ($1::text is null or coin = $1)
       and ($2::text is null or status = $2)
       and ($3::float8 is null or fired_at >= to_timestamp($3 / 1000.0))
       and ($4::boolean is null or market_wide = $4)
     order by fired_at desc, id desc limit $5`,
    [f.coin ?? null, f.status ?? null, f.sinceMs ?? null, f.marketWide ?? null, f.limit],
  );
  return rows;
}
