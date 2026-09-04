import { pool } from "../db/pool.js";
import { hl } from "../hl/client.js";
import type { Candle } from "../hl/types.js";
import {
  changesBundleCached,
  latestPositioningFor,
  liqTotals,
  positioningAt,
  singleTick,
  trackerCoverage,
  type AssetRow,
  type ChangeRow,
  type PositioningRow,
} from "./queries.js";
import { aprPct, cached, pctChange } from "./util.js";

// One-call "what happened to this coin" recap: liquidations by side, price change
// with all-time-high detection, open-interest change with record-high detection,
// and long/short trader deltas over a trailing window — the numbers a ticker
// line is written from, in one request.

const DAY_MS = 86_400_000;
const HL_CANDLE_CAP = 5_000; // HL retains ~5000 most recent candles per interval
const ATH_CACHE_MS = 300_000; // one candleSnapshot (weight 20) per coin per 5min at most
const RECORD_CACHE_MS = 60_000;
const NEAR_PCT = 5; // flags.nearAllTimeHigh threshold
const DOMINANT_SHARE_PCT = 60; // dominantSide names one side above this share of liquidated notional

// ---- Recorded extremes (our own candles) ----

interface ExtremesRow {
  px_h: number | null;
  px_h_at: Date | null;
  px_l: number | null;
  px_l_at: Date | null;
  oi_usd_h: number | null;
  oi_usd_h_at: Date | null;
  oi_usd_l: number | null;
  oi_usd_l_at: Date | null;
  n: number;
}

// Price/OI highs and lows over [from, to) from the 5m candles (180d retention —
// far beyond the raw-tick bound on the recap window).
async function candleExtremes(coin: string, fromMs: number, toMs: number): Promise<ExtremesRow> {
  const { rows } = await pool.query<ExtremesRow>(
    `with c as (
       select t, mid_h, mid_l, oi_usd_h, oi_usd_l from perp_candles_5m
       where coin = $1 and t >= to_timestamp($2 / 1000.0) and t < to_timestamp($3 / 1000.0)
     )
     select
       (select mid_h from c where mid_h is not null order by mid_h desc, t limit 1) as px_h,
       (select t from c where mid_h is not null order by mid_h desc, t limit 1) as px_h_at,
       (select mid_l from c where mid_l is not null order by mid_l asc, t limit 1) as px_l,
       (select t from c where mid_l is not null order by mid_l asc, t limit 1) as px_l_at,
       (select oi_usd_h from c where oi_usd_h is not null order by oi_usd_h desc, t limit 1) as oi_usd_h,
       (select t from c where oi_usd_h is not null order by oi_usd_h desc, t limit 1) as oi_usd_h_at,
       (select oi_usd_l from c where oi_usd_l is not null order by oi_usd_l asc, t limit 1) as oi_usd_l,
       (select t from c where oi_usd_l is not null order by oi_usd_l asc, t limit 1) as oi_usd_l_at,
       (select count(*) from c)::int as n`,
    [coin, fromMs, toMs],
  );
  return rows[0] ?? { px_h: null, px_h_at: null, px_l: null, px_l_at: null, oi_usd_h: null, oi_usd_h_at: null, oi_usd_l: null, oi_usd_l_at: null, n: 0 };
}

interface OiRecordRow {
  oi_usd_h: number | null;
  oi_usd_h_at: Date | null;
  oi_h: number | null;
  oi_h_at: Date | null;
  recorded_since: Date | null;
}

// Highest open interest this service has ever recorded for the coin. 1h candles
// are kept forever; the last couple of days of 5m candles are unioned in because
// the current hour's 1h bucket may not be rolled up yet.
function oiRecord(coin: string): Promise<OiRecordRow> {
  return cached(`recap:oirecord:${coin}`, RECORD_CACHE_MS, async () => {
    const { rows } = await pool.query<OiRecordRow>(
      `with c as (
         select t, oi_usd_h, oi_h from perp_candles_1h where coin = $1
         union all
         select t, oi_usd_h, oi_h from perp_candles_5m where coin = $1 and t >= now() - interval '2 days'
       )
       select
         (select oi_usd_h from c where oi_usd_h is not null order by oi_usd_h desc, t limit 1) as oi_usd_h,
         (select t from c where oi_usd_h is not null order by oi_usd_h desc, t limit 1) as oi_usd_h_at,
         (select oi_h from c where oi_h is not null order by oi_h desc, t limit 1) as oi_h,
         (select t from c where oi_h is not null order by oi_h desc, t limit 1) as oi_h_at,
         (select min(t) from c) as recorded_since`,
      [coin],
    );
    return rows[0] ?? { oi_usd_h: null, oi_usd_h_at: null, oi_h: null, oi_h_at: null, recorded_since: null };
  });
}

// ---- All-time high (Hyperliquid's own daily candles) ----

interface DailyCandle {
  t: number;
  T: number;
  h: number;
}

// Full daily history from HL — ~5000 daily candles covers every listing since
// the exchange launched. Cached per coin; a failed fetch is not cached.
function dailyCandles(coin: string): Promise<DailyCandle[]> {
  return cached(`recap:daily:${coin}`, ATH_CACHE_MS, async () => {
    const now = Date.now();
    const candles: Candle[] = await hl.candleSnapshot(coin, "1d", Math.max(0, now - HL_CANDLE_CAP * DAY_MS), now);
    return candles
      .map((c) => ({ t: c.t, T: c.T, h: parseFloat(c.h) }))
      .filter((c) => Number.isFinite(c.h))
      .sort((a, b) => a.t - b.t);
  });
}

export interface AthBlock {
  px: number;
  at: string; // exact 5m-candle time when set in the window, otherwise the day it was set
  isNewInWindow: boolean; // a high above every prior high was printed inside this window
  priorAthPx: number | null; // the record going into the window (null for a listing younger than the window)
  pctBelowAth: number | null; // current price vs the all-time high (0 = at the high)
  listedSince: string;
  dailyCandles: number;
  source: "hl-daily-candles";
}

async function allTimeHigh(
  coin: string,
  px: number | null,
  windowStartMs: number,
  windowHigh: { px: number | null; at: Date | null },
): Promise<AthBlock | null> {
  let daily: DailyCandle[];
  try {
    daily = await dailyCandles(coin);
  } catch {
    return null;
  }
  if (daily.length === 0) return null;

  // Prior record = every day fully closed before the window, plus the pre-window
  // part of the day straddling the window start (from our own 5m candles — if we
  // have no coverage there, the whole straddling day counts as prior, which can
  // only under-claim a new high, never invent one).
  let priorAth: number | null = null;
  let priorAthAt: number | null = null;
  const consider = (h: number, at: number): void => {
    if (priorAth === null || h > priorAth) {
      priorAth = h;
      priorAthAt = at;
    }
  };
  for (const c of daily) {
    if (c.T <= windowStartMs) consider(c.h, c.t);
  }
  const straddle = daily.find((c) => c.t < windowStartMs && c.T > windowStartMs);
  if (straddle) {
    const pre = await candleExtremes(coin, straddle.t, windowStartMs);
    if (pre.n > 0) {
      if (pre.px_h !== null) consider(pre.px_h, pre.px_h_at?.getTime() ?? straddle.t);
    } else {
      consider(straddle.h, straddle.t);
    }
  }

  // Window high: our 5m candles + the live price, and any daily candle that lies
  // entirely inside the window (covers a fresh deployment with thin 5m coverage).
  let inWindow: number | null = windowHigh.px;
  let inWindowAt: number | null = windowHigh.at ? windowHigh.at.getTime() : null;
  const bump = (h: number, at: number): void => {
    if (inWindow === null || h > inWindow) {
      inWindow = h;
      inWindowAt = at;
    }
  };
  if (px !== null) bump(px, Date.now());
  for (const c of daily) if (c.t >= windowStartMs) bump(c.h, c.t);

  if (inWindow === null && priorAth === null) return null;
  const isNew = inWindow !== null && (priorAth === null || inWindow > priorAth);
  const ath: number = isNew ? inWindow! : priorAth!;
  const athAt: number = isNew ? inWindowAt! : priorAthAt!;
  return {
    px: ath,
    at: new Date(athAt).toISOString(),
    isNewInWindow: isNew,
    priorAthPx: priorAth,
    pctBelowAth: px !== null && ath > 0 ? Math.max(0, ((ath - px) / ath) * 100) : null,
    listedSince: new Date(daily[0]!.t).toISOString(),
    dailyCandles: daily.length,
    source: "hl-daily-candles",
  };
}

// ---- Assembly ----

function positioningSummary(r: PositioningRow): Record<string, unknown> {
  const n = r.n_long + r.n_short;
  return {
    t: r.ts.toISOString(),
    tMs: r.ts.getTime(),
    nLong: r.n_long,
    nShort: r.n_short,
    nTraders: n,
    pctLong: n > 0 ? (r.n_long / n) * 100 : null,
    ntlLongUsd: r.ntl_long,
    ntlShortUsd: r.ntl_short,
    source: r.source,
    entries: entrySummary(r),
  };
}

// Size-weighted average entry per side and how many positions with a known
// entry are in profit at the snapshot's price. Null on pre-migration and
// backfilled (hypertracker) rows, which carry no entry prices.
function entrySummary(r: PositioningRow): Record<string, unknown> | null {
  if (r.n_long_entry === null || r.n_short_entry === null) return null;
  const longProfit = r.n_long_profit ?? 0;
  const shortProfit = r.n_short_profit ?? 0;
  return {
    avgEntryLong: r.avg_entry_long,
    avgEntryShort: r.avg_entry_short,
    longsInProfit: longProfit,
    longsUnderwater: r.n_long_entry - longProfit,
    shortsInProfit: shortProfit,
    shortsUnderwater: r.n_short_entry - shortProfit,
    pctLongsInProfit: r.n_long_entry > 0 ? (longProfit / r.n_long_entry) * 100 : null,
    pctShortsInProfit: r.n_short_entry > 0 ? (shortProfit / r.n_short_entry) * 100 : null,
    withKnownEntry: { long: r.n_long_entry, short: r.n_short_entry },
  };
}

const iso = (d: Date | null | undefined): string | null => (d ? d.toISOString() : null);

export interface Recap {
  ok: true;
  body: Record<string, unknown>;
}

export interface RecapNoData {
  ok: false;
}

export async function buildRecap(asset: AssetRow, windowName: string, windowMs: number): Promise<Recap | RecapNoData> {
  const coin = asset.coin;
  const tick = await singleTick(coin);
  if (!tick) return { ok: false };
  const now = Date.now();
  const windowStartMs = now - windowMs;

  const [bundle, liq, windowExt, record, posNow, posThen, coverage] = await Promise.all([
    changesBundleCached(windowMs),
    liqTotals(windowMs, coin),
    candleExtremes(coin, windowStartMs, now),
    oiRecord(coin),
    latestPositioningFor(coin),
    positioningAt(coin, windowStartMs),
    trackerCoverage(),
  ]);
  const change: ChangeRow | undefined = bundle.rows.find((r) => r.coin === coin);
  const ath = await allTimeHigh(coin, tick.px, windowStartMs, { px: windowExt.px_h, at: windowExt.px_h_at });

  // ---- price ----
  const pxThen = change?.pxThen ?? null;
  const pxChangePct = change?.pxChangePct ?? null;
  const price = {
    now: tick.px,
    then: pxThen,
    thenTs: change?.thenTs ?? null,
    changePct: pxChangePct,
    changeAbs: tick.px !== null && pxThen !== null ? tick.px - pxThen : null,
    hl24hChangePct: pctChange(tick.px, tick.prev_day_px),
    windowHigh: maxWithLive(windowExt.px_h, windowExt.px_h_at, tick.px, tick.ts),
    windowLow: windowExt.px_l !== null ? { px: Math.min(windowExt.px_l, tick.px ?? Infinity), at: iso(windowExt.px_l_at) } : null,
    allTimeHigh: ath,
  };

  // ---- open interest ----
  const oiThen = change?.oiUsdThen ?? null;
  const recordCandidate = maxWithLive(record.oi_usd_h, record.oi_usd_h_at, tick.oi_usd, tick.ts);
  const recordCandidateWindow = maxWithLive(windowExt.oi_usd_h, windowExt.oi_usd_h_at, tick.oi_usd, tick.ts);
  const recordCoins = maxWithLive(record.oi_h, record.oi_h_at, tick.open_interest, tick.ts);
  const recordUsd = recordCandidate?.px ?? null;
  const recordAtMs = recordCandidate?.at ? Date.parse(recordCandidate.at) : null;
  const oiRecordHigh = recordUsd !== null && recordAtMs !== null && recordAtMs >= windowStartMs;
  const recordedSinceMs = record.recorded_since?.getTime() ?? null;
  const openInterest = {
    nowUsd: tick.oi_usd,
    thenUsd: oiThen,
    changeUsd: tick.oi_usd !== null && oiThen !== null ? tick.oi_usd - oiThen : null,
    changePct: change?.oiUsdChangePct ?? null,
    now: tick.open_interest,
    windowHigh: recordCandidateWindow ? { usd: recordCandidateWindow.px, at: recordCandidateWindow.at } : null,
    windowLow: windowExt.oi_usd_l !== null ? { usd: Math.min(windowExt.oi_usd_l, tick.oi_usd ?? Infinity), at: iso(windowExt.oi_usd_l_at) } : null,
    record: {
      usd: recordUsd,
      at: recordCandidate?.at ?? null,
      coins: recordCoins?.px ?? null,
      coinsAt: recordCoins?.at ?? null,
      isRecordHigh: oiRecordHigh, // the record was set (or matched) inside this window
      pctBelowRecord: tick.oi_usd !== null && recordUsd !== null && recordUsd > 0 ? Math.max(0, ((recordUsd - tick.oi_usd) / recordUsd) * 100) : null,
      recordedSince: iso(record.recorded_since),
      recordedDays: recordedSinceMs !== null ? Math.round(((now - recordedSinceMs) / DAY_MS) * 10) / 10 : null,
    },
  };

  // ---- liquidations ----
  const l = liq[0];
  const longNtl = l?.long_ntl ?? 0;
  const shortNtl = l?.short_ntl ?? 0;
  const totalNtl = longNtl + shortNtl;
  const shortShare = totalNtl > 0 ? (shortNtl / totalNtl) * 100 : null;
  const dominantSide: "shorts" | "longs" | "balanced" | "none" =
    totalNtl === 0 ? "none" : shortShare! >= DOMINANT_SHARE_PCT ? "shorts" : shortShare! <= 100 - DOMINANT_SHARE_PCT ? "longs" : "balanced";
  const liquidations = {
    longs: { ntlUsd: longNtl, events: l?.long_events ?? 0, fills: l?.long_fills ?? 0, traders: l?.long_traders ?? 0 },
    shorts: { ntlUsd: shortNtl, events: l?.short_events ?? 0, fills: l?.short_fills ?? 0, traders: l?.short_traders ?? 0 },
    totalNtlUsd: totalNtl,
    events: (l?.long_events ?? 0) + (l?.short_events ?? 0),
    traders: l?.traders ?? 0, // distinct wallets liquidated (either side)
    shortSharePct: shortShare,
    dominantSide,
  };

  // ---- positioning ----
  let positioning: Record<string, unknown> | null = null;
  let longsChangePct: number | null = null;
  if (posNow) {
    const nowSum = positioningSummary(posNow);
    const thenSum = posThen ? positioningSummary(posThen) : null;
    longsChangePct = posThen ? pctChange(posNow.n_long, posThen.n_long) : null;
    positioning = {
      ...nowSum,
      then: thenSum,
      changes: posThen
        ? {
            nLongDelta: posNow.n_long - posThen.n_long,
            nLongChangePct: longsChangePct,
            nShortDelta: posNow.n_short - posThen.n_short,
            nShortChangePct: pctChange(posNow.n_short, posThen.n_short),
            pctLongDelta:
              typeof nowSum.pctLong === "number" && typeof thenSum?.pctLong === "number" ? nowSum.pctLong - thenSum.pctLong : null,
            ntlLongChangePct: pctChange(posNow.ntl_long, posThen.ntl_long),
            ntlShortChangePct: pctChange(posNow.ntl_short, posThen.ntl_short),
            avgEntryLongDelta: posNow.avg_entry_long !== null && posThen.avg_entry_long !== null ? posNow.avg_entry_long - posThen.avg_entry_long : null,
            avgEntryLongChangePct: pctChange(posNow.avg_entry_long, posThen.avg_entry_long),
            avgEntryShortDelta: posNow.avg_entry_short !== null && posThen.avg_entry_short !== null ? posNow.avg_entry_short - posThen.avg_entry_short : null,
            avgEntryShortChangePct: pctChange(posNow.avg_entry_short, posThen.avg_entry_short),
          }
        : null,
      coverage,
    };
  }

  const flags = {
    newAllTimeHigh: ath ? ath.isNewInWindow : null,
    nearAllTimeHigh: ath?.pctBelowAth !== null && ath?.pctBelowAth !== undefined ? ath.pctBelowAth <= NEAR_PCT : null,
    oiRecordHigh,
    liquidationsSide: dominantSide,
    longsIncreased: longsChangePct !== null ? longsChangePct > 0 : null,
  };


  return {
    ok: true,
    body: {
      coin,
      window: windowName,
      windowMs,
      from: new Date(windowStartMs).toISOString(),
      to: tick.ts.toISOString(),
      asOf: tick.ts.toISOString(),
      flags,
      price,
      openInterest,
      liquidations,
      positioning,
      funding: {
        hr: tick.funding_hr,
        aprPct: tick.funding_hr !== null ? aprPct(tick.funding_hr) : null,
        hrThen: change?.fundingHrThen ?? null,
      },
      dayNtlVlm: tick.day_ntl_vlm,
      maxLeverage: asset.max_leverage,
    },
  };
}

// Recorded high vs the live tick (the live tick may not be rolled into a candle yet).
function maxWithLive(h: number | null, hAt: Date | null, live: number | null, liveAt: Date): { px: number; at: string } | null {
  if (h === null && live === null) return null;
  if (h === null || (live !== null && live > h)) return { px: live!, at: liveAt.toISOString() };
  return { px: h, at: hAt ? hAt.toISOString() : liveAt.toISOString() };
}
