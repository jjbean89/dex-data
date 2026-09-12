import type { FastifyInstance, FastifyReply } from "fastify";
import { config } from "../config.js";
import {
  DAY_MS,
  MCAP_INDEXES,
  MCAP_INDEX_NAMES,
  MCAP_INTERVAL_MS,
  dailyClosesAt,
  historyInfo,
  indexValue,
  indexVolume,
  isMcapIndex,
  latestSnapshot,
  mcapCandles,
  snapshotAt,
  type McapCandleRow,
  type McapIndex,
  type McapInterval,
  type McapSnapshot,
} from "../db/marketcap.js";
import { bad, cached, notFound, parseLimit, parseTimeMs, pctChange } from "./util.js";

// Global market-cap index endpoints — the feeds behind a TradingView-style
// CRYPTOCAP:TOTAL3 / CRYPTOCAP:OTHERS chart (daily OHLC + volume + EMAs).

type Query = Record<string, string | undefined>;

const CACHE_MS = 10_000;
const SERIES_CACHE_MS = 60_000;
const CANDLE_LIMIT_MAX = 5_000;
const CANDLE_LIMIT_DEFAULT = 365;
const DEFAULT_EMAS = "21,200";
const EMA_MAX_PERIOD = 1_000;
const EMA_MAX_COUNT = 6;
const INTERVALS = Object.keys(MCAP_INTERVAL_MS) as McapInterval[];

function noData(reply: FastifyReply): FastifyReply {
  return reply.code(503).send({
    error: {
      code: "no_marketcap_data",
      message: "no market-cap snapshots yet — the collector polls CoinGecko shortly after boot (check MARKETCAP_ENABLED and collector logs)",
    },
  });
}

// EMA seeded with the SMA of the first `period` closes (TradingView's ta.ema),
// null until the seed is complete.
export function emaSeries(closes: number[], period: number): Array<number | null> {
  const out: Array<number | null> = new Array<number | null>(closes.length).fill(null);
  const alpha = 2 / (period + 1);
  let ema: number | null = null;
  let seedSum = 0;
  for (let i = 0; i < closes.length; i++) {
    const c = closes[i]!;
    if (ema === null) {
      seedSum += c;
      if (i + 1 >= period) ema = seedSum / period;
    } else {
      ema = alpha * c + (1 - alpha) * ema;
    }
    out[i] = ema;
  }
  return out;
}

function parseEmaPeriods(raw: string | undefined): number[] | null {
  const src = raw === undefined ? DEFAULT_EMAS : raw.trim();
  if (src === "" || src === "none" || src === "0") return [];
  const periods = [...new Set(src.split(",").map((s) => Number(s.trim())))];
  if (periods.length > EMA_MAX_COUNT) return null;
  for (const p of periods) if (!Number.isInteger(p) || p < 2 || p > EMA_MAX_PERIOD) return null;
  return periods.sort((a, b) => a - b);
}

// The whole stored series for an index/interval, ascending, so EMAs are seeded
// from all available history (the way a chart computes them) before the
// requested window is cut out.
function fullSeries(idx: McapIndex, interval: McapInterval, toMs: number): Promise<McapCandleRow[]> {
  const bucket = MCAP_INTERVAL_MS[interval];
  const toKey = Math.ceil(toMs / bucket) * bucket;
  return cached(`mcap:series:${idx}:${interval}:${toKey}`, SERIES_CACHE_MS, () =>
    mcapCandles(idx, interval, 0, toKey, 1_000_000),
  );
}

function serializeCandle(r: McapCandleRow, emas: Array<[number, number | null]>): Record<string, unknown> {
  const row: Record<string, unknown> = {
    t: r.t.toISOString(),
    tMs: r.t.getTime(),
    o: r.o,
    h: r.h,
    l: r.l,
    c: r.c,
    v: r.v,
    source: r.source,
  };
  if (emas.length > 0) row.ema = Object.fromEntries(emas.map(([p, v]) => [String(p), v]));
  return row;
}

interface IndexQuote {
  index: McapIndex;
  name: string;
  marketCapUsd: number;
  volume24hUsd: number;
  change24hPct: number | null;
  change7dPct: number | null;
  change30dPct: number | null;
  excludes: string[];
}

async function quotes(latest: McapSnapshot): Promise<Record<McapIndex, IndexQuote>> {
  const nowMs = latest.ts.getTime();
  const today = Math.floor(nowMs / DAY_MS) * DAY_MS;
  const dayAgo = await snapshotAt(nowMs - DAY_MS, 2 * config.mcapPollMs + 60_000);
  const out = {} as Record<McapIndex, IndexQuote>;
  for (const idx of MCAP_INDEXES) {
    const closes = await dailyClosesAt(idx, [today - 7 * DAY_MS, today - 30 * DAY_MS]);
    const now = indexValue(latest, idx);
    const excludes = idx === "total" ? [] : idx === "total2" ? ["bitcoin"] : idx === "total3" ? ["bitcoin", "ethereum"] : latest.top_ids;
    out[idx] = {
      index: idx,
      name: idx === "others" ? `Crypto Total Market Cap Excluding Top ${latest.top_n}` : MCAP_INDEX_NAMES[idx],
      marketCapUsd: now,
      volume24hUsd: indexVolume(latest, idx),
      change24hPct: dayAgo ? pctChange(now, indexValue(dayAgo, idx)) : null,
      change7dPct: pctChange(now, closes.get(today - 7 * DAY_MS) ?? null),
      change30dPct: pctChange(now, closes.get(today - 30 * DAY_MS) ?? null),
      excludes,
    };
  }
  return out;
}

function ageOf(latest: McapSnapshot): { asOf: string; ageSec: number; stale: boolean } {
  const ageSec = Math.round((Date.now() - latest.ts.getTime()) / 1000);
  return { asOf: latest.ts.toISOString(), ageSec, stale: ageSec * 1000 > 3 * config.mcapPollMs };
}

export function registerMarketcapRoutes(app: FastifyInstance): void {
  // Every index right now, with the constituents behind OTHERS.
  app.get("/v1/marketcap", async (_req, reply) => {
    const res = await cached("mcap:summary", CACHE_MS, async () => {
      const latest = await latestSnapshot();
      if (!latest) return null;
      const [indexes, history] = await Promise.all([quotes(latest), historyInfo("total3")]);
      return {
        ...ageOf(latest),
        vs: "usd",
        source: "coingecko",
        btc: { marketCapUsd: latest.btc_usd, dominancePct: (latest.btc_usd / latest.total_usd) * 100 },
        eth: { marketCapUsd: latest.eth_usd, dominancePct: (latest.eth_usd / latest.total_usd) * 100 },
        top: { n: latest.top_n, ids: latest.top_ids, marketCapUsd: latest.top_usd, volume24hUsd: latest.top_vol_usd },
        indexes,
        history: {
          firstDay: history.firstDay?.toISOString() ?? null,
          days: history.days,
          bySource: history.bySource,
        },
      };
    });
    if (!res) return noData(reply);
    return res;
  });

  app.get("/v1/marketcap/:index", async (req, reply) => {
    const { index } = req.params as { index: string };
    const idx = index.toLowerCase();
    if (!isMcapIndex(idx)) return notFound(reply, `no index "${index}" — use one of ${MCAP_INDEXES.join(", ")}`);
    const res = await cached(`mcap:quote:${idx}`, CACHE_MS, async () => {
      const latest = await latestSnapshot();
      if (!latest) return null;
      const [q, history] = await Promise.all([quotes(latest), historyInfo(idx)]);
      return {
        ...ageOf(latest),
        vs: "usd",
        ...q[idx],
        history: { firstDay: history.firstDay?.toISOString() ?? null, days: history.days, bySource: history.bySource },
      };
    });
    if (!res) return noData(reply);
    return res;
  });

  // OHLC candles + volume + EMAs: everything a chart of the index needs.
  app.get("/v1/marketcap/:index/candles", async (req, reply) => {
    const { index } = req.params as { index: string };
    const idx = index.toLowerCase();
    if (!isMcapIndex(idx)) return notFound(reply, `no index "${index}" — use one of ${MCAP_INDEXES.join(", ")}`);
    const q = req.query as Query;
    const interval = (q.interval ?? "1d") as McapInterval;
    if (!INTERVALS.includes(interval)) return bad(reply, `invalid interval "${q.interval}" — use ${INTERVALS.join(", ")}`);
    const limit = parseLimit(q.limit, CANDLE_LIMIT_DEFAULT, CANDLE_LIMIT_MAX);
    if (limit === null) return bad(reply, "invalid limit");
    const from = parseTimeMs(q.from);
    const to = parseTimeMs(q.to);
    if (from === null || to === null) return bad(reply, "invalid from/to — use epoch ms, epoch seconds, or an ISO timestamp");
    const periods = parseEmaPeriods(q.ema);
    if (periods === null) return bad(reply, `invalid ema — up to ${EMA_MAX_COUNT} integer periods between 2 and ${EMA_MAX_PERIOD}, e.g. ema=21,200 (ema=none to omit)`);
    const bucketMs = MCAP_INTERVAL_MS[interval];
    const toMs = to ?? Date.now() + bucketMs; // include the live partial candle by default
    const fromMs = from ?? 0;
    if (fromMs >= toMs) return bad(reply, "from must be before to");

    let rows: McapCandleRow[];
    let emaCols: Array<Array<number | null>> = [];
    if (periods.length > 0) {
      const all = await fullSeries(idx, interval, toMs);
      const closes = all.map((r) => r.c);
      const cols = periods.map((p) => emaSeries(closes, p));
      let end = all.length;
      while (end > 0 && all[end - 1]!.t.getTime() >= toMs) end--;
      let start = end;
      while (start > 0 && end - start < limit && all[start - 1]!.t.getTime() >= fromMs) start--;
      rows = all.slice(start, end);
      emaCols = cols.map((c) => c.slice(start, end));
    } else {
      rows = await mcapCandles(idx, interval, fromMs, toMs, limit);
    }
    const latest = rows.length > 0 ? null : await latestSnapshot();
    if (rows.length === 0 && !latest) return noData(reply);
    return {
      index: idx,
      name: MCAP_INDEX_NAMES[idx],
      interval,
      vs: "usd",
      ema: periods,
      count: rows.length,
      data: rows.map((r, i) => serializeCandle(r, periods.map((p, j) => [p, emaCols[j]![i] ?? null]))),
    };
  });
}
