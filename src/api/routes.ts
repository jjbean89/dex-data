import type { FastifyInstance, FastifyReply } from "fastify";
import { config } from "../config.js";
import { pool } from "../db/pool.js";
import {
  changesBundle,
  fundingRows,
  latestPositioning,
  latestPositioningFor,
  latestTicks,
  marketCandles,
  marketOiCloseAt,
  perpCandles,
  perpList,
  positioningAt,
  positioningHistory,
  resolveCoin,
  singleTick,
  toleranceFor,
  trackerCoverage,
  type CandleInterval,
  type ChangeRow,
  type ChangesBundle,
  type MarketCandleRow,
  type PerpCandleRow,
  type PositioningRow,
} from "./queries.js";
import { aprPct, cached, parseLimit, parseTimeMs, parseWindow, pctChange, rollingMean } from "./util.js";

type Query = Record<string, string | undefined>;

const CACHE_MS = 10_000;
const CANDLE_LIMIT_MAX = 5_000;
const BUCKET_MS: Record<CandleInterval, number> = { "5m": 300_000, "1h": 3_600_000, "1d": 86_400_000 };
const CHANGE_WINDOWS = ["1h", "4h", "24h"] as const;

function bad(reply: FastifyReply, message: string): FastifyReply {
  return reply.code(400).send({ error: { code: "bad_request", message } });
}

function notFound(reply: FastifyReply, message: string): FastifyReply {
  return reply.code(404).send({ error: { code: "not_found", message } });
}

function noRecentData(reply: FastifyReply): FastifyReply {
  return reply
    .code(503)
    .send({ error: { code: "no_recent_data", message: "no ticks recorded in the last 3 minutes — collector down or still warming up" } });
}

function positioningWarmingUp(
  reply: FastifyReply,
  coverage: { tracked: number; pending: number; provisional: number },
): FastifyReply {
  return reply.code(503).send({
    error: {
      code: "no_positioning_data",
      message: "no positioning snapshots yet — the tracker is discovering and bootstrapping wallets",
    },
    coverage,
  });
}

function getChanges(windowMs: number): Promise<ChangesBundle> {
  return cached(`changes:${windowMs}`, CACHE_MS, () => changesBundle(windowMs));
}

// Resolves interval/from/to/limit query params shared by the candle endpoints.
function candleRange(
  q: Query,
  reply: FastifyReply,
): { interval: CandleInterval; fromMs: number; toMs: number; limit: number } | null {
  const interval = (q.interval ?? "1h") as CandleInterval;
  if (!["5m", "1h", "1d"].includes(interval)) {
    bad(reply, `invalid interval "${q.interval}" — use 5m, 1h, or 1d`);
    return null;
  }
  const limit = parseLimit(q.limit, 300, CANDLE_LIMIT_MAX);
  if (limit === null) {
    bad(reply, "invalid limit");
    return null;
  }
  const from = parseTimeMs(q.from);
  const to = parseTimeMs(q.to);
  if (from === null || to === null) {
    bad(reply, "invalid from/to — use epoch ms, epoch seconds, or an ISO timestamp");
    return null;
  }
  const bucketMs = BUCKET_MS[interval];
  const toMs = to ?? Date.now() + bucketMs; // include the live partial bucket by default
  const fromMs = from ?? toMs - limit * bucketMs;
  if (fromMs >= toMs) {
    bad(reply, "from must be before to");
    return null;
  }
  return { interval, fromMs, toMs, limit };
}

function serializePerpCandle(r: PerpCandleRow): Record<string, unknown> {
  return {
    t: r.t.toISOString(),
    tMs: r.t.getTime(),
    mid: { o: r.mid_o, h: r.mid_h, l: r.mid_l, c: r.mid_c },
    markC: r.mark_c,
    oracleC: r.oracle_c,
    oi: { o: r.oi_o, h: r.oi_h, l: r.oi_l, c: r.oi_c },
    oiUsd: { o: r.oi_usd_o, h: r.oi_usd_h, l: r.oi_usd_l, c: r.oi_usd_c },
    fundingHr: r.funding_hr_c,
    fundingAprPct: r.funding_hr_c !== null ? aprPct(r.funding_hr_c) : null,
    premiumAvg: r.premium_a,
    dayNtlVlm: r.day_ntl_vlm_c,
    nTicks: r.n_ticks,
  };
}

function serializePositioning(r: PositioningRow, includeCoin: boolean): Record<string, unknown> {
  const n = r.n_long + r.n_short;
  const ntlLong = r.ntl_long;
  const ntlShort = r.ntl_short;
  return {
    ...(includeCoin ? { coin: r.coin } : {}),
    t: r.ts.toISOString(),
    tMs: r.ts.getTime(),
    nLong: r.n_long,
    nShort: r.n_short,
    nTraders: n,
    pctLong: n > 0 ? (r.n_long / n) * 100 : null,
    longShortRatio: r.n_short > 0 ? r.n_long / r.n_short : null,
    szLong: r.sz_long,
    szShort: r.sz_short,
    ntlLongUsd: ntlLong,
    ntlShortUsd: ntlShort,
    netNtlUsd: ntlLong !== null && ntlShort !== null ? ntlLong - ntlShort : null,
    tradersTracked: r.traders_tracked,
    source: r.source,
  };
}

function serializeMarketCandle(r: MarketCandleRow): Record<string, unknown> {
  return {
    t: r.t.toISOString(),
    tMs: r.t.getTime(),
    oiUsd: { o: r.oi_usd_o, h: r.oi_usd_h, l: r.oi_usd_l, c: r.oi_usd_c },
    fundingHrOiw: r.funding_hr_oiw_a,
    fundingAprPctOiw: r.funding_hr_oiw_a !== null ? aprPct(r.funding_hr_oiw_a) : null,
    dayNtlVlm: r.day_ntl_vlm_c,
    nCoins: r.n_coins,
    nTicks: r.n_ticks,
  };
}

export function registerRoutes(app: FastifyInstance): void {
  app.get("/", async () => ({
    service: "dex-data",
    description: "Custom Hyperliquid market data: recorded open interest, funding, and derived change windows.",
    endpoints: [
      "GET /health",
      "GET /v1/perps",
      "GET /v1/perps/changes?window=1h&sort=px|oi|funding|volume&dir=desc&limit=50&minOiUsd=0",
      "GET /v1/perps/:coin",
      "GET /v1/perps/:coin/candles?interval=5m|1h|1d&from=&to=&limit=300",
      "GET /v1/perps/:coin/funding-history?from=&to=&limit=168",
      "GET /v1/perps/positioning?sort=traders|pctLong&dir=desc&limit=250",
      "GET /v1/perps/:coin/positioning",
      "GET /v1/perps/:coin/positioning/history?from=&to=&limit=288",
      "GET /v1/market/snapshot",
      "GET /v1/market/oi?interval=5m|1h|1d&from=&to=&limit=300",
      "GET /v1/market/funding?interval=1h&smooth=8h&from=&to=&limit=300",
    ],
  }));

  // Seed pipeline diagnostics: import progress plus the collector's recent
  // operational events, so failures are debuggable without host log access.
  app.get("/v1/ops/seed", async () => {
    const [progress, events] = await Promise.all([
      pool.query<{ source: string; done: number }>(
        "select source, count(*)::int as done from seed_progress group by source",
      ),
      pool.query<{ ts: Date; tag: string; level: string; message: string }>(
        "select ts, tag, level, message from ops_events order by ts desc limit 50",
      ),
    ]);
    const { rows: totalRows } = await pool.query<{ n: number }>(
      "select count(*)::int as n from perp_assets where is_delisted = false",
    );
    const total = totalRows[0]?.n ?? 0;
    const doneBy = new Map(progress.rows.map((r) => [r.source, r.done]));
    return {
      census: { done: doneBy.get("hypertracker") ?? 0, total },
      history: { done: doneBy.get("hypertracker-history") ?? 0, total },
      recentEvents: events.rows.map((e) => ({
        t: e.ts.toISOString(),
        tag: e.tag,
        level: e.level,
        message: e.message,
      })),
    };
  });

  app.get("/health", async (_req, reply) => {
    try {
      const { rows } = await pool.query<{ last: Date | null; coins: number }>(
        `select max(ts) as last, count(distinct coin)::int as coins
         from perp_ticks where ts >= now() - interval '10 minutes'`,
      );
      const last = rows[0]?.last ?? null;
      const tickAgeSec = last ? Math.round((Date.now() - last.getTime()) / 1000) : null;
      return {
        ok: true,
        lastTickAt: last ? last.toISOString() : null,
        tickAgeSec,
        ticksStale: tickAgeSec === null || tickAgeSec > 120,
        liveCoins: rows[0]?.coins ?? 0,
      };
    } catch {
      return reply.code(500).send({ ok: false, error: { code: "db_unreachable", message: "database query failed" } });
    }
  });

  app.get("/v1/perps", async () =>
    cached("perps:list", CACHE_MS, async () => {
      const rows = await perpList();
      return {
        count: rows.length,
        data: rows.map((r) => ({
          coin: r.coin,
          szDecimals: r.sz_decimals,
          maxLeverage: r.max_leverage,
          isDelisted: r.is_delisted,
          firstSeen: r.first_seen.toISOString(),
          asOf: r.ts ? r.ts.toISOString() : null,
          px: r.px,
          markPx: r.mark_px,
          oiUsd: r.oi_usd,
          openInterest: r.open_interest,
          fundingHr: r.funding_hr,
          fundingAprPct: r.funding_hr !== null ? aprPct(r.funding_hr) : null,
          dayNtlVlm: r.day_ntl_vlm,
          hl24hChangePct: pctChange(r.px, r.prev_day_px),
        })),
      };
    }),
  );

  app.get("/v1/perps/changes", async (req, reply) => {
    const q = req.query as Query;
    const windowRaw = q.window ?? "1h";
    const windowMs = parseWindow(windowRaw);
    if (windowMs === null) return bad(reply, `invalid window "${windowRaw}" — use e.g. 5m, 15m, 1h, 4h, 24h, 3d`);
    if (windowMs < 5 * 60_000) return bad(reply, "window must be at least 5m");
    if (windowMs > config.rawRetentionDays * 86_400_000) {
      return bad(reply, `window exceeds raw tick retention (${config.rawRetentionDays}d) — use the candle endpoints for longer ranges`);
    }
    const sorters: Record<string, (r: ChangeRow) => number | null> = {
      px: (r) => r.pxChangePct,
      oi: (r) => r.oiUsdChangePct,
      funding: (r) => r.fundingHr,
      volume: (r) => r.dayNtlVlm,
    };
    const sortRaw = q.sort ?? "px";
    const sorter = sorters[sortRaw];
    if (!sorter) return bad(reply, `invalid sort "${sortRaw}" — use px|oi|funding|volume`);
    const dir = q.dir ?? "desc";
    if (dir !== "asc" && dir !== "desc") return bad(reply, 'dir must be "asc" or "desc"');
    const limit = parseLimit(q.limit, 500, 500);
    if (limit === null) return bad(reply, "invalid limit");
    const minOiUsd = q.minOiUsd !== undefined && q.minOiUsd !== "" ? Number(q.minOiUsd) : 0;
    if (!Number.isFinite(minOiUsd)) return bad(reply, "invalid minOiUsd");

    const bundle = await getChanges(windowMs);
    if (!bundle.asOf) return noRecentData(reply);
    const sign = dir === "desc" ? -1 : 1;
    const data = bundle.rows
      .filter((r) => (r.oiUsd ?? 0) >= minOiUsd)
      .sort((a, b) => {
        const av = sorter(a);
        const bv = sorter(b);
        if (av === null && bv === null) return a.coin.localeCompare(b.coin);
        if (av === null) return 1;
        if (bv === null) return -1;
        return (av - bv) * sign;
      })
      .slice(0, limit);
    return {
      window: windowRaw,
      asOf: bundle.asOf,
      toleranceSec: Math.round(toleranceFor(windowMs) / 1000),
      count: data.length,
      data,
    };
  });

  app.get("/v1/perps/positioning", async (req, reply) => {
    const q = req.query as Query;
    const sortRaw = q.sort ?? "traders";
    const sorters: Record<string, (r: PositioningRow) => number> = {
      traders: (r) => r.n_long + r.n_short,
      pctLong: (r) => (r.n_long + r.n_short > 0 ? r.n_long / (r.n_long + r.n_short) : 0),
    };
    const sorter = sorters[sortRaw];
    if (!sorter) return bad(reply, `invalid sort "${sortRaw}" — use traders|pctLong`);
    const dir = q.dir ?? "desc";
    if (dir !== "asc" && dir !== "desc") return bad(reply, 'dir must be "asc" or "desc"');
    const limit = parseLimit(q.limit, 250, 500);
    if (limit === null) return bad(reply, "invalid limit");

    const { rows, coverage } = await cached("positioning:latest", 30_000, async () => ({
      rows: await latestPositioning(),
      coverage: await trackerCoverage(),
    }));
    if (rows.length === 0) return positioningWarmingUp(reply, coverage);
    const sign = dir === "desc" ? -1 : 1;
    const data = [...rows].sort((a, b) => (sorter(a) - sorter(b)) * sign).slice(0, limit);
    return { count: data.length, coverage, data: data.map((r) => serializePositioning(r, true)) };
  });

  app.get("/v1/perps/:coin/positioning", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const [latest, coverage] = await Promise.all([latestPositioningFor(asset.coin), trackerCoverage()]);
    if (!latest) return positioningWarmingUp(reply, coverage);
    const now = Date.now();
    const [ago1h, ago24h] = await Promise.all([
      positioningAt(asset.coin, now - 3_600_000),
      positioningAt(asset.coin, now - 86_400_000),
    ]);
    const changeVs = (then: PositioningRow | null): Record<string, unknown> | null => {
      if (!then) return null;
      const cur = serializePositioning(latest, false);
      const prev = serializePositioning(then, false);
      return {
        tMs: then.ts.getTime(),
        nLongThen: then.n_long,
        nShortThen: then.n_short,
        nLongDelta: latest.n_long - then.n_long,
        nShortDelta: latest.n_short - then.n_short,
        pctLongThen: prev.pctLong,
        pctLongDelta:
          typeof cur.pctLong === "number" && typeof prev.pctLong === "number"
            ? (cur.pctLong as number) - (prev.pctLong as number)
            : null,
      };
    };
    return {
      coin: asset.coin,
      ...serializePositioning(latest, false),
      coverage,
      changes: { "1h": changeVs(ago1h), "24h": changeVs(ago24h) },
    };
  });

  app.get("/v1/perps/:coin/positioning/history", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const q = req.query as Query;
    const limit = parseLimit(q.limit, 288, CANDLE_LIMIT_MAX);
    if (limit === null) return bad(reply, "invalid limit");
    const from = parseTimeMs(q.from);
    const to = parseTimeMs(q.to);
    if (from === null || to === null) return bad(reply, "invalid from/to — use epoch ms, epoch seconds, or an ISO timestamp");
    const toMs = to ?? Date.now() + 60_000;
    const fromMs = from ?? toMs - limit * config.positionsSnapshotMs;
    if (fromMs >= toMs) return bad(reply, "from must be before to");
    const rows = await positioningHistory(asset.coin, fromMs, toMs, limit);
    return { coin: asset.coin, count: rows.length, data: rows.map((r) => serializePositioning(r, false)) };
  });

  app.get("/v1/perps/:coin", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const tick = await singleTick(asset.coin);
    if (!tick) return noRecentData(reply);

    const changes: Record<string, unknown> = {};
    for (const w of CHANGE_WINDOWS) {
      const windowMs = parseWindow(w)!;
      const row = (await getChanges(windowMs)).rows.find((r) => r.coin === asset.coin);
      changes[w] = row
        ? {
            pxThen: row.pxThen,
            pxChangePct: row.pxChangePct,
            oiUsdThen: row.oiUsdThen,
            oiUsdChangePct: row.oiUsdChangePct,
            fundingHrThen: row.fundingHrThen,
            thenTs: row.thenTs,
          }
        : null;
    }

    return {
      coin: asset.coin,
      szDecimals: asset.sz_decimals,
      maxLeverage: asset.max_leverage,
      isDelisted: asset.is_delisted,
      firstSeen: asset.first_seen.toISOString(),
      asOf: tick.ts.toISOString(),
      px: tick.px,
      midPx: tick.mid_px,
      markPx: tick.mark_px,
      oraclePx: tick.oracle_px,
      premium: tick.premium,
      openInterest: tick.open_interest,
      oiUsd: tick.oi_usd,
      fundingHr: tick.funding_hr,
      fundingAprPct: tick.funding_hr !== null ? aprPct(tick.funding_hr) : null,
      dayNtlVlm: tick.day_ntl_vlm,
      hl24hChangePct: pctChange(tick.px, tick.prev_day_px),
      changes,
    };
  });

  app.get("/v1/perps/:coin/candles", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const range = candleRange(req.query as Query, reply);
    if (!range) return reply;
    const rows = await perpCandles(asset.coin, range.interval, range.fromMs, range.toMs, range.limit);
    return { coin: asset.coin, interval: range.interval, count: rows.length, data: rows.map(serializePerpCandle) };
  });

  app.get("/v1/perps/:coin/funding-history", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const q = req.query as Query;
    const limit = parseLimit(q.limit, 168, CANDLE_LIMIT_MAX);
    if (limit === null) return bad(reply, "invalid limit");
    const from = parseTimeMs(q.from);
    const to = parseTimeMs(q.to);
    if (from === null || to === null) return bad(reply, "invalid from/to — use epoch ms, epoch seconds, or an ISO timestamp");
    const toMs = to ?? Date.now();
    const fromMs = from ?? toMs - limit * 3_600_000;
    if (fromMs >= toMs) return bad(reply, "from must be before to");
    const rows = await fundingRows(asset.coin, fromMs, toMs, limit);
    return {
      coin: asset.coin,
      count: rows.length,
      data: rows.map((r) => ({
        t: r.ts.toISOString(),
        tMs: r.ts.getTime(),
        rateHr: r.rate_hr,
        aprPct: aprPct(r.rate_hr),
        premium: r.premium,
      })),
    };
  });

  app.get("/v1/market/snapshot", async (_req, reply) => {
    const snap = await cached("market:snapshot", CACHE_MS, async () => {
      const ticks = await latestTicks();
      if (ticks.length === 0) return null;
      let totalOi = 0;
      let wSum = 0;
      let wDen = 0;
      let totalVlm = 0;
      let nCoins = 0;
      let asOf: Date | null = null;
      for (const t of ticks) {
        if (t.oi_usd !== null) {
          totalOi += t.oi_usd;
          nCoins++;
          if (t.funding_hr !== null) {
            wSum += t.funding_hr * t.oi_usd;
            wDen += t.oi_usd;
          }
        }
        if (t.day_ntl_vlm !== null) totalVlm += t.day_ntl_vlm;
        if (!asOf || t.ts > asOf) asOf = t.ts;
      }
      const now = Date.now();
      const [oi1hAgo, oi24hAgo] = await Promise.all([
        marketOiCloseAt(now - 3_600_000),
        marketOiCloseAt(now - 86_400_000),
      ]);
      const fundingOiw = wDen > 0 ? wSum / wDen : null;
      return {
        asOf: asOf ? asOf.toISOString() : null,
        nCoins,
        totalOiUsd: totalOi,
        oiUsdChangePct1h: pctChange(totalOi, oi1hAgo),
        oiUsdChangePct24h: pctChange(totalOi, oi24hAgo),
        fundingHrOiw: fundingOiw,
        fundingAprPctOiw: fundingOiw !== null ? aprPct(fundingOiw) : null,
        totalDayNtlVlm: totalVlm,
      };
    });
    if (!snap) return noRecentData(reply);
    return snap;
  });

  app.get("/v1/market/oi", async (req, reply) => {
    const range = candleRange(req.query as Query, reply);
    if (!range) return reply;
    const rows = await marketCandles(range.interval, range.fromMs, range.toMs, range.limit);
    return { interval: range.interval, count: rows.length, data: rows.map(serializeMarketCandle) };
  });

  app.get("/v1/market/funding", async (req, reply) => {
    const q = req.query as Query;
    const range = candleRange(q, reply);
    if (!range) return reply;
    let smoothBuckets = 0;
    if (q.smooth !== undefined && q.smooth !== "") {
      const smoothMs = parseWindow(q.smooth);
      if (smoothMs === null) return bad(reply, `invalid smooth "${q.smooth}" — use e.g. 8h, 24h`);
      smoothBuckets = Math.max(1, Math.round(smoothMs / BUCKET_MS[range.interval]));
    }
    // Fetch extra lookback so the first smoothed points have a full window behind them.
    const fetchFromMs = range.fromMs - smoothBuckets * BUCKET_MS[range.interval];
    const rows = await marketCandles(range.interval, fetchFromMs, range.toMs, range.limit + smoothBuckets);
    const smoothed = smoothBuckets > 0 ? rollingMean(rows.map((r) => r.funding_hr_oiw_a), smoothBuckets) : null;
    const data = rows
      .map((r, i) => ({
        t: r.t.toISOString(),
        tMs: r.t.getTime(),
        fundingHrOiw: r.funding_hr_oiw_a,
        fundingAprPctOiw: r.funding_hr_oiw_a !== null ? aprPct(r.funding_hr_oiw_a) : null,
        ...(smoothed ? { fundingHrOiwSmooth: smoothed[i] ?? null } : {}),
        nCoins: r.n_coins,
      }))
      .filter((r) => r.tMs >= range.fromMs)
      .slice(-range.limit);
    return { interval: range.interval, smooth: q.smooth ?? null, count: data.length, data };
  });
}
