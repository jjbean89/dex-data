import type { FastifyInstance, FastifyReply } from "fastify";
import { EMA_TF_MS, config, type LiqAlertSide } from "../config.js";
import { liqWindowTotals, listLiqAlerts, loadLiqAlertRules, sideSlice, type LiqAlertRow, type LiqAlertRuleRow } from "../db/liq-alerts.js";
import { listLiqWhales, type LiqWhaleRow } from "../db/liq-whales.js";
import { pool } from "../db/pool.js";
import { VOL_TABLES, listVolSignals, oiUsdAt, volCandles, volumeContext, volumeMetrics, windowBuckets, type VolBarRow, type VolSignalRow } from "../db/volume.js";
import {
  LATEST_MAX_AGE_MS,
  LIQ_BUCKET_MS,
  changesBundleCached,
  emaStates,
  emaStatesFor,
  fundingRows,
  latestPositioning,
  latestPositioningFor,
  latestTicks,
  lastLiqAt,
  bridgeStatus,
  openPositionsFor,
  recentBridgeDeposits,
  whaleWallets,
  listWhaleAlerts,
  liqCandles,
  liqTotals,
  liqVenueTotals,
  marketCandles,
  marketOiCloseAt,
  perpCandles,
  perpList,
  positioningAt,
  positioningHistory,
  recentLiqFills,
  resolveCoin,
  singleTick,
  toleranceFor,
  trackerCoverage,
  type CandleInterval,
  type ChangeRow,
  type ChangesBundle,
  type EmaStateApiRow,
  type LiqCandleRow,
  type LiqFillRow,
  type LiqTotalsRow,
  type MarketCandleRow,
  type PerpCandleRow,
  type PositioningRow,
  type TickRow,
  type WalletPositionRow,
  type WhaleRow,
} from "./queries.js";
import { buildRecap } from "./recap.js";
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
  return changesBundleCached(windowMs);
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
    entries: serializeEntries(r),
  };
}

// Entry-price analytics (size-weighted avg entry per side; in-profit vs underwater
// counts among positions with a known entry price). Null on pre-migration and
// backfilled rows.
function serializeEntries(r: PositioningRow): Record<string, unknown> | null {
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

// Resolves interval/from/to/limit for the liquidation series endpoints, which
// support more intervals than the tick-candle endpoints (sums aggregate exactly).
function liqRange(
  q: Query,
  reply: FastifyReply,
): { interval: string; bucketMs: number; fromMs: number; toMs: number; limit: number } | null {
  const interval = q.interval ?? "1h";
  const bucketMs = LIQ_BUCKET_MS[interval];
  if (!bucketMs) {
    bad(reply, `invalid interval "${q.interval}" — use ${Object.keys(LIQ_BUCKET_MS).join(", ")}`);
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
  const toMs = to ?? Date.now() + bucketMs; // include the live partial bucket by default
  const fromMs = from ?? toMs - limit * bucketMs;
  if (fromMs >= toMs) {
    bad(reply, "from must be before to");
    return null;
  }
  return { interval, bucketMs, fromMs, toMs, limit };
}

function serializeLiqCandle(r: LiqCandleRow): Record<string, unknown> {
  return {
    t: r.t.toISOString(),
    tMs: r.t.getTime(),
    longs: { ntlUsd: r.long_ntl, events: r.long_events, fills: r.long_fills },
    shorts: { ntlUsd: r.short_ntl, events: r.short_events, fills: r.short_fills },
    totalNtlUsd: r.long_ntl + r.short_ntl,
    events: r.long_events + r.short_events,
  };
}

const ZERO_LIQ_TOTALS = {
  longs: { ntlUsd: 0, events: 0, fills: 0, wallets: 0 },
  shorts: { ntlUsd: 0, events: 0, fills: 0, wallets: 0 },
  totalNtlUsd: 0,
  events: 0,
  wallets: 0,
};

function serializeLiqTotals(r: LiqTotalsRow | undefined): Record<string, unknown> {
  if (!r) return ZERO_LIQ_TOTALS;
  return {
    longs: { ntlUsd: r.long_ntl, events: r.long_events, fills: r.long_fills, wallets: r.long_wallets },
    shorts: { ntlUsd: r.short_ntl, events: r.short_events, fills: r.short_fills, wallets: r.short_wallets },
    totalNtlUsd: r.long_ntl + r.short_ntl,
    events: r.long_events + r.short_events,
    wallets: r.wallets,
  };
}

function bridgeWarmingUp(reply: FastifyReply): FastifyReply {
  return reply.code(503).send({
    error: {
      code: "no_bridge_data",
      message: "no bridge deposits scanned yet — the whale tracker is off (WHALES_ENABLED=false) or still on its first poll",
    },
  });
}

function serializeBridgeStatus(s: { last_block: string; updated_at: Date; last_deposit_at: Date | null }): Record<string, unknown> {
  const ageSec = Math.round((Date.now() - s.updated_at.getTime()) / 1000);
  return {
    lastBlock: Number(s.last_block),
    syncedAt: s.updated_at.toISOString(),
    syncAgeSec: ageSec,
    stale: ageSec > 300,
    lastDepositAt: s.last_deposit_at ? s.last_deposit_at.toISOString() : null,
  };
}

function serializeWhale(r: WhaleRow, positions: WalletPositionRow[], marks: Map<string, number | null>): Record<string, unknown> {
  const iso = (d: Date | null): string | null => (d ? d.toISOString() : null);
  // A brand-new account's first ledger entry IS its first deposit of this episode.
  const isNewAccount =
    r.ledger_checked_at === null
      ? null
      : r.ledger_first_at !== null && r.ledger_first_at.getTime() >= r.first_at.getTime() - 600_000;
  const accountAgeDays = r.ledger_first_at ? (Date.now() - r.ledger_first_at.getTime()) / 86_400_000 : null;
  // Held at the first deposit of this episode (null until the tracker's first check).
  const before = r.baseline_positions ? new Map(r.baseline_positions.map((b) => [b.coin, b.szi])) : null;
  let totalNtlUsd = 0;
  const pos = positions.map((p) => {
    const mark = marks.get(p.coin) ?? null;
    const ntlUsd = mark !== null ? Math.abs(p.szi) * mark : null;
    if (ntlUsd !== null) totalNtlUsd += ntlUsd;
    const pnl = mark !== null && p.entry_px !== null ? (mark - p.entry_px) * p.szi : null;
    return {
      coin: p.coin,
      side: p.szi > 0 ? "long" : "short",
      sz: Math.abs(p.szi),
      ntlUsd,
      entryPx: p.entry_px,
      markPx: mark,
      unrealizedPnlUsd: pnl,
      // Not held (or held the other way) when the wallet was funded — a new position, not a top-up.
      openedAfterFunding: before === null ? null : !before.has(p.coin) || Math.sign(before.get(p.coin) ?? 0) !== Math.sign(p.szi),
      updatedAt: p.updated_at.toISOString(),
    };
  });
  return {
    address: r.address,
    deposits: { usd: r.deposited_usd, n: r.n_deposits, firstAt: r.first_at.toISOString(), lastAt: r.last_at.toISOString() },
    flaggedAt: iso(r.flagged_at),
    watchUntil: iso(r.watch_until),
    account: {
      valueUsd: r.account_value,
      totalNtlPosUsd: r.total_ntl_pos,
      checkedAt: iso(r.state_checked_at),
    },
    ledgerFirstAt: iso(r.ledger_first_at),
    isNewAccount,
    accountAgeDays: accountAgeDays !== null ? Math.round(accountAgeDays * 100) / 100 : null,
    firstTradeAt: iso(r.first_trade_at),
    positionedAt: iso(r.positioned_at),
    hasOpenPosition: r.has_position,
    openNtlUsd: positions.length > 0 ? totalNtlUsd : 0,
    positions: pos,
  };
}

function serializeLiqFill(r: LiqFillRow, includeCoin: boolean): Record<string, unknown> {
  return {
    t: r.ts.toISOString(),
    tMs: r.ts.getTime(),
    ...(includeCoin ? { coin: r.coin } : {}),
    side: r.side,
    px: r.px,
    sz: r.sz,
    ntlUsd: r.ntl,
    wallet: r.wallet,
    method: r.method,
    tid: r.tid,
  };
}

function serializeLiqWhale(r: LiqWhaleRow): Record<string, unknown> {
  return {
    id: r.id,
    wallet: r.wallet,
    explorer: `https://app.hyperliquid.xyz/explorer/address/${r.wallet}`,
    detectedAt: r.detected_at.toISOString(),
    from: r.from_ts.toISOString(),
    to: r.to_ts.toISOString(),
    toMs: r.to_ts.getTime(),
    durationSec: Math.round((r.to_ts.getTime() - r.from_ts.getTime()) / 1000),
    ntlUsd: r.ntl,
    thresholdUsd: r.threshold_usd,
    events: r.events,
    fills: r.fills,
    coins: r.coins.map((c) => ({ coin: c.coin, side: c.side, ntlUsd: c.ntl, events: c.events, fills: c.fills })),
    active: r.active,
    delivered: r.delivered,
    deliveryError: r.delivery_error,
  };
}

function serializeVolBar(r: VolBarRow): Record<string, unknown> {
  const ntl = r.buy_ntl + r.sell_ntl;
  const sz = r.buy_sz + r.sell_sz;
  return {
    t: r.t.toISOString(),
    tMs: r.t.getTime(),
    o: r.o,
    h: r.h,
    l: r.l,
    c: r.c,
    ntlUsd: ntl,
    buyNtlUsd: r.buy_ntl,
    sellNtlUsd: r.sell_ntl,
    deltaUsd: r.buy_ntl - r.sell_ntl,
    buySharePct: ntl > 0 ? (r.buy_ntl / ntl) * 100 : null,
    twapNtlUsd: r.twap_ntl,
    sz,
    vwap: sz > 0 ? ntl / sz : null,
    trades: r.buy_n + r.sell_n,
  };
}

function serializeVolSignal(r: VolSignalRow): Record<string, unknown> {
  return {
    id: r.id,
    coin: r.coin,
    firedAt: r.fired_at.toISOString(),
    firedAtMs: r.fired_at.getTime(),
    status: r.status,
    bias: r.bias,
    marketWide: r.market_wide,
    window: { from: r.t_from.toISOString(), to: r.t_to.toISOString(), bars: r.bars },
    volNtlUsd: r.vol_ntl,
    expectedNtlUsd: r.baseline_ntl,
    rvol: r.rvol,
    minBarRvol: r.min_bar_rvol,
    pxFrom: r.px_from,
    pxTo: r.px_to,
    pxMovePct: r.px_move_pct,
    atrPct: r.atr_pct,
    oiFromUsd: r.oi_from,
    oiToUsd: r.oi_to,
    oiChangePct: r.oi_change_pct,
    buySharePct: r.buy_share_pct,
    twapSharePct: r.twap_share_pct,
    confirmedAt: r.confirmed_at ? r.confirmed_at.toISOString() : null,
    breakoutMovePct: r.breakout_move_pct,
    breakoutRvol: r.breakout_rvol,
    closedAt: r.closed_at ? r.closed_at.toISOString() : null,
    message: r.message,
    delivered: r.delivered,
    deliveryError: r.delivery_error,
  };
}

function parseAlertSide(raw: string | undefined): LiqAlertSide | undefined | null {
  if (raw === undefined || raw === "") return undefined;
  return raw === "long" || raw === "short" || raw === "total" ? raw : null;
}

function serializeAlert(r: LiqAlertRow): Record<string, unknown> {
  return {
    id: r.id,
    t: r.ts.toISOString(),
    tMs: r.ts.getTime(),
    coin: r.coin,
    window: r.win,
    side: r.side,
    ntlUsd: r.ntl_usd,
    thresholdUsd: r.threshold_usd,
    pctOfThreshold: r.threshold_usd > 0 ? (r.ntl_usd / r.threshold_usd) * 100 : null,
    events: r.events,
    fills: r.fills,
    longs: { ntlUsd: r.long_ntl, events: r.long_events },
    shorts: { ntlUsd: r.short_ntl, events: r.short_events },
    totalNtlUsd: r.long_ntl + r.short_ntl,
    message: r.message,
    delivered: r.delivered,
    deliveryError: r.delivery_error,
  };
}

// Windows for the liquidation board/summaries: raw-fill exact, so bounded by the
// liq_fills retention rather than tick retention.
function parseLiqWindows(raw: string | undefined, reply: FastifyReply): Array<{ name: string; ms: number }> | null {
  const names = csvParam(raw ?? "");
  if (names === null) {
    bad(reply, "invalid windows — use a comma-separated list like windows=1h,24h");
    return null;
  }
  const list = names.length > 0 ? names : ["1h", "24h"];
  const out: Array<{ name: string; ms: number }> = [];
  for (const name of list) {
    const ms = parseWindow(name);
    if (ms === null || ms < 5 * 60_000) {
      bad(reply, `invalid window "${name}" — use e.g. 15m, 1h, 4h, 24h, 7d`);
      return null;
    }
    if (ms > config.liqRetentionDays * 86_400_000) {
      bad(reply, `window "${name}" exceeds liquidation fill retention (${config.liqRetentionDays}d) — use the candle endpoints for longer ranges`);
      return null;
    }
    out.push({ name, ms });
  }
  return out;
}

interface EmaCoinEntry {
  coin: string;
  asOf: string | null;
  px: number | null;
  oiUsd: number | null;
  tfs: Record<string, unknown>;
}

const tfAsc = (a: string, b: string): number => (EMA_TF_MS[a] ?? Infinity) - (EMA_TF_MS[b] ?? Infinity);

// One coin's EMA block: per timeframe, the raw EMAs plus the screener columns —
// price distance from each EMA and the fastest-vs-slowest spread (the "cross"
// column: positive = fast EMA above slow, a sign flip = golden/death cross).
function buildEmaEntry(coin: string, tick: TickRow | null, rows: EmaStateApiRow[]): EmaCoinEntry {
  const px = tick?.px ?? null;
  const byTf = new Map<string, EmaStateApiRow[]>();
  for (const r of rows) {
    const list = byTf.get(r.tf) ?? [];
    list.push(r);
    byTf.set(r.tf, list);
  }
  const tfs: Record<string, unknown> = {};
  for (const tf of [...byTf.keys()].sort(tfAsc)) {
    const tfRows = byTf.get(tf)!.sort((a, b) => a.period - b.period);
    const ema: Record<string, number | null> = {};
    const pxVsEmaPct: Record<string, number | null> = {};
    let nCandles = 0;
    for (const r of tfRows) {
      ema[String(r.period)] = r.ema;
      pxVsEmaPct[String(r.period)] = pctChange(px, r.ema);
      nCandles = Math.max(nCandles, r.n_candles);
    }
    const fast = tfRows[0]!;
    const slow = tfRows[tfRows.length - 1]!;
    const lastOpenMs = Number(tfRows[0]!.last_open_ms);
    tfs[tf] = {
      t: new Date(lastOpenMs).toISOString(), // open of the last closed candle applied
      tMs: lastOpenMs,
      nCandles,
      ema,
      pxVsEmaPct,
      spreadPct: tfRows.length > 1 ? pctChange(fast.ema, slow.ema) : null,
    };
  }
  return { coin, asOf: tick ? tick.ts.toISOString() : null, px, oiUsd: tick?.oi_usd ?? null, tfs };
}

interface EmaBase {
  asOf: string | null;
  periods: number[];
  timeframes: string[];
  entries: EmaCoinEntry[];
}

// Assembled once per cache window: every live coin's EMA block joined with its
// latest price, sorted by open interest like the universe list.
async function emaBase(): Promise<EmaBase> {
  const [rows, ticks] = await Promise.all([emaStates(), latestTicks()]);
  const tickBy = new Map(ticks.map((t) => [t.coin, t]));
  const byCoin = new Map<string, EmaStateApiRow[]>();
  const periods = new Set<number>();
  const timeframes = new Set<string>();
  for (const r of rows) {
    const list = byCoin.get(r.coin) ?? [];
    list.push(r);
    byCoin.set(r.coin, list);
    periods.add(r.period);
    timeframes.add(r.tf);
  }
  const entries = [...byCoin.entries()].map(([coin, coinRows]) => buildEmaEntry(coin, tickBy.get(coin) ?? null, coinRows));
  entries.sort((a, b) => {
    if (a.oiUsd === null && b.oiUsd === null) return a.coin.localeCompare(b.coin);
    if (a.oiUsd === null) return 1;
    if (b.oiUsd === null) return -1;
    return b.oiUsd - a.oiUsd || a.coin.localeCompare(b.coin);
  });
  let asOf: string | null = null;
  for (const t of ticks) if (!asOf || t.ts.toISOString() > asOf) asOf = t.ts.toISOString();
  return {
    asOf,
    periods: [...periods].sort((a, b) => a - b),
    timeframes: [...timeframes].sort(tfAsc),
    entries,
  };
}

// Comma-separated list param: [] = absent (no filter), null = present but
// invalid. Repeated params (?tf=1h&tf=4h) arrive as arrays — treat them as a list.
function csvParam(v: unknown): string[] | null {
  const s = Array.isArray(v) ? v.join(",") : typeof v === "string" ? v : v === undefined ? "" : null;
  if (s === null) return null;
  if (s === "") return [];
  const names = s
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x !== "");
  return names.length > 0 ? names : null;
}

function emasWarmingUp(reply: FastifyReply): FastifyReply {
  return reply.code(503).send({
    error: {
      code: "no_ema_data",
      message:
        "no EMA state yet — the collector seeds it from Hyperliquid's candle history shortly after first boot (check EMAS_ENABLED and collector logs)",
    },
  });
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
      "GET /v1/perps/emas?tf=1h,4h,12h,1d&coins=&minOiUsd=0&limit=500",
      "GET /v1/perps/:coin",
      "GET /v1/perps/:coin/recap?window=24h",
      "GET /v1/perps/:coin/emas",
      "GET /v1/perps/:coin/candles?interval=5m|1h|1d&from=&to=&limit=300",
      "GET /v1/perps/:coin/funding-history?from=&to=&limit=168",
      "GET /v1/perps/positioning?sort=traders|pctLong&dir=desc&limit=250",
      "GET /v1/perps/:coin/positioning",
      "GET /v1/perps/:coin/positioning/history?from=&to=&limit=288",
      "GET /v1/perps/liquidations?windows=1h,24h&sort=ntl|events&dir=desc&limit=250",
      "GET /v1/perps/:coin/liquidations?interval=5m|15m|1h|4h|12h|1d&from=&to=&limit=300",
      "GET /v1/perps/:coin/liquidations/recent?limit=50",
      "GET /v1/market/snapshot",
      "GET /v1/market/oi?interval=5m|1h|1d&from=&to=&limit=300",
      "GET /v1/market/funding?interval=1h&smooth=8h&from=&to=&limit=300",
      "GET /v1/market/liquidations?interval=5m|15m|1h|4h|12h|1d&from=&to=&limit=300",
      "GET /v1/market/liquidations/recent?limit=50",
      "GET /v1/market/liquidations/whales?wallet=&coin=&since=&minNtlUsd=&active=&limit=50",
      "GET /v1/perps/volume?bars=3&sort=rvol|volume&limit=50&minBarUsd=0",
      "GET /v1/perps/:coin/volume?interval=1m|5m|1h&from=&to=&limit=300",
      "GET /v1/signals?coin=&status=open|confirmed|expired&since=&marketWide=&limit=100",
      "GET /v1/alerts?coin=&window=&side=long|short|total&since=&limit=100",
      "GET /v1/alerts/rules",
      "GET /v1/whales/new?window=1h&minUsd=1000000&positioned=true|false&newOnly=false&limit=100",
      "GET /v1/bridge/deposits?window=24h&minUsd=100000&limit=100",
      "GET /v1/whales/alerts?kind=funded|positioned&address=&since=&limit=100",
    ],
  }));

  // Volume board: every coin's relative volume over the last N 5m bars against
  // its own baseline, with the flatness/positioning numbers the detector uses —
  // "who is trading abnormally right now", sorted by rvol.
  app.get("/v1/perps/volume", async (req, reply) => {
    const q = req.query as Query;
    const bars = parseLimit(q.bars, 3, 12);
    if (bars === null) return bad(reply, "invalid bars — use 1..12 (5m bars)");
    const limit = parseLimit(q.limit, 50, 500);
    if (limit === null) return bad(reply, "invalid limit");
    const sortRaw = q.sort ?? "rvol";
    if (sortRaw !== "rvol" && sortRaw !== "volume") return bad(reply, `invalid sort "${sortRaw}" — use rvol|volume`);
    let minBarUsd = 0;
    if (q.minBarUsd !== undefined && q.minBarUsd !== "") {
      minBarUsd = Number(q.minBarUsd);
      if (!Number.isFinite(minBarUsd) || minBarUsd < 0) return bad(reply, "invalid minBarUsd");
    }
    const rows = await cached(`vol:board:${bars}`, 30_000, async () => {
      const nowMs = Date.now();
      const w = windowBuckets(bars, nowMs);
      const [ctx, oiStart, oiNow] = await Promise.all([volumeContext(null, bars, nowMs), oiUsdAt(null, w.startMs), oiUsdAt(null, nowMs)]);
      const out: Array<Record<string, unknown> & { rvol: number | null; volNtlUsd: number; avgBarUsd: number }> = [];
      for (const [coin, c] of ctx) {
        const m = volumeMetrics(c, oiStart.get(coin) ?? null, oiNow.get(coin) ?? null, nowMs);
        if (!m) continue;
        out.push({
          coin,
          rvol: m.rvol,
          minBarRvol: m.minBarRvol,
          volNtlUsd: m.volNtl,
          expectedNtlUsd: m.expectedNtl,
          avgBarUsd: m.avgBarUsd,
          bars: m.bars,
          from: new Date(m.tFromMs).toISOString(),
          pxMovePct: m.moveP,
          atrPct: m.atrPct,
          moveAtr: m.moveAtr,
          buySharePct: m.buySharePct,
          twapSharePct: m.twapSharePct,
          oiChangePct: m.oiChangePct,
          historyHours: Math.round(m.historyHours * 10) / 10,
        });
      }
      return { asOf: new Date(nowMs).toISOString(), windowFrom: new Date(w.startMs).toISOString(), out };
    });
    const ranked = rows.out
      .filter((r) => r.avgBarUsd >= minBarUsd)
      .sort((a, b) => (sortRaw === "rvol" ? (b.rvol ?? -1) - (a.rvol ?? -1) : b.volNtlUsd - a.volNtlUsd) || a.coin!.toString().localeCompare(b.coin!.toString()))
      .slice(0, limit);
    return { asOf: rows.asOf, bars, windowFrom: rows.windowFrom, count: ranked.length, data: ranked };
  });

  // Per-coin volume bars from the trade tape (buy/sell split, TWAP share, VWAP).
  app.get("/v1/perps/:coin/volume", async (req, reply) => {
    const { coin } = req.params as { coin: string };
    const asset = await resolveCoin(coin);
    if (!asset) return notFound(reply, `unknown coin "${coin}"`);
    const q = req.query as Query;
    const interval = q.interval ?? "5m";
    const spec = VOL_TABLES[interval];
    if (!spec) return bad(reply, `invalid interval "${interval}" — use ${Object.keys(VOL_TABLES).join(", ")}`);
    const limit = parseLimit(q.limit, 300, CANDLE_LIMIT_MAX);
    if (limit === null) return bad(reply, "invalid limit");
    const from = parseTimeMs(q.from);
    const to = parseTimeMs(q.to);
    if (from === null || to === null) return bad(reply, "invalid from/to — use epoch ms, epoch seconds, or an ISO timestamp");
    const toMs = to ?? Date.now() + spec.ms;
    const fromMs = from ?? toMs - limit * spec.ms;
    if (fromMs >= toMs) return bad(reply, "from must be before to");
    const rows = await volCandles(asset.coin, spec.table, fromMs, toMs, limit);
    return { coin: asset.coin, interval, count: rows.length, data: rows.map(serializeVolBar) };
  });

  // Volume-leading-price signals fired by the collector, newest first.
  app.get("/v1/signals", async (req, reply) => {
    const q = req.query as Query;
    const limit = parseLimit(q.limit, 100, 1000);
    if (limit === null) return bad(reply, "invalid limit");
    const since = parseTimeMs(q.since);
    if (since === null) return bad(reply, "invalid since — use epoch ms, epoch seconds, or an ISO timestamp");
    if (q.status !== undefined && q.status !== "" && !["open", "confirmed", "expired"].includes(q.status)) {
      return bad(reply, `invalid status "${q.status}" — use open|confirmed|expired`);
    }
    let marketWide: boolean | undefined;
    if (q.marketWide !== undefined && q.marketWide !== "") {
      if (q.marketWide !== "true" && q.marketWide !== "false") return bad(reply, 'marketWide must be "true" or "false"');
      marketWide = q.marketWide === "true";
    }
    let coin: string | undefined;
    if (q.coin !== undefined && q.coin !== "") {
      const asset = await resolveCoin(q.coin);
      coin = asset ? asset.coin : q.coin;
    }
    const rows = await listVolSignals({
      ...(coin !== undefined ? { coin } : {}),
      ...(q.status ? { status: q.status } : {}),
      ...(since !== undefined ? { sinceMs: since } : {}),
      ...(marketWide !== undefined ? { marketWide } : {}),
      limit,
    });
    return { count: rows.length, data: rows.map(serializeVolSignal) };
  });

  // Liquidation threshold alerts fired by the collector (see LIQ_ALERT_RULES),
  // newest first. Every alert is one threshold crossing of one rule.
  app.get("/v1/alerts", async (req, reply) => {
    const q = req.query as Query;
    const limit = parseLimit(q.limit, 100, 1000);
    if (limit === null) return bad(reply, "invalid limit");
    const since = parseTimeMs(q.since);
    if (since === null) return bad(reply, "invalid since — use epoch ms, epoch seconds, or an ISO timestamp");
    const side = parseAlertSide(q.side);
    if (side === null) return bad(reply, `invalid side "${q.side}" — use long|short|total`);
    let coin: string | undefined;
    if (q.coin !== undefined && q.coin !== "") {
      const asset = await resolveCoin(q.coin);
      coin = asset ? asset.coin : q.coin; // unknown names still filter (they just match nothing)
    }
    if (q.window !== undefined && q.window !== "" && parseWindow(q.window) === null) {
      return bad(reply, `invalid window "${q.window}" — use e.g. 15m, 1h, 24h`);
    }
    const rows = await listLiqAlerts({
      ...(coin !== undefined ? { coin } : {}),
      ...(q.window ? { win: q.window } : {}),
      ...(side !== undefined ? { side } : {}),
      ...(since !== undefined ? { sinceMs: since } : {}),
      limit,
    });
    return { count: rows.length, data: rows.map(serializeAlert) };
  });

  // The configured rules with their live window values and armed state: the
  // "alert board" — how close each coin is to its next alert.
  app.get("/v1/alerts/rules", async () => {
    const rules = await cached("alerts:rules", CACHE_MS, async () => {
      const rows = await loadLiqAlertRules();
      const byCoin = new Map<string, LiqAlertRuleRow[]>();
      for (const r of rows) {
        let list = byCoin.get(r.coin);
        if (!list) byCoin.set(r.coin, (list = []));
        list.push(r);
      }
      const out: Array<Record<string, unknown>> = [];
      for (const [coin, coinRules] of byCoin) {
        const totals = await liqWindowTotals(
          coin,
          coinRules.map((r) => Number(r.window_ms)),
        );
        for (const r of coinRules) {
          const t = totals.get(Number(r.window_ms));
          const slice = t ? sideSlice(t, r.side) : { ntl: 0, events: 0, fills: 0 };
          out.push({
            coin: r.coin,
            window: r.win,
            side: r.side,
            thresholdUsd: r.threshold_usd,
            current: { ntlUsd: slice.ntl, events: slice.events, fills: slice.fills },
            pctOfThreshold: r.threshold_usd > 0 ? (slice.ntl / r.threshold_usd) * 100 : null,
            active: r.active,
            lastEvalAt: r.last_eval_at ? r.last_eval_at.toISOString() : null,
            lastFiredAt: r.last_fired_at ? r.last_fired_at.toISOString() : null,
          });
        }
      }
      return out;
    });
    const evalAts = rules.map((r) => r.lastEvalAt as string | null).filter((t): t is string => t !== null);
    const lastEval = evalAts.length > 0 ? evalAts.sort().at(-1)! : null;
    return {
      asOf: new Date().toISOString(),
      lastEvalAt: lastEval,
      evaluatorStale: lastEval === null || Date.now() - Date.parse(lastEval) > 5 * 60_000,
      count: rules.length,
      rules,
    };
  });

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
      // Cached briefly so uptime monitors hammering /health don't each scan ticks;
      // the age math runs per request, so staleness detection stays live.
      const { last, coins } = await cached("health:ticks", CACHE_MS, async () => {
        const { rows } = await pool.query<{ last: Date | null; coins: number }>(
          `select max(ts) as last, count(distinct coin)::int as coins
           from perp_ticks where ts >= now() - interval '10 minutes'`,
        );
        return { last: rows[0]?.last ?? null, coins: rows[0]?.coins ?? 0 };
      });
      const tickAgeSec = last ? Math.round((Date.now() - last.getTime()) / 1000) : null;
      return {
        ok: true,
        lastTickAt: last ? last.toISOString() : null,
        tickAgeSec,
        ticksStale: tickAgeSec === null || tickAgeSec > 120,
        liveCoins: coins,
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

  // The whole EMA board in one payload: every live coin × timeframe × period,
  // with the screener columns precomputed against the latest price.
  app.get("/v1/perps/emas", async (req, reply) => {
    const q = req.query as Query;
    const tfNames = csvParam(q.tf);
    if (tfNames === null) return bad(reply, "invalid tf — use a comma-separated list like tf=1h,4h");
    let tfFilter: Set<string> | null = null;
    if (tfNames.length > 0) {
      for (const name of tfNames) {
        if (!(name in EMA_TF_MS)) return bad(reply, `invalid tf "${name}" — use any of ${Object.keys(EMA_TF_MS).join(", ")}`);
      }
      tfFilter = new Set(tfNames);
    }
    const coinNames = csvParam(q.coins);
    if (coinNames === null) return bad(reply, "invalid coins — use a comma-separated list like coins=BTC,ETH");
    let coinFilter: Set<string> | null = null;
    if (coinNames.length > 0) {
      coinFilter = new Set<string>();
      for (const raw of coinNames) {
        const asset = await resolveCoin(raw);
        if (!asset) return notFound(reply, `no perp named "${raw}"`);
        coinFilter.add(asset.coin);
      }
    }
    const limit = parseLimit(q.limit, 500, 500);
    if (limit === null) return bad(reply, "invalid limit");
    const minOiUsd = q.minOiUsd !== undefined && q.minOiUsd !== "" ? Number(q.minOiUsd) : 0;
    if (!Number.isFinite(minOiUsd)) return bad(reply, "invalid minOiUsd");

    const base = await cached("emas:all", CACHE_MS, emaBase);
    if (base.entries.length === 0) return emasWarmingUp(reply);
    const data = base.entries
      .filter((e) => (coinFilter ? coinFilter.has(e.coin) : true))
      .filter((e) => (e.oiUsd ?? 0) >= minOiUsd)
      .slice(0, limit)
      .map((e) =>
        tfFilter
          ? { ...e, tfs: Object.fromEntries(Object.entries(e.tfs).filter(([tf]) => tfFilter.has(tf))) }
          : e,
      );
    return {
      asOf: base.asOf,
      periods: base.periods,
      timeframes: tfFilter ? base.timeframes.filter((tf) => tfFilter.has(tf)) : base.timeframes,
      count: data.length,
      data,
    };
  });

  app.get("/v1/perps/:coin/emas", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const [rows, tickRaw] = await Promise.all([emaStatesFor(asset.coin), singleTick(asset.coin)]);
    if (rows.length === 0) return emasWarmingUp(reply);
    // Same staleness contract as the board endpoint: px is live or null, never stale.
    const tick = tickRaw && Date.now() - tickRaw.ts.getTime() <= LATEST_MAX_AGE_MS ? tickRaw : null;
    return {
      ...buildEmaEntry(asset.coin, tick, rows),
      periods: [...new Set(rows.map((r) => r.period))].sort((a, b) => a - b),
      timeframes: [...new Set(rows.map((r) => r.tf))].sort(tfAsc),
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

  // Liquidation board: per-coin totals over trailing windows ("how much got
  // liquidated in the last hour/day"), plus the venue-wide sum per window.
  app.get("/v1/perps/liquidations", async (req, reply) => {
    const q = req.query as Query;
    const windows = parseLiqWindows(q.windows, reply);
    if (!windows) return reply;
    const sortRaw = q.sort ?? "ntl";
    if (sortRaw !== "ntl" && sortRaw !== "events") return bad(reply, `invalid sort "${sortRaw}" — use ntl|events`);
    const dir = q.dir ?? "desc";
    if (dir !== "asc" && dir !== "desc") return bad(reply, 'dir must be "asc" or "desc"');
    const limit = parseLimit(q.limit, 250, 500);
    if (limit === null) return bad(reply, "invalid limit");

    const [perWindow, lastAt] = await Promise.all([
      Promise.all(windows.map((w) => cached(`liq:totals:${w.ms}`, CACHE_MS, () => liqTotals(w.ms, null)))),
      cached("liq:lastAt", CACHE_MS, lastLiqAt),
    ]);
    const byWindow = perWindow.map((rows) => new Map(rows.map((r) => [r.coin, r])));
    const coins = new Set<string>();
    for (const rows of perWindow) for (const r of rows) coins.add(r.coin);
    const rank = (r: LiqTotalsRow | undefined): number =>
      r ? (sortRaw === "ntl" ? r.long_ntl + r.short_ntl : r.long_events + r.short_events) : 0;
    const sign = dir === "desc" ? -1 : 1;
    const ranked = [...coins]
      .sort((a, b) => (rank(byWindow[0]!.get(a)) - rank(byWindow[0]!.get(b))) * sign || a.localeCompare(b))
      .slice(0, limit);
    const venue = await Promise.all(windows.map((w) => liqVenueTotals(w.ms)));
    const totals = Object.fromEntries(windows.map((w, i) => [w.name, serializeLiqTotals(venue[i])]));
    return {
      windows: windows.map((w) => w.name),
      lastLiqAt: lastAt ? lastAt.toISOString() : null,
      count: ranked.length,
      totals,
      data: ranked.map((coin) => ({
        coin,
        windows: Object.fromEntries(windows.map((w, i) => [w.name, serializeLiqTotals(byWindow[i]!.get(coin))])),
      })),
    };
  });

  // Per-coin liquidation histogram (the "aggregated liquidations" pane series).
  // Buckets with no liquidations are omitted — chart against the timestamps.
  app.get("/v1/perps/:coin/liquidations", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const range = liqRange(req.query as Query, reply);
    if (!range) return reply;
    const rows = await liqCandles(asset.coin, range.bucketMs, range.fromMs, range.toMs, range.limit);
    return { coin: asset.coin, interval: range.interval, count: rows.length, data: rows.map(serializeLiqCandle) };
  });

  // Raw liquidation prints, newest first (wallet-level detail is public on-chain data).
  app.get("/v1/perps/:coin/liquidations/recent", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const limit = parseLimit((req.query as Query).limit, 50, 500);
    if (limit === null) return bad(reply, "invalid limit");
    const rows = await recentLiqFills(asset.coin, limit);
    return { coin: asset.coin, count: rows.length, data: rows.map((r) => serializeLiqFill(r, false)) };
  });

  // The ticker recap: liquidations by side, price change + all-time-high check,
  // OI change + record-high check, and long/short trader deltas over one trailing
  // window — the numbers a ticker line is written from, in one request.
  app.get("/v1/perps/:coin/recap", async (req, reply) => {
    const { coin: coinRaw } = req.params as { coin: string };
    const asset = await resolveCoin(coinRaw);
    if (!asset) return notFound(reply, `no perp named "${coinRaw}"`);
    const q = req.query as Query;
    const windowRaw = q.window ?? "24h";
    const windowMs = parseWindow(windowRaw);
    if (windowMs === null) return bad(reply, `invalid window "${windowRaw}" — use e.g. 1h, 4h, 24h, 7d`);
    if (windowMs < 5 * 60_000) return bad(reply, "window must be at least 5m");
    const maxDays = Math.min(config.rawRetentionDays, config.liqRetentionDays);
    if (windowMs > maxDays * 86_400_000) {
      return bad(reply, `window exceeds raw tick retention (${maxDays}d) — use the candle endpoints for longer ranges`);
    }
    const recap = await cached(`recap:${asset.coin}:${windowMs}`, CACHE_MS, () => buildRecap(asset, windowRaw, windowMs));
    if (!recap.ok) return noRecentData(reply);
    return recap.body;
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

    const [liq1h, liq24h] = await Promise.all([
      liqTotals(3_600_000, asset.coin),
      liqTotals(86_400_000, asset.coin),
    ]);

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
      liquidations: {
        "1h": serializeLiqTotals(liq1h[0]),
        "24h": serializeLiqTotals(liq24h[0]),
      },
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

  // Venue-wide liquidation histogram: per-coin buckets summed across every coin.
  app.get("/v1/market/liquidations", async (req, reply) => {
    const range = liqRange(req.query as Query, reply);
    if (!range) return reply;
    const rows = await liqCandles(null, range.bucketMs, range.fromMs, range.toMs, range.limit);
    return { interval: range.interval, count: rows.length, data: rows.map(serializeLiqCandle) };
  });

  // Large liquidated accounts: every wallet whose liquidation burst crossed
  // LIQ_WHALE_THRESHOLD, with the per-coin breakdown. Newest first.
  app.get("/v1/market/liquidations/whales", async (req, reply) => {
    const q = req.query as Query;
    const limit = parseLimit(q.limit, 50, 500);
    if (limit === null) return bad(reply, "invalid limit");
    const since = parseTimeMs(q.since);
    if (since === null) return bad(reply, "invalid since — use epoch ms, epoch seconds, or an ISO timestamp");
    let minNtlUsd: number | undefined;
    if (q.minNtlUsd !== undefined && q.minNtlUsd !== "") {
      minNtlUsd = Number(q.minNtlUsd);
      if (!Number.isFinite(minNtlUsd) || minNtlUsd < 0) return bad(reply, "invalid minNtlUsd");
    }
    let active: boolean | undefined;
    if (q.active !== undefined && q.active !== "") {
      if (q.active !== "true" && q.active !== "false") return bad(reply, 'active must be "true" or "false"');
      active = q.active === "true";
    }
    let wallet: string | undefined;
    if (q.wallet !== undefined && q.wallet !== "") {
      if (!/^0x[0-9a-fA-F]{40}$/.test(q.wallet)) return bad(reply, "invalid wallet — expected a 0x-prefixed 40-hex address");
      wallet = q.wallet.toLowerCase();
    }
    let coin: string | undefined;
    if (q.coin !== undefined && q.coin !== "") {
      const asset = await resolveCoin(q.coin);
      coin = asset ? asset.coin : q.coin;
    }
    const rows = await listLiqWhales({
      ...(wallet !== undefined ? { wallet } : {}),
      ...(coin !== undefined ? { coin } : {}),
      ...(since !== undefined ? { sinceMs: since } : {}),
      ...(minNtlUsd !== undefined ? { minNtlUsd } : {}),
      ...(active !== undefined ? { active } : {}),
      limit,
    });
    return { count: rows.length, data: rows.map(serializeLiqWhale) };
  });

  app.get("/v1/market/liquidations/recent", async (req, reply) => {
    const limit = parseLimit((req.query as Query).limit, 50, 500);
    if (limit === null) return bad(reply, "invalid limit");
    const rows = await recentLiqFills(null, limit);
    return { count: rows.length, data: rows.map((r) => serializeLiqFill(r, true)) };
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

  // ---- Whale discovery ----

  // Wallets whose Arbitrum→Hyperliquid bridge deposits over the trailing window
  // total at least minUsd, with what the watcher knows about each on Hyperliquid:
  // account value, account age (first ledger entry), first fill on the tape,
  // and their open positions marked at the live price.
  app.get("/v1/whales/new", async (req, reply) => {
    const q = req.query as Query;
    const windowRaw = q.window ?? "1h";
    const windowMs = parseWindow(windowRaw);
    const maxWindowMs = config.bridgeRetentionDays * 86_400_000;
    if (windowMs === null || windowMs < 60_000 || windowMs > maxWindowMs) {
      return bad(reply, `invalid window "${windowRaw}" — use 1m…${config.bridgeRetentionDays}d`);
    }
    const minUsd = q.minUsd === undefined || q.minUsd === "" ? config.whaleMinUsd : Number(q.minUsd);
    if (!Number.isFinite(minUsd) || minUsd < 0) return bad(reply, "invalid minUsd");
    let positioned: boolean | null = null;
    if (q.positioned !== undefined && q.positioned !== "") {
      if (q.positioned !== "true" && q.positioned !== "false") return bad(reply, 'positioned must be "true" or "false"');
      positioned = q.positioned === "true";
    }
    if (q.newOnly !== undefined && q.newOnly !== "" && q.newOnly !== "true" && q.newOnly !== "false") {
      return bad(reply, 'newOnly must be "true" or "false"');
    }
    const newOnly = q.newOnly === "true";
    const limit = parseLimit(q.limit, 100, 500);
    if (limit === null) return bad(reply, "invalid limit");

    const status = await cached("bridge:status", CACHE_MS, bridgeStatus);
    if (!status) return bridgeWarmingUp(reply);
    const { rows, positions, ticks } = await cached(
      `whales:${windowMs}:${minUsd}:${positioned}:${newOnly}:${limit}`,
      CACHE_MS,
      async () => {
        const rows = await whaleWallets(windowMs, minUsd, positioned, newOnly, limit);
        const [positions, ticks] = await Promise.all([openPositionsFor(rows.map((r) => r.address)), latestTicks()]);
        return { rows, positions, ticks };
      },
    );
    const marks = new Map(ticks.map((t) => [t.coin, t.mark_px]));
    const byAddress = new Map<string, WalletPositionRow[]>();
    for (const p of positions) {
      const list = byAddress.get(p.address);
      if (list) list.push(p);
      else byAddress.set(p.address, [p]);
    }
    return {
      window: windowRaw,
      minUsd,
      asOf: new Date().toISOString(),
      bridge: serializeBridgeStatus(status),
      count: rows.length,
      data: rows.map((r) => serializeWhale(r, byAddress.get(r.address) ?? [], marks)),
    };
  });

  // Whale alert history, newest first: what was (or would have been) pushed to the webhook.
  app.get("/v1/whales/alerts", async (req, reply) => {
    const q = req.query as Query;
    const limit = parseLimit(q.limit, 100, 1000);
    if (limit === null) return bad(reply, "invalid limit");
    const since = parseTimeMs(q.since);
    if (since === null) return bad(reply, "invalid since — use epoch ms, epoch seconds, or an ISO timestamp");
    if (q.kind !== undefined && q.kind !== "" && q.kind !== "funded" && q.kind !== "positioned") {
      return bad(reply, `invalid kind "${q.kind}" — use funded|positioned`);
    }
    const address = q.address?.trim().toLowerCase();
    if (address !== undefined && address !== "" && !/^0x[0-9a-f]{40}$/.test(address)) return bad(reply, "invalid address");
    const rows = await listWhaleAlerts({
      ...(q.kind ? { kind: q.kind } : {}),
      ...(address ? { address } : {}),
      ...(since !== undefined ? { sinceMs: since } : {}),
      limit,
    });
    return {
      webhook: config.liqAlertWebhookUrl !== "",
      events: config.whaleAlertEvents,
      count: rows.length,
      data: rows.map((r) => ({
        id: r.id,
        t: r.ts.toISOString(),
        tMs: r.ts.getTime(),
        kind: r.kind,
        address: r.address,
        depositedUsd: r.deposited_usd,
        accountValueUsd: r.account_value,
        totalNtlPosUsd: r.total_ntl_pos,
        isNewAccount: r.is_new_account,
        ledgerFirstAt: r.ledger_first_at ? r.ledger_first_at.toISOString() : null,
        positions: r.positions,
        opened: r.opened,
        message: r.message,
        delivered: r.delivered,
        deliveryError: r.delivery_error,
      })),
    };
  });

  // Raw bridge deposit feed, newest first (public on-chain data).
  app.get("/v1/bridge/deposits", async (req, reply) => {
    const q = req.query as Query;
    const windowRaw = q.window ?? "24h";
    const windowMs = parseWindow(windowRaw);
    const maxWindowMs = config.bridgeRetentionDays * 86_400_000;
    if (windowMs === null || windowMs < 60_000 || windowMs > maxWindowMs) {
      return bad(reply, `invalid window "${windowRaw}" — use 1m…${config.bridgeRetentionDays}d`);
    }
    const minUsd = q.minUsd === undefined || q.minUsd === "" ? 100_000 : Number(q.minUsd);
    if (!Number.isFinite(minUsd) || minUsd < 0) return bad(reply, "invalid minUsd");
    const limit = parseLimit(q.limit, 100, 1_000);
    if (limit === null) return bad(reply, "invalid limit");
    const status = await cached("bridge:status", CACHE_MS, bridgeStatus);
    if (!status) return bridgeWarmingUp(reply);
    const rows = await cached(`bridge:deposits:${windowMs}:${minUsd}:${limit}`, CACHE_MS, () =>
      recentBridgeDeposits(windowMs, minUsd, limit),
    );
    return {
      window: windowRaw,
      minUsd,
      recordFloorUsd: config.bridgeMinRecordUsd,
      bridge: serializeBridgeStatus(status),
      count: rows.length,
      data: rows.map((r) => ({
        t: r.ts.toISOString(),
        tMs: r.ts.getTime(),
        address: r.address,
        usdc: r.usdc,
        txHash: r.tx_hash,
        logIndex: r.log_index,
        block: Number(r.block_number),
      })),
    };
  });
}
