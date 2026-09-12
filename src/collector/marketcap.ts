import { CoinGeckoError, coingecko, type CgMarket } from "../coingecko/client.js";
import { config } from "../config.js";
import {
  DAY_MS,
  firstBackfilledDay,
  insertSnapshot,
  latestSnapshot,
  missingDays,
  rollupDaily,
  upsertBackfillCandles,
  type BackfillCandle,
  type McapSnapshot,
} from "../db/marketcap.js";
import { opsEvent } from "../db/ops.js";
import { sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";

// Global market-cap index recorder (TOTAL / TOTAL2 / TOTAL3 / OTHERS).
//
// Every MCAP_POLL_MS: /global (total cap + volume) and /coins/markets (the
// top-N constituents, BTC and ETH among them) → one snapshot row → the touched
// UTC days' candles are rebuilt from snapshots. Two requests per poll.
//
// History from before the recorder existed is backfilled close-only, once at
// boot and again whenever a day goes missing (downtime): BTC, ETH and the top-N
// coins from /coins/{id}/market_chart; the total from /global/market_cap_chart
// on pro plans, otherwise reconstructed from the top MCAP_APPROX_COINS coins'
// histories scaled to today's /global coverage. OTHERS history uses today's
// top-N membership throughout (TradingView's index is rebalanced periodically;
// the difference is small and disappears once days are recorded live).

const BACKFILL_CHECK_MS = 6 * 3_600_000;
const BACKFILL_RETRY_MS = 30 * 60_000;
const BACKFILL_GAP_LOOKBACK_DAYS = 365;
const FORWARD_FILL_MAX_DAYS = 3;
const MAX_MARKETS_PAGE = 250;

type DayMap = Map<number, { mc: number; vol: number }>;

function dayStart(ms: number): number {
  return Math.floor(ms / DAY_MS) * DAY_MS;
}

interface TopPick {
  btc: CgMarket;
  eth: CgMarket;
  top: CgMarket[];
}

function pickTop(markets: CgMarket[]): TopPick {
  const ignore = new Set(config.mcapIgnoreIds);
  const ranked = markets.filter((m) => typeof m.market_cap === "number" && m.market_cap > 0);
  const btc = ranked.find((m) => m.id === "bitcoin");
  const eth = ranked.find((m) => m.id === "ethereum");
  if (!btc || !eth) throw new Error("coins/markets page is missing bitcoin or ethereum");
  const top = ranked.filter((m) => !ignore.has(m.id)).slice(0, config.mcapTopN);
  if (top.length < config.mcapTopN) {
    throw new Error(`coins/markets returned only ${top.length} eligible coins for the top ${config.mcapTopN}`);
  }
  return { btc, eth, top };
}

function marketsPageSize(): number {
  return Math.min(MAX_MARKETS_PAGE, config.mcapTopN + config.mcapIgnoreIds.length + 5);
}

export async function pollMarketcap(): Promise<McapSnapshot> {
  const global = await coingecko.global();
  const markets = await coingecko.markets(marketsPageSize());
  const totalUsd = global.data.total_market_cap.usd;
  const totalVol = global.data.total_volume.usd;
  if (typeof totalUsd !== "number" || !(totalUsd > 0) || typeof totalVol !== "number") {
    throw new Error("global response has no usd market cap");
  }
  const { btc, eth, top } = pickTop(markets);
  const sum = (rows: CgMarket[], f: (m: CgMarket) => number | null): number => rows.reduce((a, m) => a + (f(m) ?? 0), 0);
  const snapshot: McapSnapshot = {
    ts: new Date(),
    total_usd: totalUsd,
    total_vol_usd: totalVol,
    btc_usd: btc.market_cap ?? 0,
    btc_vol_usd: btc.total_volume ?? 0,
    eth_usd: eth.market_cap ?? 0,
    eth_vol_usd: eth.total_volume ?? 0,
    top_usd: sum(top, (m) => m.market_cap),
    top_vol_usd: sum(top, (m) => m.total_volume),
    top_n: config.mcapTopN,
    top_ids: top.map((m) => m.id),
  };
  await insertSnapshot(snapshot);
  return snapshot;
}

// CoinGecko's daily points sit at 00:00 UTC and carry the value at that instant —
// the close of the *previous* day. The final point is live and is skipped.
function dailyPoints(mcs: Array<[number, number]>, vols: Array<[number, number]>): DayMap {
  const volByTs = new Map(vols.map(([ts, v]) => [ts, v]));
  const out: DayMap = new Map();
  for (let i = 0; i < mcs.length - 1; i++) {
    const [ts, mc] = mcs[i]!;
    const day = dayStart(ts);
    if (ts - day > 6 * 3_600_000) continue; // sub-daily granularity — not a midnight point
    if (!(mc > 0)) continue;
    out.set(day - DAY_MS, { mc, vol: volByTs.get(ts) ?? 0 });
  }
  return out;
}

// Carries the previous day's values across short holes in a coin's history.
function forwardFill(m: DayMap): DayMap {
  if (m.size === 0) return m;
  const days = [...m.keys()].sort((a, b) => a - b);
  const out: DayMap = new Map();
  let prev: { mc: number; vol: number } | null = null;
  let prevDay = 0;
  for (const day of days) {
    if (prev !== null) {
      const gap = (day - prevDay) / DAY_MS - 1;
      if (gap > 0 && gap <= FORWARD_FILL_MAX_DAYS) {
        for (let d = prevDay + DAY_MS; d < day; d += DAY_MS) out.set(d, prev);
      }
    }
    const v = m.get(day)!;
    out.set(day, v);
    prev = v;
    prevDay = day;
  }
  return out;
}

function addInto(sum: DayMap, m: DayMap): void {
  for (const [day, v] of m) {
    const cur = sum.get(day);
    if (cur) {
      cur.mc += v.mc;
      cur.vol += v.vol;
    } else {
      sum.set(day, { mc: v.mc, vol: v.vol });
    }
  }
}

interface BackfillResult {
  source: "coingecko" | "approx";
  rows: number;
  requests: number;
  firstDay: number | null;
}

export async function backfillMarketcap(isStopped: () => boolean, topIds: string[]): Promise<BackfillResult | null> {
  let days = config.mcapBackfillDays;
  let requests = 0;
  const charts = new Map<string, DayMap>();

  async function chart(id: string): Promise<DayMap> {
    const hit = charts.get(id);
    if (hit) return hit;
    let res;
    try {
      requests++;
      res = await coingecko.marketChart(id, days);
    } catch (err) {
      // Public/demo keys can't read more than a year back — fall back once, for every coin.
      if (days === "max" && err instanceof CoinGeckoError && (err.status === 401 || err.status === 403 || err.status === 400)) {
        log("mcap", `history "max" refused (HTTP ${err.status}) — this key reads 365 days; set COINGECKO_PLAN=pro for full history`);
        days = 365;
        requests++;
        res = await coingecko.marketChart(id, days);
      } else {
        throw err;
      }
    }
    const m = forwardFill(dailyPoints(res.market_caps, res.total_volumes));
    charts.set(id, m);
    return m;
  }

  // Constituents first: BTC, ETH, then today's top-N (BTC/ETH usually among them).
  const constituentIds = [...new Set(["bitcoin", "ethereum", ...topIds])];
  for (const id of constituentIds) {
    if (isStopped()) return null;
    await chart(id);
  }

  // The total: exact on pro plans, otherwise reconstructed.
  let total: DayMap | null = null;
  let source: "coingecko" | "approx" = "coingecko";
  if (config.coingeckoPlan === "pro") {
    try {
      requests++;
      const g = await coingecko.globalMarketCapChart(days);
      total = forwardFill(dailyPoints(g.market_cap_chart.market_cap, g.market_cap_chart.volume));
    } catch (err) {
      if (!(err instanceof CoinGeckoError) || err.status === 429 || err.status >= 500) throw err;
      log("mcap", `global/market_cap_chart unavailable on this key (HTTP ${err.status}) — falling back to the approximate total`);
    }
  }
  if (total === null) {
    if (config.mcapApproxCoins === 0) {
      log("mcap", "no total history source: /global/market_cap_chart needs a pro key and MCAP_APPROX_COINS=0 — history starts today");
      return null;
    }
    source = "approx";
    const universe: CgMarket[] = [];
    for (let page = 1; universe.length < config.mcapApproxCoins; page++) {
      if (isStopped()) return null;
      requests++;
      const batch = await coingecko.markets(Math.min(MAX_MARKETS_PAGE, config.mcapApproxCoins - universe.length), page);
      universe.push(...batch.filter((m) => typeof m.market_cap === "number" && m.market_cap > 0));
      if (batch.length < MAX_MARKETS_PAGE) break;
    }
    requests++;
    const g = await coingecko.global();
    const capNow = universe.reduce((a, m) => a + (m.market_cap ?? 0), 0);
    const volNow = universe.reduce((a, m) => a + (m.total_volume ?? 0), 0);
    const capCoverage = capNow > 0 && g.data.total_market_cap.usd ? g.data.total_market_cap.usd / capNow : 1;
    const volCoverage = volNow > 0 && g.data.total_volume.usd ? g.data.total_volume.usd / volNow : 1;
    log(
      "mcap",
      `approximating total history from ${universe.length} coins (they carry ${(100 / capCoverage).toFixed(1)}% of /global today; scaling ×${capCoverage.toFixed(4)}) — ${universe.length - charts.size} chart fetches at ${config.coingeckoReqDelayMs}ms`,
    );
    const sum: DayMap = new Map();
    let fetched = 0;
    for (const m of universe) {
      if (isStopped()) return null;
      try {
        addInto(sum, await chart(m.id));
      } catch (err) {
        logErr("mcap", `history for ${m.id} failed — skipping it in the approximate total`, err);
      }
      fetched++;
      if (fetched % 50 === 0) log("mcap", `approximate total: ${fetched}/${universe.length} coin histories fetched`);
    }
    total = new Map([...sum].map(([day, v]) => [day, { mc: v.mc * capCoverage, vol: v.vol * volCoverage }]));
  }

  const btc = charts.get("bitcoin")!;
  const eth = charts.get("ethereum")!;
  const topCharts = topIds.map((id) => charts.get(id)).filter((m): m is DayMap => m !== undefined);
  const today = dayStart(Date.now());
  const rows: BackfillCandle[] = [];
  let firstDay: number | null = null;
  for (const [day, t] of [...total].sort((a, b) => a[0] - b[0])) {
    if (day >= today || !(t.mc > 0)) continue;
    const b = btc.get(day);
    const e = eth.get(day);
    rows.push({ idx: "total", tMs: day, c: t.mc, v: t.vol, source });
    if (!b) continue;
    rows.push({ idx: "total2", tMs: day, c: t.mc - b.mc, v: t.vol - b.vol, source });
    if (!e) continue;
    rows.push({ idx: "total3", tMs: day, c: t.mc - b.mc - e.mc, v: t.vol - b.vol - e.vol, source });
    let topMc = 0;
    let topVol = 0;
    for (const m of topCharts) {
      const v = m.get(day);
      if (v) {
        topMc += v.mc;
        topVol += v.vol;
      }
    }
    rows.push({ idx: "others", tMs: day, c: t.mc - topMc, v: t.vol - topVol, source });
    if (firstDay === null) firstDay = day;
  }
  let written = 0;
  for (let i = 0; i < rows.length; i += 5_000) {
    written += await upsertBackfillCandles(rows.slice(i, i + 5_000));
  }
  return { source, rows: written, requests, firstDay };
}

// True until history has been imported once, then whenever a day inside the
// imported range goes missing (the recorder was down and no candle was built).
async function backfillNeeded(): Promise<boolean> {
  const yesterday = dayStart(Date.now()) - DAY_MS;
  const first = await firstBackfilledDay("total");
  if (first === null) return true;
  const from = Math.max(first.getTime(), yesterday - BACKFILL_GAP_LOOKBACK_DAYS * DAY_MS);
  if (from > yesterday) return false;
  return (await missingDays("total", from, yesterday)) > 0;
}

export function startMarketcap(isStopped: () => boolean): () => Promise<void> {
  let polls = 0;
  let lastPollMs = 0;
  let lastLog = 0;

  async function sleepUntil(deadline: number): Promise<void> {
    while (!isStopped() && Date.now() < deadline) {
      await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
    }
  }

  async function pollLoop(): Promise<void> {
    // Catch up the candles of days that were live when the process last stopped.
    try {
      await rollupDaily(Date.now() - 2 * DAY_MS);
    } catch (err) {
      logErr("mcap", "boot rollup failed", err);
    }
    while (!isStopped()) {
      const started = Date.now();
      try {
        const s = await pollMarketcap();
        polls++;
        await rollupDaily(lastPollMs > 0 ? Math.min(lastPollMs, started) : started - DAY_MS);
        lastPollMs = started;
        if (polls === 1 || Date.now() - lastLog >= 3_600_000) {
          const b = (n: number): string => `$${(n / 1e9).toFixed(2)}B`;
          log(
            "mcap",
            `total ${b(s.total_usd)}, total3 ${b(s.total_usd - s.btc_usd - s.eth_usd)}, others ${b(s.total_usd - s.top_usd)} (top ${s.top_n}: ${s.top_ids.join(", ")}) — poll #${polls}`,
          );
          lastLog = Date.now();
        }
      } catch (err) {
        logErr("mcap", "poll failed", err);
      }
      await sleepUntil(started + config.mcapPollMs);
    }
  }

  async function backfillLoop(): Promise<void> {
    while (!isStopped()) {
      let retryMs = BACKFILL_CHECK_MS;
      try {
        if (await backfillNeeded()) {
          const latest = await latestSnapshot();
          if (!latest) {
            retryMs = 15_000; // the first poll hasn't landed yet — it names the top-N
          } else {
            const started = Date.now();
            const r = await backfillMarketcap(isStopped, latest.top_ids);
            if (r) {
              const msg =
                `history backfilled: ${r.rows} candle rows (${r.source}${r.firstDay !== null ? `, from ${new Date(r.firstDay).toISOString().slice(0, 10)}` : ""}) ` +
                `in ${r.requests} requests over ${Math.round((Date.now() - started) / 1000)}s`;
              log("mcap", msg);
              void opsEvent("mcap", "info", msg);
            }
          }
        }
      } catch (err) {
        retryMs = BACKFILL_RETRY_MS;
        logErr("mcap", "history backfill failed — retrying later", err);
        void opsEvent("mcap", "warn", `history backfill failed: ${err instanceof Error ? err.message : String(err)}`);
      }
      await sleepUntil(Date.now() + retryMs);
    }
  }

  const loops = [pollLoop()];
  if (config.mcapBackfill) loops.push(backfillLoop());
  log(
    "mcap",
    `recorder started: CoinGecko ${config.coingeckoPlan} plan via ${new URL(config.coingeckoApiUrl).host}, poll ${Math.round(config.mcapPollMs / 60_000)}min, top ${config.mcapTopN}, backfill ${config.mcapBackfill ? `${config.mcapBackfillDays === "max" ? "full history" : `${config.mcapBackfillDays}d`} (${config.coingeckoPlan === "pro" ? "exact" : `approx from ${config.mcapApproxCoins} coins`})` : "off"}`,
  );
  return async () => {
    await Promise.allSettled(loops);
    log("mcap", "recorder stopped");
  };
}
