import { log } from "./log.js";

function numEnv(name: string, def: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return def;
  const v = Number(raw);
  if (!Number.isFinite(v)) throw new Error(`env ${name} must be a number, got "${raw}"`);
  return v;
}

export type Role = "all" | "api" | "collector";

// Timeframes the EMA tracker can maintain: Hyperliquid-native candle intervals
// that are whole-hour multiples (epoch/UTC-aligned), so every one of them can be
// advanced from the hourly close stream after the initial per-timeframe seed.
export const EMA_TF_MS: Record<string, number> = {
  "1h": 3_600_000,
  "2h": 7_200_000,
  "4h": 14_400_000,
  "8h": 28_800_000,
  "12h": 43_200_000,
  "1d": 86_400_000,
};

function listEnv(name: string, def: string): string[] {
  const raw = process.env[name];
  const src = raw === undefined || raw.trim() === "" ? def : raw;
  return [
    ...new Set(
      src
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s !== ""),
    ),
  ];
}

// Liquidation alert rules: "COIN:WINDOW:THRESHOLD[:SIDES]" entries, comma-separated.
//   COIN      Hyperliquid coin name as it prints on the tape (BTC, ETH, kPEPE …)
//   WINDOW    trailing window, e.g. 15m, 1h, 24h (1d also accepted)
//   THRESHOLD USD notional; k/m/b suffixes allowed (15M = 15_000_000)
//   SIDES     long | short | total | both (default both = long and short evaluated
//             separately; total = long + short as one number)
// Each configured side becomes its own rule with its own armed state.
export type LiqAlertSide = "long" | "short" | "total";

export interface LiqAlertRule {
  coin: string;
  window: string;
  windowMs: number;
  side: LiqAlertSide;
  thresholdUsd: number;
}

export const LIQ_ALERT_DEFAULT_RULES =
  "BTC:15m:15M,BTC:1h:40M,BTC:24h:100M,ETH:15m:10M,ETH:1h:15M,ETH:24h:100M";

export function parseUsd(raw: string): number | null {
  const m = /^\$?([\d_]*\.?\d+(?:e\d+)?)\s*([kmb])?$/i.exec(raw.trim());
  if (!m) return null;
  const n = Number(m[1]!.replace(/_/g, ""));
  if (!Number.isFinite(n) || n <= 0) return null;
  const mult = m[2] === undefined ? 1 : { k: 1e3, m: 1e6, b: 1e9 }[m[2].toLowerCase()]!;
  return n * mult;
}

export function parseWindowMs(raw: string): number | null {
  const m = /^(\d{1,3})(m|h|d)$/.exec(raw.trim());
  if (!m) return null;
  const n = parseInt(m[1]!, 10);
  if (n === 0) return null;
  return n * (m[2] === "m" ? 60_000 : m[2] === "h" ? 3_600_000 : 86_400_000);
}

function usdEnv(name: string, def: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") return def;
  if (/^0+(\.0+)?$/.test(raw.trim())) return 0;
  const v = parseUsd(raw);
  if (v === null) throw new Error(`env ${name} must be a USD amount like 10M, 500k or 10000000, got "${raw}"`);
  return v;
}

function windowEnv(name: string, def: string): { name: string; ms: number } {
  const raw = process.env[name];
  const src = raw === undefined || raw.trim() === "" ? def : raw.trim();
  const ms = parseWindowMs(src);
  if (ms === null) throw new Error(`env ${name} must be a window like 15m, 1h or 24h, got "${raw}"`);
  return { name: src, ms };
}

export function parseLiqAlertRules(raw: string): LiqAlertRule[] {
  const out = new Map<string, LiqAlertRule>();
  for (const entry of raw.split(",").map((s) => s.trim()).filter((s) => s !== "")) {
    const parts = entry.split(":").map((s) => s.trim());
    if (parts.length < 3 || parts.length > 4) {
      throw new Error(`LIQ_ALERT_RULES: "${entry}" — expected COIN:WINDOW:THRESHOLD[:SIDES]`);
    }
    const [coin, window, thresholdRaw, sidesRaw = "both"] = parts as [string, string, string, string?];
    if (coin === "") throw new Error(`LIQ_ALERT_RULES: "${entry}" — empty coin`);
    const windowMs = parseWindowMs(window);
    if (windowMs === null || windowMs < 60_000) {
      throw new Error(`LIQ_ALERT_RULES: "${entry}" — invalid window "${window}" (use e.g. 15m, 1h, 24h)`);
    }
    const thresholdUsd = parseUsd(thresholdRaw);
    if (thresholdUsd === null) {
      throw new Error(`LIQ_ALERT_RULES: "${entry}" — invalid threshold "${thresholdRaw}" (use e.g. 15M, 500k, 15000000)`);
    }
    const sidesKey = (sidesRaw ?? "both").toLowerCase();
    const sides: LiqAlertSide[] =
      sidesKey === "both" ? ["long", "short"] : sidesKey === "long" || sidesKey === "short" || sidesKey === "total" ? [sidesKey] : [];
    if (sides.length === 0) {
      throw new Error(`LIQ_ALERT_RULES: "${entry}" — invalid sides "${sidesRaw}" (use long, short, total, or both)`);
    }
    for (const side of sides) {
      out.set(`${coin}|${window}|${side}`, { coin, window, windowMs, side, thresholdUsd });
    }
  }
  return [...out.values()];
}

export const config = {
  role: (process.env.ROLE ?? "all") as Role,
  databaseUrl: process.env.DATABASE_URL ?? "",
  hlApiUrl: process.env.HL_API_URL ?? "https://api.hyperliquid.xyz",
  port: numEnv("PORT", 3000),
  host: process.env.HOST ?? "0.0.0.0",

  pollIntervalMs: numEnv("POLL_INTERVAL_MS", 15_000),
  fundingSyncIntervalMs: numEnv("FUNDING_SYNC_INTERVAL_MS", 3_600_000),
  fundingReqDelayMs: numEnv("FUNDING_REQ_DELAY_MS", 2_000),
  fundingBackfillDays: numEnv("FUNDING_BACKFILL_DAYS", 30),
  rollupIntervalMs: numEnv("ROLLUP_INTERVAL_MS", 60_000),
  rawRetentionDays: numEnv("RAW_RETENTION_DAYS", 14),
  candles5mRetentionDays: numEnv("CANDLES_5M_RETENTION_DAYS", 180),

  // Long/short trader tracking (trades WebSocket + per-wallet position ledger).
  positionsEnabled: process.env.POSITIONS_ENABLED !== "false",
  hlWsUrl: process.env.HL_WS_URL ?? "wss://api.hyperliquid.xyz/ws",
  positionsFlushMs: numEnv("POSITIONS_FLUSH_MS", 5_000),
  bootstrapDelayMs: numEnv("BOOTSTRAP_DELAY_MS", 400),
  positionsSnapshotMs: numEnv("POSITIONS_SNAPSHOT_MS", 300_000),
  reverifyIntervalMs: numEnv("REVERIFY_INTERVAL_MS", 21_600_000),
  reverifyBatch: numEnv("REVERIFY_BATCH", 2_000),

  // Liquidation recorder (classifies tape trades via paced userFillsByTime
  // verification and keeps per-coin liquidation fills + candles).
  liquidationsEnabled: process.env.LIQUIDATIONS_ENABLED !== "false",
  liqVerifyDelayMs: numEnv("LIQ_VERIFY_DELAY_MS", 3_000),
  liqVerifyLagMs: numEnv("LIQ_VERIFY_LAG_MS", 8_000),
  liqWalletCooldownMs: numEnv("LIQ_WALLET_COOLDOWN_MS", 60_000),
  liqBackfillHours: numEnv("LIQ_BACKFILL_HOURS", 6),
  liqBackfillWallets: numEnv("LIQ_BACKFILL_WALLETS", 250),
  liqRetentionDays: numEnv("LIQ_RETENTION_DAYS", 90),

  // Liquidation threshold alerts (evaluated by the collector from liq_fills).
  liqAlertsEnabled: process.env.LIQ_ALERTS_ENABLED !== "false",
  liqAlertRules: parseLiqAlertRules(process.env.LIQ_ALERT_RULES?.trim() || LIQ_ALERT_DEFAULT_RULES),
  liqAlertIntervalMs: numEnv("LIQ_ALERT_INTERVAL_MS", 30_000),
  liqAlertRearmPct: numEnv("LIQ_ALERT_REARM_PCT", 80),
  liqAlertWebhookUrl: process.env.LIQ_ALERT_WEBHOOK_URL?.trim() ?? "",
  liqAlertWebhookFormat: (process.env.LIQ_ALERT_WEBHOOK_FORMAT ?? "auto") as "auto" | "json" | "discord" | "slack",

  // Large liquidated accounts: collect any wallet whose liquidation burst
  // (fills across all coins with no gap longer than the window) crosses the
  // threshold. 0 disables.
  liqWhaleThresholdUsd: usdEnv("LIQ_WHALE_THRESHOLD", 10_000_000),
  liqWhaleWindow: windowEnv("LIQ_WHALE_WINDOW", "1h"),
  liqWhaleNotify: process.env.LIQ_WHALE_NOTIFY !== "false",

  // Volume bars from the trade tape (per-bar notional, buy/sell split, TWAP share).
  volumeEnabled: process.env.VOLUME_ENABLED !== "false",
  volFlushMs: numEnv("VOL_FLUSH_MS", 10_000),
  vol1mRetentionDays: numEnv("VOL_1M_RETENTION_DAYS", 30),

  // "Volume leading price" detector: abnormal volume on flat price with
  // positioning confirmation, evaluated on 5m bars.
  volSignalsEnabled: process.env.VOL_SIGNALS_ENABLED !== "false",
  volSignalCoins: listEnv("VOL_SIGNAL_COINS", ""), // empty = every live coin
  volSignalBars: numEnv("VOL_SIGNAL_BARS", 3),
  volSignalRvol: numEnv("VOL_SIGNAL_RVOL", 4),
  volSignalMinBarRvol: numEnv("VOL_SIGNAL_MIN_BAR_RVOL", 1.5),
  volSignalMinBarUsd: usdEnv("VOL_SIGNAL_MIN_BAR_USD", 250_000),
  volSignalMaxMoveAtr: numEnv("VOL_SIGNAL_MAX_MOVE_ATR", 1.5),
  volSignalMinOiPct: numEnv("VOL_SIGNAL_MIN_OI_PCT", 1),
  volSignalMinImbalancePct: numEnv("VOL_SIGNAL_MIN_IMBALANCE_PCT", 60),
  volSignalBreakoutAtr: numEnv("VOL_SIGNAL_BREAKOUT_ATR", 3),
  volSignalMaxCoins: numEnv("VOL_SIGNAL_MAX_COINS", 15),
  volSignalExpireMin: numEnv("VOL_SIGNAL_EXPIRE_MIN", 120),
  volSignalMinHistoryHours: numEnv("VOL_SIGNAL_MIN_HISTORY_HOURS", 6),
  volSignalNotify: process.env.VOL_SIGNAL_NOTIFY !== "false",

  // Moving-average tracker (EMAs per coin per timeframe, from HL's official candles).
  emasEnabled: process.env.EMAS_ENABLED !== "false",
  emaTimeframes: listEnv("EMA_TIMEFRAMES", "1h,4h,12h,1d").sort(
    (a, b) => (EMA_TF_MS[a] ?? Infinity) - (EMA_TF_MS[b] ?? Infinity),
  ),
  emaPeriods: [...new Set(listEnv("EMA_PERIODS", "21,200").map(Number))].sort((a, b) => a - b),
  emaReqDelayMs: numEnv("EMA_REQ_DELAY_MS", 2_000),
  emaReseedDays: numEnv("EMA_RESEED_DAYS", 7),

  // HyperTracker census seed (optional; runs once when a key is present).
  hypertrackerApiKey: process.env.HYPERTRACKER_API_KEY ?? "",
  hypertrackerBaseUrl: process.env.HYPERTRACKER_BASE_URL ?? "https://ht-api.coinmarketman.com/api",
  hypertrackerReqDelayMs: numEnv("HYPERTRACKER_REQ_DELAY_MS", 1_500),
  hypertrackerRetryMs: numEnv("HYPERTRACKER_RETRY_MS", 10_800_000),
  hypertrackerDeepHistory: process.env.HYPERTRACKER_DEEP_HISTORY === "true",
  hypertrackerDeepStart: process.env.HYPERTRACKER_DEEP_START ?? "2025-04-04T00:00:00Z",

  // Whale discovery: Arbitrum bridge deposit watcher + per-wallet HL enrichment.
  whalesEnabled: process.env.WHALES_ENABLED !== "false",
  arbitrumRpcUrl: process.env.ARBITRUM_RPC_URL ?? "https://arb1.arbitrum.io/rpc",
  arbitrumUsdcAddress: (process.env.ARBITRUM_USDC_ADDRESS ?? "0xaf88d065e77c8cC2239327C5EDb3A432268e5831").toLowerCase(),
  hlBridgeAddress: (process.env.HL_BRIDGE_ADDRESS ?? "0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7").toLowerCase(),
  bridgePollMs: numEnv("BRIDGE_POLL_MS", 15_000),
  bridgeConfirmations: numEnv("BRIDGE_CONFIRMATIONS", 10),
  bridgeBackfillHours: numEnv("BRIDGE_BACKFILL_HOURS", 6),
  bridgeMinRecordUsd: numEnv("BRIDGE_MIN_RECORD_USD", 1_000),
  bridgeRetentionDays: numEnv("BRIDGE_RETENTION_DAYS", 90),
  whaleMinUsd: numEnv("WHALE_MIN_USD", 1_000_000),
  whaleWindowHours: numEnv("WHALE_WINDOW_HOURS", 1),
  whaleWatchHours: numEnv("WHALE_WATCH_HOURS", 24),
  whaleWatchPollMs: numEnv("WHALE_WATCH_POLL_MS", 60_000),
  // "positioned" alerts need this much notional in positions opened after funding
  // (new coin or side flip vs. what the wallet held at its first deposit); 0 = any size.
  whalePositionMinUsd: numEnv("WHALE_POSITION_MIN_USD", 1_000_000),
  // Whale alerts go to the same webhook as the liquidation alerts (LIQ_ALERT_WEBHOOK_URL).
  whaleAlertEvents: listEnv("WHALE_ALERT_EVENTS", "funded,positioned"),

  pgSslNoVerify: process.env.PG_SSL_NO_VERIFY === "true",
};

export function assertConfig(): void {
  if (!config.databaseUrl) {
    throw new Error("DATABASE_URL is required (postgres://user:pass@host:port/db)");
  }
  if (!["all", "api", "collector"].includes(config.role)) {
    throw new Error(`ROLE must be one of all|api|collector, got "${config.role}"`);
  }
  if (config.pollIntervalMs < 2_000) {
    throw new Error("POLL_INTERVAL_MS below 2000ms would burn the Hyperliquid rate-limit budget");
  }
  // The alert webhook is shared by liquidation and whale alerts.
  if (!["auto", "json", "discord", "slack"].includes(config.liqAlertWebhookFormat)) {
    throw new Error(`LIQ_ALERT_WEBHOOK_FORMAT must be one of auto|json|discord|slack, got "${config.liqAlertWebhookFormat}"`);
  }
  if (config.liqAlertWebhookUrl !== "" && !/^https?:\/\//i.test(config.liqAlertWebhookUrl)) {
    throw new Error("LIQ_ALERT_WEBHOOK_URL must be an http(s) URL");
  }
  if (config.liquidationsEnabled) {
    if (config.liqVerifyDelayMs < 1_000) {
      throw new Error("LIQ_VERIFY_DELAY_MS below 1000ms would burn the Hyperliquid rate-limit budget (userFillsByTime is weight 20)");
    }
    if (config.liqRetentionDays < 1) throw new Error("LIQ_RETENTION_DAYS must be at least 1");
    if (config.liqBackfillHours < 0 || config.liqBackfillHours > 168) {
      throw new Error("LIQ_BACKFILL_HOURS must be between 0 and 168");
    }
    if (config.liqAlertsEnabled) {
      if (config.liqAlertIntervalMs < 5_000) throw new Error("LIQ_ALERT_INTERVAL_MS must be at least 5000");
      if (config.liqAlertRearmPct <= 0 || config.liqAlertRearmPct > 100) {
        throw new Error("LIQ_ALERT_REARM_PCT must be between 1 and 100");
      }
      for (const r of config.liqAlertRules) {
        if (r.windowMs > config.liqRetentionDays * 86_400_000) {
          throw new Error(`LIQ_ALERT_RULES: window "${r.window}" for ${r.coin} exceeds LIQ_RETENTION_DAYS (${config.liqRetentionDays}d)`);
        }
      }
    }
    if (config.liqWhaleThresholdUsd > 0) {
      if (config.liqWhaleWindow.ms < 60_000 || config.liqWhaleWindow.ms > 86_400_000) {
        throw new Error("LIQ_WHALE_WINDOW must be between 1m and 24h");
      }
    }
  }
  if (config.volumeEnabled) {
    if (config.volFlushMs < 1_000) throw new Error("VOL_FLUSH_MS must be at least 1000");
    if (config.vol1mRetentionDays < 1) throw new Error("VOL_1M_RETENTION_DAYS must be at least 1");
  }
  if (config.volumeEnabled && config.volSignalsEnabled) {
    if (!Number.isInteger(config.volSignalBars) || config.volSignalBars < 1 || config.volSignalBars > 12) {
      throw new Error("VOL_SIGNAL_BARS must be an integer between 1 and 12 (5m bars)");
    }
    if (config.volSignalRvol <= 1) throw new Error("VOL_SIGNAL_RVOL must be above 1");
    if (config.volSignalMinBarRvol < 0 || config.volSignalMinBarRvol > config.volSignalRvol) {
      throw new Error("VOL_SIGNAL_MIN_BAR_RVOL must be between 0 and VOL_SIGNAL_RVOL");
    }
    if (config.volSignalMaxMoveAtr <= 0) throw new Error("VOL_SIGNAL_MAX_MOVE_ATR must be positive");
    if (config.volSignalBreakoutAtr <= config.volSignalMaxMoveAtr) {
      throw new Error("VOL_SIGNAL_BREAKOUT_ATR must be larger than VOL_SIGNAL_MAX_MOVE_ATR");
    }
    if (config.volSignalMinImbalancePct < 50 || config.volSignalMinImbalancePct > 100) {
      throw new Error("VOL_SIGNAL_MIN_IMBALANCE_PCT must be between 50 and 100");
    }
    if (config.volSignalMinOiPct < 0) throw new Error("VOL_SIGNAL_MIN_OI_PCT must be non-negative");
    if (config.volSignalMaxCoins < 1) throw new Error("VOL_SIGNAL_MAX_COINS must be at least 1");
    if (config.volSignalExpireMin < 5) throw new Error("VOL_SIGNAL_EXPIRE_MIN must be at least 5");
  }
  if (config.whalesEnabled) {
    if (!/^https?:\/\//.test(config.arbitrumRpcUrl)) {
      throw new Error("ARBITRUM_RPC_URL must be an http(s) JSON-RPC endpoint (or set WHALES_ENABLED=false)");
    }
    for (const [name, v] of [["ARBITRUM_USDC_ADDRESS", config.arbitrumUsdcAddress], ["HL_BRIDGE_ADDRESS", config.hlBridgeAddress]]) {
      if (!/^0x[0-9a-f]{40}$/.test(v!)) throw new Error(`${name} must be a 20-byte hex address`);
    }
    if (config.bridgePollMs < 2_000) throw new Error("BRIDGE_POLL_MS below 2000ms would hammer the Arbitrum RPC");
    if (config.bridgeConfirmations < 0 || config.bridgeConfirmations > 1_000) {
      throw new Error("BRIDGE_CONFIRMATIONS must be between 0 and 1000");
    }
    if (config.bridgeBackfillHours < 0 || config.bridgeBackfillHours > 168) {
      throw new Error("BRIDGE_BACKFILL_HOURS must be between 0 and 168");
    }
    if (config.whaleMinUsd <= 0) throw new Error("WHALE_MIN_USD must be positive");
    if (config.whaleWindowHours <= 0 || config.whaleWindowHours > 168) {
      throw new Error("WHALE_WINDOW_HOURS must be between 0 and 168");
    }
    if (config.whaleWatchHours < 1 || config.whaleWatchHours > 720) {
      throw new Error("WHALE_WATCH_HOURS must be between 1 and 720");
    }
    if (config.whaleWatchPollMs < 10_000) throw new Error("WHALE_WATCH_POLL_MS below 10000ms would burn clearinghouseState budget");
    if (!(config.whalePositionMinUsd >= 0)) throw new Error("WHALE_POSITION_MIN_USD must be zero or positive");
    if (config.bridgeRetentionDays < 1) throw new Error("BRIDGE_RETENTION_DAYS must be at least 1");
    for (const e of config.whaleAlertEvents) {
      if (e !== "funded" && e !== "positioned") {
        throw new Error(`WHALE_ALERT_EVENTS: "${e}" is not an event — use any of funded, positioned`);
      }
    }
  }
  if (config.emasEnabled) {
    for (const tf of config.emaTimeframes) {
      if (!(tf in EMA_TF_MS)) {
        throw new Error(`EMA_TIMEFRAMES: "${tf}" is not supported — use any of ${Object.keys(EMA_TF_MS).join(", ")}`);
      }
    }
    if (config.emaTimeframes.length === 0) throw new Error("EMA_TIMEFRAMES must name at least one timeframe");
    if (config.emaPeriods.length === 0) throw new Error("EMA_PERIODS must name at least one period");
    for (const p of config.emaPeriods) {
      if (!Number.isInteger(p) || p < 2 || p > 1_200) {
        throw new Error(`EMA_PERIODS: "${p}" is invalid — use integers between 2 and 1200 (e.g. 21,200)`);
      }
    }
    if (config.emaReqDelayMs < 500) {
      throw new Error("EMA_REQ_DELAY_MS below 500ms risks the Hyperliquid rate-limit budget during seeding sweeps");
    }
  }
  // Railway bills every byte through the public TCP proxy as egress; the collector
  // talks to Postgres constantly, so that misconfiguration quietly gets expensive.
  try {
    const host = new URL(config.databaseUrl).hostname;
    if (/\.rlwy\.net$|\.railway\.app$/i.test(host)) {
      log(
        "config",
        `WARNING: DATABASE_URL host "${host}" is Railway's public proxy — all DB traffic is billed as egress. ` +
          "Use the private-network reference (${{Postgres.DATABASE_URL}} → postgres.railway.internal) instead.",
      );
    }
  } catch {
    // non-URL conninfo strings are fine
  }
}
