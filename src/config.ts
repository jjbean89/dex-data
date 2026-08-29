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
