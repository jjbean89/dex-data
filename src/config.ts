import { log } from "./log.js";

function numEnv(name: string, def: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return def;
  const v = Number(raw);
  if (!Number.isFinite(v)) throw new Error(`env ${name} must be a number, got "${raw}"`);
  return v;
}

export type Role = "all" | "api" | "collector";

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
