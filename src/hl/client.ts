import { config } from "../config.js";
import type { Candle, FundingHistoryEntry, MetaAndAssetCtxs } from "./types.js";

const MAX_RETRIES = 3;
const REQUEST_TIMEOUT_MS = 15_000;

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

// POST to /info with retries on 429/5xx/network errors and a hard per-request timeout.
async function info<T>(body: Record<string, unknown>): Promise<T> {
  let lastErr: unknown;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (attempt > 0) await sleep(1_000 * 2 ** (attempt - 1) + Math.random() * 500);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const res = await fetch(`${config.hlApiUrl}/info`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (res.status === 429 || res.status >= 500) {
        lastErr = new Error(`HL info ${String(body.type)}: HTTP ${res.status}`);
        continue;
      }
      if (!res.ok) {
        throw new Error(`HL info ${String(body.type)}: HTTP ${res.status} ${await res.text()}`);
      }
      return (await res.json()) as T;
    } catch (err) {
      if (err instanceof Error && (err.name === "AbortError" || err instanceof TypeError)) {
        lastErr = err; // timeout or network failure — retry
        continue;
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(`HL info ${String(body.type)}: retries exhausted`);
}

export const hl = {
  metaAndAssetCtxs: () => info<MetaAndAssetCtxs>({ type: "metaAndAssetCtxs" }),

  // Settled funding, oldest-first from startTime, max 500 rows per call.
  fundingHistory: (coin: string, startTime: number, endTime?: number) =>
    info<FundingHistoryEntry[]>({
      type: "fundingHistory",
      coin,
      startTime,
      ...(endTime !== undefined ? { endTime } : {}),
    }),

  // Up to ~5000 most recent candles per interval are retained by HL.
  candleSnapshot: (coin: string, interval: string, startTime: number, endTime: number) =>
    info<Candle[]>({ type: "candleSnapshot", req: { coin, interval, startTime, endTime } }),
};
