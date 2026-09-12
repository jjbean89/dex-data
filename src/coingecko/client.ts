import { config } from "../config.js";
import { sleep } from "../hl/client.js";

// CoinGecko REST client. Every request runs through one paced queue so the
// market-cap poll and a history backfill can't double up on the per-minute
// budget (public ≈ 5–15 req/min, demo 30/min, pro 500/min). 429s honour
// Retry-After; 5xx/network errors back off and retry.

const MAX_RETRIES = 4;
const REQUEST_TIMEOUT_MS = 20_000;

export interface CgGlobal {
  data: {
    active_cryptocurrencies: number;
    total_market_cap: Record<string, number>;
    total_volume: Record<string, number>;
    market_cap_percentage: Record<string, number>;
    updated_at: number; // epoch seconds
  };
}

export interface CgMarket {
  id: string;
  symbol: string;
  name: string;
  market_cap: number | null;
  market_cap_rank: number | null;
  total_volume: number | null;
  current_price: number | null;
}

export interface CgMarketChart {
  prices: Array<[number, number]>;
  market_caps: Array<[number, number]>;
  total_volumes: Array<[number, number]>;
}

export interface CgGlobalChart {
  market_cap_chart: {
    market_cap: Array<[number, number]>;
    volume: Array<[number, number]>;
  };
}

export class CoinGeckoError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly body: string,
  ) {
    super(message);
    this.name = "CoinGeckoError";
  }
}

let chain: Promise<unknown> = Promise.resolve();
let lastRequestAt = 0;

function enqueue<T>(fn: () => Promise<T>): Promise<T> {
  const run = chain.then(fn);
  chain = run.catch(() => undefined);
  return run;
}

async function get<T>(path: string, params: Record<string, string | number>): Promise<T> {
  const url = new URL(`${config.coingeckoApiUrl}${path}`);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, String(v));
  const headers: Record<string, string> = { accept: "application/json" };
  if (config.coingeckoApiKey !== "") {
    headers[config.coingeckoPlan === "pro" ? "x-cg-pro-api-key" : "x-cg-demo-api-key"] = config.coingeckoApiKey;
  }
  return enqueue(async () => {
    let lastErr: unknown;
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      const wait = lastRequestAt + config.coingeckoReqDelayMs - Date.now();
      if (wait > 0) await sleep(wait);
      lastRequestAt = Date.now();
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
      try {
        const res = await fetch(url, { headers, signal: controller.signal });
        const body = await res.text();
        if (res.status === 429 || res.status >= 500) {
          lastErr = new CoinGeckoError(`CoinGecko ${path}: HTTP ${res.status}`, res.status, body);
          const retryAfter = Number(res.headers.get("retry-after"));
          await sleep(Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1_000 : 10_000 * 2 ** attempt);
          continue;
        }
        if (!res.ok) {
          throw new CoinGeckoError(`CoinGecko ${path}: HTTP ${res.status} ${body.slice(0, 300)}`, res.status, body);
        }
        return JSON.parse(body) as T;
      } catch (err) {
        if (err instanceof Error && (err.name === "AbortError" || err instanceof TypeError)) {
          lastErr = err; // timeout or network failure — retry
          await sleep(2_000 * 2 ** attempt);
          continue;
        }
        throw err;
      } finally {
        clearTimeout(timer);
      }
    }
    throw lastErr instanceof Error ? lastErr : new Error(`CoinGecko ${path}: retries exhausted`);
  });
}

export const coingecko = {
  // Total market cap, 24h volume and dominance across every coin CoinGecko tracks.
  global: () => get<CgGlobal>("/global", {}),

  // Coins ranked by market cap, up to 250 per page.
  markets: (perPage: number, page = 1) =>
    get<CgMarket[]>("/coins/markets", {
      vs_currency: "usd",
      order: "market_cap_desc",
      per_page: Math.min(250, perPage),
      page,
      sparkline: "false",
    }),

  // One point per day (00:00 UTC) once days > 90, plus a trailing live point.
  // Public/demo keys serve at most 365 days; pro keys accept "max".
  marketChart: (id: string, days: number | "max") =>
    get<CgMarketChart>(`/coins/${encodeURIComponent(id)}/market_chart`, { vs_currency: "usd", days }),

  // Total market cap history — pro plans only.
  globalMarketCapChart: (days: number | "max") =>
    get<CgGlobalChart>("/global/market_cap_chart", { vs_currency: "usd", days }),
};
