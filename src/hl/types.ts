// Shapes returned by the Hyperliquid info endpoint (POST {HL_API_URL}/info).
// All numeric values arrive as strings; timestamps are epoch milliseconds.

export interface PerpUniverseEntry {
  name: string;
  szDecimals: number;
  maxLeverage: number;
  marginTableId?: number;
  isDelisted?: boolean;
}

export interface PerpAssetCtx {
  funding: string; // current hourly funding rate, decimal (e.g. "0.0000125")
  openInterest: string; // in coins, not USD
  prevDayPx: string; // price 24h ago (rolling) — source of HL's 24h change
  dayNtlVlm: string; // rolling 24h notional volume (USDC)
  dayBaseVlm?: string;
  premium: string | null;
  oraclePx: string;
  markPx: string;
  midPx: string | null; // can be null for markets with an empty book
  impactPxs: string[] | null;
}

export type MetaAndAssetCtxs = [{ universe: PerpUniverseEntry[] }, PerpAssetCtx[]];

export interface FundingHistoryEntry {
  coin: string;
  fundingRate: string; // settled rate for that hour, decimal
  premium: string;
  time: number;
}

export interface Candle {
  t: number; // bucket open time (ms)
  T: number; // bucket close time (ms)
  s: string; // coin
  i: string; // interval
  o: string;
  c: string;
  h: string;
  l: string;
  v: string; // base volume
  n: number; // trade count
}
