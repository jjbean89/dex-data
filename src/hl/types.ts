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

export interface AssetPosition {
  type: string; // "oneWay"
  position: {
    coin: string;
    szi: string; // signed size in coins: >0 long, <0 short
    entryPx?: string;
    positionValue?: string;
    unrealizedPnl?: string;
    leverage?: { type: string; value: number };
  };
}

export interface ClearinghouseState {
  assetPositions: AssetPosition[];
  marginSummary?: { accountValue: string; totalNtlPos: string; totalRawUsd?: string; totalMarginUsed?: string };
  withdrawable?: string;
  time: number;
}

// A fill from userFills/userFillsByTime. On a liquidation print, BOTH parties'
// fills carry the `liquidation` object (verified empirically); `liquidatedUser`
// names the forced wallet, and `side` is the queried wallet's side of the trade.
export interface UserFill {
  coin: string;
  px: string;
  sz: string;
  side: "A" | "B"; // A = the queried wallet sold, B = bought
  time: number;
  dir: string;
  hash: string;
  oid: number;
  crossed: boolean;
  tid: number;
  liquidation?: {
    liquidatedUser?: string;
    markPx: string;
    method: "market" | "backstop";
  };
}

// One entry from userNonFundingLedgerUpdates: deposits, withdrawals, and
// transfers (internal, sub-account, spot, vault). Oldest first from startTime.
// `usdc` is present on the USDC-denominated types (deposit, withdraw, transfers).
export interface LedgerUpdate {
  time: number;
  hash: string;
  delta: {
    type: string; // "deposit" | "withdraw" | "internalTransfer" | "subAccountTransfer" | "accountClassTransfer" | "spotTransfer" | "vaultDeposit" | ...
    usdc?: string;
    [k: string]: unknown;
  };
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
