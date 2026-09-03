import { pool } from "./pool.js";

export interface WhaleCoinBreakdown {
  coin: string;
  side: "long" | "short";
  ntl: number;
  events: number;
  fills: number;
  // Price context (absent on rows recorded before migration 015):
  pxBefore?: number | null; // 1h before the burst's first fill
  pxStart?: number | null; // at the first fill
  pxEnd?: number | null; // at the latest fill
  // What the wallet still holds in this coin after the burst (null = not checked):
  remainingSz?: number | null; // signed, HL convention (>0 long)
  remainingNtlUsd?: number | null;
  fullyLiquidated?: boolean | null;
}

export interface WhaleStateAfter {
  checkedAt: string;
  accountValue: number | null;
  totalNtlPos: number | null;
  positions: Array<{ coin: string; szi: number; entryPx: number | null; positionValue: number | null }>;
}

export interface LiqWhaleRow {
  id: string; // bigserial arrives as a string
  wallet: string;
  detected_at: Date;
  from_ts: Date;
  to_ts: Date;
  ntl: number;
  events: number;
  fills: number;
  coins: WhaleCoinBreakdown[];
  active: boolean;
  threshold_usd: number;
  delivered: boolean | null;
  delivery_error: string | null;
  updated_at: Date;
  state_after: WhaleStateAfter | null;
  state_checked_at: Date | null;
}

export interface LiqWhaleFilter {
  wallet?: string;
  coin?: string;
  sinceMs?: number;
  minNtlUsd?: number;
  active?: boolean;
  limit: number;
}

// Whale episodes, newest first (by latest fill).
export async function listLiqWhales(f: LiqWhaleFilter): Promise<LiqWhaleRow[]> {
  const { rows } = await pool.query<LiqWhaleRow>(
    `select * from liq_whales
     where ($1::text is null or wallet = $1)
       and ($2::text is null or coins @> jsonb_build_array(jsonb_build_object('coin', $2::text)))
       and ($3::float8 is null or to_ts >= to_timestamp($3 / 1000.0))
       and ($4::float8 is null or ntl >= $4)
       and ($5::boolean is null or active = $5)
     order by to_ts desc, id desc limit $6`,
    [f.wallet ?? null, f.coin ?? null, f.sinceMs ?? null, f.minNtlUsd ?? null, f.active ?? null, f.limit],
  );
  return rows;
}
