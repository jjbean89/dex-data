import { pool } from "../db/pool.js";
import { hl } from "../hl/client.js";

function num(s: string | null | undefined): number | null {
  if (s === null || s === undefined) return null;
  const v = parseFloat(s);
  return Number.isFinite(v) ? v : null;
}

// One sweep: snapshot every live perp from a single metaAndAssetCtxs call.
export async function collectTick(): Promise<{ ts: Date; coins: number }> {
  const [meta, ctxs] = await hl.metaAndAssetCtxs();
  const ts = new Date();

  const aCoin: string[] = [];
  const aSz: Array<number | null> = [];
  const aLev: Array<number | null> = [];
  const aDel: boolean[] = [];

  const tCoin: string[] = [];
  const tMark: Array<number | null> = [];
  const tMid: Array<number | null> = [];
  const tOracle: Array<number | null> = [];
  const tPrev: Array<number | null> = [];
  const tFund: Array<number | null> = [];
  const tOi: Array<number | null> = [];
  const tOiUsd: Array<number | null> = [];
  const tPrem: Array<number | null> = [];
  const tVlm: Array<number | null> = [];

  meta.universe.forEach((u, i) => {
    aCoin.push(u.name);
    aSz.push(u.szDecimals ?? null);
    aLev.push(u.maxLeverage ?? null);
    aDel.push(u.isDelisted === true);
    const ctx = ctxs[i];
    if (!ctx || u.isDelisted) return;
    const mark = num(ctx.markPx);
    const oi = num(ctx.openInterest);
    tCoin.push(u.name);
    tMark.push(mark);
    tMid.push(num(ctx.midPx));
    tOracle.push(num(ctx.oraclePx));
    tPrev.push(num(ctx.prevDayPx));
    tFund.push(num(ctx.funding));
    tOi.push(oi);
    tOiUsd.push(mark !== null && oi !== null ? mark * oi : null);
    tPrem.push(num(ctx.premium));
    tVlm.push(num(ctx.dayNtlVlm));
  });

  await pool.query(
    `insert into perp_assets (coin, sz_decimals, max_leverage, is_delisted)
     select * from unnest($1::text[], $2::int[], $3::int[], $4::boolean[])
     on conflict (coin) do update set
       sz_decimals = excluded.sz_decimals,
       max_leverage = excluded.max_leverage,
       is_delisted = excluded.is_delisted,
       last_seen = now()`,
    [aCoin, aSz, aLev, aDel],
  );

  await pool.query(
    `insert into perp_ticks (ts, coin, mark_px, mid_px, oracle_px, prev_day_px, funding_hr,
                             open_interest, oi_usd, premium, day_ntl_vlm)
     select $1::timestamptz, * from unnest($2::text[], $3::float8[], $4::float8[], $5::float8[],
                                           $6::float8[], $7::float8[], $8::float8[], $9::float8[],
                                           $10::float8[], $11::float8[])
     on conflict do nothing`,
    [ts, tCoin, tMark, tMid, tOracle, tPrev, tFund, tOi, tOiUsd, tPrem, tVlm],
  );

  return { ts, coins: tCoin.length };
}
