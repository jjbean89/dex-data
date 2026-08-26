import { config } from "../config.js";
import { pool } from "../db/pool.js";
import { hl, sleep } from "../hl/client.js";
import { logErr } from "../log.js";

const PAGE_SIZE = 500; // HL caps fundingHistory responses at 500 rows

// Sweep every live coin, pulling settled funding forward from our per-coin cursor
// (or FUNDING_BACKFILL_DAYS back on first contact). Paced to stay far under the
// 1200 weight/min IP budget alongside the tick poller.
export async function syncFunding(isStopped: () => boolean): Promise<{ coins: number; inserted: number }> {
  const { rows: coins } = await pool.query<{ coin: string }>(
    "select coin from perp_assets where is_delisted = false order by coin",
  );
  const defaultStart = Date.now() - config.fundingBackfillDays * 86_400_000;
  let inserted = 0;
  for (const { coin } of coins) {
    if (isStopped()) break;
    try {
      inserted += await syncCoin(coin, defaultStart, isStopped);
    } catch (err) {
      logErr("funding", `sync failed for ${coin}`, err);
    }
  }
  return { coins: coins.length, inserted };
}

async function syncCoin(coin: string, defaultStart: number, isStopped: () => boolean): Promise<number> {
  const { rows } = await pool.query<{ max: Date | null }>(
    "select max(ts) as max from funding_history where coin = $1",
    [coin],
  );
  let start = rows[0]?.max ? rows[0].max.getTime() + 1 : defaultStart;
  let inserted = 0;
  while (!isStopped()) {
    await sleep(config.fundingReqDelayMs);
    const page = await hl.fundingHistory(coin, start);
    if (page.length === 0) break;
    const ts: Date[] = [];
    const rate: number[] = [];
    const prem: Array<number | null> = [];
    for (const e of page) {
      const r = parseFloat(e.fundingRate);
      if (!Number.isFinite(r)) continue;
      const p = parseFloat(e.premium);
      ts.push(new Date(e.time));
      rate.push(r);
      prem.push(Number.isFinite(p) ? p : null);
    }
    if (ts.length > 0) {
      const res = await pool.query(
        `insert into funding_history (coin, ts, rate_hr, premium)
         select $1, * from unnest($2::timestamptz[], $3::float8[], $4::float8[])
         on conflict do nothing`,
        [coin, ts, rate, prem],
      );
      inserted += res.rowCount ?? 0;
    }
    const lastTime = page[page.length - 1]!.time;
    if (page.length < PAGE_SIZE || lastTime + 1 <= start) break;
    start = lastTime + 1;
  }
  return inserted;
}
