import { pool } from "./pool.js";

// Global market-cap indexes (TradingView's CRYPTOCAP:TOTAL / TOTAL2 / TOTAL3 /
// OTHERS) derived from CoinGecko snapshots. See migrations/016_market_caps.sql.

export const MCAP_INDEXES = ["total", "total2", "total3", "others"] as const;
export type McapIndex = (typeof MCAP_INDEXES)[number];

export const MCAP_INDEX_NAMES: Record<McapIndex, string> = {
  total: "Crypto Total Market Cap",
  total2: "Crypto Total Market Cap Excluding BTC",
  total3: "Crypto Total Market Cap Excluding BTC and ETH",
  others: "Crypto Total Market Cap Excluding Top 10",
};

export function isMcapIndex(raw: string): raw is McapIndex {
  return (MCAP_INDEXES as readonly string[]).includes(raw);
}

export const DAY_MS = 86_400_000;
export const CANDLE_SOURCES = ["recorded", "coingecko", "approx"] as const;
export type CandleSource = (typeof CANDLE_SOURCES)[number];

// Index value + 24h volume per snapshot row `s`, one row per index.
const INDEX_VALUES_SQL = `
  cross join lateral (values
    ('total',  s.total_usd,                         s.total_vol_usd),
    ('total2', s.total_usd - s.btc_usd,             s.total_vol_usd - s.btc_vol_usd),
    ('total3', s.total_usd - s.btc_usd - s.eth_usd, s.total_vol_usd - s.btc_vol_usd - s.eth_vol_usd),
    ('others', s.total_usd - s.top_usd,             s.total_vol_usd - s.top_vol_usd)
  ) as x(idx, val, vol)`;

export interface McapSnapshot {
  ts: Date;
  total_usd: number;
  total_vol_usd: number;
  btc_usd: number;
  btc_vol_usd: number;
  eth_usd: number;
  eth_vol_usd: number;
  top_usd: number;
  top_vol_usd: number;
  top_n: number;
  top_ids: string[];
}

export async function insertSnapshot(s: McapSnapshot): Promise<void> {
  await pool.query(
    `insert into mcap_snapshots
       (ts, total_usd, total_vol_usd, btc_usd, btc_vol_usd, eth_usd, eth_vol_usd, top_usd, top_vol_usd, top_n, top_ids)
     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
     on conflict (ts) do nothing`,
    [s.ts, s.total_usd, s.total_vol_usd, s.btc_usd, s.btc_vol_usd, s.eth_usd, s.eth_vol_usd, s.top_usd, s.top_vol_usd, s.top_n, s.top_ids],
  );
}

export async function latestSnapshot(): Promise<McapSnapshot | null> {
  const { rows } = await pool.query<McapSnapshot>("select * from mcap_snapshots order by ts desc limit 1");
  return rows[0] ?? null;
}

// Latest snapshot at or before `atMs`, if one exists within `toleranceMs` of it.
export async function snapshotAt(atMs: number, toleranceMs: number): Promise<McapSnapshot | null> {
  const { rows } = await pool.query<McapSnapshot>(
    `select * from mcap_snapshots
     where ts <= to_timestamp($1 / 1000.0) and ts > to_timestamp(($1 - $2) / 1000.0)
     order by ts desc limit 1`,
    [atMs, toleranceMs],
  );
  return rows[0] ?? null;
}

export function indexValue(s: McapSnapshot, idx: McapIndex): number {
  switch (idx) {
    case "total":
      return s.total_usd;
    case "total2":
      return s.total_usd - s.btc_usd;
    case "total3":
      return s.total_usd - s.btc_usd - s.eth_usd;
    case "others":
      return s.total_usd - s.top_usd;
  }
}

export function indexVolume(s: McapSnapshot, idx: McapIndex): number {
  switch (idx) {
    case "total":
      return s.total_vol_usd;
    case "total2":
      return s.total_vol_usd - s.btc_vol_usd;
    case "total3":
      return s.total_vol_usd - s.btc_vol_usd - s.eth_vol_usd;
    case "others":
      return s.total_vol_usd - s.top_vol_usd;
  }
}

// Rebuilds the daily candle of every index for every UTC day with snapshots at
// or after `fromMs`. Recorded rows always replace backfilled ones.
export async function rollupDaily(fromMs: number): Promise<number> {
  const r = await pool.query(
    `insert into mcap_candles_1d (idx, t, o, h, l, c, v, source, n)
     select idx, day,
       (array_agg(val order by ts))[1], max(val), min(val), (array_agg(val order by ts desc))[1],
       (array_agg(vol order by ts desc))[1], 'recorded', count(*)::int
     from (
       select date_trunc('day', s.ts) as day, s.ts, x.idx, x.val, x.vol
       from mcap_snapshots s ${INDEX_VALUES_SQL}
       where s.ts >= date_trunc('day', to_timestamp($1 / 1000.0))
     ) q
     group by idx, day
     on conflict (idx, t) do update
       set o = excluded.o, h = excluded.h, l = excluded.l, c = excluded.c, v = excluded.v,
           source = excluded.source, n = excluded.n`,
    [fromMs],
  );
  return r.rowCount ?? 0;
}

export interface BackfillCandle {
  idx: McapIndex;
  tMs: number; // UTC day start
  c: number;
  v: number | null;
  source: Exclude<CandleSource, "recorded">;
}

// Close-only history rows. Never overwrites recorded candles; approximate rows
// never overwrite exact CoinGecko ones.
export async function upsertBackfillCandles(rows: BackfillCandle[]): Promise<number> {
  if (rows.length === 0) return 0;
  const r = await pool.query(
    `insert into mcap_candles_1d (idx, t, o, h, l, c, v, source, n)
     select idx, to_timestamp(t_ms / 1000.0), c, c, c, c, v, source, 0
     from unnest($1::text[], $2::bigint[], $3::double precision[], $4::double precision[], $5::text[])
       as u(idx, t_ms, c, v, source)
     on conflict (idx, t) do update
       set o = excluded.o, h = excluded.h, l = excluded.l, c = excluded.c, v = excluded.v, source = excluded.source, n = 0
       where mcap_candles_1d.source <> 'recorded'
         and not (mcap_candles_1d.source = 'coingecko' and excluded.source = 'approx')`,
    [rows.map((r) => r.idx), rows.map((r) => r.tMs), rows.map((r) => r.c), rows.map((r) => r.v), rows.map((r) => r.source)],
  );
  return r.rowCount ?? 0;
}

// Days in [fromMs, toMs] (day starts, inclusive) with no candle for `idx`.
export async function missingDays(idx: McapIndex, fromMs: number, toMs: number): Promise<number> {
  const { rows } = await pool.query<{ n: string }>(
    `select count(*) as n
     from generate_series(date_trunc('day', to_timestamp($2 / 1000.0)), date_trunc('day', to_timestamp($3 / 1000.0)), interval '1 day') d
     left join mcap_candles_1d c on c.idx = $1 and c.t = d
     where c.t is null`,
    [idx, fromMs, toMs],
  );
  return Number(rows[0]?.n ?? 0);
}

// Earliest day that came from CoinGecko history rather than live recording.
export async function firstBackfilledDay(idx: McapIndex): Promise<Date | null> {
  const { rows } = await pool.query<{ t: Date | null }>(
    "select min(t) as t from mcap_candles_1d where idx = $1 and source <> 'recorded'",
    [idx],
  );
  return rows[0]?.t ?? null;
}

export interface HistoryInfo {
  firstDay: Date | null;
  lastDay: Date | null;
  days: number;
  bySource: Record<CandleSource, number>;
}

export async function historyInfo(idx: McapIndex): Promise<HistoryInfo> {
  const { rows } = await pool.query<{ source: CandleSource; n: string; first: Date; last: Date }>(
    "select source, count(*) as n, min(t) as first, max(t) as last from mcap_candles_1d where idx = $1 group by source",
    [idx],
  );
  const bySource: Record<CandleSource, number> = { recorded: 0, coingecko: 0, approx: 0 };
  let firstDay: Date | null = null;
  let lastDay: Date | null = null;
  let days = 0;
  for (const r of rows) {
    bySource[r.source] = Number(r.n);
    days += Number(r.n);
    if (firstDay === null || r.first < firstDay) firstDay = r.first;
    if (lastDay === null || r.last > lastDay) lastDay = r.last;
  }
  return { firstDay, lastDay, days, bySource };
}

export interface McapCandleRow {
  t: Date;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number | null;
  source: CandleSource | "mixed";
  n: number;
}

export type McapInterval = "1h" | "4h" | "12h" | "1d" | "1w";
export const MCAP_INTERVAL_MS: Record<McapInterval, number> = {
  "1h": 3_600_000,
  "4h": 14_400_000,
  "12h": 43_200_000,
  "1d": DAY_MS,
  "1w": 7 * DAY_MS,
};

// Candles for one index, ascending, the most recent `limit` within [fromMs, toMs).
//   1d      the daily table (recorded + backfilled history)
//   1w      daily rows folded into Monday-anchored weeks (volume summed)
//   1h–12h  snapshots bucketed on the fly (volume = trailing 24h at the close)
export async function mcapCandles(
  idx: McapIndex,
  interval: McapInterval,
  fromMs: number,
  toMs: number,
  limit: number,
): Promise<McapCandleRow[]> {
  let sql: string;
  if (interval === "1d") {
    sql = `
      select t, o, h, l, c, v, source, n from mcap_candles_1d
      where idx = $1 and t >= to_timestamp($2 / 1000.0) and t < to_timestamp($3 / 1000.0)
      order by t desc limit $4`;
  } else if (interval === "1w") {
    sql = `
      select * from (
        select date_bin('7 days', t, timestamptz '1970-01-05') as t,
          (array_agg(o order by t))[1] as o, max(h) as h, min(l) as l, (array_agg(c order by t desc))[1] as c,
          sum(v) as v,
          case when count(distinct source) = 1 then min(source) else 'mixed' end as source,
          sum(n)::int as n
        from mcap_candles_1d
        where idx = $1 and t >= to_timestamp($2 / 1000.0) and t < to_timestamp($3 / 1000.0)
        group by 1
      ) w
      order by t desc limit $4`;
  } else {
    sql = `
      select * from (
        select date_bin(make_interval(secs => ${MCAP_INTERVAL_MS[interval] / 1000}), s.ts, timestamptz 'epoch') as t,
          (array_agg(x.val order by s.ts))[1] as o, max(x.val) as h, min(x.val) as l,
          (array_agg(x.val order by s.ts desc))[1] as c, (array_agg(x.vol order by s.ts desc))[1] as v,
          'recorded' as source, count(*)::int as n
        from mcap_snapshots s ${INDEX_VALUES_SQL}
        where x.idx = $1 and s.ts >= to_timestamp($2 / 1000.0) and s.ts < to_timestamp($3 / 1000.0)
        group by 1
      ) b
      order by t desc limit $4`;
  }
  const { rows } = await pool.query<McapCandleRow>(sql, [idx, fromMs, toMs, limit]);
  return rows.reverse();
}

// Daily closes at exactly the given day starts (for N-day change figures).
export async function dailyClosesAt(idx: McapIndex, dayStartsMs: number[]): Promise<Map<number, number>> {
  const { rows } = await pool.query<{ t: Date; c: number }>(
    `select t, c from mcap_candles_1d
     where idx = $1 and t = any(select to_timestamp(unnest($2::bigint[]) / 1000.0))`,
    [idx, dayStartsMs],
  );
  return new Map(rows.map((r) => [r.t.getTime(), r.c]));
}

export async function pruneSnapshots(retentionDays: number): Promise<number> {
  const r = await pool.query("delete from mcap_snapshots where ts < now() - make_interval(days => $1)", [retentionDays]);
  return r.rowCount ?? 0;
}
