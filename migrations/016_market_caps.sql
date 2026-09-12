-- Global crypto market-cap indexes — the CRYPTOCAP:TOTAL / TOTAL2 / TOTAL3 /
-- OTHERS series TradingView charts — built from CoinGecko.
--
--   total   every coin CoinGecko tracks
--   total2  total minus BTC
--   total3  total minus BTC and ETH
--   others  total minus the top-N coins by market cap (N = MCAP_TOP_N, 10 by default)
--
-- The collector polls CoinGecko's /global (total cap + 24h volume) and
-- /coins/markets (top-N constituents) every MCAP_POLL_MS and stores one
-- snapshot row per poll. Index values are derived per snapshot, then rolled up
-- into daily OHLC candles. Daily history from before the collector existed is
-- backfilled close-only from CoinGecko's market-chart endpoints.

create table mcap_snapshots (
  ts            timestamptz not null primary key,
  total_usd     double precision not null,   -- /global total market cap
  total_vol_usd double precision not null,   -- /global 24h volume
  btc_usd       double precision not null,
  btc_vol_usd   double precision not null,
  eth_usd       double precision not null,
  eth_vol_usd   double precision not null,
  top_usd       double precision not null,   -- sum of the top-N constituents (BTC and ETH included)
  top_vol_usd   double precision not null,
  top_n         int not null,
  top_ids       text[] not null              -- constituent CoinGecko ids, rank order
);

-- Daily OHLC per index. source ranks how the row was built:
--   recorded   rolled up from snapshots (real intraday OHLC; always wins)
--   coingecko  close-only from CoinGecko history (o = h = l = c)
--   approx     close-only, total reconstructed from the top-N coin histories
--              scaled to /global coverage (plans without /global/market_cap_chart)
create table mcap_candles_1d (
  idx    text not null,
  t      timestamptz not null,   -- UTC day start
  o      double precision not null,
  h      double precision not null,
  l      double precision not null,
  c      double precision not null,
  v      double precision,       -- 24h volume at the close
  source text not null,
  n      int not null default 0, -- snapshots folded in (0 for backfilled rows)
  primary key (idx, t)
);
create index mcap_candles_1d_t_idx on mcap_candles_1d (t);
