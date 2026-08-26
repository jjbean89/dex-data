-- Perp asset registry, upserted from the HL "meta" universe on every tick sweep.
create table perp_assets (
  coin         text primary key,
  sz_decimals  int,
  max_leverage int,
  is_delisted  boolean not null default false,
  first_seen   timestamptz not null default now(),
  last_seen    timestamptz not null default now()
);

-- Raw per-coin snapshots from metaAndAssetCtxs: one row per live coin per poll tick.
-- Short-lived (pruned after RAW_RETENTION_DAYS); the candle tables below keep long history.
create table perp_ticks (
  ts            timestamptz not null,
  coin          text not null,
  mark_px       double precision,
  mid_px        double precision,
  oracle_px     double precision,
  prev_day_px   double precision,
  funding_hr    double precision, -- current hourly funding rate, decimal (0.0000125 = 0.00125%/hr)
  open_interest double precision, -- in coins
  oi_usd        double precision, -- open_interest * mark_px at tick time
  premium       double precision,
  day_ntl_vlm   double precision, -- rolling 24h notional volume (USDC)
  primary key (coin, ts)
);
create index perp_ticks_ts_idx on perp_ticks (ts);

-- Settled hourly funding from HL fundingHistory (authoritative; backfillable to May 2023).
create table funding_history (
  coin    text not null,
  ts      timestamptz not null,
  rate_hr double precision not null,
  premium double precision,
  primary key (coin, ts)
);
create index funding_history_ts_idx on funding_history (ts);

-- Per-coin candles rolled up from perp_ticks. _o/_h/_l/_c = open/high/low/close within the
-- bucket; _a = average; oi_* is open interest in coins, oi_usd_* in USD.
create table perp_candles_5m (
  coin          text not null,
  t             timestamptz not null, -- bucket start (UTC)
  mid_o double precision, mid_h double precision, mid_l double precision, mid_c double precision,
  mark_c        double precision,
  oracle_c      double precision,
  oi_o double precision, oi_h double precision, oi_l double precision, oi_c double precision,
  oi_usd_o double precision, oi_usd_h double precision, oi_usd_l double precision, oi_usd_c double precision,
  funding_hr_c  double precision,
  premium_a     double precision,
  day_ntl_vlm_c double precision,
  n_ticks       int not null,
  primary key (coin, t)
);
create index perp_candles_5m_t_idx on perp_candles_5m (t);

create table perp_candles_1h (
  coin          text not null,
  t             timestamptz not null,
  mid_o double precision, mid_h double precision, mid_l double precision, mid_c double precision,
  mark_c        double precision,
  oracle_c      double precision,
  oi_o double precision, oi_h double precision, oi_l double precision, oi_c double precision,
  oi_usd_o double precision, oi_usd_h double precision, oi_usd_l double precision, oi_usd_c double precision,
  funding_hr_c  double precision,
  premium_a     double precision,
  day_ntl_vlm_c double precision,
  n_ticks       int not null,
  primary key (coin, t)
);
create index perp_candles_1h_t_idx on perp_candles_1h (t);

-- Venue-wide candles: per tick, open interest is summed across all live coins and funding is
-- OI-weighted; the bucket then takes OHLC over those per-tick totals. This is the
-- "aggregated OI" / "OI-weighted funding" series.
create table market_candles_5m (
  t timestamptz primary key,
  oi_usd_o double precision, oi_usd_h double precision, oi_usd_l double precision, oi_usd_c double precision,
  funding_hr_oiw_a double precision, -- bucket avg of per-tick OI-weighted hourly funding
  day_ntl_vlm_c    double precision, -- sum of rolling 24h notional volume at bucket close
  n_coins          int,
  n_ticks          int not null
);

create table market_candles_1h (
  t timestamptz primary key,
  oi_usd_o double precision, oi_usd_h double precision, oi_usd_l double precision, oi_usd_c double precision,
  funding_hr_oiw_a double precision,
  day_ntl_vlm_c    double precision,
  n_coins          int,
  n_ticks          int not null
);
