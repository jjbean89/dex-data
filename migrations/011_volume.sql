-- Volume bars built from the public trade tape.
--
-- Hyperliquid's API exposes only a rolling 24h volume per coin; per-bar volume
-- exists only if you record it. The collector sums every print on the trades
-- WebSocket into 1m buckets (buy/sell split by taker side, TWAP prints — the
-- all-zero-hash fills — tracked separately), and recomputes the touched 5m/1h
-- buckets from the 1m rows in the same transaction.
--
-- Notional is USDC (px × sz); OHLC is of trade prices. vwap = ntl / sz.
-- 1m rows are pruned after VOL_1M_RETENTION_DAYS, 5m after
-- CANDLES_5M_RETENTION_DAYS, 1h rows are permanent.

create table vol_candles_1m (
  coin     text not null,
  t        timestamptz not null,       -- bucket start (UTC)
  o        double precision not null,
  h        double precision not null,
  l        double precision not null,
  c        double precision not null,
  buy_ntl  double precision not null,  -- taker-buy notional
  sell_ntl double precision not null,  -- taker-sell notional
  buy_sz   double precision not null,
  sell_sz  double precision not null,
  buy_n    int not null,               -- prints
  sell_n   int not null,
  twap_ntl double precision not null,  -- notional from TWAP prints (subset of buy+sell)
  last_ms  bigint not null,            -- exchange time of the latest print folded in (o/c ordering)
  slot     int not null,               -- seconds since midnight UTC of the bucket (same-time-of-day baselines)
  primary key (coin, t)
);
create index vol_candles_1m_t_idx on vol_candles_1m (t);

create table vol_candles_5m (like vol_candles_1m including all);
create table vol_candles_1h (like vol_candles_1m including all);

-- "Volume leading price" signals: abnormal volume while price is still flat,
-- with positioning confirmation (open interest or taker imbalance). One row per
-- buildup per coin; a later breakout bar confirms it in place.
create table vol_signals (
  id               bigserial primary key,
  coin             text not null,
  fired_at         timestamptz not null default now(),
  t_from           timestamptz not null,   -- first 5m bar of the buildup window
  t_to             timestamptz not null,   -- last (possibly partial) bar's start
  bars             int not null,
  vol_ntl          double precision not null, -- window notional
  baseline_ntl     double precision not null, -- expected notional for the window (sum of per-bar baselines)
  rvol             double precision not null, -- vol_ntl / baseline_ntl
  min_bar_rvol     double precision not null,
  px_from          double precision not null,
  px_to            double precision not null,
  px_move_pct      double precision not null,
  atr_pct          double precision not null, -- typical 5m range (%), the flatness yardstick
  oi_from          double precision,
  oi_to            double precision,
  oi_change_pct    double precision,
  buy_share_pct    double precision not null, -- taker-buy share of window notional
  twap_share_pct   double precision not null,
  bias             text not null,          -- 'long' | 'short' | 'mixed'
  market_wide      boolean not null default false, -- many coins triggered together
  status           text not null default 'open', -- 'open' | 'confirmed' | 'expired'
  confirmed_at     timestamptz,
  breakout_move_pct double precision,
  breakout_rvol    double precision,
  closed_at        timestamptz,
  message          text not null,
  delivered        boolean,
  delivery_error   text,
  updated_at       timestamptz not null default now()
);
create index vol_signals_fired_idx on vol_signals (fired_at desc);
create index vol_signals_coin_idx on vol_signals (coin, fired_at desc);

-- Same-time-of-day baseline lookups.
create index vol_candles_5m_slot_idx on vol_candles_5m (slot, t);
