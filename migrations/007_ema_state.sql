-- Latest EMA per (coin, timeframe, period), maintained by the collector from
-- Hyperliquid's official candles: seeded once from a full candleSnapshot fetch
-- (TradingView-style — SMA over the first `period` closes, then the recursion),
-- then advanced incrementally as candles close. `ema` stays null while a young
-- listing is still accumulating its first `period` closes (seed_sum/seed_n).
create table ema_state (
  coin         text not null,
  tf           text not null,             -- '1h' | '2h' | '4h' | '8h' | '12h' | '1d'
  period       int  not null,             -- e.g. 21, 200
  ema          double precision,          -- as of the last closed candle; null while seeding
  seed_sum     double precision not null default 0,
  seed_n       int not null default 0,
  n_candles    int not null default 0,    -- closed candles applied so far (convergence hint)
  last_open_ms bigint not null,           -- open time (epoch ms) of the last closed candle applied
  seeded_at    timestamptz not null default now(), -- when this row was last (re)built from full history
  updated_at   timestamptz not null default now(),
  primary key (coin, tf, period)
);
