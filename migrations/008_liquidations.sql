-- Liquidation recording.
--
-- Liquidations print on the public trades WebSocket as ordinary trades (no marker),
-- but a wallet's fills from userFillsByTime carry an explicit liquidation object on
-- BOTH parties of a liquidation print (verified empirically). The recorder verifies
-- tape wallets — most-active first, so market makers classify the bulk of the flow —
-- and stores every liquidation-marked fill here; candle rollups are recomputed per
-- touched bucket in the same transaction.
--
-- side = which side got liquidated: 'long' (forced sell) or 'short' (forced buy).
-- Fills of one forced order share the same (wallet, ts) — that pair is the event key.

create table liq_fills (
  tid      bigint primary key,      -- HL trade id (unique across assets)
  ts       timestamptz not null,    -- exchange trade time (ms precision)
  coin     text not null,
  side     text not null,           -- 'long' | 'short'
  px       double precision not null,
  sz       double precision not null, -- coins
  ntl      double precision not null, -- px * sz (USDC notional)
  wallet   text not null,           -- liquidated address
  method   text                     -- 'market' | 'backstop'
);
create index liq_fills_coin_ts_idx on liq_fills (coin, ts);
create index liq_fills_ts_idx on liq_fills (ts);

-- Rolled up from liq_fills; *_events counts distinct forced orders, *_fills raw prints.
-- 5m rows are pruned on the same schedule as other 5m candles; 1h rows are permanent.
create table liq_candles_5m (
  coin         text not null,
  t            timestamptz not null, -- bucket start (UTC)
  long_ntl     double precision not null,
  short_ntl    double precision not null,
  long_events  int not null,
  short_events int not null,
  long_fills   int not null,
  short_fills  int not null,
  primary key (coin, t)
);
create index liq_candles_5m_t_idx on liq_candles_5m (t);

create table liq_candles_1h (
  coin         text not null,
  t            timestamptz not null,
  long_ntl     double precision not null,
  short_ntl    double precision not null,
  long_events  int not null,
  short_events int not null,
  long_fills   int not null,
  short_fills  int not null,
  primary key (coin, t)
);
create index liq_candles_1h_t_idx on liq_candles_1h (t);
