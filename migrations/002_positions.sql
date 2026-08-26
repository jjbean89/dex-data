-- Trader/position tracking for long-short counts.
--
-- Wallets are discovered from the public trades WebSocket (each fill carries
-- [buyer, seller]). A wallet's true positions are fetched once via
-- clearinghouseState ("bootstrap"), then maintained by applying signed fill
-- deltas. positioning_snapshots freezes per-coin long/short counts on an
-- interval — that series is the change-over-time record.

create table traders (
  address           text primary key,
  first_seen        timestamptz not null default now(),
  last_trade_at     timestamptz,
  bootstrapped_at   timestamptz,
  bootstrap_pending boolean not null default true
);
create index traders_pending_idx on traders (last_trade_at desc) where bootstrap_pending;

create table positions (
  address    text not null,
  coin       text not null,
  szi        double precision not null, -- signed size in coins: >0 long, <0 short
  entry_px   double precision,
  updated_at timestamptz not null default now(),
  primary key (address, coin)
);
create index positions_coin_idx on positions (coin) where szi <> 0;

create table positioning_snapshots (
  ts              timestamptz not null, -- aligned to the snapshot interval
  coin            text not null,
  n_long          int not null,
  n_short         int not null,
  sz_long         double precision not null, -- total coins held long
  sz_short        double precision not null, -- total coins held short (absolute)
  ntl_long        double precision,          -- USD notional at snapshot-time mark
  ntl_short       double precision,
  traders_tracked int not null,              -- bootstrapped wallets at snapshot time (coverage)
  primary key (coin, ts)
);
create index positioning_snapshots_ts_idx on positioning_snapshots (ts);
