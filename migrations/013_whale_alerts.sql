-- Whale alerts: one row per notable whale event, mirroring liq_alerts.
--   'funded'     — a wallet crossed WHALE_MIN_USD in bridge deposits (sent after the
--                  first Hyperliquid check, so it carries account value and age)
--   'positioned' — a flagged wallet was first seen with an open position
-- Per-episode markers on whale_wallets stop a restart from re-sending them; they
-- reset with the other episode markers when a wallet is re-funded after its watch.

create table whale_alerts (
  id              bigserial primary key,
  ts              timestamptz not null default now(),
  kind            text not null,             -- 'funded' | 'positioned'
  address         text not null,
  deposited_usd   double precision not null,
  account_value   double precision,
  total_ntl_pos   double precision,
  is_new_account  boolean,
  ledger_first_at timestamptz,
  positions       jsonb not null default '[]', -- [{coin, side, sz, entryPx}]
  message         text not null,
  delivered       boolean,                   -- null = no webhook configured
  delivery_error  text
);
create index whale_alerts_ts_idx on whale_alerts (ts desc);
create index whale_alerts_address_idx on whale_alerts (address, ts desc);

alter table whale_wallets add column funded_alerted_at timestamptz;
alter table whale_wallets add column position_alerted_at timestamptz;
