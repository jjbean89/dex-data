-- Large liquidated accounts ("whales").
--
-- recorded_at lets the whale tracker pick up fills by when they were discovered,
-- not when they traded: the recorder classifies fills seconds to minutes after
-- they print and backfills hours after downtime, and a late-discovered $10M
-- liquidation must still be collected.
alter table liq_fills add column recorded_at timestamptz not null default now();
create index liq_fills_recorded_at_idx on liq_fills (recorded_at);

-- One row per liquidation episode of one wallet: the burst of that wallet's
-- liquidation fills (across all coins) with no gap longer than LIQ_WHALE_WINDOW,
-- recorded once the burst's notional crosses LIQ_WHALE_THRESHOLD. The row is
-- updated while the episode is still open (active), then frozen.
create table liq_whales (
  id             bigserial primary key,
  wallet         text not null,
  detected_at    timestamptz not null default now(),
  from_ts        timestamptz not null,    -- first liquidation fill of the episode
  to_ts          timestamptz not null,    -- latest liquidation fill so far
  ntl            double precision not null, -- total liquidated notional (USD) in the episode
  events         int not null,            -- forced orders (distinct fill timestamps)
  fills          int not null,
  coins          jsonb not null,          -- [{coin, side, ntl, events, fills}] sorted by ntl desc
  active         boolean not null default true,
  threshold_usd  double precision not null,
  delivered      boolean,                 -- null = no webhook configured / notify off
  delivery_error text,
  updated_at     timestamptz not null default now()
);
create index liq_whales_detected_idx on liq_whales (detected_at desc);
create index liq_whales_wallet_idx on liq_whales (wallet, to_ts desc);
create index liq_whales_ntl_idx on liq_whales (ntl desc);
