-- Whale discovery from bridge deposits.
--
-- Hyperliquid's API has no venue-wide deposit feed (every account endpoint is
-- keyed by wallet), but external money enters through the Bridge2 contract on
-- Arbitrum as an ordinary USDC transfer whose SENDER is the Hyperliquid account
-- credited. The collector polls those transfer logs, records them here, flags
-- addresses whose trailing-window deposits cross WHALE_MIN_USD, and then watches
-- each flagged wallet on Hyperliquid (account value, account age from its
-- ledger, first fill on the trade tape, open positions).

create table bridge_deposits (
  tx_hash      text not null,
  log_index    int not null,
  block_number bigint not null,
  ts           timestamptz not null,   -- Arbitrum block timestamp
  address      text not null,          -- depositor (lowercase hex) = HL account credited
  usdc         double precision not null,
  primary key (tx_hash, log_index)
);
create index bridge_deposits_ts_idx on bridge_deposits (ts);
create index bridge_deposits_address_idx on bridge_deposits (address, ts);

-- Single-row poll cursor (last Arbitrum block fully scanned).
create table bridge_sync (
  id         int primary key default 1 check (id = 1),
  last_block bigint not null,
  updated_at timestamptz not null default now()
);

create table whale_wallets (
  address           text primary key,
  flagged_at        timestamptz not null default now(),
  deposited_usd     double precision not null, -- bridge deposits in the qualifying trailing window
  first_deposit_at  timestamptz not null,      -- earliest deposit of the qualifying window
  last_deposit_at   timestamptz not null,
  watch_until       timestamptz not null,      -- HL state polling stops after this
  -- Hyperliquid enrichment (null until checked)
  account_value     double precision,          -- perps account value (USDC)
  total_ntl_pos     double precision,          -- total open position notional
  state_checked_at  timestamptz,
  ledger_first_at   timestamptz,               -- first ledger entry ever = account age
  ledger_checked_at timestamptz,
  first_trade_at    timestamptz,               -- first fill seen on the tape after flagging
  positioned_at     timestamptz                -- first time an open position was observed
);
create index whale_wallets_flagged_idx on whale_wallets (flagged_at desc);
create index whale_wallets_watch_idx on whale_wallets (watch_until) where positioned_at is null;
