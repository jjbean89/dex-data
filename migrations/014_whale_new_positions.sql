-- Whale "positioned" alerts fire on positions opened AFTER funding, not on
-- whatever the wallet already held. A known account topping up margin on 30
-- open shorts used to trip the alert on its first check; now the tracker
-- reconstructs the wallet's positions as of the episode's first deposit (current
-- state minus the net of every fill since) and alerts only when a position that
-- was not in that baseline — a new coin, or a side flip — reaches
-- WHALE_POSITION_MIN_USD of notional.

-- [{coin, szi}] as of first_deposit_at; null until the first check of the episode
-- (resets with the other episode markers when a wallet is re-funded).
alter table whale_wallets add column baseline_positions jsonb;

-- positioned_at now means: first time a position opened after this episode's
-- funding was observed (any size). Fast polling runs until the alert has fired.
drop index if exists whale_wallets_watch_idx;
create index whale_wallets_watch_idx on whale_wallets (watch_until) where position_alerted_at is null;

-- The positions the alert is about, [{coin, side, sz, entryPx, ntlUsd}];
-- `positions` keeps the wallet's full snapshot at alert time.
alter table whale_alerts add column opened jsonb not null default '[]';
