-- Context for whale liquidations: what the wallet still holds afterwards, so a
-- record can say "the entire BTC position was liquidated" rather than just how
-- much was. Captured from clearinghouseState when the episode is detected and
-- again when it closes; null for episodes recorded before this migration or when
-- the fetch failed.
--
-- The `coins` breakdown additionally carries per-coin price context
-- ({pxBefore, pxStart, pxEnd} — price 1h before the burst, at its first fill, at
-- its latest fill) and the remaining position ({remainingSz, remainingNtlUsd,
-- fullyLiquidated}); older rows lack those keys and serialize as null.
alter table liq_whales add column state_after jsonb;          -- {checkedAt, accountValue, totalNtlPos, positions:[{coin, szi, entryPx, positionValue}]}
alter table liq_whales add column state_checked_at timestamptz;
