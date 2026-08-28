-- Entry-price analytics per snapshot: size-weighted average entry per side and
-- how many positions (with a known entry) are in profit at the snapshot mark.
-- Underwater = n_*_entry - n_*_profit. Null on rows that predate this migration
-- and on backfilled external history.
alter table positioning_snapshots add column avg_entry_long double precision;
alter table positioning_snapshots add column avg_entry_short double precision;
alter table positioning_snapshots add column n_long_entry int;
alter table positioning_snapshots add column n_short_entry int;
alter table positioning_snapshots add column n_long_profit int;
alter table positioning_snapshots add column n_short_profit int;
