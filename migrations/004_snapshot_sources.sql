-- Positioning snapshots can now come from two sources:
--   'live'         — computed from our own position ledger (full fidelity)
--   'hypertracker' — historical backfill imported from HyperTracker's
--                    position-metrics export (counts + notionals; no coin-size
--                    breakdown or coverage figure, hence the relaxed NOT NULLs)
alter table positioning_snapshots alter column sz_long drop not null;
alter table positioning_snapshots alter column sz_short drop not null;
alter table positioning_snapshots alter column traders_tracked drop not null;
alter table positioning_snapshots add column source text not null default 'live';
