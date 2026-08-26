-- Tracks per-coin completion of external census seeds (e.g. HyperTracker), so a
-- restarted collector resumes where it left off instead of re-importing everything.
-- To force a full re-seed: delete from seed_progress where source = 'hypertracker';
create table seed_progress (
  source        text not null,
  coin          text not null,
  completed_at  timestamptz not null default now(),
  wallets       int not null,
  rows_imported int not null,
  primary key (source, coin)
);
