-- Operational events written by the collector (seed progress, failures, quota
-- pauses) so diagnostics are readable through the API without host log access.
-- Pruned by the retention job.
create table ops_events (
  id      bigserial primary key,
  ts      timestamptz not null default now(),
  tag     text not null,
  level   text not null default 'info',
  message text not null
);
create index ops_events_ts_idx on ops_events (ts desc);
