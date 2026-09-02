-- Liquidation threshold alerts.
--
-- The collector evaluates each configured rule (coin × trailing window × side)
-- against the exact trailing-window liquidated notional from liq_fills, and
-- records one alert per threshold crossing. A rule re-arms once its window
-- value falls back below the re-arm level, so a single cascade produces a single
-- alert per rule rather than one every evaluation.
--
-- liq_alert_rules is the collector's live rule set (mirrored from LIQ_ALERT_RULES
-- at boot) plus per-rule state, so the API can report thresholds and armed state
-- without sharing the collector's environment.

create table liq_alert_rules (
  coin          text not null,
  win           text not null,             -- '15m' | '1h' | '24h' …, as configured
  side          text not null,             -- 'long' | 'short' | 'total'
  window_ms     bigint not null,
  threshold_usd double precision not null,
  active        boolean not null default false, -- currently over threshold (alert fired, not yet re-armed)
  last_value    double precision,          -- window notional at the last evaluation
  last_eval_at  timestamptz,
  last_fired_at timestamptz,
  updated_at    timestamptz not null default now(),
  primary key (coin, win, side)
);

create table liq_alerts (
  id             bigserial primary key,
  ts             timestamptz not null default now(), -- when the crossing was detected
  coin           text not null,
  win            text not null,
  window_ms      bigint not null,
  side           text not null,            -- which side's liquidations crossed: 'long' | 'short' | 'total'
  ntl_usd        double precision not null, -- the value that crossed (that side's window notional)
  threshold_usd  double precision not null,
  events         int not null,             -- forced orders on that side in the window
  fills          int not null,
  long_ntl       double precision not null, -- full window breakdown at detection time
  short_ntl      double precision not null,
  long_events    int not null,
  short_events   int not null,
  message        text not null,
  delivered      boolean,                  -- null = no webhook configured
  delivery_error text
);
create index liq_alerts_ts_idx on liq_alerts (ts desc);
create index liq_alerts_coin_ts_idx on liq_alerts (coin, ts desc);
