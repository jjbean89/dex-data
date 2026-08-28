# dex-data

Custom Hyperliquid market data service. Hyperliquid's public API only exposes the **current** open interest and funding rate, and only a **24h** price change — this service records the whole market on a tight loop and serves the derived data Hyperliquid can't give you:

- **Price change over any window** (5m, 1h, 4h, 24h, 7d, …) for every perp, not just 24h
- **Open interest history** — per-coin and venue-wide OI candles (this data exists nowhere in HL's API; it only exists if you record it)
- **Funding history** — settled hourly rates (synced from HL back to whatever backfill depth you choose) plus recorded live rates
- **Venue-wide aggregates** — total OI candles and OI-weighted average funding, the "Aggregated Open Interest" / "Aggregated Funding Rate" style series
- **Long/short trader counts** — how many wallets are long vs. short each coin, with change over time (derived from the public trade tape + per-wallet position tracking; this exists in no API anywhere)

One process polls `metaAndAssetCtxs` (every live perp in a single request) every 15s, rolls ticks up into 5m/1h candles, prunes raw data on a retention schedule, and runs the trades-WebSocket position tracker. A second process serves a read-only JSON API.

```
┌───────────┐   POST /info every 15s    ┌───────────┐        ┌───────────┐
│ Hyperliquid│ ────────────────────────▶ │ collector │ ─────▶ │ Postgres  │
│  mainnet   │   fundingHistory hourly   │  (ROLE=   │        │ ticks +   │
└───────────┘                            │ collector)│        │ candles   │
                                         └───────────┘        └─────┬─────┘
                                                                    │
                                          ┌───────────┐             │
                              your apps ◀─│    api    │ ◀───────────┘
                              third parties│  (ROLE=api)│  cached reads
                                          └───────────┘
```

> **Start the collector before anything else matters.** Price and funding gaps can be repaired from HL's candle/funding endpoints, but **open interest history is unrecoverable** — every hour the collector isn't running is OI data lost forever.

## Quickstart (local)

```bash
npm install
cp .env.example .env           # set DATABASE_URL
npm run dev                    # migrates, then runs collector + API in one process
curl localhost:3000/v1/perps/changes?window=1h
```

Change endpoints need history to compare against, so right after first boot they return `null` change values until the window has data (1h change works after 1h of uptime; funding history fills within minutes).

## Deploying on Railway

One repo, three Railway services:

1. **Postgres** — add Railway's Postgres database to the project.
2. **collector** — *New Service → GitHub repo → this repo.* Railway builds the Dockerfile automatically. Set variables:
   - `DATABASE_URL` = `${{Postgres.DATABASE_URL}}`
   - `ROLE` = `collector`
   - Optionally `FUNDING_BACKFILL_DAYS=1200` once, to pull settled funding all the way back to May 2023 (the paced sweep chips away at it safely).
   - No public domain needed. Enable restart-on-failure (default).
3. **api** — second service from the same repo:
   - `DATABASE_URL` = `${{Postgres.DATABASE_URL}}`
   - `ROLE` = `api`
   - Generate a public domain. Set the healthcheck path to `/health`. Railway injects `PORT` automatically.

Use the **private-network** `DATABASE_URL` reference (`${{Postgres.DATABASE_URL}}`) so traffic stays internal; if you ever connect through Railway's public proxy instead, set `PG_SSL_NO_VERIFY=true`.

Deploys are zero-drama: the collector shuts down gracefully on SIGTERM and the DB persists, so a redeploy costs seconds of ticks. Both services run migrations at boot behind an advisory lock, so start order doesn't matter. (`ROLE=all` runs both in one service if you want to start smaller.)

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres connection string |
| `ROLE` | `all` | `collector`, `api`, or `all` |
| `PORT` / `HOST` | `3000` / `0.0.0.0` | API listen address (Railway sets `PORT`) |
| `HL_API_URL` | `https://api.hyperliquid.xyz` | Point at `https://api.hyperliquid-testnet.xyz` for testnet |
| `POLL_INTERVAL_MS` | `15000` | Tick cadence. One request covers all coins (weight 20 of HL's 1200/min IP budget) |
| `FUNDING_SYNC_INTERVAL_MS` | `3600000` | Settled-funding sweep cadence |
| `FUNDING_REQ_DELAY_MS` | `2000` | Pacing between fundingHistory requests |
| `FUNDING_BACKFILL_DAYS` | `30` | First-contact backfill depth per coin (HL has data back to 2023-05) |
| `ROLLUP_INTERVAL_MS` | `60000` | Candle rollup cadence |
| `RAW_RETENTION_DAYS` | `14` | Raw tick retention — also the max `window` on `/changes` |
| `CANDLES_5M_RETENTION_DAYS` | `180` | 5m candle retention (1h candles are kept forever) |
| `POSITIONS_ENABLED` | `true` | Long/short trader tracking (trades WebSocket + position ledger) |
| `HL_WS_URL` | `wss://api.hyperliquid.xyz/ws` | Trades feed |
| `POSITIONS_SNAPSHOT_MS` | `300000` | Long/short snapshot cadence (resolution of the history series) |
| `BOOTSTRAP_DELAY_MS` | `400` | Pacing between clearinghouseState wallet lookups |
| `POSITIONS_FLUSH_MS` / `REVERIFY_INTERVAL_MS` / `REVERIFY_BATCH` | `1000` / `6h` / `2000` | Fill-delta write cadence; self-heal sweep |
| `HYPERTRACKER_API_KEY` | — | Enables the one-time starting-census import (see positioning docs below) |
| `HYPERTRACKER_BASE_URL` / `HYPERTRACKER_REQ_DELAY_MS` | `https://ht-api.coinmarketman.com/api` / `1500` | Census source + pacing |
| `PG_SSL_NO_VERIFY` | `false` | Accept self-signed Postgres TLS (Railway public proxy) |

## API

All responses are JSON, CORS `*`, UTC timestamps (ISO + `tMs` epoch millis on series). Errors: `{"error":{"code","message"}}` with 400/404/503. Funding rates are **hourly decimals** (`0.0000125` = 0.00125%/hr ≈ 10.95% APR — `aprPct` fields do the conversion). OI is reported in coins (`openInterest`) and USD (`oiUsd` = OI × mark price).

### `GET /v1/perps/changes?window=1h`
The headline endpoint: price, OI, and funding change over any window for every coin, sorted. Params: `window` (`5m`…`14d`, rolling, default `1h`), `sort` (`px|oi|funding|volume`), `dir`, `limit`, `minOiUsd` (filter dust).

```json
{ "window": "1h", "asOf": "2026-08-26T15:18:18.354Z", "toleranceSec": 180, "count": 2,
  "data": [{ "coin": "BTC", "px": 77752.5, "pxThen": 76195.49, "pxChangePct": 2.04,
             "oiUsd": 2959237278.4, "oiUsdThen": 2755498389.0, "oiUsdChangePct": 7.39,
             "fundingHr": 0.0000125, "fundingHrThen": 0.00000625, "fundingAprPct": 10.95,
             "dayNtlVlm": 3840737419.7, "hl24hChangePct": -1.67, "thenTs": "2026-08-26T14:17:33.462Z" }] }
```
"Then" is the recorded tick nearest to `now - window` (±5% of the window, clamped 90s–15min; reported as `toleranceSec`). Missing coverage → `null` changes, never fabricated values.

### `GET /v1/perps` · `GET /v1/perps/:coin`
Universe list (sorted by OI) and a single-coin snapshot with `changes` for 1h/4h/24h inline. Coin names are matched case-insensitively (`btc` → `BTC`; exact match wins for names like `kPEPE`).

### `GET /v1/perps/:coin/candles?interval=5m|1h|1d`
OHLC candles **of both price and open interest** rolled up from recorded ticks — this is the per-coin OI chart feed. Params: `from`, `to` (epoch ms/s or ISO), `limit` (default 300, max 5000; most recent within range, ascending). Each row: `mid {o,h,l,c}`, `oi {o,h,l,c}` (coins), `oiUsd {o,h,l,c}`, `markC`, `oracleC`, `fundingHr`, `premiumAvg`, `dayNtlVlm`, `nTicks`.

### `GET /v1/perps/:coin/funding-history`
Settled hourly funding (authoritative, from HL's ledger): `{t, rateHr, aprPct, premium}`. Default last 168 hours; `from`/`to`/`limit` as above.

### `GET /v1/market/snapshot`
Venue totals right now: `totalOiUsd`, `oiUsdChangePct1h/24h`, OI-weighted funding (`fundingHrOiw`, `fundingAprPctOiw`), `totalDayNtlVlm`, `nCoins`.

### `GET /v1/market/oi?interval=5m|1h|1d`
**Aggregated open interest candles** — total venue OI summed per tick, OHLC per bucket (true venue highs/lows, not summed per-coin highs).

### `GET /v1/market/funding?interval=1h&smooth=8h`
**OI-weighted average funding series**, optionally smoothed with a trailing mean (`smooth=8h` reproduces the classic "8h average" view of HL's hourly rates).

### `GET /v1/perps/:coin/positioning` · `/positioning/history` · `GET /v1/perps/positioning`
**Number of traders long vs. short**, per coin. Nowhere in Hyperliquid's API — derived here by tracking every wallet seen on the public trade tape (each fill names buyer and seller), bootstrapping its true positions via `clearinghouseState`, then maintaining them from fills. Snapshots freeze the counts every 5 minutes; `/history` serves that series (your change-over-time), the coin endpoint adds 1h/24h deltas inline, and the bare `/positioning` lists all coins.

```json
{ "coin": "BTC", "t": "2026-08-26T16:10:00.000Z", "nLong": 412, "nShort": 268, "nTraders": 680,
  "pctLong": 60.6, "longShortRatio": 1.54, "szLong": 18342.1, "szShort": 12007.9,
  "ntlLongUsd": 1431201852.2, "ntlShortUsd": 936914233.8, "netNtlUsd": 494287618.4,
  "tradersTracked": 9184, "coverage": { "tracked": 9184, "pending": 1201 },
  "changes": { "1h": { "nLongDelta": 18, "nShortDelta": -5, "pctLongDelta": 1.2, ... }, "24h": null } }
```

**Starting census + history backfill via HyperTracker (optional, recommended):** tape discovery alone misses wallets that opened positions before launch and haven't traded since. Set `HYPERTRACKER_API_KEY` ([get one here](https://app.coinmarketman.com/hypertracker/api)) and the collector runs two one-time imports from [HyperTracker](https://docs.coinmarketman.com/), each ~1 request per coin and resumable per coin across restarts (see `seed_progress`):

1. **Census** — every currently open position per coin (`/external/positions/open/coin/{coin}`, main DEX only; HIP-3 builder-exchange rows are filtered out). Imported positions are **provisional**: every seeded wallet stays queued for verification, and the bootstrapper progressively replaces imported rows with Hyperliquid's official `clearinghouseState` (tape-active wallets keep queue priority; verified wallets are never touched by a seed). `coverage.provisional` in API responses tracks what's still awaiting verification.
2. **Count history** — their 2h-sampled long/short position-count series per coin (a recent window via the export endpoint) imported into `positioning_snapshots` with `source: "hypertracker"`, so your change-over-time chart starts before this service existed. Live rows are never overwritten; consumers can distinguish sources via the `source` field (export rows have no per-coin size breakdown or `tradersTracked`).
3. **Deep archive** (`HYPERTRACKER_DEEP_HISTORY=true`, opt-in, off by default) — walks each coin's full paginated `position-metrics` series backward to `HYPERTRACKER_DEEP_START` (default 2025-04-04, the start of their record), ~15-minute samples including size breakdowns. Resume needs no cursors: each request asks for rows older than the oldest already imported, so quota interruptions lose nothing. Coins import in open-interest order. This pass is request-hungry (tens of requests per coin): the free tier chips away at it across daily quota windows for weeks; one month of a paid tier clears the whole archive in hours.

Quota-aware: on HyperTracker's **free tier (100 requests/day)** the two passes (~350 requests total) complete automatically over ~4 days — the seeder detects quota exhaustion, pauses cleanly, and resumes on the next boot; any paid tier finishes in one pass. Check [their pricing/terms](https://docs.coinmarketman.com/endpoints/rate-limits-and-pricing) for your usage — especially if you redistribute derived data.

Honest semantics — read this before charting it:
- **Coverage grows over time.** A wallet enters the ledger the first time it trades after the tracker starts (or via the census seed above); its *full* position set (all coins, including dormant ones) is captured at bootstrap. Without a seed, counts climb steeply in the first days as the active-trader universe is discovered, then settle into real signal. `tradersTracked` / `coverage` tell you how mature the dataset is — early on, chart `pctLong` (composition) rather than raw counts.
- A wallet that never trades after launch is invisible until it does. Every wallet counts once (vaults and market makers included; note HLP itself is one wallet).
- Positions are maintained from fills with the `[buyer, seller]` convention (verified empirically: 29/33 exact matches to 1e-9 against clearinghouseState, remainder explained by in-flight fills). WebSocket gaps trigger automatic re-baselining of recently active wallets, and a rolling re-verify sweep re-checks the longest-unverified wallets — drift self-heals within hours.

### `GET /health`
`{ok, lastTickAt, tickAgeSec, ticksStale, liveCoins}` — wire this to Railway's healthcheck.

## Semantics worth knowing

- **Windows are rolling** (now vs. exactly N ago), matching how HL's own `prevDayPx` behaves. `hl24hChangePct` (HL's official number) is included alongside for comparison.
- **Funding is hourly on Hyperliquid.** `funding_hr` on ticks is the live predicted rate; `funding-history` is the settled ledger. Early history (pre-mid-2023) settled every 8h — rows carry whatever HL reports.
- **Delisted coins** stop ticking but keep their history; new listings are picked up automatically on the next tick.
- **Retention:** raw ticks 14d → 5m candles 180d → 1h candles forever. `/changes` windows are bounded by raw retention; longer lookbacks come from the candle endpoints.
- **Scale:** ~176 live coins × 4 ticks/min ≈ 1M rows/day raw, pruned at 14d ≈ 14M rows steady-state — comfortable for stock Postgres. If you later want years of raw ticks, TimescaleDB is a drop-in upgrade (deploy the `timescale/timescaledb` image as a Railway service instead of managed Postgres).
- **Redundancy:** OI can't be backfilled, so if this becomes commercial, run a second collector against a second DB (different egress IP) as insurance.

## Roadmap

- **Spot markets** — `spotMetaAndAssetCtxs` gives the same snapshot for all spot pairs; same tick/candle pattern (no OI/funding there).
- **Cross-venue funding** — `predictedFundings` returns HL vs Binance vs Bybit rates + intervals per coin → funding-arb endpoints.
- **Leaderboards & signals** — OI-up-price-down divergence, funding flips, new-listing alerts; all derivable from existing tables.
- **Gap repair** — backfill price candles from HL `candleSnapshot` after downtime (1m ≈ 3.5d retained upstream, 1h ≈ 208d).
- **Liquidation tape** — the trades firehose is already ingested for position tracking; flagging and aggregating liquidation fills is an increment on top.
- **Third-party hardening** — API keys + per-key rate limits, OpenAPI spec, SSE/WS push.
