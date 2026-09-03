# dex-data

Custom Hyperliquid market data service. Hyperliquid's public API only exposes the **current** open interest and funding rate, and only a **24h** price change — this service records the whole market on a tight loop and serves the derived data Hyperliquid can't give you:

- **Price change over any window** (5m, 1h, 4h, 24h, 7d, …) for every perp, not just 24h
- **Open interest history** — per-coin and venue-wide OI candles (this data exists nowhere in HL's API; it only exists if you record it)
- **Funding history** — settled hourly rates (synced from HL back to whatever backfill depth you choose) plus recorded live rates
- **Venue-wide aggregates** — total OI candles and OI-weighted average funding, the "Aggregated Open Interest" / "Aggregated Funding Rate" style series
- **Long/short trader counts** — how many wallets are long vs. short each coin, with change over time (derived from the public trade tape + per-wallet position tracking; this exists in no API anywhere)
- **Ticker recap** — one call per coin that returns liquidations by side, price change with an all-time-high check, open-interest change with a record-high check, and long/short trader deltas over a window, plus a pre-written headline ("$2.46M of HYPE shorts were liquidated as price broke to a new all-time high…")
- **Liquidations** — per-coin and venue-wide liquidation histograms (long vs. short notional, event counts) on any timeframe, trailing-window totals ("how much got liquidated in the last hour/day"), and a raw liquidation feed with wallets — reconstructed from public data; Hyperliquid publishes no liquidation feed at all
- **Liquidation alerts** — threshold rules per coin × trailing window (15m / 1h / 24h) × side; one alert per crossing, persisted and queryable, optionally pushed to a webhook (Discord/Slack/JSON)
- **Large liquidated accounts** — every wallet liquidated for more than a threshold ($10M by default) in one burst, with the per-coin breakdown, collected as it happens
- **Volume bars for every coin** — per-bar notional from the public trade tape (1m/5m/1h), split by taker side with TWAP flow flagged; Hyperliquid only exposes a rolling 24h number, so like OI this exists only if you record it
- **Volume-leading-price signals** — abnormal volume while price is still flat, confirmed by open-interest growth or one-sided taker flow: the accumulation that precedes a move, with breakout confirmation after the fact
- **EMAs for every coin on every timeframe** — EMA 21/200 on 1h/4h/12h/1d (all configurable), seeded from full candle history to match TradingView, plus the screener columns derived from them (price-vs-EMA %, EMA-vs-EMA cross spread) in one response
- **New whale wallets** — every wallet that bridged $1M+ (configurable) into Hyperliquid in the last hour, whether the account is brand new, and whether it has opened a position since (with the positions, and which of them were held before the money arrived) — from the Arbitrum bridge's deposit logs joined to per-wallet Hyperliquid state; Hyperliquid's own API has no deposit feed at all. Each new whale, and the first large position it opens after funding, can be pushed to Discord/Slack

One process polls `metaAndAssetCtxs` (every live perp in a single request) every 15s, rolls ticks up into 5m/1h candles, prunes raw data on a retention schedule, runs the trades-WebSocket position tracker, and polls the Arbitrum bridge for incoming deposits. A second process serves a read-only JSON API.

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

Use the **private-network** `DATABASE_URL` reference (`${{Postgres.DATABASE_URL}}`) so traffic stays internal — Railway bills public-proxy traffic as egress, and the collector talks to Postgres constantly, so the wrong URL turns into a real line item. The service logs a loud warning at boot if it sees a public-proxy host. If you must go through the proxy anyway, set `PG_SSL_NO_VERIFY=true`.

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
| `EMAS_ENABLED` | `true` | EMA tracker (see `/v1/perps/emas`) |
| `EMA_TIMEFRAMES` | `1h,4h,12h,1d` | Timeframes to maintain — any of `1h,2h,4h,8h,12h,1d` |
| `EMA_PERIODS` | `21,200` | EMA lengths per timeframe |
| `EMA_REQ_DELAY_MS` | `2000` | Pacing between candleSnapshot requests (steady state ≈ 1 request/coin/hour) |
| `EMA_RESEED_DAYS` | `7` | Re-seed each coin from full history every N days (self-heal; `0` disables) |
| `POSITIONS_ENABLED` | `true` | Long/short trader tracking (trades WebSocket + position ledger) |
| `HL_WS_URL` | `wss://api.hyperliquid.xyz/ws` | Trades feed |
| `POSITIONS_SNAPSHOT_MS` | `300000` | Long/short snapshot cadence (resolution of the history series) |
| `BOOTSTRAP_DELAY_MS` | `400` | Pacing between clearinghouseState wallet lookups |
| `POSITIONS_FLUSH_MS` / `REVERIFY_INTERVAL_MS` / `REVERIFY_BATCH` | `5000` / `6h` / `2000` | Fill-delta write cadence; self-heal sweep |
| `LIQUIDATIONS_ENABLED` | `true` | Liquidation recorder (see `/v1/perps/liquidations`) |
| `LIQ_VERIFY_DELAY_MS` | `3000` | Pacing between userFillsByTime verification requests (weight 20 each) |
| `LIQ_VERIFY_LAG_MS` / `LIQ_WALLET_COOLDOWN_MS` | `8000` / `60000` | Burst batching lag; per-wallet re-verify floor |
| `LIQ_BACKFILL_HOURS` / `LIQ_BACKFILL_WALLETS` | `6` / `250` | Downtime heal: sweep recently-active wallets over the missed window |
| `LIQ_RETENTION_DAYS` | `90` | Raw liquidation fill retention (candles follow the candle retentions) |
| `LIQ_ALERTS_ENABLED` | `true` | Liquidation threshold alerts (see `/v1/alerts`; needs the recorder) |
| `LIQ_ALERT_RULES` | `BTC:15m:15M,BTC:1h:40M,BTC:24h:100M,ETH:15m:10M,ETH:1h:15M,ETH:24h:100M` | `COIN:WINDOW:THRESHOLD[:SIDES]` list; sides `long`/`short`/`total`/`both` (default `both` = longs and shorts alerted separately) |
| `LIQ_ALERT_INTERVAL_MS` | `30000` | Rule evaluation cadence |
| `LIQ_ALERT_REARM_PCT` | `80` | A fired rule re-arms once its window value drops below this % of the threshold |
| `LIQ_ALERT_WEBHOOK_URL` / `LIQ_ALERT_WEBHOOK_FORMAT` | — / `auto` | Optional push target for liquidation alerts, whale liquidations, volume signals, and bridge-whale alerts; format `auto` picks `discord`/`slack` from the host, else raw `json` |
| `LIQ_WHALE_THRESHOLD` | `10M` | Collect any wallet liquidated for at least this much (USD) in one burst; `0` disables |
| `LIQ_WHALE_WINDOW` | `1h` | Max gap between a wallet's liquidation fills for them to count as one burst |
| `LIQ_WHALE_NOTIFY` | `true` | Also push whale liquidations to the webhook |
| `VOLUME_ENABLED` | `true` | Volume bars from the trade tape (see `/v1/perps/:coin/volume`) |
| `VOL_FLUSH_MS` / `VOL_1M_RETENTION_DAYS` | `10000` / `30` | Write cadence; 1m bar retention (5m follow `CANDLES_5M_RETENTION_DAYS`, 1h forever) |
| `VOL_SIGNALS_ENABLED` | `true` | Volume-leading-price detector (see `/v1/signals`) |
| `VOL_SIGNAL_COINS` | — (all) | Restrict the detector to a comma list of coins |
| `VOL_SIGNAL_BARS` | `3` | Window length in 5m bars |
| `VOL_SIGNAL_RVOL` / `VOL_SIGNAL_MIN_BAR_RVOL` | `5` / `2` | Window volume vs. baseline; floor every bar must clear (sustained, not one print) |
| `VOL_SIGNAL_MIN_BAR_USD` | `500k` | Ignore coins trading less than this per 5m bar |
| `VOL_SIGNAL_MAX_MOVE_ATR` | `1.5` | "Flat": net move ≤ this × the coin's typical 5m range |
| `VOL_SIGNAL_MIN_OI_PCT` / `VOL_SIGNAL_MIN_IMBALANCE_PCT` | `1` / `70` | Positioning confirmation: OI *growth* over the window, or taker share one way |
| `VOL_SIGNAL_BREAKOUT_ATR` / `VOL_SIGNAL_BREAKOUT_RVOL` | `3` / `2` | Confirms a signal when a later bar exceeds this × range on at least this × baseline volume (the cumulative-move path scales the range by √bars elapsed) |
| `VOL_SIGNAL_MAX_COINS` | `10` | More coins than this in buildup at once (open signals + new) = market-wide event, one summary per cooldown window |
| `VOL_SIGNAL_EXPIRE_MIN` / `VOL_SIGNAL_COOLDOWN_MIN` | `120` / `60` | Signal lifetime without a breakout; how long a coin stays quiet after its signal closes |
| `VOL_SIGNAL_MIN_HISTORY_HOURS` | `6` | Bars needed before a coin is eligible |
| `VOL_SIGNAL_NOTIFY` | `true` | Push signals and confirmations to the webhook |
| `WHALES_ENABLED` | `true` | Bridge deposit watcher + whale wallet tracking (see `/v1/whales/new`) |
| `ARBITRUM_RPC_URL` | `https://arb1.arbitrum.io/rpc` | Any Arbitrum One JSON-RPC endpoint (public works; a free provider key is steadier) |
| `ARBITRUM_USDC_ADDRESS` / `HL_BRIDGE_ADDRESS` | native USDC / Bridge2 | Override for testnet |
| `BRIDGE_POLL_MS` / `BRIDGE_CONFIRMATIONS` | `15000` / `10` | Poll cadence; blocks behind head to scan (reorg guard) |
| `BRIDGE_BACKFILL_HOURS` / `BRIDGE_MIN_RECORD_USD` / `BRIDGE_RETENTION_DAYS` | `6` / `1000` / `90` | First-boot scan depth; smallest deposit recorded; raw deposit retention |
| `WHALE_MIN_USD` / `WHALE_WINDOW_HOURS` | `1000000` / `1` | Flag a wallet when its deposits over the trailing window reach this |
| `WHALE_WATCH_HOURS` / `WHALE_WATCH_POLL_MS` | `24` / `60000` | How long, and how often, a flagged wallet is polled on HL for a position opened after funding |
| `WHALE_POSITION_MIN_USD` | `1000000` | The `positioned` alert needs this much notional in positions opened after funding (new coins or side flips vs. what the wallet held at its first deposit; adding to an existing position never counts); `0` = any size |
| `WHALE_ALERT_EVENTS` | `funded,positioned` | Which whale events to push to `LIQ_ALERT_WEBHOOK_URL` (shared with the liquidation alerts) |
| `WHALE_FUNDED_MAX_AGE_HOURS` | `24` | `funded` alerts only for wallets whose first-ever Hyperliquid activity is this recent |
| `HYPERTRACKER_API_KEY` | — | Enables the one-time starting-census import (see positioning docs below) |
| `HYPERTRACKER_BASE_URL` / `HYPERTRACKER_REQ_DELAY_MS` | `https://ht-api.coinmarketman.com/api` / `1500` | Census source + pacing |
| `PG_SSL_NO_VERIFY` | `false` | Accept self-signed Postgres TLS (Railway public proxy) |

## API

All responses are JSON (gzip-compressed when the client sends `Accept-Encoding: gzip`), CORS `*`, UTC timestamps (ISO + `tMs` epoch millis on series). Errors: `{"error":{"code","message"}}` with 400/404/503. Funding rates are **hourly decimals** (`0.0000125` = 0.00125%/hr ≈ 10.95% APR — `aprPct` fields do the conversion). OI is reported in coins (`openInterest`) and USD (`oiUsd` = OI × mark price).

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

### `GET /v1/perps/emas` · `GET /v1/perps/:coin/emas`
**The EMA board for your app in one request**: every live coin × timeframe × period, computed from Hyperliquid's official candles and joined with the live price. Per timeframe you get the raw EMAs plus the screener columns — `pxVsEmaPct` (% distance of the current price from each EMA) and `spreadPct` (fastest EMA vs slowest, the "cross" column: positive = 21 above 200, a sign flip = golden/death cross). Params: `tf` (comma list to subset timeframes), `coins` (comma list), `minOiUsd`, `limit`. Sorted by OI descending.

```json
{ "asOf": "2026-08-29T18:20:12.001Z", "periods": [21, 200], "timeframes": ["1h", "4h", "12h", "1d"], "count": 2,
  "data": [{ "coin": "ENA", "asOf": "2026-08-29T18:20:11.512Z", "px": 0.15588, "oiUsd": 104501821.9,
             "tfs": {
               "1h": { "t": "2026-08-29T17:00:00.000Z", "tMs": 1788022800000, "nCandles": 5000,
                        "ema": { "21": 0.158470, "200": 0.147728 },
                        "pxVsEmaPct": { "21": -1.63, "200": 5.52 }, "spreadPct": 7.27 },
               "4h": { "...": "same shape" }, "12h": { }, "1d": { } } }] }
```

Semantics worth knowing:
- **Computation matches TradingView's `ta.ema`**: seeded with the SMA of the first `period` closes of the coin's *full* candle history (HL retains ~5000 candles per interval — EMA200 is fully converged on every timeframe), then `ema = α·close + (1−α)·ema` per closed candle, α = 2/(period+1). Screeners that feed only a few hundred candles into their EMA will disagree on high timeframes — this one won't.
- `ema` values are as of the **last closed candle** (`t` = its open time); they only change when a candle closes, while `px` (and therefore `pxVsEmaPct`) is live from the latest tick. If you want the TradingView-style live line that treats the forming candle as if it closed now, compute `α·px + (1−α)·ema` client-side.
- A listing younger than `period` candles reports `null` for that EMA until enough closes exist (`nCandles` tells you how many it has). `spreadPct` is `null` whenever either end is.
- The collector seeds all coins within minutes of first boot (one paced candleSnapshot request per coin per timeframe), then stays current with ~1 hourly request per coin. New listings are picked up on the next hourly sweep; every coin is re-seeded from full history every `EMA_RESEED_DAYS` as a self-heal.

### `GET /v1/perps` · `GET /v1/perps/:coin`
Universe list (sorted by OI) and a single-coin snapshot with `changes` for 1h/4h/24h inline. Coin names are matched case-insensitively (`btc` → `BTC`; exact match wins for names like `kPEPE`). For the narrative version of the snapshot (liquidations, ATH, record OI, trader deltas, headline) see `/recap` below.

### `GET /v1/perps/:coin/recap?window=24h`
**The ticker recap in one call** — everything needed to write *"in the past 24 hours, $2.46M of HYPE shorts were liquidated as price broke to a new all-time high; traders bet heavily as HYPE longs increased by 14% and open interest reached its highest level on Hyperliquid"*: liquidations by side, price change with an all-time-high check, open-interest change with a record-high check, and long/short trader deltas over one trailing window, plus a `headline` sentence pre-written from those numbers. Params: `window` (`5m`…`14d`, rolling, default `24h`). Coin names are matched case-insensitively.

```json
{ "coin": "HYPE", "window": "24h", "from": "2026-09-02T18:47:34.646Z", "asOf": "2026-09-03T18:47:13.374Z",
  "headline": "In the past 24 hours, $2.46M of HYPE shorts were liquidated as price broke to a new all-time high of $1,000 (+25.0%). Traders bet heavily as HYPE longs increased by 14.0% (1,000 → 1,140 traders) and open interest reached its highest level ever recorded on Hyperliquid ($520M, +30.0%).",
  "flags": { "newAllTimeHigh": true, "nearAllTimeHigh": true, "oiRecordHigh": true, "liquidationsSide": "shorts", "longsIncreased": true },
  "price": { "now": 1000, "then": 800, "changePct": 25.0, "changeAbs": 200, "hl24hChangePct": 25.0,
             "windowHigh": { "px": 1010, "at": "2026-09-03T06:00:00.000Z" }, "windowLow": { "px": 790, "at": "..." },
             "allTimeHigh": { "px": 1010, "at": "2026-09-03T06:00:00.000Z", "isNewInWindow": true, "priorAthPx": 720,
                              "pctBelowAth": 0.99, "listedSince": "2024-12-05T00:00:00.000Z", "dailyCandles": 638, "source": "hl-daily-candles" } },
  "openInterest": { "nowUsd": 520000000, "thenUsd": 400000000, "changeUsd": 120000000, "changePct": 30.0, "now": 520000,
                    "windowHigh": { "usd": 530000000, "at": "..." }, "windowLow": { "usd": 390000000, "at": "..." },
                    "record": { "usd": 530000000, "at": "2026-09-03T06:00:00.000Z", "coins": 700000, "coinsAt": "...",
                                "isRecordHigh": true, "pctBelowRecord": 1.89, "recordedSince": "2026-08-24T18:00:00.000Z", "recordedDays": 10 } },
  "liquidations": { "longs": { "ntlUsd": 199800, "events": 1, "fills": 1 }, "shorts": { "ntlUsd": 2461000, "events": 2, "fills": 3 },
                    "totalNtlUsd": 2660800, "events": 3, "shortSharePct": 92.5, "dominantSide": "shorts" },
  "positioning": { "nLong": 1140, "nShort": 760, "nTraders": 1900, "pctLong": 60.0, "ntlLongUsd": 300000000, "ntlShortUsd": 180000000,
                   "then": { "nLong": 1000, "nShort": 800, "...": "same shape" },
                   "changes": { "nLongDelta": 140, "nLongChangePct": 14.0, "nShortDelta": -40, "nShortChangePct": -5.0,
                                "pctLongDelta": 4.44, "ntlLongChangePct": 50.0, "ntlShortChangePct": 12.5 },
                   "coverage": { "tracked": 9184, "pending": 1201, "provisional": 0 } },
  "funding": { "hr": 0.00003, "aprPct": 26.28, "hrThen": 0.00001 }, "dayNtlVlm": 1500000000, "maxLeverage": 10 }
```

Where each block comes from, and what the flags mean:
- **`liquidations`** — exact trailing-window totals from raw fills (same numbers as `/v1/perps/liquidations`). `dominantSide` is `shorts`/`longs` when one side is ≥ 60% of liquidated notional, else `balanced` (`none` when nothing was liquidated); the headline names the dominant side.
- **`price` / `openInterest` change** — now vs. the recorded tick nearest `now − window`, identical to `/changes` (so `null` until the window has history). `windowHigh`/`windowLow` come from the 5m candles plus the live tick.
- **`allTimeHigh`** — from Hyperliquid's own daily candles (the full listing history, fetched once per coin per 5 minutes; `null` if the fetch fails or the coin has no candles). `isNewInWindow` is true when a high above every prior high printed inside the window; the pre-window part of the day that straddles the window start is resolved from our 5m candles, so a window shorter than a day still gets a precise answer. `pctBelowAth` is the current price's distance from the high (`0` = at the high); `flags.nearAllTimeHigh` = within 5%.
- **`openInterest.record`** — the highest OI **this service has recorded** (1h candles are kept forever, so the record deepens the longer the collector runs — `recordedSince`/`recordedDays` say how deep it is). `isRecordHigh` is true when that record was set inside the window; the headline then says "highest level ever recorded on Hyperliquid".
- **`positioning`** — the long/short trader counts now vs. the snapshot nearest `now − window`, with count and notional deltas (`nLongChangePct` is the "longs increased by X%" number). `null` while the tracker is warming up or when `POSITIONS_ENABLED=false`; `changes` is `null` when no snapshot exists that far back.
- **`headline`** — assembled from the fields above, so it degrades honestly: sides, directions, and the "all-time high" / "record open interest" claims only appear when the data supports them, and blocks that are `null` are left out of the sentence. Everything in it is also present as a number, so you can write your own copy from the same payload.

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

### `GET /v1/perps/:coin/liquidations` · `GET /v1/market/liquidations`
**Liquidation histograms** — the "aggregated liquidations" pane under a chart: long vs. short liquidated notional and event counts per bucket, per coin or venue-wide. Params: `interval` (`5m|15m|1h|4h|12h|1d`, default `1h`), `from`/`to`/`limit` as on the candle endpoints. Buckets with no liquidations are omitted — align by timestamp when charting. `events` counts forced liquidation orders (all fills of one forced order share a timestamp and wallet); `fills` counts raw prints.

```json
{ "coin": "BTC", "interval": "4h", "count": 2,
  "data": [{ "t": "2026-08-30T12:00:00.000Z", "tMs": 1788091200000,
             "longs":  { "ntlUsd": 184301.2, "events": 3, "fills": 9 },
             "shorts": { "ntlUsd": 4291822.55, "events": 41, "fills": 118 },
             "totalNtlUsd": 4476123.75, "events": 44 }] }
```

### `GET /v1/perps/liquidations?windows=1h,24h`
**The liquidation board** — per-coin totals over trailing windows plus the venue-wide sum, in one response: "how many liquidations in the past hour / day, for every coin". Params: `windows` (comma list, `15m`…`{LIQ_RETENTION_DAYS}d`, default `1h,24h`), `sort` (`ntl|events`, applied to the first window), `dir`, `limit`. Windows are computed exactly from raw fills, not bucket-aligned.

```json
{ "windows": ["1h", "24h"], "lastLiqAt": "2026-08-30T17:09:58.664Z", "count": 37,
  "totals": { "1h":  { "longs": { "ntlUsd": 12007.9, "events": 2, "fills": 2 },
                        "shorts": { "ntlUsd": 861204.1, "events": 55, "fills": 240 },
                        "totalNtlUsd": 873212.0, "events": 57 },
              "24h": { "...": "same shape" } },
  "data": [{ "coin": "BTC", "windows": { "1h": { "...": "..." }, "24h": { "...": "..." } } }] }
```

### `GET /v1/perps/:coin/liquidations/recent` · `GET /v1/market/liquidations/recent`
The raw liquidation tape, newest first: `{t, side, px, sz, ntlUsd, wallet, method, tid}` (`side` = which side got liquidated; `wallet` = the liquidated address — public on-chain data; `method` = `market` for order-book liquidations, `backstop` for liquidator-vault takeovers). `/v1/perps/:coin` also carries a `liquidations` block with 1h/24h totals inline.

How this works — and its honest semantics (Hyperliquid has **no** liquidation feed):
- **Detection.** Forced closes print on the public trades WebSocket looking exactly like normal trades (same shape, real hash — verified empirically; the all-zero-hash prints are TWAP fills, not liquidations). But a wallet's fills from `userFillsByTime` carry an explicit `liquidation` marker on **both parties** of a liquidation print, naming the liquidated wallet. So the recorder classifies tape trades by verifying wallets: one paced request classifies *every* trade that wallet touched in the window.
- **Coverage is deliberately concentrated, and measured.** The verify budget (`LIQ_VERIFY_DELAY_MS`, ~400 weight/min of HL's 1200/min at default) goes to the wallet covering the most unclassified trades. Market makers sit on one side of most flow (top 30 wallets ≈ ⅔ of all trades, measured live), and a forced order splinters into several prints, pushing cascade victims up the queue — so liquidation flow is caught at well above the raw trade-coverage rate. The collector logs its live classification coverage every 5 minutes; trades unclassified after 15 minutes are dropped from the queue and counted against coverage. Treat totals as a floor, tight in practice.
- **Downtime heals.** Unlike open interest, liquidation history is recoverable after the fact: on boot and after WebSocket gaps the recorder sweeps recently-active wallets' fills over the missed window (`LIQ_BACKFILL_HOURS`). Deeper backfills are possible by temporarily raising it.
- **Backstop liquidations** (liquidator-vault takeovers below ⅔ maintenance margin) don't print on the book; they're recorded when a swept wallet's fills reveal them, flagged `method: "backstop"`.
- Idempotent by construction: fills are keyed by HL's trade id, and candle buckets are recomputed from raw fills in the same transaction, so re-discovery and late verification never double-count.

### `GET /v1/perps/:coin/volume?interval=1m|5m|1h`
**Volume bars from the trade tape.** Hyperliquid's API only carries a rolling 24h volume per coin; these are real per-bar numbers, summed from every print on the trades WebSocket. Each bar: OHLC of trade prices, `ntlUsd`, taker-side split (`buyNtlUsd` / `sellNtlUsd` / `deltaUsd` / `buySharePct`), `twapNtlUsd` (flow from TWAP prints — the all-zero-hash fills), size, `vwap`, `trades`. `from`/`to`/`limit` as on the candle endpoints. Bars with no prints are omitted. Recorded from the moment the collector starts — like open interest there is no backfill, and a restart loses the seconds between its last flush and the boot.

### `GET /v1/perps/volume?bars=3`
**The volume board**: every coin's relative volume over the last `bars` 5m bars against its own baseline, sorted by `rvol`, plus the numbers the detector below uses: `pxMovePct` and `moveAtr` (the move in units of the coin's typical 5m range), `buySharePct`, `twapSharePct`, `oiChangePct`. Params: `bars` (1–12), `sort` (`rvol|volume`), `minBarUsd`, `limit`. Cached 30s. `rvol` is `null` until a coin has a day of bars.

Baseline = the median 5m notional over the trailing 24h, blended 50/50 with the median of the same time-of-day slots over the prior week once a week of bars exists (volume has a strong intraday cycle; a plain trailing average fires every day at the US open). Medians, not means, so one spike bar doesn't poison its own baseline for a day. The live bar counts once a minute of it has elapsed, with its expectation pro-rated.

### `GET /v1/signals`
**Volume leading price.** The collector evaluates every coin each minute and fires a *buildup* signal when abnormal volume arrives while price is still flat — and something confirms positions are being built rather than churned:

| Condition | Default |
|---|---|
| Window volume vs. baseline | ≥ 5× over 3 bars (15m), every bar ≥ 2× |
| Minimum size | ≥ $500k per 5m bar |
| Price flat | net move ≤ 1.5 × typical 5m range |
| Positioning | open interest **grew** ≥ 1% over the window, **or** taker flow ≥ 70% one way |

`bias` is `long` / `short` / `mixed` from the taker split (and OI direction). A signal stays `open` (one per coin) and is **confirmed** when a subsequent 5m bar moves more than 3× the coin's range on at least 2× baseline volume, or when the cumulative move since the signal exceeds 3× range × √(bars elapsed) while volume is still elevated (price drifts about √n ranges over n bars on its own, so an unscaled cumulative test would confirm nearly everything inside two hours); it **expires** after 2h otherwise. After a signal closes either way the coin stays quiet for `VOL_SIGNAL_COOLDOWN_MIN` (1h), so a buildup that keeps running doesn't re-fire the minute its signal confirms. Confirmed vs. expired counts are the detector's scorecard — tune the thresholds from `/v1/signals?status=confirmed` against `status=expired` once a couple of weeks of bars exist.

Params: `coin`, `status` (`open|confirmed|expired`), `since`, `marketWide`, `limit`. Each row carries the window numbers at fire time (`rvol`, `volNtlUsd` vs `expectedNtlUsd`, `pxMovePct`, `atrPct`, `oiChangePct`, `buySharePct`, `twapSharePct`), the confirmation (`breakoutMovePct`, `breakoutRvol`, `confirmedAt`) and delivery status.

```json
{ "count": 1,
  "data": [{ "id": "12", "coin": "HYPE", "firedAt": "2026-09-02T17:41:02.113Z", "status": "confirmed", "bias": "long", "marketWide": false,
             "window": { "from": "2026-09-02T17:25:00.000Z", "to": "2026-09-02T17:35:00.000Z", "bars": 3 },
             "volNtlUsd": 9120000, "expectedNtlUsd": 1610000, "rvol": 5.66, "minBarRvol": 2.9,
             "pxFrom": 43.71, "pxTo": 43.86, "pxMovePct": 0.34, "atrPct": 0.41,
             "oiFromUsd": 412000000, "oiToUsd": 425600000, "oiChangePct": 3.3, "buySharePct": 64.2, "twapSharePct": 8.1,
             "confirmedAt": "2026-09-02T18:03:02.510Z", "breakoutMovePct": 4.8, "breakoutRvol": 61.3,
             "message": "HYPE: 5.7x volume over 15min ($9.12M vs $1.61M expected), price +0.34% (typical 5m range 0.41%), OI +3.30%, taker buys 64% → long buildup",
             "delivered": true }] }
```

Honest semantics:
- **Market-wide events are separated out.** When more than `VOL_SIGNAL_MAX_COINS` coins are in buildup at once — signals still open plus the ones triggering this pass (a venue-wide move, a macro print; the coins trip over several minutes, rarely in the same one) — the new rows are recorded with `marketWide: true` and the webhook gets a single summary per cooldown window instead of one message per coin; their confirmations aren't pushed either.
- **Rvol is against a median.** The baseline is the coin's *median* 5m notional, so a thin coin whose typical bar is $4k prints "180× volume" on $600k; that is what `VOL_SIGNAL_MIN_BAR_USD` is for. Raise it (or set `VOL_SIGNAL_COINS`) if the small caps are the noise.
- **Known false positives**: funding settlement at the top of the hour, large TWAPs (`twapSharePct` tells you when the "accumulation" is a scheduled order), listings younger than `VOL_SIGNAL_MIN_HISTORY_HOURS`, and market-maker churn — the OI/flow confirmation exists to drop the last one.
- Signals and confirmations go to `LIQ_ALERT_WEBHOOK_URL` as `📈 …` / `✅ …` messages (JSON envelopes `volume_buildup`, `volume_breakout`, `volume_market_wide`).

### `GET /v1/alerts` · `GET /v1/alerts/rules`
**Liquidation threshold alerts.** The collector evaluates every rule in `LIQ_ALERT_RULES` — a coin, a trailing window, a threshold in USD notional, and which side(s) — against the exact trailing-window totals from the raw liquidation fills (the same numbers `/v1/perps/liquidations` reports). Out of the box:

| Coin | 15m | 1h | 24h |
|---|---|---|---|
| BTC | $15M | $40M | $100M |
| ETH | $10M | $15M | $100M |

Each threshold applies to **longs liquidated and shorts liquidated separately** (`both`, the default): "$15M of BTC longs liquidated in 15 minutes" and "$15M of BTC shorts liquidated in 15 minutes" are two rules with independent state. Append `:total` to a rule to alert on the combined number instead, or `:long` / `:short` for one side only. Thresholds take `k`/`M`/`B` suffixes; windows are anything from `1m` up to `LIQ_RETENTION_DAYS`.

`GET /v1/alerts` is the alert history, newest first. Params: `coin`, `window`, `side` (`long|short|total`), `since` (epoch ms/s or ISO), `limit` (default 100, max 1000).

```json
{ "count": 1,
  "data": [{ "id": "42", "t": "2026-09-02T14:53:13.167Z", "tMs": 1788360793167,
             "coin": "BTC", "window": "15m", "side": "long",
             "ntlUsd": 16000000, "thresholdUsd": 15000000, "pctOfThreshold": 106.67,
             "events": 8, "fills": 8,
             "longs": { "ntlUsd": 16000000, "events": 8 }, "shorts": { "ntlUsd": 0, "events": 0 }, "totalNtlUsd": 16000000,
             "message": "BTC: $16.00M of longs liquidated in the last 15m (8 forced orders; threshold $15.00M)",
             "delivered": true, "deliveryError": null }] }
```

`GET /v1/alerts/rules` is the live board: every rule with its threshold, the current window value, `pctOfThreshold` (how close the next alert is), `active` (fired and not yet re-armed), and `lastFiredAt`. `evaluatorStale` flips true when the collector hasn't evaluated in 5 minutes.

Semantics:
- **Edge-triggered with hysteresis.** A rule fires once when its window value crosses the threshold, then stays `active` until the value drops below `LIQ_ALERT_REARM_PCT`% of the threshold (fills aging out of the trailing window). A cascade produces one alert per rule, not one per evaluation, and a value hovering around the threshold doesn't flap. Once re-armed, a fresh crossing fires again.
- **State survives restarts** (`liq_alert_rules`): an alert already sent is not re-sent after a redeploy, and a crossing that happened during downtime fires on boot with the current value.
- **Values are the recorder's floors.** Liquidations are classified a few seconds to minutes after they print (and backfilled after downtime), so the window value can keep climbing after an alert; the alert carries the value at detection time. See the liquidation recorder's coverage notes above.
- **Delivery.** Every alert is logged (`[alerts] ALERT …`), stored, and — with `LIQ_ALERT_WEBHOOK_URL` set — POSTed as JSON. Discord and Slack incoming-webhook URLs are auto-detected and receive a one-line message (`content` / `text`); anything else gets `{ "type": "liquidation_threshold", "alert": { …same shape as /v1/alerts… } }` (whale liquidations arrive on the same webhook as `{ "type": "whale_liquidation", "whale": { … } }`). Three attempts with backoff; the outcome lands in `delivered` / `deliveryError` on the row and failures are also written to `ops_events`.
- Rule changes take effect on the next collector boot. Coin names are Hyperliquid's exact spelling (`BTC`, `ETH`, `kPEPE`); an unknown coin is logged as a warning rather than silently never firing.

### `GET /v1/market/liquidations/whales`
**Large liquidated accounts.** Every wallet whose liquidations in one burst add up to at least `LIQ_WHALE_THRESHOLD` ($10M by default), newest first, with what got liquidated. A burst is one wallet's liquidation fills, across all coins, with no gap longer than `LIQ_WHALE_WINDOW` — so a position taken down in several partial liquidations over a few minutes is one record, and a wallet liquidated on BTC and ETH in the same cascade is one record with both. Params: `wallet`, `coin`, `since`, `minNtlUsd`, `active` (`true` = burst still going), `limit` (default 50, max 500).

```json
{ "count": 1,
  "data": [{ "id": "7", "wallet": "0x…", "explorer": "https://app.hyperliquid.xyz/explorer/address/0x…",
             "detectedAt": "2026-09-02T15:20:41.002Z", "from": "2026-09-02T15:18:02.113Z", "to": "2026-09-02T15:20:19.870Z",
             "toMs": 1788362419870, "durationSec": 138,
             "ntlUsd": 13000000, "thresholdUsd": 10000000, "events": 3, "fills": 9,
             "coins": [{ "coin": "BTC", "side": "long", "ntlUsd": 6000000, "events": 1, "fills": 4 },
                       { "coin": "ETH", "side": "long", "ntlUsd": 7000000, "events": 2, "fills": 5 }],
             "active": false, "delivered": true, "deliveryError": null }] }
```

Semantics:
- **Records grow while the burst is open.** A wallet is recorded the moment its burst crosses the threshold (and pushed to the webhook once, as `🐋 …`); further liquidations within the window fold into the same record (`ntlUsd`, `events`, `coins`, `to` update) until the window passes with no new fills, then it freezes. Later liquidations of the same wallet start a new record.
- **Late discovery still counts.** The tracker keys on when fills were recorded, not when they traded, so liquidations the recorder classifies late or backfills after downtime are collected too (a burst older than the window is recorded already closed).
- Wallets are public on-chain addresses; `explorer` links to Hyperliquid's own explorer. Totals are the recorder's floors (see coverage notes above) — a large liquidation is exactly the flow the verification queue prioritises, so these are caught reliably in practice.
### `GET /v1/whales/new?window=1h&minUsd=1000000`
**New whale wallets**: every address whose deposits into Hyperliquid over the trailing window total at least `minUsd`, with what the tracker has learned about each on Hyperliquid — account value, account age, when it first traded, and its open positions marked at the live price, each flagged `openedAfterFunding` (not held, or held the other way, when this episode's first deposit landed; `null` until the tracker's first check). `positionedAt` is the first time such a position was seen. Params: `window` (`1m`…`{BRIDGE_RETENTION_DAYS}d`, default `1h`), `minUsd` (default `WHALE_MIN_USD`), `positioned` (`true` = only wallets with an open position right now, `false` = only funded-but-idle ones), `newOnly=true` (only brand-new accounts — first ledger entry ever is inside this deposit episode), `limit`. Sorted by deposited amount.

```json
{ "window": "1h", "minUsd": 1000000, "asOf": "2026-09-02T15:40:12.001Z",
  "bridge": { "lastBlock": 380412991, "syncedAt": "2026-09-02T15:40:03.512Z", "syncAgeSec": 9, "stale": false, "lastDepositAt": "2026-09-02T15:39:58.000Z" },
  "count": 1,
  "data": [{ "address": "0x9a3c…e41b",
             "deposits": { "usd": 2500000, "n": 2, "firstAt": "2026-09-02T15:02:11.000Z", "lastAt": "2026-09-02T15:04:40.000Z" },
             "flaggedAt": "2026-09-02T15:04:52.118Z", "watchUntil": "2026-09-03T15:04:40.000Z",
             "account": { "valueUsd": 2498120.4, "totalNtlPosUsd": 7400000.0, "checkedAt": "2026-09-02T15:39:31.204Z" },
             "ledgerFirstAt": "2026-09-02T15:02:11.000Z", "isNewAccount": true, "accountAgeDays": 0.03,
             "firstTradeAt": "2026-09-02T15:11:08.416Z", "positionedAt": "2026-09-02T15:11:08.416Z", "hasOpenPosition": true,
             "openNtlUsd": 7400000.0,
             "positions": [{ "coin": "BTC", "side": "long", "sz": 95.2, "ntlUsd": 7400000.0, "entryPx": 77510.0, "markPx": 77731.1,
                             "unrealizedPnlUsd": 21048.7, "openedAfterFunding": true, "updatedAt": "2026-09-02T15:39:31.204Z" }] }] }
```

How this works — Hyperliquid's API cannot answer this question by itself:
- **Every account endpoint on Hyperliquid is keyed by wallet** (the deposit ledger included) and the WebSocket's only global channels are market data, so there is nothing to poll or subscribe to for "who just got funded". But external money enters through the **Bridge2 contract on Arbitrum** as a plain USDC transfer whose *sender is the Hyperliquid account credited* (the app's gas-free path uses a permit; the Transfer event is still user → bridge). One `eth_getLogs` filter on the USDC contract with the bridge as recipient is therefore a complete, real-time feed of external deposits. The collector polls it every 15s from a stored block cursor (chunked, with the chunk size adapting to whatever cap the RPC enforces), records deposits ≥ `BRIDGE_MIN_RECORD_USD`, and flags any address whose deposits over the trailing `WHALE_WINDOW_HOURS` reach `WHALE_MIN_USD`. Downtime heals itself: the cursor resumes where it stopped (up to 7 days back).
- **Flagged wallets are then watched on Hyperliquid.** One `clearinghouseState` (weight 2) per poll captures account value and open positions until a large position opened after funding appears (the `positioned` alert), then every 15 minutes for `WHALE_WATCH_HOURS`. The first check also reads the wallet's ledger from time zero (`userNonFundingLedgerUpdates`) — its first entry is the account's age, which is what `isNewAccount` and `accountAgeDays` report: a brand-new whale versus a known one topping up — and reconstructs what the wallet held *before* the deposit: its current positions with every fill since the first deposit unwound (one `userFillsByTime`). That baseline is what separates a new position from a top-up: a known account adding to thirty open shorts is not news, the same account opening a fresh one is. If the trade tape is on, a watched wallet's first fill is caught the moment it prints (both parties of every fill are on the public tape) and triggers an immediate state refresh — `firstTradeAt` is exact to the fill, at zero API cost.
- `positioned` filters on the **live** positions ledger (a wallet that opened and closed reads `false` with `positionedAt` set); the positions listed come from the same ledger the long/short tracker maintains, so they stay current from fills after the first check. `openNtlUsd` and `ntlUsd` are marked at the latest tick.
- **What it misses, honestly:** money that never touches Arbitrum — transfers between Hyperliquid accounts, sub-account funding, HyperEVM→Core moves — and native-asset deposits through Hyperunit (those land as spot BTC/ETH/SOL, not USDC margin). A new wallet funded from an existing whale's Hyperliquid account is invisible here. A deposit routed through a contract that forwards funds could credit an address other than the transfer sender; the tracker records the sender.
- Requests: the bridge watcher costs ~8 RPC calls per minute (head + logs per poll) plus one batched block lookup per poll that found deposits; whale polling is a handful of weight-2 calls per minute at most.

### `GET /v1/whales/alerts?kind=funded|positioned`
**Whale alerts** — the push feed behind the board. Two events per whale episode, each sent at most once (state survives restarts):

- `funded` — a **fresh** wallet crossed `WHALE_MIN_USD`: its first-ever Hyperliquid ledger entry is within `WHALE_FUNDED_MAX_AGE_HOURS` (default 24h). Sent right after the tracker's first Hyperliquid check, which reads the wallet's ledger from time zero, so it says how new the account is and what its perps account is worth. A known whale topping up is logged as skipped and never pushed — it still appears on `/v1/whales/new` and still gets a `positioned` alert.
- `positioned` — a flagged wallet opened a position it did not hold when it was funded: a new coin, or a side flip, never an addition to an existing position. Sent once per episode, the first time such positions add up to `WHALE_POSITION_MIN_USD` of notional, with those positions (side, notional, entry), how many positions the wallet already held before funding, total notional, and account value.

Every alert is logged (`[whales] ALERT …`), stored, and — with `LIQ_ALERT_WEBHOOK_URL` set, the same webhook the liquidation alerts use — POSTed. Discord and Slack incoming-webhook URLs are auto-detected and receive a one-line message ending in the wallet's explorer link; anything else gets `{type: "whale_funded" | "whale_positioned", alert: {…}}` with the structured fields (`opened` lists the newly opened positions, `positions` the wallet's whole book). Delivery is retried, and the stored row records `delivered` / `deliveryError`. `WHALE_ALERT_EVENTS` narrows which events are sent. Params: `kind`, `address`, `since` (epoch ms/s or ISO), `limit` (default 100, max 1000).

```
🐋 New whale funded: 0x9a3c…e41b bridged $2.50M into Hyperliquid in several deposits (latest 2 min ago) — brand-new account (first activity 4 min ago), perps account $2.50M. https://app.hyperliquid.xyz/explorer/address/0x9a3c…
🐋 Whale opened a new position: 0x9a3c…e41b long BTC $7.40M @ 77510 — $7.40M newly opened, $7.40M total notional, account $2.50M. Bridged $2.50M 9 min ago, brand-new account (first activity 11 min ago). https://app.hyperliquid.xyz/explorer/address/0x9a3c…
🐋 Whale opened a new position: 0x7fda…17d1 long HYPE $3.10M @ 41.2 — $3.10M newly opened on top of 32 positions held before funding, $155.97M total notional, account $42.49M. Bridged $2.00M 31 min ago, account 24mo old. https://app.hyperliquid.xyz/explorer/address/0x7fda…
```

```json
{ "webhook": true, "events": ["funded", "positioned"], "count": 1,
  "data": [{ "id": "7", "t": "2026-09-02T15:11:10.204Z", "tMs": 1788361870204, "kind": "positioned", "address": "0x9a3c…e41b",
             "depositedUsd": 2500000, "accountValueUsd": 2498120.4, "totalNtlPosUsd": 7400000.0,
             "isNewAccount": true, "ledgerFirstAt": "2026-09-02T15:02:11.000Z",
             "positions": [{ "coin": "BTC", "side": "long", "sz": 95.2, "entryPx": 77510.0 }],
             "opened": [{ "coin": "BTC", "side": "long", "sz": 95.2, "entryPx": 77510.0, "ntlUsd": 7400000.0 }],
             "message": "Whale opened a new position: …", "delivered": true, "deliveryError": null }] }
```

### `GET /v1/bridge/deposits?window=24h&minUsd=100000`
The raw deposit tape behind the whale board, newest first: `{t, address, usdc, txHash, logIndex, block}` for every recorded bridge deposit ≥ `minUsd` in the window (default `100000`; anything down to `BRIDGE_MIN_RECORD_USD` is available). Same `bridge` freshness block as above.

### `GET /health`
`{ok, lastTickAt, tickAgeSec, ticksStale, liveCoins}` — wire this to Railway's healthcheck.

## Semantics worth knowing

- **Windows are rolling** (now vs. exactly N ago), matching how HL's own `prevDayPx` behaves. `hl24hChangePct` (HL's official number) is included alongside for comparison.
- **Funding is hourly on Hyperliquid.** `funding_hr` on ticks is the live predicted rate; `funding-history` is the settled ledger. Early history (pre-mid-2023) settled every 8h — rows carry whatever HL reports.
- **Delisted coins** stop ticking but keep their history; new listings are picked up automatically on the next tick.
- **Retention:** raw ticks 14d → 5m candles 180d → 1h candles forever (volume: 1m bars 30d, 5m 180d, 1h forever). `/changes` windows are bounded by raw retention; longer lookbacks come from the candle endpoints.
- **Scale:** ~176 live coins × 4 ticks/min ≈ 1M rows/day raw, pruned at 14d ≈ 14M rows steady-state — comfortable for stock Postgres. If you later want years of raw ticks, TimescaleDB is a drop-in upgrade (deploy the `timescale/timescaledb` image as a Railway service instead of managed Postgres).
- **Redundancy:** OI can't be backfilled, so if this becomes commercial, run a second collector against a second DB (different egress IP) as insurance.
- **Cost levers**, in descending order of impact, if the Railway bill needs trimming: keep `DATABASE_URL` on the private network (see above); `POSITIONS_ENABLED=false` drops the wallet bootstrapper and the two largest tables (the WebSocket firehose stays if liquidations are on); `LIQUIDATIONS_ENABLED=false` drops the liquidation recorder and its verification traffic; `WHALES_ENABLED=false` drops the bridge watcher (no HL budget to speak of, but one fewer external dependency); `POLL_INTERVAL_MS=30000` halves raw-tick volume and write load with candles still built from 10 ticks per 5m bucket; `REVERIFY_BATCH` scales the background clearinghouseState traffic; `RAW_RETENTION_DAYS` bounds the biggest table. Everything already in place — watermarked rollups, batched writes, response compression, capped retention — needs no tuning.

## Roadmap

- **Spot markets** — `spotMetaAndAssetCtxs` gives the same snapshot for all spot pairs; same tick/candle pattern (no OI/funding there).
- **Cross-venue funding** — `predictedFundings` returns HL vs Binance vs Bybit rates + intervals per coin → funding-arb endpoints.
- **Leaderboards & signals** — OI-up-price-down divergence, funding flips, new-listing alerts; all derivable from existing tables (liquidation threshold alerts exist today — see `/v1/alerts`).
- **Gap repair** — backfill price candles from HL `candleSnapshot` after downtime (1m ≈ 3.5d retained upstream, 1h ≈ 208d).
- **Third-party hardening** — API keys + per-key rate limits, OpenAPI spec, SSE/WS push.
