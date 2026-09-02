import { config } from "../config.js";
import { sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";
import { startLiqAlerts } from "./alerts.js";
import { EMA_SWEEP_LAG_MS, syncEmas } from "./emas.js";
import { syncFunding } from "./funding.js";
import { startLiquidationsRecorder } from "./liquidations.js";
import { startPositionsTracker } from "./positions.js";
import { pruneOldData } from "./retention.js";
import { runSeeder, seedComplete, shouldSeed } from "./seed-hypertracker.js";
import { bootstrapRollups, runIncrementalRollups } from "./rollups.js";
import { startTradeTape } from "./tape.js";
import { collectTick } from "./ticks.js";
import { startWhaleTracker } from "./whales.js";

const RETENTION_INTERVAL_MS = 3_600_000;

// Starts the four collector loops; returns a stop function that resolves once all
// loops have wound down (used for graceful SIGTERM on Railway deploys).
export function startCollector(): () => Promise<void> {
  let stopped = false;
  const isStopped = (): boolean => stopped;

  async function tickLoop(): Promise<void> {
    let ticks = 0;
    let lastLog = 0;
    while (!stopped) {
      const started = Date.now();
      try {
        const { coins } = await collectTick();
        ticks++;
        if (Date.now() - lastLog >= 60_000) {
          log("ticks", `recorded ${coins} coins (tick #${ticks})`);
          lastLog = Date.now();
        }
      } catch (err) {
        logErr("ticks", "collect failed", err);
      }
      await pauseUntil(started + config.pollIntervalMs);
    }
  }

  async function rollupLoop(): Promise<void> {
    try {
      const chunks = await bootstrapRollups();
      if (chunks > 0) log("rollups", `bootstrapped candles over ${chunks} day chunk(s)`);
    } catch (err) {
      logErr("rollups", "bootstrap failed", err);
    }
    while (!stopped) {
      await pauseUntil(Date.now() + config.rollupIntervalMs);
      if (stopped) break;
      try {
        await runIncrementalRollups();
      } catch (err) {
        logErr("rollups", "incremental rollup failed", err);
      }
    }
  }

  async function fundingLoop(): Promise<void> {
    while (!stopped) {
      const started = Date.now();
      try {
        const { coins, inserted } = await syncFunding(isStopped);
        if (coins === 0) {
          // First boot: the asset registry fills on the first tick — retry shortly.
          await pauseUntil(Date.now() + 30_000);
          continue;
        }
        log("funding", `sweep complete: ${coins} coins, ${inserted} new settled rates`);
      } catch (err) {
        logErr("funding", "sweep failed", err);
      }
      await pauseUntil(started + config.fundingSyncIntervalMs);
    }
  }

  // EMA states only change when candles close, so the loop aligns to hour
  // boundaries (+ a small lag so HL's closes are final). Per-coin cursors make
  // extra passes nearly free, which keeps failure retries and post-boundary
  // catch-up passes cheap.
  async function emaLoop(): Promise<void> {
    while (!stopped) {
      const started = Date.now();
      let failures = 0;
      try {
        const r = await syncEmas(isStopped);
        if (r.coins === 0) {
          // First boot: the asset registry fills on the first tick — retry shortly.
          await pauseUntil(Date.now() + 30_000);
          continue;
        }
        failures = r.failures;
        if (r.requests > 0) {
          log(
            "emas",
            `sweep: ${r.requests} candle fetches over ${r.coins} coins` +
              `${r.seededTfs > 0 ? `, ${r.seededTfs} timeframes seeded` : ""}, ${r.rowsWritten} rows updated` +
              `${failures > 0 ? `, ${failures} coins failed` : ""}`,
          );
        }
      } catch (err) {
        failures = 1;
        logErr("emas", "sweep failed", err);
      }
      if (stopped) break;
      const nextHour = (Math.floor(started / 3_600_000) + 1) * 3_600_000 + EMA_SWEEP_LAG_MS;
      // Long sweeps (initial seeding) can cross an hour boundary: pauseUntil
      // returns immediately then, and the follow-up pass sweeps up the new closes.
      await pauseUntil(failures > 0 ? Date.now() + 600_000 : nextHour);
    }
  }

  async function retentionLoop(): Promise<void> {
    while (!stopped) {
      await pauseUntil(Date.now() + RETENTION_INTERVAL_MS);
      if (stopped) break;
      try {
        const pruned = await pruneOldData();
        if (pruned.ticks > 0 || pruned.candles5m > 0 || pruned.flatPositions > 0) {
          log("retention", `pruned ${pruned.ticks} raw ticks, ${pruned.candles5m} 5m candles, ${pruned.flatPositions} flat positions`);
        }
      } catch (err) {
        logErr("retention", "prune failed", err);
      }
    }
  }

  // Sleep in short slices so a stop request takes effect within ~500ms.
  async function pauseUntil(deadline: number): Promise<void> {
    while (!stopped && Date.now() < deadline) {
      await sleep(Math.min(500, Math.max(0, deadline - Date.now())));
    }
  }

  // Seed imports retry on an interval, not just at boot: API quota windows
  // (e.g. HyperTracker's free-tier daily reset) pass without a redeploy.
  async function seedLoop(): Promise<void> {
    while (!stopped) {
      try {
        await runSeeder(isStopped);
        if (await seedComplete()) {
          log("seed", "all imports complete");
          return;
        }
        log("seed", `imports incomplete — retrying in ${Math.round(config.hypertrackerRetryMs / 3_600_000)}h`);
      } catch (err) {
        logErr("seed", "seeder crashed — retrying later", err);
      }
      await pauseUntil(Date.now() + config.hypertrackerRetryMs);
    }
  }

  const loops = [tickLoop(), rollupLoop(), fundingLoop(), retentionLoop()];
  if (config.emasEnabled) {
    loops.push(emaLoop());
  }
  if (config.positionsEnabled && shouldSeed()) {
    loops.push(seedLoop());
  }
  const tape = config.positionsEnabled || config.liquidationsEnabled ? startTradeTape() : null;
  const stops: Array<() => Promise<void>> = [];
  if (tape && config.positionsEnabled) stops.push(startPositionsTracker(isStopped, tape));
  if (tape && config.liquidationsEnabled) stops.push(startLiquidationsRecorder(isStopped, tape));
  if (config.liquidationsEnabled && config.liqAlertsEnabled && config.liqAlertRules.length > 0) {
    stops.push(startLiqAlerts(isStopped));
  }
  if (config.whalesEnabled) stops.push(startWhaleTracker(isStopped, tape));
  log(
    "collector",
    `started: poll ${config.pollIntervalMs}ms, funding sweep every ${Math.round(config.fundingSyncIntervalMs / 60_000)}min, backfill ${config.fundingBackfillDays}d, positions ${config.positionsEnabled ? "on" : "off"}, liquidations ${config.liquidationsEnabled ? "on" : "off"}, liq alerts ${config.liquidationsEnabled && config.liqAlertsEnabled ? `${config.liqAlertRules.length} rules` : "off"}, emas ${config.emasEnabled ? `${config.emaPeriods.join("/")} × ${config.emaTimeframes.join("/")}` : "off"}, whales ${config.whalesEnabled ? `≥$${config.whaleMinUsd.toLocaleString("en-US")}/${config.whaleWindowHours}h` : "off"}`,
  );

  return async () => {
    stopped = true;
    tape?.stop();
    await Promise.allSettled([...loops, ...stops.map((stop) => stop())]);
    log("collector", "stopped");
  };
}
