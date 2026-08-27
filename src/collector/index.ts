import { config } from "../config.js";
import { sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";
import { syncFunding } from "./funding.js";
import { startPositionsTracker } from "./positions.js";
import { pruneOldData } from "./retention.js";
import { runSeeder, seedComplete, shouldSeed } from "./seed-hypertracker.js";
import { bootstrapRollups, runIncrementalRollups } from "./rollups.js";
import { collectTick } from "./ticks.js";

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

  async function retentionLoop(): Promise<void> {
    while (!stopped) {
      await pauseUntil(Date.now() + RETENTION_INTERVAL_MS);
      if (stopped) break;
      try {
        const pruned = await pruneOldData();
        if (pruned.ticks > 0 || pruned.candles5m > 0) {
          log("retention", `pruned ${pruned.ticks} raw ticks, ${pruned.candles5m} 5m candles`);
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
  if (config.positionsEnabled && shouldSeed()) {
    loops.push(seedLoop());
  }
  const stopPositions = config.positionsEnabled ? startPositionsTracker(isStopped) : null;
  log(
    "collector",
    `started: poll ${config.pollIntervalMs}ms, funding sweep every ${Math.round(config.fundingSyncIntervalMs / 60_000)}min, backfill ${config.fundingBackfillDays}d, positions ${config.positionsEnabled ? "on" : "off"}`,
  );

  return async () => {
    stopped = true;
    await Promise.allSettled([...loops, ...(stopPositions ? [stopPositions()] : [])]);
    log("collector", "stopped");
  };
}
