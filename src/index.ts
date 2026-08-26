import { startApi } from "./api/index.js";
import { startCollector } from "./collector/index.js";
import { assertConfig, config } from "./config.js";
import { migrate } from "./db/migrate.js";
import { closePool } from "./db/pool.js";
import { log, logErr } from "./log.js";

assertConfig();
await migrate();

const stops: Array<() => Promise<unknown>> = [];
if (config.role === "all" || config.role === "collector") {
  stops.push(startCollector());
}
if (config.role === "all" || config.role === "api") {
  const app = await startApi();
  stops.push(() => app.close());
}
log("main", `dex-data running (role=${config.role})`);

let shuttingDown = false;
async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  log("main", `${signal} received, shutting down`);
  setTimeout(() => process.exit(1), 10_000).unref(); // hard stop if a loop hangs
  await Promise.allSettled(stops.map((stop) => stop()));
  await closePool().catch(() => undefined);
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("unhandledRejection", (err) => {
  logErr("main", "unhandled rejection", err);
  process.exit(1);
});
