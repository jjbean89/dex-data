import { assertConfig } from "../config.js";
import { log, logErr } from "../log.js";
import { migrate } from "./migrate.js";
import { closePool } from "./pool.js";

assertConfig();
try {
  await migrate();
  log("migrate", "done");
} catch (err) {
  logErr("migrate", "failed", err);
  process.exitCode = 1;
} finally {
  await closePool();
}
