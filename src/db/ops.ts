import { pool } from "./pool.js";

// Best-effort operational event log (readable via GET /v1/ops/seed).
// Never throws — diagnostics must not break the pipeline they describe.
export async function opsEvent(tag: string, level: "info" | "warn" | "error", message: string): Promise<void> {
  try {
    await pool.query("insert into ops_events (tag, level, message) values ($1, $2, $3)", [
      tag,
      level,
      message.slice(0, 2000),
    ]);
  } catch {
    /* ignore */
  }
}
