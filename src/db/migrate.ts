import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pool } from "./pool.js";
import { log } from "../log.js";

// Works from both src/ (tsx) and dist/ (compiled): migrations/ sits at the repo root.
const MIGRATIONS_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../migrations");
const MIGRATE_LOCK_KEY = 727_712; // arbitrary app-wide advisory lock id

export async function migrate(): Promise<void> {
  const client = await pool.connect();
  try {
    // Serialize concurrent starts (e.g. collector + api services booting together on Railway).
    await client.query("select pg_advisory_lock($1)", [MIGRATE_LOCK_KEY]);
    await client.query(
      `create table if not exists schema_migrations (
         version int primary key,
         applied_at timestamptz not null default now()
       )`,
    );
    const applied = new Set<number>(
      (await client.query<{ version: number }>("select version from schema_migrations")).rows.map((r) => r.version),
    );
    const files = (await readdir(MIGRATIONS_DIR)).filter((f) => /^\d+_.+\.sql$/.test(f)).sort();
    for (const file of files) {
      const version = parseInt(file, 10);
      if (applied.has(version)) continue;
      const sql = await readFile(path.join(MIGRATIONS_DIR, file), "utf8");
      log("migrate", `applying ${file}`);
      await client.query("begin");
      try {
        await client.query(sql);
        await client.query("insert into schema_migrations (version) values ($1)", [version]);
        await client.query("commit");
      } catch (err) {
        await client.query("rollback");
        throw err;
      }
    }
  } finally {
    await client.query("select pg_advisory_unlock($1)", [MIGRATE_LOCK_KEY]).catch(() => undefined);
    client.release();
  }
}
