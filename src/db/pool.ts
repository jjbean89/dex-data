import pg from "pg";
import { config } from "../config.js";

export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: 10,
  ...(config.pgSslNoVerify ? { ssl: { rejectUnauthorized: false } } : {}),
});

export async function closePool(): Promise<void> {
  await pool.end();
}
