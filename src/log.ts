export function log(tag: string, msg: string): void {
  console.log(`${new Date().toISOString()} [${tag}] ${msg}`);
}

export function logErr(tag: string, msg: string, err?: unknown): void {
  const detail = err instanceof Error ? err.message : err !== undefined ? String(err) : "";
  console.error(`${new Date().toISOString()} [${tag}] ${msg}${detail ? `: ${detail}` : ""}`);
}
