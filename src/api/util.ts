// Parse "5m" / "1h" / "24h" / "3d" style windows into milliseconds. Returns null when invalid.
export function parseWindow(raw: string): number | null {
  const m = /^(\d{1,3})(m|h|d)$/.exec(raw);
  if (!m) return null;
  const n = parseInt(m[1]!, 10);
  if (n === 0) return null;
  const unit = m[2] === "m" ? 60_000 : m[2] === "h" ? 3_600_000 : 86_400_000;
  return n * unit;
}

// Accepts epoch millis (13 digits), epoch seconds (10 digits), or an ISO date string.
// undefined → not provided; null → invalid.
export function parseTimeMs(raw: string | undefined): number | null | undefined {
  if (raw === undefined || raw === "") return undefined;
  if (/^\d+$/.test(raw)) {
    const n = parseInt(raw, 10);
    if (!Number.isFinite(n)) return null;
    return n < 1e12 ? n * 1000 : n;
  }
  const d = Date.parse(raw);
  return Number.isFinite(d) ? d : null;
}

export function parseLimit(raw: string | undefined, def: number, max: number): number | null {
  if (raw === undefined || raw === "") return def;
  const n = parseInt(raw, 10);
  if (!Number.isFinite(n) || n < 1) return null;
  return Math.min(n, max);
}

export function pctChange(now: number | null, then: number | null): number | null {
  if (now === null || then === null || then === 0) return null;
  return ((now - then) / Math.abs(then)) * 100;
}

// Hourly funding rate (decimal) → annualized percentage.
export function aprPct(rateHr: number): number {
  return rateHr * 24 * 365 * 100;
}

// Tiny in-process TTL cache. Stores the promise so concurrent requests share one compute.
const cache = new Map<string, { exp: number; value: Promise<unknown> }>();

export function cached<T>(key: string, ttlMs: number, compute: () => Promise<T>): Promise<T> {
  const hit = cache.get(key);
  if (hit && hit.exp > Date.now()) return hit.value as Promise<T>;
  const value = compute().catch((err: unknown) => {
    cache.delete(key);
    throw err;
  });
  cache.set(key, { exp: Date.now() + ttlMs, value });
  if (cache.size > 500) {
    for (const [k, v] of cache) if (v.exp <= Date.now()) cache.delete(k);
  }
  return value;
}

// Trailing mean over up to n samples, skipping nulls (used for funding smoothing).
export function rollingMean(vals: Array<number | null>, n: number): Array<number | null> {
  const out: Array<number | null> = [];
  const q: Array<number | null> = [];
  let sum = 0;
  let count = 0;
  for (const v of vals) {
    q.push(v);
    if (v !== null) {
      sum += v;
      count++;
    }
    if (q.length > n) {
      const old = q.shift();
      if (old !== null && old !== undefined) {
        sum -= old;
        count--;
      }
    }
    out.push(count > 0 ? sum / count : null);
  }
  return out;
}
