import { config } from "../config.js";
import { sleep } from "../hl/client.js";

// Minimal JSON-RPC client for the Arbitrum bridge watcher: eth_blockNumber,
// eth_getLogs, and batched eth_getBlockByNumber (for log timestamps). Retries
// on 429/5xx/network errors with a hard per-request timeout, like the HL client.

const MAX_RETRIES = 3;
const REQUEST_TIMEOUT_MS = 20_000;

export interface EthLog {
  address: string;
  topics: string[];
  data: string;
  blockNumber: string; // hex
  transactionHash: string;
  logIndex: string; // hex
  removed?: boolean;
}

interface RpcResponse<T> {
  id: number;
  result?: T;
  error?: { code: number; message: string };
}

export class RpcError extends Error {
  constructor(
    readonly method: string,
    readonly code: number,
    message: string,
  ) {
    super(`arbitrum ${method}: ${message} (code ${code})`);
  }
}

async function post<T>(body: unknown, label: string): Promise<T> {
  let lastErr: unknown;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (attempt > 0) await sleep(1_000 * 2 ** (attempt - 1) + Math.random() * 500);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const res = await fetch(config.arbitrumRpcUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (res.status === 429 || res.status >= 500) {
        lastErr = new Error(`arbitrum ${label}: HTTP ${res.status}`);
        continue;
      }
      if (!res.ok) throw new Error(`arbitrum ${label}: HTTP ${res.status} ${await res.text()}`);
      return (await res.json()) as T;
    } catch (err) {
      if (err instanceof Error && (err.name === "AbortError" || err instanceof TypeError)) {
        lastErr = err;
        continue;
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(`arbitrum ${label}: retries exhausted`);
}

let nextId = 1;

async function call<T>(method: string, params: unknown[]): Promise<T> {
  const id = nextId++;
  const res = await post<RpcResponse<T>>({ jsonrpc: "2.0", id, method, params }, method);
  if (res.error) throw new RpcError(method, res.error.code, res.error.message);
  if (res.result === undefined) throw new Error(`arbitrum ${method}: empty result`);
  return res.result;
}

export const toHex = (n: number): string => `0x${n.toString(16)}`;
export const fromHex = (h: string): number => parseInt(h, 16);

export const arb = {
  blockNumber: async (): Promise<number> => fromHex(await call<string>("eth_blockNumber", [])),

  getLogs: (filter: { fromBlock: number; toBlock: number; address: string; topics: Array<string | null> }): Promise<EthLog[]> =>
    call<EthLog[]>("eth_getLogs", [
      { fromBlock: toHex(filter.fromBlock), toBlock: toHex(filter.toBlock), address: filter.address, topics: filter.topics },
    ]),

  // Block timestamps (epoch ms) for a set of block numbers, fetched in one
  // JSON-RPC batch per 50 blocks. Missing blocks are omitted from the map.
  blockTimestamps: async (blocks: number[]): Promise<Map<number, number>> => {
    const out = new Map<number, number>();
    for (let i = 0; i < blocks.length; i += 50) {
      const chunk = blocks.slice(i, i + 50);
      const batch = chunk.map((b) => ({ jsonrpc: "2.0", id: b, method: "eth_getBlockByNumber", params: [toHex(b), false] }));
      const res = await post<Array<RpcResponse<{ timestamp: string } | null>>>(batch, "eth_getBlockByNumber");
      if (!Array.isArray(res)) throw new Error("arbitrum eth_getBlockByNumber: batch response was not an array");
      for (const r of res) {
        if (r.error) throw new RpcError("eth_getBlockByNumber", r.error.code, r.error.message);
        if (r.result?.timestamp) out.set(r.id, fromHex(r.result.timestamp) * 1000);
      }
    }
    return out;
  },
};

// Public RPCs cap eth_getLogs by block range or result count with provider-specific
// messages; any of these means "ask for a smaller range".
export function isRangeTooLarge(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const m = err.message.toLowerCase();
  return (
    m.includes("block range") ||
    m.includes("too many") ||
    m.includes("more than") ||
    m.includes("exceed") ||
    m.includes("limit") ||
    m.includes("too large") ||
    m.includes("query timeout") ||
    (err instanceof RpcError && err.code === -32005)
  );
}
