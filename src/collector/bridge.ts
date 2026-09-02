import { arb, fromHex, isRangeTooLarge, type EthLog } from "../arbitrum/rpc.js";
import { config } from "../config.js";
import { opsEvent } from "../db/ops.js";
import { pool } from "../db/pool.js";
import { sleep } from "../hl/client.js";
import { log, logErr } from "../log.js";

// Arbitrum bridge deposit watcher.
//
// A Hyperliquid deposit is a plain USDC ERC-20 transfer to the Bridge2 contract
// on Arbitrum (the app's gas-free path uses a permit, but the Transfer event is
// still user → bridge), and the sender is the Hyperliquid account credited. So
// one eth_getLogs filter — USDC contract, Transfer topic, `to` = bridge — is a
// complete real-time feed of external money entering the venue. Each poll scans
// from the stored cursor to head minus a few confirmation blocks; the range is
// chunked and the chunk size adapts to whatever cap the RPC enforces.

export const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const ARBITRUM_BLOCKS_PER_HOUR = 14_400; // ~0.25s block time — only used to size the first-boot backfill
const MAX_CATCHUP_BLOCKS = 7 * 24 * ARBITRUM_BLOCKS_PER_HOUR;
const CHUNK_MAX = 2_000;
const CHUNK_MIN = 25;
const CATCHUP_PACE_MS = 250;

export interface BridgeDeposit {
  txHash: string;
  logIndex: number;
  blockNumber: number;
  tsMs: number;
  address: string; // lowercase
  usdc: number;
}

export interface BridgePollResult {
  deposits: BridgeDeposit[]; // rows newly inserted this poll (not previously seen)
  scannedTo: number;
  head: number;
  requests: number;
}

// Adaptive range size. Public RPCs cap eth_getLogs by block span or result count
// and only say so by rejecting the request, so the watcher halves the range on a
// rejection and, after successes, grows back by bisecting between the largest
// span that worked and the smallest that failed — a handful of wasted requests
// to converge, not one per chunk. The failed span is forgotten after an hour in
// case the limit was transient.
let chunk = CHUNK_MAX;
let largestGood = 0;
let smallestBad = CHUNK_MAX + 1;
let rejectedAt = 0;
const CEILING_TTL_MS = 3_600_000;

function onRangeOk(size: number): void {
  if (rejectedAt > 0 && Date.now() - rejectedAt > CEILING_TTL_MS) {
    smallestBad = CHUNK_MAX + 1;
    rejectedAt = 0;
  }
  if (size > largestGood) largestGood = size;
  if (chunk >= CHUNK_MAX) return;
  const ceiling = smallestBad - largestGood > 1 ? Math.floor((largestGood + smallestBad) / 2) : largestGood;
  chunk = Math.max(chunk, Math.min(CHUNK_MAX, ceiling, Math.ceil(chunk * 1.5)));
}

function onRangeRejected(size: number): void {
  if (size < smallestBad) smallestBad = size;
  if (largestGood >= smallestBad) largestGood = smallestBad - 1;
  rejectedAt = Date.now();
  chunk = Math.max(CHUNK_MIN, Math.floor(size / 2));
}

export function padTopicAddress(addr: string): string {
  return `0x${addr.toLowerCase().replace(/^0x/, "").padStart(64, "0")}`;
}

// Decodes a USDC Transfer log into a deposit (sender = credited HL account).
export function decodeDeposit(l: EthLog, tsMs: number): BridgeDeposit | null {
  if (l.removed) return null;
  if (l.topics.length < 3 || l.topics[0] !== TRANSFER_TOPIC) return null;
  const from = `0x${l.topics[1]!.slice(-40).toLowerCase()}`;
  const to = `0x${l.topics[2]!.slice(-40).toLowerCase()}`;
  if (to !== config.hlBridgeAddress) return null;
  let raw: bigint;
  try {
    raw = BigInt(l.data);
  } catch {
    return null;
  }
  const usdc = Number(raw) / 1e6;
  if (!Number.isFinite(usdc) || usdc <= 0) return null;
  return {
    txHash: l.transactionHash.toLowerCase(),
    logIndex: fromHex(l.logIndex),
    blockNumber: fromHex(l.blockNumber),
    tsMs,
    address: from,
    usdc,
  };
}

async function readCursor(): Promise<number | null> {
  const { rows } = await pool.query<{ last_block: string }>("select last_block from bridge_sync where id = 1");
  return rows[0] ? Number(rows[0].last_block) : null;
}

async function writeCursor(block: number): Promise<void> {
  await pool.query(
    `insert into bridge_sync (id, last_block, updated_at) values (1, $1, now())
     on conflict (id) do update set last_block = excluded.last_block, updated_at = now()`,
    [block],
  );
}

async function insertDeposits(rows: BridgeDeposit[]): Promise<BridgeDeposit[]> {
  if (rows.length === 0) return [];
  const { rows: inserted } = await pool.query<{ tx_hash: string; log_index: number }>(
    `insert into bridge_deposits (tx_hash, log_index, block_number, ts, address, usdc)
     select * from unnest($1::text[], $2::int[], $3::bigint[], $4::timestamptz[], $5::text[], $6::float8[])
     on conflict (tx_hash, log_index) do nothing
     returning tx_hash, log_index`,
    [
      rows.map((r) => r.txHash),
      rows.map((r) => r.logIndex),
      rows.map((r) => r.blockNumber),
      rows.map((r) => new Date(r.tsMs)),
      rows.map((r) => r.address),
      rows.map((r) => r.usdc),
    ],
  );
  const keep = new Set(inserted.map((r) => `${r.tx_hash}|${r.log_index}`));
  return rows.filter((r) => keep.has(`${r.txHash}|${r.logIndex}`));
}

// Fetch one block range with adaptive chunking; returns the logs and the range
// actually covered (the RPC may reject a range as too large — we halve and retry).
async function fetchRange(from: number, to: number): Promise<{ logs: EthLog[]; to: number; requests: number }> {
  let requests = 0;
  for (;;) {
    const end = Math.min(to, from + chunk - 1);
    try {
      requests++;
      const logs = await arb.getLogs({
        fromBlock: from,
        toBlock: end,
        address: config.arbitrumUsdcAddress,
        topics: [TRANSFER_TOPIC, null, padTopicAddress(config.hlBridgeAddress)],
      });
      onRangeOk(end - from + 1);
      return { logs, to: end, requests };
    } catch (err) {
      if (chunk > CHUNK_MIN && isRangeTooLarge(err)) {
        const size = end - from + 1;
        onRangeRejected(size);
        log("bridge", `RPC rejected a ${size}-block range — retrying with ${chunk}-block chunks`);
        continue;
      }
      throw err;
    }
  }
}

export async function pollBridge(isStopped: () => boolean): Promise<BridgePollResult> {
  const head = await arb.blockNumber();
  const safeHead = head - config.bridgeConfirmations;
  let requests = 1;
  let cursor = await readCursor();
  if (cursor === null) {
    cursor = safeHead - Math.round(config.bridgeBackfillHours * ARBITRUM_BLOCKS_PER_HOUR);
    log("bridge", `first boot — scanning the last ~${config.bridgeBackfillHours}h of bridge deposits from block ${cursor + 1}`);
  } else if (safeHead - cursor > MAX_CATCHUP_BLOCKS) {
    const skipped = safeHead - MAX_CATCHUP_BLOCKS - cursor;
    await opsEvent("bridge", "warn", `cursor ${skipped} blocks behind head — skipping ahead to a 7-day catch-up window`);
    cursor = safeHead - MAX_CATCHUP_BLOCKS;
  }
  const deposits: BridgeDeposit[] = [];
  const startCursor = cursor;
  while (cursor < safeHead && !isStopped()) {
    const r = await fetchRange(cursor + 1, safeHead);
    requests += r.requests;
    const kept: Array<{ log: EthLog; block: number }> = [];
    for (const l of r.logs) {
      // Cheap pre-filter on the amount before spending a block lookup on it.
      let usdc = 0;
      try {
        usdc = Number(BigInt(l.data)) / 1e6;
      } catch {
        continue;
      }
      if (usdc >= config.bridgeMinRecordUsd) kept.push({ log: l, block: fromHex(l.blockNumber) });
    }
    if (kept.length > 0) {
      const blocks = [...new Set(kept.map((k) => k.block))];
      const ts = await arb.blockTimestamps(blocks);
      requests += Math.ceil(blocks.length / 50);
      const rows: BridgeDeposit[] = [];
      for (const k of kept) {
        const t = ts.get(k.block);
        if (t === undefined) continue;
        const d = decodeDeposit(k.log, t);
        if (d) rows.push(d);
      }
      deposits.push(...(await insertDeposits(rows)));
    }
    cursor = r.to;
    await writeCursor(cursor);
    if (cursor < safeHead) await sleep(CATCHUP_PACE_MS);
  }
  if (safeHead - startCursor > 10 * chunk) {
    log("bridge", `caught up ${cursor - startCursor} blocks (${deposits.length} deposits ≥ $${config.bridgeMinRecordUsd})`);
  }
  return { deposits, scannedTo: cursor, head, requests };
}

export function logBridgeError(msg: string, err: unknown): void {
  logErr("bridge", msg, err);
}
