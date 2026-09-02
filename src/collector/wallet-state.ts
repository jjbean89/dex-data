import { pool } from "../db/pool.js";
import { hl } from "../hl/client.js";

// One clearinghouseState fetch → the wallet's positions ledger row set, replaced
// atomically, plus the traders row marked verified. Shared by the positions
// bootstrapper (tape-discovered wallets) and the whale watcher (bridge-flagged
// wallets), so both write the `positions` table the same way.

export interface WalletPosition {
  coin: string;
  szi: number;
  entryPx: number | null;
}

export interface WalletState {
  accountValue: number | null;
  totalNtlPos: number | null;
  positions: WalletPosition[];
}

function num(raw: string | undefined): number | null {
  if (raw === undefined) return null;
  const v = parseFloat(raw);
  return Number.isFinite(v) ? v : null;
}

export async function refreshWalletState(addr: string): Promise<WalletState> {
  const state = await hl.clearinghouseState(addr);
  const positions: WalletPosition[] = [];
  for (const ap of state.assetPositions) {
    const szi = parseFloat(ap.position.szi);
    if (!Number.isFinite(szi) || szi === 0) continue;
    positions.push({ coin: ap.position.coin, szi, entryPx: num(ap.position.entryPx) });
  }
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query("delete from positions where address = $1", [addr]);
    if (positions.length > 0) {
      await client.query(
        `insert into positions (address, coin, szi, entry_px, updated_at)
         select $1::text, u.*, now() from unnest($2::text[], $3::float8[], $4::float8[]) as u`,
        [addr, positions.map((p) => p.coin), positions.map((p) => p.szi), positions.map((p) => p.entryPx)],
      );
    }
    await client.query(
      `insert into traders (address, bootstrap_pending, bootstrapped_at) values ($1, false, now())
       on conflict (address) do update set bootstrap_pending = false, bootstrapped_at = now()`,
      [addr],
    );
    await client.query("commit");
  } catch (err) {
    await client.query("rollback");
    throw err;
  } finally {
    client.release();
  }
  return {
    accountValue: num(state.marginSummary?.accountValue),
    totalNtlPos: num(state.marginSummary?.totalNtlPos),
    positions,
  };
}
