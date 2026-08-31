import { config } from "../config.js";
import { pool } from "../db/pool.js";
import { TradesFeed, type WsTrade } from "../hl/ws.js";
import { log } from "../log.js";

// One shared subscription to the HL trades firehose, fanned out to every
// consumer (position tracker, liquidations recorder) so a single socket and
// one set of per-coin subscriptions serves them all.
export interface TradeTape {
  onTrades(listener: (trades: WsTrade[]) => void): void;
  onGap(listener: (gapMs: number) => void): void;
  stop(): void;
}

export function startTradeTape(): TradeTape {
  const tradeListeners: Array<(trades: WsTrade[]) => void> = [];
  const gapListeners: Array<(gapMs: number) => void> = [];
  const feed = new TradesFeed({
    url: config.hlWsUrl,
    getCoins: async () => {
      const { rows } = await pool.query<{ coin: string }>(
        "select coin from perp_assets where is_delisted = false",
      );
      return rows.map((r) => r.coin);
    },
    onTrades: (trades) => {
      for (const listener of tradeListeners) listener(trades);
    },
    onGap: (gapMs) => {
      for (const listener of gapListeners) listener(gapMs);
    },
  });
  feed.start();
  log("tape", "trades firehose started");
  return {
    onTrades: (listener) => tradeListeners.push(listener),
    onGap: (listener) => gapListeners.push(listener),
    stop: () => feed.stop(),
  };
}
