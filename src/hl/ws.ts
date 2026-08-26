import { log, logErr } from "../log.js";

export interface WsTrade {
  coin: string;
  side: string; // "B" taker buy / "A" taker sell
  px: string;
  sz: string;
  time: number;
  hash: string;
  tid: number;
  users: [string, string]; // [buyer, seller] — verified empirically against clearinghouseState
}

interface TradesFeedOpts {
  url: string;
  getCoins: () => Promise<string[]>;
  onTrades: (trades: WsTrade[]) => void;
  // Called after reconnecting from an outage long enough that fills were missed.
  onGap: (gapMs: number) => void;
}

const PING_INTERVAL_MS = 30_000;
const STALE_MS = 75_000;
const COIN_REFRESH_MS = 60_000;
const GAP_NOTIFY_MS = 30_000;

// Resilient subscriber to the HL trades channel for every live coin:
// auto-reconnect with backoff, staleness watchdog, and subscription refresh
// so newly listed coins are picked up without a restart.
export class TradesFeed {
  private ws: WebSocket | null = null;
  private stopped = false;
  private subscribed = new Set<string>();
  private lastMessageAt = 0;
  private disconnectedAt: number | null = null;
  private reconnectDelay = 1_000;
  private timers: NodeJS.Timeout[] = [];

  constructor(private readonly opts: TradesFeedOpts) {}

  start(): void {
    this.connect();
    this.timers.push(setInterval(() => this.ping(), PING_INTERVAL_MS));
    this.timers.push(setInterval(() => void this.refreshCoins(), COIN_REFRESH_MS));
    this.timers.push(setInterval(() => this.checkStale(), 10_000));
  }

  stop(): void {
    this.stopped = true;
    for (const t of this.timers) clearInterval(t);
    this.ws?.close();
  }

  private connect(): void {
    if (this.stopped) return;
    const ws = new WebSocket(this.opts.url);
    this.ws = ws;
    ws.onopen = () => {
      this.lastMessageAt = Date.now();
      this.reconnectDelay = 1_000;
      this.subscribed.clear();
      if (this.disconnectedAt !== null) {
        const gap = Date.now() - this.disconnectedAt;
        this.disconnectedAt = null;
        if (gap > GAP_NOTIFY_MS) this.opts.onGap(gap);
      }
      void this.refreshCoins();
      log("trades-ws", "connected");
    };
    ws.onmessage = (ev) => {
      this.lastMessageAt = Date.now();
      let msg: { channel?: string; data?: WsTrade[] };
      try {
        msg = JSON.parse(String(ev.data)) as { channel?: string; data?: WsTrade[] };
      } catch {
        return;
      }
      if (msg.channel === "trades" && Array.isArray(msg.data)) this.opts.onTrades(msg.data);
    };
    ws.onclose = () => this.scheduleReconnect();
    ws.onerror = () => {
      // close always follows; reconnect is handled there
    };
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    if (this.disconnectedAt === null) this.disconnectedAt = Date.now();
    const delay = this.reconnectDelay;
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30_000);
    logErr("trades-ws", `disconnected, reconnecting in ${Math.round(delay / 1000)}s`);
    setTimeout(() => this.connect(), delay);
  }

  private ping(): void {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ method: "ping" }));
  }

  private checkStale(): void {
    if (this.stopped || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    if (Date.now() - this.lastMessageAt > STALE_MS) {
      logErr("trades-ws", "no messages for 75s, forcing reconnect");
      this.ws.close();
    }
  }

  private async refreshCoins(): Promise<void> {
    if (this.stopped || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    try {
      const coins = await this.opts.getCoins();
      for (const coin of coins) {
        if (this.subscribed.has(coin)) continue;
        this.ws.send(JSON.stringify({ method: "subscribe", subscription: { type: "trades", coin } }));
        this.subscribed.add(coin);
      }
    } catch (err) {
      logErr("trades-ws", "coin refresh failed", err);
    }
  }
}
