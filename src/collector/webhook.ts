import { config } from "../config.js";
import { sleep } from "../hl/client.js";

// Push delivery for collector notifications (liquidation alerts, whale
// liquidations). One optional URL, LIQ_ALERT_WEBHOOK_URL: Discord and Slack
// incoming webhooks get a one-line message, anything else the JSON envelope.

const WEBHOOK_TIMEOUT_MS = 10_000;
const WEBHOOK_ATTEMPTS = 3;

export type WebhookFormat = "json" | "discord" | "slack";

export function webhookConfigured(): boolean {
  return config.liqAlertWebhookUrl !== "";
}

export function webhookFormat(): WebhookFormat {
  if (config.liqAlertWebhookFormat !== "auto") return config.liqAlertWebhookFormat;
  try {
    const host = new URL(config.liqAlertWebhookUrl).hostname.toLowerCase();
    if (/(^|\.)discord(app)?\.com$/.test(host)) return "discord";
    if (/(^|\.)slack\.com$/.test(host)) return "slack";
  } catch {
    /* fall through */
  }
  return "json";
}

export function formatUsd(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `$${(n / 1e3).toFixed(1)}k`;
  return `$${n.toFixed(0)}`;
}

// Posts `text` (chat formats) or `envelope` (json) with retries. Resolves to the
// delivery outcome rather than throwing: callers record it on their own rows.
export async function sendWebhook(text: string, envelope: Record<string, unknown>): Promise<{ ok: true } | { ok: false; error: string }> {
  const fmt = webhookFormat();
  const body = JSON.stringify(fmt === "discord" ? { content: text } : fmt === "slack" ? { text } : envelope);
  let lastErr = "";
  for (let attempt = 0; attempt < WEBHOOK_ATTEMPTS; attempt++) {
    if (attempt > 0) await sleep(1_000 * 2 ** (attempt - 1));
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);
    try {
      const res = await fetch(config.liqAlertWebhookUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
        signal: controller.signal,
      });
      if (res.ok) return { ok: true };
      lastErr = `HTTP ${res.status} ${(await res.text().catch(() => "")).slice(0, 200)}`.trim();
      if (res.status >= 400 && res.status < 500 && res.status !== 429) break; // not retryable
    } catch (err) {
      lastErr = err instanceof Error ? (err.name === "AbortError" ? "timeout" : err.message) : String(err);
    } finally {
      clearTimeout(timer);
    }
  }
  return { ok: false, error: lastErr.slice(0, 500) };
}
