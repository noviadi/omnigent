// omnigent-managed-amp-native-plugin
import fs from "node:fs";
import path from "node:path";
import type { PluginAPI, ThreadID } from "@ampcode/plugin";

type Config = { sessionId: string; serverUrl: string; authHeaders: Record<string, string>; inboxDir: string };
type AmpEvent = { id?: string; thread?: { id?: ThreadID }; message?: string; status?: string; error?: unknown; messages?: Array<{ role?: string; content?: Array<{ type?: string; text?: string }> }> };

export default function omnigentNative(amp: PluginAPI): void {
  const configPath = process.env.OMNIGENT_AMP_NATIVE_CONFIG;
  if (!configPath || !fs.existsSync(configPath)) return;
  let config: Config;
  try { config = JSON.parse(fs.readFileSync(configPath, "utf8")); } catch { return; }
  if (!config || typeof config.sessionId !== "string" || typeof config.serverUrl !== "string" ||
      typeof config.inboxDir !== "string" || !config.authHeaders || typeof config.authHeaders !== "object") return;

  let managedThreadID: ThreadID | undefined;
  let activeResponseID: string | undefined;
  const responseID = (event: AmpEvent): string => `${String(managedThreadID)}:${event.id ?? "event"}`;
  const request = async (url: string, method: string, body: unknown): Promise<boolean> => {
    try {
      const response = await fetch(url, { method, headers: { "content-type": "application/json", ...config.authHeaders }, body: JSON.stringify(body) });
      return response.ok;
    } catch { return false; }
  };
  const post = (body: unknown) => request(`${config.serverUrl}/v1/sessions/${encodeURIComponent(config.sessionId)}/events`, "POST", body);
  const persistThreadID = async (id: ThreadID): Promise<void> => {
    const url = `${config.serverUrl}/v1/sessions/${encodeURIComponent(config.sessionId)}`;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      if (await request(url, "PATCH", { external_session_id: id })) return;
      await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
    }
  };

  const drain = async (): Promise<void> => {
    try {
      for (const name of fs.readdirSync(config.inboxDir).filter((n) => n.endsWith(".json")).sort()) {
        const file = path.join(config.inboxDir, name);
        let item: { id?: string; type?: string; content?: string };
        try { item = JSON.parse(fs.readFileSync(file, "utf8")); } catch { continue; }
        if (!managedThreadID) continue;
        try {
          const thread = await amp.threads.get(managedThreadID);
          if (item.type === "interrupt") await thread.cancel();
          else continue;
          fs.unlinkSync(file);
        } catch { /* retain for retry */ }
      }
    } catch { /* fail open */ }
  };
  setInterval(() => void drain(), 100).unref?.();

  amp.on("session.start", async (event: AmpEvent) => {
    const id = event.thread?.id;
    if (!id || (managedThreadID && managedThreadID !== id)) return;
    managedThreadID = id;
    await persistThreadID(id);
  });
  amp.on("agent.start", async (event: AmpEvent) => {
    if (!managedThreadID || event.thread?.id !== managedThreadID) return;
    const response_id = responseID(event);
    activeResponseID = response_id;
    const text = event.message;
    if (typeof text === "string") await post({ type: "external_conversation_item", data: { item_type: "message", response_id, item_data: { role: "user", content: [{ type: "input_text", text }] } } });
    await post({ type: "external_session_status", data: { status: "running", response_id } });
  });
  amp.on("agent.end", async (event: AmpEvent) => {
    if (!managedThreadID || event.thread?.id !== managedThreadID) return;
    const response_id = activeResponseID ?? responseID(event);
    const text = (event.messages ?? []).filter((m) => m.role === "assistant").flatMap((m) => m.content ?? []).filter((b) => b.type === "text" && typeof b.text === "string").map((b) => b.text).join("\n");
    if (text) await post({ type: "external_assistant_message", data: { agent: "Amp", text, response_id } });
    const failed = Boolean(event.error) || event.status === "error";
    await post({ type: "external_session_status", data: { status: failed ? "failed" : "idle", response_id } });
    activeResponseID = undefined;
  });
}
