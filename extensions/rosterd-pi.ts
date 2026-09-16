// rosterd-pi, R16.2: pi has no hooks file, so this extension is both the hook script and the
// missing ACP permission request. Installed by `rosterd integrate install pi`; edits are lost on
// the next install. Node builtins only; pi loads it as is.
//
//   session_start                 POST /register   same payload as rosterd-hook's SessionStart
//   before_agent_start            POST /claim      active, event prompt
//   tool_call                     POST /claim      active, event tool_call
//   tool_call with ROSTERD_GATE   POST /gate       waits for allow or deny, deny blocks the call
//   agent_end                     POST /claim      idle, event turn_end
//
// Fail open: every request 800 ms, unreachable prints one line once. The gate request has no
// client timeout; the daemon denies after harness.pi.gate_timeout_s.
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const TIMEOUT_MS = 800;
const SUMMARY_CHARS = 200;

// The daemon socket as rosterd-hook resolves it: ROSTERD_SOCKET, the platform default of R6,
// then the launchd and system unit fallbacks. First one that exists wins.
function socketPath(): string | undefined {
  const runtime = process.env.XDG_RUNTIME_DIR;
  const candidates = [
    process.env.ROSTERD_SOCKET,
    process.platform === "linux" && runtime ? path.join(runtime, "rosterd.sock") : undefined,
    path.join(os.tmpdir(), "rosterd.sock"),
    "/tmp/rosterd.sock",
    "/run/rosterd/rosterd.sock",
  ];
  return candidates.find((c) => c && fs.existsSync(c));
}

type Answer = { status: number; body: string };

function post(route: string, body: unknown, timeoutMs: number | undefined, signal?: AbortSignal): Promise<Answer> {
  return new Promise((resolve, reject) => {
    const sock = socketPath();
    if (!sock) return reject(new Error("no rosterd socket"));
    const data = JSON.stringify(body);
    const req = http.request(
      { socketPath: sock, path: route, method: "POST", headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(data) } },
      (res) => {
        let text = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => (text += chunk));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body: text }));
      },
    );
    req.on("error", reject);
    if (timeoutMs) req.setTimeout(timeoutMs, () => req.destroy(new Error("timeout")));
    signal?.addEventListener("abort", () => req.destroy(new Error("cancelled")), { once: true });
    req.end(data);
  });
}

export default function (pi: any) {
  let sessionId = "";
  let warned = false;

  const say = (ctx: any, line: string) => {
    if (ctx?.hasUI && ctx.ui?.notify) ctx.ui.notify(line, "warning");
    else process.stderr.write(line + "\n");
  };

  // Identity is the pi process, R4: the daemon reads its start ticks from the pid.
  const ident = (ctx: any) => ({ source: "hook", pid: process.pid, harness: "pi", session_id: sessionId || null, cwd: ctx?.cwd ?? process.cwd() });

  const send = async (ctx: any, route: string, body: unknown) => {
    try {
      await post(route, body, TIMEOUT_MS);
    } catch (err) {
      if (warned) return;
      warned = true;
      say(ctx, `rosterd-pi: rosterd unreachable (${(err as Error).message}); continuing without it`);
    }
  };

  const claim = (ctx: any, activity: string, event: string) =>
    send(ctx, "/claim", { ...ident(ctx), activity, event, observed_at: new Date().toISOString() });

  pi.on("session_start", async (_event: any, ctx: any) => {
    try { sessionId = ctx?.sessionManager?.getSessionId?.() ?? ""; } catch { sessionId = ""; }
    await send(ctx, "/register", {
      ...ident(ctx),
      attempt_id: process.env.WORKSPACE_ATTEMPT_ID || null,
      attempt_token: process.env.WORKSPACE_ATTEMPT_TOKEN || null,
    });
  });

  pi.on("before_agent_start", (_event: any, ctx: any) => claim(ctx, "active", "prompt"));
  pi.on("agent_end", (_event: any, ctx: any) => claim(ctx, "idle", "turn_end"));

  pi.on("tool_call", async (event: any, ctx: any) => {
    await claim(ctx, "active", "tool_call");
    const policy = process.env.ROSTERD_GATE;
    if (policy !== "attention" && policy !== "decision") return;
    let summary = "";
    try { summary = JSON.stringify(event.input ?? {}); } catch { summary = String(event.input); }
    if (summary.length > SUMMARY_CHARS) summary = summary.slice(0, SUMMARY_CHARS);
    let answer: Answer;
    try {
      // No client timeout: the daemon holds the request until allow, deny, or gate_timeout_s.
      answer = await post("/gate", { pid: process.pid, tool: event.toolName, summary, policy }, undefined, ctx?.signal);
    } catch (err) {
      const message = (err as Error).message;
      if (message === "cancelled") return { block: true, reason: "cancelled" };
      say(ctx, `rosterd-pi: gate unreachable (${message}); ${event.toolName} allowed`);
      return;
    }
    if (answer.status === 404) {
      if (!warned) say(ctx, "rosterd-pi: this rosterd has no /gate; tool calls run ungated");
      warned = true;
      return;
    }
    let outcome: { outcome?: string; reason?: string } = {};
    try { outcome = JSON.parse(answer.body); } catch { /* not JSON: treated as deny below */ }
    if (answer.status === 200 && outcome.outcome === "allow") return;
    return { block: true, reason: `rosterd denied ${event.toolName}: ${outcome.reason ?? outcome.outcome ?? `HTTP ${answer.status}`}` };
  });
}
