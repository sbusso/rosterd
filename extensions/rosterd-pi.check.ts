// The extension's self-test, the pi counterpart of `rosterd-hook --self-test`: a fake daemon on a
// Unix socket, the handlers called as pi calls them, every request and every gate outcome checked.
//
//   bun extensions/rosterd-pi.check.ts
import http from "node:http";
import fs from "node:fs";
import ext from "./rosterd-pi.ts";

const sock = (process.env.TMPDIR ?? "/tmp").replace(/\/$/, "") + "/rosterd-pi-check.sock";
try { fs.unlinkSync(sock); } catch {}
const seen: { path: string; body: any }[] = [];
let gateMode: "allow" | "deny" | "404" | "hang" = "allow";
const server = http.createServer((req, res) => {
  let text = "";
  req.on("data", (c) => (text += c));
  req.on("end", () => {
    const body = JSON.parse(text);
    seen.push({ path: req.url!, body });
    if (req.url === "/gate") {
      if (gateMode === "404") { res.statusCode = 404; return res.end('{"error":"no such route"}'); }
      if (gateMode === "hang") return; // never answers; the client must not time out on its own
      return res.end(JSON.stringify(gateMode === "allow" ? { outcome: "allow" } : { outcome: "deny", reason: "nope" }));
    }
    res.end("{}");
  });
});
await new Promise<void>((r) => server.listen(sock, r));
process.env.ROSTERD_SOCKET = sock;
process.env.WORKSPACE_ATTEMPT_ID = "01JATTEMPT";
process.env.WORKSPACE_ATTEMPT_TOKEN = "wst_secret";

const handlers: Record<string, Function> = {};
ext({ on: (name: string, fn: Function) => (handlers[name] = fn) });
const notes: string[] = [];
const ctx = { cwd: "/w", hasUI: true, ui: { notify: (m: string) => notes.push(m) }, sessionManager: { getSessionId: () => "sess-1" }, signal: undefined };
const assert = (c: any, m: string) => { if (!c) { console.error("FAIL", m); process.exit(1); } };

await handlers.session_start({ reason: "startup" }, ctx);
assert(seen[0].path === "/register", "register first");
assert(seen[0].body.source === "hook" && seen[0].body.pid === process.pid && seen[0].body.harness === "pi", "ident");
assert(seen[0].body.session_id === "sess-1" && seen[0].body.cwd === "/w", "session and cwd");
assert(seen[0].body.attempt_id === "01JATTEMPT" && seen[0].body.attempt_token === "wst_secret", "attempt");

await handlers.before_agent_start({}, ctx);
assert(seen[1].path === "/claim" && seen[1].body.activity === "active" && seen[1].body.event === "prompt", "prompt claim");
assert(typeof seen[1].body.observed_at === "string", "observed_at");

// Ungated tool call: only the claim.
let out = await handlers.tool_call({ toolName: "bash", input: { command: "ls" } }, ctx);
assert(out === undefined && seen[2].path === "/claim" && seen[2].body.event === "tool_call", "tool claim");
assert(seen.length === 3, "no gate without ROSTERD_GATE");

process.env.ROSTERD_GATE = "attention";
out = await handlers.tool_call({ toolName: "bash", input: { command: "rm -rf x" } }, ctx);
assert(out === undefined, "allow passes");
const gate = seen.find((s) => s.path === "/gate")!;
assert(gate.body.pid === process.pid && gate.body.tool === "bash" && gate.body.policy === "attention", "gate body");
assert(gate.body.summary === JSON.stringify({ command: "rm -rf x" }), "summary is the args json");

gateMode = "deny";
out = await handlers.tool_call({ toolName: "write", input: { path: "/etc/x", content: "y".repeat(500) } }, ctx);
assert(out?.block === true && /nope/.test(out.reason), "deny blocks with the reason: " + JSON.stringify(out));
assert(seen.at(-1)!.body.summary.length === 200, "summary truncated to 200");

gateMode = "hang";
const ac = new AbortController();
const pending = handlers.tool_call({ toolName: "bash", input: {} }, { ...ctx, signal: ac.signal });
setTimeout(() => ac.abort(), 1200); // longer than the 800 ms claim timeout: the gate has none
out = await pending;
assert(out?.block === true && out.reason === "cancelled", "abort cancels the gate: " + JSON.stringify(out));

gateMode = "404";
out = await handlers.tool_call({ toolName: "bash", input: {} }, ctx);
assert(out === undefined && notes.some((n) => /no \/gate/.test(n)), "404 allows with a line: " + notes);

await handlers.agent_end({}, ctx);
assert(seen.at(-1)!.body.activity === "idle" && seen.at(-1)!.body.event === "turn_end", "idle claim");

// Unreachable daemon: one line, no throw, no block.
server.close();
try { fs.unlinkSync(sock); } catch {}
process.env.ROSTERD_SOCKET = "/tmp/definitely-missing-rosterd.sock";
const before = notes.length;
await handlers.before_agent_start({}, ctx);
await handlers.agent_end({}, ctx);
out = await handlers.tool_call({ toolName: "bash", input: {} }, ctx);
assert(out === undefined, "gate unreachable allows");
assert(notes.length >= before + 1 && notes.length <= before + 2, "printed once-ish: " + notes.slice(before));
console.log("ok", seen.length, "requests;", notes);
