//! `rosterd acp`, R20: the whole swarm as one ACP agent on stdin and stdout. A session id is
//! a roster key, so one connection reaches every headless session on every node: `session/list`
//! is the swarm, `session/new` starts a session on the node and harness named in
//! `_meta.rosterd` (else the command's defaults), `session/load` joins a running one by key or
//! name. Everything else per session is relayed to the daemon's `/sessions/{key}/acp`
//! websocket, which the owner node serves and any other relays to.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use reqwest::Method;
use rosterd_proto::{Lane, Record, SwarmSnapshot};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use super::client::parse;
use super::{Client, Exit, Out};
use crate::config::Config;
use crate::node::VERSION;
use crate::runner::acp;

const INVALID_PARAMS: i64 = -32602;
const INTERNAL: i64 = -32603;

/// Who answers a request this proxy sent up a session's websocket.
enum Reply {
    /// The client, under the id it used.
    Client(Value),
    /// A task of this proxy (the handshake after new or load).
    Local(oneshot::Sender<Result<Value, Value>>),
}

#[derive(Default)]
struct Inner {
    /// Frames bound for each session's websocket.
    sessions: HashMap<String, mpsc::UnboundedSender<Value>>,
    /// Requests sent up, by the id this proxy gave them, with the session they went to.
    up: HashMap<u64, (String, Reply)>,
    /// Requests sent down to the client, by the id this proxy gave them: the session and the
    /// agent's own id.
    down: HashMap<u64, (String, Value)>,
}

struct Proxy {
    client: Client,
    socket: PathBuf,
    node: Option<String>,
    harness: Option<String>,
    out: mpsc::UnboundedSender<Value>,
    next_id: AtomicU64,
    inner: Mutex<Inner>,
}

pub async fn serve(client: Client, config: &Config, node: Option<String>, harness: Option<String>) -> Out<()> {
    let (out, mut out_rx) = mpsc::unbounded_channel::<Value>();
    let proxy = Arc::new(Proxy { client, socket: super::client::socket_path(config), node, harness, out, next_id: AtomicU64::new(1), inner: Mutex::default() });
    // A null frame is the end: everything queued before it is written first.
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(v) = out_rx.recv().await {
            if v.is_null() || stdout.write_all(format!("{v}\n").as_bytes()).await.is_err() || stdout.flush().await.is_err() {
                break;
            }
        }
    });
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut tasks = tokio::task::JoinSet::new();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
        tasks.spawn(proxy.clone().handle(v));
    }
    // The client hung up: what it asked is answered, then the process ends; the sessions run on.
    while tasks.join_next().await.is_some() {}
    proxy.send(Value::Null);
    let _ = writer.await;
    Ok(())
}

fn invalid(id: &Value, message: impl std::fmt::Display) -> Value {
    acp::error_response(id, INVALID_PARAMS, &message.to_string())
}

impl Proxy {
    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn send(&self, v: Value) {
        let _ = self.out.send(v);
    }

    /// One frame from the client.
    async fn handle(self: Arc<Self>, v: Value) {
        let Some(message) = acp::classify(&v) else { return };
        match message {
            acp::Message::Request { id, method, params } => {
                let id = id.clone();
                let answer = match method {
                    "initialize" => Ok(json!({
                        "protocolVersion": acp::PROTOCOL_VERSION,
                        "agentInfo": { "name": "rosterd", "version": VERSION },
                        "agentCapabilities": { "loadSession": true, "sessionCapabilities": { "list": {} } },
                        "authMethods": [],
                    })),
                    "authenticate" => Ok(json!({})),
                    "session/new" => self.new_session(params).await,
                    "session/load" => self.load_session(params).await,
                    "session/list" => self.list(params).await,
                    "_rosterd/nodes" => self.client.call(Method::GET, "/swarm/nodes", None).await.map_err(|e| e.message).and_then(|b| parse::<Value>(&b).map(|nodes| json!({ "nodes": nodes })).map_err(|e| e.message)),
                    _ => return self.up(&v, Reply::Client(id)),
                };
                self.send(match answer {
                    Ok(result) => acp::response(&id, result),
                    Err(message) => invalid(&id, message),
                });
            }
            acp::Message::Notification { .. } => self.up(&v, Reply::Client(Value::Null)),
            acp::Message::Response { id, result } => {
                // The client's answer to a request the agent sent (a permission, a question).
                let Some((key, agent_id)) = id.as_u64().and_then(|id| self.inner.lock().unwrap().down.remove(&id)) else { return };
                let frame = match result {
                    Ok(result) => acp::response(&agent_id, result.clone()),
                    Err(error) => json!({ "jsonrpc": "2.0", "id": agent_id, "error": error }),
                };
                let sender = self.inner.lock().unwrap().sessions.get(&key).cloned();
                if let Some(sender) = sender {
                    let _ = sender.send(frame);
                }
            }
        }
    }

    /// A request or notification for one session, by its `sessionId`.
    fn up(&self, v: &Value, reply: Reply) {
        let id = v.get("id").cloned().filter(|id| !id.is_null());
        let Some(key) = v["params"]["sessionId"].as_str().map(str::to_string) else {
            if let Some(id) = id {
                self.send(invalid(&id, "no sessionId"));
            }
            return;
        };
        let sender = self.inner.lock().unwrap().sessions.get(&key).cloned();
        let Some(sender) = sender else {
            if let Some(id) = id {
                self.send(invalid(&id, format!("session {key} is not loaded here: session/load it first")));
            }
            return;
        };
        let mut frame = v.clone();
        if id.is_some() {
            let ours = self.id();
            frame["id"] = json!(ours);
            self.inner.lock().unwrap().up.insert(ours, (key, reply));
        }
        let _ = sender.send(frame);
    }

    /// A request of this proxy's own to a session: the agent's answer.
    async fn ask(&self, key: &str, method: &str, params: Value) -> Result<Value, String> {
        let (tx, rx) = oneshot::channel();
        self.up(&json!({ "jsonrpc": "2.0", "id": 0, "method": method, "params": params.as_object().cloned().map(|mut p| { p.insert("sessionId".into(), json!(key)); Value::Object(p) }).unwrap_or_else(|| json!({ "sessionId": key })) }), Reply::Local(tx));
        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(error["message"].as_str().unwrap_or("request failed").to_string()),
            Err(_) => Err(format!("session {key} closed")),
        }
    }

    async fn new_session(self: &Arc<Self>, params: &Value) -> Result<Value, String> {
        let meta = &params["_meta"]["rosterd"];
        let field = |name: &str| meta[name].as_str().map(str::to_string);
        let harness = field("harness").or_else(|| self.harness.clone()).ok_or("no harness: pass _meta.rosterd.harness or run rosterd acp --harness")?;
        let cwd = params["cwd"].as_str().ok_or("cwd is required")?;
        let body = json!({ "harness": harness, "cwd": cwd, "name": field("name"), "model": field("model"), "effort": field("effort"), "permission_policy": field("permission_policy") });
        let path = match field("node").or_else(|| self.node.clone()) {
            Some(node) => format!("/swarm/{}/sessions", super::session::node_id(&self.client, &node).await.map_err(|e| e.message)?),
            None => "/sessions".into(),
        };
        let record: Record = parse(&self.client.call(Method::POST, &path, Some(body)).await.map_err(|e| e.message)?).map_err(|e| e.message)?;
        let answer = self.join(&record.session_key).await?;
        Ok(json!({ "sessionId": record.session_key, "modes": answer["modes"], "configOptions": answer["configOptions"], "_meta": { "rosterd": { "node": record.node, "harness": record.harness } } }))
    }

    /// The conversation so far goes down as `session/update` notifications before the answer,
    /// as the protocol has it, read from the harness's transcript on the owner node.
    async fn load_session(self: &Arc<Self>, params: &Value) -> Result<Value, String> {
        let key = params["sessionId"].as_str().ok_or("sessionId is required")?;
        let key = super::session::session_key(&self.client, key).await.map_err(|e| e.message)?;
        let record: Record = parse(&self.client.call(Method::GET, &format!("/sessions/{key}"), None).await.map_err(|e| e.message)?).map_err(|e| e.message)?;
        if record.lane != Lane::Headless {
            return Err(format!("{key} is an interactive session: rosterd attach {key}"));
        }
        let answer = self.join(&key).await?;
        let history = self.ask(&key, "_rosterd/history", json!({})).await?;
        for update in history["updates"].as_array().into_iter().flatten() {
            self.send(acp::notification("session/update", json!({ "sessionId": key, "update": update })));
        }
        Ok(json!({ "modes": answer["modes"], "configOptions": answer["configOptions"], "_meta": { "rosterd": { "sessionId": key, "node": record.node, "harness": record.harness } } }))
    }

    /// Every session in the swarm, newest activity first; an interactive one is listed for what
    /// it is, `_meta.rosterd.lane`, and cannot be loaded.
    async fn list(&self, params: &Value) -> Result<Value, String> {
        let swarm: SwarmSnapshot = parse(&self.client.call(Method::GET, "/swarm/snapshot", None).await.map_err(|e| e.message)?).map_err(|e| e.message)?;
        let cwd = params["cwd"].as_str();
        let mut records: Vec<Record> = swarm.records.into_iter().map(|r| r.record).filter(|r| r.ended_at.is_none() && cwd.is_none_or(|cwd| r.cwd.as_deref() == Some(cwd))).collect();
        records.sort_by_key(|r| std::cmp::Reverse(r.activity_at.unwrap_or(r.started_at)));
        let sessions: Vec<Value> = records
            .iter()
            .map(|r| {
                json!({
                    "sessionId": r.session_key,
                    "cwd": r.cwd.clone().unwrap_or_default(),
                    "title": r.name,
                    "updatedAt": r.activity_at.unwrap_or(r.started_at),
                    "_meta": { "rosterd": { "node": r.node, "harness": r.harness, "lane": r.lane, "activity": r.activity, "mode": r.mode, "name": r.name } },
                })
            })
            .collect();
        Ok(json!({ "sessions": sessions }))
    }

    /// The session's websocket, relayed until either side closes; then the handshake answer.
    async fn join(self: &Arc<Self>, key: &str) -> Result<Value, String> {
        if !self.inner.lock().unwrap().sessions.contains_key(key) {
            let stream = tokio::net::UnixStream::connect(&self.socket).await.map_err(|e| format!("connect {}: {e}", self.socket.display()))?;
            let (ws, _) = tokio_tungstenite::client_async(format!("ws://rosterd/sessions/{key}/acp"), stream).await.map_err(|e| e.to_string())?;
            let (tx, rx) = mpsc::unbounded_channel();
            self.inner.lock().unwrap().sessions.insert(key.to_string(), tx);
            tokio::spawn(self.clone().relay(key.to_string(), ws, rx));
        }
        self.ask(key, "_rosterd/handshake", json!({})).await
    }

    async fn relay<S>(self: Arc<Self>, key: String, ws: tokio_tungstenite::WebSocketStream<S>, mut rx: mpsc::UnboundedReceiver<Value>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (mut sink, mut source) = ws.split();
        loop {
            tokio::select! {
                frame = rx.recv() => match frame {
                    Some(v) => {
                        if sink.send(Message::Text(v.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
                message = source.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        let Ok(v) = serde_json::from_str::<Value>(text.as_str()) else { continue };
                        if v.get("error").is_some() && v.get("jsonrpc").is_none() {
                            break;
                        }
                        self.down(&key, v);
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                },
            }
        }
        // The session went: whoever waits on it hears so.
        let (waiting, _) = {
            let mut inner = self.inner.lock().unwrap();
            inner.sessions.remove(&key);
            let (ours, rest): (Vec<_>, Vec<_>) = inner.up.drain().partition(|(_, (k, _))| *k == key);
            inner.up.extend(rest);
            (ours, ())
        };
        for (_, (_, reply)) in waiting {
            match reply {
                Reply::Client(id) => self.send(acp::error_response(&id, INTERNAL, &format!("session {key} closed"))),
                Reply::Local(tx) => {
                    let _ = tx.send(Err(json!({ "message": format!("session {key} closed") })));
                }
            }
        }
    }

    /// One frame from a session, to the client.
    fn down(&self, key: &str, mut v: Value) {
        let Some(message) = acp::classify(&v) else { return };
        match message {
            acp::Message::Notification { .. } => self.send(v),
            acp::Message::Request { id, .. } => {
                let ours = self.id();
                self.inner.lock().unwrap().down.insert(ours, (key.to_string(), id.clone()));
                v["id"] = json!(ours);
                self.send(v);
            }
            acp::Message::Response { id, result } => {
                let Some((_, reply)) = id.as_u64().and_then(|id| self.inner.lock().unwrap().up.remove(&id)) else { return };
                let result = result.cloned().map_err(Value::clone);
                match reply {
                    Reply::Client(client_id) => {
                        v["id"] = client_id;
                        self.send(v);
                    }
                    Reply::Local(tx) => {
                        let _ = tx.send(result);
                    }
                }
            }
        }
    }
}

impl From<Exit> for String {
    fn from(exit: Exit) -> String {
        exit.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::extract::ws::{Message as AxMessage, WebSocket, WebSocketUpgrade};
    use axum::routing::{get, post};
    use tokio::net::UnixListener;

    /// A fake daemon: one session whose agent echoes a prompt as a message chunk, asks a
    /// permission, and answers the prompt once the permission is answered.
    async fn fake_session(mut socket: WebSocket) {
        while let Some(Ok(AxMessage::Text(text))) = socket.recv().await {
            let v: Value = serde_json::from_str(text.as_str()).unwrap();
            match v["method"].as_str() {
                Some("_rosterd/handshake") => {
                    let _ = socket.send(AxMessage::Text(acp::response(&v["id"], json!({ "modes": { "currentModeId": "code" } })).to_string().into())).await;
                }
                Some("_rosterd/history") => {
                    let history = json!({ "updates": [{ "sessionUpdate": "user_message_chunk", "content": { "type": "text", "text": "earlier" } }] });
                    let _ = socket.send(AxMessage::Text(acp::response(&v["id"], history).to_string().into())).await;
                }
                Some("session/prompt") => {
                    let echo = json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "sessionId": "n:1:2", "update": { "sessionUpdate": "agent_message_chunk", "content": v["params"]["prompt"][0] } } });
                    let _ = socket.send(AxMessage::Text(echo.to_string().into())).await;
                    let ask = json!({ "jsonrpc": "2.0", "id": "p-7", "method": "session/request_permission", "params": { "sessionId": "n:1:2", "toolCall": { "title": "Bash" }, "options": [{ "optionId": "y", "name": "Yes", "kind": "allow_once" }] } });
                    let _ = socket.send(AxMessage::Text(ask.to_string().into())).await;
                    let Some(Ok(AxMessage::Text(answer))) = socket.recv().await else { return };
                    let answer: Value = serde_json::from_str(answer.as_str()).unwrap();
                    assert_eq!(answer["id"], "p-7", "the agent's own id comes back");
                    assert_eq!(answer["result"]["outcome"]["optionId"], "y");
                    let _ = socket.send(AxMessage::Text(acp::response(&v["id"], json!({ "stopReason": "end_turn" })).to_string().into())).await;
                }
                Some("session/set_mode") => {
                    let _ = socket.send(AxMessage::Text(acp::response(&v["id"], json!({})).to_string().into())).await;
                }
                _ => {}
            }
        }
    }

    fn record() -> Value {
        json!({ "node": "probe", "node_id": "n", "session_key": "n:1:2", "pid": 1, "start_ticks": 2, "started_at": "2026-09-18T00:00:00Z", "harness": "fake", "lane": "headless", "cwd": "/w", "name": "echo" })
    }

    #[tokio::test]
    async fn the_swarm_is_one_agent_with_ids_remapped_both_ways() {
        let dir = std::env::temp_dir().join(format!("rosterd-acp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("rosterd.sock");
        let app = Router::new()
            .route("/swarm/snapshot", get(|| async { axum::Json(json!({ "schema": "rosterd.swarm.v1", "generated_at": "2026-09-18T00:00:00Z", "nodes": [{ "node_id": "n", "name": "probe", "state": "local", "peer_age_ms": 0 }], "records": [record().as_object().unwrap().clone().into_iter().chain([("peer_state".to_string(), json!("local")), ("peer_age_ms".to_string(), json!(0))]).collect::<serde_json::Map<_, _>>()] })) }))
            .route("/sessions", post(|| async { (axum::http::StatusCode::CREATED, axum::Json(record())) }))
            .route("/sessions/n:1:2", get(|| async { axum::Json(record()) }))
            .route("/sessions/n:1:2/acp", get(|ws: WebSocketUpgrade| async { ws.on_upgrade(fake_session) }));
        let listener = UnixListener::bind(&socket).unwrap();
        tokio::spawn(axum::serve(listener, app).into_future());
        let mut config = Config::default();
        config.node.socket = Some(socket.clone());
        let (out, mut out_rx) = mpsc::unbounded_channel::<Value>();
        let proxy = Arc::new(Proxy { client: Client::new(&config), socket, node: None, harness: Some("fake".into()), out, next_id: AtomicU64::new(1), inner: Mutex::default() });
        async fn next(rx: &mut mpsc::UnboundedReceiver<Value>) -> Value {
            tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await.expect("timely").expect("frame")
        }
        let feed = |v: Value| tokio::spawn(proxy.clone().handle(v));

        feed(json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": { "protocolVersion": 1 } }));
        let init = next(&mut out_rx).await;
        assert_eq!(init["result"]["agentCapabilities"]["loadSession"], true);

        feed(json!({ "jsonrpc": "2.0", "id": 2, "method": "session/list", "params": {} }));
        let list = next(&mut out_rx).await;
        assert_eq!(list["result"]["sessions"][0]["sessionId"], "n:1:2");
        assert_eq!(list["result"]["sessions"][0]["_meta"]["rosterd"]["node"], "probe");

        feed(json!({ "jsonrpc": "2.0", "id": 3, "method": "session/new", "params": { "cwd": "/w" } }));
        let new = next(&mut out_rx).await;
        assert_eq!(new["id"], 3);
        assert_eq!(new["result"]["sessionId"], "n:1:2");
        assert_eq!(new["result"]["modes"]["currentModeId"], "code");

        feed(json!({ "jsonrpc": "2.0", "id": "c-4", "method": "session/prompt", "params": { "sessionId": "n:1:2", "prompt": [{ "type": "text", "text": "hi" }] } }));
        let echo = next(&mut out_rx).await;
        assert_eq!(echo["params"]["update"]["content"]["text"], "hi");
        let ask = next(&mut out_rx).await;
        assert_eq!(ask["method"], "session/request_permission");
        assert_ne!(ask["id"], "p-7", "the client sees this proxy's id");
        feed(json!({ "jsonrpc": "2.0", "id": ask["id"], "result": { "outcome": { "outcome": "selected", "optionId": "y" } } }));
        let done = next(&mut out_rx).await;
        assert_eq!(done["id"], "c-4", "the client's id comes back");
        assert_eq!(done["result"]["stopReason"], "end_turn");

        feed(json!({ "jsonrpc": "2.0", "id": 5, "method": "session/set_mode", "params": { "sessionId": "n:1:2", "modeId": "plan" } }));
        assert_eq!(next(&mut out_rx).await["id"], 5);

        feed(json!({ "jsonrpc": "2.0", "id": 7, "method": "session/load", "params": { "sessionId": "n:1:2", "cwd": "/w" } }));
        let earlier = next(&mut out_rx).await;
        assert_eq!(earlier["method"], "session/update", "history replays before the answer");
        assert_eq!(earlier["params"]["sessionId"], "n:1:2");
        assert_eq!(earlier["params"]["update"]["content"]["text"], "earlier");
        let loaded = next(&mut out_rx).await;
        assert_eq!(loaded["id"], 7);
        assert_eq!(loaded["result"]["modes"]["currentModeId"], "code");

        feed(json!({ "jsonrpc": "2.0", "id": 6, "method": "session/prompt", "params": { "sessionId": "nope", "prompt": [] } }));
        let err = next(&mut out_rx).await;
        assert_eq!(err["error"]["code"], INVALID_PARAMS);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
