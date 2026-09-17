//! The local API and MCP server, R6, and the Tailscale listener that carries the mesh routes
//! and /events for peers, R7. Three listeners, one router each:
//!
//! - the Unix socket, mode 0600, peer pid from socket credentials (Windows named pipes are a
//!   later target);
//! - loopback HTTP with the bearer token from the config directory;
//! - the Tailscale interface over HTTPS, peer requests verified by `Mesh::authenticate`.
//!
//! Also serves the single page client at /ui, R9; the CLI of R14 lives in `crate::cli`.
//!
//! OWNER: the api agent.

mod mcp;
mod routes;
mod send;

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::body::Body;
use axum::extract::connect_info::Connected;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::serve::{IncomingStream, Listener};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::task::JoinSet;

use crate::mesh::MeshError;
use crate::node::Node;
use routes::ApiError;

/// Who is on the other end of the Unix socket, from the socket's peer credentials. Trusted: the
/// socket mode is the gate, R6, and R12 rules out several users on one node, so the pid is all
/// a handler needs.
#[derive(Clone, Debug, Default)]
pub struct Peer {
    pub pid: Option<u32>,
}

/// Peer requests are read whole before `Mesh::authenticate` sees them, R7.6.
const PEER_BODY_LIMIT: usize = 4 << 20;

/// Binds every listener and serves until one of them fails.
pub async fn serve(node: Arc<Node>) -> anyhow::Result<()> {
    let local = routes::router(node.clone())
        .merge(node.mesh.local_router())
        .nest_service("/mcp", mcp::service(node.clone()));
    // R4 and R5 write /local/…; the same router answers both spellings.
    let local = Router::new().nest("/local", local.clone()).merge(local);
    let loopback = local.clone().layer(middleware::from_fn_with_state(node.clone(), require_bearer));

    let mut listeners = JoinSet::new();

    let socket_path = node.config.socket_path();
    let socket = bind_socket(&socket_path)?;
    tracing::info!(path = %socket_path.display(), "socket listener");
    listeners.spawn(async move {
        axum::serve(socket, local.into_make_service_with_connect_info::<Peer>()).await.map_err(anyhow::Error::from)
    });

    let addr = SocketAddr::from(([127, 0, 0, 1], node.config.node.loopback_port));
    let tcp = TcpListener::bind(addr).await.with_context(|| format!("bind {addr}"))?;
    tracing::info!(%addr, "loopback listener");
    let ui = loopback.clone();
    listeners.spawn(async move { axum::serve(tcp, loopback).await.map_err(anyhow::Error::from) });

    let mesh_on_tailscale = node.config.node.listen == "tailscale";
    let ui_on_tailscale = node.config.node.ui_listen == "tailscale";
    let tailscale_ip = if mesh_on_tailscale || ui_on_tailscale { node.mesh.tailscale_ip().await } else { None };

    // The same bearer-gated router as loopback, on the tailnet: Tailscale is the network boundary
    // and the token the application one, R7.6.
    if ui_on_tailscale {
        match tailscale_ip {
            Some(ip) => {
                let addr = SocketAddr::new(ip, node.config.node.loopback_port);
                let tcp = TcpListener::bind(addr).await.with_context(|| format!("bind {addr}"))?;
                tracing::info!(%addr, "ui listener");
                listeners.spawn(async move { axum::serve(tcp, ui).await.map_err(anyhow::Error::from) });
            }
            None => tracing::warn!("no Tailscale IP; the page answers on loopback only"),
        }
    }

    if mesh_on_tailscale {
        match tailscale_ip {
            Some(ip) => {
                let addr = SocketAddr::new(ip, node.config.node.port);
                let tls = node.mesh.tls()?;
                let router = tailscale_router(&node);
                tracing::info!(%addr, "tailscale listener");
                listeners.spawn(async move {
                    axum_server::bind_rustls(addr, tls).serve(router.into_make_service()).await.map_err(anyhow::Error::from)
                });
            }
            None => tracing::warn!("no Tailscale IP; the mesh listener is off and nothing binds elsewhere, R7.6"),
        }
    } else {
        tracing::info!("node.listen = off; no mesh listener");
    }

    // A listener that stops ends the daemon; the service manager restarts it, R2.
    while let Some(result) = listeners.join_next().await {
        result??;
    }
    Ok(())
}

/// Peer routes plus /events and the session actions, R7.6: everything but hello and join passes
/// `Mesh::authenticate` first. The operator routes and /mcp are never mounted here.
fn tailscale_router(node: &Arc<Node>) -> Router {
    let authed = Router::new().route("/events", get(routes::events)).merge(routes::sessions()).with_state(node.clone());
    node.mesh.peer_router().merge(authed).layer(middleware::from_fn_with_state(node.clone(), peer_auth))
}

async fn peer_auth(State(node): State<Arc<Node>>, request: Request, next: Next) -> Response {
    let (mut parts, body) = request.into_parts();
    let path = parts.uri.path().to_string();
    if matches!(path.as_str(), "/node/hello" | "/node/join") {
        return next.run(Request::from_parts(parts, body)).await;
    }
    let bytes = match axum::body::to_bytes(body, PEER_BODY_LIMIT).await {
        Ok(bytes) => bytes,
        Err(error) => return ApiError::new(StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    match node.mesh.authenticate(&parts.method, &path, &parts.headers, &bytes) {
        Ok(peer) => {
            tracing::debug!(peer = %peer.node_id, name = %peer.name, %path, "peer request");
            parts.extensions.insert(peer);
            next.run(Request::from_parts(parts, Body::from(bytes))).await
        }
        Err(error @ MeshError::Revoked(_)) => ApiError::new(StatusCode::FORBIDDEN, error.to_string()).into_response(),
        Err(error) => ApiError::new(StatusCode::UNAUTHORIZED, error.to_string()).into_response(),
    }
}

/// `Authorization: Bearer <loopback token>` on every loopback route, R6.
async fn require_bearer(State(node): State<Arc<Node>>, request: Request, next: Next) -> Response {
    // R9: the browser loads the page bare and sends the bearer from `?token=` on its own calls.
    let path = request.uri().path();
    let path = path.strip_prefix("/local").unwrap_or(path);
    if path == "/ui" || path.starts_with("/ui/") {
        return next.run(request).await;
    }
    let presented = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match presented {
        Some(token) if constant_time_eq(token.as_bytes(), node.loopback_token.as_bytes()) => next.run(request).await,
        _ => ApiError::new(StatusCode::UNAUTHORIZED, "missing or wrong bearer token").into_response(),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The socket of R6: a stale file is removed, a live one (another daemon answering) is refused,
/// the new one is mode 0600.
fn bind_socket(path: &Path) -> anyhow::Result<SocketListener> {
    use std::os::unix::fs::PermissionsExt;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if std::os::unix::net::UnixStream::connect(path).is_ok() {
        anyhow::bail!("another rosterd is serving {}", path.display());
    }
    match std::fs::remove_file(path) {
        Ok(()) => tracing::info!(path = %path.display(), "removed stale socket"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("remove stale {}", path.display())),
    }
    let listener = UnixListener::bind(path).with_context(|| format!("bind {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(SocketListener(listener))
}

/// A `UnixListener` whose connect info is the caller's credentials.
struct SocketListener(UnixListener);

impl Listener for SocketListener {
    type Io = UnixStream;
    type Addr = Peer;

    async fn accept(&mut self) -> (UnixStream, Peer) {
        loop {
            match self.0.accept().await {
                Ok((stream, _)) => {
                    let peer = stream
                        .peer_cred()
                        .map(|cred| Peer { pid: cred.pid().map(|pid| pid as u32) })
                        .unwrap_or_default();
                    return (stream, peer);
                }
                Err(error) => {
                    tracing::warn!(%error, "socket accept");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Peer> {
        Ok(Peer::default())
    }
}

impl Connected<IncomingStream<'_, SocketListener>> for Peer {
    fn connect_info(stream: IncomingStream<'_, SocketListener>) -> Peer {
        stream.remote_addr().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use futures::StreamExt;
    use rosterd_proto::{Activity, Capabilities, Source};
    use serde_json::{Value, json};

    use super::*;
    use crate::config::Config;
    use crate::identity::Identity;
    use crate::roster::{Patch, Roster};

    struct Harness {
        node: Arc<Node>,
        socket: reqwest::Client,
        dir: std::path::PathBuf,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// A node with listen off, an ephemeral loopback port, and the socket under a temp dir;
    /// `serve` runs in the background. Nothing here reaches the runner or the mesh.
    async fn start(tag: &str) -> Harness {
        start_with(tag, |_| {}).await
    }

    async fn start_with(tag: &str, tweak: impl FnOnce(&mut Config)) -> Harness {
        let dir = std::env::temp_dir().join(format!("rosterd-api-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut config = Config::default();
        tweak(&mut config);
        config.node.name = "gibson".into();
        config.node.listen = "off".into();
        config.node.ui_listen = "loopback".into();
        config.node.socket = Some(dir.join("rosterd.sock"));
        config.node.loopback_port = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap().port()
        };
        let config = Arc::new(config);
        let identity = Identity::from_key(ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng));
        let roster = Roster::new("gibson", &identity.node_id, Capabilities::default());
        // The swarm file under the temp dir: the real membership must not leak into a test.
        let mesh = crate::mesh::Mesh::new_at(config.clone(), identity.clone(), roster.clone(), "test", &dir, "127.0.0.1:1").unwrap();
        tokio::spawn(mesh.clone().watch_local());
        let runner = crate::runner::Runner::new(config.clone(), roster.clone());
        let node = Arc::new(Node {
            config: config.clone(),
            identity,
            roster,
            mesh,
            runner,
            loopback_token: "secret-token".into(),
        });
        tokio::spawn(serve(node.clone()));
        let socket_path = config.socket_path();
        for _ in 0..100 {
            if std::os::unix::net::UnixStream::connect(&socket_path).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let socket = reqwest::Client::builder().unix_socket(socket_path).build().unwrap();
        Harness { node, socket, dir }
    }

    #[tokio::test]
    async fn snapshot_status_and_local_prefix_answer_over_the_socket() {
        let h = start("snapshot").await;
        let snap: Value = h.socket.get("http://rosterd/snapshot").send().await.unwrap().json().await.unwrap();
        assert_eq!(snap["schema"], "rosterd.snapshot.v1");
        assert_eq!(snap["node_id"], h.node.identity.node_id);

        let local: Value = h.socket.get("http://rosterd/local/snapshot").send().await.unwrap().json().await.unwrap();
        assert_eq!(local, snap);

        let status: Value = h.socket.get("http://rosterd/status").send().await.unwrap().json().await.unwrap();
        assert_eq!(status["node"], "gibson");
        assert_eq!(status["node_id"], h.node.identity.node_id);
        assert_eq!(status["sessions"], 0);

        let ui = h.socket.get("http://rosterd/ui/sessions/abc").send().await.unwrap();
        assert!(ui.headers()["content-type"].to_str().unwrap().starts_with("text/html"));

        let missing = h.socket.get("http://rosterd/nope").send().await.unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert!(missing.json::<Value>().await.unwrap()["error"].is_string());
    }

    #[tokio::test]
    async fn events_sends_the_snapshot_first() {
        let h = start("events").await;
        let response = h.socket.get("http://rosterd/events").send().await.unwrap();
        assert_eq!(response.headers()["content-type"], "text/event-stream");
        let first = response.bytes_stream().next().await.unwrap().unwrap();
        let text = std::str::from_utf8(&first).unwrap();
        assert!(text.starts_with("data: "), "{text}");
        let snap: Value = serde_json::from_str(text.trim_start_matches("data: ").trim()).unwrap();
        assert_eq!(snap["schema"], "rosterd.snapshot.v1");
    }

    #[tokio::test]
    async fn changes_stream_the_snapshot_then_one_event_per_change() {
        let h = start("changes").await;
        let response = h.socket.get("http://rosterd/swarm/changes").send().await.unwrap();
        let mut body = response.bytes_stream();
        let mut buf = String::new();
        // SSE events end with a blank line; chunks may split them anywhere.
        async fn next_event(body: &mut (impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin), buf: &mut String) -> String {
            loop {
                if let Some(end) = buf.find("\n\n") {
                    let ev = buf[..end].to_string();
                    buf.drain(..end + 2);
                    if ev.starts_with(':') {
                        continue;
                    }
                    return ev;
                }
                let chunk = tokio::time::timeout(Duration::from_secs(5), body.next()).await.expect("timely").unwrap().unwrap();
                buf.push_str(std::str::from_utf8(&chunk).unwrap());
            }
        }
        let first = next_event(&mut body, &mut buf).await;
        assert!(first.contains("\nevent: snapshot") && first.starts_with("data: {\"schema\":\"rosterd.swarm.v1\""), "{first}");

        let pid = std::process::id();
        let ticks = crate::scanner::start_ticks(pid).unwrap();
        let key = h.node.roster.apply(Source::Hook, Patch { pid: Some(pid), start_ticks: Some(ticks), harness: Some("claude".into()), ..Default::default() }).unwrap().session_key;
        let started = next_event(&mut body, &mut buf).await;
        assert!(started.ends_with("\nevent: session_started"), "{started}");
        h.node.roster.claim(Source::Hook, &key, Activity::NeedsAttention, "permission", chrono::Utc::now()).unwrap();
        let attention = next_event(&mut body, &mut buf).await;
        let data: Value = serde_json::from_str(attention.trim_start_matches("data: ").lines().next().unwrap()).unwrap();
        assert!(attention.ends_with("\nevent: attention"), "{attention}");
        assert_eq!((data["event"].as_str(), data["record"]["session_key"].as_str(), data["record"]["peer_state"].as_str()), (Some("attention"), Some(key.as_str()), Some("local")));
        h.node.roster.claim(Source::Hook, &key, Activity::Active, "permission_answered", chrono::Utc::now()).unwrap();
        assert!(next_event(&mut body, &mut buf).await.ends_with("\nevent: attention_cleared"));
    }

    #[tokio::test]
    async fn loopback_requires_the_bearer_token() {
        let h = start("loopback").await;
        let url = format!("http://127.0.0.1:{}/snapshot", h.node.config.node.loopback_port);
        let client = reqwest::Client::new();
        let refused = client.get(&url).bearer_auth("wrong").send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::UNAUTHORIZED);
        assert!(refused.json::<Value>().await.unwrap()["error"].is_string());
        let refused = client.get(&url).send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::UNAUTHORIZED);
        let ok = client.get(&url).bearer_auth("secret-token").send().await.unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(ok.json::<Value>().await.unwrap()["schema"], "rosterd.snapshot.v1");
        let mcp = client
            .post(format!("http://127.0.0.1:{}/mcp", h.node.config.node.loopback_port))
            .send()
            .await
            .unwrap();
        assert_eq!(mcp.status(), StatusCode::UNAUTHORIZED);
        for path in ["/ui", "/ui/sessions/abc", "/local/ui", "/local/ui/sessions/abc"] {
            let page = client.get(format!("http://127.0.0.1:{}{path}", h.node.config.node.loopback_port)).send().await.unwrap();
            assert_eq!(page.status(), StatusCode::OK, "{path} loads without a bearer");
            assert!(page.headers()["content-type"].to_str().unwrap().starts_with("text/html"));
        }
        let api = client.get(format!("http://127.0.0.1:{}/uid", h.node.config.node.loopback_port)).send().await.unwrap();
        assert_eq!(api.status(), StatusCode::UNAUTHORIZED, "the exemption is the /ui prefix, not a substring");
    }

    #[tokio::test]
    async fn register_claim_and_name_by_socket_peer_pid() {
        let h = start("claim").await;
        // This test process stands in for a harness; the peer pid lookup needs its real ticks.
        let pid = std::process::id();
        let ticks = crate::scanner::start_ticks(pid).unwrap();
        let record: Value = h
            .socket
            .post("http://rosterd/register")
            .json(&json!({ "source": "launcher", "pid": pid, "start_ticks": ticks, "harness": "claude", "lane": "interactive" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(record["pid"], pid);
        assert_eq!(record["sources"], json!(["launcher"]));

        let claimed: Value = h
            .socket
            .post("http://rosterd/claim")
            .json(&json!({ "pid": pid, "start_ticks": ticks, "activity": "active", "event": "prompt" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(claimed["activity"], "active");
        assert_eq!(claimed["activity_event"], "prompt");

        // Neither session_key nor pid: the socket peer (this test process) names itself.
        let named = h.socket.post("http://rosterd/name").json(&json!({ "name": "me" })).send().await.unwrap();
        assert_eq!(named.status(), StatusCode::OK);
        let named: Value = named.json().await.unwrap();
        assert_eq!(named["name"], "me");
        assert_eq!(named["session_key"], record["session_key"]);

        let bad = h.socket.post("http://rosterd/claim").json(&json!({ "activity": "active", "event": "x" })).send().await.unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        let bad = h.socket.post("http://rosterd/claim").body("{not json").send().await.unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        assert!(bad.json::<Value>().await.unwrap()["error"].is_string());
        let unknown = h.socket.post("http://rosterd/name").json(&json!({ "pid": 1, "name": "x" })).send().await.unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn session_routes_resolve_the_owner() {
        let h = start("sessions").await;
        let record = h
            .node
            .roster
            .apply(Source::Launcher, Patch { pid: Some(4242), start_ticks: Some(1), harness: Some("claude".into()), ..Default::default() })
            .unwrap();
        let key = &record.session_key;
        let ours = &h.node.identity.node_id;

        // An interactive session has no runner state; the record alone comes back.
        let got: Value = h.socket.get(format!("http://rosterd/sessions/{key}")).send().await.unwrap().json().await.unwrap();
        assert_eq!(got["session_key"], *key);
        assert!(got.get("state").is_none());

        // /swarm/{our id}/sessions/… runs here, R6.
        let via_swarm: Value =
            h.socket.get(format!("http://rosterd/swarm/{ours}/sessions/{key}")).send().await.unwrap().json().await.unwrap();
        assert_eq!(via_swarm, got);

        let named: Value = h
            .socket
            .post(format!("http://rosterd/local/sessions/{key}/name"))
            .json(&json!({ "name": "worker" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(named["name"], "worker");

        // Nobody in the swarm owns it: 404, not a proxy attempt.
        let missing = h.socket.get("http://rosterd/sessions/nope:1:1").send().await.unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert!(missing.json::<Value>().await.unwrap()["error"].is_string());

        // A stream never crosses nodes: the client is told where to connect.
        let elsewhere = h.socket.get(format!("http://rosterd/swarm/deadbeef/sessions/{key}/stream")).send().await.unwrap();
        assert_eq!(elsewhere.status(), StatusCode::BAD_REQUEST);

        // POST /sessions has no key capture; the runner's refusal maps to 400.
        let refused = h.socket.post("http://rosterd/sessions").json(&json!({ "harness": "nope" })).send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        assert!(refused.json::<Value>().await.unwrap()["error"].as_str().unwrap().contains("unknown harness nope"));
    }

    /// R16.2 over the socket: a gated call waits, shows as pending on the session, and the
    /// permission route answers it; plus explain, children, suspend, leave and status, R14.3.
    #[tokio::test]
    async fn gate_explain_children_leave_and_status_over_the_socket() {
        let h = start("gate").await;
        let pid = std::process::id();
        let ticks = crate::scanner::start_ticks(pid).unwrap();
        let mine = Patch { pid: Some(pid), start_ticks: Some(ticks), harness: Some("pi".into()), ..Default::default() };
        let key = h.node.roster.apply(Source::Hook, mine).unwrap().session_key;
        let child = Patch { pid: Some(777), start_ticks: Some(1), harness: Some("claude".into()), parent_session_key: Some(key.clone()), ..Default::default() };
        let child = h.node.roster.apply(Source::Launcher, child).unwrap();

        let gate = tokio::spawn({
            let socket = h.socket.clone();
            async move {
                let body = json!({ "pid": pid, "tool": "bash", "summary": "bash {\"cmd\":\"ls\"}", "policy": "attention" });
                socket.post("http://rosterd/gate").json(&body).send().await.unwrap().json::<Value>().await.unwrap()
            }
        });
        let mut session = Value::Null;
        for _ in 0..200 {
            session = h.socket.get(format!("http://rosterd/sessions/{key}")).send().await.unwrap().json().await.unwrap();
            if session["state"]["pending"].as_array().is_some_and(|p| !p.is_empty()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(session["activity"], "needs_attention", "{session}");
        assert_eq!(session["activity_event"], "gate:bash");
        assert_eq!(session["state"]["pending"][0]["tool"], "bash");
        assert_eq!(session["state"]["pending"][0]["options"][2]["option_id"], "deny");
        assert_eq!(session["children"][0]["session_key"], child.session_key);
        let request_id = session["state"]["pending"][0]["request_id"].clone();
        let body = json!({ "request_id": request_id, "outcome": "selected", "option_id": "allow" });
        let answered = h.socket.post(format!("http://rosterd/sessions/{key}/permission")).json(&body).send().await.unwrap();
        assert_eq!(answered.status(), StatusCode::OK);
        assert_eq!(gate.await.unwrap(), json!({ "outcome": "allow" }));
        assert_eq!(h.node.roster.get(&key).unwrap().activity, Activity::Active);
        let none = h.socket.post(format!("http://rosterd/sessions/{key}/permission")).json(&json!({ "outcome": "cancelled" })).send().await.unwrap();
        assert_eq!(none.status(), StatusCode::NOT_FOUND);
        let unknown = h.socket.post("http://rosterd/gate").json(&json!({ "pid": 1, "tool": "x", "summary": "x", "policy": "attention" })).send().await.unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

        let explain: Value = h.socket.get(format!("http://rosterd/local/sessions/{key}/explain")).send().await.unwrap().json().await.unwrap();
        assert_eq!(explain["session_key"], key);
        let harness = explain["fields"].as_array().unwrap().iter().find(|f| f["field"] == "harness").unwrap();
        assert_eq!((&harness["value"], &harness["source"]), (&json!("pi"), &json!("hook")));
        assert_eq!(h.socket.get("http://rosterd/sessions/nope:1:1/explain").send().await.unwrap().status(), StatusCode::NOT_FOUND);

        // Interactive sessions are never suspended, R15.1: the runner does not know the key.
        let refused = h.socket.post(format!("http://rosterd/sessions/{key}/suspend")).send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::NOT_FOUND);
        // Spawn needs a harness this node has.
        let spawn = h.socket.post(format!("http://rosterd/sessions/{key}/spawn")).json(&json!({ "harness": "claude" })).send().await.unwrap();
        assert_eq!(spawn.status(), StatusCode::BAD_REQUEST);
        // No swarm to leave.
        let leave = h.socket.post("http://rosterd/swarm/leave").send().await.unwrap();
        assert_eq!(leave.status(), StatusCode::SERVICE_UNAVAILABLE, "{}", leave.text().await.unwrap());

        let status: Value = h.socket.get("http://rosterd/status").send().await.unwrap().json().await.unwrap();
        assert_eq!(status["swarm_id"], Value::Null);
        assert_eq!(status["peers"], json!({ "total": 0, "reachable": 0, "unreachable": 0 }));
        assert_eq!((&status["sessions"], &status["holders"], &status["suspended"]), (&json!(2), &json!(0), &json!(0)));
        assert_eq!(status["sources"], json!(["launcher", "acp", "hook", "scan"]));
    }

    #[tokio::test]
    async fn mcp_lists_tools_and_names_the_caller() {
        let h = start("mcp").await;
        let pid = std::process::id();
        let ticks = crate::scanner::start_ticks(pid);
        let record = h
            .node
            .roster
            .apply(Source::Launcher, Patch { pid: Some(pid), start_ticks: ticks, harness: Some("claude".into()), ..Default::default() })
            .unwrap();
        h.node.roster.claim(Source::Launcher, &record.session_key, Activity::Idle, "turn_end", chrono::Utc::now()).unwrap();

        let call = |body: Value| {
            h.socket
                .post("http://rosterd/mcp")
                .header("accept", "application/json, text/event-stream")
                .json(&body)
                .send()
        };
        let listed = call(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" })).await.unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed: Value = listed.json().await.unwrap();
        let names: Vec<&str> = listed["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["roster.list", "roster.watch", "session.name", "session.spawn", "session.prompt", "session.read_state", "session.send", "session.find", "swarm.nodes"]);

        let named: Value = call(json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": { "name": "session.name", "arguments": { "name": "coordinator" } }
        }))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(named["result"]["structuredContent"]["name"], "coordinator", "{named}");
        assert_eq!(h.node.roster.get(&record.session_key).unwrap().name.as_deref(), Some("coordinator"));

        let state: Value = call(json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "session.read_state", "arguments": { "session_key": record.session_key } }
        }))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(state["result"]["structuredContent"]["activity"], "idle", "{state}");

        let listed: Value = call(json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": { "name": "roster.list", "arguments": { "scope": "node" } }
        }))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(listed["result"]["structuredContent"]["records"][0]["name"], "coordinator");

        // session.find resolves a name to the record and its node; session.send refuses the
        // caller's own session, R5.5.
        let found: Value = call(json!({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": { "name": "session.find", "arguments": { "name": "coordinator" } }
        }))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(found["result"]["structuredContent"]["session_key"], record.session_key, "{found}");
        assert_eq!(found["result"]["structuredContent"]["peer_state"], "local");
        let refused: Value = call(json!({
            "jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": { "name": "session.send", "arguments": { "to": "coordinator", "prompt": "hi" } }
        }))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(refused["result"]["isError"], true, "{refused}");
        assert!(refused["result"]["content"][0]["text"].as_str().unwrap().starts_with("400"), "{refused}");
        let missing: Value = call(json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": { "name": "session.send", "arguments": { "to": "nobody", "prompt": "hi" } }
        }))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert!(missing["result"]["content"][0]["text"].as_str().unwrap().starts_with("404"), "{missing}");
    }

    /// R5.5 agent to agent over the socket: a session named `worker` on the fake harness is
    /// prompted by name and the answer carries its recap. Skips when the holder or the fake
    /// adapter cannot be had, like the runner's e2e tests.
    #[tokio::test]
    async fn send_prompts_a_session_by_name_and_answers_with_its_recap() {
        let Some((bin, fake)) = crate::runner::e2e_test::binaries() else { return };
        // A holder socket path must fit in sockaddr_un; the holders go under a short dir.
        let holders = std::env::temp_dir().join(format!("rsend{}", std::process::id()));
        let h = start_with("send", |c| {
            c.runner.holder_dir = holders.clone();
            c.runner.holder_bin = Some(bin);
            c.runner.default_permission_policy = rosterd_proto::PermissionPolicy::Auto;
            c.runner.resume_on_crash = false;
            c.harness.insert("fake".into(), crate::config::HarnessConfig { adapter: fake.to_string_lossy().into_owned(), ..Default::default() });
        })
        .await;
        let started = h.socket.post("http://rosterd/sessions").json(&json!({ "harness": "fake", "name": "worker" })).send().await.unwrap();
        assert_eq!(started.status(), StatusCode::CREATED);
        let key = started.json::<Value>().await.unwrap()["session_key"].as_str().unwrap().to_string();

        let sent = h.socket.post("http://rosterd/send").json(&json!({ "to": "worker", "prompt": "hello" })).send().await.unwrap();
        assert_eq!(sent.status(), StatusCode::OK);
        let sent: Value = sent.json().await.unwrap();
        assert_eq!(sent["session_key"], key, "{sent}");
        assert_eq!(sent["node"], "gibson");
        assert_eq!(sent["reached"], true, "{sent}");
        assert_eq!(sent["recap"], "You said: hello", "{sent}");
        assert!(sent["stop_reason"].is_string(), "{sent}");
        assert_eq!(sent["activity"], "idle");
        assert_eq!(sent["pending"], json!([]));

        // By key and by pid as well; an unknown name is 404.
        let by_key: Value = h.socket.post("http://rosterd/send").json(&json!({ "to": key, "prompt": "again" })).send().await.unwrap().json().await.unwrap();
        assert_eq!(by_key["recap"], "You said: again", "{by_key}");
        let pid = h.node.roster.get(&key).unwrap().pid.to_string();
        let by_pid: Value = h.socket.post("http://rosterd/send").json(&json!({ "to": pid, "prompt": "pid" })).send().await.unwrap().json().await.unwrap();
        assert_eq!(by_pid["session_key"], key, "{by_pid}");
        let missing = h.socket.post("http://rosterd/send").json(&json!({ "to": "nobody", "prompt": "x" })).send().await.unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        let stopped = h.socket.delete(format!("http://rosterd/sessions/{key}")).send().await.unwrap();
        assert_eq!(stopped.status(), StatusCode::OK);
        let _ = std::fs::remove_dir_all(&holders);
    }

    #[tokio::test]
    async fn pair_refuses_a_loopback_only_page() {
        let h = start("pair").await;
        let refused = h.socket.get("http://rosterd/pair").send().await.unwrap();
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        assert!(refused.json::<Value>().await.unwrap()["error"].as_str().unwrap().contains("ui_listen"));
    }

    #[test]
    fn constant_time_eq_compares_whole_slices() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
    }
}
