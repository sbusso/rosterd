//! The JSON routes of R6, the session actions of R5 and R15, the gate of R16.2, and the cross
//! node proxy of R7.5. Success bodies are the plain object; errors are `{error}` with 400
//! validation, 404 not found, 409 conflict or suspended, 429 resume limit (`retry_after_s`),
//! 502 proxy failure, 503 no swarm.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::PathRejection;
use axum::extract::{ConnectInfo, FromRequest, FromRequestParts, Path, Request, State};
use axum::http::request::Parts;
use axum::http::{Method, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use rosterd_proto::{Activity, HarnessState, HerdrHandle, Lane, Liveness, PermissionPolicy, Record, Source, TmuxHandle};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, WatchStream};

use super::Peer;
use crate::gate;
use crate::mesh::{MeshError, PeerAuth};
use crate::node::{Node, VERSION};
use crate::roster::{Patch, RosterError};
use crate::runner::{PatchSession, PermissionAnswer, PromptRequest, QuestionAnswer, RunnerError, SessionState, StartSession};

/// The single page client, R9. Written by the ui agent; served as is.
const UI: &str = include_str!("../../../../ui/index.html");
const KEEP_ALIVE: Duration = Duration::from_secs(15);
/// Swarm events coalesce bursts of peer snapshots into one emission.
const SWARM_DEBOUNCE: Duration = Duration::from_millis(100);
const BODY_LIMIT: usize = 4 << 20;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    /// Extra body fields next to `error`, such as `retry_after_s`.
    pub details: serde_json::Map<String, Value>,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        ApiError { status, message: message.into(), details: Default::default() }
    }
    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = self.details;
        body.insert("error".into(), Value::String(self.message));
        (self.status, Json(Value::Object(body))).into_response()
    }
}

impl From<RosterError> for ApiError {
    fn from(error: RosterError) -> Self {
        let status = match error {
            RosterError::NotFound(_) => StatusCode::NOT_FOUND,
            RosterError::NoIdentity | RosterError::Invalid(_) => StatusCode::BAD_REQUEST,
        };
        ApiError::new(status, error.to_string())
    }
}

impl From<RunnerError> for ApiError {
    fn from(error: RunnerError) -> Self {
        let status = match error {
            RunnerError::NotFound(_) | RunnerError::NoPending(_) => StatusCode::NOT_FOUND,
            RunnerError::Suspended(_) => StatusCode::CONFLICT,
            RunnerError::UnknownHarness(_) => StatusCode::BAD_REQUEST,
            // R15.3: over max_resumes_per_hour the session stays suspended until the window passes.
            RunnerError::ResumeLimit { retry_after_s, .. } => {
                let mut refused = ApiError::new(StatusCode::TOO_MANY_REQUESTS, error.to_string());
                refused.details.insert("retry_after_s".into(), json!(retry_after_s));
                return refused;
            }
            RunnerError::Roster(error) => return ApiError::from(error),
            // R7.7: the harness cannot work here right now; the body carries the mark.
            RunnerError::Unhealthy(ref health) => {
                let mut refused = ApiError::new(StatusCode::CONFLICT, error.to_string());
                refused.details.insert("health".into(), json!(health));
                return refused;
            }
            RunnerError::AuthRequired => StatusCode::UNAUTHORIZED,
            RunnerError::Acp(_) | RunnerError::Holder(_) | RunnerError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError::new(status, error.to_string())
    }
}

impl From<MeshError> for ApiError {
    fn from(error: MeshError) -> Self {
        let status = match error {
            MeshError::UnknownNode(_) => StatusCode::NOT_FOUND,
            MeshError::Revoked(_) => StatusCode::FORBIDDEN,
            MeshError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            MeshError::NoSwarm => StatusCode::SERVICE_UNAVAILABLE,
            MeshError::Unreachable(_) | MeshError::Other(_) => StatusCode::BAD_GATEWAY,
        };
        ApiError::new(status, error.to_string())
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(error: serde_json::Error) -> Self {
        ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

/// A JSON body, any content type; a bad one is a 400 `{error}` like every other refusal.
pub struct Body<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for Body<T> {
    type Rejection = ApiError;

    async fn from_request(request: Request, _state: &S) -> Result<Self, ApiError> {
        let bytes = axum::body::to_bytes(request.into_body(), BODY_LIMIT)
            .await
            .map_err(|error| ApiError::bad_request(error.to_string()))?;
        let bytes = if bytes.is_empty() { b"{}".as_slice() } else { &bytes };
        serde_json::from_slice(bytes).map(Body).map_err(|error| ApiError::bad_request(format!("body: {error}")))
    }
}

fn parse<T: DeserializeOwned>(value: &Value) -> Result<T, ApiError> {
    serde_json::from_value(value.clone()).map_err(|error| ApiError::bad_request(format!("body: {error}")))
}

fn ok<T: Serialize>(value: T) -> Result<Response, ApiError> {
    Ok(Json(value).into_response())
}

fn sse_json<T: Serialize>(value: &T) -> Event {
    Event::default().json_data(value).unwrap_or_else(|error| Event::default().comment(error.to_string()))
}

/// Everything the socket and loopback listeners share. The mesh operator routes and /mcp are
/// merged in by `serve`.
pub fn router(node: Arc<Node>) -> Router {
    Router::new()
        .route("/snapshot", get(snapshot))
        .route("/events", get(events))
        .route("/register", post(register))
        .route("/claim", post(claim))
        .route("/name", post(name))
        .route("/gate", post(gate))
        .route("/status", get(status))
        .route("/pair", get(pair))
        .route("/swarm/snapshot", get(swarm_snapshot))
        .route("/swarm/events", get(swarm_events))
        .route("/swarm/changes", get(swarm_changes))
        .route("/swarm/nodes", get(swarm_nodes))
        .route("/swarm/leave", post(swarm_leave))
        // Any /sessions path under /swarm/{node_id}/ runs on that node, R6.
        .nest("/swarm/{node_id}", sessions())
        .route("/ui", get(ui))
        .route("/ui/sessions/{key}", get(ui))
        .merge(sessions())
        .fallback(|| async { ApiError::not_found("no such route") })
        .with_state(node)
}

/// The session actions of R5. Also mounted on the Tailscale listener for proxied actions.
pub fn sessions() -> Router<Arc<Node>> {
    Router::new()
        .route("/sessions", post(start_session))
        .route("/sessions/{key}", get(get_session).patch(patch_session).delete(delete_session))
        .route("/sessions/{key}/prompt", post(prompt))
        .route("/sessions/{key}/cancel", post(cancel))
        .route("/sessions/{key}/open", post(open_session))
        .route("/sessions/{key}/stream", get(stream))
        .route("/sessions/{key}/permission", post(permission))
        .route("/sessions/{key}/answer", post(answer_question))
        .route("/sessions/{key}/name", post(session_name))
        .route("/sessions/{key}/explain", get(explain))
        .route("/sessions/{key}/suspend", post(suspend))
        .route("/sessions/{key}/resume", post(resume))
        .route("/sessions/{key}/spawn", post(spawn))
}

async fn snapshot(State(node): State<Arc<Node>>) -> Json<Arc<rosterd_proto::Snapshot>> {
    Json(node.roster.snapshot())
}

/// SSE, the whole roster first and on every change, R6.
pub async fn events(State(node): State<Arc<Node>>) -> Response {
    let stream = WatchStream::new(node.roster.watch()).map(|snapshot| Ok::<_, Infallible>(sse_json(&*snapshot)));
    Sse::new(stream).keep_alive(KeepAlive::new().interval(KEEP_ALIVE)).into_response()
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct RegisterBody {
    /// launcher or hook; hook when absent.
    source: Option<Source>,
    session_key: Option<String>,
    pid: Option<u32>,
    start_ticks: Option<u64>,
    started_at: Option<DateTime<Utc>>,
    harness: Option<String>,
    session_id: Option<String>,
    lane: Option<Lane>,
    name: Option<String>,
    cwd: Option<String>,
    tty: Option<String>,
    tmux: Option<TmuxHandle>,
    herdr: Option<HerdrHandle>,
}

fn local_source(source: Option<Source>) -> Result<Source, ApiError> {
    match source.unwrap_or(Source::Hook) {
        source @ (Source::Launcher | Source::Hook) => Ok(source),
        other => Err(ApiError::bad_request(format!("source must be launcher or hook, not {other:?}"))),
    }
}

/// Launcher or hook registration, R4.
async fn register(State(node): State<Arc<Node>>, Body(body): Body<RegisterBody>) -> Result<Json<Record>, ApiError> {
    let source = local_source(body.source)?;
    let record = node.roster.apply(
        source,
        Patch {
            session_key: body.session_key,
            pid: body.pid,
            start_ticks: body.start_ticks,
            started_at: body.started_at,
            harness: body.harness,
            session_id: body.session_id,
            lane: body.lane,
            name: body.name,
            cwd: body.cwd,
            tty: body.tty,
            tmux: body.tmux,
            herdr: body.herdr,
            ..Patch::default()
        },
    )?;
    Ok(Json(record))
}

#[derive(serde::Deserialize)]
struct ClaimBody {
    #[serde(default)]
    source: Option<Source>,
    session_key: Option<String>,
    pid: Option<u32>,
    start_ticks: Option<u64>,
    activity: Activity,
    event: String,
    observed_at: Option<DateTime<Utc>>,
}

/// An activity claim by session key or pid, R6. A pid the roster has not seen becomes a
/// hook-only session first.
async fn claim(State(node): State<Arc<Node>>, Body(body): Body<ClaimBody>) -> Result<Json<Record>, ApiError> {
    let source = local_source(body.source)?;
    let key = match body.session_key {
        Some(key) => key,
        None => {
            let patch = Patch { pid: body.pid, start_ticks: body.start_ticks, ..Patch::default() };
            node.roster.apply(source, patch)?.session_key
        }
    };
    let at = body.observed_at.unwrap_or_else(Utc::now);
    let record = node.roster.claim(source, &key, body.activity, &body.event, at)?;
    // R7.7: a hook that saw its harness ask for a login marks the harness, as the runner does
    // on session/new; a later working claim from that harness clears the login mark only.
    if source == Source::Hook {
        let login = node.runner.health.get(&record.harness, at).is_some_and(|h| h.state == HarnessState::LoginRequired);
        if body.event == "login" {
            node.runner.mark(&record.harness, HarnessState::LoginRequired, None, Some("hook: login".into()));
        } else if login && body.activity != Activity::NeedsAttention {
            node.runner.healthy(&record.harness);
        }
    }
    Ok(Json(record))
}

#[derive(serde::Deserialize)]
struct NameBody {
    session_key: Option<String>,
    pid: Option<u32>,
    /// Null or absent clears the name.
    name: Option<String>,
}

/// Set or clear a display name, R6. With neither key nor pid the socket peer names itself.
async fn name(
    State(node): State<Arc<Node>>,
    peer: Option<Extension<ConnectInfo<Peer>>>,
    Body(body): Body<NameBody>,
) -> Result<Json<Record>, ApiError> {
    let key = match (body.session_key, body.pid) {
        (Some(key), _) => key,
        (None, Some(pid)) => key_for_pid(&node, pid)?,
        (None, None) => {
            let pid = peer
                .and_then(|Extension(ConnectInfo(peer))| peer.pid)
                .ok_or_else(|| ApiError::bad_request("session_key or pid is required"))?;
            key_for_pid(&node, pid)?
        }
    };
    Ok(Json(node.roster.set_name(&key, body.name, Source::Hook)?))
}

/// The session a live process reports under: its own record or the root it was collapsed
/// into, R3. Nothing is registered on the way.
pub(super) fn key_for_pid(node: &Node, pid: u32) -> Result<String, ApiError> {
    crate::scanner::start_ticks(pid)
        .and_then(|ticks| node.roster.resolve_key(pid, ticks))
        .ok_or_else(|| ApiError::not_found(format!("no session for pid {pid}")))
}

/// Everything `rosterd status` prints, R14.3, for the CLI and the UI.
async fn status(State(node): State<Arc<Node>>) -> Json<Value> {
    let snapshot = node.roster.snapshot();
    let nodes = node.mesh.nodes();
    let peers: Vec<_> = nodes.iter().filter(|n| n.state != rosterd_proto::PeerState::Local && !n.revoked).collect();
    let reachable = peers.iter().filter(|n| n.state == rosterd_proto::PeerState::Reachable).count();
    let open = || snapshot.records.iter().filter(|r| r.liveness != Liveness::Ended);
    let mut sources = vec!["launcher", "acp", "hook"];
    sources.extend(node.config.sources.files.then_some("files"));
    sources.push("scan");
    Json(json!({
        "node": node.config.node.name,
        "node_id": node.identity.node_id,
        "version": VERSION,
        "socket": node.config.socket_path(),
        "loopback_port": node.config.node.loopback_port,
        "listen": node.config.node.listen,
        "ui_listen": node.config.node.ui_listen,
        "swarm_id": node.mesh.swarm_id(),
        "nodes": nodes.len(),
        "peers": { "total": peers.len(), "reachable": reachable, "unreachable": peers.len() - reachable },
        "sessions": open().count(),
        "holders": open().filter(|r| node.runner.owns(&r.session_key)).count(),
        "suspended": open().filter(|r| r.liveness == Liveness::Suspended).count(),
        "sources": sources,
        "health": snapshot.capabilities.health,
    }))
}

async fn swarm_snapshot(State(node): State<Arc<Node>>) -> Json<rosterd_proto::SwarmSnapshot> {
    Json(node.mesh.swarm_snapshot())
}

/// SSE, R6: one `snapshot` event with the whole swarm, then one event per change, named by
/// the change (`attention`, `session_started`, `node`, ...). Derived from consecutive swarm
/// frames, so a client keeps one connection and never diffs.
async fn swarm_changes(State(node): State<Arc<Node>>) -> Response {
    let first = node.mesh.swarm_snapshot();
    let head = futures::stream::once(async move { Ok::<_, Infallible>(sse_json(&first).event("snapshot")) });
    let prev = node.mesh.swarm_snapshot();
    let rest = futures::stream::unfold((node.mesh.changed(), prev), move |(mut changed, prev)| {
        let node = node.clone();
        async move {
            changed.changed().await.ok()?;
            tokio::time::sleep(SWARM_DEBOUNCE).await;
            changed.borrow_and_update();
            let next = node.mesh.swarm_snapshot();
            let events: Vec<_> = rosterd_proto::changes(&prev, &next).iter().map(|c| Ok::<_, Infallible>(sse_json(c).event(c.name()))).collect();
            Some((futures::stream::iter(events), (changed, next)))
        }
    })
    .flatten();
    Sse::new(head.chain(rest)).keep_alive(KeepAlive::new().interval(KEEP_ALIVE)).into_response()
}

/// SSE, the whole swarm first and on any change anywhere, R6, debounced.
async fn swarm_events(State(node): State<Arc<Node>>) -> Response {
    let stream = futures::stream::unfold((node.mesh.changed(), true), move |(mut changed, first)| {
        let node = node.clone();
        async move {
            if !first {
                changed.changed().await.ok()?;
                tokio::time::sleep(SWARM_DEBOUNCE).await;
                changed.borrow_and_update();
            }
            Some((Ok::<_, Infallible>(sse_json(&node.mesh.swarm_snapshot())), (changed, false)))
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(KEEP_ALIVE)).into_response()
}

async fn swarm_nodes(State(node): State<Arc<Node>>) -> Json<Vec<rosterd_proto::NodeHealth>> {
    Json(node.mesh.nodes())
}

/// R14.3 `leave`: this node revokes itself and forgets the swarm. Local socket only.
async fn swarm_leave(State(node): State<Arc<Node>>) -> Result<Json<Value>, ApiError> {
    node.mesh.leave().await?;
    Ok(Json(json!({ "swarm_id": null, "nodes": node.mesh.nodes() })))
}

#[derive(serde::Deserialize)]
struct GateBody {
    /// The gated process; the socket peer when absent.
    pid: Option<u32>,
    tool: String,
    summary: String,
    policy: PermissionPolicy,
}

/// R16.2: a harness extension holds a tool call here until it is allowed or denied. The
/// session waits in needs_attention meanwhile (as acp when the runner drives it, since the
/// extension stands in for the permission request pi's adapter never sends; as hook otherwise).
async fn gate(
    State(node): State<Arc<Node>>,
    peer: Option<Extension<ConnectInfo<Peer>>>,
    Body(body): Body<GateBody>,
) -> Result<Json<gate::GateAnswer>, ApiError> {
    let pid = body
        .pid
        .or_else(|| peer.and_then(|Extension(ConnectInfo(peer))| peer.pid))
        .ok_or_else(|| ApiError::bad_request("pid is required"))?;
    let key = key_for_pid(&node, pid)?;
    let record = node.roster.get(&key).ok_or_else(|| ApiError::not_found(format!("no session {key}")))?;
    let source = if record.holder.is_some() { Source::Acp } else { Source::Hook };
    let timeout = node.config.harness.get(&record.harness).map(|h| h.gate_timeout_s).unwrap_or(crate::config::HarnessConfig::default().gate_timeout_s);
    node.roster.claim(source, &key, Activity::NeedsAttention, &format!("gate:{}", body.tool), Utc::now())?;
    let answer = gate::wait(&key, &body.tool, &body.summary, body.policy, Duration::from_secs(timeout)).await;
    node.roster.claim(source, &key, Activity::Active, "permission_answered", Utc::now())?;
    Ok(Json(answer))
}

/// What the phone app scans, R9: a `rosterd://pair` link carrying the page's address on the
/// tailnet and the bearer, as a QR. Only the tailnet address is reachable from a phone.
async fn pair(State(node): State<Arc<Node>>) -> Result<Json<Value>, ApiError> {
    if node.config.node.ui_listen != "tailscale" {
        return Err(ApiError::new(StatusCode::CONFLICT, "node.ui_listen is loopback; set it to tailscale"));
    }
    let ip = node.mesh.tailscale_ip().await.ok_or_else(|| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "no Tailscale IP"))?;
    let link = format!("rosterd://pair?url=http://{ip}:{}&token={}", node.config.node.loopback_port, node.loopback_token);
    let svg = qrcode::QrCode::new(link.as_bytes())
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .render::<qrcode::render::svg::Color>()
        .quiet_zone(false)
        .min_dimensions(240, 240)
        .build();
    Ok(Json(json!({ "link": link, "svg": svg })))
}

async fn ui() -> Html<&'static str> {
    Html(UI)
}

/// A session route's captures: the key, and the node when the path came in under
/// `/swarm/{node_id}/`. Knows whether the request is a peer's proxied action.
struct Captures {
    node: Arc<Node>,
    key: String,
    node_id: Option<String>,
    from_peer: bool,
}

impl FromRequestParts<Arc<Node>> for Captures {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, node: &Arc<Node>) -> Result<Self, ApiError> {
        let params = match Path::<HashMap<String, String>>::from_request_parts(parts, node).await {
            Ok(Path(params)) => params,
            Err(PathRejection::MissingPathParams(_)) => HashMap::new(),
            Err(error) => return Err(ApiError::bad_request(error.body_text())),
        };
        Ok(Captures {
            node: node.clone(),
            key: params.get("key").cloned().unwrap_or_default(),
            node_id: params.get("node_id").cloned(),
            from_peer: parts.extensions.get::<PeerAuth>().is_some(),
        })
    }
}

impl Captures {
    /// The node this action runs on: None here, Some elsewhere, R7.5. A proxied action never
    /// hops again.
    fn remote(&self) -> Result<Option<String>, ApiError> {
        remote_owner(&self.node, self.node_id.as_deref(), &self.key, self.from_peer)
    }

    fn path(&self, suffix: &str) -> String {
        format!("/sessions/{}{suffix}", self.key)
    }

    /// The record on this node, once `remote` said the session is here.
    fn record(&self) -> Result<Record, ApiError> {
        self.node.roster.get(&self.key).ok_or_else(|| ApiError::not_found(format!("no session {}", self.key)))
    }

    /// Relays the owner's status and body, R7.5.
    async fn proxy(&self, node_id: String, method: Method, path: String, body: Option<Value>) -> Result<Response, ApiError> {
        let (status, value) = self.node.mesh.proxy(&node_id, method, &path, body).await?;
        Ok((status, Json(value)).into_response())
    }
}

/// Where a session action runs: None for this node. `node_id` is an explicit target from the
/// path; without one the owner comes from the roster, then the swarm snapshot.
pub(super) fn remote_owner(node: &Node, node_id: Option<&str>, key: &str, from_peer: bool) -> Result<Option<String>, ApiError> {
    let ours = node.identity.node_id.as_str();
    if let Some(target) = node_id.filter(|target| *target != ours) {
        return Ok(Some(target.to_string()));
    }
    if key.is_empty() || node.roster.get(key).is_some() {
        return Ok(None);
    }
    if from_peer {
        return Err(ApiError::not_found(format!("no session {key} on this node")));
    }
    match node.mesh.owner_of(key) {
        Some(owner) if owner == ours => Ok(None),
        Some(owner) => Ok(Some(owner)),
        None => Err(ApiError::not_found(format!("no session {key}"))),
    }
}

/// POST /sessions, R5.1: 201 with the record.
async fn start_session(captures: Captures, Body(body): Body<Value>) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, "/sessions".into(), Some(body)).await;
    }
    let request: StartSession = parse(&body)?;
    let warnings = captures.node.runner.start_warnings(&request);
    let record = captures.node.runner.start(request).await?;
    let mut value = serde_json::to_value(&record)?;
    if !warnings.is_empty() {
        // R16.1: the session runs, but not as asked.
        value["warnings"] = json!(warnings);
    }
    Ok((StatusCode::CREATED, Json(value)).into_response())
}

/// The record, the runner's or the gate's state when there is one, and the child sessions on
/// this node, R14.3 `read`.
async fn get_session(captures: Captures) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::GET, captures.path(""), None).await;
    }
    let record = captures.record()?;
    let mut value = serde_json::to_value(&record)?;
    if let Some(state) = session_state(&captures.node, &record) {
        value["state"] = serde_json::to_value(state)?;
    }
    let children: Vec<Record> =
        captures.node.roster.snapshot().records.iter().filter(|r| r.parent_session_key.as_deref() == Some(&record.session_key)).cloned().collect();
    value["children"] = serde_json::to_value(children)?;
    ok(value)
}

/// Activity, recap and pending requests, never the transcript, R6: the runner's view when this
/// node drives the session (live or suspended), else one built for a gated session (R16.2).
/// A gate pending on a driven session is listed with the runner's. None for a plain record.
pub(super) fn session_state(node: &Node, record: &Record) -> Option<SessionState> {
    let gated = gate::pending(&record.session_key);
    let mut state = node.runner.state(&record.session_key).ok();
    if state.is_none() && !gated.is_empty() {
        state = Some(SessionState {
            session_key: record.session_key.clone(),
            activity: record.activity,
            last_recap: None,
            pending: Vec::new(),
            questions: Vec::new(),
            login: None,
            permission_policy: record.permission_policy.unwrap_or(node.config.runner.default_permission_policy),
        });
    }
    if let Some(state) = &mut state {
        state.pending.extend(gated);
    }
    state
}

/// R14.3 `explain`: which source set each field and when, and the refused claims.
async fn explain(captures: Captures) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::GET, captures.path("/explain"), None).await;
    }
    ok(captures.node.roster.explain(&captures.key)?)
}

/// R15.2 by hand: the holder stops, the record stays suspended.
async fn suspend(captures: Captures) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/suspend"), None).await;
    }
    ok(captures.node.runner.suspend(&captures.key).await?)
}

/// R15.3: the new record, under a new key bound to the same harness session id.
async fn resume(captures: Captures) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/resume"), None).await;
    }
    ok(captures.node.runner.resume(&captures.key).await?)
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
pub(super) struct SpawnBody {
    pub harness: String,
    pub cwd: Option<String>,
    pub name: Option<String>,
    pub permission_policy: Option<PermissionPolicy>,
    pub model: Option<String>,
}

/// R5.4: a child session under the parent's key. Shared by `POST /sessions/{key}/spawn` and
/// the MCP tool session.spawn.
pub(super) async fn spawn_child(node: &Node, parent_key: &str, body: SpawnBody) -> Result<Value, ApiError> {
    let parent = node.roster.get(parent_key).ok_or_else(|| ApiError::not_found(format!("no session {parent_key}")))?;
    if body.harness.is_empty() {
        return Err(ApiError::bad_request("harness is required"));
    }
    let record = node
        .runner
        .start(StartSession {
            harness: body.harness,
            cwd: body.cwd.or(parent.cwd),
            parent_session_key: Some(parent.session_key),
            name: body.name,
            model: body.model,
            // A child inherits the parent's policy unless the call overrides it, R5.4.
            permission_policy: body.permission_policy.or(parent.permission_policy),
            ..StartSession::default()
        })
        .await?;
    Ok(json!({ "session_key": record.session_key, "record": record }))
}

async fn spawn(captures: Captures, Body(body): Body<Value>) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/spawn"), Some(body)).await;
    }
    ok(spawn_child(&captures.node, &captures.key, parse(&body)?).await?)
}

async fn patch_session(captures: Captures, Body(body): Body<Value>) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::PATCH, captures.path(""), Some(body)).await;
    }
    let patch: PatchSession = parse(&body)?;
    ok(captures.node.runner.patch(&captures.key, patch).await?)
}

/// DELETE stops the holder, R5.6.
async fn delete_session(captures: Captures) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::DELETE, captures.path(""), None).await;
    }
    ok(captures.node.runner.stop(&captures.key).await?)
}

async fn prompt(captures: Captures, Body(body): Body<Value>) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/prompt"), Some(body)).await;
    }
    let request: PromptRequest = parse(&body)?;
    ok(captures.node.runner.prompt(&captures.key, request).await?)
}

async fn cancel(captures: Captures) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/cancel"), None).await;
    }
    ok(captures.node.runner.cancel(&captures.key).await?)
}

/// Jumps to the session on the machine that has it: `rosterd-open` with the tmux or herdr handle,
/// run by the owner node so a page on a phone focuses the pane on the desk. Headless sessions have
/// no pane; the page links their conversation view itself.
async fn open_session(captures: Captures) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/open"), None).await;
    }
    let record = captures.record()?;
    let (kind, handle) = match (&record.herdr, &record.tmux) {
        (Some(h), _) => ("herdr", json!(h)),
        (_, Some(t)) => ("tmux", json!(t)),
        _ => return Err(ApiError::bad_request(format!("nothing to open for {}: no tmux or herdr handle", captures.key))),
    };
    let handle = json!({ "kind": kind, "machine": record.node, "session_key": captures.key, kind: handle });
    let opener = std::env::current_exe().ok().and_then(|exe| exe.parent().map(|dir| dir.join("rosterd-open"))).filter(|p| p.is_file());
    let opener = opener.or_else(|| crate::integrate::which(&std::env::var_os("PATH").unwrap_or_default(), "rosterd-open"));
    let Some(opener) = opener else { return Err(ApiError::bad_request("rosterd-open is not installed on this node".to_string())) };
    let out = tokio::process::Command::new(opener).arg("--handle").arg(handle.to_string()).output().await.map_err(|e| ApiError::bad_request(e.to_string()))?;
    if !out.status.success() {
        return Err(ApiError::bad_request(String::from_utf8_lossy(&out.stderr).trim().to_string()));
    }
    ok(json!({ "opened": kind }))
}

/// The raw ACP notification stream, R5.5. Served by the owner only: `Mesh::proxy` carries one
/// JSON answer, not a stream.
async fn stream(captures: Captures) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return Err(ApiError::bad_request(format!("session {} streams from node {node_id}; connect there", captures.key)));
    }
    let stream = BroadcastStream::new(captures.node.runner.stream(&captures.key)?).map(|item| {
        Ok::<_, Infallible>(match item {
            Ok(notification) => sse_json(&notification),
            Err(BroadcastStreamRecvError::Lagged(n)) => Event::default().comment(format!("lagged {n}")),
        })
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(KEEP_ALIVE)).into_response())
}

#[derive(serde::Deserialize)]
struct PermissionBody {
    /// The oldest pending request when absent.
    request_id: Option<Value>,
    #[serde(flatten)]
    answer: PermissionAnswer,
    /// Travels to a gated harness on a deny, R16.2.
    reason: Option<String>,
}

/// `{request_id?, outcome: selected|cancelled, option_id?, reason?}`, R5.3 attention. The
/// runner answers a session it drives; a session without one is answered at the gate, R16.2.
async fn permission(captures: Captures, Body(body): Body<Value>) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/permission"), Some(body)).await;
    }
    let PermissionBody { request_id, answer, reason } = parse(&body)?;
    let node = &captures.node;
    let runners = request_id
        .clone()
        .or_else(|| node.runner.state(&captures.key).ok().and_then(|s| s.pending.first().map(|p| p.request_id.clone())));
    if let Some(id) = runners {
        match node.runner.answer_permission(&captures.key, &id, answer.clone()).await {
            Ok(record) => return ok(record),
            Err(RunnerError::NotFound(_) | RunnerError::NoPending(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    gate::answer(&captures.key, request_id.as_ref(), answer, reason)?;
    ok(captures.record()?)
}

#[derive(serde::Deserialize)]
struct AnswerBody {
    /// The oldest pending question when absent.
    request_id: Option<Value>,
    #[serde(flatten)]
    answer: QuestionAnswer,
}

/// Answers an agent's `elicitation/create`, the form the page built from its schema.
async fn answer_question(captures: Captures, Body(body): Body<Value>) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/answer"), Some(body)).await;
    }
    let AnswerBody { request_id, answer } = parse(&body)?;
    ok(captures.node.runner.answer_question(&captures.key, request_id.as_ref(), answer).await?)
}

async fn session_name(captures: Captures, Body(body): Body<Value>) -> Result<Response, ApiError> {
    if let Some(node_id) = captures.remote()? {
        return captures.proxy(node_id, Method::POST, captures.path("/name"), Some(body)).await;
    }
    let name = parse::<NameBody>(&body)?.name;
    ok(captures.node.roster.set_name(&captures.key, name, Source::Hook)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_body_flattens_the_answer() {
        let selected: PermissionBody =
            serde_json::from_value(json!({ "request_id": 7, "outcome": "selected", "option_id": "allow" })).unwrap();
        assert_eq!(selected.request_id, Some(json!(7)));
        assert!(matches!(selected.answer, PermissionAnswer::Selected { option_id } if option_id == "allow"));
        let cancelled: PermissionBody = serde_json::from_value(json!({ "request_id": "r1", "outcome": "cancelled" })).unwrap();
        assert!(matches!(cancelled.answer, PermissionAnswer::Cancelled));
        let bare: PermissionBody = serde_json::from_value(json!({ "outcome": "selected", "option_id": "deny", "reason": "no" })).unwrap();
        assert_eq!((bare.request_id, bare.reason.as_deref()), (None, Some("no")));
        assert!(serde_json::from_value::<PermissionBody>(json!({ "request_id": 1, "outcome": "maybe" })).is_err());
    }

    #[test]
    fn resume_limit_carries_retry_after_in_the_body() {
        let error = ApiError::from(RunnerError::ResumeLimit { session_key: "k".into(), retry_after_s: 42 });
        assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error.details["retry_after_s"], json!(42));
        assert_eq!(ApiError::from(RunnerError::Suspended("k".into())).status, StatusCode::CONFLICT);
        let mark = rosterd_proto::HarnessHealth { harness: "claude".into(), state: HarnessState::LoginRequired, since: Utc::now(), until: None, detail: Some("hook: login".into()) };
        let error = ApiError::from(RunnerError::Unhealthy(mark));
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert!(error.message.starts_with("harness claude is login_required on this node since "), "{}", error.message);
        assert_eq!(error.details["health"]["state"], "login_required");
    }
}
