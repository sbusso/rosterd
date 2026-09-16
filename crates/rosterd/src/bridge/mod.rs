//! The workspace bridge, R8: the only outbound path to the record. Forwards roster events as
//! activity claims, runtime handles, runtime state and session key history (A11, A13), plus
//! recaps and decision requests. Queues on disk while the workspace is unreachable and replays
//! in order, so a resume's suspended → old key ended → new key registered lands as it happened.
//!
//! The workspace API it targets is in apps/server/src/routes (activity.ts, decisions.ts,
//! attempts.ts) and the schemas in packages/shared/src/schemas/{attempt,thread}.ts. Base path
//! `/api/v1`, `Authorization: Bearer`, every answer `{data}` or `{error:{code,message,details}}`.
//!
//! Credentials. Two kinds, two jobs:
//! - The node credential (`config.workspace.credential_file`) is an `agent` scope token. It is
//!   what `POST /tasks/:id/attempts` demands (attempts.shared.ts `requireAgent`), so it is what
//!   creates child attempts, R5.4. A `bridge` scope token could not: scopes.ts lets it reach only
//!   the herdr relay (`POST /bridges`, its socket, `PATCH /attempts/:id/terminal`) and the event
//!   feed refuses it. So the node holds no bridge WebSocket.
//! - An attempt token (`bind_attempt`, from the launcher, hook or a child attempt's launchEnv)
//!   sends everything about that one attempt: claims, heartbeats, the runtime handle and its
//!   decisions. That is the whole of what scopes.ts `ATTEMPT_TOKEN` reaches.
//!
//! Inbound. decision.ruled arrives through `GET /decisions/:id/wait`, no stream needed. Prompts
//! do not arrive at all yet: `POST /attempts/:id/terminal/prompt` relays only over the herdr
//! bridge WebSocket to a mapped pane, and nothing carries a prompt to a headless session. The
//! `inbound()` receiver exists for the runner; nothing is sent on it. See `run`.
//!
//! OWNER: the bridge agent.

mod queue;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use rosterd_proto::{Activity, EndedReason, Lane, Record, Usage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Notify, broadcast};

use crate::config::{Config, write_private};
use crate::roster::{Roster, RosterEvent};
use queue::{Item, Queue};

/// packages/shared DEFAULTS.decisionWaitMaxSeconds and waitDecisionQuery's cap.
const DECISION_WAIT_S: u64 = 300;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Backoff on a lost connection or a 5xx, R8.
const BACKOFF: (Duration, Duration) = (Duration::from_secs(1), Duration::from_secs(60));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Choice {
    pub id: String,
    pub label: String,
}

/// R5.3 `decision`: a permission request as a workspace decision request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionRequest {
    pub attempt_id: String,
    pub title: String,
    pub summary: String,
    pub choices: Vec<Choice>,
    #[serde(default)]
    pub default_choice_id: Option<String>,
    #[serde(default)]
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Ruling {
    Choice { id: String },
    FreeText { text: String },
    Withdrawn,
    Expired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpanKind {
    SpanStart,
    SpanEnd,
}

/// Everything the bridge sends besides what it derives from roster events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outbound {
    /// R5.4. In-process subagents as timeline entries on the attempt.
    Span { attempt_id: String, span_id: String, label: String, kind: SpanKind, at: DateTime<Utc> },
    /// R5.5. The final assistant message of a turn.
    Recap { attempt_id: String, text: String, at: DateTime<Utc> },
    Usage { attempt_id: String, usage: Usage, at: DateTime<Utc> },
}

/// The two inbound messages, R8.
#[derive(Debug, Clone)]
pub enum Inbound {
    /// Never produced yet: the workspace has no prompt channel a node can read, R8 gap.
    #[allow(dead_code)]
    Prompt { attempt_id: String, text: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct BridgeStatus {
    pub configured: bool,
    /// The last request the workspace answered was not a lost connection or a 5xx.
    pub connected: bool,
    pub queued: usize,
    /// R5.4 conflicts seen. The workspace has no endpoint for them; they are logged here.
    pub conflicts: u64,
    /// Spans and usage reports with no workspace endpoint yet, see `send`.
    pub dropped: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChildAttempt {
    pub attempt_id: String,
    pub token: String,
    pub task_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("no workspace configured")]
    NotConfigured,
    #[error("no token known for attempt {0}")]
    NoToken(String),
    #[error("workspace answered {status}: {body}")]
    Refused { status: u16, body: String },
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub struct Bridge {
    roster: Arc<Roster>,
    inbound: broadcast::Sender<Inbound>,
    http: reqwest::Client,
    /// `<workspace url>/api/v1`, None when unconfigured.
    api: Option<String>,
    credential: Option<String>,
    tokens: Mutex<HashMap<String, String>>,
    tokens_path: PathBuf,
    queue: Mutex<Queue>,
    /// Pokes the sender: something was queued or a token arrived.
    wake: Notify,
    connected: AtomicBool,
    conflicts: AtomicU64,
    dropped: AtomicU64,
    /// Attempts already logged for a missing token, so the log says it once.
    warned: Mutex<HashSet<String>>,
    backoff: (Duration, Duration),
}

impl Bridge {
    pub fn new(config: Arc<Config>, roster: Arc<Roster>) -> anyhow::Result<Arc<Bridge>> {
        Bridge::open(&config, roster, &crate::config::state_dir(), BACKOFF)
    }

    fn open(config: &Config, roster: Arc<Roster>, state_dir: &Path, backoff: (Duration, Duration)) -> anyhow::Result<Arc<Bridge>> {
        let (inbound, _) = broadcast::channel(256);
        let api = config.workspace.url.as_deref().map(|u| format!("{}/api/v1", u.trim_end_matches('/')));
        let credential = match std::fs::read_to_string(&config.workspace.credential_file) {
            Ok(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
            _ => None,
        };
        if api.is_none() || credential.is_none() {
            tracing::warn!("no workspace url or credential; the bridge queues and sends nothing, R8");
        }
        let tokens_path = state_dir.join("attempt-tokens.json");
        let tokens = std::fs::read(&tokens_path).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        let queue = Queue::open(&state_dir.join("bridge-queue.jsonl"))?;
        Ok(Arc::new(Bridge {
            roster,
            inbound,
            http: reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build()?,
            api,
            credential,
            tokens: Mutex::new(tokens),
            tokens_path,
            queue: Mutex::new(queue),
            wake: Notify::new(),
            connected: AtomicBool::new(false),
            conflicts: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            warned: Mutex::new(HashSet::new()),
            backoff,
        }))
    }

    /// Subscribes to roster events, drains the queue, holds the workspace event stream. Never
    /// returns. With no workspace configured it only keeps the queue.
    pub async fn run(self: Arc<Self>) {
        tokio::spawn(self.clone().drain());
        // ponytail: no inbound task. Nothing in the workspace carries a prompt to a headless
        // session yet; POST /attempts/:id/terminal/prompt only relays over the herdr bridge
        // socket to a mapped pane. When an `attempt.prompt` event (or a /attempts/:id/prompt
        // route the node polls with its credential) exists, subscribe here and forward it as
        // Inbound::Prompt on `self.inbound`.
        let mut events = self.roster.events();
        loop {
            match events.recv().await {
                Ok(event) => self.handle_event(event),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(missed = n, "bridge fell behind the roster; claims skipped")
                }
                Err(broadcast::error::RecvError::Closed) => std::future::pending().await,
            }
        }
    }

    /// Remembers the attempt token a launcher or hook handed over, so claims for that attempt
    /// can be sent. Persisted with the queue.
    ///
    /// ponytail: never pruned. The workspace revokes the token at finish (attempts.ts), after
    /// which its items answer 401 and are dropped one by one; forget the entry on that 401 if
    /// the file ever matters.
    pub fn bind_attempt(&self, attempt_id: &str, token: &str) {
        let snapshot = {
            let mut tokens = self.tokens.lock().unwrap();
            tokens.insert(attempt_id.to_string(), token.to_string());
            serde_json::to_vec(&*tokens).expect("a string map serialises")
        };
        if let Err(error) = write_private(&self.tokens_path, &snapshot) {
            tracing::error!(%error, "attempt token not persisted; claims after a restart will wait for it");
        }
        self.warned.lock().unwrap().remove(attempt_id);
        self.wake.notify_one();
    }

    pub fn send(&self, message: Outbound) {
        match message {
            // R5.5. The closest write an attempt token reaches: the heartbeat note, 500 chars,
            // read back as the attempt's lastScreenText.
            Outbound::Recap { attempt_id, text, .. } => {
                let note = clip(&text, 500);
                if note.is_empty() {
                    return;
                }
                self.enqueue(&attempt_id, "POST", &format!("/attempts/{attempt_id}/heartbeat"), serde_json::json!({ "note": note }));
            }
            // ponytail: no attempt timeline endpoint. `appendEntry` with eventType is
            // server-internal and POST /threads/:id/entries takes bodyMd only, from the caller's
            // own identity. Missing: POST /attempts/:id/entries {eventType: span_start|span_end,
            // eventPayload: {spanId, label}} reachable by the attempt token.
            Outbound::Span { attempt_id, span_id, kind, .. } => {
                tracing::debug!(%attempt_id, %span_id, ?kind, "span dropped: no workspace endpoint");
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            // ponytail: tokensIn, tokensOut and costCents land only on POST /attempts/:id/finish,
            // which the bridge never calls (R5.6). Missing: a usage field on the claim or the
            // heartbeat.
            Outbound::Usage { attempt_id, .. } => {
                tracing::debug!(%attempt_id, "usage dropped: only finish carries it");
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Creates the decision request and waits on decision wait until ruled, withdrawn or
    /// expired, R5.3.
    pub async fn request_decision(&self, request: DecisionRequest) -> Result<Ruling, BridgeError> {
        let token = self.token_for(&request.attempt_id)?;
        let body = decision_body(&request);
        let (status, answer) = self.call_retrying("POST", "/decisions", &token, Some(&body), u32::MAX).await?;
        let id = match status {
            200 | 201 => answer["data"]["id"].as_str(),
            // One open decision per attempt, decisions.ts: an earlier one still stands, wait on it.
            409 if answer["error"]["details"]["reason"] == "decision_open" => {
                answer["error"]["details"]["decision"]["id"].as_str()
            }
            _ => None,
        }
        .map(str::to_string)
        .ok_or_else(|| refused(status, &answer))?;
        loop {
            let path = format!("/decisions/{id}/wait?timeoutS={DECISION_WAIT_S}");
            let (status, answer) = self.call_retrying("GET", &path, &token, None, u32::MAX).await?;
            if !(200..300).contains(&status) {
                return Err(refused(status, &answer));
            }
            if let Some(ruling) = ruling_of(&answer["data"]) {
                return Ok(ruling);
            }
        }
    }

    /// session.spawn, R5.4: a child attempt in the workspace, created with the node credential.
    ///
    /// `task` is the workspace task the child works; without one it is the parent's task, read
    /// with the parent's token (GET /attempts/:id answers an attempt token for its own attempt).
    /// The node credential opens the attempt on that task, with `parentAttemptId` (A13), and
    /// gets the child's token in `launchEnv.WORKSPACE_ATTEMPT_TOKEN`, which is bound here.
    pub async fn create_child_attempt(&self, parent_attempt_id: &str, harness: &str, task: Option<&str>) -> Result<ChildAttempt, BridgeError> {
        let credential = self.credential.clone().ok_or(BridgeError::NotConfigured)?;
        let task_id = match task {
            Some(task) => task.to_string(),
            None => {
                let parent_token = self.token_for(parent_attempt_id)?;
                let path = format!("/attempts/{parent_attempt_id}");
                let (status, answer) = self.call_retrying("GET", &path, &parent_token, None, 3).await?;
                answer["data"]["taskId"].as_str().filter(|_| status == 200).ok_or_else(|| refused(status, &answer))?.to_string()
            }
        };
        // Once, no retry: a lost answer to a create leaves an open attempt this node cannot
        // recover the token of, and a second create answers 409 anyway.
        let body = serde_json::json!({ "harness": harness, "parentAttemptId": parent_attempt_id });
        let (status, answer) = self.call_retrying("POST", &format!("/tasks/{task_id}/attempts"), &credential, Some(&body), 1).await?;
        if !(200..300).contains(&status) {
            return Err(refused(status, &answer));
        }
        let attempt_id = answer["data"]["id"].as_str().ok_or_else(|| refused(status, &answer))?.to_string();
        let token = answer["data"]["launchEnv"]["WORKSPACE_ATTEMPT_TOKEN"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("attempt {attempt_id} was created but the workspace minted no attempt token"))?
            .to_string();
        self.bind_attempt(&attempt_id, &token);
        Ok(ChildAttempt { attempt_id, token, task_id })
    }

    pub fn inbound(&self) -> broadcast::Receiver<Inbound> {
        self.inbound.subscribe()
    }

    pub fn status(&self) -> BridgeStatus {
        BridgeStatus {
            configured: self.configured(),
            connected: self.connected.load(Ordering::Relaxed),
            queued: self.queue.lock().unwrap().len(),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }

    fn configured(&self) -> bool {
        self.api.is_some() && self.credential.is_some()
    }

    /// Roster events to queued requests, R8. Nothing here touches the network.
    pub(crate) fn handle_event(&self, event: RosterEvent) {
        match event {
            RosterEvent::Registered(r) => {
                let Some(attempt) = r.attempt_id.clone() else { return };
                // A11 runtime_state and A13 the key's history entry ride with the handle. The
                // history takes every key bound to the attempt, interactive ones too.
                let runtime = serde_json::json!({
                    "runtime": runtime_handle(&r),
                    "runtimeState": "live",
                    "sessionKey": { "key": r.session_key, "startedAt": iso(r.started_at) },
                });
                self.enqueue(&attempt, "PATCH", &format!("/attempts/{attempt}/runtime"), runtime);
                // SessionStart's heartbeat, attempt.ts: names the harness and its session.
                let heartbeat = Heartbeat { harness: non_empty(&r.harness, 80), session_id: r.session_id.as_deref().and_then(|s| non_empty(s, 200)) };
                self.enqueue(&attempt, "POST", &format!("/attempts/{attempt}/heartbeat"), serde_json::to_value(heartbeat).unwrap());
            }
            RosterEvent::Claimed(r) => {
                let Some(attempt) = r.attempt_id.clone() else { return };
                if r.activity == Activity::Unknown {
                    return; // never claimed, activityClaimInput refuses it
                }
                let claim = Claim {
                    activity: r.activity,
                    event: r.activity_event.as_deref().and_then(|e| non_empty(e, 80)),
                    harness: non_empty(&r.harness, 80),
                    session_id: r.session_id.as_deref().and_then(|s| non_empty(s, 200)),
                    observed_at: r.activity_at.map(iso),
                    seq: (r.activity_seq > 0).then_some(r.activity_seq),
                };
                self.enqueue(&attempt, "POST", &format!("/attempts/{attempt}/activity"), serde_json::to_value(claim).unwrap());
            }
            RosterEvent::Named(_) => {} // no workspace field for a display name
            // A11. The attempt is not stale, it is suspended; activity, session_id and the
            // handle stay as they are.
            RosterEvent::Suspended(r) => {
                let Some(attempt) = r.attempt_id.clone() else { return };
                self.enqueue(&attempt, "PATCH", &format!("/attempts/{attempt}/runtime"), serde_json::json!({ "runtimeState": "suspended" }));
            }
            RosterEvent::Ended(r) => {
                let Some(attempt) = r.attempt_id.clone() else { return };
                let reason = r.ended_reason.map(|x| serde_json::to_value(x).unwrap()).and_then(|v| v.as_str().map(str::to_string)).unwrap_or_else(|| "exit".into());
                // A13: the key's history entry closes. R15.3: a key ended by a resume closes only
                // its entry; the attempt carries on under the new key, whose Registered sets
                // live, so no runtime_state and no idle claim here.
                let resumed = r.ended_reason == Some(EndedReason::Suspended);
                let mut runtime = serde_json::json!({
                    "sessionKey": { "key": r.session_key, "endedAt": iso(r.ended_at.unwrap_or_else(Utc::now)), "endReason": reason },
                });
                if !resumed {
                    runtime["runtimeState"] = "ended".into();
                }
                self.enqueue(&attempt, "PATCH", &format!("/attempts/{attempt}/runtime"), runtime);
                if resumed {
                    return;
                }
                // No seq: the roster's counter may not move on end, and a claim that ties loses,
                // so the workspace allocates the next one. The queue keeps it after every claim
                // this node sent before it.
                let claim = Claim {
                    activity: Activity::Idle,
                    event: Some(format!("ended_{reason}")),
                    harness: None,
                    session_id: None,
                    observed_at: r.ended_at.map(iso),
                    seq: None,
                };
                self.enqueue(&attempt, "POST", &format!("/attempts/{attempt}/activity"), serde_json::to_value(claim).unwrap());
            }
            // R5.4. The bound attempt is someone else's; a claim under it would be wrong. No
            // endpoint takes a conflict, so it is counted and logged.
            RosterEvent::Conflict { record, attempt_id, bound_to } => {
                tracing::warn!(session = %record.session_key, %attempt_id, %bound_to, "attempt already bound elsewhere; not reported");
                self.conflicts.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn enqueue(&self, attempt_id: &str, method: &str, path: &str, body: Value) {
        let item = Item {
            id: ulid::Ulid::new().to_string(),
            attempt_id: attempt_id.to_string(),
            method: method.to_string(),
            path: path.to_string(),
            body,
            created_at: Utc::now(),
        };
        if let Err(error) = self.queue.lock().unwrap().push(item) {
            tracing::error!(%error, "bridge queue write failed; this message is lost");
        }
        self.wake.notify_one();
    }

    fn token_for(&self, attempt_id: &str) -> Result<String, BridgeError> {
        self.tokens.lock().unwrap().get(attempt_id).cloned().ok_or_else(|| BridgeError::NoToken(attempt_id.to_string()))
    }

    /// The sender, R8 offline: one request in flight, the queue in order, a lost connection or a
    /// 5xx retried with backoff, a 4xx dropped (the workspace refused it for good; a claim that
    /// lost on seq answers 200 anyway). Items whose attempt has no token yet wait in place.
    async fn drain(self: Arc<Self>) {
        if !self.configured() {
            std::future::pending::<()>().await;
        }
        let mut delay = self.backoff.0;
        loop {
            let Some(item) = self.next_sendable() else {
                self.wake.notified().await;
                continue;
            };
            match self.send_item(&item).await {
                Sent::Retry => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(self.backoff.1);
                }
                Sent::Done => {
                    delay = self.backoff.0;
                    if let Err(error) = self.queue.lock().unwrap().remove(&item.id) {
                        tracing::error!(%error, "bridge queue write failed");
                    }
                }
            }
        }
    }

    fn next_sendable(&self) -> Option<Item> {
        let tokens = self.tokens.lock().unwrap();
        let queue = self.queue.lock().unwrap();
        let mut warned = self.warned.lock().unwrap();
        let mut next = None;
        for item in queue.iter() {
            if tokens.contains_key(&item.attempt_id) {
                next = Some(item.clone());
                break;
            }
            if warned.insert(item.attempt_id.clone()) {
                tracing::warn!(attempt = %item.attempt_id, "no token for attempt; its claims wait in the queue");
            }
        }
        next
    }

    async fn send_item(&self, item: &Item) -> Sent {
        let Ok(token) = self.token_for(&item.attempt_id) else { return Sent::Retry };
        match self.call(&item.method, &item.path, &token, Some(&item.body)).await {
            Err(error) => {
                tracing::debug!(%error, path = %item.path, "workspace unreachable; will retry");
                Sent::Retry
            }
            Ok((status, _)) if status >= 500 => {
                tracing::debug!(status, path = %item.path, "workspace error; will retry");
                Sent::Retry
            }
            Ok((status, _)) if (200..300).contains(&status) => Sent::Done,
            Ok((status @ (401 | 403 | 404), _)) => {
                tracing::warn!(status, attempt = %item.attempt_id, path = %item.path, "token gone or attempt unknown; dropped");
                Sent::Done
            }
            Ok((status, body)) => {
                tracing::warn!(status, path = %item.path, body = %body, "workspace refused; dropped");
                Sent::Done
            }
        }
    }

    /// One request. `Ok` is any HTTP answer with its status and parsed body; `Err` is no answer.
    async fn call(&self, method: &str, path: &str, token: &str, body: Option<&Value>) -> Result<(u16, Value), BridgeError> {
        let api = self.api.as_ref().ok_or(BridgeError::NotConfigured)?;
        let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(anyhow::Error::from)?;
        let mut request = self.http.request(method, format!("{api}{path}")).bearer_auth(token);
        if path.contains("/wait?") {
            request = request.timeout(Duration::from_secs(DECISION_WAIT_S) + REQUEST_TIMEOUT);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = match request.send().await {
            Ok(r) => r,
            Err(error) => {
                self.connected.store(false, Ordering::Relaxed);
                return Err(anyhow::Error::from(error).into());
            }
        };
        let status = response.status().as_u16();
        self.connected.store(status < 500, Ordering::Relaxed);
        let text = response.text().await.map_err(anyhow::Error::from)?;
        Ok((status, serde_json::from_str(&text).unwrap_or(Value::Null)))
    }

    /// `call` with the queue's retry rule: a lost connection or a 5xx backs off and tries again,
    /// up to `tries`; any other answer comes back as is.
    async fn call_retrying(&self, method: &str, path: &str, token: &str, body: Option<&Value>, tries: u32) -> Result<(u16, Value), BridgeError> {
        let mut delay = self.backoff.0;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let outcome = self.call(method, path, token, body).await;
            match outcome {
                Ok((status, _)) if status >= 500 && attempt < tries => {}
                Err(BridgeError::NotConfigured) => return Err(BridgeError::NotConfigured),
                Err(_) if attempt < tries => {}
                other => return other,
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(self.backoff.1);
        }
    }
}

enum Sent {
    Done,
    Retry,
}

/// activityClaimInput, packages/shared/src/schemas/attempt.ts.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Claim {
    activity: Activity,
    #[serde(skip_serializing_if = "Option::is_none")]
    event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    harness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
}

/// heartbeatInput.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Heartbeat {
    #[serde(skip_serializing_if = "Option::is_none")]
    harness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
}

/// runtimeHandleSchema, section 43.4: `kind` names the one object present, `machine` is the
/// node name. R5.1: a runner session is kind `headless`, and `{node, sessionKey}` is what the
/// opener needs to reach /ui/sessions/{key}, R9.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeHandle<'a> {
    kind: &'static str,
    machine: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    herdr: Option<HerdrRef<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tmux: Option<TmuxRef<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headless: Option<HeadlessRef<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HeadlessRef<'a> {
    node: String,
    session_key: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HerdrRef<'a> {
    session: &'a str,
    workspace_id: &'a str,
    pane_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_name: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TmuxRef<'a> {
    session: &'a str,
    window_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    window_name: Option<&'a str>,
    pane_id: &'a str,
}

/// herdr first: it wraps a tmux pane and is the handle the workspace opener knows best. Then
/// tmux, then the runner's own session (a holder, or the headless lane with the holder gone
/// while suspended, R15.1), else none.
pub(crate) fn runtime_handle(record: &Record) -> Value {
    let machine = clip(if record.node.is_empty() { "node" } else { &record.node }, 120);
    let handle = if let Some(h) = &record.herdr {
        RuntimeHandle {
            kind: "herdr",
            machine,
            herdr: Some(HerdrRef { session: &h.session, workspace_id: &h.workspace_id, pane_id: &h.pane_id, agent_name: h.agent_name.as_deref() }),
            tmux: None,
            headless: None,
        }
    } else if let Some(t) = &record.tmux {
        RuntimeHandle {
            kind: "tmux",
            machine,
            herdr: None,
            tmux: Some(TmuxRef { session: &t.session, window_index: t.window_index, window_name: t.window_name.as_deref(), pane_id: &t.pane_id }),
            headless: None,
        }
    } else if record.holder.is_some() || record.lane == Lane::Headless {
        let headless = Some(HeadlessRef { node: machine.clone(), session_key: &record.session_key });
        RuntimeHandle { kind: "headless", machine, herdr: None, tmux: None, headless }
    } else {
        RuntimeHandle { kind: "none", machine, herdr: None, tmux: None, headless: None }
    };
    serde_json::to_value(handle).unwrap()
}

/// decisionRequestInput, packages/shared/src/schemas/thread.ts. allowFreeText and blocking are
/// left to the workspace's defaults (both true); assigneeId and expiresAt are the workspace's
/// to resolve (default reviewer, then requester; never expires).
fn decision_body(request: &DecisionRequest) -> Value {
    let title = clip(&request.title, 120);
    let summary = clip(&request.summary, 1200);
    let mut body = serde_json::json!({
        "attemptId": request.attempt_id,
        "title": title,
        "summary": if summary.is_empty() { title.clone() } else { summary },
        "choices": request.choices.iter().take(8).map(|c| {
            let id = clip(&c.id, 60);
            let label = clip(&c.label, 120);
            serde_json::json!({ "id": id, "label": if label.is_empty() { id.clone() } else { label } })
        }).collect::<Vec<_>>(),
    });
    if let Some(id) = &request.default_choice_id {
        body["defaultChoiceId"] = Value::String(id.clone());
    }
    if let Some(context) = &request.context {
        body["context"] = context.clone();
    }
    body
}

/// A decision view to a ruling; None while it is still open.
fn ruling_of(decision: &Value) -> Option<Ruling> {
    match decision["status"].as_str()? {
        "ruled" => Some(match decision["ruling"]["choiceId"].as_str() {
            Some(id) => Ruling::Choice { id: id.to_string() },
            None => Ruling::FreeText { text: decision["ruling"]["text"].as_str().unwrap_or_default().to_string() },
        }),
        "withdrawn" => Some(Ruling::Withdrawn),
        "expired" => Some(Ruling::Expired),
        _ => None,
    }
}

fn refused(status: u16, body: &Value) -> BridgeError {
    BridgeError::Refused { status, body: body.to_string() }
}

/// isoDateTime is `z.iso.datetime({ offset: false })`: UTC with a Z, no offset.
fn iso(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn clip(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

fn non_empty(s: &str, max: usize) -> Option<String> {
    (!s.is_empty()).then(|| clip(s, max))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::net::SocketAddr;

    use axum::body::Bytes;
    use axum::extract::{Request, State};
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use rosterd_proto::{EndedReason, HerdrHandle, HolderHandle, Lane, Liveness, Source, TmuxHandle};
    use serde_json::json;

    use super::*;
    use crate::config::WorkspaceConfig;

    /// What one request looked like when it reached the fake workspace.
    #[derive(Debug, Clone)]
    struct Seen {
        method: String,
        path: String,
        auth: Option<String>,
        body: Value,
    }

    #[derive(Clone, Default)]
    struct Fake {
        seen: Arc<Mutex<Vec<Seen>>>,
        /// Scripted answers in order; empty means 200 `{data:{}}`.
        script: Arc<Mutex<VecDeque<(u16, Value)>>>,
    }

    async fn record(State(fake): State<Fake>, request: Request) -> Response {
        let (parts, body) = request.into_parts();
        let bytes: Bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        fake.seen.lock().unwrap().push(Seen {
            method: parts.method.to_string(),
            path: parts.uri.path_and_query().map(|p| p.to_string()).unwrap_or_default(),
            auth: parts.headers.get("authorization").map(|v| v.to_str().unwrap().to_string()),
            body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        });
        let (status, body) = fake.script.lock().unwrap().pop_front().unwrap_or((200, json!({ "data": {} })));
        (StatusCode::from_u16(status).unwrap(), axum::Json(body)).into_response()
    }

    /// A port picked now; the server on it starts when `serve` is called, so a test can queue
    /// before the workspace is up.
    /// Tests share one process and a port the kernel handed out and got back is not reliably
    /// free again by the time `serve` binds it (macOS closes lazily), so each test takes the
    /// next rung of a per process ladder below the ephemeral range and binds it exactly once.
    fn free_port() -> SocketAddr {
        use std::sync::atomic::{AtomicU16, Ordering};
        static NEXT: AtomicU16 = AtomicU16::new(0);
        let base = 20_000 + (std::process::id() % 20_000) as u16;
        SocketAddr::from(([127, 0, 0, 1], base + NEXT.fetch_add(1, Ordering::Relaxed)))
    }

    async fn serve(addr: SocketAddr) -> Fake {
        let fake = Fake::default();
        let app = axum::Router::new().fallback(record).with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        fake
    }

    fn state_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rosterd-bridge-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn bridge(addr: SocketAddr, dir: &Path) -> Arc<Bridge> {
        let credential = dir.join("workspace.token");
        std::fs::write(&credential, "node-agent-token\n").unwrap();
        let config = Config {
            workspace: WorkspaceConfig { url: Some(format!("http://{addr}/")), credential_file: credential },
            ..Config::default()
        };
        let roster = Roster::new("gibson", "abc", Default::default());
        Bridge::open(&config, roster, dir, (Duration::from_millis(10), Duration::from_millis(40))).unwrap()
    }

    fn record_with(attempt: &str, seq: u64) -> Record {
        Record {
            node: "gibson".into(),
            node_id: "abc".into(),
            session_key: "abc:1:1".into(),
            pid: 1,
            start_ticks: 1,
            started_at: Utc::now(),
            harness: "claude".into(),
            session_id: Some("s-1".into()),
            lane: Lane::Headless,
            sources: vec![Source::Acp],
            name: None,
            activity: Activity::Active,
            activity_event: Some("tool_call".into()),
            activity_at: Some(Utc::now()),
            activity_seq: seq,
            attempt_id: Some(attempt.into()),
            parent_attempt_id: None,
            parent_session_key: None,
            cwd: None,
            tty: None,
            tmux: None,
            herdr: None,
            holder: None,
            liveness: Liveness::Live,
            ended_at: None,
            ended_reason: None,
            usage: None,
            conflict: false,
            permission_policy: None,
        }
    }

    async fn wait_for(fake: &Fake, n: usize) -> Vec<Seen> {
        for _ in 0..200 {
            let seen = fake.seen.lock().unwrap().clone();
            if seen.len() >= n {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("saw {} requests, wanted {n}", fake.seen.lock().unwrap().len());
    }

    /// The fake sees a request before the bridge pops it from the queue; wait for the pop.
    async fn queued_settles(b: &Bridge, n: usize) {
        for _ in 0..200 {
            if b.status().queued == n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("queued {}, wanted {n}", b.status().queued);
    }

    #[tokio::test]
    async fn claims_queued_while_down_arrive_in_seq_order_after_a_5xx() {
        let dir = state_dir("order");
        let addr = free_port();
        let b = bridge(addr, &dir);
        b.bind_attempt("A", "tok-A");
        tokio::spawn(b.clone().run());
        for seq in 1..=3 {
            b.handle_event(RosterEvent::Claimed(record_with("A", seq)));
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        let status = b.status();
        assert_eq!((status.configured, status.connected, status.queued), (true, false, 3));

        let fake = serve(addr).await;
        fake.script.lock().unwrap().push_back((503, json!({ "error": { "code": "unavailable" } })));
        let seen = wait_for(&fake, 4).await;
        // The 503 answered the first claim, which was then sent again; every claim in order.
        let seqs: Vec<_> = seen.iter().map(|s| s.body["seq"].as_u64().unwrap()).collect();
        assert_eq!(seqs, [1, 1, 2, 3]);
        assert!(seen.iter().all(|s| s.method == "POST" && s.path == "/api/v1/attempts/A/activity"));
        assert_eq!(seen[0].auth.as_deref(), Some("Bearer tok-A"));
        assert_eq!(seen[0].body["activity"], "active");
        assert_eq!(seen[0].body["event"], "tool_call");
        assert_eq!(seen[0].body["harness"], "claude");
        assert!(seen[0].body["observedAt"].as_str().unwrap().ends_with('Z'));
        tokio::time::sleep(Duration::from_millis(30)).await;
        let status = b.status();
        assert_eq!((status.connected, status.queued), (true, 0));
        assert_eq!(std::fs::metadata(dir.join("bridge-queue.jsonl")).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn no_token_waits_in_place_and_a_4xx_is_dropped() {
        let dir = state_dir("token");
        let addr = free_port();
        let fake = serve(addr).await;
        let b = bridge(addr, &dir);
        tokio::spawn(b.clone().run());
        b.handle_event(RosterEvent::Claimed(record_with("A", 1)));
        b.handle_event(RosterEvent::Claimed(record_with("B", 1)));
        b.bind_attempt("B", "tok-B");
        let seen = wait_for(&fake, 1).await;
        assert_eq!(seen[0].path, "/api/v1/attempts/B/activity");
        queued_settles(&b, 1).await;

        fake.script.lock().unwrap().push_back((422, json!({ "error": { "code": "validation" } })));
        b.bind_attempt("A", "tok-A");
        b.handle_event(RosterEvent::Claimed(record_with("A", 2)));
        let seen = wait_for(&fake, 3).await;
        assert_eq!(seen[1].body["seq"], 1);
        assert_eq!(seen[2].body["seq"], 2);
        queued_settles(&b, 0).await;
    }

    #[tokio::test]
    async fn registered_and_ended_map_to_runtime_heartbeat_and_idle() {
        let dir = state_dir("events");
        let addr = free_port();
        let fake = serve(addr).await;
        let b = bridge(addr, &dir);
        b.bind_attempt("A", "tok-A");
        tokio::spawn(b.clone().run());
        let mut r = record_with("A", 1);
        r.lane = Lane::Interactive;
        r.tmux = Some(TmuxHandle { session: "main".into(), window_index: 2, window_name: None, pane_id: "%5".into() });
        b.handle_event(RosterEvent::Registered(r.clone()));
        let ended_at = Utc::now();
        r.ended_reason = Some(EndedReason::Crash);
        r.ended_at = Some(ended_at);
        b.handle_event(RosterEvent::Ended(r.clone()));
        b.send(Outbound::Recap { attempt_id: "A".into(), text: "x".repeat(600), at: Utc::now() });
        b.send(Outbound::Span { attempt_id: "A".into(), span_id: "s".into(), label: "sub".into(), kind: SpanKind::SpanStart, at: Utc::now() });
        let seen = wait_for(&fake, 5).await;
        assert_eq!((seen[0].method.as_str(), seen[0].path.as_str()), ("PATCH", "/api/v1/attempts/A/runtime"));
        assert_eq!(
            seen[0].body,
            json!({
                "runtime": { "kind": "tmux", "machine": "gibson", "tmux": { "session": "main", "windowIndex": 2, "paneId": "%5" } },
                "runtimeState": "live",
                "sessionKey": { "key": "abc:1:1", "startedAt": iso(r.started_at) },
            })
        );
        assert_eq!((seen[1].method.as_str(), seen[1].path.as_str()), ("POST", "/api/v1/attempts/A/heartbeat"));
        assert_eq!(seen[1].body, json!({ "harness": "claude", "sessionId": "s-1" }));
        assert_eq!((seen[2].method.as_str(), seen[2].path.as_str()), ("PATCH", "/api/v1/attempts/A/runtime"));
        assert_eq!(seen[2].body, json!({ "runtimeState": "ended", "sessionKey": { "key": "abc:1:1", "endedAt": iso(ended_at), "endReason": "crash" } }));
        assert_eq!(seen[3].path, "/api/v1/attempts/A/activity");
        assert_eq!(seen[3].body["activity"], "idle");
        assert_eq!(seen[3].body["event"], "ended_crash");
        assert!(seen[3].body.get("seq").is_none());
        assert_eq!(seen[4].path, "/api/v1/attempts/A/heartbeat");
        assert_eq!(seen[4].body["note"].as_str().unwrap().len(), 500);
        assert_eq!(b.status().dropped, 1);
    }

    /// R15.2, R15.3, A11, A13: a holder session suspends, resumes under a new key, then exits.
    #[tokio::test]
    async fn suspend_and_resume_report_runtime_state_and_key_history_in_order() {
        let dir = state_dir("lifecycle");
        let addr = free_port();
        let fake = serve(addr).await;
        let b = bridge(addr, &dir);
        b.bind_attempt("A", "tok-A");
        tokio::spawn(b.clone().run());
        let mut old = record_with("A", 1);
        old.holder = Some(HolderHandle { socket: "/tmp/h.sock".into() });
        b.handle_event(RosterEvent::Registered(old.clone()));
        old.holder = None;
        old.liveness = Liveness::Suspended;
        b.handle_event(RosterEvent::Suspended(old.clone()));
        let at = Utc::now();
        old.liveness = Liveness::Ended;
        old.ended_reason = Some(EndedReason::Suspended);
        old.ended_at = Some(at);
        b.handle_event(RosterEvent::Ended(old.clone()));
        let mut new = record_with("A", 1);
        new.session_key = "abc:2:2".into();
        new.holder = Some(HolderHandle { socket: "/tmp/h2.sock".into() });
        b.handle_event(RosterEvent::Registered(new.clone()));
        new.ended_reason = Some(EndedReason::Exit);
        new.ended_at = Some(at);
        b.handle_event(RosterEvent::Ended(new.clone()));

        let seen = wait_for(&fake, 8).await;
        let runtime: Vec<_> = seen.iter().filter(|s| s.path == "/api/v1/attempts/A/runtime").map(|s| s.body.clone()).collect();
        assert_eq!(
            runtime,
            [
                json!({
                    "runtime": { "kind": "headless", "machine": "gibson", "headless": { "node": "gibson", "sessionKey": "abc:1:1" } },
                    "runtimeState": "live",
                    "sessionKey": { "key": "abc:1:1", "startedAt": iso(old.started_at) },
                }),
                json!({ "runtimeState": "suspended" }),
                json!({ "sessionKey": { "key": "abc:1:1", "endedAt": iso(at), "endReason": "suspended" } }),
                json!({
                    "runtime": { "kind": "headless", "machine": "gibson", "headless": { "node": "gibson", "sessionKey": "abc:2:2" } },
                    "runtimeState": "live",
                    "sessionKey": { "key": "abc:2:2", "startedAt": iso(new.started_at) },
                }),
                json!({ "runtimeState": "ended", "sessionKey": { "key": "abc:2:2", "endedAt": iso(at), "endReason": "exit" } }),
            ]
        );
        // One idle claim, for the exit; the resume's old key ended no attempt.
        let claims: Vec<_> = seen.iter().filter(|s| s.path == "/api/v1/attempts/A/activity").map(|s| s.body["event"].clone()).collect();
        assert_eq!(claims, [json!("ended_exit")]);
        assert_eq!(seen.len(), 8);
    }

    #[test]
    fn runtime_handle_maps_herdr_tmux_headless_and_none() {
        let mut r = record_with("A", 1);
        assert_eq!(runtime_handle(&r), json!({ "kind": "headless", "machine": "gibson", "headless": { "node": "gibson", "sessionKey": "abc:1:1" } }));
        r.lane = Lane::Interactive;
        assert_eq!(runtime_handle(&r), json!({ "kind": "none", "machine": "gibson" }));
        r.holder = Some(HolderHandle { socket: "/tmp/h.sock".into() });
        assert_eq!(runtime_handle(&r)["kind"], "headless");
        r.tmux = Some(TmuxHandle { session: "main".into(), window_index: 0, window_name: Some("w".into()), pane_id: "%1".into() });
        assert_eq!(runtime_handle(&r)["tmux"]["windowName"], "w");
        r.herdr = Some(HerdrHandle { session: "h".into(), workspace_id: "ws".into(), pane_id: "p".into(), agent_name: None });
        let handle = runtime_handle(&r);
        assert_eq!(handle, json!({ "kind": "herdr", "machine": "gibson", "herdr": { "session": "h", "workspaceId": "ws", "paneId": "p" } }));
    }

    #[tokio::test]
    async fn tokens_persist_privately_across_restarts() {
        let dir = state_dir("persist");
        let addr = free_port();
        let b = bridge(addr, &dir);
        b.bind_attempt("A", "tok-A");
        drop(b);
        let b = bridge(addr, &dir);
        assert_eq!(b.token_for("A").unwrap(), "tok-A");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.join("attempt-tokens.json")).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert!(matches!(b.token_for("B"), Err(BridgeError::NoToken(_))));
    }

    #[tokio::test]
    async fn decision_is_created_then_waited_to_a_choice() {
        let dir = state_dir("decision");
        let addr = free_port();
        let fake = serve(addr).await;
        let b = bridge(addr, &dir);
        b.bind_attempt("A", "tok-A");
        {
            let mut script = fake.script.lock().unwrap();
            script.push_back((201, json!({ "data": { "id": "D1", "status": "open" } })));
            script.push_back((200, json!({ "data": { "id": "D1", "status": "open" } })));
            script.push_back((200, json!({ "data": { "id": "D1", "status": "ruled", "ruling": { "choiceId": "allow", "text": null } } })));
        }
        let ruling = b
            .request_decision(DecisionRequest {
                attempt_id: "A".into(),
                title: "Run rm -rf build?".into(),
                summary: String::new(),
                choices: vec![Choice { id: "allow".into(), label: "Allow".into() }, Choice { id: "deny".into(), label: String::new() }],
                default_choice_id: Some("deny".into()),
                context: Some(json!({ "tool": "bash" })),
            })
            .await
            .unwrap();
        assert_eq!(ruling, Ruling::Choice { id: "allow".into() });
        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!(seen[0].path, "/api/v1/decisions");
        assert_eq!(seen[0].auth.as_deref(), Some("Bearer tok-A"));
        assert_eq!(
            seen[0].body,
            json!({
                "attemptId": "A",
                "title": "Run rm -rf build?",
                "summary": "Run rm -rf build?",
                "choices": [{ "id": "allow", "label": "Allow" }, { "id": "deny", "label": "deny" }],
                "defaultChoiceId": "deny",
                "context": { "tool": "bash" },
            })
        );
        assert_eq!(seen[1].path, "/api/v1/decisions/D1/wait?timeoutS=300");
        assert_eq!(seen.len(), 3);
    }

    #[tokio::test]
    async fn an_open_decision_is_waited_on_instead() {
        let dir = state_dir("open");
        let addr = free_port();
        let fake = serve(addr).await;
        let b = bridge(addr, &dir);
        b.bind_attempt("A", "tok-A");
        {
            let mut script = fake.script.lock().unwrap();
            script.push_back((
                409,
                json!({ "error": { "code": "conflict", "message": "open", "details": { "reason": "decision_open", "decision": { "id": "D0", "status": "open" } } } }),
            ));
            script.push_back((200, json!({ "data": { "id": "D0", "status": "ruled", "ruling": { "choiceId": null, "text": "ask me later" } } })));
        }
        let request = DecisionRequest {
            attempt_id: "A".into(),
            title: "t".into(),
            summary: "s".into(),
            choices: vec![Choice { id: "a".into(), label: "A".into() }],
            default_choice_id: None,
            context: None,
        };
        let ruling = b.request_decision(request.clone()).await.unwrap();
        assert_eq!(ruling, Ruling::FreeText { text: "ask me later".into() });
        assert_eq!(fake.seen.lock().unwrap()[1].path, "/api/v1/decisions/D0/wait?timeoutS=300");

        fake.script.lock().unwrap().push_back((404, json!({ "error": { "code": "not_found" } })));
        assert!(matches!(b.request_decision(request).await, Err(BridgeError::Refused { status: 404, .. })));
        assert!(matches!(b.request_decision(DecisionRequest { attempt_id: "Z".into(), ..Default::default() }).await, Err(BridgeError::NoToken(_))));
    }

    #[tokio::test]
    async fn child_attempt_reads_the_parent_then_creates_with_the_node_credential() {
        let dir = state_dir("child");
        let addr = free_port();
        let fake = serve(addr).await;
        let b = bridge(addr, &dir);
        b.bind_attempt("P", "tok-P");
        {
            let mut script = fake.script.lock().unwrap();
            script.push_back((200, json!({ "data": { "id": "P", "taskId": "T1" } })));
            script.push_back((201, json!({ "data": { "id": "C", "taskId": "T1", "launchEnv": { "WORKSPACE_ATTEMPT_ID": "C", "WORKSPACE_ATTEMPT_TOKEN": "tok-C" } } })));
        }
        let child = b.create_child_attempt("P", "claude", None).await.unwrap();
        assert_eq!((child.attempt_id.as_str(), child.token.as_str(), child.task_id.as_str()), ("C", "tok-C", "T1"));
        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!((seen[0].method.as_str(), seen[0].path.as_str(), seen[0].auth.as_deref()), ("GET", "/api/v1/attempts/P", Some("Bearer tok-P")));
        assert_eq!((seen[1].method.as_str(), seen[1].path.as_str(), seen[1].auth.as_deref()), ("POST", "/api/v1/tasks/T1/attempts", Some("Bearer node-agent-token")));
        assert_eq!(seen[1].body, json!({ "harness": "claude", "parentAttemptId": "P" }));
        assert_eq!(b.token_for("C").unwrap(), "tok-C");

        // With a task given (session.spawn, POST /sessions/{key}/spawn) the parent is not read.
        fake.script.lock().unwrap().push_back((201, json!({ "data": { "id": "D", "taskId": "T2", "launchEnv": { "WORKSPACE_ATTEMPT_TOKEN": "tok-D" } } })));
        let child = b.create_child_attempt("P", "pi", Some("T2")).await.unwrap();
        assert_eq!((child.attempt_id.as_str(), child.task_id.as_str()), ("D", "T2"));
        let seen = fake.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 3);
        assert_eq!((seen[2].method.as_str(), seen[2].path.as_str(), seen[2].auth.as_deref()), ("POST", "/api/v1/tasks/T2/attempts", Some("Bearer node-agent-token")));
        assert_eq!(seen[2].body, json!({ "harness": "pi", "parentAttemptId": "P" }));
        assert_eq!(b.token_for("D").unwrap(), "tok-D");

        fake.script.lock().unwrap().push_back((200, json!({ "data": { "id": "P", "taskId": "T1" } })));
        fake.script.lock().unwrap().push_back((409, json!({ "error": { "code": "conflict" } })));
        assert!(matches!(b.create_child_attempt("P", "claude", None).await, Err(BridgeError::Refused { status: 409, .. })));
    }

    impl Default for DecisionRequest {
        fn default() -> Self {
            DecisionRequest { attempt_id: String::new(), title: "t".into(), summary: "s".into(), choices: vec![], default_choice_id: None, context: None }
        }
    }
}
