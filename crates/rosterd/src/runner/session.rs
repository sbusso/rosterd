//! One headless session, R5: the JSON-RPC client over the holder socket and the machine that
//! turns ACP traffic into `Effect`s. The runner applies effects to the roster and the bridge, so
//! this file needs neither and is tested against a fake agent on a pipe.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use rosterd_proto::{Activity, HolderFrame, HolderState, PermissionPolicy, Plan, Usage};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::acp::{self, Message};
use super::{AuthMethod, PendingPermission, PendingQuestion, PermissionAnswer, QuestionAction, QuestionAnswer, RunnerError, SessionState};
use crate::bridge::{BridgeError, DecisionRequest, Ruling, SpanKind};

/// Raw notifications kept for a slow `stream()` reader before it starts skipping, R5.5.
const STREAM_CAPACITY: usize = 256;
/// Pause before probing a holder whose socket closed without an Exited frame.
const RECONNECT_DELAY: Duration = Duration::from_millis(500);

/// What ACP traffic asks of the roster and the bridge, in order.
#[derive(Debug)]
pub enum Effect {
    /// R5.2.
    Claim { activity: Activity, event: &'static str },
    /// The harness session id, after session/new or session/load.
    SessionId(String),
    /// R5.4.
    Span { span_id: String, label: String, kind: SpanKind },
    /// R5.5.
    Recap(String),
    /// R3.
    Usage(Usage),
    /// The agent's current mode, from session/new or `current_mode_update`.
    Mode(String),
    /// The agent's `plan`, whole.
    Plan(Plan),
    /// R5.3 `decision`: the runner asks the bridge and answers on `reply`.
    Decision { request: DecisionRequest, reply: oneshot::Sender<Result<Ruling, BridgeError>> },
    /// The adapter exited, `HolderFrame::Exited`.
    Exited { code: Option<i32>, signal: Option<i32> },
    /// The holder socket closed and did not come back.
    Lost,
}

pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

pub struct Session {
    pub key: String,
    pub harness: String,
    pub attempt_id: Option<String>,
    pub holder_pid: u32,
    pub socket: PathBuf,
    /// Set by `stop`, so the exit that follows is reported as killed, not a crash.
    pub stopping: AtomicBool,
    /// Set by `suspend`, R15.2: the exit that follows parks the session instead of ending it.
    pub suspending: AtomicBool,
    out: mpsc::UnboundedSender<String>,
    effects: mpsc::UnboundedSender<Effect>,
    stream: broadcast::Sender<Value>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, Value>>>>,
    permissions: Mutex<Vec<PendingPermission>>,
    questions: Mutex<Vec<PendingQuestion>>,
    auth_methods: Mutex<Vec<AuthMethod>>,
    /// session/new was refused with the auth error; the agent wants a login on this node.
    login: AtomicBool,
    policy: Mutex<PermissionPolicy>,
    recap: AtomicBool,
    last_recap: Mutex<Option<String>>,
    turn_text: Mutex<String>,
    /// Tool call ids of subagents whose span is open, R5.4.
    spans: Mutex<HashSet<String>>,
    state: Mutex<HolderState>,
    load_session: AtomicBool,
    /// session/load in flight: replayed history claims nothing.
    loading: AtomicBool,
    closed: AtomicBool,
}

impl Session {
    /// Starts relaying on `stream`. With `reconnect`, a closed socket is probed once through
    /// `state.socket` before the session counts as lost.
    pub fn open<S: Io + 'static>(
        key: String,
        state: HolderState,
        policy: PermissionPolicy,
        recap: bool,
        stream: S,
        reconnect: bool,
    ) -> (Arc<Session>, mpsc::UnboundedReceiver<Effect>) {
        let (out, out_rx) = mpsc::unbounded_channel();
        let (effects, effects_rx) = mpsc::unbounded_channel();
        let (broadcast, _) = broadcast::channel(STREAM_CAPACITY);
        let session = Arc::new(Session {
            key,
            harness: state.harness.clone(),
            attempt_id: state.attempt_id.clone(),
            holder_pid: state.holder_pid,
            socket: PathBuf::from(&state.socket),
            stopping: AtomicBool::new(false),
            suspending: AtomicBool::new(false),
            out,
            effects,
            stream: broadcast,
            // R2.2: ids restart with the daemon; a time base keeps them clear of a turn the
            // previous daemon started in this holder.
            next_id: AtomicU64::new(Utc::now().timestamp_millis() as u64),
            pending: Mutex::new(HashMap::new()),
            permissions: Mutex::new(Vec::new()),
            questions: Mutex::new(Vec::new()),
            auth_methods: Mutex::new(Vec::new()),
            login: AtomicBool::new(false),
            policy: Mutex::new(policy),
            recap: AtomicBool::new(recap),
            last_recap: Mutex::new(None),
            turn_text: Mutex::new(String::new()),
            spans: Mutex::new(HashSet::new()),
            state: Mutex::new(state),
            load_session: AtomicBool::new(false),
            loading: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        });
        tokio::spawn(session.clone().io_loop(Box::new(stream), out_rx, reconnect));
        (session, effects_rx)
    }

    pub fn state(&self) -> HolderState {
        self.state.lock().unwrap().clone()
    }

    pub fn session_id(&self) -> Option<String> {
        self.state.lock().unwrap().session_id.clone()
    }

    pub fn can_load(&self) -> bool {
        self.load_session.load(Ordering::Relaxed)
    }

    pub fn policy(&self) -> PermissionPolicy {
        *self.policy.lock().unwrap()
    }

    pub fn set_policy(&self, policy: PermissionPolicy) {
        *self.policy.lock().unwrap() = policy;
        self.set_state(|s| {
            s.meta.insert("permission_policy".into(), json!(policy));
        });
    }

    pub fn set_recap(&self, recap: bool) {
        self.recap.store(recap, Ordering::Relaxed);
        self.set_state(|s| {
            s.meta.insert("recap".into(), json!(recap));
        });
    }

    pub fn last_recap(&self) -> Option<String> {
        self.last_recap.lock().unwrap().clone()
    }

    pub fn view(&self) -> SessionState {
        SessionState {
            session_key: self.key.clone(),
            activity: Activity::Unknown,
            last_recap: self.last_recap(),
            pending: self.permissions.lock().unwrap().clone(),
            questions: self.questions.lock().unwrap().clone(),
            login: self.login.load(Ordering::Relaxed).then(|| self.auth_methods.lock().unwrap().clone()),
            permission_policy: self.policy(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.stream.subscribe()
    }

    /// Updates the holder's state file, R2.1 item 5.
    pub fn set_state(&self, f: impl FnOnce(&mut HolderState)) {
        let state = {
            let mut state = self.state.lock().unwrap();
            f(&mut state);
            state.clone()
        };
        self.send(&serde_json::to_value(HolderFrame::SetState { state }).expect("state frame"));
    }

    // ---- ACP client, R5.1 ------------------------------------------------------------------

    pub async fn initialize(&self) -> Result<(), RunnerError> {
        let result = self.request("initialize", acp::initialize_params()).await?;
        let load = result["agentCapabilities"]["loadSession"].as_bool().unwrap_or(false);
        self.load_session.store(load, Ordering::Relaxed);
        *self.auth_methods.lock().unwrap() = acp::auth_methods(&result);
        Ok(())
    }

    /// The agent refused a session for want of a login: the record waits on it.
    pub fn need_login(&self) {
        self.login.store(true, Ordering::Relaxed);
        self.claim(Activity::NeedsAttention, "login");
    }

    /// Returns the agent's whole answer; `sessionId` is stored.
    pub async fn new_session(&self, cwd: &str, mcp_servers: Vec<Value>) -> Result<Value, RunnerError> {
        let result = self.request("session/new", json!({"cwd": cwd, "mcpServers": mcp_servers})).await?;
        let id = result["sessionId"]
            .as_str()
            .ok_or_else(|| RunnerError::Acp("session/new returned no sessionId".into()))?
            .to_string();
        self.set_session_id(id);
        self.mode_from(&result);
        Ok(result)
    }

    /// R2.2 resume. History the agent replays claims nothing. Returns the agent's whole answer.
    pub async fn load_session(&self, session_id: &str, cwd: &str, mcp_servers: Vec<Value>) -> Result<Value, RunnerError> {
        self.loading.store(true, Ordering::Relaxed);
        let result = self.request("session/load", json!({"sessionId": session_id, "cwd": cwd, "mcpServers": mcp_servers})).await;
        self.loading.store(false, Ordering::Relaxed);
        let result = result?;
        self.set_session_id(session_id.to_string());
        self.mode_from(&result);
        Ok(result)
    }

    fn mode_from(&self, v: &Value) {
        if let Some(mode) = acp::mode_of(v) {
            let _ = self.effects.send(Effect::Mode(mode));
        }
    }

    /// One session config option the agent advertised in its session/new or session/load answer.
    pub async fn set_config_option(&self, config_id: &str, value: &str) -> Result<(), RunnerError> {
        let session_id = self.session_id().ok_or_else(|| RunnerError::Acp("no session id yet".into()))?;
        self.request("session/set_config_option", json!({"sessionId": session_id, "configId": config_id, "value": value})).await.map(drop)
    }

    fn set_session_id(&self, id: String) {
        self.set_state(|s| s.session_id = Some(id.clone()));
        let _ = self.effects.send(Effect::SessionId(id));
    }

    /// One turn, R5.5: the stop reason and, with recap on, the turn's assistant text.
    pub async fn prompt(&self, text: &str) -> Result<(Option<String>, Option<String>), RunnerError> {
        let session_id = self.session_id().ok_or_else(|| RunnerError::Acp("no session id yet".into()))?;
        self.turn_text.lock().unwrap().clear();
        self.claim(Activity::Active, "prompt");
        let result = self.request("session/prompt", acp::prompt_params(&session_id, text)).await;
        let stop_reason = result.as_ref().ok().and_then(|r| r["stopReason"].as_str().map(String::from));
        if self.closed.load(Ordering::Relaxed) {
            return result.map(|_| (stop_reason, None));
        }
        let recap = self.turn_ended();
        result.map(|_| (stop_reason, recap))
    }

    pub fn cancel(&self) {
        if let Some(session_id) = self.session_id() {
            self.send(&acp::notification("session/cancel", json!({"sessionId": session_id})));
        }
        self.claim(Activity::Idle, "cancelled");
    }

    /// R5.3 `attention`: answers a request left pending.
    pub fn answer(&self, request_id: &Value, answer: PermissionAnswer) -> Result<(), RunnerError> {
        let pending = {
            let mut permissions = self.permissions.lock().unwrap();
            let at = permissions
                .iter()
                .position(|p| &p.request_id == request_id)
                .ok_or_else(|| RunnerError::NoPending(request_id.to_string()))?;
            permissions.remove(at)
        };
        let option = match answer {
            PermissionAnswer::Selected { option_id } => Some(option_id),
            PermissionAnswer::Cancelled => None,
        };
        self.respond(&pending.request_id, outcome(option));
        self.claim(Activity::Active, "permission_answered");
        Ok(())
    }

    /// Answers an `elicitation/create` the page took; a missing id answers the oldest one.
    pub fn answer_question(&self, request_id: Option<&Value>, answer: QuestionAnswer) -> Result<(), RunnerError> {
        let pending = {
            let mut questions = self.questions.lock().unwrap();
            let at = match request_id {
                Some(id) => questions.iter().position(|q| &q.request_id == id).ok_or_else(|| RunnerError::NoPending(id.to_string()))?,
                None if questions.is_empty() => return Err(RunnerError::NoPending("(no question)".into())),
                None => 0,
            };
            questions.remove(at)
        };
        let mut result = json!({"action": answer.action});
        if let (QuestionAction::Accept, Some(content)) = (&answer.action, answer.content) {
            result["content"] = Value::Object(content);
        }
        self.respond(&pending.request_id, result);
        self.claim(Activity::Active, "question_answered");
        Ok(())
    }

    // ---- JSON-RPC plumbing -----------------------------------------------------------------

    fn send(&self, v: &Value) {
        let _ = self.out.send(v.to_string());
    }

    fn respond(&self, id: &Value, result: Value) {
        self.send(&acp::response(id, result));
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, RunnerError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(RunnerError::Acp(format!("{method}: session closed")));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        self.send(&acp::request(id, method, params));
        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) if acp::auth_required(&error) => Err(RunnerError::AuthRequired),
            Ok(Err(error)) => Err(RunnerError::Acp(format!("{method}: {error}"))),
            Err(_) => Err(RunnerError::Acp(format!("{method}: connection closed"))),
        }
    }

    fn claim(&self, activity: Activity, event: &'static str) {
        let _ = self.effects.send(Effect::Claim { activity, event });
    }

    /// R5.5: the turn's text becomes the recap when the flag is on. Claims idle.
    fn turn_ended(&self) -> Option<String> {
        let text = std::mem::take(&mut *self.turn_text.lock().unwrap());
        let recap = (self.recap.load(Ordering::Relaxed) && !text.trim().is_empty()).then_some(text);
        if let Some(text) = &recap {
            *self.last_recap.lock().unwrap() = Some(text.clone());
            let _ = self.effects.send(Effect::Recap(text.clone()));
        }
        self.claim(Activity::Idle, "turn_end");
        recap
    }

    fn fail_pending(&self, why: &str) {
        for (_, tx) in self.pending.lock().unwrap().drain() {
            let _ = tx.send(Err(json!(why)));
        }
    }

    // ---- The relay -------------------------------------------------------------------------

    async fn io_loop(self: Arc<Self>, mut stream: Box<dyn Io>, mut out: mpsc::UnboundedReceiver<String>, reconnect: bool) {
        loop {
            let (reader, mut writer) = tokio::io::split(stream);
            let mut lines = BufReader::new(reader).lines();
            let dropped = loop {
                tokio::select! {
                    line = lines.next_line() => match line {
                        Ok(Some(line)) => self.handle_line(&line),
                        _ => break true,
                    },
                    msg = out.recv() => match msg {
                        Some(line) => {
                            if writer.write_all(line.as_bytes()).await.is_err() || writer.write_all(b"\n").await.is_err() {
                                break true;
                            }
                        }
                        None => break false,
                    },
                }
            };
            if !dropped || self.closed.load(Ordering::Relaxed) {
                return;
            }
            // The socket closed without an Exited frame: a live holder takes us back with a
            // replay, a dead one is a crash, R2.2.
            if reconnect {
                tokio::time::sleep(RECONNECT_DELAY).await;
                if let Ok(s) = super::holder::connect(&self.socket).await {
                    tracing::info!(key = %self.key, "holder socket reconnected");
                    stream = Box::new(s);
                    continue;
                }
            }
            self.closed.store(true, Ordering::Relaxed);
            self.fail_pending("holder connection lost");
            let _ = self.effects.send(Effect::Lost);
            return;
        }
    }

    fn handle_line(self: &Arc<Self>, line: &str) {
        let v: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(error) => {
                tracing::debug!(key = %self.key, %error, "not JSON; dropped");
                return;
            }
        };
        if v.get("rosterd").is_some() {
            match serde_json::from_value::<HolderFrame>(v) {
                Ok(HolderFrame::Replay { frames }) => self.replay(frames),
                Ok(HolderFrame::Exited { code, signal }) => {
                    self.closed.store(true, Ordering::Relaxed);
                    self.fail_pending("adapter exited");
                    let _ = self.effects.send(Effect::Exited { code, signal });
                }
                Ok(HolderFrame::SetState { .. }) => {}
                Err(error) => tracing::warn!(key = %self.key, %error, "bad holder frame"),
            }
            return;
        }
        self.handle_message(&v, false);
    }

    /// R2.1: what the holder buffered while no daemon listened. Pending permissions are rebuilt
    /// and claimed; of the rest only the newest activity is claimed, once.
    fn replay(self: &Arc<Self>, frames: Vec<Value>) {
        let mut last = None;
        for frame in &frames {
            if let Some(claim) = self.handle_message(frame, true) {
                last = Some(claim);
            }
        }
        if let (true, Some((activity, event))) = (self.permissions.lock().unwrap().is_empty(), last) {
            self.claim(activity, event);
        }
        tracing::info!(key = %self.key, frames = frames.len(), "holder replay consumed");
    }

    /// Returns the activity an update implies. Live traffic claims it here; a replay defers.
    fn handle_message(self: &Arc<Self>, v: &Value, replay: bool) -> Option<(Activity, &'static str)> {
        match acp::classify(v)? {
            Message::Response { id, result } => {
                self.resolve(id, result);
                None
            }
            Message::Notification { method, params } => {
                if !replay {
                    let _ = self.stream.send(v.clone());
                }
                if method != "session/update" {
                    return None;
                }
                self.handle_update(params.get("update")?, replay)
            }
            Message::Request { id, method, params } => {
                if method == "session/request_permission" {
                    self.handle_permission(id.clone(), params);
                } else if method == "elicitation/create" {
                    self.handle_question(id.clone(), params);
                } else {
                    // fs/* and terminal/*: capabilities this client did not advertise.
                    self.send(&acp::error_response(id, acp::METHOD_NOT_FOUND, &format!("{method} is not supported")));
                }
                None
            }
        }
    }

    fn resolve(&self, id: &Value, result: Result<&Value, &Value>) {
        let waiting = id.as_u64().and_then(|id| self.pending.lock().unwrap().remove(&id));
        match waiting {
            Some(tx) => {
                let _ = tx.send(result.cloned().map_err(Value::clone));
            }
            // R2.2: a turn the previous daemon process started in this holder just ended.
            None if result.is_ok_and(|r| r.get("stopReason").is_some()) => {
                self.turn_ended();
            }
            None => tracing::debug!(key = %self.key, %id, "response to an unknown request"),
        }
    }

    fn handle_update(&self, update: &Value, replay: bool) -> Option<(Activity, &'static str)> {
        // ponytail: a turn that straddles a daemon restart recaps only what arrived after it;
        // the replay has no turn boundaries to cut on.
        if let (Some(text), false) = (acp::message_text(update), replay) {
            self.turn_text.lock().unwrap().push_str(text);
        }
        let tool_call_id = update.get("toolCallId").and_then(Value::as_str).unwrap_or("");
        // R5.4: the span id is derived, so an end after a daemon restart still matches.
        let span_id = format!("{}:{tool_call_id}", self.key);
        if let Some(label) = acp::subagent_label(update) {
            self.spans.lock().unwrap().insert(tool_call_id.to_string());
            if !replay {
                let _ = self.effects.send(Effect::Span { span_id, label, kind: SpanKind::SpanStart });
            }
        } else if acp::tool_call_done(update) && self.spans.lock().unwrap().remove(tool_call_id) && !replay {
            let title = update.get("title").and_then(Value::as_str).unwrap_or("subagent").to_string();
            let _ = self.effects.send(Effect::Span { span_id, label: title, kind: SpanKind::SpanEnd });
        }
        if let (false, Some(usage)) = (replay, acp::usage_of(update)) {
            let _ = self.effects.send(Effect::Usage(usage));
        }
        self.mode_from(update);
        if let Some(plan) = acp::plan_of(update) {
            let _ = self.effects.send(Effect::Plan(plan));
        }
        if self.loading.load(Ordering::Relaxed) {
            return None;
        }
        let claim = acp::activity_of(update)?;
        if !replay {
            self.claim(claim.0, claim.1);
        }
        Some(claim)
    }

    /// R5.3.
    fn handle_permission(self: &Arc<Self>, id: Value, params: &Value) {
        let options = acp::permission_options(params);
        let (title, summary) = acp::tool_summary(params);
        match self.policy() {
            PermissionPolicy::Auto => self.respond(&id, outcome(acp::auto_option(&options))),
            PermissionPolicy::Attention => {
                self.permissions.lock().unwrap().push(PendingPermission {
                    request_id: id,
                    tool: title,
                    summary,
                    options,
                    at: Utc::now(),
                });
                self.claim(Activity::NeedsAttention, "permission");
            }
            PermissionPolicy::Decision => {
                self.claim(Activity::NeedsAttention, "permission");
                let Some(attempt_id) = self.attempt_id.clone() else {
                    tracing::warn!(key = %self.key, "decision policy without an attempt; permission cancelled");
                    self.respond(&id, outcome(None));
                    self.claim(Activity::Active, "permission_answered");
                    return;
                };
                let request = DecisionRequest {
                    attempt_id,
                    title,
                    summary,
                    choices: acp::decision_choices(),
                    default_choice_id: None,
                    context: params.get("toolCall").cloned(),
                };
                let (reply, ruled) = oneshot::channel();
                let _ = self.effects.send(Effect::Decision { request, reply });
                let session = self.clone();
                tokio::spawn(async move {
                    let option = match ruled.await {
                        Ok(Ok(ruling)) => acp::ruling_option(&ruling, &options),
                        Ok(Err(error)) => {
                            tracing::warn!(key = %session.key, %error, "decision request failed; permission cancelled");
                            None
                        }
                        Err(_) => None,
                    };
                    session.respond(&id, outcome(option));
                    session.claim(Activity::Active, "permission_answered");
                });
            }
        }
    }
}

impl Session {
    /// An `elicitation/create`: form mode waits for the page, anything else is declined.
    fn handle_question(&self, id: Value, params: &Value) {
        if params.get("mode").and_then(Value::as_str) != Some("form") {
            self.respond(&id, json!({"action": "decline"}));
            return;
        }
        self.questions.lock().unwrap().push(PendingQuestion {
            request_id: id,
            message: params.get("message").and_then(Value::as_str).unwrap_or("").to_string(),
            schema: params.get("requestedSchema").cloned().unwrap_or_else(|| json!({})),
            at: Utc::now(),
        });
        self.claim(Activity::NeedsAttention, "question");
    }
}

/// session/request_permission's answer shape.
fn outcome(option: Option<String>) -> Value {
    match option {
        Some(option_id) => json!({"outcome": {"outcome": "selected", "optionId": option_id}}),
        None => json!({"outcome": {"outcome": "cancelled"}}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, ReadHalf, WriteHalf};

    /// The agent's end of the pipe.
    struct Agent {
        lines: tokio::io::Lines<BufReader<ReadHalf<DuplexStream>>>,
        w: WriteHalf<DuplexStream>,
    }

    impl Agent {
        async fn recv(&mut self) -> Value {
            let line = tokio::time::timeout(Duration::from_secs(5), self.lines.next_line()).await.expect("timely").unwrap().expect("line");
            serde_json::from_str(&line).unwrap()
        }
        async fn send(&mut self, v: Value) {
            self.w.write_all(format!("{v}\n").as_bytes()).await.unwrap();
        }
        async fn update(&mut self, update: Value) {
            self.send(json!({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "s1", "update": update}})).await;
        }
    }

    fn state() -> HolderState {
        HolderState {
            session_key: None,
            session_id: None,
            harness: "fake".into(),
            cwd: "/tmp".into(),
            attempt_id: Some("att_1".into()),
            parent_attempt_id: None,
            adapter_pid: 1,
            holder_pid: 2,
            started_at: Utc::now(),
            socket: "/nonexistent/x.sock".into(),
            meta: HashMap::new(),
            suspended: false,
        }
    }

    fn open(policy: PermissionPolicy) -> (Arc<Session>, mpsc::UnboundedReceiver<Effect>, Agent) {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (session, effects) = Session::open("n:1:2".into(), state(), policy, true, client, false);
        let (r, w) = tokio::io::split(server);
        (session, effects, Agent { lines: BufReader::new(r).lines(), w })
    }

    async fn next(effects: &mut mpsc::UnboundedReceiver<Effect>) -> Effect {
        tokio::time::timeout(Duration::from_secs(5), effects.recv()).await.expect("timely").expect("effect")
    }

    async fn expect_claim(effects: &mut mpsc::UnboundedReceiver<Effect>, activity: Activity, event: &str) {
        match next(effects).await {
            Effect::Claim { activity: a, event: e } => assert_eq!((a, e), (activity, event)),
            other => panic!("expected claim {activity:?}/{event}, got {other:?}"),
        }
    }

    async fn handshake(session: &Arc<Session>, agent: &mut Agent, effects: &mut mpsc::UnboundedReceiver<Effect>) {
        let s = session.clone();
        let init = tokio::spawn(async move { s.initialize().await });
        let req = agent.recv().await;
        assert_eq!(req["method"], "initialize");
        assert_eq!(req["params"]["protocolVersion"], 1);
        assert_eq!(req["params"]["clientCapabilities"]["terminal"], false);
        agent.send(json!({"jsonrpc": "2.0", "id": req["id"], "result": {"protocolVersion": 1, "agentCapabilities": {"loadSession": true}}})).await;
        init.await.unwrap().unwrap();
        assert!(session.can_load());

        let s = session.clone();
        let new = tokio::spawn(async move { s.new_session("/tmp", vec![json!({"type": "http", "name": "rosterd"})]).await });
        let req = agent.recv().await;
        assert_eq!(req["method"], "session/new");
        assert_eq!(req["params"]["cwd"], "/tmp");
        assert_eq!(req["params"]["mcpServers"][0]["name"], "rosterd");
        agent.send(json!({"jsonrpc": "2.0", "id": req["id"], "result": {"sessionId": "s1"}})).await;
        assert_eq!(new.await.unwrap().unwrap()["sessionId"], "s1");
        assert_eq!(session.session_id().as_deref(), Some("s1"));
        // The state file frame carries the id; the effect follows.
        let frame: HolderFrame = serde_json::from_value(agent.recv().await).unwrap();
        assert!(matches!(frame, HolderFrame::SetState { state } if state.session_id.as_deref() == Some("s1")));
        assert!(matches!(next(effects).await, Effect::SessionId(id) if id == "s1"));
    }

    #[tokio::test]
    async fn a_turn_becomes_claims_spans_usage_and_a_recap() {
        let (session, mut effects, mut agent) = open(PermissionPolicy::Auto);
        handshake(&session, &mut agent, &mut effects).await;

        let s = session.clone();
        let turn = tokio::spawn(async move { s.prompt("go").await });
        let req = agent.recv().await;
        assert_eq!(req["method"], "session/prompt");
        assert_eq!(req["params"], json!({"sessionId": "s1", "prompt": [{"type": "text", "text": "go"}]}));
        expect_claim(&mut effects, Activity::Active, "prompt").await;

        agent.update(json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "Hello "}})).await;
        expect_claim(&mut effects, Activity::Active, "message").await;

        agent.update(json!({"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "Task: explore", "status": "pending", "rawInput": {"description": "look around"}})).await;
        match next(&mut effects).await {
            Effect::Span { span_id, label, kind } => {
                assert_eq!(span_id, "n:1:2:t1");
                assert_eq!(label, "look around");
                assert_eq!(kind, SpanKind::SpanStart);
            }
            other => panic!("{other:?}"),
        }
        expect_claim(&mut effects, Activity::Active, "tool_call").await;

        agent.update(json!({"sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "in_progress"})).await;
        expect_claim(&mut effects, Activity::Active, "tool_call").await;
        agent.update(json!({"sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "completed"})).await;
        assert!(matches!(next(&mut effects).await, Effect::Span { span_id, kind: SpanKind::SpanEnd, .. } if span_id == "n:1:2:t1"));

        // auto: answered with allow_once, no claim, R5.3.
        agent.send(json!({"jsonrpc": "2.0", "id": "p1", "method": "session/request_permission", "params": {
            "sessionId": "s1",
            "toolCall": {"toolCallId": "t2", "title": "Bash", "rawInput": {"command": "rm -rf x"}},
            "options": [{"optionId": "no", "name": "No", "kind": "reject_once"}, {"optionId": "yes", "name": "Yes", "kind": "allow_once"}],
        }})).await;
        let answer = agent.recv().await;
        assert_eq!(answer, json!({"jsonrpc": "2.0", "id": "p1", "result": {"outcome": {"outcome": "selected", "optionId": "yes"}}}));

        agent.update(json!({"sessionUpdate": "usage_update", "inputTokens": 12, "outputTokens": 3})).await;
        assert!(matches!(next(&mut effects).await, Effect::Usage(u) if u.input_tokens == Some(12)));

        agent.update(json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "world"}})).await;
        expect_claim(&mut effects, Activity::Active, "message").await;

        agent.send(json!({"jsonrpc": "2.0", "id": req["id"], "result": {"stopReason": "end_turn"}})).await;
        let (stop, recap) = turn.await.unwrap().unwrap();
        assert_eq!(stop.as_deref(), Some("end_turn"));
        assert_eq!(recap.as_deref(), Some("Hello world"));
        assert!(matches!(next(&mut effects).await, Effect::Recap(t) if t == "Hello world"));
        expect_claim(&mut effects, Activity::Idle, "turn_end").await;
        assert_eq!(session.view().last_recap.as_deref(), Some("Hello world"));

        // fs/* was never advertised.
        agent.send(json!({"jsonrpc": "2.0", "id": 9, "method": "fs/read_text_file", "params": {}})).await;
        let err = agent.recv().await;
        assert_eq!(err["id"], 9);
        assert_eq!(err["error"]["code"], acp::METHOD_NOT_FOUND);

        session.cancel();
        assert_eq!(agent.recv().await, json!({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": "s1"}}));
        expect_claim(&mut effects, Activity::Idle, "cancelled").await;
    }

    #[tokio::test]
    async fn attention_leaves_the_request_pending_until_answered() {
        let (session, mut effects, mut agent) = open(PermissionPolicy::Attention);
        handshake(&session, &mut agent, &mut effects).await;
        agent.send(json!({"jsonrpc": "2.0", "id": 5, "method": "session/request_permission", "params": {
            "toolCall": {"title": "Edit", "rawInput": {"path": "a.rs"}},
            "options": [{"optionId": "yes", "name": "Yes", "kind": "allow_once"}],
        }})).await;
        expect_claim(&mut effects, Activity::NeedsAttention, "permission").await;
        let view = session.view();
        assert_eq!(view.pending.len(), 1);
        assert_eq!(view.pending[0].tool, "Edit");
        assert_eq!(view.pending[0].summary, "Edit {\"path\":\"a.rs\"}");
        assert_eq!(view.pending[0].request_id, json!(5));

        assert!(matches!(session.answer(&json!(6), PermissionAnswer::Cancelled), Err(RunnerError::NoPending(_))));
        session.answer(&json!(5), PermissionAnswer::Selected { option_id: "yes".into() }).unwrap();
        assert_eq!(agent.recv().await["result"]["outcome"]["optionId"], "yes");
        expect_claim(&mut effects, Activity::Active, "permission_answered").await;
        assert!(session.view().pending.is_empty());
    }

    #[tokio::test]
    async fn a_form_question_waits_for_the_page_and_a_url_one_is_declined() {
        let (session, mut effects, mut agent) = open(PermissionPolicy::Attention);
        handshake(&session, &mut agent, &mut effects).await;
        agent.send(json!({"jsonrpc": "2.0", "id": 7, "method": "elicitation/create", "params": {
            "sessionId": "s1", "message": "Which branch?", "mode": "form",
            "requestedSchema": {"type": "object", "properties": {"branch": {"type": "string"}}, "required": ["branch"]},
        }})).await;
        expect_claim(&mut effects, Activity::NeedsAttention, "question").await;
        let view = session.view();
        assert_eq!(view.questions[0].message, "Which branch?");
        assert_eq!(view.questions[0].schema["required"], json!(["branch"]));

        let content = json!({"branch": "main"}).as_object().cloned();
        session.answer_question(None, QuestionAnswer { action: QuestionAction::Accept, content }).unwrap();
        assert_eq!(agent.recv().await["result"], json!({"action": "accept", "content": {"branch": "main"}}));
        expect_claim(&mut effects, Activity::Active, "question_answered").await;
        assert!(session.view().questions.is_empty());

        agent.send(json!({"jsonrpc": "2.0", "id": 8, "method": "elicitation/create", "params": {
            "sessionId": "s1", "message": "Log in", "mode": "url", "elicitationId": "e1", "url": "https://x",
        }})).await;
        assert_eq!(agent.recv().await["result"], json!({"action": "decline"}));
    }

    #[tokio::test]
    async fn mode_and_plan_updates_reach_the_record() {
        let (session, mut effects, mut agent) = open(PermissionPolicy::Attention);
        handshake(&session, &mut agent, &mut effects).await;
        agent.send(json!({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "s1", "update": {
            "sessionUpdate": "current_mode_update", "currentModeId": "plan"}}})).await;
        // Each update also claims activity; those are not under test here.
        async fn skip_claims(effects: &mut mpsc::UnboundedReceiver<Effect>) -> Effect {
            loop {
                match next(effects).await {
                    Effect::Claim { .. } => continue,
                    other => return other,
                }
            }
        }
        assert!(matches!(skip_claims(&mut effects).await, Effect::Mode(m) if m == "plan"));
        agent.send(json!({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "s1", "update": {
            "sessionUpdate": "plan", "entries": [{"content": "read", "priority": "high", "status": "completed"}, {"content": "edit", "priority": "medium", "status": "in_progress"}]}}})).await;
        let Effect::Plan(plan) = skip_claims(&mut effects).await else { panic!("plan") };
        assert_eq!(plan.entries.len(), 2);
        assert_eq!(plan.entries[1].status, "in_progress");
        agent.send(json!({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "s1", "update": {
            "sessionUpdate": "usage_update", "used": 40, "size": 200}}})).await;
        assert!(matches!(skip_claims(&mut effects).await, Effect::Usage(u) if u.context_used == Some(40) && u.context_size == Some(200)));
    }

    #[tokio::test]
    async fn decision_asks_the_bridge_and_maps_the_ruling() {
        let (session, mut effects, mut agent) = open(PermissionPolicy::Decision);
        handshake(&session, &mut agent, &mut effects).await;
        let permission = |id: u64| {
            json!({"jsonrpc": "2.0", "id": id, "method": "session/request_permission", "params": {
                "toolCall": {"title": "Bash", "rawInput": {"command": "ls"}},
                "options": [{"optionId": "y", "kind": "allow_once"}, {"optionId": "n", "kind": "reject_once"}, {"optionId": "ya", "kind": "allow_always"}],
            }})
        };
        agent.send(permission(1)).await;
        expect_claim(&mut effects, Activity::NeedsAttention, "permission").await;
        let reply = match next(&mut effects).await {
            Effect::Decision { request, reply } => {
                assert_eq!(request.attempt_id, "att_1");
                assert_eq!(request.title, "Bash");
                assert_eq!(request.summary, "Bash {\"command\":\"ls\"}");
                assert_eq!(request.choices.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(), ["allow", "deny", "allow_always"]);
                reply
            }
            other => panic!("{other:?}"),
        };
        reply.send(Ok(Ruling::Choice { id: "allow_always".into() })).unwrap();
        assert_eq!(agent.recv().await["result"]["outcome"]["optionId"], "ya");
        expect_claim(&mut effects, Activity::Active, "permission_answered").await;

        agent.send(permission(2)).await;
        expect_claim(&mut effects, Activity::NeedsAttention, "permission").await;
        let Effect::Decision { reply, .. } = next(&mut effects).await else { panic!("decision") };
        reply.send(Ok(Ruling::Expired)).unwrap();
        assert_eq!(agent.recv().await["result"]["outcome"], json!({"outcome": "cancelled"}));
        expect_claim(&mut effects, Activity::Active, "permission_answered").await;

        // Policy changes apply to the next request, R5.3.
        session.set_policy(PermissionPolicy::Auto);
        let _state_frame = agent.recv().await;
        agent.send(permission(3)).await;
        assert_eq!(agent.recv().await["result"]["outcome"]["optionId"], "y");
    }

    #[tokio::test]
    async fn replay_rebuilds_pending_and_claims_the_newest_once() {
        let (session, mut effects, mut agent) = open(PermissionPolicy::Attention);
        let frames = vec![
            json!({"jsonrpc": "2.0", "method": "session/update", "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "a"}}}}),
            json!({"jsonrpc": "2.0", "method": "session/update", "params": {"update": {"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "Task"}}}),
            json!({"jsonrpc": "2.0", "id": 4, "method": "session/request_permission", "params": {"toolCall": {"title": "Bash"}, "options": [{"optionId": "y", "kind": "allow_once"}]}}),
        ];
        agent.send(serde_json::to_value(HolderFrame::Replay { frames }).unwrap()).await;
        expect_claim(&mut effects, Activity::NeedsAttention, "permission").await;
        assert_eq!(session.view().pending.len(), 1);
        // The span map is rebuilt without re-sending its start; its end still matches.
        agent.update(json!({"sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "completed"})).await;
        assert!(matches!(next(&mut effects).await, Effect::Span { kind: SpanKind::SpanEnd, span_id, .. } if span_id == "n:1:2:t1"));

        // Without a pending permission only the newest activity is claimed.
        let (_, mut effects2, mut agent2) = open(PermissionPolicy::Attention);
        let frames = vec![
            json!({"jsonrpc": "2.0", "method": "session/update", "params": {"update": {"sessionUpdate": "tool_call", "toolCallId": "t1", "title": "Bash"}}}),
            json!({"jsonrpc": "2.0", "method": "session/update", "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "a"}}}}),
        ];
        agent2.send(serde_json::to_value(HolderFrame::Replay { frames }).unwrap()).await;
        expect_claim(&mut effects2, Activity::Active, "message").await;
        // A stopReason for a request this process never sent is the previous daemon's turn
        // ending, R2.2; the recap holds only what arrived live.
        agent2.update(json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "b"}})).await;
        expect_claim(&mut effects2, Activity::Active, "message").await;
        agent2.send(json!({"jsonrpc": "2.0", "id": 1, "result": {"stopReason": "end_turn"}})).await;
        assert!(matches!(next(&mut effects2).await, Effect::Recap(t) if t == "b"));
        expect_claim(&mut effects2, Activity::Idle, "turn_end").await;
    }

    #[tokio::test]
    async fn exit_frame_fails_requests_and_reports() {
        let (session, mut effects, mut agent) = open(PermissionPolicy::Auto);
        let s = session.clone();
        let init = tokio::spawn(async move { s.initialize().await });
        let _req = agent.recv().await;
        agent.send(serde_json::to_value(HolderFrame::Exited { code: Some(1), signal: None }).unwrap()).await;
        assert!(matches!(next(&mut effects).await, Effect::Exited { code: Some(1), signal: None }));
        assert!(matches!(init.await.unwrap(), Err(RunnerError::Acp(_))));
        assert!(matches!(session.initialize().await, Err(RunnerError::Acp(m)) if m.contains("closed")));
    }

    #[tokio::test]
    async fn a_dropped_pipe_is_lost_without_reconnect() {
        let (_session, mut effects, agent) = open(PermissionPolicy::Auto);
        drop(agent);
        assert!(matches!(next(&mut effects).await, Effect::Lost));
    }
}
