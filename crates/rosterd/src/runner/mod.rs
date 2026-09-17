//! The runner, R5: an ACP client that starts holders, drives sessions, maps ACP traffic to
//! claims, applies the permission policy, emits spans, resumes after a crash, R2.2, and
//! suspends and resumes sessions, R15.
//!
//! `session` is the per-session machine (ACP in, effects out), `holder` the daemon's side of
//! the holder binary in crates/holder, `acp` the framing and the pure R5 mappings. This file
//! applies effects to the roster.
//!
//! OWNER: the holder/runner agent. Unix sockets only; Windows is a later target.

mod acp;
#[cfg(test)]
pub(crate) mod e2e_test;
mod holder;
mod session;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use rosterd_proto::{Activity, EndedReason, HolderHandle, HolderState, Lane, Liveness, PermissionPolicy, Record, Source};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

use crate::config::{Config, config_dir};
use crate::roster::{Patch, Roster, RosterError};
use holder::Paths;
use session::{Effect, Session};

/// R2.2: a second crash of one harness session inside this window is not resumed.
const RESUME_COOLDOWN: chrono::Duration = chrono::Duration::minutes(5);
const HOLDER_START_TIMEOUT: Duration = Duration::from_secs(10);
/// How long `stop` waits for the holder's Exited frame before answering anyway.
const STOP_WAIT: Duration = Duration::from_secs(5);
/// How long `suspend` waits for the holder to go: SIGTERM, the holder's 5 s escalation, exit.
const SUSPEND_WAIT: Duration = Duration::from_secs(10);
/// R15.2: how often live sessions are checked against the idle timeout.
const IDLE_TICK: Duration = Duration::from_secs(30);
/// R5.5: how long a reached wait gives the turn's task to return its recap.
const TURN_SETTLE: Duration = Duration::from_millis(500);
/// R15.3: the sliding window of `max_resumes_per_hour`.
const RESUME_WINDOW: chrono::Duration = chrono::Duration::hours(1);

/// A turn in flight: stop reason and recap, R5.5.
type Turn = tokio::task::JoinHandle<Result<(Option<String>, Option<String>), RunnerError>>;

/// POST /sessions, R5.1.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct StartSession {
    pub harness: String,
    pub cwd: Option<String>,
    pub name: Option<String>,
    /// The session this one was spawned from, R5.4.
    pub parent_session_key: Option<String>,
    pub model: Option<String>,
    pub permission_policy: Option<PermissionPolicy>,
    pub env: HashMap<String, String>,
    pub recap: Option<bool>,
    /// R15.2: overrides `runner.idle_timeout_s` for this session; 0 never suspends.
    pub idle_timeout_s: Option<u64>,
    /// Forwarded as the session config option whose id contains "effort", when the adapter
    /// advertises one.
    pub effort: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitUntil {
    Idle,
    NeedsAttention,
    Ended,
}

/// POST /sessions/{key}/prompt, R5.5.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PromptRequest {
    pub prompt: String,
    #[serde(default)]
    pub wait_until: Option<WaitUntil>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PromptOutcome {
    /// The session's record; after a resume, R15.3, the record under the new key.
    pub record: Record,
    /// True when `wait_until` was reached before the timeout.
    pub reached: bool,
    pub stop_reason: Option<String>,
    pub recap: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct PatchSession {
    pub permission_policy: Option<PermissionPolicy>,
    pub recap: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermissionOption {
    pub option_id: String,
    pub name: String,
    pub kind: String,
}

/// An ACP session/request_permission left pending under policy `attention`, R5.3.
#[derive(Debug, Clone, Serialize)]
pub struct PendingPermission {
    pub request_id: serde_json::Value,
    pub tool: String,
    pub summary: String,
    pub options: Vec<PermissionOption>,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PermissionAnswer {
    Selected { option_id: String },
    Cancelled,
}

/// An ACP `elicitation/create` the agent is waiting on, form mode.
#[derive(Debug, Clone, Serialize)]
pub struct PendingQuestion {
    pub request_id: serde_json::Value,
    pub message: String,
    /// The form's `requestedSchema`, a flat object schema.
    pub schema: serde_json::Value,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionAction {
    Accept,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuestionAnswer {
    pub action: QuestionAction,
    #[serde(default)]
    pub content: Option<serde_json::Map<String, serde_json::Value>>,
}

/// One way to log the agent in, from its initialize answer.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuthMethod {
    pub id: String,
    pub name: String,
}

/// session.read_state, R6: activity and the last recap, never the transcript.
#[derive(Debug, Clone, Serialize)]
pub struct SessionState {
    pub session_key: String,
    pub activity: Activity,
    pub last_recap: Option<String>,
    pub pending: Vec<PendingPermission>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub questions: Vec<PendingQuestion>,
    /// Set when the agent refused session/new for want of a login: the methods it offers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login: Option<Vec<AuthMethod>>,
    pub permission_policy: PermissionPolicy,
}

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("no session {0}")]
    NotFound(String),
    #[error("unknown harness {0}; add [harness.{0}] to the config")]
    UnknownHarness(String),
    #[error("no pending request {0}")]
    NoPending(String),
    /// R15.1: the key names a suspended session and the call needs a running one.
    #[error("session {0} is suspended")]
    Suspended(String),
    /// R15.3: over `max_resumes_per_hour`; the session stays suspended.
    #[error("session {session_key} was resumed too often; retry in {retry_after_s} s")]
    ResumeLimit { session_key: String, retry_after_s: u64 },
    #[error("{0}")]
    Acp(String),
    #[error("the agent wants a login on its node first")]
    AuthRequired,
    #[error("holder: {0}")]
    Holder(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Roster(#[from] RosterError),
}

/// Launch facts the holder keeps in its state file for the daemon, R2.1 item 5. The env travels
/// too, so a resume relaunches with the same environment; the file is 0600 under a 0700 directory.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct Meta {
    name: Option<String>,
    parent_session_key: Option<String>,
    /// The policy as requested; `Runner::effective_policy` applies R16.1 at every launch.
    permission_policy: Option<PermissionPolicy>,
    recap: Option<bool>,
    model: Option<String>,
    env: HashMap<String, String>,
    /// The harness session id this holder resumed, when it did.
    resumed_from: Option<String>,
    /// R15.2 per session override.
    idle_timeout_s: Option<u64>,
    effort: Option<String>,
}

impl Meta {
    fn of(state: &HolderState) -> Meta {
        serde_json::to_value(&state.meta).ok().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default()
    }

    /// The launch request that brings this holder's session back, R2.2 and R15.3.
    fn relaunch(self, state: &HolderState) -> StartSession {
        StartSession {
            harness: state.harness.clone(),
            cwd: Some(state.cwd.clone()),
            name: self.name,
            parent_session_key: self.parent_session_key,
            model: self.model,
            permission_policy: self.permission_policy,
            env: self.env,
            recap: self.recap,
            idle_timeout_s: self.idle_timeout_s,
            effort: self.effort,
        }
    }
}

/// A suspended session, R15.1: no process, the state file on disk, enough here to resume.
#[derive(Debug, Clone)]
struct Parked {
    paths: Paths,
    state: HolderState,
    last_recap: Option<String>,
}

pub struct Runner {
    config: Arc<Config>,
    roster: Arc<Roster>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    /// R15.1: suspended sessions by their last key.
    parked: Mutex<HashMap<String, Parked>>,
    /// R2.2: when each harness session last crashed.
    crashes: Mutex<HashMap<String, DateTime<Utc>>>,
    /// R15.3: when each harness session was resumed, last hour.
    resumes: Mutex<HashMap<String, Vec<DateTime<Utc>>>>,
    /// One resume at a time, so two prompts for one suspended key start one holder.
    // ponytail: one lock for all keys; resumes are rare and take a second.
    resuming: tokio::sync::Mutex<()>,
    idle_tick_ms: AtomicU64,
}

impl Runner {
    pub fn new(config: Arc<Config>, roster: Arc<Roster>) -> Arc<Runner> {
        Arc::new(Runner {
            config,
            roster,
            sessions: Mutex::new(HashMap::new()),
            parked: Mutex::new(HashMap::new()),
            crashes: Mutex::new(HashMap::new()),
            resumes: Mutex::new(HashMap::new()),
            resuming: tokio::sync::Mutex::new(()),
            idle_tick_ms: AtomicU64::new(IDLE_TICK.as_millis() as u64),
        })
    }

    /// Shortens the idle sweep's tick; call before `recover`.
    #[cfg(test)]
    pub fn set_idle_tick(&self, tick: Duration) {
        self.idle_tick_ms.store(tick.as_millis() as u64, Ordering::Relaxed);
    }

    /// On start: reconnect every live holder, replay its buffer, rebuild those sessions; report
    /// dead holders once and clean them; resume what R2.2 says to resume, restore what R15.4
    /// says to restore as suspended. Also starts the idle sweep, R15.2.
    pub async fn recover(self: &Arc<Self>) -> Result<(), RunnerError> {
        tokio::spawn(self.clone().sweep_idle());
        let dir = self.config.runner.holder_dir.clone();
        holder::ensure_dir(&dir)?;
        for (paths, state) in holder::list_states(&dir) {
            let meta = Meta::of(&state);
            let policy = self.effective_policy(&state.harness, meta.permission_policy);
            let recap = meta.recap.unwrap_or(self.config.runner.recap);
            if state.suspended {
                self.restore(&paths, state, policy);
                continue;
            }
            match holder::connect(&paths.socket).await {
                Ok(stream) => match self.attach(state, policy, recap, stream).await {
                    Ok(session) => tracing::info!(key = %session.key, socket = %paths.socket.display(), "holder reattached, R2.1"),
                    Err(error) => tracing::error!(%error, socket = %paths.socket.display(), "holder reattach failed"),
                },
                Err(_) => self.dead(&paths, state, policy),
            }
        }
        Ok(())
    }

    /// R5.1.
    pub async fn start(self: &Arc<Self>, req: StartSession) -> Result<Record, RunnerError> {
        self.launch(req, None, None).await
    }

    /// What `start` would do differently from what was asked, R16.1. The caller prints these.
    pub fn start_warnings(&self, req: &StartSession) -> Vec<String> {
        let mut warnings = Vec::new();
        let requested = req.permission_policy.unwrap_or(self.config.runner.default_permission_policy);
        if self.effective_policy(&req.harness, Some(requested)) != requested {
            warnings.push(format!(
                "pi has no permission requests without the rosterd-pi extension; policy {} runs as auto",
                policy_name(requested)
            ));
        }
        warnings
    }

    /// R16.1: a pi session without the extension gets no permission requests, so attention runs
    /// as auto.
    fn effective_policy(&self, harness: &str, requested: Option<PermissionPolicy>) -> PermissionPolicy {
        let policy = requested.unwrap_or(self.config.runner.default_permission_policy);
        let extension = self.config.harness.get("pi").is_some_and(|h| h.extension) && crate::integrate::pi_extension_installed();
        if harness == "pi" && !extension { PermissionPolicy::Auto } else { policy }
    }

    /// A prompt for a suspended key resumes it first, R15.3 item 1, when `resume_on_prompt`.
    pub async fn prompt(self: &Arc<Self>, session_key: &str, req: PromptRequest) -> Result<PromptOutcome, RunnerError> {
        let (session, session_key) = match self.session(session_key) {
            Ok(session) => (session, session_key.to_string()),
            Err(RunnerError::Suspended(key)) if self.config.runner.resume_on_prompt => {
                let record = self.resume(&key).await?;
                (self.session(&record.session_key)?, record.session_key)
            }
            Err(error) => return Err(error),
        };
        let session_key = session_key.as_str();
        let text = req.prompt;
        // The turn claims active from its own task, later than a wait for idle first reads the
        // record; claimed here first, the wait cannot answer with the idle before the turn.
        self.claim(session_key, Activity::Active, "prompt");
        let turn = tokio::spawn(async move { session.prompt(&text).await });
        let join = |turn: Turn| async { turn.await.map_err(|e| RunnerError::Acp(e.to_string()))? };
        let timeout = Duration::from_millis(req.timeout_ms.unwrap_or(0));
        let (reached, outcome) = match req.wait_until {
            Some(wanted) if !timeout.is_zero() => {
                let reached = tokio::time::timeout(timeout, self.wait_for(session_key, wanted)).await.is_ok();
                // Idle and ended land a few instructions before the turn's task returns; it gets
                // a moment to. A turn still running after that finishes in the background.
                let settle = if reached && wanted != WaitUntil::NeedsAttention { TURN_SETTLE } else { Duration::ZERO };
                let outcome = match tokio::time::timeout(settle, join(turn)).await {
                    Ok(outcome) => Some(outcome?),
                    Err(_) => None,
                };
                (reached, outcome)
            }
            wanted => {
                let outcome = join(turn).await?;
                let record = self.record(session_key)?;
                (wanted.is_none_or(|w| reached(&record, w)), Some(outcome))
            }
        };
        let (stop_reason, recap) = outcome.unwrap_or((None, None));
        Ok(PromptOutcome { record: self.record(session_key)?, reached, stop_reason, recap })
    }

    pub async fn cancel(self: &Arc<Self>, session_key: &str) -> Result<Record, RunnerError> {
        self.session(session_key)?.cancel();
        self.record(session_key)
    }

    /// DELETE: stops the holder. A suspended session
    /// has no holder; it ends with reason expired, R15.1.
    pub async fn stop(self: &Arc<Self>, session_key: &str) -> Result<Record, RunnerError> {
        let session = match self.session(session_key) {
            Ok(session) => session,
            Err(RunnerError::Suspended(_)) => {
                if let Some(parked) = self.parked.lock().unwrap().remove(session_key) {
                    parked.paths.clean();
                }
                return Ok(self.roster.end(session_key, EndedReason::Expired, Utc::now())?);
            }
            Err(error) => return Err(error),
        };
        session.stopping.store(true, std::sync::atomic::Ordering::Relaxed);
        holder::terminate(session.holder_pid);
        let deadline = tokio::time::Instant::now() + STOP_WAIT;
        while self.owns(session_key) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.record(session_key)
    }

    /// R15.2 by hand, and the idle sweep's move: the holder writes its state file marked
    /// suspended and stops; the record stays, liveness suspended, no holder.
    pub async fn suspend(self: &Arc<Self>, session_key: &str) -> Result<Record, RunnerError> {
        let session = match self.session(session_key) {
            Ok(session) => session,
            Err(RunnerError::Suspended(_)) => return self.record(session_key),
            Err(error) => return Err(error),
        };
        if session.session_id().is_none() {
            return Err(RunnerError::Acp("no session id yet; nothing to resume from".into()));
        }
        session.suspending.store(true, Ordering::Relaxed);
        session.set_state(|s| s.suspended = true);
        // The holder keeps its state file only once it holds the mark, so wait for the write
        // before stopping it.
        let paths = Paths::of_state_file(&session.socket.with_extension("json"));
        let deadline = tokio::time::Instant::now() + STOP_WAIT;
        while !holder::read_state(&paths.state).is_some_and(|s| s.suspended) {
            if tokio::time::Instant::now() >= deadline {
                session.suspending.store(false, Ordering::Relaxed);
                return Err(RunnerError::Holder(format!("holder {} did not record the suspension", session.holder_pid)));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        holder::terminate(session.holder_pid);
        let deadline = tokio::time::Instant::now() + SUSPEND_WAIT;
        while self.owns(session_key) {
            if tokio::time::Instant::now() >= deadline {
                return Err(RunnerError::Holder(format!("holder {} did not exit", session.holder_pid)));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.record(session_key)
    }

    /// R15.3: a new holder loads the stored session id; the old key ends with reason
    /// suspended and the new record, same session id, is returned. Over
    /// `max_resumes_per_hour` the session stays suspended.
    pub async fn resume(self: &Arc<Self>, session_key: &str) -> Result<Record, RunnerError> {
        let _one_at_a_time = self.resuming.lock().await;
        if self.owns(session_key) {
            return self.record(session_key);
        }
        let parked = self.parked.lock().unwrap().get(session_key).cloned();
        let Some(parked) = parked else {
            // Resumed under another key already: follow the harness session.
            return self
                .record(session_key)?
                .session_id
                .and_then(|id| self.session_by_session_id(&id))
                .map(|s| self.record(&s.key))
                .unwrap_or_else(|| Err(RunnerError::NotFound(session_key.into())));
        };
        let session_id = parked.state.session_id.clone().ok_or_else(|| RunnerError::Acp("suspended without a session id".into()))?;
        self.count_resume(session_key, &parked.state)?;
        let req = Meta::of(&parked.state).relaunch(&parked.state);
        let record = self.launch(req, Some(session_id), Some(session_key)).await?;
        tracing::info!(old = session_key, key = %record.session_key, "session resumed, R15.3");
        Ok(record)
    }

    fn count_resume(&self, session_key: &str, state: &HolderState) -> Result<(), RunnerError> {
        let now = Utc::now();
        let by = state.session_id.clone().unwrap_or_else(|| session_key.to_string());
        let mut resumes = self.resumes.lock().unwrap();
        let times = resumes.entry(by).or_default();
        times.retain(|t| now - *t < RESUME_WINDOW);
        if times.len() >= self.config.runner.max_resumes_per_hour as usize {
            let retry_after_s = times.first().map(|t| (*t + RESUME_WINDOW - now).num_seconds().max(1) as u64).unwrap_or(1);
            tracing::warn!(session_key, retry_after_s, "over max_resumes_per_hour; stays suspended, R15.3");
            return Err(RunnerError::ResumeLimit { session_key: session_key.into(), retry_after_s });
        }
        times.push(now);
        Ok(())
    }

    pub async fn patch(self: &Arc<Self>, session_key: &str, patch: PatchSession) -> Result<Record, RunnerError> {
        let session = match self.session(session_key) {
            Ok(session) => session,
            Err(RunnerError::Suspended(_)) => return self.patch_parked(session_key, patch),
            Err(error) => return Err(error),
        };
        if let Some(policy) = patch.permission_policy {
            session.set_policy(policy);
            self.roster.set_policy(session_key, policy)?;
        }
        if let Some(recap) = patch.recap {
            session.set_recap(recap);
        }
        self.record(session_key)
    }

    /// A suspended session's policy and recap flag live in its state file until it resumes.
    fn patch_parked(&self, session_key: &str, patch: PatchSession) -> Result<Record, RunnerError> {
        let mut parked = self.parked.lock().unwrap();
        let entry = parked.get_mut(session_key).ok_or_else(|| RunnerError::NotFound(session_key.into()))?;
        if let Some(policy) = patch.permission_policy {
            entry.state.meta.insert("permission_policy".into(), serde_json::json!(policy));
            self.roster.set_policy(session_key, policy)?;
        }
        if let Some(recap) = patch.recap {
            entry.state.meta.insert("recap".into(), serde_json::json!(recap));
        }
        holder::write_state(&entry.paths.state, &entry.state)?;
        self.record(session_key)
    }

    /// The raw ACP notification stream of a session, R5.5. Not stored anywhere.
    pub fn stream(&self, session_key: &str) -> Result<broadcast::Receiver<serde_json::Value>, RunnerError> {
        Ok(self.session(session_key)?.subscribe())
    }

    /// Works for a suspended key too: activity as the roster kept it, nothing pending.
    pub fn state(&self, session_key: &str) -> Result<SessionState, RunnerError> {
        let mut view = match self.session(session_key) {
            Ok(session) => session.view(),
            Err(RunnerError::Suspended(_)) => {
                let parked = self.parked.lock().unwrap().get(session_key).cloned().ok_or_else(|| RunnerError::NotFound(session_key.into()))?;
                SessionState {
                    session_key: session_key.to_string(),
                    activity: Activity::Unknown,
                    last_recap: parked.last_recap,
                    pending: Vec::new(),
                    questions: Vec::new(),
                    login: None,
                    permission_policy: self.effective_policy(&parked.state.harness, Meta::of(&parked.state).permission_policy),
                }
            }
            Err(error) => return Err(error),
        };
        view.activity = self.roster.get(session_key).map(|r| r.activity).unwrap_or_default();
        Ok(view)
    }

    /// Answers a pending permission request, R5.3 `attention`. `request_id` as JSON so a peer's
    /// proxied answer carries it unchanged.
    pub async fn answer_permission(
        self: &Arc<Self>,
        session_key: &str,
        request_id: &serde_json::Value,
        answer: PermissionAnswer,
    ) -> Result<Record, RunnerError> {
        self.session(session_key)?.answer(request_id, answer)?;
        self.record(session_key)
    }

    pub async fn answer_question(
        self: &Arc<Self>,
        session_key: &str,
        request_id: Option<&serde_json::Value>,
        answer: QuestionAnswer,
    ) -> Result<Record, RunnerError> {
        self.session(session_key)?.answer_question(request_id, answer)?;
        self.record(session_key)
    }

    /// Whether this node runs a holder for the session right now.
    pub fn owns(&self, session_key: &str) -> bool {
        self.sessions.lock().unwrap().contains_key(session_key)
    }

    // ---- Starting and attaching, R5.1 and R2.1 -----------------------------------------------

    /// A new holder around the harness's adapter; `resume` is the harness session id to load
    /// instead of opening a fresh one, R2.2, and `parked` the suspended key it replaces, R15.3.
    async fn launch(self: &Arc<Self>, req: StartSession, resume: Option<String>, parked: Option<&str>) -> Result<Record, RunnerError> {
        let harness = self.config.harness.get(&req.harness).ok_or_else(|| RunnerError::UnknownHarness(req.harness.clone()))?;
        let dir = self.config.runner.holder_dir.clone();
        holder::ensure_dir(&dir)?;
        let paths = Paths::new(&dir, &ulid::Ulid::new().to_string().to_lowercase());
        let cwd = match &req.cwd {
            Some(cwd) => PathBuf::from(cwd),
            None => std::env::current_dir()?,
        };
        let policy = self.effective_policy(&req.harness, req.permission_policy);
        let recap = req.recap.unwrap_or(self.config.runner.recap);

        // R5.1: the socket, so hooks inside the harness work too. ROSTERD_SESSION_KEY cannot be
        // exported: the key is the adapter's own pid and start time, which exist only after this
        // spawn. Hooks identify by pid plus start_ticks.
        let mut env = req.env.clone();
        env.insert("ROSTERD_SOCKET".into(), self.config.socket_path().to_string_lossy().into_owned());
        let meta = Meta {
            name: req.name.clone(),
            parent_session_key: req.parent_session_key.clone(),
            permission_policy: Some(req.permission_policy.unwrap_or(self.config.runner.default_permission_policy)),
            recap: Some(recap),
            model: req.model.clone(),
            env: req.env.clone(),
            resumed_from: resume.clone(),
            idle_timeout_s: req.idle_timeout_s,
            effort: req.effort.clone(),
        };
        let meta = serde_json::to_value(&meta).expect("meta");
        let (holder_pid, reaper) = holder::spawn(holder::Launch {
            bin: &holder::holder_bin(&self.config),
            paths: &paths,
            harness: &req.harness,
            cwd: &cwd,
            meta: &meta,
            adapter: &harness.adapter,
            args: &harness.args,
            env: &env,
        })?;
        let started = async {
            let state = holder::wait_state(&paths, &reaper, HOLDER_START_TIMEOUT).await?;
            let stream = holder::connect_retry(&paths.socket, 40).await.map_err(|e| RunnerError::Holder(format!("connect {}: {e}", paths.socket.display())))?;
            // R15.3: from here the new key owns the harness session; the old one ends with
            // reason suspended and its state file goes.
            if let Some(old) = parked {
                self.unpark(old);
            }
            self.attach(state, policy, recap, stream).await
        };
        let session = match started.await {
            Ok(session) => session,
            Err(error) => {
                holder::terminate(holder_pid);
                paths.clean();
                return Err(error);
            }
        };
        if let Err(error) = self.handshake(&session, &cwd.to_string_lossy(), resume, req.effort.as_deref()).await {
            tracing::error!(key = %session.key, %error, "ACP handshake failed; stopping the holder");
            session.stopping.store(true, std::sync::atomic::Ordering::Relaxed);
            holder::terminate(session.holder_pid);
            return Err(error);
        }
        // The drain applies the same patch from the SessionId effect; doing it here too means
        // the record returned already carries the id.
        if let Some(session_id) = session.session_id() {
            let patch = Patch { session_key: Some(session.key.clone()), session_id: Some(session_id), harness: Some(session.harness.clone()), ..Patch::default() };
            self.roster.apply(Source::Acp, patch)?;
        }
        tracing::info!(key = %session.key, harness = %session.harness, "session started, R5.1");
        self.record(&session.key)
    }

    /// initialize, then session/new or session/load, R5.1 and R2.2, then the effort option
    /// when the agent advertises one. A loaded session starts idle: nothing is running in it.
    async fn handshake(&self, session: &Arc<Session>, cwd: &str, resume: Option<String>, effort: Option<&str>) -> Result<(), RunnerError> {
        session.initialize().await?;
        let mcp = self.mcp_servers(&session.key);
        let loaded = resume.is_some();
        let answer = match resume {
            Some(session_id) if session.can_load() => session.load_session(&session_id, cwd, mcp).await?,
            Some(session_id) => {
                tracing::warn!(key = %session.key, session_id, "agent cannot load sessions; starting a fresh one");
                session.new_session(cwd, mcp).await?
            }
            None => match session.new_session(cwd, mcp).await {
                // ponytail: no auth/authenticate relay; log in on the node, then stop and start again.
                Err(RunnerError::AuthRequired) => {
                    session.need_login();
                    return Ok(());
                }
                answer => answer?,
            },
        };
        if let Some(effort) = effort {
            match acp::effort_option(&answer) {
                Some(id) => session.set_config_option(&id, effort).await?,
                None => tracing::warn!(key = %session.key, effort, "the agent advertises no effort option; ignored"),
            }
        }
        if loaded {
            self.claim(&session.key, Activity::Idle, "resumed");
        }
        Ok(())
    }

    /// R6: the rosterd MCP server every driven harness gets, with the loopback token.
    fn mcp_servers(&self, session_key: &str) -> Vec<Value> {
        let path = config_dir().join("loopback.token");
        match std::fs::read_to_string(&path) {
            Ok(token) if !token.trim().is_empty() => {
                let url = format!("http://127.0.0.1:{}/mcp", self.config.node.loopback_port);
                vec![acp::mcp_server(&url, token.trim(), session_key)]
            }
            _ => {
                tracing::warn!(path = %path.display(), "no loopback token; the session gets no rosterd MCP server");
                vec![]
            }
        }
    }

    /// A connected holder becomes a session: key from the adapter's pid and start time, the
    /// launcher and acp registrations, and the effect drain.
    async fn attach(
        self: &Arc<Self>,
        state: HolderState,
        policy: PermissionPolicy,
        recap: bool,
        stream: tokio::net::UnixStream,
    ) -> Result<Arc<Session>, RunnerError> {
        let start_ticks = self.start_ticks(state.adapter_pid).await?;
        let key = self.roster.key_of(state.adapter_pid, start_ticks);
        let (session, effects) = Session::open(key.clone(), state.clone(), policy, recap, stream, true);
        self.register(&state, &key, policy, Some(start_ticks))?;
        self.sessions.lock().unwrap().insert(key.clone(), session.clone());
        tokio::spawn(self.clone().drain(session.clone(), effects));
        if state.session_key.as_deref() != Some(&key) {
            session.set_state(|s| s.session_key = Some(key.clone()));
        }
        Ok(session)
    }

    /// The launcher and acp registrations of a holder's session.
    fn register(&self, state: &HolderState, key: &str, policy: PermissionPolicy, start_ticks: Option<u64>) -> Result<(), RunnerError> {
        let meta = Meta::of(state);
        let mut patch = launcher_patch(state, key, policy, meta);
        patch.start_ticks = start_ticks;
        self.roster.apply(Source::Launcher, patch)?;
        if let Some(session_id) = &state.session_id {
            self.roster.apply(
                Source::Acp,
                Patch { session_key: Some(key.to_string()), session_id: Some(session_id.clone()), harness: Some(state.harness.clone()), ..Patch::default() },
            )?;
        }
        Ok(())
    }

    async fn start_ticks(&self, pid: u32) -> Result<u64, RunnerError> {
        for _ in 0..20 {
            if let Some(ticks) = crate::scanner::start_ticks(pid) {
                return Ok(ticks);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(RunnerError::Holder(format!("adapter pid {pid} has no start time; did it exit?")))
    }

    /// A holder whose socket is dead: after a reboot its session is restored as suspended,
    /// R15.4; after a crash it is reported once, cleaned, maybe resumed, R2.1 and R2.2.
    fn dead(self: &Arc<Self>, paths: &Paths, state: HolderState, policy: PermissionPolicy) {
        let boot = Utc.timestamp_opt(sysinfo::System::boot_time() as i64, 0).single();
        let rebooted = boot.is_some_and(|boot| state.started_at < boot);
        if rebooted && state.session_id.is_some() {
            return self.restore(paths, state, policy);
        }
        let key = state.session_key.clone().unwrap_or_else(|| self.roster.key_of(state.adapter_pid, 0));
        let reason = if rebooted { EndedReason::Reboot } else { EndedReason::Crash };
        match self.register(&state, &key, policy, None) {
            Ok(()) => {
                if let Err(error) = self.roster.end(&key, reason, Utc::now()) {
                    tracing::warn!(%key, %error, "could not end a dead holder's session");
                }
            }
            Err(error) => tracing::warn!(%key, %error, "could not register a dead holder's session"),
        }
        tracing::info!(%key, ?reason, socket = %paths.socket.display(), "dead holder reported and cleaned");
        paths.clean();
        self.maybe_resume(state);
    }

    /// R15.4: a state file whose process is gone comes back as a suspended record under its
    /// old key, never resumed here. The file stays, marked suspended, for the resume.
    fn restore(&self, paths: &Paths, mut state: HolderState, policy: PermissionPolicy) {
        let key = state.session_key.clone().unwrap_or_else(|| self.roster.key_of(state.adapter_pid, 0));
        let registered = self.register(&state, &key, policy, None).and_then(|_| Ok(self.roster.suspend(&key, Utc::now())?));
        if let Err(error) = registered {
            tracing::warn!(%key, %error, "could not restore a suspended session");
            return;
        }
        let _ = std::fs::remove_file(&paths.socket);
        if !state.suspended {
            state.suspended = true;
            if let Err(error) = holder::write_state(&paths.state, &state) {
                tracing::warn!(%key, %error, "could not mark the state file suspended");
            }
        }
        tracing::info!(%key, "session restored as suspended, R15.4");
        self.parked.lock().unwrap().insert(key, Parked { paths: paths.clone(), state, last_recap: None });
    }

    /// R15.3: the old key of a resume ends with reason suspended and its file goes.
    fn unpark(&self, old: &str) {
        if let Some(parked) = self.parked.lock().unwrap().remove(old) {
            parked.paths.clean();
        }
        if let Err(error) = self.roster.end(old, EndedReason::Suspended, Utc::now()) {
            tracing::warn!(key = old, %error, "could not end the suspended key");
        }
    }

    // ---- Effects, R5.2 to R5.5 -------------------------------------------------------------

    async fn drain(self: Arc<Self>, session: Arc<Session>, mut effects: mpsc::UnboundedReceiver<Effect>) {
        let key = session.key.clone();
        while let Some(effect) = effects.recv().await {
            match effect {
                Effect::Claim { activity, event } => self.claim(&key, activity, event),
                Effect::SessionId(session_id) => {
                    let patch = Patch { session_key: Some(key.clone()), session_id: Some(session_id), harness: Some(session.harness.clone()), ..Patch::default() };
                    if let Err(error) = self.roster.apply(Source::Acp, patch) {
                        tracing::warn!(%key, %error, "acp registration refused");
                    }
                }
                Effect::Usage(usage) => {
                    if let Err(error) = self.roster.set_usage(&key, usage) {
                        tracing::warn!(%key, %error, "usage refused");
                    }
                }
                Effect::Mode(mode) => {
                    if let Err(error) = self.roster.update(&key, |r| r.mode = Some(mode)) {
                        tracing::warn!(%key, %error, "mode refused");
                    }
                }
                Effect::Plan(plan) => {
                    if let Err(error) = self.roster.update(&key, |r| r.plan = Some(plan)) {
                        tracing::warn!(%key, %error, "plan refused");
                    }
                }
                Effect::Exited { code, signal } => {
                    if session.suspending.load(Ordering::Relaxed) {
                        self.park(&session);
                        return;
                    }
                    let reason = if session.stopping.load(std::sync::atomic::Ordering::Relaxed) {
                        EndedReason::Killed
                    } else if code == Some(0) {
                        EndedReason::Exit
                    } else {
                        EndedReason::Crash
                    };
                    tracing::info!(%key, ?code, ?signal, ?reason, "adapter exited");
                    self.ended(&session, reason);
                    return;
                }
                Effect::Lost => {
                    if session.suspending.load(Ordering::Relaxed) {
                        self.park(&session);
                        return;
                    }
                    tracing::warn!(%key, "holder connection lost and not back; crash, R2.2");
                    self.ended(&session, EndedReason::Crash);
                    return;
                }
            }
        }
    }

    fn claim(&self, key: &str, activity: Activity, event: &'static str) {
        if let Err(error) = self.roster.claim(Source::Acp, key, activity, event, Utc::now()) {
            tracing::warn!(%key, %error, event, "claim refused");
        }
    }

    fn ended(self: &Arc<Self>, session: &Arc<Session>, reason: EndedReason) {
        self.sessions.lock().unwrap().remove(&session.key);
        if let Err(error) = self.roster.end(&session.key, reason, Utc::now()) {
            tracing::warn!(key = %session.key, %error, "could not end the session");
        }
        Paths::of_state_file(&session.socket.with_extension("json")).clean();
        if reason == EndedReason::Crash {
            self.maybe_resume(session.state());
        }
    }

    /// R15.1: the holder stopped on purpose. The record stays suspended; the state file stays.
    fn park(&self, session: &Arc<Session>) {
        self.sessions.lock().unwrap().remove(&session.key);
        let paths = Paths::of_state_file(&session.socket.with_extension("json"));
        let _ = std::fs::remove_file(&paths.socket);
        match self.roster.suspend(&session.key, Utc::now()) {
            Ok(_) => tracing::info!(key = %session.key, "session suspended, R15.1"),
            Err(error) => tracing::warn!(key = %session.key, %error, "could not suspend the record"),
        }
        self.parked.lock().unwrap().insert(session.key.clone(), Parked { paths, state: session.state(), last_recap: session.last_recap() });
    }

    /// R15.2: a live session idle past its timeout is suspended. One in needs_attention only
    /// when `suspend_on_needs_attention`. Nothing else, and never a resume, on a timer.
    async fn sweep_idle(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_millis(self.idle_tick_ms.load(Ordering::Relaxed))).await;
            let sessions: Vec<Arc<Session>> = self.sessions.lock().unwrap().values().cloned().collect();
            let now = Utc::now();
            for session in sessions {
                let Some(record) = self.roster.get(&session.key).filter(|r| r.liveness == Liveness::Live) else { continue };
                let Some(at) = record.activity_at else { continue };
                let timeout = Meta::of(&session.state()).idle_timeout_s.unwrap_or(self.config.runner.idle_timeout_s);
                let eligible = match record.activity {
                    Activity::Idle => true,
                    Activity::NeedsAttention => self.config.runner.suspend_on_needs_attention,
                    Activity::Active | Activity::Unknown => false,
                };
                if timeout == 0 || !eligible || now - at < chrono::Duration::seconds(timeout as i64) {
                    continue;
                }
                if let Err(error) = self.suspend(&session.key).await {
                    tracing::warn!(key = %session.key, %error, "idle suspend failed");
                }
            }
        }
    }

    /// R2.2: one resume per crash, loading the same harness session under a new session key.
    fn maybe_resume(self: &Arc<Self>, state: HolderState) {
        if !self.config.runner.resume_on_crash {
            return;
        }
        let Some(session_id) = state.session_id.clone() else {
            return;
        };
        let now = Utc::now();
        let previous = self.crashes.lock().unwrap().insert(session_id.clone(), now);
        if previous.is_some_and(|p| now - p < RESUME_COOLDOWN) {
            tracing::warn!(session_id, "second crash within {} s; not resumed", RESUME_COOLDOWN.num_seconds());
            return;
        }
        let req = Meta::of(&state).relaunch(&state);
        let runner = self.clone();
        tokio::spawn(async move {
            match runner.launch(req, Some(session_id.clone()), None).await {
                Ok(record) => tracing::info!(session_id, key = %record.session_key, "session resumed, R2.2"),
                Err(error) => tracing::error!(session_id, %error, "resume failed"),
            }
        });
    }

    // ---- Lookups ---------------------------------------------------------------------------

    fn session(&self, session_key: &str) -> Result<Arc<Session>, RunnerError> {
        if let Some(session) = self.sessions.lock().unwrap().get(session_key) {
            return Ok(session.clone());
        }
        if self.parked.lock().unwrap().contains_key(session_key) {
            return Err(RunnerError::Suspended(session_key.into()));
        }
        Err(RunnerError::NotFound(session_key.into()))
    }

    fn session_by_session_id(&self, session_id: &str) -> Option<Arc<Session>> {
        self.sessions.lock().unwrap().values().find(|s| s.state().session_id.as_deref() == Some(session_id)).cloned()
    }

    fn record(&self, session_key: &str) -> Result<Record, RunnerError> {
        self.roster.get(session_key).ok_or_else(|| RunnerError::NotFound(session_key.into()))
    }

    /// Follows the roster until the record reaches `wanted`, R5.5.
    async fn wait_for(&self, session_key: &str, wanted: WaitUntil) {
        let mut watch = self.roster.watch();
        loop {
            let snapshot = watch.borrow_and_update().clone();
            match snapshot.records.iter().find(|r| r.session_key == session_key) {
                Some(record) if reached(record, wanted) => return,
                None if wanted == WaitUntil::Ended => return,
                _ => {}
            }
            if watch.changed().await.is_err() {
                return;
            }
        }
    }
}

fn reached(record: &Record, wanted: WaitUntil) -> bool {
    match wanted {
        WaitUntil::Idle => record.activity == Activity::Idle,
        WaitUntil::NeedsAttention => record.activity == Activity::NeedsAttention,
        WaitUntil::Ended => record.liveness == Liveness::Ended,
    }
}

fn policy_name(policy: PermissionPolicy) -> &'static str {
    match policy {
        PermissionPolicy::Auto => "auto",
        PermissionPolicy::Attention => "attention",
    }
}

/// What the launcher asserts about a holder's session, R4 and R5.1.
fn launcher_patch(state: &HolderState, key: &str, policy: PermissionPolicy, meta: Meta) -> Patch {
    Patch {
        session_key: Some(key.to_string()),
        pid: Some(state.adapter_pid),
        started_at: Some(state.started_at),
        harness: Some(state.harness.clone()),
        lane: Some(Lane::Headless),
        name: meta.name,
        parent_session_key: meta.parent_session_key,
        cwd: Some(state.cwd.clone()),
        holder: Some(HolderHandle { socket: state.socket.clone() }),
        permission_policy: Some(policy),
        ..Patch::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_round_trips_through_the_state_file() {
        let meta = Meta {
            name: Some("worker".into()),
            parent_session_key: Some("n:1:1".into()),
            permission_policy: Some(PermissionPolicy::Auto),
            recap: Some(false),
            model: None,
            env: HashMap::from([("A".to_string(), "1".to_string())]),
            resumed_from: None,
            idle_timeout_s: Some(60),
            effort: Some("high".into()),
        };
        let map: HashMap<String, Value> = serde_json::from_value(serde_json::to_value(&meta).unwrap()).unwrap();
        let state = HolderState {
            session_key: None,
            session_id: None,
            harness: "claude".into(),
            cwd: "/w".into(),
            adapter_pid: 4,
            holder_pid: 3,
            started_at: Utc::now(),
            socket: "/h/x.sock".into(),
            meta: map,
            suspended: false,
        };
        let back = Meta::of(&state);
        assert_eq!(back.name.as_deref(), Some("worker"));
        assert_eq!(back.permission_policy, Some(PermissionPolicy::Auto));
        assert_eq!(back.recap, Some(false));
        assert_eq!(back.env["A"], "1");
        assert_eq!(back.idle_timeout_s, Some(60));
        assert_eq!(Meta::of(&HolderState { meta: HashMap::new(), ..state.clone() }).name, None);
        let req = back.clone().relaunch(&state);
        assert_eq!(req.harness, "claude");
        assert_eq!(req.effort.as_deref(), Some("high"));
        assert_eq!(req.idle_timeout_s, Some(60));
        let patch = launcher_patch(&state, "n:4:9", PermissionPolicy::Auto, back);
        assert_eq!(patch.lane, Some(Lane::Headless));
        assert_eq!(patch.parent_session_key.as_deref(), Some("n:1:1"));
        assert_eq!(patch.holder.unwrap().socket, "/h/x.sock");
        assert_eq!(patch.session_key.as_deref(), Some("n:4:9"));
    }

    #[test]
    fn wait_until_reads_the_record() {
        let mut record = rosterd_proto::Record {
            node: "n".into(),
            node_id: "id".into(),
            session_key: "k".into(),
            pid: 1,
            start_ticks: 1,
            started_at: Utc::now(),
            harness: "h".into(),
            session_id: None,
            lane: Lane::Headless,
            sources: vec![],
            name: None,
            activity: Activity::Active,
            activity_event: None,
            activity_at: None,
            activity_seq: 0,
            parent_session_key: None,
            cwd: None,
            origin: None,
            tty: None,
            tmux: None,
            herdr: None,
            holder: None,
            liveness: Liveness::Live,
            ended_at: None,
            ended_reason: None,
            usage: None,
            load: None,
            mode: None,
            plan: None,
            permission_policy: None,
        };
        assert!(!reached(&record, WaitUntil::Idle));
        record.activity = Activity::Idle;
        assert!(reached(&record, WaitUntil::Idle));
        assert!(!reached(&record, WaitUntil::Ended));
        record.liveness = Liveness::Ended;
        assert!(reached(&record, WaitUntil::Ended));
    }
}
