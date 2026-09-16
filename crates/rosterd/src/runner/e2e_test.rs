//! R2.1, R2.2 and R5 end to end: the real holder binary around examples/fake_acp.rs, driven by
//! `Session`. A second session on the same socket plays a restarted daemon: it consumes the
//! replay, rebuilds the pending permission, answers it, and sees the old turn end. Then R15
//! through `Runner`: suspend, resume on prompt, the resume limit, the idle sweep, a reboot.
//!
//! The holder binary is `rosterd-holder` in the same target directory; when it is missing the
//! test builds it with cargo, and skips (passing, with a note) if that fails too.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use rosterd_proto::{Activity, Capabilities, EndedReason, HolderState, Liveness, PermissionPolicy};
use serde_json::json;
use tokio::sync::mpsc;

use super::holder::{self, Launch, Paths};
use super::session::{Effect, Session};
use super::{PermissionAnswer, PromptRequest, Runner, RunnerError, StartSession};
use crate::bridge::{Bridge, DecisionRequest, Ruling, SpanKind};
use crate::config::{Config, HarnessConfig};
use crate::identity::Identity;
use crate::mesh::Mesh;
use crate::roster::Roster;

fn target_debug() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    exe.parent().unwrap().parent().unwrap().to_path_buf()
}

fn holder_bin() -> Option<PathBuf> {
    let debug = target_debug();
    let bin = debug.join("rosterd-holder");
    if !bin.exists() {
        let status = std::process::Command::new(env!("CARGO"))
            .args(["build", "-p", "rosterd-holder", "--target-dir"])
            .arg(debug.parent().unwrap())
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status();
        if !status.is_ok_and(|s| s.success()) {
            return None;
        }
    }
    bin.exists().then_some(bin)
}

async fn next(effects: &mut mpsc::UnboundedReceiver<Effect>) -> Effect {
    tokio::time::timeout(Duration::from_secs(10), effects.recv()).await.expect("timely effect").expect("effect")
}

async fn claim(effects: &mut mpsc::UnboundedReceiver<Effect>, activity: Activity, event: &str) {
    match next(effects).await {
        Effect::Claim { activity: a, event: e } => assert_eq!((a, e), (activity, event)),
        other => panic!("expected claim {activity:?}/{event}, got {other:?}"),
    }
}

async fn pending(session: &Session) -> serde_json::Value {
    for _ in 0..100 {
        if let Some(p) = session.view().pending.first() {
            return p.request_id.clone();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no pending permission");
}

#[tokio::test]
async fn a_session_through_the_real_holder_survives_a_daemon_restart() {
    let Some(bin) = holder_bin() else {
        eprintln!("skipped: rosterd-holder not built and cargo build failed");
        return;
    };
    let fake = target_debug().join("examples").join("fake_acp");
    if !fake.exists() {
        eprintln!("skipped: examples/fake_acp not built");
        return;
    }
    let dir = std::env::temp_dir().join(format!("rosterd-e2e-{}", std::process::id()));
    holder::ensure_dir(&dir).unwrap();
    let paths = Paths::new(&dir, "e2e");
    let meta = json!({"name": "e2e", "permission_policy": "attention", "recap": true});
    let (holder_pid, reaper) = holder::spawn(Launch {
        bin: &bin,
        paths: &paths,
        harness: "fake",
        cwd: &dir,
        attempt_id: Some("att_e2e"),
        parent_attempt_id: None,
        meta: &meta,
        adapter: fake.to_str().unwrap(),
        args: &[],
        env: &HashMap::from([("FAKE".to_string(), "1".to_string())]),
    })
    .unwrap();
    let state = holder::wait_state(&paths, &reaper, Duration::from_secs(10)).await.unwrap();
    assert_eq!(state.holder_pid, holder_pid);
    assert_eq!(state.attempt_id.as_deref(), Some("att_e2e"));

    // Daemon one: handshake, one full turn under `attention`.
    let stream = holder::connect_retry(&paths.socket, 40).await.unwrap();
    let (one, mut fx) = Session::open("test:1:1".into(), state.clone(), PermissionPolicy::Attention, true, stream, false);
    one.initialize().await.unwrap();
    assert!(one.can_load());
    assert_eq!(one.new_session(dir.to_str().unwrap(), vec![]).await.unwrap()["sessionId"], "fake-1");
    assert!(matches!(next(&mut fx).await, Effect::SessionId(id) if id == "fake-1"));
    // The state file now carries the session id, R2.1 item 5.
    let mut saved: Option<HolderState> = None;
    for _ in 0..100 {
        saved = holder::read_state(&paths.state).filter(|s| s.session_id.is_some());
        if saved.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let saved = saved.expect("state file with session id");
    assert_eq!(saved.session_id.as_deref(), Some("fake-1"));

    let s = one.clone();
    let turn = tokio::spawn(async move { s.prompt("hello").await });
    claim(&mut fx, Activity::Active, "prompt").await;
    claim(&mut fx, Activity::Active, "message").await;
    assert!(matches!(next(&mut fx).await, Effect::Span { kind: SpanKind::SpanStart, label, .. } if label == "look around"));
    claim(&mut fx, Activity::Active, "tool_call").await;
    claim(&mut fx, Activity::NeedsAttention, "permission").await;
    let id = pending(&one).await;
    assert_eq!(id, json!(100));
    one.answer(&id, PermissionAnswer::Selected { option_id: "allow".into() }).unwrap();
    claim(&mut fx, Activity::Active, "permission_answered").await;
    assert!(matches!(next(&mut fx).await, Effect::Span { kind: SpanKind::SpanEnd, .. }));
    claim(&mut fx, Activity::Active, "message").await;
    assert!(matches!(next(&mut fx).await, Effect::Usage(u) if u.input_tokens == Some(7)));
    assert!(matches!(next(&mut fx).await, Effect::Recap(t) if t == "You said: hello"));
    claim(&mut fx, Activity::Idle, "turn_end").await;
    let (stop, recap) = turn.await.unwrap().unwrap();
    assert_eq!(stop.as_deref(), Some("end_turn"));
    assert_eq!(recap.as_deref(), Some("You said: hello"));

    // A second turn blocks on its permission; then the daemon "restarts".
    let s = one.clone();
    let _turn = tokio::spawn(async move { s.prompt("again").await });
    claim(&mut fx, Activity::Active, "prompt").await;
    claim(&mut fx, Activity::Active, "message").await;
    assert!(matches!(next(&mut fx).await, Effect::Span { kind: SpanKind::SpanStart, .. }));
    claim(&mut fx, Activity::Active, "tool_call").await;
    claim(&mut fx, Activity::NeedsAttention, "permission").await;
    pending(&one).await;

    // Daemon two takes the socket: the holder drops daemon one, replays, and the pending
    // request is answered by the newcomer, R2.1 and R2.2.
    let stream = holder::connect_retry(&paths.socket, 40).await.unwrap();
    let (two, mut fx2) = Session::open("test:1:1".into(), saved, PermissionPolicy::Attention, true, stream, true);
    assert!(matches!(next(&mut fx).await, Effect::Lost));
    claim(&mut fx2, Activity::NeedsAttention, "permission").await;
    let id = pending(&two).await;
    two.answer(&id, PermissionAnswer::Selected { option_id: "allow".into() }).unwrap();
    claim(&mut fx2, Activity::Active, "permission_answered").await;
    assert!(matches!(next(&mut fx2).await, Effect::Span { kind: SpanKind::SpanEnd, .. }));
    claim(&mut fx2, Activity::Active, "message").await;
    assert!(matches!(next(&mut fx2).await, Effect::Usage(_)));
    // The prompt response carries daemon one's request id: the turn still ends here.
    assert!(matches!(next(&mut fx2).await, Effect::Recap(t) if t == "again"));
    claim(&mut fx2, Activity::Idle, "turn_end").await;

    // The stream carries only what the agent sent, R5.5.
    let mut raw = two.subscribe();
    two.cancel();
    claim(&mut fx2, Activity::Idle, "cancelled").await;
    assert!(raw.try_recv().is_err(), "cancel is outbound, not a notification from the agent");

    // Stop: SIGTERM to the holder, Exited frame, files gone, R5.6 and R2.1 item 6.
    holder::terminate(holder_pid);
    assert!(matches!(next(&mut fx2).await, Effect::Exited { signal: Some(15), .. }));
    let status = tokio::time::timeout(Duration::from_secs(5), reaper).await.unwrap().unwrap().unwrap();
    assert_eq!(status.code(), Some(143));
    assert!(!paths.socket.exists());
    assert!(!paths.state.exists());
    assert!(paths.log.exists(), "the log stays");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- R15 through the runner ---------------------------------------------------------------

/// The holder and the fake adapter, or None when neither can be had.
fn binaries() -> Option<(PathBuf, PathBuf)> {
    let bin = holder_bin()?;
    let fake = target_debug().join("examples").join("fake_acp");
    if !fake.exists() {
        eprintln!("skipped: examples/fake_acp not built");
        return None;
    }
    Some((bin, fake))
}

/// A short temp dir: a holder socket path must fit in `sockaddr_un` (104 bytes on macOS).
fn short_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("r15{tag}{}", std::process::id()))
}

/// A runner over a fresh roster, bridge and mesh in `dir`; `holders` is the holder directory,
/// shared between "daemon" instances in the reboot test.
fn node(dir: &Path, holders: &Path, bin: &Path, fake: &Path, tweak: impl FnOnce(&mut Config)) -> Arc<Runner> {
    std::fs::create_dir_all(dir).unwrap();
    // The bridge keeps its queue under the state directory; the runner reads the loopback
    // token under the config directory. Nothing else reads these in tests.
    static ENV: std::sync::Once = std::sync::Once::new();
    ENV.call_once(|| {
        let scratch = std::env::temp_dir().join(format!("rosterd-runner-env-{}", std::process::id()));
        std::fs::create_dir_all(&scratch).unwrap();
        unsafe {
            std::env::set_var("ROSTERD_STATE_DIR", &scratch);
            std::env::set_var("ROSTERD_CONFIG_DIR", &scratch);
        }
    });
    let mut config = Config::default();
    config.node.name = "test".into();
    config.workspace.credential_file = dir.join("no-such-token");
    config.runner.holder_dir = holders.to_path_buf();
    config.runner.holder_bin = Some(bin.to_path_buf());
    config.runner.default_permission_policy = PermissionPolicy::Auto;
    config.runner.resume_on_crash = false;
    config.harness.insert("fake".into(), HarnessConfig { adapter: fake.to_string_lossy().into_owned(), ..Default::default() });
    config.harness.insert("pi".into(), HarnessConfig { adapter: fake.to_string_lossy().into_owned(), extension: true, ..Default::default() });
    tweak(&mut config);
    let config = Arc::new(config);
    let identity = Identity::from_key(SigningKey::generate(&mut rand::rngs::OsRng));
    let roster = Roster::new("test", &identity.node_id, Capabilities::default());
    let bridge = Bridge::new(config.clone(), roster.clone()).unwrap();
    let mesh = Mesh::new_at(config.clone(), identity, roster.clone(), "0.1.0-test", dir, "127.0.0.1:1").unwrap();
    Runner::new(config, roster, bridge, mesh)
}

fn start(harness: &str, attempt: &str) -> StartSession {
    StartSession { harness: harness.into(), attempt_id: Some(attempt.into()), name: Some("r15".into()), ..Default::default() }
}

fn prompt(text: &str) -> PromptRequest {
    PromptRequest { prompt: text.into(), wait_until: None, timeout_ms: None }
}

/// The one state file under `holders`.
fn state_files(holders: &Path) -> Vec<HolderState> {
    holder::list_states(holders).into_iter().map(|(_, s)| s).collect()
}

async fn until(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..200 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn suspend_then_a_prompt_resumes_under_a_new_key_until_the_limit() {
    let Some((bin, fake)) = binaries() else { return };
    let dir = short_dir("a");
    let holders = dir.join("h");
    let runner = node(&dir, &holders, &bin, &fake, |c| c.runner.max_resumes_per_hour = 2);

    let mut req = start("fake", "att_r15");
    req.effort = Some("high".into());
    let record = runner.start(req).await.unwrap();
    let key = record.session_key.clone();
    assert_eq!(record.session_id.as_deref(), Some("fake-1"));
    let outcome = runner.prompt(&key, prompt("hello")).await.unwrap();
    assert_eq!(outcome.recap.as_deref(), Some("You said: hello"));
    assert_eq!(outcome.record.activity, Activity::Idle);
    // The effort option reached the adapter, which reports it back in its usage.
    assert_eq!(outcome.record.usage.unwrap().raw.unwrap()["effort"], "high");

    // R15.1 and R15.2 by hand.
    let suspended = runner.suspend(&key).await.unwrap();
    assert_eq!(suspended.liveness, Liveness::Suspended);
    assert!(suspended.holder.is_none());
    assert_eq!(suspended.activity, Activity::Idle, "activity kept");
    assert_eq!(suspended.session_id.as_deref(), Some("fake-1"));
    assert!(!runner.owns(&key));
    let files = state_files(&holders);
    assert_eq!(files.len(), 1, "the state file stays");
    assert!(files[0].suspended);
    assert_eq!(files[0].session_id.as_deref(), Some("fake-1"));
    assert!(!Path::new(&files[0].socket).exists(), "the socket went with the holder");
    let state = runner.state(&key).unwrap();
    assert_eq!(state.activity, Activity::Idle);
    assert!(state.pending.is_empty());
    assert_eq!(state.last_recap.as_deref(), Some("You said: hello"));
    assert!(matches!(runner.stream(&key), Err(RunnerError::Suspended(k)) if k == key));
    // Suspending twice is a no-op.
    assert_eq!(runner.suspend(&key).await.unwrap().liveness, Liveness::Suspended);

    // R15.3 item 1: the prompt resumes first; one call, a new key, the same attempt and id.
    let outcome = runner.prompt(&key, prompt("again")).await.unwrap();
    let key2 = outcome.record.session_key.clone();
    assert_ne!(key2, key);
    assert_eq!(outcome.record.attempt_id.as_deref(), Some("att_r15"));
    assert_eq!(outcome.record.session_id.as_deref(), Some("fake-1"));
    assert_eq!(outcome.record.liveness, Liveness::Live);
    assert_eq!(outcome.recap.as_deref(), Some("You said: again"));
    assert_eq!(outcome.record.usage.unwrap().raw.unwrap()["effort"], "high", "effort set again after the load");
    let old = runner.roster.get(&key).unwrap();
    assert_eq!(old.liveness, Liveness::Ended);
    assert_eq!(old.ended_reason, Some(EndedReason::Suspended));
    assert!(runner.owns(&key2));
    let files = state_files(&holders);
    assert_eq!(files.len(), 1, "the old state file went with the old key");
    assert!(!files[0].suspended);
    // Resuming a live key is the record itself; resuming the old key follows the attempt.
    assert_eq!(runner.resume(&key2).await.unwrap().session_key, key2);
    assert_eq!(runner.resume(&key).await.unwrap().session_key, key2);

    // The second resume is the last one this hour.
    runner.suspend(&key2).await.unwrap();
    let key3 = runner.resume(&key2).await.unwrap().session_key;
    assert_ne!(key3, key2);
    runner.suspend(&key3).await.unwrap();
    match runner.prompt(&key3, prompt("once more")).await {
        Err(RunnerError::ResumeLimit { session_key, retry_after_s }) => {
            assert_eq!(session_key, key3);
            assert!(retry_after_s > 0 && retry_after_s <= 3600);
        }
        other => panic!("expected the resume limit, got {other:?}"),
    }
    assert_eq!(runner.roster.get(&key3).unwrap().liveness, Liveness::Suspended);
    assert_eq!(state_files(&holders).len(), 1);

    // DELETE on a suspended session: no holder to stop, the session expires, the file goes.
    let ended = runner.stop(&key3).await.unwrap();
    assert_eq!(ended.liveness, Liveness::Ended);
    assert_eq!(ended.ended_reason, Some(EndedReason::Expired));
    assert!(state_files(&holders).is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn resume_on_prompt_off_refuses_and_a_ruling_reaches_the_resumed_session() {
    let Some((bin, fake)) = binaries() else { return };
    let dir = short_dir("b");
    let holders = dir.join("h");
    let runner = node(&dir, &holders, &bin, &fake, |c| c.runner.resume_on_prompt = false);
    let key = runner.start(start("fake", "att_ruling")).await.unwrap().session_key;
    runner.prompt(&key, prompt("hi")).await.unwrap();
    runner.suspend(&key).await.unwrap();
    assert!(matches!(runner.prompt(&key, prompt("no")).await, Err(RunnerError::Suspended(k)) if k == key));

    // R15.3 item 2: the ruling the session was waiting on arrives after it was suspended.
    // The fake has no permission pending after a load, so the ruling arrives as a prompt.
    let request = DecisionRequest {
        attempt_id: "att_ruling".into(),
        title: "Bash".into(),
        summary: "Bash {\"command\":\"ls\"}".into(),
        choices: vec![],
        default_choice_id: None,
        context: None,
    };
    runner.deliver(&key, &request, Ruling::Choice { id: "allow".into() }).await.unwrap();
    let current = runner.roster.live_by_attempt("att_ruling").unwrap();
    assert_ne!(current.session_key, key);
    assert_eq!(current.liveness, Liveness::Live);
    let state = runner.state(&current.session_key).unwrap();
    assert_eq!(state.last_recap.as_deref(), Some("You said: Ruling on Bash {\"command\":\"ls\"}: allow"));
    runner.stop(&current.session_key).await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn idle_timeout_suspends_and_a_reboot_restores_as_suspended() {
    let Some((bin, fake)) = binaries() else { return };
    let dir = short_dir("c");
    let holders = dir.join("h");
    let runner = node(&dir, &holders, &bin, &fake, |c| c.runner.idle_timeout_s = 3600);
    runner.set_idle_tick(Duration::from_millis(100));
    runner.recover().await.unwrap();

    // R15.2: the per session override wins; the sweep suspends within a few ticks of idle.
    let mut req = start("fake", "att_idle");
    req.idle_timeout_s = Some(1);
    let key = runner.start(req).await.unwrap().session_key;
    runner.prompt(&key, prompt("hello")).await.unwrap();
    let roster = runner.roster.clone();
    until("the idle sweep", || roster.get(&key).unwrap().liveness == Liveness::Suspended).await;
    assert!(!runner.owns(&key));
    // A session under the node timeout stays live.
    let key_live = runner.start(start("fake", "att_live")).await.unwrap().session_key;
    runner.prompt(&key_live, prompt("hello")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(roster.get(&key_live).unwrap().liveness, Liveness::Live);
    runner.suspend(&key_live).await.unwrap();

    // R15.4: a state file whose holder died before the last boot, not marked suspended.
    let boot = chrono::DateTime::from_timestamp(sysinfo::System::boot_time() as i64, 0).unwrap();
    let rebooted = Paths::new(&holders, "rebooted");
    holder::write_state(
        &rebooted.state,
        &HolderState {
            session_key: Some("nid:424242:7".into()),
            session_id: Some("fake-old".into()),
            harness: "fake".into(),
            cwd: dir.to_string_lossy().into_owned(),
            attempt_id: Some("att_rebooted".into()),
            parent_attempt_id: None,
            adapter_pid: 424242,
            holder_pid: 424241,
            started_at: boot - chrono::Duration::hours(1),
            socket: rebooted.socket.to_string_lossy().into_owned(),
            meta: HashMap::from([("name".to_string(), json!("old"))]),
            suspended: false,
        },
    )
    .unwrap();
    std::fs::write(&rebooted.socket, b"").unwrap();

    // The next daemon on the same holder directory: every file comes back suspended.
    let two = node(&dir.join("2"), &holders, &bin, &fake, |_| {});
    two.recover().await.unwrap();
    let records = two.roster.snapshot().records.clone();
    assert_eq!(records.len(), 3, "{records:#?}");
    for record in &records {
        assert_eq!(record.liveness, Liveness::Suspended, "{record:#?}");
        assert!(record.holder.is_none());
        assert!(!two.owns(&record.session_key));
    }
    let restored = two.roster.get("nid:424242:7").unwrap();
    assert_eq!(restored.session_id.as_deref(), Some("fake-old"));
    assert_eq!(restored.name.as_deref(), Some("old"));
    assert_eq!(restored.pid, 424242);
    assert!(!rebooted.socket.exists());
    assert!(holder::read_state(&rebooted.state).unwrap().suspended, "marked for the next restart");
    assert!(state_files(&holders).iter().all(|s| s.suspended));
    assert_eq!(two.state(&key).unwrap().activity, Activity::Unknown, "activity is not kept across a reboot");

    // Nothing resumes on its own; addressing one does.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(two.roster.snapshot().records.iter().all(|r| r.liveness == Liveness::Suspended));
    let back = two.resume(&key).await.unwrap();
    assert_eq!(back.liveness, Liveness::Live);
    assert_eq!(back.attempt_id.as_deref(), Some("att_idle"));
    assert_eq!(back.session_id.as_deref(), Some("fake-1"));
    assert_eq!(back.activity, Activity::Idle, "a loaded session starts idle");
    assert_eq!(two.roster.get(&key).unwrap().ended_reason, Some(EndedReason::Suspended));
    two.stop(&back.session_key).await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn pi_without_the_extension_warns_and_runs_as_auto() {
    let Some((bin, fake)) = binaries() else { return };
    let dir = short_dir("d");
    if crate::integrate::pi_extension_installed() {
        eprintln!("skipped: the pi extension is installed here");
        return;
    }
    let runner = node(&dir, &dir.join("h"), &bin, &fake, |_| {});
    let mut req = start("pi", "att_pi");
    req.permission_policy = Some(PermissionPolicy::Attention);
    let warnings = runner.start_warnings(&req);
    assert_eq!(warnings, ["pi has no permission requests without the rosterd-pi extension; policy attention runs as auto"]);
    req.permission_policy = Some(PermissionPolicy::Auto);
    assert!(runner.start_warnings(&req).is_empty());
    let mut fake_req = start("fake", "att_fake");
    fake_req.permission_policy = Some(PermissionPolicy::Decision);
    assert!(runner.start_warnings(&fake_req).is_empty());

    // R16.1: the fake's permission request is auto-answered, the turn completes on its own.
    req.permission_policy = Some(PermissionPolicy::Attention);
    let record = runner.start(req).await.unwrap();
    assert_eq!(record.permission_policy, Some(PermissionPolicy::Auto));
    let outcome = tokio::time::timeout(Duration::from_secs(10), runner.prompt(&record.session_key, prompt("go"))).await.unwrap().unwrap();
    assert_eq!(outcome.recap.as_deref(), Some("You said: go"));
    assert!(runner.state(&record.session_key).unwrap().pending.is_empty());
    runner.stop(&record.session_key).await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
