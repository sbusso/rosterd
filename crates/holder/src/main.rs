//! rosterd-holder, R2.1: owns the stdio pipes of one ACP adapter, relays JSON-RPC unchanged
//! between a per-session Unix socket and the child, keeps the last 256 notifications for a
//! reconnecting daemon, writes a state file next to the socket, and exits with the child.
//!
//! Unix sockets only; Windows named pipes are a later target.
//!
//! OWNER: the holder/runner agent.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use rosterd_proto::{HOLDER_REPLAY_BUFFER, HolderFrame, HolderState};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Parser, Debug)]
#[command(name = "rosterd-holder", version, about)]
struct Cli {
    /// Unix socket the daemon connects to.
    #[arg(long)]
    socket: PathBuf,
    /// State file written before listening, R2.1.
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    harness: String,
    #[arg(long)]
    cwd: PathBuf,
    /// JSON object of extra launch facts the daemon wants back after a restart.
    #[arg(long, default_value = "{}")]
    meta: String,
    /// The ACP adapter command and its arguments, after `--`.
    #[arg(last = true, required = true)]
    adapter: Vec<String>,
}

struct Holder {
    socket_path: PathBuf,
    state_path: PathBuf,
    state: Mutex<HolderState>,
    /// Child to daemon notifications and child-initiated requests, R2.1 item 4.
    replay: Mutex<VecDeque<Value>>,
    /// The current daemon connection: its outbound lines and its task. Replaced on connect.
    daemon: Mutex<Option<(mpsc::UnboundedSender<String>, JoinHandle<()>)>>,
    /// Lines to the child's stdin.
    child_in: mpsc::UnboundedSender<String>,
}

impl Holder {
    /// A line the child wrote: buffer it if the daemon may need it later, forward it if the
    /// daemon is connected.
    fn child_wrote(&self, line: String) {
        if let Ok(v) = serde_json::from_str::<Value>(&line) {
            let is_request_or_notification = v.get("method").is_some();
            if is_request_or_notification {
                let mut replay = self.replay.lock().unwrap();
                if replay.len() >= HOLDER_REPLAY_BUFFER {
                    replay.pop_front();
                }
                replay.push_back(v);
            }
        }
        if let Some((tx, _)) = self.daemon.lock().unwrap().as_ref() {
            let _ = tx.send(line);
        }
    }

    /// A line the daemon wrote: control frames stay here, everything else goes to the child.
    fn daemon_wrote(&self, line: String) {
        if let Ok(v) = serde_json::from_str::<Value>(&line) {
            if v.get("rosterd").is_some() {
                match serde_json::from_value::<HolderFrame>(v) {
                    Ok(HolderFrame::SetState { state }) => {
                        if let Err(error) = self.set_state(state) {
                            tracing::warn!(%error, "state file write failed");
                        }
                    }
                    Ok(other) => tracing::warn!(?other, "unexpected control frame from the daemon"),
                    Err(error) => tracing::warn!(%error, "bad control frame from the daemon"),
                }
                return;
            }
            // A response answers a child request: it need not be replayed again.
            if let (Some(id), None) = (v.get("id"), v.get("method")) {
                self.replay.lock().unwrap().retain(|f| f.get("id") != Some(id));
            }
        }
        let _ = self.child_in.send(line);
    }

    fn set_state(&self, state: HolderState) -> Result<()> {
        write_state(&self.state_path, &state)?;
        *self.state.lock().unwrap() = state;
        Ok(())
    }

    /// One daemon connection at a time: the replay frame first, then live traffic both ways.
    fn attach<S>(self: &Arc<Self>, stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let frames = self.replay.lock().unwrap().iter().cloned().collect();
        let replay = serde_json::to_string(&HolderFrame::Replay { frames }).expect("replay frame");
        let _ = tx.send(replay);

        let holder = self.clone();
        let task = tokio::spawn(async move {
            let (reader, mut writer) = tokio::io::split(stream);
            let mut lines = BufReader::new(reader).lines();
            loop {
                tokio::select! {
                    line = lines.next_line() => match line {
                        Ok(Some(line)) => holder.daemon_wrote(line),
                        _ => return,
                    },
                    out = rx.recv() => match out {
                        Some(line) => {
                            if writer.write_all(line.as_bytes()).await.is_err()
                                || writer.write_all(b"\n").await.is_err()
                            {
                                return;
                            }
                        }
                        None => return,
                    },
                }
            }
        });
        if let Some((_, old)) = self.daemon.lock().unwrap().replace((tx, task)) {
            old.abort();
        }
    }

    /// The child exited: tell the daemon, flush, then remove the files, R2.1 item 6.
    async fn exited(&self, code: Option<i32>, signal: Option<i32>) {
        let current = self.daemon.lock().unwrap().take();
        if let Some((tx, task)) = current {
            let frame = serde_json::to_string(&HolderFrame::Exited { code, signal }).expect("exited frame");
            let _ = tx.send(frame);
            drop(tx);
            let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
        }
        let _ = std::fs::remove_file(&self.socket_path);
        // R15.1: a suspended session keeps its state file; the daemon resumes from it.
        if !self.state.lock().unwrap().suspended {
            let _ = std::fs::remove_file(&self.state_path);
        }
    }
}

fn write_state(path: &Path, state: &HolderState) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(state)?).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename to {}", path.display()))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let meta: HashMap<String, Value> = serde_json::from_str(&cli.meta).context("--meta must be a JSON object")?;

    // R2.1 item 1: the adapter as a child with stdio pipes; stderr stays ours for the log.
    let mut child = Command::new(&cli.adapter[0])
        .args(&cli.adapter[1..])
        .current_dir(&cli.cwd)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .with_context(|| format!("spawn {}", cli.adapter[0]))?;
    let adapter_pid = child.id().context("adapter exited before it had a pid")?;
    let stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");

    // R2.1 item 5: the state file, before anyone can connect.
    let state = HolderState {
        session_key: None,
        session_id: None,
        harness: cli.harness,
        cwd: cli.cwd.to_string_lossy().into_owned(),
        adapter_pid,
        holder_pid: std::process::id(),
        started_at: chrono::Utc::now(),
        socket: cli.socket.to_string_lossy().into_owned(),
        meta,
        suspended: false,
    };
    write_state(&cli.state, &state)?;

    // R2.1 item 2: the per-session socket, 0600.
    let _ = std::fs::remove_file(&cli.socket);
    let listener = UnixListener::bind(&cli.socket).with_context(|| format!("bind {}", cli.socket.display()))?;
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&cli.socket, std::fs::Permissions::from_mode(0o600));
    }

    let (child_in, mut child_in_rx) = mpsc::unbounded_channel::<String>();
    let holder = Arc::new(Holder {
        socket_path: cli.socket,
        state_path: cli.state,
        state: Mutex::new(state),
        replay: Mutex::new(VecDeque::with_capacity(HOLDER_REPLAY_BUFFER)),
        daemon: Mutex::new(None),
        child_in,
    });

    // R2.1 item 3: relay both ways, unchanged.
    tokio::spawn(async move {
        let mut stdin = stdin;
        while let Some(line) = child_in_rx.recv().await {
            if stdin.write_all(line.as_bytes()).await.is_err() || stdin.write_all(b"\n").await.is_err() {
                return;
            }
        }
    });
    tokio::spawn({
        let holder = holder.clone();
        async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                holder.child_wrote(line);
            }
        }
    });
    tokio::spawn({
        let holder = holder.clone();
        async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => holder.attach(stream),
                    Err(error) => {
                        tracing::warn!(%error, "accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    });
    // SIGTERM or SIGINT: forward to the child, escalate after 5 s; the exit path below cleans up.
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! { _ = term.recv() => {}, _ = int.recv() => {} }
        tracing::info!(adapter_pid, "signalled; forwarding SIGTERM to the adapter");
        unsafe { libc::kill(adapter_pid as libc::pid_t, libc::SIGTERM) };
        tokio::time::sleep(Duration::from_secs(5)).await;
        unsafe { libc::kill(adapter_pid as libc::pid_t, libc::SIGKILL) };
    });

    // R2.1 item 6: exit with the child.
    let status = child.wait().await.context("wait for the adapter")?;
    let (code, signal) = {
        use std::os::unix::process::ExitStatusExt;
        (status.code(), status.signal())
    };
    tracing::info!(?code, ?signal, "adapter exited");
    holder.exited(code, signal).await;
    std::process::exit(code.unwrap_or_else(|| 128 + signal.unwrap_or(1)));
}
