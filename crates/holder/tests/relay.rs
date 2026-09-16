//! R2.1 end to end: the holder around `cat`. Connect, relay a line, reconnect and receive the
//! replay, update the state file, SIGTERM, files gone.

use std::path::PathBuf;
use std::time::Duration;

use rosterd_proto::{HolderFrame, HolderState};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

async fn connect(socket: &PathBuf) -> (tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>, tokio::net::unix::OwnedWriteHalf) {
    let stream = UnixStream::connect(socket).await.expect("connect");
    let (r, w) = stream.into_split();
    (BufReader::new(r).lines(), w)
}

async fn next(lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>) -> Value {
    let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line()).await.expect("timely").expect("read").expect("a line");
    serde_json::from_str(&line).expect("json")
}

#[tokio::test]
async fn relays_replays_and_cleans_up() {
    let dir = std::env::temp_dir().join(format!("rosterd-holder-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("s.sock");
    let state = dir.join("s.json");
    let mut holder = tokio::process::Command::new(env!("CARGO_BIN_EXE_rosterd-holder"))
        .args(["--socket", socket.to_str().unwrap(), "--state", state.to_str().unwrap()])
        .args(["--harness", "cat", "--cwd", dir.to_str().unwrap(), "--attempt-id", "att_1"])
        .args(["--meta", r#"{"name":"echo"}"#, "--", "cat"])
        .spawn()
        .expect("spawn holder");

    // State file before the socket, R2.1 item 5.
    let mut written = None;
    for _ in 0..100 {
        if let Ok(s) = std::fs::read_to_string(&state).map_err(drop).and_then(|t| serde_json::from_str::<HolderState>(&t).map_err(drop)) {
            written = Some(s);
            if socket.exists() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let written = written.expect("state file");
    assert_eq!(written.harness, "cat");
    assert_eq!(written.attempt_id.as_deref(), Some("att_1"));
    assert_eq!(written.meta["name"], "echo");
    assert_eq!(written.holder_pid, holder.id().unwrap());
    assert_ne!(written.adapter_pid, 0);

    // First connection: an empty replay, then the echo.
    let (mut lines, mut w) = connect(&socket).await;
    let replay: HolderFrame = serde_json::from_value(next(&mut lines).await).unwrap();
    assert_eq!(replay, HolderFrame::Replay { frames: vec![] });
    let notification = json!({"jsonrpc": "2.0", "method": "session/update", "params": {"n": 1}});
    let request = json!({"jsonrpc": "2.0", "id": 7, "method": "session/request_permission", "params": {}});
    let response = json!({"jsonrpc": "2.0", "id": 3, "result": {}});
    for m in [&notification, &request, &response] {
        w.write_all(format!("{m}\n").as_bytes()).await.unwrap();
    }
    assert_eq!(next(&mut lines).await, notification);
    assert_eq!(next(&mut lines).await, request);
    assert_eq!(next(&mut lines).await, response);

    // SetState updates the file; the frame never reaches the child.
    let mut updated = written.clone();
    updated.session_key = Some("node:1:2".into());
    w.write_all(format!("{}\n", serde_json::to_string(&HolderFrame::SetState { state: updated }).unwrap()).as_bytes()).await.unwrap();
    let mut seen = false;
    for _ in 0..100 {
        let current = serde_json::from_str::<HolderState>(&std::fs::read_to_string(&state).unwrap_or_default());
        if current.is_ok_and(|s| s.session_key.as_deref() == Some("node:1:2")) {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(seen, "state file updated");
    drop((lines, w));

    // Second connection: the replay holds the notification and the request, not the response.
    // Answering the request prunes it.
    let (mut lines, mut w) = connect(&socket).await;
    let replay: HolderFrame = serde_json::from_value(next(&mut lines).await).unwrap();
    assert_eq!(replay, HolderFrame::Replay { frames: vec![notification.clone(), request.clone()] });
    let answer = json!({"jsonrpc": "2.0", "id": 7, "result": {"outcome": {"outcome": "cancelled"}}});
    w.write_all(format!("{answer}\n").as_bytes()).await.unwrap();
    assert_eq!(next(&mut lines).await, answer);
    drop((lines, w));
    let (mut lines, _w) = connect(&socket).await;
    let replay: HolderFrame = serde_json::from_value(next(&mut lines).await).unwrap();
    assert_eq!(replay, HolderFrame::Replay { frames: vec![notification.clone()] });

    // SIGTERM: forwarded to cat, Exited frame, files gone, exit status carries the signal.
    unsafe { libc::kill(holder.id().unwrap() as i32, libc::SIGTERM) };
    let exited: HolderFrame = serde_json::from_value(next(&mut lines).await).unwrap();
    assert_eq!(exited, HolderFrame::Exited { code: None, signal: Some(15) });
    let status = tokio::time::timeout(Duration::from_secs(5), holder.wait()).await.unwrap().unwrap();
    assert_eq!(status.code(), Some(143));
    assert!(!socket.exists(), "socket removed");
    assert!(!state.exists(), "state file removed");
    let _ = std::fs::remove_dir_all(&dir);
}
