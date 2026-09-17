//! R9 attach: the session's terminal over a websocket. Binary frames are pty bytes both ways;
//! a text frame from the client is `{"resize":{"cols":N,"rows":N}}`, from the server
//! `{"exit":{"code":N}}` or `{"error":"..."}` before the close. The owner node runs
//! `tmux attach` in a pty; any other node relays to the owner over the mesh, so the page and
//! `rosterd attach` never know where the pane is.
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use rosterd_proto::TmuxHandle;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;

use crate::mesh::PeerWebSocket;

#[derive(Deserialize)]
pub struct Size {
    #[serde(default = "default_cols")]
    pub cols: u16,
    #[serde(default = "default_rows")]
    pub rows: u16,
}

fn default_cols() -> u16 {
    80
}

fn default_rows() -> u16 {
    24
}

fn pty_size(cols: u16, rows: u16) -> PtySize {
    PtySize { rows: rows.max(2), cols: cols.max(2), pixel_width: 0, pixel_height: 0 }
}

async fn fail(mut socket: WebSocket, error: impl std::fmt::Display) {
    let _ = socket.send(Message::Text(json!({ "error": error.to_string() }).to_string().into())).await;
    let _ = socket.close().await;
}

/// The owner side: `tmux attach` to the pane in a pty, spliced with the socket. The socket
/// closing ends the attach; the tmux session stays.
pub(super) async fn serve(socket: WebSocket, tmux: TmuxHandle, size: Size) {
    let pair = match native_pty_system().openpty(pty_size(size.cols, size.rows)) {
        Ok(pair) => pair,
        Err(error) => return fail(socket, error).await,
    };
    let mut command = CommandBuilder::new("tmux");
    let window = format!("{}:{}", tmux.session, tmux.window_index);
    command.args(["attach", "-t", &tmux.session, ";", "select-window", "-t", &window, ";", "select-pane", "-t", &tmux.pane_id]);
    command.env("TERM", "xterm-256color");
    let mut child = match pair.slave.spawn_command(command) {
        Ok(child) => child,
        Err(error) => return fail(socket, error).await,
    };
    drop(pair.slave);
    let (mut reader, mut writer) = match (pair.master.try_clone_reader(), pair.master.take_writer()) {
        (Ok(reader), Ok(writer)) => (reader, writer),
        (Err(error), _) | (_, Err(error)) => return fail(socket, error).await,
    };
    let master = Arc::new(Mutex::new(pair.master));

    // The pty is blocking I/O: one thread reads it, one writes it, one waits for the child.
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(64);
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if out_tx.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });
    let (in_tx, in_rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        for bytes in in_rx {
            if writer.write_all(&bytes).and_then(|_| writer.flush()).is_err() {
                break;
            }
        }
    });
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<u32>();
    std::thread::spawn(move || {
        let code = child.wait().map(|status| status.exit_code()).unwrap_or(1);
        let _ = exit_tx.send(code);
    });

    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            out = out_rx.recv() => match out {
                Some(bytes) => {
                    if sink.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                // The pty closed: the tmux client exited (detach, kill-session, or the session ended).
                None => {
                    let code = exit_rx.await.unwrap_or(1);
                    let _ = sink.send(Message::Text(json!({ "exit": { "code": code } }).to_string().into())).await;
                    let _ = sink.close().await;
                    return;
                }
            },
            message = stream.next() => match message {
                Some(Ok(Message::Binary(bytes))) => {
                    if in_tx.send(bytes.to_vec()).is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Text(text))) => {
                    if let Some(resize) = serde_json::from_str::<Value>(text.as_str()).ok().and_then(|v| v.get("resize").cloned())
                        && let (Some(cols), Some(rows)) = (resize["cols"].as_u64(), resize["rows"].as_u64())
                    {
                        let _ = master.lock().unwrap_or_else(|e| e.into_inner()).resize(pty_size(cols as u16, rows as u16));
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => {}
            },
        }
    }
    // The client went: dropping the master hangs up the pty and the tmux client exits.
}

/// Any other node: the socket spliced with the owner's, frame for frame.
pub(super) async fn relay(socket: WebSocket, upstream: PeerWebSocket) {
    let (mut down_sink, mut down_stream) = socket.split();
    let (mut up_sink, mut up_stream) = upstream.split();
    let to_up = async {
        while let Some(Ok(message)) = down_stream.next().await {
            let message = match message {
                Message::Binary(bytes) => tungstenite::Message::Binary(bytes),
                Message::Text(text) => tungstenite::Message::Text(text.as_str().into()),
                Message::Close(_) => break,
                _ => continue,
            };
            if up_sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = up_sink.close().await;
    };
    let to_down = async {
        while let Some(Ok(message)) = up_stream.next().await {
            let message = match message {
                tungstenite::Message::Binary(bytes) => Message::Binary(bytes),
                tungstenite::Message::Text(text) => Message::Text(text.as_str().into()),
                tungstenite::Message::Close(_) => break,
                _ => continue,
            };
            if down_sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = down_sink.close().await;
    };
    tokio::select! {
        _ = to_up => {}
        _ = to_down => {}
    }
}
