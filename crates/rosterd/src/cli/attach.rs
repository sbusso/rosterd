//! `rosterd attach KEY`, R9: this terminal spliced with the session's over the local socket's
//! websocket. Raw mode while attached, the window size follows SIGWINCH, and the exit code is
//! the tmux client's.
use std::path::Path;

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

pub async fn attach(socket: &Path, key: &str) -> anyhow::Result<()> {
    let (cols, rows) = term_size().unwrap_or((80, 24));
    let stream = tokio::net::UnixStream::connect(socket).await.with_context(|| format!("connect {}", socket.display()))?;
    let url = format!("ws://rosterd/sessions/{key}/attach?cols={cols}&rows={rows}");
    let (ws, _) = tokio_tungstenite::client_async(url, stream).await.context("attach")?;
    let raw = RawMode::enable();
    let (mut sink, mut source) = ws.split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut winch = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())?;
    let mut buf = [0u8; 4096];
    let mut code = 0;
    let mut failure = None;
    loop {
        tokio::select! {
            read = stdin.read(&mut buf) => match read {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if sink.send(Message::Binary(buf[..n].to_vec().into())).await.is_err() {
                        break;
                    }
                }
            },
            message = source.next() => match message {
                Some(Ok(Message::Binary(bytes))) => {
                    stdout.write_all(&bytes).await?;
                    stdout.flush().await?;
                }
                Some(Ok(Message::Text(text))) => {
                    let value: Value = serde_json::from_str(text.as_str()).unwrap_or(Value::Null);
                    if let Some(exit) = value["exit"]["code"].as_u64() {
                        code = exit as i32;
                        break;
                    }
                    if let Some(error) = value["error"].as_str() {
                        failure = Some(error.to_string());
                        break;
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(error)) => {
                    failure = Some(error.to_string());
                    break;
                }
                _ => {}
            },
            _ = winch.recv() => {
                if let Some((cols, rows)) = term_size() {
                    let _ = sink.send(Message::Text(json!({ "resize": { "cols": cols, "rows": rows } }).to_string().into())).await;
                }
            }
        }
    }
    drop(raw);
    if let Some(failure) = failure {
        anyhow::bail!("{failure}");
    }
    std::process::exit(code)
}

fn term_size() -> Option<(u16, u16)> {
    let mut size: libc::winsize = unsafe { std::mem::zeroed() };
    // SAFETY: TIOCGWINSZ fills a winsize; a failure leaves it zeroed and is reported.
    let ok = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size) } == 0;
    (ok && size.ws_col > 0 && size.ws_row > 0).then_some((size.ws_col, size.ws_row))
}

/// The terminal in raw mode for the attach, restored on drop; nothing when stdin is not a tty.
struct RawMode(Option<libc::termios>);

impl RawMode {
    fn enable() -> Self {
        let mut termios: libc::termios = unsafe { std::mem::zeroed() };
        // SAFETY: tcgetattr/tcsetattr on stdin with a termios this function owns.
        unsafe {
            if libc::isatty(libc::STDIN_FILENO) == 0 || libc::tcgetattr(libc::STDIN_FILENO, &mut termios) != 0 {
                return RawMode(None);
            }
            let saved = termios;
            libc::cfmakeraw(&mut termios);
            if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &termios) != 0 {
                return RawMode(None);
            }
            RawMode(Some(saved))
        }
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        if let Some(saved) = self.0 {
            // SAFETY: restores the termios captured by `enable`.
            unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &saved) };
        }
    }
}
