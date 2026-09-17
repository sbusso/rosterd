//! HTTP/1 over the daemon's Unix socket, R6, and the CLI's failure type with its exit code,
//! R14.2. Bodies come back as received so `--json` never re-serializes.

use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::config::Config;

const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A failed command: the exit code and the line printed on stderr.
#[derive(Debug)]
pub struct Exit {
    pub code: u8,
    pub message: String,
}

pub type Out<T> = Result<T, Exit>;

impl Exit {
    pub fn user(message: impl Into<String>) -> Self {
        Exit { code: 1, message: message.into() }
    }
    fn down(message: impl Into<String>) -> Self {
        Exit { code: 2, message: message.into() }
    }
    fn peer(message: impl Into<String>) -> Self {
        Exit { code: 3, message: message.into() }
    }
}

impl From<anyhow::Error> for Exit {
    fn from(error: anyhow::Error) -> Self {
        Exit::user(format!("{error:#}"))
    }
}

impl From<serde_json::Error> for Exit {
    fn from(error: serde_json::Error) -> Self {
        Exit::user(format!("unexpected answer: {error}"))
    }
}

impl From<std::io::Error> for Exit {
    fn from(error: std::io::Error) -> Self {
        Exit::user(error.to_string())
    }
}

pub struct Client {
    socket: PathBuf,
    http: reqwest::Client,
}

impl Client {
    pub fn new(config: &Config) -> Self {
        let socket = socket_path(config);
        // No client wide timeout: `call` sets one per request, a stream runs until Ctrl+C.
        let http = reqwest::Client::builder().unix_socket(socket.clone()).build().expect("reqwest client");
        Client { socket, http }
    }

    /// One request with the default timeout; the body as received.
    pub async fn call(&self, method: Method, path: &str, body: Option<Value>) -> Out<String> {
        self.call_with(method, path, body, CALL_TIMEOUT).await
    }

    /// A non-2xx answer becomes an `Exit` carrying its `{error}` text: 502 is the owner node
    /// unreachable behind the proxy (exit 3), everything else a user error (exit 1). A socket
    /// that does not answer is exit 2.
    pub async fn call_with(&self, method: Method, path: &str, body: Option<Value>, timeout: Duration) -> Out<String> {
        let mut request = self.http.request(method, format!("http://rosterd{path}")).timeout(timeout);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|error| self.transport(error))?;
        let status = response.status();
        let text = response.text().await.map_err(|error| self.transport(error))?;
        if status.is_success() {
            return Ok(text);
        }
        Err(api_error(status, &text))
    }

    /// GET an SSE stream and hand every `data:` payload to `on_frame` until it says stop.
    pub async fn events(&self, path: &str, mut on_frame: impl FnMut(&str) -> Out<bool>) -> Out<()> {
        let response = self.http.get(format!("http://rosterd{path}")).send().await.map_err(|error| self.transport(error))?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(api_error(status, &text));
        }
        let mut stream = response.bytes_stream();
        let mut buffer: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            buffer.extend_from_slice(&chunk.map_err(|error| self.transport(error))?);
            while let Some(end) = buffer.windows(2).position(|w| w == b"\n\n") {
                let event: Vec<u8> = buffer.drain(..end + 2).collect();
                let data = sse_data(&String::from_utf8_lossy(&event));
                if !data.is_empty() && !on_frame(&data)? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn transport(&self, error: reqwest::Error) -> Exit {
        if error.is_timeout() {
            return Exit::user(format!("timed out waiting for rosterd: {error}"));
        }
        Exit::down(format!("rosterd is not answering on {}; is it running? ({error})", self.socket.display()))
    }
}

fn api_error(status: StatusCode, text: &str) -> Exit {
    let message = match serde_json::from_str::<Value>(text).ok().and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string)) {
        Some(error) => error,
        None => format!("{status}: {text}"),
    };
    if status == StatusCode::BAD_GATEWAY { Exit::peer(message) } else { Exit::user(message) }
}

/// The `data:` lines of one SSE event, joined with newlines as the spec says.
fn sse_data(event: &str) -> String {
    event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|data| data.strip_prefix(' ').unwrap_or(data))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The daemon's socket as the daemon itself resolves it: `node.socket` or the platform default
/// of R6, which `ROSTERD_SOCKET` overrides. A daemon under launchd or the system unit has no
/// shell TMPDIR or XDG_RUNTIME_DIR, R11; the scripts probe the same two places.
pub(super) fn socket_path(config: &Config) -> PathBuf {
    let configured = config.socket_path();
    if configured.exists() {
        return configured;
    }
    ["/tmp/rosterd.sock", "/run/rosterd/rosterd.sock"].into_iter().map(PathBuf::from).find(|p| p.exists()).unwrap_or(configured)
}

pub fn parse<T: DeserializeOwned>(text: &str) -> Out<T> {
    Ok(serde_json::from_str(text)?)
}

/// `--json`: the body as received. The newline is for a terminal only, so a pipe or a file gets
/// the API's bytes and nothing else, A14 acceptance 5.
pub fn emit(body: &str) -> Out<()> {
    let mut out = std::io::stdout().lock();
    out.write_all(body.as_bytes())?;
    if out.is_terminal() {
        out.write_all(b"\n")?;
    }
    Ok(out.flush()?)
}

pub fn stdout_is_tty() -> bool {
    std::io::stdout().is_terminal()
}

/// Columns padded to their widest cell, two spaces apart; a dimmed row is ANSI dim on a tty.
pub fn table(header: &[&str], rows: &[(Vec<String>, bool)]) -> String {
    let dim = stdout_is_tty();
    let mut widths: Vec<usize> = header.iter().map(|h| h.chars().count()).collect();
    for (cells, _) in rows {
        for (i, cell) in cells.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let line = |cells: &[String]| {
        cells.iter().enumerate().map(|(i, c)| format!("{c:<width$}", width = widths[i])).collect::<Vec<_>>().join("  ").trim_end().to_string()
    };
    let mut out = line(&header.iter().map(|h| h.to_string()).collect::<Vec<_>>()) + "\n";
    for (cells, dimmed) in rows {
        let text = line(cells);
        out += &if *dimmed && dim { format!("\x1b[2m{text}\x1b[0m\n") } else { text + "\n" };
    }
    out
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::json;
    use tokio::net::UnixListener;

    use super::*;

    #[tokio::test]
    async fn exit_codes_follow_the_transport_and_status() {
        let dir = std::env::temp_dir().join(format!("rosterd-cli-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("rosterd.sock");
        let app = Router::new()
            .route("/snapshot", get(|| async { Json(json!({ "schema": "rosterd.snapshot.v1", "records": [] })) }))
            .route("/node/revoke", post(|| async { (StatusCode::NOT_FOUND, Json(json!({ "error": "unknown node deadbeef" }))) }))
            .route("/sessions/x/prompt", post(|| async { (StatusCode::BAD_GATEWAY, Json(json!({ "error": "node w unreachable" }))) }));
        let listener = UnixListener::bind(&socket).unwrap();
        tokio::spawn(axum::serve(listener, app).into_future());
        let mut config = Config::default();
        config.node.socket = Some(socket);
        let client = Client::new(&config);

        let snap = client.call(Method::GET, "/snapshot", None).await.unwrap();
        assert_eq!(snap, json!({ "schema": "rosterd.snapshot.v1", "records": [] }).to_string(), "the body as received");

        let err = client.call(Method::POST, "/node/revoke", Some(json!({ "node_id": "deadbeef" }))).await.unwrap_err();
        assert_eq!((err.code, err.message.as_str()), (1, "unknown node deadbeef"));

        let peer = client.call(Method::POST, "/sessions/x/prompt", Some(json!({}))).await.unwrap_err();
        assert_eq!((peer.code, peer.message.as_str()), (3, "node w unreachable"));

        // A plain file, not a socket: the connect fails without probing the platform paths.
        std::fs::write(dir.join("nobody.sock"), b"").unwrap();
        config.node.socket = Some(dir.join("nobody.sock"));
        let down = Client::new(&config).call(Method::GET, "/snapshot", None).await.unwrap_err();
        assert_eq!(down.code, 2);
        assert!(down.message.contains("is it running"), "{}", down.message);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sse_data_joins_data_lines() {
        assert_eq!(sse_data("event: x\ndata: {\"a\":1}\n\n"), "{\"a\":1}");
        assert_eq!(sse_data("data:one\ndata: two\n"), "one\ntwo");
        assert_eq!(sse_data(": keep-alive\n"), "");
    }
}
