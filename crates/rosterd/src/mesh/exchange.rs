//! Snapshot exchange, R7.4: one long lived signed GET /events per peer, each `data:` line a
//! complete snapshot, reconnect with backoff 1 s to 30 s. Reachable means the last connect
//! succeeded; the snapshot outlives the connection and is dropped after 24 h unreachable.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use axum::http::Method;
use futures::StreamExt;
use rosterd_proto::Snapshot;

use super::Mesh;
use super::wire::signed_headers;

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Silence longer than this ends the stream and reconnects without touching reachability.
// ponytail: a peer whose /events sends no keepalive reconnects every 90 s; add a `: ping` on
// the api side if the reconnects show up in logs.
const IDLE: Duration = Duration::from_secs(90);

/// Incremental server-sent-events reader: feed chunks, get the `data` of each complete event.
#[derive(Default)]
pub struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    pub fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        self.buf.extend(chunk.iter().filter(|b| **b != b'\r'));
        let mut out = Vec::new();
        while let Some(end) = self.buf.windows(2).position(|w| w == b"\n\n") {
            let event = String::from_utf8_lossy(&self.buf[..end]).into_owned();
            self.buf.drain(..end + 2);
            let data: Vec<&str> = event
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(|line| line.strip_prefix(' ').unwrap_or(line))
                .collect();
            if !data.is_empty() {
                out.push(data.join("\n"));
            }
        }
        out
    }
}

/// Runs until aborted by the supervisor in `Mesh::run`.
pub async fn subscribe(mesh: Arc<Mesh>, node_id: String) {
    let mut backoff = BACKOFF_MIN;
    loop {
        match stream_once(&mesh, &node_id).await {
            Ok(()) => {
                backoff = BACKOFF_MIN;
                tokio::time::sleep(BACKOFF_MIN).await;
            }
            Err(error) => {
                tracing::debug!(peer = %node_id, %error, "events stream down");
                mesh.set_reachable(&node_id, false);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

/// One connection: Ok when the peer answered and the stream later ended or went idle, Err
/// when it could not be reached at all.
async fn stream_once(mesh: &Mesh, node_id: &str) -> Result<()> {
    let address = mesh.address_of(node_id).context("no address")?;
    let swarm_key = mesh.swarm_key().context("not in a swarm")?;
    let headers = signed_headers(mesh.identity(), &swarm_key, &Method::GET, "/events", b"");
    let response = mesh.client().get(format!("https://{address}/events")).headers(headers).send().await?;
    ensure!(response.status().is_success(), "events answered {}", response.status());
    mesh.set_reachable(node_id, true);

    let mut stream = response.bytes_stream();
    let mut parser = SseParser::default();
    while let Ok(Some(chunk)) = tokio::time::timeout(IDLE, stream.next()).await {
        for data in parser.push(&chunk?) {
            match serde_json::from_str::<Snapshot>(&data) {
                Ok(snapshot) if snapshot.node_id == node_id => mesh.store_snapshot(snapshot),
                Ok(snapshot) => tracing::warn!(peer = %node_id, claimed = %snapshot.node_id, "snapshot for another node dropped"),
                Err(error) => tracing::debug!(peer = %node_id, %error, "unparsable event"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_parser_yields_data_of_complete_events_only() {
        let mut parser = SseParser::default();
        assert!(parser.push(b": ping\n\ndata: {\"a\"").is_empty());
        assert_eq!(parser.push(b":1}\r\n\r\nevent: x\ndata: one\ndata:two\n\ndata: tail"), vec!["{\"a\":1}", "one\ntwo"]);
        assert_eq!(parser.push(b"\n\n"), vec!["tail"]);
    }
}
