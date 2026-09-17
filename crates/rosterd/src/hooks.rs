//! Webhooks, R19: a client registers a URL for event names and rosterd POSTs each matching
//! event to it. rosterd knows a list of URLs, never what stands behind one, so the systems of
//! R8 stay clients. The events are the ones `/swarm/changes` streams: this node's journal
//! entries and the changes diffed from peer snapshots.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use rosterd_proto::Change;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::node::Node;

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hook {
    pub id: String,
    pub url: String,
    /// Event names to deliver; empty means every event.
    #[serde(default)]
    pub events: Vec<String>,
    /// Sent back as `Authorization: Bearer` so the receiver knows the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl Hook {
    fn wants(&self, event: &str) -> bool {
        self.events.is_empty() || self.events.iter().any(|e| e == event)
    }
}

#[derive(Debug, Deserialize)]
pub struct NewHook {
    pub url: String,
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub token: Option<String>,
}

pub struct Hooks {
    path: PathBuf,
    inner: Mutex<Vec<Hook>>,
    client: reqwest::Client,
}

impl Hooks {
    /// Loads `<state dir>/hooks.json`; absent or unreadable means none.
    pub fn open(path: PathBuf) -> Arc<Hooks> {
        let hooks = std::fs::read(&path).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).unwrap_or_default();
        Arc::new(Hooks { path, inner: Mutex::new(hooks), client: reqwest::Client::new() })
    }

    pub fn list(&self) -> Vec<Hook> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn add(&self, new: NewHook) -> Result<Hook, String> {
        let url = reqwest::Url::parse(&new.url).map_err(|e| format!("url: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("url: http or https only".into());
        }
        let hook = Hook { id: ulid::Ulid::new().to_string().to_lowercase(), url: new.url, events: new.events, token: new.token, created_at: Utc::now() };
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.push(hook.clone());
        self.save(&inner);
        Ok(hook)
    }

    pub fn remove(&self, id: &str) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let before = inner.len();
        inner.retain(|h| h.id != id);
        let removed = inner.len() != before;
        if removed {
            self.save(&inner);
        }
        removed
    }

    fn save(&self, hooks: &[Hook]) {
        let write = || -> std::io::Result<()> {
            if let Some(parent) = self.path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&self.path, serde_json::to_vec_pretty(hooks)?)
        };
        if let Err(error) = write() {
            tracing::warn!(%error, path = %self.path.display(), "hooks save failed");
        }
    }

    /// POSTs `{event, data}` to every hook that wants `event`, all at once, and waits for them
    /// so one hook sees events in order. A failure is logged and dropped: the client resyncs
    /// from `/swarm/changes?since=` like any other, R18.
    // ponytail: no retry and one round per event; a queue per hook if a slow receiver ever
    // holds the others back.
    pub async fn deliver(&self, event: &str, data: &Value) {
        let targets: Vec<Hook> = self.list().into_iter().filter(|h| h.wants(event)).collect();
        if targets.is_empty() {
            return;
        }
        let body = json!({ "event": event, "data": data });
        futures::future::join_all(targets.iter().map(|hook| async {
            let mut request = self.client.post(&hook.url).timeout(DELIVERY_TIMEOUT).header("X-Rosterd-Event", event).json(&body);
            if let Some(token) = &hook.token {
                request = request.bearer_auth(token);
            }
            match request.send().await {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => tracing::warn!(id = %hook.id, url = %hook.url, status = %response.status(), "hook refused"),
                Err(error) => tracing::warn!(id = %hook.id, url = %hook.url, %error, "hook delivery failed"),
            }
        }))
        .await;
    }

    /// Delivers the same stream `/swarm/changes` serves live: journal entries and peer changes.
    pub async fn run(self: Arc<Self>, node: Arc<Node>) {
        let local = BroadcastStream::new(node.journal.subscribe()).filter_map(|item| async move {
            match item {
                Ok(entry) => Some((entry.name(), serde_json::to_value(&entry).ok()?)),
                Err(BroadcastStreamRecvError::Lagged(n)) => {
                    tracing::warn!(lagged = n, "hooks fell behind the journal");
                    None
                }
            }
        });
        let peers = crate::api::peer_changes(node.clone()).filter_map(|change: Change| async move { Some((change.name(), serde_json::to_value(&change).ok()?)) });
        let mut events = std::pin::pin!(futures::stream::select(local, peers));
        while let Some((name, data)) = events.next().await {
            self.deliver(name, &data).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hooks_persist_filter_and_refuse_bad_urls() {
        let path = std::env::temp_dir().join(format!("rosterd-hooks-{}", std::process::id())).join("hooks.json");
        let _ = std::fs::remove_file(&path);
        let hooks = Hooks::open(path.clone());
        assert!(hooks.add(NewHook { url: "ftp://x".into(), events: vec![], token: None }).is_err());
        assert!(hooks.add(NewHook { url: "not a url".into(), events: vec![], token: None }).is_err());
        let a = hooks.add(NewHook { url: "http://127.0.0.1:1/a".into(), events: vec!["attention".into()], token: None }).unwrap();
        let b = hooks.add(NewHook { url: "http://127.0.0.1:1/b".into(), events: vec![], token: Some("t".into()) }).unwrap();
        assert!(a.wants("attention") && !a.wants("activity") && b.wants("activity"));
        drop(hooks);
        let hooks = Hooks::open(path.clone());
        assert_eq!(hooks.list().iter().map(|h| h.id.as_str()).collect::<Vec<_>>(), [a.id.as_str(), b.id.as_str()]);
        assert_eq!(hooks.list()[1].token.as_deref(), Some("t"));
        assert!(hooks.remove(&a.id) && !hooks.remove(&a.id));
        assert_eq!(Hooks::open(path.clone()).list().len(), 1);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
