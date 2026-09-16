//! The swarm, R7: node identity on the wire, discovery over Tailscale and static peers, invite
//! and join, gossiped membership and revocation, the per-peer snapshot exchange over SSE, and
//! proxied actions to the owning node. All node to node traffic is HTTPS on the Tailscale
//! interface, certificate pinned to the node key, requests signed, swarm key in a header, R7.6.
//!
//! OWNER: the mesh agent.

mod discovery;
mod exchange;
mod membership;
mod tls;
mod wire;

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use rosterd_proto::{NodeHealth, PeerState, SWARM_SCHEMA, Snapshot, SwarmRecord, SwarmSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::config::{Config, config_dir};
use crate::identity::Identity;
use crate::roster::Roster;
use membership::{MemberRecord, Membership, Sealed, public_key, seal, spki_der, unseal};
use wire::{Hello, Invite, INVITE_TTL_SECS, header as header_str, mint_invite, now_ms, parse_invite, signed_headers, verify_invite, verify_signed};

/// Header carrying the swarm key, R7.6.
pub const SWARM_KEY_HEADER: &str = "x-rosterd-swarm";
/// Header carrying the sender's node id.
pub const NODE_ID_HEADER: &str = "x-rosterd-node";
/// Header carrying the hex Ed25519 signature over method, path, timestamp and body hash.
pub const SIGNATURE_HEADER: &str = "x-rosterd-signature";
pub const TIMESTAMP_HEADER: &str = "x-rosterd-timestamp";

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);
const GREET_TIMEOUT: Duration = Duration::from_secs(5);
const JOIN_TIMEOUT: Duration = Duration::from_secs(15);
// ponytail: one ceiling for every proxied action; a prompt with wait_until longer than this
// times out here first. Take the timeout from the request if that happens.
const PROXY_TIMEOUT: Duration = Duration::from_secs(300);
const PEER_SNAPSHOT_TTL_SECS: i64 = 24 * 3600;

#[derive(Debug, thiserror::Error)]
pub enum MeshError {
    #[error("unknown node {0}")]
    UnknownNode(String),
    #[error("node {0} is revoked")]
    Revoked(String),
    #[error("node {0} is unreachable")]
    Unreachable(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("not in a swarm yet; run rosterd join or rosterd invite")]
    NoSwarm,
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl IntoResponse for MeshError {
    fn into_response(self) -> Response {
        let status = match &self {
            MeshError::UnknownNode(_) => StatusCode::NOT_FOUND,
            MeshError::Revoked(_) => StatusCode::FORBIDDEN,
            MeshError::Unreachable(_) => StatusCode::BAD_GATEWAY,
            MeshError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            MeshError::NoSwarm => StatusCode::CONFLICT,
            MeshError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

/// A verified peer request.
#[derive(Debug, Clone)]
pub struct PeerAuth {
    pub node_id: String,
    pub name: String,
}

/// The last complete snapshot of one peer, R7.4.
struct Peer {
    snapshot: Option<Snapshot>,
    received_at: Option<DateTime<Utc>>,
    reachable: bool,
    unreachable_since: Option<DateTime<Utc>>,
}

#[derive(Default)]
struct SwarmState {
    membership: Option<Membership>,
    /// `host:port` this node tells peers; the Tailscale IP and port in production.
    advertised: Option<String>,
    /// Addresses discovery confirmed, per node id; fresher than the signed record's.
    learned: HashMap<String, String>,
    peers: HashMap<String, Peer>,
    /// Minted, unused invite nonces with their expiry in unix seconds, R7.3 single use.
    nonces: HashMap<String, i64>,
    /// SubjectPublicKeyInfo DER accepted besides members': the admitter's during a join.
    pins: Vec<Vec<u8>>,
}

impl SwarmState {
    fn address_of(&self, node_id: &str) -> Option<String> {
        self.learned
            .get(node_id)
            .cloned()
            .or_else(|| self.membership.as_ref()?.member(node_id)?.address.clone())
    }

    /// Drops snapshots of peers unreachable for over 24 h, R7.4. Returns whether any went.
    fn expire(&mut self, now: DateTime<Utc>) -> bool {
        let before = self.peers.len();
        self.peers.retain(|_, peer| {
            peer.reachable || peer.unreachable_since.is_none_or(|since| (now - since).num_seconds() < PEER_SNAPSHOT_TTL_SECS)
        });
        self.peers.len() != before
    }
}

pub struct Mesh {
    config: Arc<Config>,
    identity: Identity,
    roster: Arc<Roster>,
    version: &'static str,
    changed: watch::Sender<u64>,
    /// `<config dir>/swarm.json`, mode 0600.
    file: PathBuf,
    state: Arc<Mutex<SwarmState>>,
    client: reqwest::Client,
    cert: tls::NodeCert,
}

#[derive(Debug, Serialize, Deserialize)]
struct JoinRequest {
    invite: String,
    hello: Hello,
}

#[derive(Debug, Serialize, Deserialize)]
struct JoinResponse {
    swarm_id: String,
    swarm_key_sealed: Sealed,
    members: Vec<MemberRecord>,
}

impl Mesh {
    pub fn new(config: Arc<Config>, identity: Identity, roster: Arc<Roster>, version: &'static str) -> anyhow::Result<Arc<Mesh>> {
        Mesh::build(config, identity, roster, version, config_dir().join("swarm.json"), None)
    }

    /// A mesh whose file and advertised address the test chooses, so two nodes can share one
    /// process and one loopback interface. Production binds Tailscale only, R7.6.
    #[cfg(test)]
    pub fn new_at(
        config: Arc<Config>,
        identity: Identity,
        roster: Arc<Roster>,
        version: &'static str,
        dir: &std::path::Path,
        address: &str,
    ) -> anyhow::Result<Arc<Mesh>> {
        Mesh::build(config, identity, roster, version, dir.join("swarm.json"), Some(address.to_string()))
    }

    fn build(
        config: Arc<Config>,
        identity: Identity,
        roster: Arc<Roster>,
        version: &'static str,
        file: PathBuf,
        advertised: Option<String>,
    ) -> anyhow::Result<Arc<Mesh>> {
        // Both ring and aws-lc-rs are compiled in through reqwest and axum-server; rustls then
        // needs a process default before any plain `ClientConfig::builder()` elsewhere.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let membership = Membership::load(&file)?;
        let state = Arc::new(Mutex::new(SwarmState { membership, advertised, ..SwarmState::default() }));
        let pins = state.clone();
        let client = tls::client(Arc::new(move |spki: &[u8]| {
            let state = pins.lock().unwrap_or_else(|e| e.into_inner());
            state.pins.iter().any(|pin| pin == spki)
                || state
                    .membership
                    .as_ref()
                    .is_some_and(|m| m.active().any(|member| spki_der(&member.public_key).is_ok_and(|der| der == spki)))
        }))?;
        let cert = tls::node_cert(&identity, &config.node.name)?;
        let (changed, _) = watch::channel(0);
        Ok(Arc::new(Mesh { config, identity, roster, version, changed, file, state, client, cert }))
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    fn lock(&self) -> MutexGuard<'_, SwarmState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v += 1);
    }

    fn now_secs() -> i64 {
        Utc::now().timestamp()
    }

    pub(super) fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub(super) fn swarm_key(&self) -> Option<[u8; 32]> {
        self.lock().membership.as_ref().map(|m| m.swarm_key)
    }

    pub(super) fn address_of(&self, node_id: &str) -> Option<String> {
        self.lock().address_of(node_id)
    }

    /// Discovery, hello probing, and the long lived /events subscription to every peer. Never
    /// returns.
    pub async fn run(self: Arc<Self>) {
        if self.lock().advertised.is_none()
            && let Some(ip) = self.tailscale_ip().await
        {
            self.lock().advertised = Some(format!("{ip}:{}", self.config.node.port));
        }
        tokio::join!(self.clone().watch_local(), self.clone().discover_forever(), self.clone().exchange_forever());
    }

    async fn watch_local(self: Arc<Self>) {
        let mut local = self.roster.watch();
        while local.changed().await.is_ok() {
            self.bump();
        }
    }

    async fn discover_forever(self: Arc<Self>) {
        let mut tick = tokio::time::interval(DISCOVERY_INTERVAL);
        loop {
            tick.tick().await;
            self.discover_once().await;
        }
    }

    /// One pass of R7.2: refresh the advertised address, greet every member we have an
    /// address for (gossip), and probe every Tailscale or static address not yet bound to one.
    async fn discover_once(&self) {
        if let Some(ip) = self.tailscale_ip().await {
            self.lock().advertised = Some(format!("{ip}:{}", self.config.node.port));
        }
        let (known, own) = {
            let state = self.lock();
            let Some(membership) = &state.membership else { return };
            let known: Vec<String> = membership
                .active()
                .filter(|m| m.node_id != self.identity.node_id)
                .filter_map(|m| state.address_of(&m.node_id))
                .collect();
            (known, state.advertised.clone())
        };
        let port = self.config.node.port;
        let mut targets: Vec<String> = discovery::tailscale_peers().await.into_iter().map(|ip| format!("{ip}:{port}")).collect();
        targets.extend(self.config.swarm.static_peers.iter().cloned());
        targets.extend(known.iter().cloned());
        let mut seen = HashSet::new();
        let targets: Vec<String> = targets.into_iter().filter(|t| Some(t) != own.as_ref() && seen.insert(t.clone())).collect();
        futures::future::join_all(targets.iter().map(|address| async move {
            if let Err(error) = self.greet(address).await {
                tracing::debug!(%address, %error, "hello failed");
            }
        }))
        .await;
    }

    /// Keeps one `/events` subscription per active member with an address, R7.4.
    async fn exchange_forever(self: Arc<Self>) {
        let mut tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            let wanted: HashSet<String> = {
                let mut state = self.lock();
                let expired = state.expire(Utc::now());
                let wanted = state
                    .membership
                    .as_ref()
                    .map(|m| {
                        m.active()
                            .filter(|r| r.node_id != self.identity.node_id && state.address_of(&r.node_id).is_some())
                            .map(|r| r.node_id.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                drop(state);
                if expired {
                    self.bump();
                }
                wanted
            };
            for node_id in &wanted {
                if !tasks.contains_key(node_id) {
                    tasks.insert(node_id.clone(), tokio::spawn(exchange::subscribe(self.clone(), node_id.clone())));
                }
            }
            let gone: Vec<String> = tasks.keys().filter(|id| !wanted.contains(*id)).cloned().collect();
            for node_id in gone {
                if let Some(task) = tasks.remove(&node_id) {
                    task.abort();
                }
                self.set_reachable(&node_id, false);
            }
        }
    }

    /// Union of the local roster and every peer's last snapshot, each tagged with peer state and
    /// age, R7.4. Peers unreachable for over 24 h are dropped.
    pub fn swarm_snapshot(&self) -> SwarmSnapshot {
        let now = Utc::now();
        let local = self.roster.snapshot();
        let mut state = self.lock();
        state.expire(now);
        let mut records: Vec<SwarmRecord> = local
            .records
            .iter()
            .map(|record| SwarmRecord { record: record.clone(), peer_state: PeerState::Local, peer_age_ms: 0 })
            .collect();
        for peer in state.peers.values() {
            let Some(snapshot) = &peer.snapshot else { continue };
            let peer_state = if peer.reachable { PeerState::Reachable } else { PeerState::Unreachable };
            let peer_age_ms = age_ms(now, peer.received_at);
            records.extend(
                snapshot.records.iter().map(|record| SwarmRecord { record: record.clone(), peer_state, peer_age_ms }),
            );
        }
        SwarmSnapshot { schema: SWARM_SCHEMA.into(), generated_at: now, nodes: self.nodes_locked(&state, now), records }
    }

    /// Bumps on any change anywhere: local roster or a peer snapshot.
    pub fn changed(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub fn nodes(&self) -> Vec<NodeHealth> {
        let now = Utc::now();
        let mut state = self.lock();
        state.expire(now);
        self.nodes_locked(&state, now)
    }

    fn nodes_locked(&self, state: &SwarmState, now: DateTime<Utc>) -> Vec<NodeHealth> {
        let local = self.roster.snapshot();
        let mut nodes = vec![NodeHealth {
            node_id: self.identity.node_id.clone(),
            name: self.config.node.name.clone(),
            address: state.advertised.clone(),
            state: PeerState::Local,
            peer_age_ms: 0,
            version: Some(self.version.into()),
            capabilities: local.capabilities.clone(),
            revoked: false,
        }];
        let Some(membership) = &state.membership else { return nodes };
        for member in membership.members.values().filter(|m| m.node_id != self.identity.node_id) {
            let peer = state.peers.get(&member.node_id);
            nodes.push(NodeHealth {
                node_id: member.node_id.clone(),
                name: member.name.clone(),
                address: state.address_of(&member.node_id),
                state: if peer.is_some_and(|p| p.reachable) { PeerState::Reachable } else { PeerState::Unreachable },
                peer_age_ms: peer.map(|p| age_ms(now, p.received_at)).unwrap_or(0),
                version: member.version.clone(),
                capabilities: peer.and_then(|p| p.snapshot.as_ref()).map(|s| s.capabilities.clone()).unwrap_or_default(),
                revoked: member.revoked,
            });
        }
        nodes
    }

    /// The node id owning a session key, local included.
    pub fn owner_of(&self, session_key: &str) -> Option<String> {
        if self.roster.get(session_key).is_some() {
            return Some(self.identity.node_id.clone());
        }
        let state = self.lock();
        state
            .peers
            .iter()
            .find(|(_, peer)| peer.snapshot.as_ref().is_some_and(|s| s.records.iter().any(|r| r.session_key == session_key)))
            .map(|(node_id, _)| node_id.clone())
    }

    /// The live session bound to an attempt anywhere in the swarm, R5.4.
    pub fn live_by_attempt(&self, attempt_id: &str) -> Option<SwarmRecord> {
        self.swarm_snapshot()
            .records
            .into_iter()
            .find(|r| r.record.attempt_id.as_deref() == Some(attempt_id) && r.record.liveness != rosterd_proto::Liveness::Ended)
    }

    /// Peer facing routes served on the Tailscale listener: /node/hello, /node/join, and the
    /// gossip endpoints. The api mounts this and also serves /events there; requests reach
    /// handlers only after `authenticate` passed, except /node/hello and /node/join.
    pub fn peer_router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/node/hello", get(hello_get).post(hello_post))
            .route("/node/join", post(join_post))
            .with_state(self.clone())
    }

    /// Operator routes on the local socket only: POST /node/invite, POST /node/join
    /// `{peer, token}`, POST /node/revoke `{node_id}`, GET /node/members. The CLI calls these.
    pub fn local_router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/node/invite", post(invite_post))
            .route("/node/join", post(join_local))
            .route("/node/revoke", post(revoke_post))
            .route("/node/members", get(members_get))
            .with_state(self.clone())
    }

    /// Verifies a peer request: swarm key header, signature by a known non revoked member,
    /// timestamp within skew. Used by the api as middleware on the Tailscale listener. `path`
    /// is the request path without its query.
    pub fn authenticate(&self, method: &Method, path: &str, headers: &HeaderMap, body: &[u8]) -> Result<PeerAuth, MeshError> {
        let state = self.lock();
        let membership = state.membership.as_ref().ok_or(MeshError::NoSwarm)?;
        let unauthorized = |e: anyhow::Error| MeshError::Unauthorized(e.to_string());
        let swarm = header_str(headers, SWARM_KEY_HEADER).map_err(unauthorized)?;
        if swarm != membership.key_hash() {
            return Err(MeshError::Unauthorized("swarm key does not match".into()));
        }
        let node_id = header_str(headers, NODE_ID_HEADER).map_err(unauthorized)?;
        let member = membership.member(node_id).ok_or_else(|| MeshError::UnknownNode(node_id.into()))?;
        if member.revoked {
            return Err(MeshError::Revoked(node_id.into()));
        }
        verify_signed(headers, &member.public_key, method, path, body, Self::now_secs()).map_err(unauthorized)?;
        Ok(PeerAuth { node_id: member.node_id.clone(), name: member.name.clone() })
    }

    /// Sends a session action to the owning node with this node's signature and returns its
    /// answer, R7.5. `path` is the owner-local path, e.g. /sessions/{key}/prompt.
    pub async fn proxy(
        &self,
        node_id: &str,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value), MeshError> {
        let (address, swarm_key) = {
            let state = self.lock();
            let membership = state.membership.as_ref().ok_or(MeshError::NoSwarm)?;
            let member = membership.member(node_id).ok_or_else(|| MeshError::UnknownNode(node_id.into()))?;
            if member.revoked {
                return Err(MeshError::Revoked(node_id.into()));
            }
            (state.address_of(node_id).ok_or_else(|| MeshError::Unreachable(node_id.into()))?, membership.swarm_key)
        };
        let bytes = match &body {
            Some(value) => serde_json::to_vec(value).map_err(anyhow::Error::from)?,
            None => Vec::new(),
        };
        let signed_path = path.split('?').next().unwrap_or(path);
        let headers = signed_headers(&self.identity, &swarm_key, &method, signed_path, &bytes);
        let mut request = self.client.request(method, format!("https://{address}{path}")).headers(headers).timeout(PROXY_TIMEOUT);
        if body.is_some() {
            request = request.header(header::CONTENT_TYPE, "application/json").body(bytes);
        }
        let response = request.send().await.map_err(|error| {
            tracing::debug!(%node_id, %address, %error, "proxy failed");
            MeshError::Unreachable(node_id.into())
        })?;
        let status = response.status();
        let text = response.text().await.map_err(anyhow::Error::from)?;
        let value = if text.is_empty() { Value::Null } else { serde_json::from_str(&text).unwrap_or(json!({ "error": text })) };
        Ok((status, value))
    }

    /// The self-signed certificate whose key is the node key, R7.6.
    pub fn tls(&self) -> anyhow::Result<axum_server::tls_rustls::RustlsConfig> {
        tls::server_config(&self.cert)
    }

    /// This machine's Tailscale IPv4, from `tailscale ip -4`, or None when Tailscale is absent.
    pub async fn tailscale_ip(&self) -> Option<IpAddr> {
        discovery::tailscale_ip().await
    }

    // Membership operations, R7.3.

    /// Mints a single use invite; the first one creates the swarm.
    /// R14.3 `invite --ttl MINUTES` picks the life; `INVITE_TTL_SECS` is the default hour.
    pub fn invite_for(&self, ttl_secs: i64) -> Result<String, MeshError> {
        let mut state = self.lock();
        let created = state.membership.is_none();
        if created {
            let membership = Membership::create(&self.identity, &self.config.node.name, self.version, state.advertised.clone())?;
            membership.save(&self.file)?;
            state.membership = Some(membership);
        }
        let now = Self::now_secs();
        state.nonces.retain(|_, expires_at| *expires_at > now);
        let membership = state.membership.as_ref().expect("just ensured");
        let invite = Invite {
            swarm_id: membership.swarm_id.clone(),
            nonce: ulid::Ulid::new().to_string().to_lowercase(),
            expires_at: now + ttl_secs,
            admitter: self.identity.node_id.clone(),
            admitter_public_key: self.identity.public_hex(),
            admitter_address: state.advertised.clone(),
        };
        let token = mint_invite(&invite, &membership.swarm_key);
        state.nonces.insert(invite.nonce, invite.expires_at);
        drop(state);
        if created {
            self.bump();
        }
        Ok(token)
    }

    /// Joins through `peer` (or the admitter address inside the invite) with TLS pinned to the
    /// admitter's key, then greets every member so they learn this node.
    pub async fn join(&self, peer: &str, token: &str) -> Result<(), MeshError> {
        let unauthorized = |e: anyhow::Error| MeshError::Unauthorized(e.to_string());
        let invite = parse_invite(token).map_err(unauthorized)?;
        let address = if peer.is_empty() { invite.admitter_address.clone() } else { Some(peer.to_string()) };
        let address = address.ok_or_else(|| MeshError::Unauthorized("invite names no admitter address; pass the peer".into()))?;
        if let Some(membership) = &self.lock().membership
            && membership.swarm_id != invite.swarm_id
        {
            return Err(MeshError::Other(anyhow::anyhow!(
                "already in swarm {}; remove {} to leave it first",
                membership.swarm_id,
                self.file.display()
            )));
        }
        let pin = spki_der(&invite.admitter_public_key).map_err(unauthorized)?;
        self.lock().pins.push(pin.clone());
        let joined = self.join_pinned(&invite, &address, token).await;
        self.lock().pins.retain(|p| p != &pin);
        joined?;
        self.bump();

        let others: Vec<String> = {
            let state = self.lock();
            let membership = state.membership.as_ref().expect("joined");
            membership
                .active()
                .filter(|m| m.node_id != self.identity.node_id && m.node_id != invite.admitter)
                .filter_map(|m| state.address_of(&m.node_id))
                .collect()
        };
        futures::future::join_all(others.iter().map(|address| async move {
            if let Err(error) = self.greet(address).await {
                tracing::debug!(%address, %error, "greeting after join failed");
            }
        }))
        .await;
        Ok(())
    }

    async fn join_pinned(&self, invite: &Invite, address: &str, token: &str) -> Result<(), MeshError> {
        let request = JoinRequest { invite: token.to_string(), hello: self.hello()? };
        let response = self
            .client
            .post(format!("https://{address}/node/join"))
            .json(&request)
            .timeout(JOIN_TIMEOUT)
            .send()
            .await
            .map_err(|error| MeshError::Unreachable(format!("{address}: {error}")))?;
        let status = response.status();
        let value: Value = response.json().await.map_err(anyhow::Error::from)?;
        if !status.is_success() {
            let message = value.get("error").and_then(Value::as_str).unwrap_or("join refused").to_string();
            return Err(MeshError::Unauthorized(format!("{address} answered {status}: {message}")));
        }
        let answer: JoinResponse = serde_json::from_value(value).map_err(anyhow::Error::from)?;
        let swarm_key: [u8; 32] = unseal(&self.identity, &answer.swarm_key_sealed)?
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("sealed swarm key is not 32 bytes"))?;
        let mut membership = Membership { swarm_id: answer.swarm_id, swarm_key, members: Default::default() };
        // The list came over a channel pinned to the admitter's key: trusted as is.
        for record in answer.members {
            membership.insert_trusted(record);
        }
        if !membership.is_active(&self.identity.node_id) {
            return Err(MeshError::Unauthorized("admitter did not list this node as a member".into()));
        }
        if membership.member(&invite.admitter).map(|m| m.public_key.as_str()) != Some(invite.admitter_public_key.as_str()) {
            return Err(MeshError::Unauthorized("admitter's key differs from the invite's".into()));
        }
        membership.save(&self.file)?;
        let mut state = self.lock();
        state.learned.insert(invite.admitter.clone(), address.to_string());
        state.membership = Some(membership);
        Ok(())
    }

    /// The admitter's side of R7.3: verifies invite and hello, signs the joiner in, seals the
    /// swarm key to its node key.
    fn admit(&self, request: JoinRequest) -> Result<JoinResponse, MeshError> {
        let unauthorized = |e: anyhow::Error| MeshError::Unauthorized(e.to_string());
        let mut state = self.lock();
        let SwarmState { membership, nonces, learned, .. } = &mut *state;
        let membership = membership.as_mut().ok_or(MeshError::NoSwarm)?;
        let invite = verify_invite(&request.invite, &membership.swarm_key, &membership.swarm_id, Self::now_secs()).map_err(unauthorized)?;
        if invite.admitter != self.identity.node_id || nonces.remove(&invite.nonce).is_none() {
            return Err(MeshError::Unauthorized("invite was not minted here or was already used".into()));
        }
        let hello = request.hello;
        hello.verify().map_err(unauthorized)?;
        if hello.node_id == self.identity.node_id {
            return Err(MeshError::Unauthorized("a node cannot join itself".into()));
        }
        let signed_at = membership.member(&hello.node_id).map_or(now_ms(), |m| now_ms().max(m.signed_at + 1));
        let record = MemberRecord {
            node_id: hello.node_id.clone(),
            name: hello.name.clone(),
            public_key: hello.public_key.clone(),
            address: hello.address.clone(),
            version: Some(hello.version.clone()),
            signed_at,
            revoked: false,
            signed_by: String::new(),
            signature: String::new(),
        }
        .sign(&self.identity)?;
        membership.insert_trusted(record);
        if let Some(address) = &hello.address {
            learned.insert(hello.node_id.clone(), address.clone());
        }
        membership.save(&self.file)?;
        let sealed = seal(&public_key(&hello.public_key).map_err(unauthorized)?, &membership.swarm_key)?;
        let answer = JoinResponse {
            swarm_id: membership.swarm_id.clone(),
            swarm_key_sealed: sealed,
            members: membership.members.values().cloned().collect(),
        };
        drop(state);
        self.bump();
        tracing::info!(node = %hello.name, node_id = %hello.node_id, "admitted to the swarm");
        Ok(answer)
    }

    /// Signs a revocation for `node_id`, drops its snapshot at once, gossips it from here on.
    pub fn revoke(&self, node_id: &str) -> Result<(), MeshError> {
        let mut state = self.lock();
        let SwarmState { membership, peers, learned, .. } = &mut *state;
        let membership = membership.as_mut().ok_or(MeshError::NoSwarm)?;
        if node_id == self.identity.node_id {
            return Err(MeshError::Other(anyhow::anyhow!("a node cannot revoke itself")));
        }
        let current = membership.member(node_id).ok_or_else(|| MeshError::UnknownNode(node_id.into()))?.clone();
        let record = MemberRecord { revoked: true, signed_at: now_ms().max(current.signed_at + 1), ..current }.sign(&self.identity)?;
        membership.insert_trusted(record);
        membership.save(&self.file)?;
        peers.remove(node_id);
        learned.remove(node_id);
        drop(state);
        self.bump();
        Ok(())
    }

    /// R14.3 `leave`: this node revokes itself, tells every reachable peer (best effort, one
    /// hello each so they merge the revocation), then forgets the swarm: the membership file
    /// with the swarm key in it goes, and `nodes()` is the local node alone.
    pub async fn leave(&self) -> Result<(), MeshError> {
        let addresses: Vec<String> = {
            let mut state = self.lock();
            let membership = state.membership.as_mut().ok_or(MeshError::NoSwarm)?;
            let me = self.identity.node_id.as_str();
            let current = membership.member(me).ok_or_else(|| MeshError::UnknownNode(me.into()))?.clone();
            let record = MemberRecord { revoked: true, signed_at: now_ms().max(current.signed_at + 1), ..current }.sign(&self.identity)?;
            membership.insert_trusted(record);
            let others: Vec<String> = membership.active().filter(|m| m.node_id != me).map(|m| m.node_id.clone()).collect();
            others.iter().filter_map(|id| state.address_of(id)).collect()
        };
        for address in addresses {
            // The peer merges our members list before it refuses us as revoked; the refusal is expected.
            if let Err(error) = self.greet(&address).await {
                tracing::debug!(%address, %error, "leave: hello answered");
            }
        }
        let mut state = self.lock();
        *state = SwarmState { advertised: state.advertised.take(), ..SwarmState::default() };
        drop(state);
        match std::fs::remove_file(&self.file) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(MeshError::Other(anyhow::Error::from(error).context(format!("remove {}", self.file.display())))),
        }
        self.bump();
        tracing::info!("left the swarm; standalone, R14.3");
        Ok(())
    }

    /// The swarm this node belongs to, None when standalone.
    pub fn swarm_id(&self) -> Option<String> {
        self.lock().membership.as_ref().map(|m| m.swarm_id.clone())
    }

    /// This node's signed hello with its membership list, R7.2 R7.3.
    fn hello(&self) -> Result<Hello, MeshError> {
        let state = self.lock();
        let snapshot = self.roster.snapshot();
        Ok(Hello {
            node_id: self.identity.node_id.clone(),
            name: self.config.node.name.clone(),
            public_key: self.identity.public_hex(),
            version: self.version.into(),
            swarm_id: state.membership.as_ref().map(|m| m.swarm_id.clone()),
            address: state.advertised.clone(),
            capabilities: snapshot.capabilities.clone(),
            members: state.membership.as_ref().map(|m| m.members.values().cloned().collect()).unwrap_or_default(),
            signed_at: now_ms(),
            signature: String::new(),
        }
        .sign(&self.identity)?)
    }

    /// A hello arrived: verify it, merge its membership, learn the sender's address. `reached`
    /// is the address we reached the sender at, when we did the reaching.
    fn absorb_hello(&self, hello: &Hello, reached: Option<&str>) -> Result<(), MeshError> {
        hello.verify().map_err(|e| MeshError::Unauthorized(e.to_string()))?;
        let mut state = self.lock();
        let mut bumped = false;
        let SwarmState { membership, learned, peers, .. } = &mut *state;
        if let Some(membership) = membership {
            if hello.swarm_id.as_deref() == Some(membership.swarm_id.as_str()) {
                let changed = membership.merge(&hello.members);
                if !changed.is_empty() {
                    membership.save(&self.file)?;
                    for node_id in &changed {
                        if membership.member(node_id).is_some_and(|m| m.revoked) {
                            peers.remove(node_id);
                            learned.remove(node_id);
                        }
                    }
                    bumped = true;
                }
            }
            if membership.member(&hello.node_id).is_some_and(|m| m.revoked) {
                return Err(MeshError::Revoked(hello.node_id.clone()));
            }
            if membership.is_active(&hello.node_id)
                && let Some(address) = reached.map(str::to_string).or_else(|| hello.address.clone())
                && learned.get(&hello.node_id) != Some(&address)
            {
                learned.insert(hello.node_id.clone(), address);
                bumped = true;
            }
        }
        drop(state);
        if bumped {
            self.bump();
        }
        Ok(())
    }

    /// POST /node/hello to `address`: exchange hellos, merge theirs.
    async fn greet(&self, address: &str) -> Result<(), MeshError> {
        let hello = self.hello()?;
        let body = serde_json::to_vec(&hello).map_err(anyhow::Error::from)?;
        let mut request = self.client.post(format!("https://{address}/node/hello")).timeout(GREET_TIMEOUT);
        if let Some(key) = self.swarm_key() {
            request = request.headers(signed_headers(&self.identity, &key, &Method::POST, "/node/hello", &body));
        }
        let response = request
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| MeshError::Unreachable(format!("{address}: {error}")))?;
        if !response.status().is_success() {
            return Err(MeshError::Unauthorized(format!("{address} answered {}", response.status())));
        }
        let theirs: Hello = response.json().await.map_err(anyhow::Error::from)?;
        self.absorb_hello(&theirs, Some(address))
    }

    // Peer store, written by the exchange tasks.

    pub(super) fn store_snapshot(&self, snapshot: Snapshot) {
        let mut state = self.lock();
        let revoked = state.membership.as_ref().is_some_and(|m| !m.is_active(&snapshot.node_id));
        if revoked {
            return;
        }
        let now = Utc::now();
        let peer = state.peers.entry(snapshot.node_id.clone()).or_insert_with(|| Peer {
            snapshot: None,
            received_at: None,
            reachable: true,
            unreachable_since: None,
        });
        peer.snapshot = Some(snapshot);
        peer.received_at = Some(now);
        peer.reachable = true;
        peer.unreachable_since = None;
        drop(state);
        self.bump();
    }

    pub(super) fn set_reachable(&self, node_id: &str, reachable: bool) {
        let mut state = self.lock();
        if !state.peers.contains_key(node_id) {
            if !reachable {
                return;
            }
            state.peers.insert(
                node_id.to_string(),
                Peer { snapshot: None, received_at: None, reachable: false, unreachable_since: None },
            );
        }
        let peer = state.peers.get_mut(node_id).expect("present");
        if peer.reachable == reachable {
            return;
        }
        peer.reachable = reachable;
        peer.unreachable_since = (!reachable).then_some(Utc::now());
        drop(state);
        self.bump();
    }

    #[cfg(test)]
    fn peer_for_test(&self, node_id: &str, snapshot: Snapshot, received_at: DateTime<Utc>, unreachable_since: Option<DateTime<Utc>>) {
        self.lock().peers.insert(
            node_id.into(),
            Peer { snapshot: Some(snapshot), received_at: Some(received_at), reachable: unreachable_since.is_none(), unreachable_since },
        );
    }
}

fn age_ms(now: DateTime<Utc>, received_at: Option<DateTime<Utc>>) -> u64 {
    received_at.map(|at| (now - at).num_milliseconds().max(0) as u64).unwrap_or(0)
}

// Peer routes, R7.2 R7.3. Unauthenticated, so they check the caller against revocations
// themselves.

fn refuse_revoked(mesh: &Mesh, headers: &HeaderMap) -> Result<(), MeshError> {
    if let Some(node_id) = headers.get(NODE_ID_HEADER).and_then(|v| v.to_str().ok())
        && mesh.lock().membership.as_ref().and_then(|m| m.member(node_id)).is_some_and(|m| m.revoked)
    {
        return Err(MeshError::Revoked(node_id.into()));
    }
    Ok(())
}

async fn hello_get(State(mesh): State<Arc<Mesh>>, headers: HeaderMap) -> Result<Json<Hello>, MeshError> {
    refuse_revoked(&mesh, &headers)?;
    Ok(Json(mesh.hello()?))
}

async fn hello_post(State(mesh): State<Arc<Mesh>>, headers: HeaderMap, Json(hello): Json<Hello>) -> Result<Json<Hello>, MeshError> {
    refuse_revoked(&mesh, &headers)?;
    mesh.absorb_hello(&hello, None)?;
    let ours = mesh.hello()?;
    if ours.swarm_id.is_some() && ours.swarm_id != hello.swarm_id {
        return Err(MeshError::Unauthorized("not a member of this swarm".into()));
    }
    Ok(Json(ours))
}

async fn join_post(State(mesh): State<Arc<Mesh>>, Json(request): Json<JoinRequest>) -> Result<Json<JoinResponse>, MeshError> {
    mesh.admit(request).map(Json)
}

// Operator routes on the local socket.

#[derive(Deserialize)]
struct JoinCommand {
    #[serde(default)]
    peer: String,
    token: String,
}

#[derive(Deserialize)]
struct RevokeCommand {
    node_id: String,
}

#[derive(Deserialize, Default)]
struct InviteCommand {
    #[serde(default)]
    ttl_minutes: Option<i64>,
}

async fn invite_post(State(mesh): State<Arc<Mesh>>, body: Option<Json<InviteCommand>>) -> Result<Json<Value>, MeshError> {
    let ttl = body.and_then(|Json(c)| c.ttl_minutes).filter(|m| *m > 0).map_or(INVITE_TTL_SECS, |m| m * 60);
    Ok(Json(json!({ "invite": mesh.invite_for(ttl)? })))
}

async fn join_local(State(mesh): State<Arc<Mesh>>, Json(command): Json<JoinCommand>) -> Result<Json<Value>, MeshError> {
    mesh.join(&command.peer, &command.token).await?;
    Ok(Json(members_json(&mesh)))
}

async fn revoke_post(State(mesh): State<Arc<Mesh>>, Json(command): Json<RevokeCommand>) -> Result<Json<Value>, MeshError> {
    mesh.revoke(&command.node_id)?;
    Ok(Json(members_json(&mesh)))
}

async fn members_get(State(mesh): State<Arc<Mesh>>) -> Json<Value> {
    Json(members_json(&mesh))
}

fn members_json(mesh: &Mesh) -> Value {
    let state = mesh.lock();
    match &state.membership {
        Some(m) => json!({ "swarm_id": m.swarm_id, "members": m.members.values().collect::<Vec<_>>() }),
        None => json!({ "swarm_id": null, "members": [] }),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::response::sse::{Event, Sse};
    use ed25519_dalek::SigningKey;
    use futures::StreamExt;
    use rosterd_proto::{Capabilities, Record};

    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rosterd-mesh-{tag}-{}-{}", std::process::id(), ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn node(name: &str, address: &str) -> (Arc<Mesh>, PathBuf) {
        let dir = temp_dir(name);
        let identity = Identity::from_key(SigningKey::generate(&mut rand::rngs::OsRng));
        let mut config = Config::default();
        config.node.name = name.into();
        let roster = Roster::new(name, &identity.node_id, Capabilities { harnesses: vec!["claude".into()], ..Default::default() });
        let mesh = Mesh::new_at(Arc::new(config), identity, roster, "0.1.0-test", &dir, address).unwrap();
        (mesh, dir)
    }

    fn snapshot_with_record(node: &str, node_id: &str, attempt: &str) -> Snapshot {
        let mut snapshot = Snapshot::empty(node, node_id);
        snapshot.seq = 1;
        snapshot.records.push(
            serde_json::from_value::<Record>(json!({
                "node": node, "node_id": node_id, "session_key": format!("{node_id}:42:7"), "pid": 42, "start_ticks": 7,
                "started_at": Utc::now(), "harness": "claude", "lane": "headless", "attempt_id": attempt
            }))
            .unwrap(),
        );
        snapshot
    }

    /// The api agent's /events, reduced: authenticate, then one snapshot and silence.
    async fn events(State(mesh): State<Arc<Mesh>>, method: Method, headers: HeaderMap) -> Response {
        match mesh.authenticate(&method, "/events", &headers, b"") {
            Ok(_) => {
                let local = mesh.roster.snapshot();
                let snapshot = snapshot_with_record(&local.node, &local.node_id, &format!("attempt-{}", local.node));
                let first = futures::stream::once(async move { Ok::<_, std::convert::Infallible>(Event::default().json_data(snapshot).unwrap()) });
                Sse::new(first.chain(futures::stream::pending())).into_response()
            }
            Err(error) => error.into_response(),
        }
    }

    fn serve(mesh: &Arc<Mesh>) -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        mesh.lock().advertised = Some(address.to_string());
        let router = mesh.peer_router().merge(Router::new().route("/events", get(events)).with_state(mesh.clone()));
        let server = axum_server::from_tcp_rustls(listener, mesh.tls().unwrap()).unwrap();
        tokio::spawn(server.serve(router.into_make_service()));
        address
    }

    async fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
        for _ in 0..200 {
            if ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {what}");
    }

    #[tokio::test]
    async fn two_nodes_join_over_loopback_tls_and_see_each_other() {
        let (a, dir_a) = node("gibson", "127.0.0.1:1");
        let a_addr = serve(&a);
        let (b, dir_b) = node("wintermute", "127.0.0.1:1");
        let b_addr = serve(&b);

        // No swarm yet: peer requests are refused, an invite creates one.
        assert!(matches!(a.authenticate(&Method::GET, "/events", &HeaderMap::new(), b""), Err(MeshError::NoSwarm)));
        let token = a.invite_for(INVITE_TTL_SECS).unwrap();
        assert!(dir_a.join("swarm.json").exists());
        assert!(matches!(b.join(&a_addr.to_string(), "garbage").await, Err(MeshError::Unauthorized(_))));

        b.join(&a_addr.to_string(), &token).await.unwrap();
        assert_eq!(a.swarm_key(), b.swarm_key());
        assert_eq!(a.nodes().len(), 2);
        assert_eq!(b.nodes().len(), 2);
        assert_eq!(Membership::load(&dir_b.join("swarm.json")).unwrap().unwrap().members.len(), 2);
        // Single use: the same invite is refused the second time.
        assert!(matches!(b.join(&a_addr.to_string(), &token).await, Err(MeshError::Unauthorized(_))));

        // Exchange: each subscribes to the other's /events and stores the snapshot.
        tokio::spawn(a.clone().exchange_forever());
        tokio::spawn(b.clone().exchange_forever());
        wait_for("both snapshots on both nodes", || {
            a.swarm_snapshot().records.len() == 1 && b.swarm_snapshot().records.len() == 1
        })
        .await;
        let on_a = a.swarm_snapshot();
        assert!(on_a.nodes.iter().all(|n| n.state != PeerState::Unreachable));
        let from_b = on_a.records.iter().find(|r| r.peer_state == PeerState::Reachable).unwrap();
        assert_eq!(from_b.record.node_id, b.identity.node_id);
        assert_eq!(a.owner_of(&from_b.record.session_key).as_deref(), Some(b.identity.node_id.as_str()));
        assert_eq!(a.live_by_attempt("attempt-wintermute").unwrap().record.node_id, b.identity.node_id);
        assert!(a.live_by_attempt("attempt-nobody").is_none());

        // Proxy: a signed request to the owner comes back with its status; here /node/hello.
        let (status, body) = a.proxy(&b.identity.node_id, Method::GET, "/node/hello", None).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["node_id"], b.identity.node_id);

        // A greeting carries membership both ways; a stranger's certificate is refused.
        a.greet(&b_addr.to_string()).await.unwrap();
        let (stranger, _) = node("case", "127.0.0.1:1");
        assert!(matches!(a.greet(&serve(&stranger).to_string()).await, Err(MeshError::Unreachable(_))));

        // Leave, R14.3: a third node joins and leaves; a hears the self-revocation, the leaver
        // is standalone with no swarm file.
        let (c, dir_c) = node("molly", "127.0.0.1:1");
        c.join(&a_addr.to_string(), &a.invite_for(INVITE_TTL_SECS).unwrap()).await.unwrap();
        assert_eq!((a.nodes().len(), c.swarm_id(), c.nodes().len()), (3, a.swarm_id(), 3));
        c.leave().await.unwrap();
        assert!(!dir_c.join("swarm.json").exists());
        assert_eq!((c.swarm_id(), c.nodes().len()), (None, 1));
        assert!(matches!(c.leave().await, Err(MeshError::NoSwarm)));
        assert!(a.nodes().iter().any(|n| n.node_id == c.identity.node_id && n.revoked), "{:?}", a.nodes());
        std::fs::remove_dir_all(&dir_c).unwrap();

        // Revoke b on a: refused on hello and on /events, snapshot gone at once.
        a.revoke(&b.identity.node_id).unwrap();
        assert!(a.swarm_snapshot().records.is_empty());
        assert!(a.nodes().iter().any(|n| n.node_id == b.identity.node_id && n.revoked));
        assert!(matches!(b.greet(&a_addr.to_string()).await, Err(MeshError::Unauthorized(_))));
        let headers = signed_headers(&b.identity, &b.swarm_key().unwrap(), &Method::GET, "/events", b"");
        assert!(matches!(a.authenticate(&Method::GET, "/events", &headers, b""), Err(MeshError::Revoked(_))));
        assert!(matches!(a.proxy(&b.identity.node_id, Method::GET, "/node/hello", None).await, Err(MeshError::Revoked(_))));
        // Gossip: b hears its own revocation from a's hello.
        b.absorb_hello(&a.hello().unwrap(), None).unwrap();
        assert!(!b.lock().membership.as_ref().unwrap().is_active(&b.identity.node_id));

        std::fs::remove_dir_all(&dir_a).unwrap();
        std::fs::remove_dir_all(&dir_b).unwrap();
    }

    #[tokio::test]
    async fn peer_snapshots_age_and_drop_after_a_day_unreachable() {
        // No swarm: snapshots from anyone are kept, so the store can be exercised directly.
        let (a, dir) = node("gibson", "127.0.0.1:1");
        let now = Utc::now();
        a.peer_for_test("fresh", snapshot_with_record("fresh", "fresh", "x"), now - chrono::Duration::seconds(5), None);
        a.peer_for_test(
            "stale",
            snapshot_with_record("stale", "stale", "y"),
            now - chrono::Duration::hours(30),
            Some(now - chrono::Duration::hours(23)),
        );
        a.peer_for_test(
            "gone",
            snapshot_with_record("gone", "gone", "z"),
            now - chrono::Duration::hours(30),
            Some(now - chrono::Duration::hours(25)),
        );
        let swarm = a.swarm_snapshot();
        let by_state = |state| swarm.records.iter().filter(|r| r.peer_state == state).count();
        assert_eq!(by_state(PeerState::Reachable), 1);
        assert_eq!(by_state(PeerState::Unreachable), 1);
        assert_eq!(swarm.records.len(), 2);
        let fresh = swarm.records.iter().find(|r| r.record.node_id == "fresh").unwrap();
        assert!((4_900..8_000).contains(&fresh.peer_age_ms), "{}", fresh.peer_age_ms);
        let stale = swarm.records.iter().find(|r| r.record.node_id == "stale").unwrap();
        assert!(stale.peer_age_ms >= 30 * 3_600_000);
        assert!(a.owner_of("gone:42:7").is_none());
        assert_eq!(a.owner_of("fresh:42:7").as_deref(), Some("fresh"));

        // Local roster changes and stored snapshots both bump `changed`.
        let mut changed = a.changed();
        let before = *changed.borrow_and_update();
        a.store_snapshot(snapshot_with_record("fresh", "fresh", "x"));
        assert!(*changed.borrow_and_update() > before);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn invites_are_single_use_and_expire() {
        let (a, dir) = node("gibson", "127.0.0.1:1");
        let token = a.invite_for(INVITE_TTL_SECS).unwrap();
        let invite = parse_invite(&token).unwrap();
        assert_eq!(invite.admitter, a.identity.node_id);
        assert_eq!(invite.admitter_address.as_deref(), Some("127.0.0.1:1"));
        assert!(invite.expires_at > Mesh::now_secs() + INVITE_TTL_SECS - 5);
        let joiner = Identity::from_key(SigningKey::generate(&mut rand::rngs::OsRng));
        let hello = |id: &Identity| {
            Hello {
                node_id: id.node_id.clone(),
                name: "wintermute".into(),
                public_key: id.public_hex(),
                version: "0".into(),
                swarm_id: Some(invite.swarm_id.clone()),
                address: Some("127.0.0.1:2".into()),
                capabilities: Capabilities::default(),
                members: vec![],
                signed_at: now_ms(),
                signature: String::new(),
            }
            .sign(id)
            .unwrap()
        };
        let answer = a.admit(JoinRequest { invite: token.clone(), hello: hello(&joiner) }).unwrap();
        assert_eq!(answer.members.len(), 2);
        assert_eq!(unseal(&joiner, &answer.swarm_key_sealed).unwrap(), a.swarm_key().unwrap());
        // Burnt.
        assert!(matches!(a.admit(JoinRequest { invite: token, hello: hello(&joiner) }), Err(MeshError::Unauthorized(_))));
        // Expired: mint one, then age it past the hour.
        let token = a.invite_for(INVITE_TTL_SECS).unwrap();
        let expired = parse_invite(&token).unwrap();
        a.lock().nonces.insert(expired.nonce.clone(), 0);
        let stale = mint_invite(&Invite { expires_at: Mesh::now_secs() - 1, ..expired }, &a.swarm_key().unwrap());
        assert!(matches!(a.admit(JoinRequest { invite: stale, hello: hello(&joiner) }), Err(MeshError::Unauthorized(_))));
        // A hello whose signature is not the joiner's is refused.
        let token = a.invite_for(INVITE_TTL_SECS).unwrap();
        let mut forged = hello(&joiner);
        forged.name = "root".into();
        assert!(matches!(a.admit(JoinRequest { invite: token, hello: forged }), Err(MeshError::Unauthorized(_))));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
