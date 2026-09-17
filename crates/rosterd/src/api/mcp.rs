//! The MCP server of R6 at /mcp on the socket and loopback listeners, over rmcp's streamable
//! HTTP transport. Stateless: every request gets a fresh handler, and the caller is identified
//! per request from the `X-Rosterd-Session` header the runner sets in the harness MCP config,
//! else from the socket peer pid (R6: "identified by its PID from the socket peer credentials").
//! rmcp injects the HTTP `Parts` into the request context, and axum's `ConnectInfo<Peer>` rides
//! in those parts' extensions, so no extra plumbing is needed.

use std::sync::Arc;

use axum::extract::ConnectInfo;
use axum::http::{Method, StatusCode, request::Parts};
use rmcp::ErrorData as McpError;
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rosterd_proto::Source;
use serde_json::{Value, json};

use super::Peer;
use super::routes::{ApiError, SpawnBody, key_for_pid, remote_owner, session_state, spawn_child};
use crate::node::{Node, VERSION};
use crate::runner::PromptRequest;

/// Names the calling session; the runner sets it in every harness's MCP config, R6.
pub const SESSION_HEADER: &str = "x-rosterd-session";

pub fn service(node: Arc<Node>) -> StreamableHttpService<RosterMcp, LocalSessionManager> {
    // The socket mode and the loopback bearer are the gates, not the Host header (a socket
    // client sends any host it likes).
    let mut config = StreamableHttpServerConfig::default().disable_allowed_hosts();
    config.legacy_session_mode = false;
    config.json_response = true;
    StreamableHttpService::new(move || Ok(RosterMcp { node: node.clone() }), Arc::new(LocalSessionManager::default()), config)
}

pub struct RosterMcp {
    node: Arc<Node>,
}

fn tool(name: &'static str, description: &'static str, schema: Value) -> Tool {
    let Value::Object(schema) = schema else { unreachable!("tool schemas are objects") };
    Tool::new(name, description, schema)
}

fn tools() -> Vec<Tool> {
    let scope = json!({
        "type": "object",
        "properties": { "scope": { "type": "string", "enum": ["node", "swarm"], "default": "node" } }
    });
    vec![
        tool("roster.list", "Every coding agent session on this node (scope node) or on every node of the swarm (scope swarm).", scope.clone()),
        tool("roster.watch", "The current roster; this transport has no push, so call again to refresh.", scope),
        tool(
            "session.name",
            "Set or clear the display name of the calling session.",
            json!({ "type": "object", "properties": { "name": { "type": ["string", "null"] } }, "required": ["name"] }),
        ),
        tool(
            "session.spawn",
            "Start a child session whose parent is the calling session. Returns session_key and the record.",
            json!({
                "type": "object",
                "properties": {
                    "harness": { "type": "string" },
                    "cwd": { "type": "string" },
                    "name": { "type": "string" },
                    "permission_policy": { "type": "string", "enum": ["auto", "attention"] },
                    "model": { "type": "string" }
                },
                "required": ["harness"]
            }),
        ),
        tool(
            "session.prompt",
            "Send a turn to a session, on any node. Optionally wait until idle, needs_attention or ended.",
            json!({
                "type": "object",
                "properties": {
                    "session_key": { "type": "string" },
                    "prompt": { "type": "string" },
                    "wait_until": { "type": "string", "enum": ["idle", "needs_attention", "ended"] },
                    "timeout_ms": { "type": "integer" }
                },
                "required": ["session_key", "prompt"]
            }),
        ),
        tool(
            "session.read_state",
            "Activity, pending permission requests and the last recap of a session; never the transcript.",
            json!({ "type": "object", "properties": { "session_key": { "type": "string" } }, "required": ["session_key"] }),
        ),
        tool("swarm.nodes", "Swarm membership with health.", json!({ "type": "object", "properties": {} })),
    ]
}

impl ServerHandler for RosterMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("rosterd", VERSION))
            .with_instructions("The roster of coding agent sessions on this machine and its swarm. Name yourself, spawn children, prompt and read peers.")
    }

    async fn list_tools(&self, _: Option<PaginatedRequestParams>, _: RequestContext<RoleServer>) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(tools()))
    }

    async fn call_tool(&self, request: CallToolRequestParams, context: RequestContext<RoleServer>) -> Result<CallToolResponse, McpError> {
        let args = Value::Object(request.arguments.unwrap_or_default());
        let result = match request.name.as_ref() {
            "roster.list" => self.roster_list(&args),
            "roster.watch" => self.roster_list(&args).map(|snapshot| json!({ "snapshot": snapshot, "push": false })),
            "session.name" => self.session_name(&context, &args),
            "session.spawn" => self.session_spawn(&context, &args).await,
            "session.prompt" => self.session_prompt(&args).await,
            "session.read_state" => self.read_state(&args).await,
            "swarm.nodes" => Ok(json!({ "nodes": self.node.mesh.nodes() })),
            other => return Err(McpError::invalid_params(format!("unknown tool {other}"), None)),
        };
        Ok(match result {
            Ok(value) => CallToolResult::structured(value),
            Err(ApiError { status, message, .. }) => CallToolResult::error(vec![ContentBlock::text(format!("{status}: {message}"))]),
        }
        .into())
    }
}

fn arg<'a>(args: &'a Value, name: &str) -> Result<&'a str, ApiError> {
    args.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, format!("{name} is required")))
}

impl RosterMcp {
    /// The calling session: the header the runner sets, else the socket peer's process.
    fn caller(&self, context: &RequestContext<RoleServer>) -> Result<String, ApiError> {
        let parts = context
            .extensions
            .get::<Parts>()
            .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "no http parts on the mcp request"))?;
        if let Some(key) = parts.headers.get(SESSION_HEADER).and_then(|value| value.to_str().ok()) {
            return Ok(key.to_string());
        }
        let pid = parts
            .extensions
            .get::<ConnectInfo<Peer>>()
            .and_then(|ConnectInfo(peer)| peer.pid)
            .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "caller unknown: send X-Rosterd-Session or call over the socket"))?;
        key_for_pid(&self.node, pid)
    }

    /// A proxied action's answer, R7.5: the owner's body, or its `{error}` under its status.
    async fn relay(&self, owner: &str, method: Method, path: &str, body: Option<Value>) -> Result<Value, ApiError> {
        let (status, value) = self.node.mesh.proxy(owner, method, path, body).await?;
        if status.is_success() {
            return Ok(value);
        }
        let message = value.get("error").and_then(Value::as_str).unwrap_or("proxy failed").to_string();
        Err(ApiError::new(status, message))
    }

    fn roster_list(&self, args: &Value) -> Result<Value, ApiError> {
        match args.get("scope").and_then(Value::as_str).unwrap_or("node") {
            "node" => Ok(serde_json::to_value(self.node.roster.snapshot())?),
            "swarm" => Ok(serde_json::to_value(self.node.mesh.swarm_snapshot())?),
            other => Err(ApiError::new(StatusCode::BAD_REQUEST, format!("scope must be node or swarm, not {other}"))),
        }
    }

    fn session_name(&self, context: &RequestContext<RoleServer>, args: &Value) -> Result<Value, ApiError> {
        let key = self.caller(context)?;
        let name = args.get("name").and_then(Value::as_str).map(String::from);
        let record = self.node.roster.set_name(&key, name.clone(), Source::Hook)?;
        self.node.journal.action("name", Some(key), Some("local".into()), json!({ "name": name }));
        Ok(serde_json::to_value(record)?)
    }

    /// R5.4: a child session under the caller; the same operation as `POST /sessions/{key}/spawn`.
    async fn session_spawn(&self, context: &RequestContext<RoleServer>, args: &Value) -> Result<Value, ApiError> {
        let key = self.caller(context)?;
        let body: SpawnBody = serde_json::from_value(args.clone()).map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
        spawn_child(&self.node, &key, body, Some("local".into())).await
    }

    async fn session_prompt(&self, args: &Value) -> Result<Value, ApiError> {
        let key = arg(args, "session_key")?;
        if let Some(owner) = remote_owner(&self.node, None, key, false)? {
            let body = json!({ "prompt": args["prompt"], "wait_until": args["wait_until"], "timeout_ms": args["timeout_ms"] });
            return self.relay(&owner, Method::POST, &format!("/sessions/{key}/prompt"), Some(body)).await;
        }
        let request: PromptRequest = serde_json::from_value(args.clone()).map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.to_string()))?;
        let detail = json!({ "prompt": super::routes::brief(&request.prompt) });
        let outcome = self.node.runner.prompt(key, request).await?;
        self.node.journal.action("prompt", Some(key.to_string()), Some("local".into()), detail);
        Ok(serde_json::to_value(outcome)?)
    }

    /// Activity and the last recap, never the transcript, R6. A session this node does not
    /// drive (no holder) answers from its record alone.
    async fn read_state(&self, args: &Value) -> Result<Value, ApiError> {
        let key = arg(args, "session_key")?;
        if let Some(owner) = remote_owner(&self.node, None, key, false)? {
            let answer = self.relay(&owner, Method::GET, &format!("/sessions/{key}"), None).await?;
            return Ok(answer.get("state").cloned().unwrap_or(answer));
        }
        let record = self.node.roster.get(key).ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("no session {key}")))?;
        if let Some(state) = session_state(&self.node, &record) {
            return Ok(serde_json::to_value(state)?);
        }
        Ok(json!({
            "session_key": record.session_key,
            "activity": record.activity,
            "last_recap": null,
            "pending": [],
            "permission_policy": record.permission_policy.unwrap_or(self.node.config.runner.default_permission_policy),
        }))
    }
}
