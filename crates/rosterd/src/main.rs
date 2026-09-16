//! rosterd: one daemon per machine that knows every coding agent session on it, drives headless
//! sessions over ACP, joins a swarm of peers over Tailscale, and reports to the workspace.
//! One binary serves both the daemon (`rosterd daemon`) and the CLI of R14.

mod api;
mod bridge;
mod cli;
mod config;
mod doctor;
mod gate;
mod identity;
mod integrate;
mod mesh;
mod node;
mod roster;
mod runner;
mod scanner;
mod setup;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use rosterd_proto::Capabilities;

use crate::cli::Command;
use crate::config::{Config, config_dir, config_path, state_dir, write_private};
use crate::identity::Identity;
use crate::node::{Node, VERSION};

/// The roster daemon and its command line, R14.
#[derive(Parser, Debug)]
#[command(name = "rosterd", version = VERSION)]
pub struct Cli {
    /// Config file; defaults to <config dir>/rosterd.toml.
    #[arg(long, global = true, env = "ROSTERD_CONFIG")]
    config: Option<PathBuf>,
    /// Print the API frame for the resource instead of a table, R14.2.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        // A bad invocation is a user error, R14.2: exit 1, not clap's 2. Help and version stay 0.
        Err(error) => {
            let _ = error.print();
            return if error.use_stderr() { ExitCode::from(1) } else { ExitCode::SUCCESS };
        }
    };
    let Some(command) = cli.command else {
        // R14: no subcommand prints help.
        let _ = Cli::command().print_help();
        return ExitCode::SUCCESS;
    };
    let config_path = cli.config.unwrap_or_else(config_path);
    if matches!(command, Command::Daemon) {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
            .init();
        return match serve(config_path).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("rosterd: {error:#}");
                ExitCode::FAILURE
            }
        };
    }
    match cli::run(command, &config_path, cli.json).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(exit) => {
            eprintln!("rosterd: {}", exit.message);
            ExitCode::from(exit.code)
        }
    }
}

async fn serve(config_path: PathBuf) -> Result<()> {
    let config = Arc::new(Config::load(&config_path)?);
    let identity = Identity::load_or_create(&config_dir())?;
    tracing::info!(node = %config.node.name, node_id = %identity.node_id, version = VERSION, "rosterd starting");

    let capabilities = Capabilities {
        harnesses: config.harness.keys().cloned().collect(),
        files_enabled: config.sources.files,
        ..Capabilities::default()
    };
    let roster = roster::Roster::new(&config.node.name, &identity.node_id, capabilities);
    // R8, acceptance 2: claim sequences continue across a restart, or the workspace refuses them.
    roster.persist_seqs(state_dir().join("seqs.json"));
    let mesh = mesh::Mesh::new(config.clone(), identity.clone(), roster.clone(), VERSION)?;
    let bridge = bridge::Bridge::new(config.clone(), roster.clone())?;
    let runner = runner::Runner::new(config.clone(), roster.clone(), bridge.clone(), mesh.clone());
    let node = Arc::new(Node {
        config: config.clone(),
        identity,
        roster: roster.clone(),
        mesh: mesh.clone(),
        bridge: bridge.clone(),
        runner: runner.clone(),
        loopback_token: loopback_token()?,
    });

    tokio::spawn(scanner::run(config.clone(), roster.clone()));
    tokio::spawn(mesh.clone().run());
    tokio::spawn(bridge.clone().run());
    if let Err(error) = runner.recover().await {
        tracing::error!(%error, "holder recovery failed; the roster still answers, R2");
    }
    api::serve(node).await
}

/// The loopback bearer token, R6: created once, mode 0600, read by local clients.
fn loopback_token() -> Result<String> {
    let path = config_dir().join("loopback.token");
    if let Ok(token) = std::fs::read_to_string(&path) {
        let token = token.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }
    let token = ulid::Ulid::new().to_string().to_lowercase() + &ulid::Ulid::new().to_string().to_lowercase();
    write_private(&path, token.as_bytes())?;
    Ok(token)
}
