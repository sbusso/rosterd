//! Everything a request handler can reach, built once in `serve`.

use std::sync::Arc;

use crate::config::Config;
use crate::hooks::Hooks;
use crate::identity::Identity;
use crate::journal::Journal;
use crate::mesh::Mesh;
use crate::roster::Roster;
use crate::runner::Runner;
use crate::usage::Roots;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct Node {
    pub config: Arc<Config>,
    pub identity: Identity,
    pub roster: Arc<Roster>,
    pub mesh: Arc<Mesh>,
    pub runner: Arc<Runner>,
    /// The append-only log of R18; the writer task is spawned beside `mesh.run()`.
    pub journal: Arc<Journal>,
    /// The webhooks of R19, at `<state dir>/hooks.json`; the deliverer runs beside the journal writer.
    pub hooks: Arc<Hooks>,
    /// The loopback bearer token, R6. Stored at `<config dir>/loopback.token`, mode 0600.
    pub loopback_token: String,
    /// Where the harnesses keep their transcripts, for GET /usage.
    pub usage_roots: Roots,
}
