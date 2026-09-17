//! Everything a request handler can reach, built once in `serve`.

use std::sync::Arc;

use crate::config::Config;
use crate::identity::Identity;
use crate::journal::Journal;
use crate::mesh::Mesh;
use crate::roster::Roster;
use crate::runner::Runner;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct Node {
    pub config: Arc<Config>,
    pub identity: Identity,
    pub roster: Arc<Roster>,
    pub mesh: Arc<Mesh>,
    pub runner: Arc<Runner>,
    /// The append-only log of R18; the writer task is spawned beside `mesh.run()`.
    pub journal: Arc<Journal>,
    /// The loopback bearer token, R6. Stored at `<config dir>/loopback.token`, mode 0600.
    pub loopback_token: String,
}
