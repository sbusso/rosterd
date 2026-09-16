//! `rosterd setup`: the installer, a checklist on a terminal screen. Every row is checked the
//! way `doctor` checks it and fixed in place when picked: binaries on PATH, the config, the
//! service, the daemon, the three harnesses and their adapters, hooks and the pi extension, the
//! workspace credential, a swarm to join. The rows are data (`steps`); the screen (`tui`) knows
//! nothing about rosterd, so another tool can hand it its own rows.
//!
//! Without a terminal, or with `--yes`, the needed rows run in order and print one line each.

pub mod tui;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::config::{Config, config_dir};
use crate::doctor::{adapter_package, harness_package, socket_status};
use crate::integrate::{self, Paths, Target, which};

const HOOK: &str = include_str!("../../../../scripts/rosterd-hook");
const LAUNCH: &str = include_str!("../../../../scripts/rosterd-launch");
const OPEN: &str = include_str!("../../../../scripts/rosterd-open");
const SCRIPTS: [(&str, &str); 3] = [("rosterd-hook", HOOK), ("rosterd-launch", LAUNCH), ("rosterd-open", OPEN)];
#[cfg(target_os = "macos")]
const PLIST: &str = include_str!("../../../../packaging/com.rosterd.daemon.plist");
#[cfg(target_os = "linux")]
const UNIT: &str = include_str!("../../../../packaging/rosterd.service");

/// What every step reads. Resolved once from the environment, like `integrate::Paths`.
pub struct Ctx {
    pub config: Config,
    pub config_path: PathBuf,
    pub paths: Paths,
    pub home: PathBuf,
    /// `~/.local/bin`: binaries and scripts go there, the hook must be reachable by the harness.
    pub bin_dir: PathBuf,
    /// The binary running `setup`; it installs itself.
    pub exe: PathBuf,
}

impl Ctx {
    pub fn resolve(config: Config, config_path: &Path) -> Result<Ctx> {
        let home = dirs::home_dir().context("no home directory")?;
        Ok(Ctx {
            config,
            config_path: config_path.to_path_buf(),
            paths: Paths::resolve(),
            bin_dir: home.join(".local").join("bin"),
            home,
            exe: std::env::current_exe().context("current executable")?,
        })
    }

    fn reload(&mut self) {
        if let Ok(config) = Config::load(&self.config_path) {
            self.config = config;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Done(String),
    Needed(String),
    /// Cannot run here: no service manager, no package manager. The detail says what to do.
    Unavailable(String),
}

/// A log line sink for `apply`; the screen appends, the headless run prints.
pub type Log<'a> = &'a mut dyn FnMut(String);

pub struct Step {
    pub name: &'static str,
    /// Values asked for before `apply` runs, in order; empty for most steps.
    pub inputs: &'static [&'static str],
    /// Needs the real terminal while it runs (sudo asks for a password).
    pub foreground: bool,
    /// Not part of "run everything needed"; only when picked by hand.
    pub optional: bool,
    pub check: fn(&Ctx) -> State,
    pub apply: fn(&Ctx, &[String], Log) -> Result<()>,
}

/// The installer's rows, in the order they should run.
pub fn steps() -> Vec<Step> {
    let mut steps = vec![
        Step { name: "binaries", inputs: &[], foreground: false, optional: false, check: check_binaries, apply: apply_binaries },
        Step { name: "PATH", inputs: &[], foreground: false, optional: false, check: check_path, apply: apply_path },
        Step { name: "config", inputs: &[], foreground: false, optional: false, check: check_config, apply: apply_config },
        Step { name: "service", inputs: &[], foreground: cfg!(target_os = "macos"), optional: false, check: check_service, apply: apply_service },
        Step { name: "daemon", inputs: &[], foreground: false, optional: false, check: check_daemon, apply: apply_daemon },
    ];
    for target in Target::ALL {
        steps.push(Step {
            name: match target {
                Target::Claude => "claude",
                Target::Codex => "codex",
                Target::Pi => "pi",
            },
            inputs: &[],
            foreground: false,
            optional: false,
            check: match target {
                Target::Claude => |c| check_harness(c, Target::Claude),
                Target::Codex => |c| check_harness(c, Target::Codex),
                Target::Pi => |c| check_harness(c, Target::Pi),
            },
            apply: match target {
                Target::Claude => |c, _, log| apply_harness(c, Target::Claude, log),
                Target::Codex => |c, _, log| apply_harness(c, Target::Codex, log),
                Target::Pi => |c, _, log| apply_harness(c, Target::Pi, log),
            },
        });
        steps.push(Step {
            name: match target {
                Target::Claude => "adapter claude",
                Target::Codex => "adapter codex",
                Target::Pi => "adapter pi",
            },
            inputs: &[],
            foreground: false,
            optional: false,
            check: match target {
                Target::Claude => |c| check_adapter(c, Target::Claude),
                Target::Codex => |c| check_adapter(c, Target::Codex),
                Target::Pi => |c| check_adapter(c, Target::Pi),
            },
            apply: match target {
                Target::Claude => |c, _, log| apply_adapter(c, Target::Claude, log),
                Target::Codex => |c, _, log| apply_adapter(c, Target::Codex, log),
                Target::Pi => |c, _, log| apply_adapter(c, Target::Pi, log),
            },
        });
        steps.push(Step {
            name: match target {
                Target::Claude => "hooks claude",
                Target::Codex => "hooks codex",
                Target::Pi => "extension pi",
            },
            inputs: &[],
            foreground: false,
            optional: false,
            check: match target {
                Target::Claude => |c| check_integration(c, Target::Claude),
                Target::Codex => |c| check_integration(c, Target::Codex),
                Target::Pi => |c| check_integration(c, Target::Pi),
            },
            apply: match target {
                Target::Claude => |c, _, log| apply_integration(c, Target::Claude, log),
                Target::Codex => |c, _, log| apply_integration(c, Target::Codex, log),
                Target::Pi => |c, _, log| apply_integration(c, Target::Pi, log),
            },
        });
    }
    steps.push(Step { name: "tray", inputs: &[], foreground: false, optional: false, check: check_tray, apply: apply_tray });
    steps.push(Step {
        name: "workspace",
        inputs: &["workspace url", "workspace agent token"],
        foreground: false,
        optional: true,
        check: check_workspace,
        apply: apply_workspace,
    });
    steps.push(Step {
        name: "swarm",
        inputs: &["peer address (host:port)", "invite token"],
        foreground: false,
        optional: true,
        check: check_swarm,
        apply: apply_swarm,
    });
    steps
}

/// The whole installer. A terminal gets the screen; a pipe, or `yes`, runs the needed rows.
pub fn run(config: Config, config_path: &Path, yes: bool) -> Result<bool> {
    let ctx = Ctx::resolve(config, config_path)?;
    let steps = steps();
    if yes || !std::io::IsTerminal::is_terminal(&std::io::stdout()) {
        return headless(ctx, &steps);
    }
    tui::run("rosterd setup", ctx, steps)
}

/// Runs every needed, non optional row in order, one line each; false when one failed.
fn headless(mut ctx: Ctx, steps: &[Step]) -> Result<bool> {
    let mut all_ok = true;
    for step in steps {
        let state = (step.check)(&ctx);
        match state {
            State::Done(detail) => println!("ok    {}: {detail}", step.name),
            State::Unavailable(detail) => println!("skip  {}: {detail}", step.name),
            State::Needed(detail) if step.optional => println!("skip  {}: {detail} (pick it in the screen)", step.name),
            State::Needed(_) => {
                let mut log = |line: String| println!("      {line}");
                match (step.apply)(&ctx, &[], &mut log) {
                    Ok(()) => {
                        ctx.reload();
                        match (step.check)(&ctx) {
                            State::Done(detail) => println!("done  {}: {detail}", step.name),
                            other => {
                                all_ok = false;
                                println!("FAIL  {}: still {}", step.name, detail_of(&other));
                            }
                        }
                    }
                    Err(e) => {
                        all_ok = false;
                        println!("FAIL  {}: {e:#}", step.name);
                    }
                }
            }
        }
    }
    Ok(all_ok)
}

pub fn detail_of(state: &State) -> &str {
    match state {
        State::Done(d) | State::Needed(d) | State::Unavailable(d) => d,
    }
}

// Binaries and scripts, packaging/install.sh in Rust: the running binary copies itself.

fn check_binaries(ctx: &Ctx) -> State {
    // A package (brew, pacman) already put this binary and its four siblings on PATH.
    if let Some(found) = which(&ctx.paths.search_path, "rosterd")
        && found.canonicalize().ok() == ctx.exe.canonicalize().ok()
        && let Some(dir) = found.parent()
        && dir.join("rosterd-holder").is_file()
        && SCRIPTS.iter().all(|(name, body)| std::fs::read(dir.join(name)).ok().as_deref() == Some(body.as_bytes()))
    {
        return State::Done(format!("on PATH in {}", dir.display()));
    }
    let mut missing = Vec::new();
    if !same_bytes(&ctx.exe, &ctx.bin_dir.join("rosterd")) {
        missing.push("rosterd");
    }
    if !ctx.bin_dir.join("rosterd-holder").is_file() {
        missing.push("rosterd-holder");
    }
    for (name, body) in SCRIPTS {
        if std::fs::read(ctx.bin_dir.join(name)).ok().as_deref() != Some(body.as_bytes()) {
            missing.push(name);
        }
    }
    if missing.is_empty() {
        State::Done(format!("current in {}", ctx.bin_dir.display()))
    } else {
        State::Needed(format!("{} → {}", missing.join(", "), ctx.bin_dir.display()))
    }
}

fn apply_binaries(ctx: &Ctx, _: &[String], log: Log) -> Result<()> {
    std::fs::create_dir_all(&ctx.bin_dir)?;
    let holder = ctx.exe.with_file_name("rosterd-holder");
    if !holder.is_file() {
        bail!("rosterd-holder is not next to {} (run from dist/<target> or a built target dir)", ctx.exe.display());
    }
    install_file(&ctx.bin_dir.join("rosterd"), &std::fs::read(&ctx.exe)?)?;
    install_file(&ctx.bin_dir.join("rosterd-holder"), &std::fs::read(&holder)?)?;
    for (name, body) in SCRIPTS {
        install_file(&ctx.bin_dir.join(name), body.as_bytes())?;
    }
    // The tray is a desktop extra: the cross-built dist has none, a native build has one.
    let tray = ctx.exe.with_file_name("rosterd-tray");
    let with_tray = if tray.is_file() {
        install_file(&ctx.bin_dir.join("rosterd-tray"), &std::fs::read(&tray)?)?;
        ", rosterd-tray"
    } else {
        ""
    };
    log(format!("installed rosterd, rosterd-holder, rosterd-hook, rosterd-launch, rosterd-open{with_tray} into {}", ctx.bin_dir.display()));
    Ok(())
}

fn same_bytes(a: &Path, b: &Path) -> bool {
    match (std::fs::read(a), std::fs::read(b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

/// Executable, written whole then renamed, so a running copy never sees a half file.
fn install_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("rename to {}", path.display()))
}

const PATH_LINE: &str = "export PATH=\"$HOME/.local/bin:$PATH\"";

fn rc_exports_path(rc: &Path) -> bool {
    std::fs::read_to_string(rc).unwrap_or_default().lines().any(|l| l.trim() == PATH_LINE)
}

fn check_path(ctx: &Ctx) -> State {
    // What matters is that the harness finds the hook, wherever it was installed.
    let rc = rc_file(ctx);
    if let Some(hook) = which(&ctx.paths.search_path, "rosterd-hook") {
        State::Done(format!("{} is on PATH", hook.display()))
    } else if rc_exports_path(&rc) {
        // This shell predates the export; the next one has it.
        State::Done(format!("{} exports it; open a new shell", rc.display()))
    } else {
        State::Needed(format!("{} is not on PATH; add the export to {}", ctx.bin_dir.display(), rc.display()))
    }
}

fn apply_path(ctx: &Ctx, _: &[String], log: Log) -> Result<()> {
    let rc = rc_file(ctx);
    let line = PATH_LINE;
    let current = std::fs::read_to_string(&rc).unwrap_or_default();
    if rc_exports_path(&rc) {
        log(format!("{} already exports it; open a new shell", rc.display()));
        return Ok(());
    }
    let sep = if current.is_empty() || current.ends_with('\n') { "" } else { "\n" };
    std::fs::write(&rc, format!("{current}{sep}\n# rosterd: the hook must be reachable by the harness\n{line}\n"))?;
    log(format!("appended to {}; open a new shell", rc.display()));
    Ok(())
}

fn rc_file(ctx: &Ctx) -> PathBuf {
    let shell = std::env::var("SHELL").unwrap_or_default();
    ctx.home.join(if shell.ends_with("zsh") { ".zshrc" } else { ".bashrc" })
}

fn check_config(ctx: &Ctx) -> State {
    if ctx.config_path.is_file() {
        State::Done(ctx.config_path.display().to_string())
    } else {
        State::Needed(format!("write {}", ctx.config_path.display()))
    }
}

/// The same template packaging/install.sh writes, R10.
fn apply_config(ctx: &Ctx, _: &[String], log: Log) -> Result<()> {
    let dir = ctx.config_path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    if ctx.config_path.exists() {
        log("config already there, left as is".into());
        return Ok(());
    }
    let name = crate::config::hostname();
    let name = name.split('.').next().unwrap_or(&name);
    let text = format!(
        "# rosterd, R10. Pin the name: a renamed machine is a new name, the id stays.\n\
         [node]\nname = \"{name}\"\nport = 8791\nlisten = \"tailscale\"\nloopback_port = 8790\nui_listen = \"loopback\"\n\n\
         [swarm]\nstatic_peers = []\n\n\
         [workspace]\n# url = \"https://ws.example.ts.net\"\n# credential_file = \"{cred}\"\n\n\
         [runner]\ndefault_permission_policy = \"attention\"\nresume_on_crash = true\nrecap = true\nidle_timeout_s = 1800\n\n\
         [sources]\nfiles = false\nscan_interval_ms = 2000\n\n\
         [harness.claude]\nadapter = \"claude-agent-acp\"\n[harness.codex]\nadapter = \"codex-acp\"\n\
         [harness.pi]\nadapter = \"pi-acp\"\nextension = true\n",
        cred = dir.join("workspace.token").display()
    );
    crate::config::write_private(&ctx.config_path, text.as_bytes())?;
    log(format!("wrote {}", ctx.config_path.display()));
    Ok(())
}

// The service, R11: a LaunchDaemon on macOS (sudo, survives logout), a systemd user unit on Linux.

#[cfg(target_os = "macos")]
const LABEL: &str = "com.rosterd.daemon";

#[cfg(target_os = "macos")]
fn check_service(_: &Ctx) -> State {
    let loaded = Command::new("launchctl").args(["print", &format!("system/{LABEL}")]).stdout(Stdio::null()).stderr(Stdio::null()).status().map(|s| s.success()).unwrap_or(false);
    if loaded {
        State::Done(format!("LaunchDaemon {LABEL} loaded"))
    } else {
        State::Needed("install the LaunchDaemon (asks for sudo)".into())
    }
}

#[cfg(target_os = "macos")]
fn apply_service(ctx: &Ctx, _: &[String], log: Log) -> Result<()> {
    let user = std::env::var("USER").unwrap_or_default();
    let tmpdir = run_capture("getconf", &["DARWIN_USER_TEMP_DIR"]).unwrap_or_else(|| "/tmp/".into());
    let rendered = PLIST.replace("__USER__", &user).replace("__HOME__", &ctx.home.to_string_lossy()).replace("__TMPDIR__", tmpdir.trim());
    let path = ctx.config_path.with_file_name(format!("{LABEL}.plist"));
    std::fs::write(&path, rendered)?;
    let target = format!("/Library/LaunchDaemons/{LABEL}.plist");
    let script = format!(
        "sudo launchctl bootout system {target} 2>/dev/null; sudo install -m644 -o root -g wheel '{}' {target} && sudo launchctl bootstrap system {target}",
        path.display()
    );
    log(format!("$ {script}"));
    let status = Command::new("sh").args(["-c", &script]).status()?;
    if !status.success() {
        bail!("launchctl failed ({status}); the rendered plist is at {}", path.display());
    }
    log(format!("LaunchDaemon loaded (sudo launchctl print system/{LABEL})"));
    Ok(())
}

#[cfg(target_os = "linux")]
fn check_service(_: &Ctx) -> State {
    match run_capture("systemctl", &["--user", "is-enabled", "rosterd"]) {
        Some(out) if out.trim() == "enabled" => State::Done("rosterd.service enabled".into()),
        Some(_) => State::Needed("enable the systemd user unit".into()),
        None => State::Unavailable("no systemd --user; start with rosterd daemon".into()),
    }
}

#[cfg(target_os = "linux")]
fn apply_service(ctx: &Ctx, _: &[String], log: Log) -> Result<()> {
    // A package (pacman) ships the unit under /usr/lib; otherwise the embedded one goes to the user.
    if !Path::new("/usr/lib/systemd/user/rosterd.service").is_file() {
        let dir = std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from).unwrap_or_else(|| ctx.home.join(".config")).join("systemd").join("user");
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("rosterd.service"), UNIT)?;
    }
    for args in [&["--user", "daemon-reload"][..], &["--user", "enable", "--now", "rosterd"]] {
        let status = Command::new("systemctl").args(args).status()?;
        if !status.success() {
            bail!("systemctl {} failed ({status})", args.join(" "));
        }
    }
    log("rosterd.service enabled and started (systemctl --user status rosterd)".into());
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn check_service(_: &Ctx) -> State {
    State::Unavailable("no service file for this OS; start with rosterd daemon".into())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn apply_service(_: &Ctx, _: &[String], _: Log) -> Result<()> {
    bail!("no service file for this OS")
}

fn check_daemon(ctx: &Ctx) -> State {
    let socket = ctx.config.socket_path();
    match socket_status(&socket) {
        Ok(()) => State::Done(format!("answers on {}", socket.display())),
        Err(e) => State::Needed(format!("{e} on {}", socket.display())),
    }
}

/// The service starts it when there is one; otherwise a detached `rosterd daemon` in its own
/// process group, logging under the state directory, R2.
fn apply_daemon(ctx: &Ctx, _: &[String], log: Log) -> Result<()> {
    if let State::Done(_) = check_service(ctx) {
        #[cfg(target_os = "macos")]
        let _ = Command::new("sudo").args(["launchctl", "kickstart", "-k", &format!("system/{LABEL}")]).status();
        #[cfg(target_os = "linux")]
        let _ = Command::new("systemctl").args(["--user", "restart", "rosterd"]).status();
    } else {
        let state = crate::config::state_dir();
        std::fs::create_dir_all(&state)?;
        let out = std::fs::File::create(state.join("daemon.log"))?;
        let bin = if ctx.bin_dir.join("rosterd").is_file() { ctx.bin_dir.join("rosterd") } else { ctx.exe.clone() };
        let mut command = Command::new(&bin);
        command.arg("daemon").arg("--config").arg(&ctx.config_path).stdin(Stdio::null()).stdout(out.try_clone()?).stderr(out);
        #[cfg(unix)]
        std::os::unix::process::CommandExt::process_group(&mut command, 0);
        command.spawn().with_context(|| format!("spawn {}", bin.display()))?;
        log(format!("started {} daemon, log in {}", bin.display(), state.join("daemon.log").display()));
    }
    let socket = ctx.config.socket_path();
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        if socket_status(&socket).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    bail!("no answer on {} after 8 s", socket.display())
}

// The three harnesses and their adapters: npm packages, installed with bun when it is there.

fn check_harness(ctx: &Ctx, target: Target) -> State {
    match which(&ctx.paths.search_path, target.name()) {
        Some(bin) => State::Done(bin.display().to_string()),
        None => match package_manager(ctx) {
            Some(pm) => State::Needed(format!("{pm} {}", harness_package(target))),
            None => State::Unavailable(format!("no bun or npm on PATH to install {}", harness_package(target))),
        },
    }
}

fn apply_harness(ctx: &Ctx, target: Target, log: Log) -> Result<()> {
    install_package(ctx, harness_package(target), log)
}

fn check_adapter(ctx: &Ctx, target: Target) -> State {
    let adapter = integrate::adapter_name(target, &ctx.config);
    match which(&ctx.paths.search_path, &adapter) {
        Some(bin) => State::Done(bin.display().to_string()),
        None => {
            let commit = ctx.config.harness.get(target.name()).and_then(|h| h.adapter_commit.clone());
            let package = adapter_package(target, &adapter, commit.as_deref());
            match package_manager(ctx) {
                Some(pm) => State::Needed(format!("{pm} {package}")),
                None => State::Unavailable(format!("no bun or npm on PATH to install {package}")),
            }
        }
    }
}

fn apply_adapter(ctx: &Ctx, target: Target, log: Log) -> Result<()> {
    let adapter = integrate::adapter_name(target, &ctx.config);
    let commit = ctx.config.harness.get(target.name()).and_then(|h| h.adapter_commit.clone());
    install_package(ctx, &adapter_package(target, &adapter, commit.as_deref()), log)
}

/// `bun add -g` when bun is on PATH (the house rule), else `npm i -g`.
fn package_manager(ctx: &Ctx) -> Option<&'static str> {
    if which(&ctx.paths.search_path, "bun").is_some() {
        Some("bun add -g")
    } else if which(&ctx.paths.search_path, "npm").is_some() {
        Some("npm i -g")
    } else {
        None
    }
}

fn install_package(ctx: &Ctx, package: &str, log: Log) -> Result<()> {
    let pm = package_manager(ctx).context("no bun or npm on PATH")?;
    let command = format!("{pm} {package}");
    log(format!("$ {command}"));
    run_logged("sh", &["-c", &command], log)
}

fn check_integration(ctx: &Ctx, target: Target) -> State {
    let status = integrate::status_at(&ctx.config, &ctx.paths, false).into_iter().find(|s| s.harness == target.name());
    let what = if target == Target::Pi { "extension" } else { "hooks" };
    match status {
        Some(s) if s.current => State::Done(format!("{what} installed")),
        Some(s) if s.installed => State::Needed(format!("{what} installed, not current")),
        _ => State::Needed(format!("{what} not installed")),
    }
}

fn apply_integration(ctx: &Ctx, target: Target, log: Log) -> Result<()> {
    integrate::install_at(target, &ctx.config, &ctx.paths)?;
    log(format!("rosterd integrate install {}", target.name()));
    Ok(())
}

// The menu bar tray, started at login: a LaunchAgent on macOS, an autostart entry on Linux.

fn tray_bin(ctx: &Ctx) -> Option<PathBuf> {
    [ctx.bin_dir.join("rosterd-tray"), ctx.exe.with_file_name("rosterd-tray")].into_iter().find(|p| p.is_file()).or_else(|| which(&ctx.paths.search_path, "rosterd-tray"))
}

#[cfg(target_os = "macos")]
const TRAY_LABEL: &str = "com.rosterd.tray";

#[cfg(target_os = "macos")]
fn tray_domain() -> String {
    format!("gui/{}", run_capture("id", &["-u"]).unwrap_or_default().trim())
}

#[cfg(target_os = "macos")]
fn check_tray(ctx: &Ctx) -> State {
    let loaded = Command::new("launchctl").args(["print", &format!("{}/{TRAY_LABEL}", tray_domain())]).stdout(Stdio::null()).stderr(Stdio::null()).status().map(|s| s.success()).unwrap_or(false);
    match (loaded, tray_bin(ctx)) {
        (true, _) => State::Done(format!("LaunchAgent {TRAY_LABEL} loaded")),
        (false, Some(_)) => State::Needed("start the menu bar tray at login".into()),
        (false, None) => State::Unavailable("no rosterd-tray binary; a native build has one".into()),
    }
}

#[cfg(target_os = "macos")]
fn apply_tray(ctx: &Ctx, _: &[String], log: Log) -> Result<()> {
    let bin = tray_bin(ctx).context("no rosterd-tray binary")?;
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{TRAY_LABEL}</string>
  <key>ProgramArguments</key><array><string>{bin}</string></array>
  <key>EnvironmentVariables</key>
  <dict><key>PATH</key><string>{home}/.local/bin:{home}/.bun/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string></dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
</dict>
</plist>
"#,
        bin = bin.display(),
        home = ctx.home.display()
    );
    let dir = ctx.home.join("Library").join("LaunchAgents");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{TRAY_LABEL}.plist"));
    std::fs::write(&path, plist)?;
    let domain = tray_domain();
    let _ = Command::new("launchctl").args(["bootout", &format!("{domain}/{TRAY_LABEL}")]).stdout(Stdio::null()).stderr(Stdio::null()).status();
    let status = Command::new("launchctl").args(["bootstrap", &domain, &path.to_string_lossy()]).status()?;
    if !status.success() {
        bail!("launchctl bootstrap failed ({status}); the plist is at {}", path.display());
    }
    log(format!("tray running and at login ({})", path.display()));
    Ok(())
}

#[cfg(target_os = "linux")]
fn tray_desktop(ctx: &Ctx) -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from).unwrap_or_else(|| ctx.home.join(".config")).join("autostart").join("rosterd-tray.desktop")
}

#[cfg(target_os = "linux")]
fn check_tray(ctx: &Ctx) -> State {
    let display = std::env::var_os("DISPLAY").is_some() || std::env::var_os("WAYLAND_DISPLAY").is_some();
    match (tray_desktop(ctx).is_file(), tray_bin(ctx), display) {
        (true, _, _) => State::Done("autostart entry present".into()),
        (false, _, false) => State::Unavailable("no display; the tray is for a desktop".into()),
        (false, Some(_), true) => State::Needed("start the tray at login".into()),
        (false, None, true) => State::Unavailable("no rosterd-tray binary; a native build has one".into()),
    }
}

#[cfg(target_os = "linux")]
fn apply_tray(ctx: &Ctx, _: &[String], log: Log) -> Result<()> {
    let bin = tray_bin(ctx).context("no rosterd-tray binary")?;
    let path = tray_desktop(ctx);
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&path, format!("[Desktop Entry]\nType=Application\nName=rosterd tray\nExec={}\nX-GNOME-Autostart-enabled=true\n", bin.display()))?;
    if run_capture("pgrep", &["-x", "rosterd-tray"]).is_none() {
        let mut command = Command::new(&bin);
        command.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        std::os::unix::process::CommandExt::process_group(&mut command, 0);
        command.spawn()?;
    }
    log(format!("tray running and at login ({})", path.display()));
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn check_tray(_: &Ctx) -> State {
    State::Unavailable("no tray autostart for this OS".into())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn apply_tray(_: &Ctx, _: &[String], _: Log) -> Result<()> {
    bail!("no tray autostart for this OS")
}

// The workspace credential, R8, and a swarm to join, R7.3: the two rows that ask for input.

fn check_workspace(ctx: &Ctx) -> State {
    match &ctx.config.workspace.url {
        None => State::Needed("optional: url and agent token for the workspace bridge".into()),
        Some(url) => match std::fs::metadata(&ctx.config.workspace.credential_file) {
            Ok(m) if m.len() > 0 => State::Done(url.clone()),
            _ => State::Needed(format!("{url}, but {} is missing", ctx.config.workspace.credential_file.display())),
        },
    }
}

fn apply_workspace(ctx: &Ctx, inputs: &[String], log: Log) -> Result<()> {
    let [url, token] = inputs else { bail!("workspace needs a url and a token") };
    let url = url.trim();
    let token = token.trim();
    if !(url.starts_with("http://") || url.starts_with("https://")) || token.is_empty() {
        bail!("the url must start with http:// or https:// and the token must not be empty");
    }
    let credential = ctx.config.workspace.credential_file.clone();
    crate::config::write_private(&credential, format!("{token}\n").as_bytes())?;
    let mut doc = std::fs::read_to_string(&ctx.config_path).unwrap_or_default().parse::<toml_edit::DocumentMut>().context("parse rosterd.toml")?;
    if !doc.contains_table("workspace") {
        doc["workspace"] = toml_edit::table();
    }
    doc["workspace"]["url"] = toml_edit::value(url);
    doc["workspace"]["credential_file"] = toml_edit::value(credential.to_string_lossy().as_ref());
    crate::config::write_private(&ctx.config_path, doc.to_string().as_bytes())?;
    log(format!("workspace {url}, token in {}; restart the daemon to connect", credential.display()));
    Ok(())
}

fn check_swarm(_: &Ctx) -> State {
    let swarm = config_dir().join("swarm.json");
    if swarm.is_file() {
        State::Done("member of a swarm (rosterd nodes)".into())
    } else {
        State::Needed("optional: join a swarm with a peer address and an invite".into())
    }
}

fn apply_swarm(ctx: &Ctx, inputs: &[String], log: Log) -> Result<()> {
    let [peer, token] = inputs else { bail!("swarm needs a peer address and an invite token") };
    let bin = if ctx.bin_dir.join("rosterd").is_file() { ctx.bin_dir.join("rosterd") } else { ctx.exe.clone() };
    let bin = bin.to_string_lossy().into_owned();
    log(format!("$ rosterd join {peer} --token …"));
    run_logged(&bin, &["--config", &ctx.config_path.to_string_lossy(), "join", peer.trim(), "--token", token.trim()], log)
}

// Process helpers.

fn run_capture(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).stderr(Stdio::null()).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Runs to completion, every output line into the log, an error when it fails.
fn run_logged(program: &str, args: &[&str], log: Log) -> Result<()> {
    use std::io::{BufRead, BufReader};
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn {program}"))?;
    let stderr = child.stderr.take().map(|e| std::thread::spawn(move || BufReader::new(e).lines().map_while(Result::ok).collect::<Vec<_>>()));
    if let Some(stdout) = child.stdout.take() {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            log(line);
        }
    }
    if let Some(lines) = stderr.and_then(|t| t.join().ok()) {
        for line in lines {
            log(line);
        }
    }
    let status = child.wait()?;
    if !status.success() {
        bail!("{program} exited with {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(dir: &Path) -> Ctx {
        let home = dir.join("home");
        std::fs::create_dir_all(&home).unwrap();
        Ctx {
            config: Config::default(),
            config_path: dir.join("rosterd.toml"),
            paths: Paths {
                claude_settings: home.join(".claude/settings.json"),
                codex_hooks: home.join(".codex/hooks.json"),
                pi_extension: home.join(".pi/agent/extensions/rosterd-pi.ts"),
                lock: dir.join("adapters.lock"),
                search_path: std::ffi::OsString::from(""),
            },
            bin_dir: home.join(".local/bin"),
            home,
            exe: std::env::current_exe().unwrap(),
        }
    }

    /// The rows that write files: needed, then applied, then done, then applied again with no
    /// change, the way `integrate install` is byte idempotent.
    #[test]
    fn config_path_and_workspace_rows_apply_then_read_done() {
        let dir = std::env::temp_dir().join(format!("rosterd-setup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut ctx = ctx(&dir);
        let mut log = |_: String| {};

        assert!(matches!(check_config(&ctx), State::Needed(_)));
        apply_config(&ctx, &[], &mut log).unwrap();
        assert!(matches!(check_config(&ctx), State::Done(_)));
        let written = std::fs::read_to_string(&ctx.config_path).unwrap();
        apply_config(&ctx, &[], &mut log).unwrap();
        assert_eq!(std::fs::read_to_string(&ctx.config_path).unwrap(), written, "second apply changes nothing");

        assert!(matches!(check_path(&ctx), State::Needed(_)));
        apply_path(&ctx, &[], &mut log).unwrap();
        apply_path(&ctx, &[], &mut log).unwrap();
        let rc = std::fs::read_to_string(rc_file(&ctx)).unwrap();
        assert_eq!(rc.matches("export PATH=").count(), 1, "the export line is added once");
        ctx.paths.search_path = std::env::join_paths([ctx.bin_dir.clone()]).unwrap();
        assert!(matches!(check_path(&ctx), State::Done(_)));

        ctx.config.workspace.credential_file = dir.join("workspace.token");
        assert!(matches!(check_workspace(&ctx), State::Needed(_)));
        assert!(apply_workspace(&ctx, &["ftp://x".into(), "t".into()], &mut log).is_err());
        apply_workspace(&ctx, &["https://ws.example".into(), "tok\n".into()], &mut log).unwrap();
        ctx.reload();
        assert_eq!(ctx.config.workspace.url.as_deref(), Some("https://ws.example"));
        assert_eq!(std::fs::read_to_string(dir.join("workspace.token")).unwrap(), "tok\n");
        assert!(matches!(check_workspace(&ctx), State::Done(_)));
        assert!(std::fs::read_to_string(&ctx.config_path).unwrap().contains("# rosterd, R10"), "toml_edit kept the comments");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
