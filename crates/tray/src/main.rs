//! rosterd-tray: the roster in the menu bar, macOS and Linux, the page's view as a native menu
//! over the CLI: `rosterd watch --swarm --json` feeds it one swarm snapshot per change,
//! `rosterd changes --json` its attention events for the notifications, `rosterd allow|deny|open|ui` act.
//! Nothing here touches the socket, the config or the token; the CLI already does.

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rosterd_proto::{Activity, Change, Liveness, NodeHealth, PeerState, Record, SwarmRecord, SwarmSnapshot, age};
use tao::event::{Event, StartCause};
use tao::event_loop::{ControlFlow, EventLoopBuilder, EventLoopProxy};
use tray_icon::menu::{IconMenuItem, Menu, MenuEvent, MenuItem, PredefinedMenuItem, Submenu};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

/// The one name the menu bar uses; the daemon, the CLI and the page are the same word.
const APP: &str = "rosterd";

enum UserEvent {
    View(View),
    Menu(MenuEvent),
    /// A menu started or stopped tracking the mouse (macOS).
    Tracking(bool),
}

enum View {
    Waiting,
    Roster(SwarmSnapshot),
    /// The watch ended: the daemon is down or unreachable; the text is its last word.
    Down(String),
}

fn main() {
    let rosterd = rosterd_bin();
    #[allow(unused_mut)]
    let mut event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
    // A menu bar item only: no Dock icon, no app switcher entry.
    #[cfg(target_os = "macos")]
    {
        use tao::platform::macos::{ActivationPolicy, EventLoopExtMacOS};
        event_loop.set_activation_policy(ActivationPolicy::Accessory);
    }
    let proxy = event_loop.create_proxy();
    MenuEvent::set_event_handler(Some(move |event| {
        let _ = proxy.send_event(UserEvent::Menu(event));
    }));
    let feed = event_loop.create_proxy();
    let bin = rosterd.clone();
    thread::spawn(move || follow(&bin, &feed));
    let bin = rosterd.clone();
    thread::spawn(move || notify_attention(&bin));
    #[cfg(target_os = "macos")]
    watch_tracking(event_loop.create_proxy());

    let mut tray: Option<TrayIcon> = None;
    let mut view = View::Waiting;
    // Replacing the menu while it is open closes it (muda cancels tracking when the old NSMenu
    // drops), so a view that arrives then waits for the menu to close.
    let (mut open, mut pending) = (false, false);
    event_loop.run(move |event, _, flow| {
        *flow = ControlFlow::Wait;
        match event {
            // Created once the loop runs, tauri-apps/tray-icon#90.
            Event::NewEvents(StartCause::Init) => {
                let built = TrayIconBuilder::new().with_tooltip(APP).with_icon(dot(Shade::Idle)).with_icon_as_template(true).build();
                match built {
                    Ok(t) => {
                        tray = Some(t);
                        render(tray.as_ref().unwrap(), &view);
                    }
                    Err(e) => {
                        eprintln!("rosterd-tray: no tray icon: {e}");
                        *flow = ControlFlow::Exit;
                    }
                }
                #[cfg(target_os = "macos")]
                if let Some(rl) = objc2_core_foundation::CFRunLoop::main() {
                    rl.wake_up();
                }
            }
            Event::UserEvent(UserEvent::View(v)) => {
                view = v;
                pending = true;
            }
            Event::UserEvent(UserEvent::Tracking(tracking)) => open = tracking,
            Event::UserEvent(UserEvent::Menu(e)) => {
                if e.id.as_ref() == "quit" {
                    tray.take();
                    *flow = ControlFlow::Exit;
                } else {
                    act(&rosterd, e.id.as_ref());
                }
            }
            _ => {}
        }
        if let (true, false, Some(t)) = (pending, open, &tray) {
            render(t, &view);
            pending = false;
        }
    })
}

/// AppKit says when any menu starts and stops tracking the mouse.
#[cfg(target_os = "macos")]
fn watch_tracking(feed: EventLoopProxy<UserEvent>) {
    use objc2_app_kit::{NSMenuDidBeginTrackingNotification, NSMenuDidEndTrackingNotification};
    use objc2_foundation::{NSNotification, NSNotificationCenter};
    let center = NSNotificationCenter::defaultCenter();
    for (name, tracking) in [(unsafe { NSMenuDidBeginTrackingNotification }, true), (unsafe { NSMenuDidEndTrackingNotification }, false)] {
        let feed = feed.clone();
        let block = block2::RcBlock::new(move |_: std::ptr::NonNull<NSNotification>| {
            let _ = feed.send_event(UserEvent::Tracking(tracking));
        });
        // The center holds the observer; it lives as long as the process.
        std::mem::forget(unsafe { center.addObserverForName_object_queue_usingBlock(Some(name), None, None, &block) });
    }
}

/// The sibling `rosterd` of this binary, else the one on PATH.
fn rosterd_bin() -> PathBuf {
    std::env::current_exe().ok().map(|exe| exe.with_file_name("rosterd")).filter(|p| p.is_file()).unwrap_or_else(|| PathBuf::from("rosterd"))
}

/// `rosterd watch --swarm --json` forever: every line is a whole swarm snapshot; when it ends,
/// say why and retry.
fn follow(rosterd: &PathBuf, feed: &EventLoopProxy<UserEvent>) {
    loop {
        let child = Command::new(rosterd).args(["watch", "--swarm", "--json"]).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn();
        let mut child = match child {
            Ok(c) => c,
            Err(e) => {
                let _ = feed.send_event(UserEvent::View(View::Down(format!("cannot run {}: {e}", rosterd.display()))));
                thread::sleep(Duration::from_secs(5));
                continue;
            }
        };
        for line in BufReader::new(child.stdout.take().unwrap()).lines().map_while(Result::ok) {
            if let Ok(snapshot) = serde_json::from_str::<SwarmSnapshot>(&line) {
                let _ = feed.send_event(UserEvent::View(View::Roster(snapshot)));
            }
        }
        let mut err = String::new();
        let _ = child.stderr.take().unwrap().read_to_string(&mut err);
        let _ = child.wait();
        let why = err.lines().last().map(|l| l.trim_start_matches("rosterd: ").to_string()).unwrap_or_else(|| "watch ended".into());
        let _ = feed.send_event(UserEvent::View(View::Down(why)));
        thread::sleep(Duration::from_secs(2));
    }
}

/// `rosterd changes --json` forever: every `attention` event is a notification, R9, one per
/// claim (session_key and activity_seq). The snapshot frame and every other event are skipped.
fn notify_attention(rosterd: &PathBuf) {
    let mut seen: HashSet<(String, u64)> = HashSet::new();
    loop {
        let child = Command::new(rosterd).args(["changes", "--json"]).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn();
        let Ok(mut child) = child else {
            thread::sleep(Duration::from_secs(5));
            continue;
        };
        for line in BufReader::new(child.stdout.take().unwrap()).lines().map_while(Result::ok) {
            let Ok(Change::Attention { record, .. }) = serde_json::from_str::<Change>(&line) else { continue };
            let r = &record.record;
            if seen.len() > 4096 {
                seen.clear();
            }
            if seen.insert((r.session_key.clone(), r.activity_seq)) {
                notify(&format!("{} · {} · {}", label(r), r.node, r.activity_event.as_deref().unwrap_or("attention")));
            }
        }
        let _ = child.wait();
        thread::sleep(Duration::from_secs(2));
    }
}

/// A user notification titled `rosterd`: the tray is an unbundled binary, so the platform's
/// script runner posts it.
fn notify(text: &str) {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut c = Command::new("osascript");
        c.args(["-e", &applescript(text)]);
        c
    };
    #[cfg(not(target_os = "macos"))]
    let mut command = {
        let mut c = Command::new("notify-send");
        c.args([APP, text]);
        c
    };
    match command.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn() {
        Ok(mut child) => {
            thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Err(e) => eprintln!("rosterd-tray: notify: {e}"),
    }
}

/// `display notification` with the text as one AppleScript string literal.
fn applescript(text: &str) -> String {
    format!("display notification \"{}\" with title \"{APP}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

/// A menu id is the CLI line it stands for; the arguments are not shell parsed.
fn cli_args(id: &str) -> Option<Vec<&str>> {
    Some(match id.split_once(':') {
        None if id == "ui" => vec!["ui"],
        Some(("open", key)) => vec!["open", key],
        Some(("allow", key)) => vec!["allow", key],
        Some(("always", key)) => vec!["allow", key, "--always"],
        Some(("deny", key)) => vec!["deny", key],
        _ => return None,
    })
}

fn act(rosterd: &PathBuf, id: &str) {
    let Some(args) = cli_args(id) else { return };
    match Command::new(rosterd).args(&args).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit()).spawn() {
        Ok(mut child) => {
            thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Err(e) => eprintln!("rosterd-tray: {}: {e}", args.join(" ")),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Shade {
    Attention,
    Active,
    Idle,
    Down,
}

fn render(tray: &TrayIcon, view: &View) {
    let menu = Menu::new();
    let (summary, shade, attention) = match view {
        View::Waiting => ("connecting…".to_string(), Shade::Down, 0),
        View::Down(why) => (why.clone(), Shade::Down, 0),
        View::Roster(s) => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
            let live: Vec<&SwarmRecord> = s.records.iter().filter(|r| r.record.liveness != Liveness::Ended).collect();
            let attention = live.iter().filter(|r| r.record.activity == Activity::NeedsAttention).count();
            let summary = match (live.len(), attention) {
                (0, _) => "no sessions".to_string(),
                (n, 0) => format!("{n} session{}", plural(n)),
                (n, a) => format!("{n} session{} · {a} need{} attention", plural(n), if a == 1 { "s" } else { "" }),
            };
            let _ = menu.append(&MenuItem::with_id("summary", &summary, false, None));
            let mut nodes: Vec<&NodeHealth> = s.nodes.iter().collect();
            nodes.sort_by_key(|n| node_order(n.state));
            for node in nodes {
                let _ = menu.append(&PredefinedMenuItem::section_header(&format!("{} · {}", node.name.to_uppercase(), health(node))));
                if node.state == PeerState::Unreachable {
                    continue;
                }
                let mut rows: Vec<&&SwarmRecord> = live.iter().filter(|r| r.record.node_id == node.node_id).collect();
                rows.sort_by(|a, b| rank(&a.record).cmp(&rank(&b.record)).then_with(|| when(&b.record).cmp(&when(&a.record))));
                if rows.is_empty() {
                    let _ = menu.append(&MenuItem::with_id(format!("none:{}", node.node_id), "no sessions", false, None));
                }
                for r in rows {
                    let r = &r.record;
                    let text = row_text(r, now);
                    let key = r.session_key.as_str();
                    if r.activity == Activity::NeedsAttention && r.liveness == Liveness::Live && r.activity_event.as_deref() == Some("permission") {
                        let sub = Submenu::new(&text, true);
                        #[cfg(any(target_os = "macos", target_os = "windows"))]
                        sub.set_icon(Some(disc(Shade::Attention, false)));
                        let _ = sub.append_items(&[
                            &MenuItem::with_id(format!("allow:{key}"), "Allow", true, None),
                            &MenuItem::with_id(format!("always:{key}"), "Allow always", true, None),
                            &MenuItem::with_id(format!("deny:{key}"), "Deny", true, None),
                            &PredefinedMenuItem::separator(),
                            &MenuItem::with_id(format!("open:{key}"), "Open", true, None),
                        ]);
                        let _ = menu.append(&sub);
                    } else {
                        let (shade, hollow) = match rank(r) {
                            0 => (Shade::Attention, false),
                            1 => (Shade::Active, false),
                            2 => (Shade::Idle, false),
                            3 => (Shade::Idle, true),
                            _ => (Shade::Down, false),
                        };
                        let _ = menu.append(&IconMenuItem::with_id(format!("open:{key}"), &text, true, Some(disc(shade, hollow)), None));
                    }
                }
            }
            let shade = if attention > 0 {
                Shade::Attention
            } else if live.iter().any(|r| r.record.liveness == Liveness::Live && r.record.activity == Activity::Active) {
                Shade::Active
            } else {
                Shade::Idle
            };
            (summary, shade, attention)
        }
    };
    if !matches!(view, View::Roster(_)) {
        let _ = menu.append(&MenuItem::with_id("summary", &summary, false, None));
    }
    let _ = menu.append_items(&[
        &PredefinedMenuItem::separator(),
        &MenuItem::with_id("ui", "Open in browser", true, None),
        &MenuItem::with_id("quit", "Quit", true, None),
    ]);
    tray.set_menu(Some(Box::new(menu)));
    let _ = tray.set_tooltip(Some(format!("{APP} · {summary}")));
    // macOS: a template icon plus a count beside it; Linux: the colour is the signal.
    #[cfg(target_os = "macos")]
    {
        // `set_icon` drops the template flag (tray-icon 0.25 passes false), which paints black on black.
        let _ = tray.set_icon_with_as_template(Some(dot(if shade == Shade::Down { Shade::Down } else { Shade::Idle })), true);
        tray.set_title(if attention > 0 { Some(format!("⚠ {attention}")) } else { None });
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = attention;
        let _ = tray.set_icon(Some(dot(shade)));
    }
}

/// This node first, then the reachable peers, then the unreachable ones.
fn node_order(state: PeerState) -> u8 {
    match state {
        PeerState::Local => 0,
        PeerState::Reachable => 1,
        PeerState::Incompatible => 2,
        PeerState::Unreachable => 3,
    }
}

/// The page's node header: `this node`, `revoked`, or the state with when it was heard and its uptime.
fn health(node: &NodeHealth) -> String {
    let state = match node.state {
        _ if node.revoked => return "revoked".into(),
        PeerState::Local => return "this node".into(),
        PeerState::Reachable => "reachable",
        PeerState::Unreachable => "unreachable",
        PeerState::Incompatible => return format!("incompatible {}", node.version.as_deref().unwrap_or("-")),
    };
    let seen = node.seen_ms.map(|ms| format!(" {} ago", age(ms / 1000))).unwrap_or_default();
    let up = node.uptime_ms.map(|ms| format!(" · up {}", age(ms / 1000))).unwrap_or_default();
    format!("{state}{seen}{up}")
}

/// One word for a row, liveness first, as the page does: ended, suspended and stale say more
/// than the last activity did.
fn word(r: &Record) -> &'static str {
    match (r.liveness, r.activity) {
        (Liveness::Ended, _) => "ended",
        (Liveness::Suspended, _) => "suspended",
        (Liveness::Stale, _) => "stale",
        (_, Activity::NeedsAttention) => "needs attention",
        (_, Activity::Active) => "active",
        (_, Activity::Idle) => "idle",
        (_, Activity::Unknown) => "unknown",
    }
}

/// The page's RANK: the order of rows within a node.
fn rank(r: &Record) -> u8 {
    match word(r) {
        "needs attention" => 0,
        "active" => 1,
        "idle" => 2,
        "unknown" => 3,
        "ended" => 5,
        _ => 4,
    }
}

/// When the row last moved: the last activity, else the start.
fn when(r: &Record) -> i64 {
    r.activity_at.unwrap_or(r.started_at).timestamp()
}

/// `label  harness  word  age`, the page's row: the name and the project (the cwd's last
/// segment), else where the session sits; never a pid.
fn row_text(r: &Record, now: i64) -> String {
    format!("{}  {}  {}  {}", label(r), r.harness, word(r), age((now - when(r)).max(0) as u64))
}

/// The page's label: the name when set and the project beside it, else the project alone, else
/// the origin.
fn label(r: &Record) -> String {
    let project = r.cwd.as_deref().and_then(|cwd| std::path::Path::new(cwd).file_name()).and_then(|f| f.to_str()).unwrap_or_default();
    let name = r.name.as_deref().filter(|n| !n.is_empty()).map(str::to_string).unwrap_or_else(|| if project.is_empty() { r.origin.clone().unwrap_or_default() } else { String::new() });
    [name.as_str(), project].iter().filter(|p| !p.is_empty()).copied().collect::<Vec<_>>().join(" ")
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

fn rgb(shade: Shade) -> [u8; 3] {
    match shade {
        Shade::Attention => [255, 179, 0],
        Shade::Active => [76, 175, 80],
        Shade::Idle | Shade::Down => [136, 153, 170],
    }
}

/// A row's status dot: filled in the shade's colour, or a ring when nothing has been heard.
fn disc(shade: Shade, hollow: bool) -> tray_icon::menu::Icon {
    const SIZE: usize = 14;
    let rgb = rgb(shade);
    let dim = if shade == Shade::Down { 0.5 } else { 1.0 };
    let mut rgba = Vec::with_capacity(SIZE * SIZE * 4);
    for y in 0..SIZE {
        for x in 0..SIZE {
            let d = ((x as f32 + 0.5 - 7.0).powi(2) + (y as f32 + 0.5 - 7.0).powi(2)).sqrt();
            let d = if hollow { (d - 3.6).abs() - 1.0 } else { d - 4.5 };
            let a = (0.5 - d).clamp(0.0, 1.0) * dim;
            rgba.extend_from_slice(&[rgb[0], rgb[1], rgb[2], (a * 255.0) as u8]);
        }
    }
    tray_icon::menu::Icon::from_rgba(rgba, SIZE as u32, SIZE as u32).expect("icon buffer")
}

/// The roster glyph, three rows with a dot each: black as a template on macOS, the shade's colour
/// elsewhere; dimmed when the daemon is down. Drawn at 36 px for a crisp 18 pt on a 2x screen.
fn dot(shade: Shade) -> Icon {
    const SIZE: usize = 36;
    let rgb: [u8; 3] = if cfg!(target_os = "macos") { [0, 0, 0] } else { rgb(shade) };
    let rows = [9.0f32, 18.0, 27.0];
    let coverage = |x: f32, y: f32| -> f32 {
        let mut d = f32::MAX;
        for cy in rows {
            d = d.min(((x - 8.0).powi(2) + (y - cy).powi(2)).sqrt() - 2.6);
            let px = x.clamp(15.0, 30.0);
            d = d.min(((x - px).powi(2) + (y - cy).powi(2)).sqrt() - 1.7);
        }
        (0.5 - d).clamp(0.0, 1.0)
    };
    let dim = if shade == Shade::Down { 0.4 } else { 1.0 };
    let mut rgba = Vec::with_capacity(SIZE * SIZE * 4);
    for y in 0..SIZE {
        for x in 0..SIZE {
            let a = coverage(x as f32 + 0.5, y as f32 + 0.5) * dim;
            rgba.extend_from_slice(&[rgb[0], rgb[1], rgb[2], (a * 255.0) as u8]);
        }
    }
    Icon::from_rgba(rgba, SIZE as u32, SIZE as u32).expect("icon buffer")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(pid: u32, name: Option<&str>, activity: &str, liveness: &str, cwd: Option<&str>, origin: Option<&str>) -> Record {
        serde_json::from_value(serde_json::json!({
            "node": "n", "node_id": "abc", "session_key": format!("abc:{pid}:1"), "pid": pid, "start_ticks": 1,
            "started_at": "2026-09-16T00:00:00Z", "harness": "claude", "lane": "interactive",
            "name": name, "activity": activity, "liveness": liveness, "cwd": cwd, "origin": origin,
        }))
        .unwrap()
    }

    #[test]
    fn rows_are_the_pages_rows_and_ids_are_cli_lines() {
        let mut rows = [
            rec(1, Some("docs"), "idle", "live", None, None),
            rec(2, None, "active", "live", Some("/home/x/api"), None),
            rec(3, Some("fix"), "needs_attention", "live", Some("/home/x/api"), None),
            rec(4, Some("old"), "active", "suspended", None, None),
            rec(5, None, "unknown", "live", None, Some("ssh")),
        ];
        rows.sort_by_key(rank);
        let now = rows[0].started_at.timestamp() + 7200;
        let texts: Vec<String> = rows.iter().map(|r| row_text(r, now)).collect();
        assert_eq!(texts, ["fix api  claude  needs attention  2h", "api  claude  active  2h", "docs  claude  idle  2h", "ssh  claude  unknown  2h", "old  claude  suspended  2h"]);
        assert_eq!(cli_args("always:abc:3:1"), Some(vec!["allow", "abc:3:1", "--always"]));
        assert_eq!(cli_args("open:abc:3:1"), Some(vec!["open", "abc:3:1"]));
        assert_eq!(cli_args("ui"), Some(vec!["ui"]));
        assert_eq!(cli_args("summary"), None);
        assert_eq!(applescript(r#"fix api · n · say "hi" \ bye"#), r#"display notification "fix api · n · say \"hi\" \\ bye" with title "rosterd""#);
    }

    #[test]
    fn node_headers_read_like_the_page() {
        let node = |state: PeerState, seen: Option<u64>, up: Option<u64>| NodeHealth {
            node_id: "n".into(), name: "mato".into(), address: None, state, peer_age_ms: 0, seen_ms: seen, uptime_ms: up,
            version: None, capabilities: Default::default(), revoked: false,
        };
        assert_eq!(health(&node(PeerState::Local, Some(0), Some(4 * 3_600_000))), "this node");
        assert_eq!(health(&node(PeerState::Reachable, Some(1500), Some(4 * 3_600_000))), "reachable 1s ago · up 4h");
        assert_eq!(health(&node(PeerState::Unreachable, None, None)), "unreachable");
        let mut states = [PeerState::Unreachable, PeerState::Local, PeerState::Reachable];
        states.sort_by_key(|s| node_order(*s));
        assert_eq!(states, [PeerState::Local, PeerState::Reachable, PeerState::Unreachable]);
    }
}
