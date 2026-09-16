//! rosterd-tray: the roster in the menu bar, macOS and Linux. A native menu over the CLI:
//! `rosterd watch --json` feeds it one snapshot per change, `rosterd allow|deny|open|ui` act.
//! Nothing here touches the socket, the config or the token; the CLI already does.

use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rosterd_proto::{Activity, Liveness, Record, Snapshot, age};
use tao::event::{Event, StartCause};
use tao::event_loop::{ControlFlow, EventLoopBuilder, EventLoopProxy};
use tray_icon::menu::{IconMenuItem, Menu, MenuEvent, MenuItem, PredefinedMenuItem, Submenu};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

/// The one name the menu bar uses; the daemon, the CLI and the page are the same word.
const APP: &str = "rosterd";
/// Menu sections, indexed by `rank`.
const SECTIONS: [&str; 5] = ["Needs attention", "Active", "Idle", "Unknown", "Suspended"];

enum UserEvent {
    View(View),
    Menu(MenuEvent),
    /// A menu started or stopped tracking the mouse (macOS).
    Tracking(bool),
}

enum View {
    Waiting,
    Roster(Snapshot),
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

/// `rosterd watch --json` forever: every line is a whole snapshot; when it ends, say why and retry.
fn follow(rosterd: &PathBuf, feed: &EventLoopProxy<UserEvent>) {
    loop {
        let child = Command::new(rosterd).args(["watch", "--json"]).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn();
        let mut child = match child {
            Ok(c) => c,
            Err(e) => {
                let _ = feed.send_event(UserEvent::View(View::Down(format!("cannot run {}: {e}", rosterd.display()))));
                thread::sleep(Duration::from_secs(5));
                continue;
            }
        };
        for line in BufReader::new(child.stdout.take().unwrap()).lines().map_while(Result::ok) {
            if let Ok(snapshot) = serde_json::from_str::<Snapshot>(&line) {
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
            let mut rows: Vec<&Record> = s.records.iter().filter(|r| r.liveness != Liveness::Ended).collect();
            rows.sort_by_key(|r| (rank(r), r.name.clone().unwrap_or_default(), r.pid));
            let attention = rows.iter().filter(|r| rank(r) == 0).count();
            let summary = match (rows.len(), attention) {
                (0, _) => "no sessions".to_string(),
                (n, 0) => format!("{n} session{}", plural(n)),
                (n, a) => format!("{n} session{} · {a} need{} attention", plural(n), if a == 1 { "s" } else { "" }),
            };
            if rows.is_empty() {
                let _ = menu.append(&MenuItem::with_id("summary", &summary, false, None));
            }
            for (k, title) in SECTIONS.iter().enumerate() {
                let group: Vec<&&Record> = rows.iter().filter(|r| usize::from(rank(r)) == k).collect();
                if group.is_empty() {
                    continue;
                }
                let _ = menu.append(&PredefinedMenuItem::section_header(&format!("{title} · {}", group.len())));
                for r in group {
                    let text = row_text(r, now);
                    let key = r.session_key.as_str();
                    if k == 0 {
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
                        let (shade, hollow) = match k {
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
            } else if rows.iter().any(|r| r.activity == Activity::Active) {
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

/// The section a row belongs to, in menu order.
fn rank(r: &Record) -> u8 {
    match (r.liveness, r.activity) {
        (_, Activity::NeedsAttention) => 0,
        (Liveness::Suspended, _) => 4,
        (_, Activity::Active) => 1,
        (_, Activity::Idle) => 2,
        (_, Activity::Unknown) => 3,
    }
}

/// `label  harness  age`: the display name (the name, else the cwd's last segment in brackets,
/// else where the session sits; never a pid), then how long since the last activity, or since
/// the start when nothing was heard.
fn row_text(r: &Record, now: i64) -> String {
    let base = r.cwd.as_deref().and_then(|cwd| std::path::Path::new(cwd).file_name()).and_then(|f| f.to_str());
    let label = match (r.name.as_deref().filter(|n| !n.is_empty()), base) {
        (Some(name), _) => name.to_string(),
        (None, Some(base)) => format!("[{base}]"),
        (None, None) => r.origin.clone().unwrap_or_default(),
    };
    let since = r.activity_at.unwrap_or(r.started_at).timestamp();
    let stale = if r.liveness == Liveness::Stale { " · stale" } else { "" };
    let cpu = r.load.map(|l| format!("{}%  ", l.cpu_pct)).unwrap_or_default();
    format!("{label}  {}  {cpu}{}{stale}", r.harness, age((now - since).max(0) as u64)).trim_start().to_string()
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

    fn rec(pid: u32, name: Option<&str>, activity: &str, liveness: &str) -> Record {
        serde_json::from_value(serde_json::json!({
            "node": "n", "node_id": "abc", "session_key": format!("abc:{pid}:1"), "pid": pid, "start_ticks": 1,
            "started_at": "2026-09-16T00:00:00Z", "harness": "claude", "lane": "interactive",
            "name": name, "activity": activity, "liveness": liveness, "cwd": if pid == 2 { Some("/home/x/api") } else { None },
        }))
        .unwrap()
    }

    #[test]
    fn rows_sort_attention_first_and_ids_are_cli_lines() {
        let mut rows = [rec(1, Some("docs"), "idle", "live"), rec(2, None, "active", "live"), rec(3, Some("fix"), "needs_attention", "live"), rec(4, Some("old"), "active", "suspended")];
        rows.sort_by_key(|r| (rank(r), r.name.clone().unwrap_or_default(), r.pid));
        let now = rows[0].started_at.timestamp() + 7200;
        let texts: Vec<String> = rows.iter().map(|r| row_text(r, now)).collect();
        assert_eq!(texts, ["fix  claude  2h", "[api]  claude  2h", "docs  claude  2h", "old  claude  2h"]);
        assert_eq!(rows.iter().map(|r| SECTIONS[usize::from(rank(r))]).collect::<Vec<_>>(), ["Needs attention", "Active", "Idle", "Suspended"]);
        assert_eq!(cli_args("always:abc:3:1"), Some(vec!["allow", "abc:3:1", "--always"]));
        assert_eq!(cli_args("open:abc:3:1"), Some(vec!["open", "abc:3:1"]));
        assert_eq!(cli_args("ui"), Some(vec!["ui"]));
        assert_eq!(cli_args("summary"), None);
    }
}
