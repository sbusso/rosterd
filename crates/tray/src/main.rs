//! rosterd-tray: the roster in the menu bar, macOS and Linux. A native menu over the CLI:
//! `rosterd watch --json` feeds it one snapshot per change, `rosterd allow|deny|open|ui` act.
//! Nothing here touches the socket, the config or the token; the CLI already does.

use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use rosterd_proto::{Activity, Liveness, Record, Snapshot};
use tao::event::{Event, StartCause};
use tao::event_loop::{ControlFlow, EventLoopBuilder, EventLoopProxy};
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem, Submenu};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

enum UserEvent {
    Snapshot(Snapshot),
    /// The watch ended: the daemon is down or unreachable; the text is its last word.
    Down(String),
    Menu(MenuEvent),
}

enum View {
    Waiting,
    Roster(Snapshot),
    Down(String),
}

fn main() {
    let rosterd = rosterd_bin();
    let event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();
    let proxy = event_loop.create_proxy();
    MenuEvent::set_event_handler(Some(move |event| {
        let _ = proxy.send_event(UserEvent::Menu(event));
    }));
    let feed = event_loop.create_proxy();
    let bin = rosterd.clone();
    thread::spawn(move || follow(&bin, &feed));

    let mut tray: Option<TrayIcon> = None;
    let mut view = View::Waiting;
    event_loop.run(move |event, _, flow| {
        *flow = ControlFlow::Wait;
        match event {
            // Created once the loop runs, tauri-apps/tray-icon#90.
            Event::NewEvents(StartCause::Init) => {
                let built = TrayIconBuilder::new().with_tooltip("rosterd").with_icon(dot(Shade::Idle)).with_icon_as_template(true).build();
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
            Event::UserEvent(UserEvent::Snapshot(s)) => {
                view = View::Roster(s);
                if let Some(t) = &tray {
                    render(t, &view);
                }
            }
            Event::UserEvent(UserEvent::Down(m)) => {
                view = View::Down(m);
                if let Some(t) = &tray {
                    render(t, &view);
                }
            }
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
    })
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
                let _ = feed.send_event(UserEvent::Down(format!("cannot run {}: {e}", rosterd.display())));
                thread::sleep(Duration::from_secs(5));
                continue;
            }
        };
        for line in BufReader::new(child.stdout.take().unwrap()).lines().map_while(Result::ok) {
            if let Ok(snapshot) = serde_json::from_str::<Snapshot>(&line) {
                let _ = feed.send_event(UserEvent::Snapshot(snapshot));
            }
        }
        let mut err = String::new();
        let _ = child.stderr.take().unwrap().read_to_string(&mut err);
        let _ = child.wait();
        let why = err.lines().last().map(|l| l.trim_start_matches("rosterd: ").to_string()).unwrap_or_else(|| "watch ended".into());
        let _ = feed.send_event(UserEvent::Down(why));
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
        View::Waiting => ("connecting to rosterd…".to_string(), Shade::Down, 0),
        View::Down(why) => (format!("rosterd: {why}"), Shade::Down, 0),
        View::Roster(s) => {
            let mut rows: Vec<&Record> = s.records.iter().filter(|r| r.liveness != Liveness::Ended).collect();
            rows.sort_by_key(|r| (rank(r), r.name.clone().unwrap_or_default(), r.pid));
            let attention = rows.iter().filter(|r| r.activity == Activity::NeedsAttention).count();
            let summary = match (rows.len(), attention) {
                (0, _) => "no sessions".to_string(),
                (n, 0) => format!("{n} session{}", plural(n)),
                (n, a) => format!("{n} session{} · {a} need{} attention", plural(n), if a == 1 { "s" } else { "" }),
            };
            let shade = if attention > 0 {
                Shade::Attention
            } else if rows.iter().any(|r| r.activity == Activity::Active) {
                Shade::Active
            } else {
                Shade::Idle
            };
            let _ = menu.append(&MenuItem::with_id("summary", &summary, false, None));
            let _ = menu.append(&PredefinedMenuItem::separator());
            for r in rows {
                let text = row_text(r);
                let key = r.session_key.as_str();
                if r.activity == Activity::NeedsAttention {
                    let sub = Submenu::new(&text, true);
                    let _ = sub.append_items(&[
                        &MenuItem::with_id(format!("allow:{key}"), "Allow", true, None),
                        &MenuItem::with_id(format!("always:{key}"), "Allow always", true, None),
                        &MenuItem::with_id(format!("deny:{key}"), "Deny", true, None),
                        &PredefinedMenuItem::separator(),
                        &MenuItem::with_id(format!("open:{key}"), "Open", true, None),
                    ]);
                    let _ = menu.append(&sub);
                } else {
                    let _ = menu.append(&MenuItem::with_id(format!("open:{key}"), &text, true, None));
                }
            }
            (summary, shade, attention)
        }
    };
    if !matches!(view, View::Roster(_)) {
        let _ = menu.append(&MenuItem::with_id("summary", &summary, false, None));
    }
    let _ = menu.append_items(&[
        &PredefinedMenuItem::separator(),
        &MenuItem::with_id("ui", "Open rosterd", true, None),
        &MenuItem::with_id("quit", "Quit rosterd-tray", true, None),
    ]);
    tray.set_menu(Some(Box::new(menu)));
    let _ = tray.set_tooltip(Some(format!("rosterd · {summary}")));
    // macOS: a template icon plus a count beside it; Linux: the colour is the signal.
    #[cfg(target_os = "macos")]
    {
        let _ = tray.set_icon(Some(dot(if shade == Shade::Down { Shade::Down } else { Shade::Idle })));
        tray.set_title(if attention > 0 { Some(format!("⚠ {attention}")) } else { None });
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = attention;
        let _ = tray.set_icon(Some(dot(shade)));
    }
}

fn rank(r: &Record) -> u8 {
    match (r.liveness, r.activity) {
        (_, Activity::NeedsAttention) => 0,
        (Liveness::Suspended, _) => 4,
        (_, Activity::Active) => 1,
        (_, Activity::Idle) => 2,
        (_, Activity::Unknown) => 3,
    }
}

fn row_text(r: &Record) -> String {
    let glyph = match (r.liveness, r.activity) {
        (Liveness::Suspended, _) => "⏸",
        (_, Activity::NeedsAttention) => "⚠",
        (_, Activity::Active) => "●",
        (_, Activity::Idle) => "○",
        (_, Activity::Unknown) => "·",
    };
    // The CLI's display name, R14.1: the name, else the cwd's last segment in brackets.
    let label = match r.name.as_deref().filter(|n| !n.is_empty()) {
        Some(name) => name.to_string(),
        None => match r.cwd.as_deref().and_then(|cwd| std::path::Path::new(cwd).file_name()).and_then(|f| f.to_str()) {
            Some(base) => format!("[{base}]"),
            None => format!("pid {}", r.pid),
        },
    };
    let state = match (r.liveness, r.activity) {
        (Liveness::Suspended, _) => "suspended",
        (Liveness::Stale, _) => "stale",
        (_, Activity::NeedsAttention) => "needs attention",
        (_, Activity::Active) => "active",
        (_, Activity::Idle) => "idle",
        (_, Activity::Unknown) => "unknown",
    };
    format!("{glyph} {label}  {}  {state}", r.harness)
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// A filled circle; on macOS black and drawn as a template, elsewhere the colour of the shade.
fn dot(shade: Shade) -> Icon {
    let size = if cfg!(target_os = "macos") { 22 } else { 32 };
    let rgb: [u8; 3] = if cfg!(target_os = "macos") {
        [0, 0, 0]
    } else {
        match shade {
            Shade::Attention => [255, 179, 0],
            Shade::Active => [76, 175, 80],
            Shade::Idle => [136, 153, 170],
            Shade::Down => [85, 85, 85],
        }
    };
    let mut rgba = Vec::with_capacity(size * size * 4);
    let c = (size as f32 - 1.0) / 2.0;
    let r = size as f32 * 0.36;
    for y in 0..size {
        for x in 0..size {
            let d = ((x as f32 - c).powi(2) + (y as f32 - c).powi(2)).sqrt();
            // One pixel of anti-aliasing at the edge; a hollow ring when the daemon is down.
            let mut a = (r + 0.5 - d).clamp(0.0, 1.0);
            if shade == Shade::Down {
                a *= (d - (r - 2.0) + 0.5).clamp(0.0, 1.0);
            }
            rgba.extend_from_slice(&[rgb[0], rgb[1], rgb[2], (a * 255.0) as u8]);
        }
    }
    Icon::from_rgba(rgba, size as u32, size as u32).expect("icon buffer")
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
        let texts: Vec<String> = rows.iter().map(row_text).collect();
        assert_eq!(texts, ["⚠ fix  claude  needs attention", "● [api]  claude  active", "○ docs  claude  idle", "⏸ old  claude  suspended"]);
        assert_eq!(cli_args("always:abc:3:1"), Some(vec!["allow", "abc:3:1", "--always"]));
        assert_eq!(cli_args("open:abc:3:1"), Some(vec!["open", "abc:3:1"]));
        assert_eq!(cli_args("ui"), Some(vec!["ui"]));
        assert_eq!(cli_args("summary"), None);
    }
}
