//! `rosterd tui`: the roster as a screen arranged around the human's attention. What waits on
//! you first, an answer away; then every work piece in the swarm; enter opens one, its stream
//! and a line to steer it. The API is the only thing it talks to, so what it can do a script
//! can do.
//!
//! Keys on the board: up/down or j/k move, enter opens, 1-9 pick a pending request's option,
//! y allows, a allows always, n denies or declines, c cancels the turn, z suspends, r resumes,
//! s stops, q quits. In a piece letters type: the line is a prompt, or the answer when a
//! question pending takes one text field; esc closes. Only with a request pending and the line
//! empty do 1-9, y, a and n answer it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use futures::{SinkExt, StreamExt};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table, TableState};
use ratatui::{DefaultTerminal, Frame};
use reqwest::Method;
use rosterd_proto::{Activity, Lane, Liveness, PeerState, Record, SwarmSnapshot, age};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use super::client::{parse, socket_path};
use super::resolve::display_name;
use super::{Client, Out};
use crate::config::Config;
use crate::runner::acp;

/// One session with what it waits on, from /swarm/snapshot plus GET /sessions/{key} when it
/// needs attention.
#[derive(Clone)]
struct Piece {
    record: Record,
    pending: Vec<Value>,
    questions: Vec<Value>,
}

enum Msg {
    Key(KeyCode, KeyModifiers),
    Redraw,
    Roster(Vec<Piece>),
    /// One rendered line of a piece's stream.
    Line(String, String),
    /// A piece's stream ended, with why when it failed.
    Closed(String, Option<String>),
    Note(String),
}

/// A piece open on the screen: its stream so far and the line under it.
struct Open {
    key: String,
    lines: Vec<String>,
    input: String,
    task: tokio::task::JoinHandle<()>,
}

struct App {
    client: Client,
    socket: PathBuf,
    tx: mpsc::UnboundedSender<Msg>,
    pieces: Vec<Piece>,
    cursor: usize,
    open: Option<Open>,
    note: Option<String>,
    quit: bool,
}

pub async fn run(client: Client, config: &Config) -> Out<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut app = App { client: client.clone(), socket: socket_path(config), tx: tx.clone(), pieces: Vec::new(), cursor: 0, open: None, note: None, quit: false };
    let poll = tokio::spawn(poll(client, tx.clone()));
    let keys = tx.clone();
    std::thread::spawn(move || {
        while let Ok(event) = event::read() {
            let msg = match event {
                Event::Key(key) if key.kind == KeyEventKind::Press => Msg::Key(key.code, key.modifiers),
                Event::Resize(..) => Msg::Redraw,
                _ => continue,
            };
            if keys.send(msg).is_err() {
                break;
            }
        }
    });
    let mut terminal = ratatui::init();
    let result = app.event_loop(&mut terminal, &mut rx).await;
    ratatui::restore();
    poll.abort();
    if let Some(open) = app.open.take() {
        open.task.abort();
    }
    result
}

/// /swarm/snapshot every second, and the pending requests of every session that waits.
async fn poll(client: Client, tx: mpsc::UnboundedSender<Msg>) {
    loop {
        match snapshot(&client).await {
            Ok(pieces) => {
                if tx.send(Msg::Roster(pieces)).is_err() {
                    return;
                }
            }
            Err(exit) => {
                let _ = tx.send(Msg::Note(exit.message));
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn snapshot(client: &Client) -> Out<Vec<Piece>> {
    let swarm: SwarmSnapshot = parse(&client.call(Method::GET, "/swarm/snapshot", None).await?)?;
    let local = swarm.nodes.iter().find(|n| n.state == PeerState::Local).map(|n| n.node_id.clone()).unwrap_or_default();
    let mut pieces = Vec::new();
    for row in swarm.records {
        let record = row.record;
        if record.liveness == Liveness::Ended {
            continue;
        }
        let (mut pending, mut questions) = (Vec::new(), Vec::new());
        if record.activity == Activity::NeedsAttention && record.lane == Lane::Headless {
            let path = if record.node_id == local { format!("/sessions/{}", record.session_key) } else { format!("/swarm/{}/sessions/{}", record.node_id, record.session_key) };
            if let Ok(body) = client.call(Method::GET, &path, None).await
                && let Ok(session) = serde_json::from_str::<Value>(&body)
            {
                pending = session["state"]["pending"].as_array().cloned().unwrap_or_default();
                questions = session["state"]["questions"].as_array().cloned().unwrap_or_default();
            }
        }
        pieces.push(Piece { record, pending, questions });
    }
    pieces.sort_by_key(|p| (rank(p), std::cmp::Reverse(p.record.activity_at.unwrap_or(p.record.started_at))));
    Ok(pieces)
}

/// Waiting first, then working, then the rest; suspended last.
fn rank(piece: &Piece) -> u8 {
    match (piece.record.liveness, piece.record.activity) {
        (Liveness::Suspended, _) => 4,
        (_, Activity::NeedsAttention) => 0,
        (_, Activity::Active) => 1,
        (_, Activity::Idle) => 2,
        _ => 3,
    }
}

fn state(piece: &Piece) -> String {
    if piece.record.liveness == Liveness::Suspended {
        return "suspended".into();
    }
    if let Some(p) = piece.pending.first() {
        return format!("permission {} {}", p["tool"].as_str().unwrap_or("-"), p["summary"].as_str().unwrap_or_default()).trim_end().to_string();
    }
    if let Some(q) = piece.questions.first() {
        return format!("question {}", q["message"].as_str().unwrap_or("-"));
    }
    let activity = serde_json::to_value(piece.record.activity).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default();
    match piece.record.activity_event.as_deref().filter(|e| !e.is_empty()) {
        Some(event) if piece.record.activity != Activity::Idle => format!("{activity} {event}"),
        _ => activity,
    }
}

/// The options of the first pending request, numbered as the keys pick them.
fn options(piece: &Piece) -> Vec<(usize, String)> {
    if let Some(p) = piece.pending.first() {
        return p["options"].as_array().into_iter().flatten().enumerate().map(|(i, o)| (i + 1, o["name"].as_str().unwrap_or("-").to_string())).collect();
    }
    if !piece.questions.is_empty() {
        return vec![(1, "accept".into()), (2, "decline".into())];
    }
    Vec::new()
}

/// The one text field a question takes, when its schema is that simple.
fn text_field(question: &Value) -> Option<String> {
    let props = question["schema"]["properties"].as_object()?;
    let mut strings = props.iter().filter(|(_, v)| v["type"] == "string");
    let (name, _) = strings.next()?;
    strings.next().is_none().then(|| name.clone())
}

impl App {
    async fn event_loop(&mut self, terminal: &mut DefaultTerminal, rx: &mut mpsc::UnboundedReceiver<Msg>) -> Out<()> {
        terminal.draw(|f| self.draw(f))?;
        while !self.quit {
            let Some(msg) = rx.recv().await else { break };
            self.handle(msg);
            // Everything queued behind it, then one draw.
            while let Ok(msg) = rx.try_recv() {
                self.handle(msg);
            }
            terminal.draw(|f| self.draw(f))?;
        }
        Ok(())
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Key(code, modifiers) => self.key(code, modifiers),
            Msg::Redraw => {}
            Msg::Roster(pieces) => {
                let key = self.selected().map(|p| p.record.session_key.clone());
                self.pieces = pieces;
                self.cursor = key.and_then(|k| self.pieces.iter().position(|p| p.record.session_key == k)).unwrap_or(0).min(self.pieces.len().saturating_sub(1));
            }
            Msg::Line(key, line) => {
                if let Some(open) = self.open.as_mut().filter(|o| o.key == key) {
                    append(&mut open.lines, &line);
                }
            }
            Msg::Closed(key, error) => {
                if let Some(open) = self.open.as_mut().filter(|o| o.key == key) {
                    open.lines.push(error.map(|e| format!("stream: {e}")).unwrap_or_else(|| "stream closed".into()));
                }
            }
            Msg::Note(note) => self.note = Some(note),
        }
    }

    fn selected(&self) -> Option<&Piece> {
        match &self.open {
            Some(open) => self.pieces.iter().find(|p| p.record.session_key == open.key),
            None => self.pieces.get(self.cursor),
        }
    }

    fn key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        self.note = None;
        if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('c') {
            self.quit = true;
            return;
        }
        if self.open.is_some() {
            match code {
                KeyCode::Esc => self.close(),
                KeyCode::Enter => {
                    let text = std::mem::take(self.input());
                    if !text.is_empty() {
                        self.send_line(text);
                    }
                }
                KeyCode::Backspace => {
                    self.input().pop();
                }
                KeyCode::Char('u') if modifiers.contains(KeyModifiers::CONTROL) => self.input().clear(),
                // Letters type. Only an answer key on an empty line, with a request pending, acts.
                KeyCode::Char(c) if !(self.input().is_empty() && self.waiting() && matches!(c, '1'..='9' | 'y' | 'a' | 'n') && self.action(c)) => self.input().push(c),
                _ => {}
            }
            return;
        }
        match code {
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Down | KeyCode::Char('j') => self.cursor = (self.cursor + 1).min(self.pieces.len().saturating_sub(1)),
            KeyCode::Up | KeyCode::Char('k') => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Enter => self.open(),
            KeyCode::Char(c) => {
                self.action(c);
            }
            _ => {}
        }
    }

    fn waiting(&self) -> bool {
        self.selected().is_some_and(|p| !p.pending.is_empty() || !p.questions.is_empty())
    }

    fn input(&mut self) -> &mut String {
        &mut self.open.as_mut().expect("a piece is open").input
    }

    /// A one-key action on the selected piece; false when the key is none.
    fn action(&mut self, c: char) -> bool {
        let Some(piece) = self.selected().cloned() else { return false };
        let key = piece.record.session_key.clone();
        match c {
            'y' => self.answer(&piece, |o| o["kind"] == "allow_once" || o["option_id"] == "allow"),
            'a' => self.answer(&piece, |o| o["kind"] == "allow_always" || o["option_id"] == "allow_always"),
            'n' => self.answer(&piece, |o| o["kind"] == "reject_once" || o["option_id"] == "deny"),
            '1'..='9' => {
                let n = c as usize - '1' as usize;
                self.answer(&piece, move |o| o["_n"] == n);
            }
            'c' => self.post(&key, "/cancel", Method::POST, None, "cancelled"),
            'z' => self.post(&key, "/suspend", Method::POST, None, "suspended"),
            'r' => self.post(&key, "/resume", Method::POST, None, "resumed"),
            's' => self.post(&key, "", Method::DELETE, None, "stopped"),
            _ => return false,
        }
        true
    }

    /// Answers the first pending permission with the option `pick` accepts (options carry
    /// their index as `_n`), or the first question: option 1 accepts, 2 declines.
    fn answer(&mut self, piece: &Piece, pick: impl Fn(&Value) -> bool) {
        let key = &piece.record.session_key;
        if let Some(p) = piece.pending.first() {
            let mut options: Vec<Value> = p["options"].as_array().cloned().unwrap_or_default();
            for (i, o) in options.iter_mut().enumerate() {
                o["_n"] = json!(i);
            }
            let Some(option) = options.iter().find(|o| pick(o)) else {
                self.note = Some("no such option".into());
                return;
            };
            let body = json!({ "request_id": p["request_id"], "outcome": "selected", "option_id": option["option_id"] });
            let done = format!("{} {}", option["name"].as_str().unwrap_or("answered"), p["tool"].as_str().unwrap_or_default());
            self.post(key, "/permission", Method::POST, Some(body), &done);
        } else if let Some(q) = piece.questions.first() {
            let accept = pick(&json!({ "kind": "allow_once", "option_id": "allow", "_n": 0 }));
            let decline = pick(&json!({ "kind": "reject_once", "option_id": "deny", "_n": 1 }));
            if accept && text_field(q).is_some() {
                self.note = Some("type the answer on its line".into());
                return;
            }
            if accept || decline {
                let body = json!({ "request_id": q["request_id"], "action": if accept { "accept" } else { "decline" } });
                self.post(key, "/answer", Method::POST, Some(body), if accept { "accepted" } else { "declined" });
            }
        }
    }

    /// The line under an open piece: the answer to a pending one-field question, else a prompt.
    fn send_line(&mut self, text: String) {
        let Some(piece) = self.selected().cloned() else { return };
        let key = piece.record.session_key.clone();
        if let Some(field) = piece.questions.first().and_then(text_field) {
            let body = json!({ "request_id": piece.questions[0]["request_id"], "action": "accept", "content": { field: text } });
            self.post(&key, "/answer", Method::POST, Some(body), "answered");
            return;
        }
        if let Some(open) = self.open.as_mut() {
            append(&mut open.lines, &format!("> {text}"));
        }
        let client = self.client.clone();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let path = format!("/sessions/{key}/prompt");
            if let Err(exit) = client.call_with(Method::POST, &path, Some(json!({ "prompt": text })), Duration::from_secs(3600)).await {
                let _ = tx.send(Msg::Note(exit.message));
            }
        });
    }

    fn post(&mut self, key: &str, suffix: &str, method: Method, body: Option<Value>, done: &str) {
        let client = self.client.clone();
        let tx = self.tx.clone();
        let (path, done) = (format!("/sessions/{key}{suffix}"), done.to_string());
        tokio::spawn(async move {
            let note = match client.call(method, &path, body).await {
                Ok(_) => done,
                Err(exit) => exit.message,
            };
            let _ = tx.send(Msg::Note(note));
        });
    }

    /// The selected piece's stream over its ACP websocket: the history first, then live.
    fn open(&mut self) {
        let Some(piece) = self.pieces.get(self.cursor) else { return };
        if piece.record.lane != Lane::Headless {
            self.note = Some(format!("interactive: rosterd attach {}", piece.record.session_key));
            return;
        }
        let key = piece.record.session_key.clone();
        let task = tokio::spawn(stream(self.socket.clone(), key.clone(), self.tx.clone()));
        self.open = Some(Open { key, lines: Vec::new(), input: String::new(), task });
    }

    fn close(&mut self) {
        if let Some(open) = self.open.take() {
            open.task.abort();
        }
    }

    fn draw(&mut self, f: &mut Frame) {
        let area = f.area();
        let [main, foot] = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(area);
        match &self.open {
            Some(open) => self.draw_piece(f, main, open),
            None => self.draw_board(f, main),
        }
        if let Some(note) = &self.note {
            f.render_widget(Paragraph::new(note.as_str()).style(Style::default().fg(Color::Yellow)), foot);
        }
    }

    fn draw_board(&self, f: &mut Frame, area: Rect) {
        let now = Utc::now();
        let waiting = self.pieces.iter().filter(|p| !p.pending.is_empty() || !p.questions.is_empty()).count();
        let rows = self.pieces.iter().map(|p| {
            let r = &p.record;
            let project = r.cwd.as_deref().and_then(|c| std::path::Path::new(c).file_name()).and_then(|f| f.to_str()).unwrap_or("-");
            let when = r.activity_at.map(|at| age((now - at).num_seconds().max(0) as u64)).unwrap_or_else(|| "-".into());
            let mut what = state(p);
            let opts = options(p);
            if !opts.is_empty() {
                what = format!("{what}  {}", opts.iter().map(|(n, name)| format!("{n} {name}")).collect::<Vec<_>>().join("  "));
            }
            let style = match (r.liveness, r.activity) {
                (Liveness::Suspended, _) => Style::default().fg(Color::DarkGray),
                (_, Activity::NeedsAttention) => Style::default().fg(Color::Yellow),
                (_, Activity::Active) => Style::default().fg(Color::Green),
                _ => Style::default(),
            };
            Row::new(vec![display_name(r), project.to_string(), r.node.clone(), r.harness.clone(), when, what]).style(style)
        });
        let title = format!(" {waiting} waiting  {} pieces ", self.pieces.len());
        let table = Table::new(rows, [Constraint::Length(24), Constraint::Length(16), Constraint::Length(10), Constraint::Length(8), Constraint::Length(4), Constraint::Min(20)])
            .block(Block::default().borders(Borders::ALL).title(title))
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        let mut state = TableState::default().with_selected(Some(self.cursor));
        f.render_stateful_widget(table, area, &mut state);
    }

    fn draw_piece(&self, f: &mut Frame, area: Rect, open: &Open) {
        let piece = self.pieces.iter().find(|p| p.record.session_key == open.key);
        let [head, body, line] = Layout::vertical([Constraint::Length(2), Constraint::Min(1), Constraint::Length(1)]).areas(area);
        let mut header = vec![];
        if let Some(p) = piece {
            let r = &p.record;
            header.push(Line::from(vec![
                Span::styled(display_name(r), Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(format!("  {}  {}  {}", r.node, r.harness, r.cwd.as_deref().unwrap_or("-"))),
            ]));
            let mut what = state(p);
            let opts = options(p);
            if !opts.is_empty() {
                what = format!("{what}  {}", opts.iter().map(|(n, name)| format!("{n} {name}")).collect::<Vec<_>>().join("  "));
            }
            let style = if r.activity == Activity::NeedsAttention { Style::default().fg(Color::Yellow) } else { Style::default() };
            header.push(Line::from(Span::styled(what, style)));
        } else {
            header.push(Line::from(open.key.as_str()));
            header.push(Line::from("gone"));
        }
        f.render_widget(Paragraph::new(header), head);
        let width = body.width.max(1) as usize;
        let rows: Vec<Line> = open.lines.iter().flat_map(|l| wrap(l, width)).map(Line::from).collect();
        let skip = rows.len().saturating_sub(body.height as usize);
        f.render_widget(Paragraph::new(rows.into_iter().skip(skip).collect::<Vec<_>>()), body);
        let prompt = if piece.and_then(|p| p.questions.first()).and_then(text_field).is_some() { "? " } else { "> " };
        f.render_widget(Paragraph::new(format!("{prompt}{}", open.input)), line);
        f.set_cursor_position((line.x + (prompt.len() + open.input.chars().count()) as u16, line.y));
    }
}

/// Adds a rendered stream line. `\u{1}` marks an agent chunk: it continues the agent line
/// before it, and starts one after anything else.
fn append(lines: &mut Vec<String>, line: &str) {
    if let Some(chunk) = line.strip_prefix('\u{1}') {
        let agent = |l: &String| !["> ", "⚙ ", "? ", "· ", "plan "].iter().any(|m| l.starts_with(m));
        let joined = match lines.last() {
            Some(last) if agent(last) => format!("{}{chunk}", lines.pop().unwrap_or_default()),
            _ => chunk.to_string(),
        };
        lines.extend(joined.split('\n').map(str::to_string));
        return;
    }
    lines.extend(line.split('\n').map(str::to_string));
}

/// Hard-wrapped at `width` characters.
fn wrap(line: &str, width: usize) -> Vec<String> {
    if line.is_empty() {
        return vec![String::new()];
    }
    let chars: Vec<char> = line.chars().collect();
    chars.chunks(width.max(1)).map(|c| c.iter().collect()).collect()
}

/// One ACP update as a stream line: `\u{1}` prefixes an agent chunk, which continues the line
/// before it. None for what the screen does not show (thoughts, tool progress).
fn render_update(update: &Value) -> Option<String> {
    let text = || update["content"]["text"].as_str().unwrap_or_default().to_string();
    match update["sessionUpdate"].as_str()? {
        "agent_message_chunk" => Some(format!("\u{1}{}", text())),
        "user_message_chunk" => Some(format!("> {}", text())),
        "tool_call" => Some(format!("⚙ {}", update["title"].as_str().unwrap_or("tool"))),
        "plan" => {
            let entries = update["entries"].as_array().cloned().unwrap_or_default();
            let done = entries.iter().filter(|e| e["status"] == "completed").count();
            let now = entries.iter().find(|e| e["status"] == "in_progress").and_then(|e| e["content"].as_str()).unwrap_or_default();
            Some(format!("plan {done}/{}  {now}", entries.len()))
        }
        "session_info_update" => acp::title_of(update).map(|t| format!("· {t}")),
        _ => None,
    }
}

/// The piece's websocket: `_rosterd/history` first, then every update and request as it comes.
async fn stream(socket: PathBuf, key: String, tx: mpsc::UnboundedSender<Msg>) {
    let closed = |error: Option<String>| Msg::Closed(key.clone(), error);
    let unix = match tokio::net::UnixStream::connect(&socket).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(closed(Some(e.to_string())));
            return;
        }
    };
    let (ws, _) = match tokio_tungstenite::client_async(format!("ws://rosterd/sessions/{key}/acp"), unix).await {
        Ok(ws) => ws,
        Err(e) => {
            let _ = tx.send(closed(Some(e.to_string())));
            return;
        }
    };
    let (mut sink, mut source) = ws.split();
    let _ = sink.send(WsMessage::Text(json!({ "jsonrpc": "2.0", "id": 1, "method": "_rosterd/history", "params": {} }).to_string().into())).await;
    let mut seen: HashMap<String, ()> = HashMap::new();
    while let Some(Ok(WsMessage::Text(text))) = source.next().await {
        let v: Value = serde_json::from_str(text.as_str()).unwrap_or(Value::Null);
        let lines: Vec<String> = match acp::classify(&v) {
            Some(acp::Message::Response { id, result: Ok(result) }) if *id == json!(1) => result["updates"].as_array().into_iter().flatten().filter_map(render_update).collect(),
            Some(acp::Message::Notification { method: "session/update", params }) => render_update(&params["update"]).into_iter().collect(),
            Some(acp::Message::Request { id, method, params }) => {
                // A pending request replays at every connect; each shows once.
                if seen.insert(id.to_string(), ()).is_some() {
                    continue;
                }
                match method {
                    "session/request_permission" => vec![format!("? {}", params["toolCall"]["title"].as_str().unwrap_or("permission"))],
                    "elicitation/create" => vec![format!("? {}", params["message"].as_str().unwrap_or("question"))],
                    _ => Vec::new(),
                }
            }
            _ => Vec::new(),
        };
        for line in lines {
            if tx.send(Msg::Line(key.clone(), line)).is_err() {
                return;
            }
        }
    }
    let _ = tx.send(closed(None));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_continue_a_line_and_wrap_is_hard() {
        let mut lines = Vec::new();
        append(&mut lines, "> fix it");
        append(&mut lines, &render_update(&json!({ "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "On " } })).unwrap());
        append(&mut lines, &render_update(&json!({ "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "it.\nDone." } })).unwrap());
        append(&mut lines, &render_update(&json!({ "sessionUpdate": "tool_call", "title": "Bash" })).unwrap());
        append(&mut lines, &render_update(&json!({ "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "after" } })).unwrap());
        assert_eq!(lines, ["> fix it", "On it.", "Done.", "⚙ Bash", "after"]);
        assert_eq!(render_update(&json!({ "sessionUpdate": "agent_thought_chunk", "content": { "text": "hmm" } })), None);
        assert_eq!(wrap("abcdefg", 3), ["abc", "def", "g"]);
        assert_eq!(wrap("", 3), [""]);
    }

    #[test]
    fn a_piece_says_what_it_waits_on_and_which_keys_answer() {
        let record: Record = serde_json::from_value(json!({
            "node": "gibson", "node_id": "g", "session_key": "g:1:1", "pid": 1, "start_ticks": 1, "started_at": "2026-09-16T10:00:00Z",
            "harness": "claude", "lane": "headless", "activity": "needs_attention", "activity_event": "permission", "liveness": "live", "cwd": "/w/api",
        }))
        .unwrap();
        let piece = Piece {
            record,
            pending: vec![json!({ "request_id": 7, "tool": "bash", "summary": "rm -rf build", "options": [{ "option_id": "y", "name": "Yes", "kind": "allow_once" }, { "option_id": "n", "name": "No", "kind": "reject_once" }] })],
            questions: Vec::new(),
        };
        assert_eq!(state(&piece), "permission bash rm -rf build");
        assert_eq!(options(&piece), [(1, "Yes".to_string()), (2, "No".to_string())]);
        assert_eq!(rank(&piece), 0);
        let question = json!({ "schema": { "properties": { "branch": { "type": "string" } } } });
        assert_eq!(text_field(&question).as_deref(), Some("branch"));
        assert_eq!(text_field(&json!({ "schema": { "properties": { "a": { "type": "string" }, "b": { "type": "string" } } } })), None);
    }
}
