//! The installer screen: a checklist above, a log below. It knows the rows only as `Step`s, so
//! any tool with a list of checks and fixes can use it. Rows run one at a time in a worker
//! thread while the screen keeps drawing; a row marked `foreground` (sudo) gets the terminal
//! back for the time it runs.
//!
//! Keys: up/down or j/k move, space picks a row, enter runs the picked rows (or the current
//! one), `a` picks every needed row, `r` checks everything again, q quits.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};

use super::{Ctx, State, Step, detail_of};

enum Status {
    Idle,
    Running,
    Failed(String),
}

struct Row {
    state: State,
    picked: bool,
    status: Status,
}

enum Mode {
    List,
    /// Asking for a step's inputs, one field at a time.
    Input { step: usize, values: Vec<String>, buf: String },
}

enum Msg {
    Line(String),
    Done(Result<(), String>),
}

struct App {
    title: String,
    ctx: Arc<Mutex<Ctx>>,
    steps: Vec<Step>,
    rows: Vec<Row>,
    cursor: usize,
    log: Vec<String>,
    mode: Mode,
    queue: VecDeque<(usize, Vec<String>)>,
    /// The row a worker is running, with its channel.
    running: Option<(usize, Receiver<Msg>)>,
    quit: bool,
}

pub fn run(title: &str, ctx: Ctx, steps: Vec<Step>) -> Result<bool> {
    let rows = steps.iter().map(|s| Row { state: (s.check)(&ctx), picked: false, status: Status::Idle }).collect();
    let mut app = App {
        title: title.to_string(),
        ctx: Arc::new(Mutex::new(ctx)),
        steps,
        rows,
        cursor: 0,
        log: vec!["space picks a row, enter runs it; a picks everything needed".into()],
        mode: Mode::List,
        queue: VecDeque::new(),
        running: None,
        quit: false,
    };
    let mut terminal = ratatui::init();
    let result = app.event_loop(&mut terminal);
    ratatui::restore();
    result?;
    Ok(app.rows.iter().zip(&app.steps).all(|(r, s)| s.optional || matches!(r.state, State::Done(_) | State::Unavailable(_))))
}

impl App {
    fn event_loop(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        while !self.quit {
            terminal.draw(|f| self.draw(f))?;
            self.drain();
            if self.running.is_none() {
                self.start_next(terminal)?;
            }
            if event::poll(Duration::from_millis(50))?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                self.key(key.code, key.modifiers);
            }
        }
        Ok(())
    }

    fn key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        if let Mode::Input { step, values, buf } = &mut self.mode {
            match code {
                KeyCode::Esc => self.mode = Mode::List,
                KeyCode::Enter => {
                    values.push(std::mem::take(buf));
                    if values.len() == self.steps[*step].inputs.len() {
                        let job = (*step, std::mem::take(values));
                        self.mode = Mode::List;
                        self.queue.push_back(job);
                    }
                }
                KeyCode::Backspace => {
                    buf.pop();
                }
                KeyCode::Char('u') if modifiers.contains(KeyModifiers::CONTROL) => buf.clear(),
                KeyCode::Char(c) => buf.push(c),
                _ => {}
            }
            return;
        }
        match code {
            KeyCode::Char('q') | KeyCode::Esc => {
                if self.running.is_some() {
                    self.log.push("a row is still running; wait for it".into());
                } else {
                    self.quit = true;
                }
            }
            KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => self.quit = true,
            KeyCode::Up | KeyCode::Char('k') => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => self.cursor = (self.cursor + 1).min(self.rows.len() - 1),
            KeyCode::Char(' ') => {
                if self.runnable(self.cursor) {
                    self.rows[self.cursor].picked = !self.rows[self.cursor].picked;
                }
            }
            KeyCode::Char('a') => {
                for i in 0..self.rows.len() {
                    self.rows[i].picked = self.runnable(i) && !self.steps[i].optional;
                }
            }
            KeyCode::Char('r') => self.recheck_all(),
            KeyCode::Enter => {
                let picked: Vec<usize> = (0..self.rows.len()).filter(|&i| self.rows[i].picked).collect();
                let targets = if picked.is_empty() { vec![self.cursor] } else { picked };
                for i in targets {
                    if !self.runnable(i) {
                        continue;
                    }
                    self.rows[i].picked = false;
                    if self.steps[i].inputs.is_empty() {
                        self.queue.push_back((i, Vec::new()));
                    } else {
                        // One input step at a time; the rest of the picks wait in the queue.
                        self.mode = Mode::Input { step: i, values: Vec::new(), buf: String::new() };
                        break;
                    }
                }
            }
            _ => {}
        }
    }

    fn runnable(&self, i: usize) -> bool {
        matches!(self.rows[i].state, State::Needed(_)) && !matches!(self.rows[i].status, Status::Running)
    }

    fn recheck_all(&mut self) {
        let mut ctx = self.ctx.lock().unwrap();
        ctx.reload();
        for (row, step) in self.rows.iter_mut().zip(&self.steps) {
            if !matches!(row.status, Status::Running) {
                row.state = (step.check)(&ctx);
            }
        }
    }

    /// Worker output into the log; a finished row is checked again.
    fn drain(&mut self) {
        let Some((i, rx)) = &self.running else { return };
        let i = *i;
        let mut finished = None;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                Msg::Line(line) => self.log.push(line),
                Msg::Done(result) => finished = Some(result),
            }
        }
        if let Some(result) = finished {
            self.running = None;
            self.finish(i, result);
        }
    }

    fn finish(&mut self, i: usize, result: Result<(), String>) {
        let mut ctx = self.ctx.lock().unwrap();
        ctx.reload();
        self.rows[i].state = (self.steps[i].check)(&ctx);
        drop(ctx);
        self.rows[i].status = match result {
            Ok(()) => {
                self.log.push(format!("{}: {}", self.steps[i].name, detail_of(&self.rows[i].state)));
                Status::Idle
            }
            Err(e) => {
                self.log.push(format!("{}: FAILED: {e}", self.steps[i].name));
                Status::Failed(e)
            }
        };
    }

    fn start_next(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        let Some((i, inputs)) = self.queue.pop_front() else { return Ok(()) };
        let step = &self.steps[i];
        self.rows[i].status = Status::Running;
        self.log.push(format!("{}: running", step.name));
        if step.foreground {
            // sudo needs the real terminal: leave the screen, run, come back.
            ratatui::restore();
            println!("\n{}: {}\n", step.name, detail_of(&self.rows[i].state));
            let mut log = |line: String| println!("{line}");
            let result = (step.apply)(&self.ctx.lock().unwrap(), &inputs, &mut log).map_err(|e| format!("{e:#}"));
            *terminal = ratatui::init();
            self.finish(i, result);
            return Ok(());
        }
        let (tx, rx): (Sender<Msg>, Receiver<Msg>) = channel();
        let ctx = Arc::clone(&self.ctx);
        let apply = step.apply;
        std::thread::spawn(move || {
            let ctx = ctx.lock().unwrap();
            let mut log = |line: String| {
                let _ = tx.send(Msg::Line(line));
            };
            let result = apply(&ctx, &inputs, &mut log).map_err(|e| format!("{e:#}"));
            let _ = tx.send(Msg::Done(result));
        });
        self.running = Some((i, rx));
        Ok(())
    }

    fn draw(&self, f: &mut Frame) {
        let [header, list, log] = Layout::vertical([Constraint::Length(1), Constraint::Min(8), Constraint::Length(9)]).areas(f.area());
        f.render_widget(
            Line::from(vec![
                Span::styled(format!(" {} ", self.title), Style::default().add_modifier(Modifier::BOLD)),
                Span::styled("  ↑↓ move  space pick  enter run  a all needed  r recheck  q quit", Style::default().fg(Color::DarkGray)),
            ]),
            header,
        );
        let width = self.steps.iter().map(|s| s.name.len()).max().unwrap_or(8);
        let items: Vec<ListItem> = self
            .rows
            .iter()
            .zip(&self.steps)
            .map(|(row, step)| {
                let (glyph, color) = match (&row.status, &row.state) {
                    (Status::Running, _) => ("…", Color::Cyan),
                    (Status::Failed(_), _) => ("✗", Color::Red),
                    (_, State::Done(_)) => ("✓", Color::Green),
                    (_, State::Needed(_)) => ("○", Color::Yellow),
                    (_, State::Unavailable(_)) => ("–", Color::DarkGray),
                };
                let pick = if row.picked { "[x]" } else { "[ ]" };
                let detail = match &row.status {
                    Status::Failed(e) => e.clone(),
                    _ => detail_of(&row.state).to_string(),
                };
                ListItem::new(Line::from(vec![
                    Span::raw(format!(" {pick} ")),
                    Span::styled(glyph, Style::default().fg(color)),
                    Span::raw(format!(" {:width$}  ", step.name)),
                    Span::styled(detail, Style::default().fg(if matches!(row.state, State::Unavailable(_)) { Color::DarkGray } else { Color::Reset })),
                ]))
            })
            .collect();
        let mut state = ListState::default().with_selected(Some(self.cursor));
        f.render_stateful_widget(List::new(items).highlight_style(Style::default().add_modifier(Modifier::REVERSED)), list, &mut state);

        let visible = log.height.saturating_sub(2) as usize;
        let start = self.log.len().saturating_sub(visible);
        let lines: Vec<Line> = self.log[start..].iter().map(|l| Line::from(l.as_str())).collect();
        f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }).block(Block::default().borders(Borders::TOP).title(" log ")), log);

        if let Mode::Input { step, values, buf } = &self.mode {
            let prompt = self.steps[*step].inputs[values.len()];
            let shown = if prompt.contains("token") { "•".repeat(buf.chars().count()) } else { buf.clone() };
            let area = centered(f.area(), 70, 5);
            f.render_widget(Clear, area);
            f.render_widget(
                Paragraph::new(vec![Line::from(format!("{prompt}:")), Line::from(format!("> {shown}▏"))])
                    .block(Block::default().borders(Borders::ALL).title(format!(" {} — enter next, esc cancel ", self.steps[*step].name))),
                area,
            );
        }
    }
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect { x: area.x + (area.width - width) / 2, y: area.y + (area.height - height) / 2, width, height }
}
