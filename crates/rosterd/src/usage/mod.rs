//! Token and cost roll-up from the harnesses' own transcripts on this node, GET /usage. Only
//! the usage fields of each entry are read (model, token counts, timestamp), never the text;
//! nothing is collected or sent anywhere, a peer asks over the mesh and gets the roll-up.
//!
//! Claude Code writes one JSONL per session under `~/.claude/projects/<cwd>/`; assistant
//! entries carry `message.model`, `message.usage` and repeat while streaming, so one
//! `(message.id, requestId)` counts once. Codex writes one rollout JSONL per session under
//! `~/.codex/sessions/<y>/<m>/<d>/`; `turn_context` names the model and every `token_count`
//! event carries the running `total_token_usage`, so a turn is the delta to the previous one.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::SystemTime;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rosterd_proto::{DayUsage, NodeUsage, USAGE_SCHEMA};
use serde_json::Value;

/// USD per million tokens: input, output, cache read, cache write. Matched by the longest
/// family prefix (`gpt-5` covers `gpt-5-2025-08-07`, not `gpt-5.5`), so add a row for a new
/// model only when its price differs from its family.
/// ponytail: static list, updated by hand when a vendor changes a price; the upgrade path is a
/// prices file in the config dir, not a fetch.
const PRICES: &[(&str, f64, f64, f64, f64)] = &[
    ("claude-fable-5-1", 10.0, 50.0, 0.25, 12.5),
    ("claude-fable-5", 10.0, 50.0, 1.0, 12.5),
    ("claude-opus-5", 5.0, 25.0, 0.5, 6.25),
    ("claude-opus-4-8", 5.0, 25.0, 0.5, 6.25),
    ("claude-opus-4-7", 5.0, 25.0, 0.5, 6.25),
    ("claude-opus-4-6", 5.0, 25.0, 0.5, 6.25),
    ("claude-opus-4-5", 5.0, 25.0, 0.5, 6.25),
    ("claude-opus-4", 15.0, 75.0, 1.5, 18.75),
    ("claude-sonnet-5", 2.0, 10.0, 0.2, 2.5),
    ("claude-sonnet-4", 3.0, 15.0, 0.3, 3.75),
    ("claude-haiku-4-5", 1.0, 5.0, 0.1, 1.25),
    ("gpt-5-codex", 1.25, 10.0, 0.125, 0.0),
    ("gpt-5-mini", 0.25, 2.0, 0.025, 0.0),
    ("gpt-5-nano", 0.05, 0.4, 0.005, 0.0),
    ("gpt-5", 1.25, 10.0, 0.125, 0.0),
];

/// Where each harness keeps its transcripts. The daemon passes the real homes; a test a temp dir.
#[derive(Debug, Clone)]
pub struct Roots {
    pub claude: PathBuf,
    pub codex: PathBuf,
}

impl Roots {
    pub fn home() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        Roots { claude: home.join(".claude").join("projects"), codex: home.join(".codex").join("sessions") }
    }
}

/// One API response's worth of tokens.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub at: DateTime<Utc>,
    pub model: String,
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

struct Cached {
    mtime: SystemTime,
    size: u64,
    rows: Vec<Row>,
}

/// path → parsed rows, keyed on mtime and size so a repeated call re-reads only what changed.
/// ponytail: process-wide and never shrunk below the files inside the window; fine for one
/// home directory, a bounded LRU if a node ever holds tens of thousands of transcripts.
static CACHE: LazyLock<Mutex<HashMap<PathBuf, Cached>>> = LazyLock::new(Default::default);

/// `7d`, `12h`, a date, or an RFC 3339 datetime.
pub fn parse_since(text: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let text = text.trim();
    if let Some(days) = text.strip_suffix('d').and_then(|n| n.parse::<i64>().ok()) {
        return Ok(now - Duration::days(days));
    }
    if let Some(hours) = text.strip_suffix('h').and_then(|n| n.parse::<i64>().ok()) {
        return Ok(now - Duration::hours(hours));
    }
    if let Ok(at) = DateTime::parse_from_rfc3339(text) {
        return Ok(at.with_timezone(&Utc));
    }
    if let Ok(day) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Ok(day.and_hms_opt(0, 0, 0).expect("midnight").and_utc());
    }
    Err(format!("since must be Nd, Nh, YYYY-MM-DD or RFC 3339, not {text:?}"))
}

/// The roll-up of every transcript modified since `since`, per (day, harness, model), sorted.
/// Blocking file I/O: call it from `spawn_blocking`.
pub fn node_usage(roots: &Roots, node: &str, node_id: &str, since: DateTime<Utc>) -> NodeUsage {
    let files = [("claude", &roots.claude, parse_claude as fn(&str) -> Vec<Row>), ("codex", &roots.codex, parse_codex)]
        .into_iter()
        .flat_map(|(harness, root, parse)| jsonl_files(root).into_iter().map(move |path| (harness, path, parse)));
    let mut buckets: HashMap<(NaiveDate, &str, String), (DayUsage, HashSet<PathBuf>)> = HashMap::new();
    let mut cache = CACHE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    for (harness, path, parse) in files {
        let Ok(meta) = std::fs::metadata(&path) else { continue };
        let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if DateTime::<Utc>::from(mtime) < since {
            continue;
        }
        let fresh = cache.get(&path).is_some_and(|c| c.mtime == mtime && c.size == meta.len());
        if !fresh {
            let rows = std::fs::read_to_string(&path).map(|text| parse(&text)).unwrap_or_default();
            cache.insert(path.clone(), Cached { mtime, size: meta.len(), rows });
        }
        for row in cache[&path].rows.iter().filter(|row| row.at >= since) {
            let key = (row.at.date_naive(), harness, row.model.clone());
            let (bucket, sessions) = buckets.entry(key).or_insert_with(|| {
                let empty = DayUsage {
                    day: row.at.date_naive(),
                    harness: harness.into(),
                    model: row.model.clone(),
                    input_tokens: 0,
                    output_tokens: 0,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                    cost_usd: None,
                    sessions: 0,
                };
                (empty, HashSet::new())
            });
            bucket.input_tokens += row.input;
            bucket.output_tokens += row.output;
            bucket.cache_read_tokens += row.cache_read;
            bucket.cache_write_tokens += row.cache_write;
            sessions.insert(path.clone());
        }
    }
    drop(cache);
    let mut days: Vec<DayUsage> = buckets
        .into_values()
        .map(|(mut bucket, sessions)| {
            bucket.sessions = sessions.len() as u32;
            bucket.cost_usd = cost_usd(&bucket);
            bucket
        })
        .collect();
    days.sort_by(|a, b| (a.day, &a.harness, &a.model).cmp(&(b.day, &b.harness, &b.model)));
    NodeUsage { schema: USAGE_SCHEMA.into(), node: node.into(), node_id: node_id.into(), generated_at: Utc::now(), since, days }
}

/// The price of a bucket, None when the model is not in `PRICES`.
pub fn cost_usd(bucket: &DayUsage) -> Option<f64> {
    let (_, input, output, cache_read, cache_write) = PRICES
        .iter()
        .filter(|(family, ..)| bucket.model == *family || bucket.model.strip_prefix(family).is_some_and(|rest| rest.starts_with('-')))
        .max_by_key(|(family, ..)| family.len())?;
    let per_million = |tokens: u64, price: f64| tokens as f64 * price / 1e6;
    Some(
        per_million(bucket.input_tokens, *input)
            + per_million(bucket.output_tokens, *output)
            + per_million(bucket.cache_read_tokens, *cache_read)
            + per_million(bucket.cache_write_tokens, *cache_write),
    )
}

/// Every `*.jsonl` under `root`, recursively; a missing root is no files.
fn jsonl_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "jsonl") {
                out.push(path);
            }
        }
    }
    out
}

fn u64_at(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn timestamp(value: &Value) -> Option<DateTime<Utc>> {
    value.get("timestamp")?.as_str().and_then(|t| DateTime::parse_from_rfc3339(t).ok()).map(|t| t.with_timezone(&Utc))
}

/// Claude Code: assistant entries, one per `(message.id, requestId)`; `<synthetic>` entries
/// carry no tokens and are skipped.
pub fn parse_claude(text: &str) -> Vec<Row> {
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for line in text.lines() {
        let Ok(entry) = serde_json::from_str::<Value>(line) else { continue };
        if entry.get("type").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let message = &entry["message"];
        let (Some(model), Some(usage), Some(at)) = (message.get("model").and_then(Value::as_str), message.get("usage"), timestamp(&entry)) else { continue };
        if model == "<synthetic>" {
            continue;
        }
        if let Some(id) = message.get("id").and_then(Value::as_str) {
            let request = entry.get("requestId").and_then(Value::as_str).unwrap_or_default();
            if !seen.insert((id.to_string(), request.to_string())) {
                continue;
            }
        }
        rows.push(Row {
            at,
            model: model.into(),
            input: u64_at(usage, "input_tokens"),
            output: u64_at(usage, "output_tokens"),
            cache_read: u64_at(usage, "cache_read_input_tokens"),
            cache_write: u64_at(usage, "cache_creation_input_tokens"),
        });
    }
    rows
}

/// Codex: the model from the latest `turn_context`, tokens as the delta of each `token_count`
/// event's `total_token_usage` to the previous one. OpenAI's `input_tokens` includes the cached
/// ones, so input here is the uncached part.
/// ponytail: a rollout file whose first total is not zero (a resumed session in a new file)
/// counts its history again; per-session totals across files if that ever shows.
pub fn parse_codex(text: &str) -> Vec<Row> {
    let mut model = String::from("unknown");
    let mut prev = [0u64; 4];
    let mut rows = Vec::new();
    for line in text.lines() {
        let Ok(entry) = serde_json::from_str::<Value>(line) else { continue };
        let payload = &entry["payload"];
        match entry.get("type").and_then(Value::as_str) {
            Some("turn_context") => {
                if let Some(name) = payload.get("model").and_then(Value::as_str) {
                    model = name.into();
                }
            }
            Some("event_msg") if payload.get("type").and_then(Value::as_str) == Some("token_count") => {
                let (Some(total), Some(at)) = (payload.get("info").and_then(|info| info.get("total_token_usage")), timestamp(&entry)) else { continue };
                let now = [u64_at(total, "input_tokens"), u64_at(total, "cached_input_tokens"), u64_at(total, "output_tokens"), u64_at(total, "cache_write_input_tokens")];
                let [input, cached, output, cache_write] = std::array::from_fn(|i| now[i].saturating_sub(prev[i]));
                prev = now;
                if input + output + cache_write == 0 {
                    continue;
                }
                rows.push(Row { at, model: model.clone(), input: input.saturating_sub(cached), output, cache_read: cached, cache_write });
            }
            _ => {}
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLAUDE: &str = r#"{"type":"user","timestamp":"2026-09-10T10:00:00.000Z","message":{"role":"user","content":"hi"}}
{"type":"assistant","timestamp":"2026-09-10T10:00:01.000Z","requestId":"req_1","message":{"id":"msg_1","model":"claude-opus-5","usage":{"input_tokens":2,"output_tokens":100,"cache_creation_input_tokens":1000,"cache_read_input_tokens":5000}}}
{"type":"assistant","timestamp":"2026-09-10T10:00:02.000Z","requestId":"req_1","message":{"id":"msg_1","model":"claude-opus-5","usage":{"input_tokens":2,"output_tokens":100,"cache_creation_input_tokens":1000,"cache_read_input_tokens":5000}}}
{"type":"assistant","timestamp":"2026-09-10T10:00:03.000Z","requestId":"req_2","message":{"id":"msg_2","model":"<synthetic>","usage":{"input_tokens":0,"output_tokens":0}}}
{"type":"assistant","timestamp":"2026-09-11T01:00:00.000Z","requestId":"req_3","message":{"id":"msg_3","model":"claude-opus-5","usage":{"input_tokens":10,"output_tokens":50,"cache_creation_input_tokens":0,"cache_read_input_tokens":6000}}}
not json
"#;

    const CODEX: &str = r#"{"timestamp":"2026-09-10T08:00:00.000Z","type":"session_meta","payload":{"id":"x"}}
{"timestamp":"2026-09-10T08:00:01.000Z","type":"turn_context","payload":{"model":"gpt-5-codex"}}
{"timestamp":"2026-09-10T08:00:02.000Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"cached_input_tokens":400,"output_tokens":50,"cache_write_input_tokens":0},"last_token_usage":{}}}}
{"timestamp":"2026-09-10T08:00:03.000Z","type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{}}}
{"timestamp":"2026-09-10T08:00:04.000Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":2500,"cached_input_tokens":1400,"output_tokens":80,"cache_write_input_tokens":0},"last_token_usage":{}}}}
"#;

    #[test]
    fn claude_entries_count_once_per_message_and_request() {
        let rows = parse_claude(CLAUDE);
        assert_eq!(rows.len(), 2);
        assert_eq!((rows[0].input, rows[0].output, rows[0].cache_read, rows[0].cache_write), (2, 100, 5000, 1000));
        assert_eq!(rows[1].model, "claude-opus-5");
    }

    #[test]
    fn codex_turns_are_deltas_of_the_running_total() {
        let rows = parse_codex(CODEX);
        assert_eq!(rows.len(), 2);
        assert_eq!((rows[0].input, rows[0].cache_read, rows[0].output), (600, 400, 50));
        assert_eq!((rows[1].input, rows[1].cache_read, rows[1].output), (500, 1000, 30));
        assert_eq!(rows[1].model, "gpt-5-codex");
    }

    #[test]
    fn cost_uses_the_longest_family_prefix_and_none_for_strangers() {
        let bucket = |model: &str| DayUsage {
            day: NaiveDate::from_ymd_opt(2026, 9, 10).unwrap(),
            harness: "claude".into(),
            model: model.into(),
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
            cache_read_tokens: 1_000_000,
            cache_write_tokens: 1_000_000,
            cost_usd: None,
            sessions: 1,
        };
        assert_eq!(cost_usd(&bucket("claude-opus-5")), Some(5.0 + 25.0 + 0.5 + 6.25));
        assert_eq!(cost_usd(&bucket("claude-opus-4-8")), Some(36.75), "the 4.8 row, not the 4.0 family");
        assert_eq!(cost_usd(&bucket("claude-opus-4-20250514")), Some(15.0 + 75.0 + 1.5 + 18.75));
        assert_eq!(cost_usd(&bucket("gpt-5-2025-08-07")), Some(1.25 + 10.0 + 0.125));
        assert_eq!(cost_usd(&bucket("gpt-5.5")), None, "a dot release is not the gpt-5 family");
        assert_eq!(cost_usd(&bucket("<synthetic>")), None);
    }

    #[test]
    fn since_accepts_relative_and_absolute_forms() {
        let now = DateTime::parse_from_rfc3339("2026-09-17T12:00:00Z").unwrap().with_timezone(&Utc);
        assert_eq!(parse_since("7d", now).unwrap().to_rfc3339(), "2026-09-10T12:00:00+00:00");
        assert_eq!(parse_since("6h", now).unwrap().to_rfc3339(), "2026-09-17T06:00:00+00:00");
        assert_eq!(parse_since("2026-09-01", now).unwrap().to_rfc3339(), "2026-09-01T00:00:00+00:00");
        assert_eq!(parse_since("2026-09-01T10:00:00+02:00", now).unwrap().to_rfc3339(), "2026-09-01T08:00:00+00:00");
        assert!(parse_since("yesterday", now).is_err());
    }

    #[test]
    fn roll_up_groups_by_day_harness_and_model_over_a_temp_home() {
        let dir = std::env::temp_dir().join(format!("rosterd-usage-{}", std::process::id()));
        let roots = Roots { claude: dir.join("claude"), codex: dir.join("codex") };
        std::fs::create_dir_all(roots.claude.join("p")).unwrap();
        std::fs::create_dir_all(roots.codex.join("2026/09/10")).unwrap();
        std::fs::write(roots.claude.join("p/a.jsonl"), CLAUDE).unwrap();
        std::fs::write(roots.claude.join("p/b.jsonl"), CLAUDE).unwrap();
        std::fs::write(roots.codex.join("2026/09/10/r.jsonl"), CODEX).unwrap();
        let since = DateTime::parse_from_rfc3339("2026-09-01T00:00:00Z").unwrap().with_timezone(&Utc);

        let usage = node_usage(&roots, "gibson", "id", since);
        assert_eq!(usage.schema, "rosterd.usage.v1");
        let keys: Vec<(String, &str, &str)> = usage.days.iter().map(|d| (d.day.to_string(), d.harness.as_str(), d.model.as_str())).collect();
        assert_eq!(keys, [("2026-09-10".to_string(), "claude", "claude-opus-5"), ("2026-09-10".into(), "codex", "gpt-5-codex"), ("2026-09-11".into(), "claude", "claude-opus-5")]);
        let first = &usage.days[0];
        assert_eq!((first.input_tokens, first.output_tokens, first.cache_read_tokens, first.cache_write_tokens, first.sessions), (4, 200, 10_000, 2000, 2));
        assert!(first.cost_usd.is_some());
        assert_eq!(usage.days[1].sessions, 1);

        // Nothing inside a later window; the file cache is keyed on mtime and size, so a rewrite
        // with new content is re-read.
        let later = DateTime::parse_from_rfc3339("2026-09-11T00:00:00Z").unwrap().with_timezone(&Utc);
        assert!(node_usage(&roots, "gibson", "id", later).days.iter().all(|d| d.day.to_string() == "2026-09-11"));
        std::fs::write(roots.claude.join("p/b.jsonl"), "").unwrap();
        let again = node_usage(&roots, "gibson", "id", since);
        assert_eq!(again.days[0].sessions, 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
