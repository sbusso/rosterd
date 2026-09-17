//! Per-harness health on this node, R7.7: one word per harness so a client or coordinator
//! skips a node whose harness cannot work right now. Observed from the runner's own ACP
//! traffic and from hook claims; rosterd never touches a credential (R12).

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use rosterd_proto::{HarnessHealth, HarnessState};

/// How long a mark lasts when the error names no deadline. rosterd cannot see a login or an
/// install happen (R12), so a mark that refuses starts must expire: the next start after it
/// probes the harness again and either clears the mark or sets it afresh.
pub const DEFAULT_TTL: Duration = Duration::minutes(5);

#[derive(Default)]
pub struct Health {
    // ponytail: one state per harness, the latest wins; login and rate limit at once is rare.
    entries: Mutex<HashMap<String, HarnessHealth>>,
}

impl Health {
    /// Marks `harness`; `since` is kept when the state is unchanged. A missing `until` is
    /// now plus `DEFAULT_TTL`.
    pub fn set(&self, harness: &str, state: HarnessState, until: Option<DateTime<Utc>>, detail: Option<String>) {
        let now = Utc::now();
        let mut entries = self.entries.lock().unwrap();
        let since = entries.get(harness).filter(|h| h.state == state && h.until.is_none_or(|u| u > now)).map_or(now, |h| h.since);
        let until = Some(until.unwrap_or(now + DEFAULT_TTL));
        entries.insert(harness.to_string(), HarnessHealth { harness: harness.to_string(), state, since, until, detail, extra: Default::default() });
    }

    pub fn clear(&self, harness: &str) {
        self.entries.lock().unwrap().remove(harness);
    }

    /// The live mark on `harness`, none when ok or expired.
    pub fn get(&self, harness: &str, now: DateTime<Utc>) -> Option<HarnessHealth> {
        self.entries.lock().unwrap().get(harness).filter(|h| h.until.is_none_or(|u| u > now)).cloned()
    }

    /// Every live mark, sorted by harness; expired ones are dropped on the way.
    pub fn render(&self, now: DateTime<Utc>) -> Vec<HarnessHealth> {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, h| h.until.is_none_or(|u| u > now));
        let mut out: Vec<HarnessHealth> = entries.values().cloned().collect();
        out.sort_by(|a, b| a.harness.cmp(&b.harness));
        out
    }
}

/// Whether a turn's error says the harness is rate limited or out of quota.
pub fn rate_limited(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    ["rate limit", "rate_limit", "429", "overloaded", "usage limit", "quota"].iter().any(|needle| m.contains(needle))
}

/// The deadline a rate limit error carries, when it does: `retry-after: 30` or `retry after 30s`
/// as seconds from `now`, `resets at 2026-09-17T10:00:00Z` as a timestamp.
// ponytail: token scan after "retry"/"reset", no regex; add forms as real adapters show them.
pub fn retry_at(message: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let lower = message.to_ascii_lowercase();
    let tail = lower.find("retry").or_else(|| lower.find("reset")).map(|i| &message[i..])?;
    for token in tail.split(|c: char| c.is_whitespace() || c == ',' || c == ';').skip(1).take(4) {
        let token = token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '+' && c != '.');
        if let Ok(at) = DateTime::parse_from_rfc3339(token) {
            return Some(at.with_timezone(&Utc));
        }
        let digits = token.trim_end_matches(|c: char| c.is_ascii_alphabetic());
        if let Ok(seconds) = digits.parse::<u64>() {
            let seconds = if token.ends_with("ms") { seconds / 1000 } else if token.ends_with('m') { seconds * 60 } else { seconds };
            return Some(now + Duration::seconds(seconds as i64));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_clear_expire_and_render() {
        let h = Health::default();
        let now = Utc::now();
        assert!(h.render(now).is_empty());
        h.set("claude", HarnessState::LoginRequired, None, Some("auth".into()));
        let first = h.get("claude", now).unwrap();
        assert_eq!(first.state, HarnessState::LoginRequired);
        assert_eq!(first.until, Some(first.since + DEFAULT_TTL));
        // Same state again keeps `since`, refreshes `until`.
        h.set("claude", HarnessState::LoginRequired, Some(now + Duration::hours(1)), None);
        let again = h.get("claude", now).unwrap();
        assert_eq!(again.since, first.since);
        assert_eq!(again.until, Some(now + Duration::hours(1)));
        // A new state restarts `since`.
        h.set("codex", HarnessState::RateLimited, Some(now + Duration::seconds(30)), None);
        assert_eq!(h.render(now).iter().map(|e| e.harness.as_str()).collect::<Vec<_>>(), ["claude", "codex"]);
        assert!(h.get("codex", now + Duration::seconds(31)).is_none(), "expired");
        assert_eq!(h.render(now + Duration::seconds(31)).len(), 1);
        h.clear("claude");
        assert!(h.render(now).is_empty());
        assert!(h.get("pi", now).is_none());
    }

    #[test]
    fn rate_limit_messages() {
        for m in ["session/prompt: Rate limit exceeded", "HTTP 429", "Overloaded", "You have hit your usage limit", "quota exceeded", "rate_limit_error"] {
            assert!(rate_limited(m), "{m}");
        }
        for m in ["connection closed", "method not found", "cwd does not exist"] {
            assert!(!rate_limited(m), "{m}");
        }
        let now = Utc::now();
        assert_eq!(retry_at("429 too many requests; retry-after: 30", now), Some(now + Duration::seconds(30)));
        assert_eq!(retry_at("rate limited, retry after 2m", now), Some(now + Duration::seconds(120)));
        assert_eq!(retry_at("usage limit reached, resets at 2026-09-17T10:00:00Z", now), Some("2026-09-17T10:00:00Z".parse().unwrap()));
        assert_eq!(retry_at("overloaded", now), None);
        assert_eq!(retry_at("retry later", now), None);
    }
}
