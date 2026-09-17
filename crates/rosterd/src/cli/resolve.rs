//! KEY resolution, R14.1: an exact session_key, then a PID on the local node, then a unique
//! display name in the scope. Ambiguous lists the candidates; never a guess. And the age
//! column, R14.3.

use rosterd_proto::{Liveness, Record};

use super::Exit;

/// The first column of `list`: the name, else the cwd basename in brackets, else the key.
pub fn display_name(record: &Record) -> String {
    if let Some(name) = record.name.as_deref().filter(|n| !n.is_empty()) {
        return name.to_string();
    }
    match record.cwd.as_deref().and_then(|cwd| std::path::Path::new(cwd).file_name()).and_then(|f| f.to_str()) {
        Some(base) => format!("[{base}]"),
        None => record.origin.clone().unwrap_or_else(|| record.session_key.clone()),
    }
}

/// `records` is the scope; `local_node_id` says which of them may match by pid.
pub fn resolve<'a>(key: &str, local_node_id: &str, records: &'a [Record]) -> Result<&'a Record, Exit> {
    if let Some(record) = records.iter().find(|r| r.session_key == key) {
        return Ok(record);
    }
    let live = || records.iter().filter(|r| r.liveness != Liveness::Ended);
    if let Ok(pid) = key.parse::<u32>() {
        let by_pid: Vec<&Record> = live().filter(|r| r.node_id == local_node_id && r.pid == pid).collect();
        match by_pid.as_slice() {
            [one] => return Ok(one),
            [] => {}
            many => return Err(ambiguous(key, many)),
        }
    }
    let by_name: Vec<&Record> = live().filter(|r| display_name(r) == key).collect();
    match by_name.as_slice() {
        [one] => Ok(one),
        [] => Err(Exit::user(format!("no session {key}"))),
        many => Err(ambiguous(key, many)),
    }
}

fn ambiguous(key: &str, candidates: &[&Record]) -> Exit {
    let mut message = format!("{key} is ambiguous; use one of");
    for r in candidates {
        message += &format!("\n  {}  {}  {}  pid {}", r.session_key, display_name(r), r.node, r.pid);
    }
    Exit::user(message)
}

#[cfg(test)]
mod tests {
    use rosterd_proto::{Activity, Lane};

    use super::*;

    fn record(node_id: &str, pid: u32, name: Option<&str>, cwd: Option<&str>) -> Record {
        Record {
            node: node_id.into(),
            node_id: node_id.into(),
            session_key: rosterd_proto::session_key(node_id, pid, 1),
            pid,
            start_ticks: 1,
            started_at: chrono::Utc::now(),
            harness: "claude".into(),
            session_id: None,
            lane: Lane::Interactive,
            sources: vec![],
            name: name.map(Into::into),
            activity: Activity::Idle,
            activity_event: None,
            activity_at: None,
            activity_seq: 0,
            attempt_id: None,
            parent_attempt_id: None,
            parent_session_key: None,
            cwd: cwd.map(Into::into),
            origin: None,
            tty: None,
            tmux: None,
            herdr: None,
            holder: None,
            liveness: Liveness::Live,
            ended_at: None,
            ended_reason: None,
            usage: None,
            load: None,
            mode: None,
            plan: None,
            conflict: false,
            permission_policy: None,
        }
    }

    #[test]
    fn exact_then_pid_then_unique_name_then_ambiguous() {
        let mut records = vec![
            record("local", 42, Some("builder"), None),
            record("local", 43, None, Some("/home/me/proj")),
            record("remote", 42, Some("builder"), None),
            record("remote", 44, Some("42"), None),
        ];
        records[2].liveness = Liveness::Ended;
        assert_eq!(resolve("remote:42:1", "local", &records).unwrap().node, "remote", "exact beats liveness");
        assert_eq!(resolve("42", "local", &records).unwrap().session_key, "local:42:1", "pid on the local node beats a name");
        assert_eq!(resolve("[proj]", "local", &records).unwrap().pid, 43, "cwd basename in brackets is the fallback name");
        assert_eq!(resolve("builder", "local", &records).unwrap().pid, 42, "an ended record never competes");
        records[2].liveness = Liveness::Live;
        let err = resolve("builder", "local", &records).unwrap_err();
        assert_eq!(err.code, 1);
        assert!(err.message.contains("local:42:1") && err.message.contains("remote:42:1"), "{}", err.message);
        assert_eq!(resolve("nobody", "local", &records).unwrap_err().message, "no session nobody");
    }
}
