//! Discovery, R7.2: Tailscale peers from `tailscale status --json`, static peers from config,
//! this machine's Tailscale IP. mDNS is not built; `swarm.mdns` stays a config flag.

use std::net::IpAddr;
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;

const TAILSCALE_TIMEOUT: Duration = Duration::from_secs(2);

/// Runs `tailscale <args>` best effort: absent binary, failure or 2 s without an answer all
/// yield None.
async fn tailscale(args: &[&str]) -> Option<String> {
    let child = tokio::process::Command::new("tailscale")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(TAILSCALE_TIMEOUT, child).await.ok()?.ok()?;
    output.status.success().then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

/// `tailscale ip -4`.
pub async fn tailscale_ip() -> Option<IpAddr> {
    tailscale(&["ip", "-4"]).await?.lines().find_map(|line| line.trim().parse().ok())
}

/// The first Tailscale IP of every online peer.
pub async fn tailscale_peers() -> Vec<IpAddr> {
    match tailscale(&["status", "--json"]).await.and_then(|text| serde_json::from_str(&text).ok()) {
        Some(status) => parse_status(&status),
        None => Vec::new(),
    }
}

pub fn parse_status(status: &Value) -> Vec<IpAddr> {
    let Some(peers) = status.get("Peer").and_then(Value::as_object) else { return Vec::new() };
    peers
        .values()
        .filter(|peer| peer.get("Online").and_then(Value::as_bool).unwrap_or(false))
        .filter_map(|peer| peer.get("TailscaleIPs")?.get(0)?.as_str()?.parse().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_yields_first_ip_of_online_peers_only() {
        let status = serde_json::json!({
            "Peer": {
                "nodekey:a": { "Online": true, "TailscaleIPs": ["100.64.0.12", "fd7a::1"] },
                "nodekey:b": { "Online": false, "TailscaleIPs": ["100.64.0.13"] },
                "nodekey:c": { "Online": true, "TailscaleIPs": [] },
                "nodekey:d": { "TailscaleIPs": ["100.64.0.14"] }
            }
        });
        assert_eq!(parse_status(&status), vec!["100.64.0.12".parse::<IpAddr>().unwrap()]);
        assert!(parse_status(&serde_json::json!({})).is_empty());
    }

    #[tokio::test]
    async fn absent_or_slow_tailscale_is_none() {
        // Whatever this machine has, the call returns within the timeout and never panics.
        let _ = tokio::time::timeout(Duration::from_secs(3), tailscale_ip()).await.expect("bounded");
    }
}
