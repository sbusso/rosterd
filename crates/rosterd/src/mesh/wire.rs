//! What crosses the wire between nodes, R7: canonical JSON for signatures, the hello of R7.2,
//! the invite of R7.3, and the signed request headers of R7.6.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use axum::http::{HeaderMap, HeaderValue, Method};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rosterd_proto::Capabilities;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::membership::MemberRecord;
use super::{NODE_ID_HEADER, SIGNATURE_HEADER, SWARM_KEY_HEADER, TIMESTAMP_HEADER};
use crate::identity::{Identity, node_id_of, verify};

/// Unix milliseconds, the clock every signed record uses.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Compact JSON with object keys sorted at every level, so signer and verifier serialise the
/// same bytes whatever the field order on the wire.
pub fn canonical(value: &Value) -> Vec<u8> {
    fn sort(value: Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut sorted: Vec<(String, Value)> = map.into_iter().map(|(k, v)| (k, sort(v))).collect();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.into_iter().map(sort).collect()),
            other => other,
        }
    }
    serde_json::to_vec(&sort(value.clone())).expect("json value serialises")
}

/// The bytes a signed struct is signed over: its canonical JSON minus `signature`.
pub fn signing_bytes<T: Serialize>(signed: &T) -> Result<Vec<u8>> {
    let mut value = serde_json::to_value(signed)?;
    if let Value::Object(map) = &mut value {
        map.remove("signature");
    }
    Ok(canonical(&value))
}

/// HMAC-SHA256, RFC 2104, from sha2 alone. Keys longer than the block are hashed first.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let inner: Vec<u8> = block.iter().map(|b| b ^ 0x36).collect();
    let outer: Vec<u8> = block.iter().map(|b| b ^ 0x5c).collect();
    let inner_hash = Sha256::new().chain_update(inner).chain_update(message).finalize();
    Sha256::new().chain_update(outer).chain_update(inner_hash).finalize().into()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The hello of R7.2 plus the membership it gossips, R7.3. Signed by the node key over the
/// canonical JSON without `signature`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub node_id: String,
    pub name: String,
    pub public_key: String,
    pub version: String,
    #[serde(default)]
    pub swarm_id: Option<String>,
    /// `host:port` this node answers on; peers learn it from here.
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(default)]
    pub members: Vec<MemberRecord>,
    pub signed_at: i64,
    #[serde(default)]
    pub signature: String,
    /// Fields a newer node signed that this one does not know: kept, so the canonical form is
    /// the sender's and the signature verifies. Every signed struct on the wire does this.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, Value>,
}

impl Hello {
    pub fn sign(mut self, identity: &Identity) -> Result<Hello> {
        self.signature = hex::encode(identity.sign(&signing_bytes(&self)?).to_bytes());
        Ok(self)
    }

    /// The signature verifies with the embedded key and the key is the node id's.
    pub fn verify(&self) -> Result<()> {
        let public = verify(&self.public_key, &signing_bytes(self)?, &self.signature).context("hello signature")?;
        ensure!(node_id_of(&public) == self.node_id, "hello node_id does not match its public key");
        Ok(())
    }
}

/// The invite of R7.3: JSON, base64url, then an HMAC tag with the swarm key. Single use and one
/// hour, enforced by the admitter's nonce set.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Invite {
    pub swarm_id: String,
    pub nonce: String,
    /// Unix seconds.
    pub expires_at: i64,
    pub admitter: String,
    pub admitter_public_key: String,
    #[serde(default)]
    pub admitter_address: Option<String>,
}

pub const INVITE_TTL_SECS: i64 = 3600;

pub fn mint_invite(invite: &Invite, swarm_key: &[u8]) -> String {
    let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(invite).expect("invite serialises"));
    let tag = hex::encode(hmac_sha256(swarm_key, body.as_bytes()));
    format!("{body}.{tag}")
}

/// Decodes without checking the tag: the joiner has no swarm key yet and only needs the
/// admitter's key and address to reach it.
pub fn parse_invite(token: &str) -> Result<Invite> {
    let (body, _) = token.split_once('.').context("invite has no tag")?;
    let json = URL_SAFE_NO_PAD.decode(body).context("invite base64")?;
    serde_json::from_slice(&json).context("invite json")
}

/// Tag, swarm and expiry; the nonce is the caller's to burn.
pub fn verify_invite(token: &str, swarm_key: &[u8], swarm_id: &str, now_secs: i64) -> Result<Invite> {
    let (body, tag) = token.split_once('.').context("invite has no tag")?;
    let expected = hmac_sha256(swarm_key, body.as_bytes());
    let tag = hex::decode(tag).context("invite tag hex")?;
    ensure!(constant_time_eq(&expected, &tag), "invite tag does not match this swarm");
    let invite = parse_invite(token)?;
    ensure!(invite.swarm_id == swarm_id, "invite is for another swarm");
    ensure!(invite.expires_at > now_secs, "invite expired");
    Ok(invite)
}

/// What a request signature covers, R7.6.
pub fn signing_input(method: &Method, path: &str, timestamp: i64, body: &[u8]) -> String {
    format!("{}\n{}\n{}\n{}", method.as_str(), path, timestamp, sha256_hex(body))
}

/// The four headers every node to node request carries. The swarm header is the hash of the
/// key, never the key.
pub fn signed_headers(identity: &Identity, swarm_key: &[u8], method: &Method, path: &str, body: &[u8]) -> HeaderMap {
    let timestamp = chrono::Utc::now().timestamp();
    let signature = identity.sign(signing_input(method, path, timestamp, body).as_bytes());
    let mut headers = HeaderMap::new();
    headers.insert(NODE_ID_HEADER, HeaderValue::from_str(&identity.node_id).expect("hex"));
    headers.insert(TIMESTAMP_HEADER, HeaderValue::from(timestamp));
    headers.insert(SWARM_KEY_HEADER, HeaderValue::from_str(&sha256_hex(swarm_key)).expect("hex"));
    headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&hex::encode(signature.to_bytes())).expect("hex"));
    headers
}

pub const MAX_SKEW_SECS: i64 = 60;

pub fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str> {
    match headers.get(name) {
        Some(value) => value.to_str().with_context(|| format!("{name} is not ascii")),
        None => bail!("missing {name}"),
    }
}

/// The timestamp and signature checks of `authenticate`; the caller resolved the signer's key
/// and membership already.
pub fn verify_signed(
    headers: &HeaderMap,
    public_key_hex: &str,
    method: &Method,
    path: &str,
    body: &[u8],
    now_secs: i64,
) -> Result<()> {
    let timestamp: i64 = header(headers, TIMESTAMP_HEADER)?.parse().context("timestamp")?;
    ensure!((now_secs - timestamp).abs() <= MAX_SKEW_SECS, "timestamp skew over {MAX_SKEW_SECS}s");
    let signature = header(headers, SIGNATURE_HEADER)?;
    verify(public_key_hex, signing_input(method, path, timestamp, body).as_bytes(), signature).context("request signature")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;

    use super::*;

    fn identity() -> Identity {
        Identity::from_key(SigningKey::generate(&mut rand::rngs::OsRng))
    }

    #[test]
    fn canonical_json_sorts_keys_at_every_level() {
        let a: Value = serde_json::from_str(r#"{"b":{"z":1,"a":[{"y":1,"x":2}]},"a":2}"#).unwrap();
        assert_eq!(canonical(&a), br#"{"a":2,"b":{"a":[{"x":2,"y":1}],"z":1}}"#);
    }

    #[test]
    fn hmac_matches_rfc4231_case_2() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(hex::encode(mac), "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    }

    #[test]
    fn hello_round_trips_and_tamper_is_detected() {
        let id = identity();
        let hello = Hello {
            node_id: id.node_id.clone(),
            name: "gibson".into(),
            public_key: id.public_hex(),
            version: "0.1.0".into(),
            swarm_id: None,
            address: Some("127.0.0.1:1".into()),
            capabilities: Capabilities::default(),
            members: vec![],
            signed_at: now_ms(),
            signature: String::new(),
            extra: Default::default(),
        }
        .sign(&id)
        .unwrap();
        let mut value = serde_json::to_value(&hello).unwrap();
        let back: Hello = serde_json::from_str(&serde_json::to_string(&hello).unwrap()).unwrap();
        back.verify().unwrap();

        value["name"] = Value::String("wintermute".into());
        let tampered: Hello = serde_json::from_value(value).unwrap();
        assert!(tampered.verify().is_err());

        let mut wrong_id = hello.clone();
        wrong_id.node_id = "00".repeat(16);
        let wrong_id = wrong_id.sign(&id).unwrap();
        assert!(wrong_id.verify().is_err());
    }

    /// A newer node signs fields this one does not know; they must survive the round trip so
    /// the signature still covers them, and an empty `health` stays off the wire.
    #[test]
    fn hello_with_unknown_fields_still_verifies() {
        let id = identity();
        let mut value = serde_json::to_value(Hello {
            node_id: id.node_id.clone(),
            name: "gibson".into(),
            public_key: id.public_hex(),
            version: "0.2.0".into(),
            swarm_id: None,
            address: None,
            capabilities: Capabilities::default(),
            members: vec![],
            signed_at: now_ms(),
            signature: String::new(),
            extra: Default::default(),
        })
        .unwrap();
        assert!(value["capabilities"].get("health").is_none());
        value["future"] = Value::Bool(true);
        value["capabilities"]["future"] = Value::String("yes".into());
        let unsigned: Hello = serde_json::from_value(value).unwrap();
        let signed = unsigned.sign(&id).unwrap();
        let back: Hello = serde_json::from_str(&serde_json::to_string(&signed).unwrap()).unwrap();
        back.verify().unwrap();
        assert_eq!(back.extra["future"], Value::Bool(true));
        assert_eq!(back.capabilities.extra["future"], Value::String("yes".into()));
    }

    #[test]
    fn invite_verifies_only_with_the_key_swarm_and_time() {
        let key = [7u8; 32];
        let invite = Invite {
            swarm_id: "s1".into(),
            nonce: "n1".into(),
            expires_at: 1_000,
            admitter: "a".into(),
            admitter_public_key: "aa".into(),
            admitter_address: None,
        };
        let token = mint_invite(&invite, &key);
        assert_eq!(parse_invite(&token).unwrap(), invite);
        assert_eq!(verify_invite(&token, &key, "s1", 999).unwrap(), invite);
        assert!(verify_invite(&token, &[8u8; 32], "s1", 999).is_err());
        assert!(verify_invite(&token, &key, "s2", 999).is_err());
        assert!(verify_invite(&token, &key, "s1", 1_000).is_err());
        let (body, _) = token.split_once('.').unwrap();
        assert!(verify_invite(&format!("{body}.00"), &key, "s1", 999).is_err());
    }

    #[test]
    fn signed_request_headers_verify_and_reject_skew_and_tamper() {
        let id = identity();
        let key = [1u8; 32];
        let headers = signed_headers(&id, &key, &Method::POST, "/sessions/x/prompt", b"{}");
        assert_eq!(header(&headers, SWARM_KEY_HEADER).unwrap(), sha256_hex(&key));
        let now = chrono::Utc::now().timestamp();
        verify_signed(&headers, &id.public_hex(), &Method::POST, "/sessions/x/prompt", b"{}", now).unwrap();
        assert!(verify_signed(&headers, &id.public_hex(), &Method::POST, "/sessions/y/prompt", b"{}", now).is_err());
        assert!(verify_signed(&headers, &id.public_hex(), &Method::POST, "/sessions/x/prompt", b"{ }", now).is_err());
        assert!(verify_signed(&headers, &id.public_hex(), &Method::GET, "/sessions/x/prompt", b"{}", now).is_err());
        assert!(verify_signed(&headers, &id.public_hex(), &Method::POST, "/sessions/x/prompt", b"{}", now + 61).is_err());
        assert!(verify_signed(&headers, &identity().public_hex(), &Method::POST, "/sessions/x/prompt", b"{}", now).is_err());
    }
}
