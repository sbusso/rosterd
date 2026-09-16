//! Membership, R7.3: the swarm id and key, one signed record per node, the merge rule for
//! gossip, revocation, the `swarm.json` file, and the swarm key sealed to a joiner's key.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use chacha20poly1305::aead::Aead;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, Nonce};
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::wire::{now_ms, sha256_hex, signing_bytes};
use crate::config::write_private;
use crate::identity::{Identity, node_id_of, verify};

/// One node as a member sees it. Signed by `signed_by`'s node key over the canonical JSON of
/// the other fields. Only the admitter (join) and a revoker write new records: nobody re-signs
/// on an address change, so a stale view can never un-revoke a node by accident.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemberRecord {
    pub node_id: String,
    pub name: String,
    pub public_key: String,
    /// `host:port` at admission; discovery keeps a fresher one outside the record.
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    /// Unix milliseconds.
    pub signed_at: i64,
    #[serde(default)]
    pub revoked: bool,
    pub signed_by: String,
    #[serde(default)]
    pub signature: String,
}

impl MemberRecord {
    pub fn sign(mut self, signer: &Identity) -> Result<MemberRecord> {
        self.signed_by = signer.node_id.clone();
        self.signature = hex::encode(signer.sign(&signing_bytes(&self)?).to_bytes());
        Ok(self)
    }

    /// The signature is `signer_public`'s and the record's key belongs to its node id.
    pub fn verify_with(&self, signer_public_hex: &str) -> Result<()> {
        verify(signer_public_hex, &signing_bytes(self)?, &self.signature).context("member signature")?;
        let public = public_key(&self.public_key)?;
        ensure!(node_id_of(&public) == self.node_id, "member node_id does not match its public key");
        Ok(())
    }

    /// Newest `signed_at` wins; at equal time a revocation wins. A strict order, so every node
    /// converges whatever the gossip order.
    pub fn beats(&self, current: &MemberRecord) -> bool {
        self.signed_at > current.signed_at || (self.signed_at == current.signed_at && self.revoked && !current.revoked)
    }
}

pub fn public_key(hex_key: &str) -> Result<VerifyingKey> {
    let bytes: [u8; 32] = hex::decode(hex_key)?.as_slice().try_into().context("public key length")?;
    VerifyingKey::from_bytes(&bytes).context("public key")
}

/// The SubjectPublicKeyInfo DER a certificate for this key carries, R7.6 pinning.
pub fn spki_der(hex_key: &str) -> Result<Vec<u8>> {
    Ok(public_key(hex_key)?.to_public_key_der()?.as_bytes().to_vec())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Membership {
    pub swarm_id: String,
    #[serde(with = "hex_key")]
    pub swarm_key: [u8; 32],
    pub members: BTreeMap<String, MemberRecord>,
}

mod hex_key {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(key: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(key))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        let text = String::deserialize(d)?;
        let bytes = hex::decode(text).map_err(serde::de::Error::custom)?;
        bytes.as_slice().try_into().map_err(|_| serde::de::Error::custom("swarm key is not 32 bytes"))
    }
}

impl Membership {
    /// The first node creates the swarm and signs itself in, R7.3.
    pub fn create(identity: &Identity, name: &str, version: &str, address: Option<String>) -> Result<Membership> {
        let mut swarm_key = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut swarm_key);
        let mut membership = Membership {
            swarm_id: ulid::Ulid::new().to_string().to_lowercase(),
            swarm_key,
            members: BTreeMap::new(),
        };
        let me = MemberRecord {
            node_id: identity.node_id.clone(),
            name: name.into(),
            public_key: identity.public_hex(),
            address,
            version: Some(version.into()),
            signed_at: now_ms(),
            revoked: false,
            signed_by: String::new(),
            signature: String::new(),
        }
        .sign(identity)?;
        membership.members.insert(me.node_id.clone(), me);
        Ok(membership)
    }

    pub fn load(path: &Path) -> Result<Option<Membership>> {
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        Ok(Some(serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        write_private(path, &serde_json::to_vec_pretty(self)?)
    }

    /// hex sha256 of the swarm key: the header value, never the key, R7.6.
    pub fn key_hash(&self) -> String {
        sha256_hex(&self.swarm_key)
    }

    pub fn member(&self, node_id: &str) -> Option<&MemberRecord> {
        self.members.get(node_id)
    }

    /// Present and not revoked.
    pub fn is_active(&self, node_id: &str) -> bool {
        self.members.get(node_id).is_some_and(|m| !m.revoked)
    }

    pub fn active(&self) -> impl Iterator<Item = &MemberRecord> {
        self.members.values().filter(|m| !m.revoked)
    }

    /// Stores a record this node signed itself or received over the pinned join channel.
    /// Returns whether anything changed.
    pub fn insert_trusted(&mut self, record: MemberRecord) -> bool {
        match self.members.get(&record.node_id) {
            Some(current) if !record.beats(current) => false,
            _ => {
                self.members.insert(record.node_id.clone(), record);
                true
            }
        }
    }

    /// Gossip, R7.3: keeps every record signed by a member this node already trusts, newest
    /// wins. Passes repeat so a record can vouch for the signer of the next one. Returns the
    /// node ids whose record changed.
    pub fn merge(&mut self, records: &[MemberRecord]) -> Vec<String> {
        let mut changed = Vec::new();
        loop {
            let mut progressed = false;
            for record in records {
                let Some(signer) = self.members.get(&record.signed_by) else { continue };
                if signer.revoked || record.verify_with(&signer.public_key).is_err() {
                    continue;
                }
                if self.insert_trusted(record.clone()) {
                    changed.push(record.node_id.clone());
                    progressed = true;
                }
            }
            if !progressed {
                return changed;
            }
        }
    }
}

/// The swarm key encrypted to the joiner's node key, R7.3: ephemeral X25519 against the
/// Ed25519 key's Montgomery form, ChaCha20-Poly1305 under sha256(shared, ephemeral, recipient).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sealed {
    pub ephemeral_public: String,
    pub nonce: String,
    pub ciphertext: String,
}

fn box_key(shared: &[u8; 32], ephemeral: &[u8; 32], recipient: &[u8; 32]) -> [u8; 32] {
    Sha256::new().chain_update(shared).chain_update(ephemeral).chain_update(recipient).finalize().into()
}

pub fn seal(recipient: &VerifyingKey, plaintext: &[u8]) -> Result<Sealed> {
    let recipient_x = x25519_dalek::PublicKey::from(recipient.to_montgomery().to_bytes());
    let ephemeral = x25519_dalek::EphemeralSecret::random_from_rng(rand::rngs::OsRng);
    let ephemeral_public = x25519_dalek::PublicKey::from(&ephemeral);
    let shared = ephemeral.diffie_hellman(&recipient_x);
    ensure!(shared.was_contributory(), "recipient key is a low order point");
    let key = box_key(shared.as_bytes(), ephemeral_public.as_bytes(), recipient_x.as_bytes());
    let mut nonce = [0u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let ciphertext = ChaCha20Poly1305::new(Key::from_slice(&key))
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| anyhow::anyhow!("seal"))?;
    Ok(Sealed {
        ephemeral_public: hex::encode(ephemeral_public.as_bytes()),
        nonce: hex::encode(nonce),
        ciphertext: hex::encode(ciphertext),
    })
}

pub fn unseal(identity: &Identity, sealed: &Sealed) -> Result<Vec<u8>> {
    let ephemeral: [u8; 32] = hex::decode(&sealed.ephemeral_public)?.as_slice().try_into().context("ephemeral length")?;
    let nonce: [u8; 12] = hex::decode(&sealed.nonce)?.as_slice().try_into().context("nonce length")?;
    let ciphertext = hex::decode(&sealed.ciphertext)?;
    let secret = x25519_dalek::StaticSecret::from(identity.signing_key().to_scalar_bytes());
    let me = x25519_dalek::PublicKey::from(&secret);
    let shared = secret.diffie_hellman(&x25519_dalek::PublicKey::from(ephemeral));
    ensure!(shared.was_contributory(), "ephemeral key is a low order point");
    let key = box_key(shared.as_bytes(), &ephemeral, me.as_bytes());
    ChaCha20Poly1305::new(Key::from_slice(&key))
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_slice())
        .map_err(|_| anyhow::anyhow!("sealed swarm key does not open with this node key"))
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;

    use super::*;

    fn identity() -> Identity {
        Identity::from_key(SigningKey::generate(&mut rand::rngs::OsRng))
    }

    fn record(id: &Identity, name: &str, signed_at: i64, revoked: bool, signer: &Identity) -> MemberRecord {
        MemberRecord {
            node_id: id.node_id.clone(),
            name: name.into(),
            public_key: id.public_hex(),
            address: None,
            version: None,
            signed_at,
            revoked,
            signed_by: String::new(),
            signature: String::new(),
        }
        .sign(signer)
        .unwrap()
    }

    #[test]
    fn merge_newest_wins_revoked_wins_ties_and_unknown_signers_are_ignored() {
        let founder = identity();
        let joiner = identity();
        let stranger = identity();
        let mut membership = Membership::create(&founder, "gibson", "0.1.0", None).unwrap();
        assert_eq!(membership.members.len(), 1);

        // Signed by the founder: kept.
        let joined = record(&joiner, "wintermute", 10, false, &founder);
        assert_eq!(membership.merge(std::slice::from_ref(&joined)), vec![joiner.node_id.clone()]);
        // Same record again: no change.
        assert!(membership.merge(std::slice::from_ref(&joined)).is_empty());
        // Older non-revoked record: ignored.
        assert!(membership.merge(&[record(&joiner, "old", 5, false, &founder)]).is_empty());
        // Equal time, revoked: wins.
        assert_eq!(membership.merge(&[record(&joiner, "wintermute", 10, true, &founder)]).len(), 1);
        assert!(!membership.is_active(&joiner.node_id));
        // Equal time, non-revoked against revoked: loses.
        assert!(membership.merge(std::slice::from_ref(&joined)).is_empty());
        // Newer non-revoked (a re-invite): wins.
        assert_eq!(membership.merge(&[record(&joiner, "wintermute", 11, false, &founder)]).len(), 1);
        assert!(membership.is_active(&joiner.node_id));

        // A revoked node's signature is refused, a stranger's too.
        membership.merge(&[record(&joiner, "wintermute", 12, true, &founder)]);
        assert!(membership.merge(&[record(&stranger, "s", 20, false, &joiner)]).is_empty());
        assert!(membership.merge(&[record(&stranger, "s", 20, false, &stranger)]).is_empty());
        // A tampered record fails verification.
        let mut forged = record(&stranger, "s", 20, false, &founder);
        forged.name = "root".into();
        assert!(membership.merge(&[forged]).is_empty());

        // Order inside a batch does not matter: the joiner's record vouches for the one it signed.
        let mut fresh = Membership::create(&founder, "gibson", "0.1.0", None).unwrap();
        let joined = record(&joiner, "wintermute", 30, false, &founder);
        let vouched = record(&stranger, "case", 31, false, &joiner);
        assert_eq!(fresh.merge(&[vouched, joined]).len(), 2);
    }

    #[test]
    fn file_round_trips_with_mode_0600() {
        let founder = identity();
        let dir = std::env::temp_dir().join(format!("rosterd-mesh-membership-{}", std::process::id()));
        let path = dir.join("swarm.json");
        let membership = Membership::create(&founder, "gibson", "0.1.0", Some("100.64.0.1:8791".into())).unwrap();
        membership.save(&path).unwrap();
        let back = Membership::load(&path).unwrap().unwrap();
        assert_eq!(back.swarm_id, membership.swarm_id);
        assert_eq!(back.swarm_key, membership.swarm_key);
        assert_eq!(back.members, membership.members);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sealed_key_opens_only_with_the_recipient_key() {
        let joiner = identity();
        let other = identity();
        let sealed = seal(&joiner.public(), &[9u8; 32]).unwrap();
        assert_eq!(unseal(&joiner, &sealed).unwrap(), vec![9u8; 32]);
        assert!(unseal(&other, &sealed).is_err());
        let mut broken = sealed.clone();
        broken.ciphertext.replace_range(0..2, "00");
        assert!(unseal(&joiner, &broken).is_err());
    }
}
