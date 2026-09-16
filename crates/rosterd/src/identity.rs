//! Node identity, R7.1: an Ed25519 keypair generated at first start under the config directory.
//! `node_id` is the hex SHA-256 fingerprint of the public key, truncated to 16 bytes.

use std::path::Path;

use anyhow::{Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

use crate::config::write_private;

#[derive(Clone)]
pub struct Identity {
    key: SigningKey,
    pub node_id: String,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity").field("node_id", &self.node_id).finish()
    }
}

pub fn node_id_of(public: &VerifyingKey) -> String {
    hex::encode(&Sha256::digest(public.as_bytes())[..16])
}

impl Identity {
    /// Reads `node.key` (32 raw secret bytes, mode 0600) or creates it.
    pub fn load_or_create(config_dir: &Path) -> Result<Identity> {
        let path = config_dir.join("node.key");
        let key = if path.exists() {
            let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
            let bytes: [u8; 32] = bytes.as_slice().try_into().context("node.key is not 32 bytes")?;
            SigningKey::from_bytes(&bytes)
        } else {
            let key = SigningKey::generate(&mut rand::rngs::OsRng);
            write_private(&path, key.as_bytes())?;
            key
        };
        Ok(Identity::from_key(key))
    }

    pub fn from_key(key: SigningKey) -> Identity {
        let node_id = node_id_of(&key.verifying_key());
        Identity { key, node_id }
    }

    pub fn public(&self) -> VerifyingKey {
        self.key.verifying_key()
    }

    pub fn public_hex(&self) -> String {
        hex::encode(self.public().as_bytes())
    }

    pub fn sign(&self, message: &[u8]) -> Signature {
        self.key.sign(message)
    }

    /// The raw signing key, for the TLS certificate that pins to it, R7.6.
    pub fn signing_key(&self) -> &SigningKey {
        &self.key
    }
}

pub fn verify(public_hex: &str, message: &[u8], signature_hex: &str) -> Result<VerifyingKey> {
    let public: [u8; 32] = hex::decode(public_hex)?.as_slice().try_into().context("public key length")?;
    let public = VerifyingKey::from_bytes(&public)?;
    let signature: [u8; 64] = hex::decode(signature_hex)?.as_slice().try_into().context("signature length")?;
    public.verify(message, &Signature::from_bytes(&signature))?;
    Ok(public)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_survives_reload_and_signatures_verify() {
        let dir = std::env::temp_dir().join(format!("rosterd-id-{}", std::process::id()));
        let a = Identity::load_or_create(&dir).unwrap();
        let b = Identity::load_or_create(&dir).unwrap();
        assert_eq!(a.node_id, b.node_id);
        assert_eq!(a.node_id.len(), 32);
        let sig = a.sign(b"hello");
        assert!(verify(&a.public_hex(), b"hello", &hex::encode(sig.to_bytes())).is_ok());
        assert!(verify(&a.public_hex(), b"hellp", &hex::encode(sig.to_bytes())).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
