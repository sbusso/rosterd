//! Trust, R7.6: a self-signed certificate whose key is the node key, and a client that accepts
//! a peer's certificate only when its public key is one this node expects.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::identity::Identity;

/// The node's certificate and key, DER. Built once at start.
#[derive(Clone)]
pub struct NodeCert {
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
}

pub fn node_cert(identity: &Identity, name: &str) -> Result<NodeCert> {
    let key = identity.signing_key().to_pkcs8_der().context("node key to pkcs8")?.as_bytes().to_vec();
    let key_pair = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(&PrivatePkcs8KeyDer::from(key.clone()), &rcgen::PKCS_ED25519)
        .context("node key for rcgen")?;
    // SAN is the node name when it is a valid DNS name, else the node id. Nothing checks it:
    // peers pin the key, not the name.
    let san = rcgen::CertificateParams::new(vec![name.to_string()])
        .or_else(|_| rcgen::CertificateParams::new(vec![identity.node_id.clone()]))
        .context("certificate params")?;
    let cert = san.self_signed(&key_pair).context("self-signed certificate")?;
    Ok(NodeCert { cert: cert.der().to_vec(), key })
}

pub fn server_config(cert: &NodeCert) -> Result<RustlsConfig> {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(vec![CertificateDer::from(cert.cert.clone())], PrivateKeyDer::Pkcs8(cert.key.clone().into()))
        .context("server tls")?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(RustlsConfig::from_config(Arc::new(config)))
}

/// Answers whether a peer certificate's SubjectPublicKeyInfo DER is one we expect right now.
pub type PinCheck = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

struct Pinned {
    allowed: PinCheck,
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl std::fmt::Debug for Pinned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Pinned")
    }
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let spki = ParsedCertificate::try_from(end_entity)?.subject_public_key_info();
        if (self.allowed)(spki.as_ref()) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::ApplicationVerificationFailure))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// The rustls side of `client`, shared with the websocket client of the attach relay, R9.
pub fn client_config(allowed: PinCheck) -> Result<Arc<rustls::ClientConfig>> {
    let provider = rustls::crypto::ring::default_provider();
    let verifier = Pinned { allowed, algorithms: provider.signature_verification_algorithms };
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// One client for every peer request. No overall timeout: the /events subscription lives for
/// hours; callers put one on each short request.
pub fn client(allowed: PinCheck) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .use_preconfigured_tls((*client_config(allowed)?).clone())
        .connect_timeout(Duration::from_secs(5))
        .no_proxy()
        .build()
        .context("peer client")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ed25519_dalek::SigningKey;

    use super::*;
    use crate::mesh::membership::spki_der;

    fn identity() -> Identity {
        Identity::from_key(SigningKey::generate(&mut rand::rngs::OsRng))
    }

    #[test]
    fn certificate_is_refused_unless_its_key_is_the_expected_one() {
        let a = identity();
        let b = identity();
        let cert = node_cert(&a, "gibson").unwrap();
        let der = CertificateDer::from(cert.cert.clone());
        let expect = |id: &Identity| {
            let spki = spki_der(&id.public_hex()).unwrap();
            Pinned {
                allowed: Arc::new(move |seen: &[u8]| seen == spki.as_slice()),
                algorithms: rustls::crypto::ring::default_provider().signature_verification_algorithms,
            }
        };
        let name = ServerName::try_from("127.0.0.1").unwrap();
        assert!(expect(&a).verify_server_cert(&der, &[], &name, &[], UnixTime::now()).is_ok());
        assert!(expect(&b).verify_server_cert(&der, &[], &name, &[], UnixTime::now()).is_err());
        // A name the DNS rules refuse still yields a certificate.
        assert!(node_cert(&a, "Mato's MacBook").is_ok());
    }
}
