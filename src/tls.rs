use std::{fs, path::Path};

use anyhow::{Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let data = fs::read(path).with_context(|| format!("read {path:?}"))?;
    let certs = rustls_pemfile::certs(&mut &*data)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parse certs in {path:?}"))?;
    anyhow::ensure!(!certs.is_empty(), "no certificates in {path:?}");
    Ok(certs)
}

pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let data = fs::read(path).with_context(|| format!("read {path:?}"))?;
    rustls_pemfile::private_key(&mut &*data)
        .with_context(|| format!("parse key in {path:?}"))?
        .with_context(|| format!("no private key in {path:?}"))
}

/// jetstream_quic builds its rustls configs with the process-wide provider.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
