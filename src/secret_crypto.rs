//! AES-256-GCM two-way encryption for at-rest secrets (S3 creds in
//! `timefusion_projects`). Key is supplied via the
//! `TIMEFUSION_CONFIG_ENCRYPTION_KEY` env var as a base64-encoded 32-byte
//! value. Ciphertext is stored as `enc:v1:<base64(nonce||ct||tag)>`.
//!
//! Plaintext (un-prefixed) rows are still accepted on read so the feature
//! can be rolled out without a forced backfill — re-encrypt with
//! `timefusion encrypt-secret <value>` and UPDATE the row.

use std::sync::OnceLock;

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
};
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine, engine::general_purpose::STANDARD as B64};

pub const ENC_PREFIX: &str = "enc:v1:";
const KEY_ENV: &str = "TIMEFUSION_CONFIG_ENCRYPTION_KEY";
const NONCE_LEN: usize = 12;

static CIPHER: OnceLock<Option<Aes256Gcm>> = OnceLock::new();

fn cipher() -> &'static Option<Aes256Gcm> {
    CIPHER.get_or_init(|| match std::env::var(KEY_ENV) {
        Ok(s) if !s.is_empty() => match B64.decode(s.trim()) {
            Ok(bytes) if bytes.len() == 32 => Some(Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&bytes))),
            Ok(_) => {
                tracing::error!("{} is not 32 bytes after base64 decode; encryption disabled", KEY_ENV);
                None
            }
            Err(e) => {
                tracing::error!("{} is not valid base64 ({}); encryption disabled", KEY_ENV, e);
                None
            }
        },
        _ => None,
    })
}

pub fn key_configured() -> bool {
    cipher().is_some()
}

/// Encrypt a plaintext secret. Errors if no key is configured.
pub fn encrypt(plaintext: &str) -> Result<String> {
    let c = cipher().as_ref().ok_or_else(|| anyhow!("{} not set — cannot encrypt", KEY_ENV))?;
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    let ct = c.encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes()).map_err(|e| anyhow!("AES-GCM encrypt failed: {e}"))?;
    let mut buf = Vec::with_capacity(NONCE_LEN + ct.len());
    buf.extend_from_slice(&nonce);
    buf.extend_from_slice(&ct);
    Ok(format!("{ENC_PREFIX}{}", B64.encode(buf)))
}

/// Decrypt a value loaded from `timefusion_projects`. Pass-through for
/// values without the `enc:v1:` prefix (legacy plaintext rows).
pub fn decrypt_or_passthrough(value: &str) -> Result<String> {
    let Some(rest) = value.strip_prefix(ENC_PREFIX) else {
        return Ok(value.to_string());
    };
    let c = cipher().as_ref().ok_or_else(|| anyhow!("row is encrypted ({ENC_PREFIX}…) but {KEY_ENV} is not set"))?;
    let bytes = B64.decode(rest).context("encrypted secret is not valid base64")?;
    if bytes.len() <= NONCE_LEN {
        bail!("encrypted secret payload too short");
    }
    let (nonce, ct) = bytes.split_at(NONCE_LEN);
    let pt = c
        .decrypt(Nonce::from_slice(nonce), ct)
        .map_err(|e| anyhow!("AES-GCM decrypt failed (key mismatch or tampered ciphertext): {e}"))?;
    String::from_utf8(pt).context("decrypted secret is not valid UTF-8")
}

/// CLI helper: `timefusion encrypt-secret <plaintext>` — encrypts the
/// argument and prints the `enc:v1:…` string for use in SQL inserts.
pub fn run_cli() -> Result<()> {
    let mut args = std::env::args().skip(2); // skip binary + "encrypt-secret"
    let plaintext = args.next().ok_or_else(|| anyhow!("usage: timefusion encrypt-secret <plaintext>"))?;
    println!("{}", encrypt(&plaintext)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_key<F: FnOnce()>(f: F) {
        // OnceCell makes this awkward; rely on env being set before any call.
        let key = B64.encode([7u8; 32]);
        // SAFETY: tests are single-threaded by serial_test elsewhere; this
        // module's tests don't race because OnceCell is set on first use.
        unsafe { std::env::set_var(KEY_ENV, key) };
        f();
    }

    #[test]
    fn roundtrip() {
        with_key(|| {
            let ct = encrypt("AKIAEXAMPLE").unwrap();
            assert!(ct.starts_with(ENC_PREFIX));
            assert_eq!(decrypt_or_passthrough(&ct).unwrap(), "AKIAEXAMPLE");
        });
    }

    #[test]
    fn plaintext_passthrough() {
        with_key(|| {
            assert_eq!(decrypt_or_passthrough("plain").unwrap(), "plain");
        });
    }
}
