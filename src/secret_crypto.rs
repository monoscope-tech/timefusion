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
    AeadCore, Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, OsRng},
};
use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD as B64};

pub const ENC_PREFIX: &str = "enc:v1:";
const KEY_ENV: &str = "TIMEFUSION_CONFIG_ENCRYPTION_KEY";
const NONCE_LEN: usize = 12;

static CIPHER: OnceLock<Option<Aes256Gcm>> = OnceLock::new();

fn cipher() -> Option<&'static Aes256Gcm> {
    CIPHER
        .get_or_init(|| {
            let raw = std::env::var(KEY_ENV).ok().filter(|s| !s.is_empty())?;
            B64.decode(raw.trim())
                .map_err(|e| anyhow!("is not valid base64 ({e})"))
                .and_then(|b| <[u8; 32]>::try_from(b).map_err(|_| anyhow!("is not 32 bytes after base64 decode")))
                .map(|b| Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&b)))
                .inspect_err(|e| tracing::error!("{KEY_ENV} {e}; encryption disabled"))
                .ok()
        })
        .as_ref()
}

pub fn key_configured() -> bool {
    cipher().is_some()
}

/// Encrypt a plaintext secret. Errors if no key is configured.
pub fn encrypt(plaintext: &str) -> Result<String> {
    let c = cipher().ok_or_else(|| anyhow!("{KEY_ENV} not set — cannot encrypt"))?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ct = c.encrypt(&nonce, plaintext.as_bytes()).map_err(|e| anyhow!("AES-GCM encrypt failed: {e}"))?;
    Ok(format!("{ENC_PREFIX}{}", B64.encode([nonce.as_slice(), ct.as_slice()].concat())))
}

/// Decrypt a value loaded from `timefusion_projects`. Pass-through for
/// values without the `enc:v1:` prefix (legacy plaintext rows).
pub fn decrypt_or_passthrough(value: &str) -> Result<String> {
    let Some(rest) = value.strip_prefix(ENC_PREFIX) else {
        return Ok(value.to_string());
    };
    let c = cipher().ok_or_else(|| anyhow!("row is encrypted ({ENC_PREFIX}…) but {KEY_ENV} is not set"))?;
    let bytes = B64.decode(rest).context("encrypted secret is not valid base64")?;
    let (nonce, ct) = bytes.split_at_checked(NONCE_LEN).filter(|(_, ct)| !ct.is_empty()).context("encrypted secret payload too short")?;
    let pt = c.decrypt(Nonce::from_slice(nonce), ct).map_err(|e| anyhow!("AES-GCM decrypt failed (key mismatch or tampered ciphertext): {e}"))?;
    String::from_utf8(pt).context("decrypted secret is not valid UTF-8")
}

/// CLI helper: `timefusion encrypt-secret <plaintext>` — encrypts the
/// argument and prints the `enc:v1:…` string for use in SQL inserts.
pub fn run_cli() -> Result<()> {
    // nth(2): skips binary + "encrypt-secret"
    let plaintext = std::env::args().nth(2).ok_or_else(|| anyhow!("usage: timefusion encrypt-secret <plaintext>"))?;
    println!("{}", encrypt(&plaintext)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    // CIPHER is a OnceLock, so the key must be in the env before this
    // process's first cipher() call; #[serial] keeps that ordering race-free
    // against other set_var tests in this binary.
    #[test]
    #[serial]
    fn roundtrip_and_plaintext_passthrough() {
        // SAFETY: #[serial] guarantees no other test in this binary mutates
        // env concurrently.
        unsafe { std::env::set_var(KEY_ENV, B64.encode([7u8; 32])) };
        let ct = encrypt("AKIAEXAMPLE").unwrap();
        assert!(ct.starts_with(ENC_PREFIX));
        assert_eq!(decrypt_or_passthrough(&ct).unwrap(), "AKIAEXAMPLE");
        assert_eq!(decrypt_or_passthrough("plain").unwrap(), "plain");
        // nonce-only payload => no ciphertext left after the split
        assert!(decrypt_or_passthrough(&format!("{ENC_PREFIX}{}", B64.encode([0u8; NONCE_LEN]))).is_err());
    }
}
