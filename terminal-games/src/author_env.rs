// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::sync::OnceLock;

use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce, aead::Aead};
use hkdf::Hkdf;
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tracing::warn;

pub const AUTHOR_ENV_KEY_ENV: &str = "AUTHOR_ENV_SECRET_KEY";
const AUTHOR_ENV_DEV_FALLBACK_KEY: &str = "terminal-games-dev-author-env-key";
pub const MAX_AUTHOR_ENVS: usize = 64;
pub const MAX_AUTHOR_ENV_NAME_BYTES: usize = 64;
pub const MAX_AUTHOR_ENV_VALUE_BYTES: usize = 8 * 1024;
pub const MAX_AUTHOR_ENV_TOTAL_BYTES: usize = 64 * 1024;
pub const AUTHOR_ENV_SALT_BYTES: usize = 16;
pub const AUTHOR_ENV_BLOB_BYTES: usize = 64 * 1024;
const AUTHOR_ENV_TAG_BYTES: usize = 16;
const AUTHOR_ENV_PLAINTEXT_BYTES: usize = AUTHOR_ENV_BLOB_BYTES - AUTHOR_ENV_TAG_BYTES;
const AUTHOR_ENV_KEY_INFO: &[u8] = b"terminal-games author env key v1";
const AUTHOR_ENV_NONCE_INFO: &[u8] = b"terminal-games author env nonce v1";
const AUTHOR_ENV_LENGTH_BYTES: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthorEnvVar {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct EncryptedAuthorEnvBlob {
    pub salt: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

pub fn validate_author_envs(envs: &[AuthorEnvVar]) -> anyhow::Result<()> {
    anyhow::ensure!(
        envs.len() <= MAX_AUTHOR_ENVS,
        "too many env vars (max {})",
        MAX_AUTHOR_ENVS
    );
    let mut seen = std::collections::BTreeSet::new();
    let mut total_bytes = 0usize;
    for env in envs {
        validate_author_env_name(&env.name)?;
        anyhow::ensure!(
            env.value.len() <= MAX_AUTHOR_ENV_VALUE_BYTES,
            "env value for '{}' exceeds {} bytes",
            env.name,
            MAX_AUTHOR_ENV_VALUE_BYTES
        );
        anyhow::ensure!(
            seen.insert(env.name.clone()),
            "duplicate env var '{}'",
            env.name
        );
        total_bytes = total_bytes.saturating_add(env.name.len() + env.value.len());
    }
    anyhow::ensure!(
        total_bytes <= MAX_AUTHOR_ENV_TOTAL_BYTES,
        "env vars exceed {} total bytes",
        MAX_AUTHOR_ENV_TOTAL_BYTES
    );
    Ok(())
}

pub fn validate_author_env_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.is_empty(), "env name cannot be empty");
    anyhow::ensure!(
        name.len() <= MAX_AUTHOR_ENV_NAME_BYTES,
        "env name '{}' exceeds {} bytes",
        name,
        MAX_AUTHOR_ENV_NAME_BYTES
    );
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        anyhow::bail!("env name cannot be empty");
    };
    anyhow::ensure!(
        first == '_' || first.is_ascii_alphabetic(),
        "env name '{}' must start with a letter or underscore",
        name
    );
    anyhow::ensure!(
        chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()),
        "env name '{}' must contain only letters, numbers, and underscores",
        name
    );
    Ok(())
}

pub fn encrypt_author_env_blob(envs: &[AuthorEnvVar]) -> anyhow::Result<EncryptedAuthorEnvBlob> {
    validate_author_envs(envs)?;
    let encoded = serde_json::to_vec(envs)?;
    anyhow::ensure!(
        encoded.len() + AUTHOR_ENV_LENGTH_BYTES <= AUTHOR_ENV_PLAINTEXT_BYTES,
        "env vars exceed fixed encrypted blob capacity"
    );

    let mut plaintext = vec![0u8; AUTHOR_ENV_PLAINTEXT_BYTES];
    plaintext[..AUTHOR_ENV_LENGTH_BYTES]
        .copy_from_slice(&(encoded.len() as u32).to_le_bytes());
    plaintext[AUTHOR_ENV_LENGTH_BYTES..AUTHOR_ENV_LENGTH_BYTES + encoded.len()]
        .copy_from_slice(&encoded);
    OsRng.fill_bytes(&mut plaintext[AUTHOR_ENV_LENGTH_BYTES + encoded.len()..]);

    let mut salt = [0u8; AUTHOR_ENV_SALT_BYTES];
    OsRng.fill_bytes(&mut salt);
    let (cipher, nonce) = cipher_and_nonce_from_salt(&salt)?;
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_slice())
        .map_err(|_| anyhow::anyhow!("failed to encrypt env blob"))?;
    anyhow::ensure!(
        ciphertext.len() == AUTHOR_ENV_BLOB_BYTES,
        "unexpected encrypted env blob size {}",
        ciphertext.len()
    );
    Ok(EncryptedAuthorEnvBlob {
        salt: salt.to_vec(),
        ciphertext,
    })
}

pub fn decrypt_author_env_blob(salt: &[u8], ciphertext: &[u8]) -> anyhow::Result<Vec<AuthorEnvVar>> {
    anyhow::ensure!(
        salt.len() == AUTHOR_ENV_SALT_BYTES,
        "invalid env salt length {}",
        salt.len()
    );
    anyhow::ensure!(
        ciphertext.len() == AUTHOR_ENV_BLOB_BYTES,
        "invalid env blob length {}",
        ciphertext.len()
    );
    let (cipher, nonce) = cipher_and_nonce_from_salt(salt)?;
    let plaintext = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| anyhow::anyhow!("failed to decrypt env blob"))?;
    anyhow::ensure!(
        plaintext.len() == AUTHOR_ENV_PLAINTEXT_BYTES,
        "invalid decrypted env blob length {}",
        plaintext.len()
    );
    let encoded_len = u32::from_le_bytes(
        plaintext[..AUTHOR_ENV_LENGTH_BYTES]
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid env blob length prefix"))?,
    ) as usize;
    anyhow::ensure!(
        encoded_len + AUTHOR_ENV_LENGTH_BYTES <= plaintext.len(),
        "invalid env blob payload length {}",
        encoded_len
    );
    let envs = serde_json::from_slice::<Vec<AuthorEnvVar>>(
        &plaintext[AUTHOR_ENV_LENGTH_BYTES..AUTHOR_ENV_LENGTH_BYTES + encoded_len],
    )?;
    validate_author_envs(&envs)?;
    Ok(envs)
}

fn cipher_and_nonce_from_salt(salt: &[u8]) -> anyhow::Result<(XChaCha20Poly1305, XNonce)> {
    let hk = Hkdf::<Sha256>::new(Some(salt), author_env_secret_key().as_bytes());
    let mut key = [0u8; 32];
    hk.expand(AUTHOR_ENV_KEY_INFO, &mut key)
        .map_err(|_| anyhow::anyhow!("failed to derive author env encryption key"))?;
    let mut nonce = [0u8; 24];
    hk.expand(AUTHOR_ENV_NONCE_INFO, &mut nonce)
        .map_err(|_| anyhow::anyhow!("failed to derive author env nonce"))?;
    Ok((
        XChaCha20Poly1305::new(&key.into()),
        *XNonce::from_slice(&nonce),
    ))
}

fn author_env_secret_key() -> &'static str {
    static KEY: OnceLock<String> = OnceLock::new();
    KEY.get_or_init(|| match std::env::var(AUTHOR_ENV_KEY_ENV) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            warn!(
                "{AUTHOR_ENV_KEY_ENV} is not set; using the built-in development fallback key"
            );
            AUTHOR_ENV_DEV_FALLBACK_KEY.to_string()
        }
    })
}
