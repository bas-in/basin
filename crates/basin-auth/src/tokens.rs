//! Opaque token generation, hashing, and constant-time comparison.
//!
//! Refresh tokens, email-verification tokens, password-reset tokens, and
//! magic-link tokens all share the same wire shape: a long random hex string
//! sent to the user, with only the sha256 hash stored in Postgres.
//!
//! The DB never sees the raw token, so a database leak does not directly
//! leak valid tokens. Lookups go via `sha256(presented).0` as the primary
//! key, then `subtle::ConstantTimeEq` re-checks the bytes (defence in depth
//! against any future migration to a non-PK lookup).

use rand::RngCore;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Length of the raw token (in bytes of entropy). 32 bytes = 256 bits;
/// rendered as 64 hex chars on the wire.
pub const TOKEN_BYTES: usize = 32;

/// What an email-token row in the DB is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmailTokenPurpose {
    Verify,
    Reset,
    MagicLink,
}

impl EmailTokenPurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            EmailTokenPurpose::Verify => "verify",
            EmailTokenPurpose::Reset => "reset",
            EmailTokenPurpose::MagicLink => "magic_link",
        }
    }
}

/// Generate a fresh opaque token. Returns `(raw_token, sha256_hex)`. The raw
/// is what we send to the user; the hash is what we store.
pub fn generate() -> (String, String) {
    let mut buf = [0u8; TOKEN_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    let raw = hex::encode(buf);
    let hash = hash_token(&raw);
    (raw, hash)
}

/// `hex(sha256(token.as_bytes()))`. Stable across processes; safe to use as a
/// Postgres primary key.
pub fn hash_token(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

/// Constant-time equality on token-hash strings. Defence in depth against
/// timing oracles in any code path that doesn't use the hash as a PK.
pub fn ct_eq(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_round_trips() {
        let (raw, hash) = generate();
        assert_eq!(raw.len(), TOKEN_BYTES * 2, "raw is hex of TOKEN_BYTES");
        assert_eq!(hash.len(), 64, "sha256 hex is 64 chars");
        assert_eq!(hash_token(&raw), hash);
    }

    #[test]
    fn ct_eq_matches_eq() {
        assert!(ct_eq("abc", "abc"));
        assert!(!ct_eq("abc", "abd"));
        assert!(!ct_eq("abc", "abcd"));
    }

    #[test]
    fn distinct_tokens_distinct_hashes() {
        let (_, h1) = generate();
        let (_, h2) = generate();
        assert_ne!(h1, h2);
    }
}
