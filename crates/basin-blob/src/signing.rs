//! Blob signed-URL signing-secret management (P2-1).
//!
//! # Security rationale
//!
//! Previously signed URLs were validated against the server's JWT secret
//! (`AuthService::signing_secret()`).  This created a coupling: rotating the
//! JWT secret to invalidate compromised JWTs simultaneously invalidated all
//! outstanding signed URLs (operational pain), while rotating to a *new* blob
//! signing secret required modifying the JWT rotation path (cross-crate
//! coupling).
//!
//! This module introduces a **dedicated** `BlobSigningSecret` that:
//!
//! 1. Lives entirely inside `basin-blob` (no dependency on `basin-auth`).
//! 2. Can be rotated independently of the JWT secret.
//! 3. Exposes a `rotate()` method; after rotation, any token that was minted
//!    with the previous key will **fail** verification because the HMAC is
//!    computed over the new secret.
//!
//! # Cross-crate dependency (required follow-up)
//!
//! `basin-rest/src/routes/storage_sign.rs` currently obtains the signing
//! secret via `state.cfg.auth.signing_secret()` (which returns the raw JWT
//! secret from `basin-auth`).  For P2-1 to be fully effective, `basin-rest`
//! must be updated to:
//!
//! 1. Hold an `Arc<BlobSigningSecret>` in its `Inner` state.
//! 2. Call `BlobSigningSecret::compute_mac` / `BlobSigningSecret::verify_mac`
//!    instead of the JWT secret.
//! 3. Expose a `/storage/v1/admin/rotate-signing-key` endpoint (or a
//!    Kubernetes operator hook) that calls `BlobSigningSecret::rotate()`.
//!
//! **This file delivers the `basin-blob` half of the fix.  The `basin-rest`
//! wiring is a cross-crate follow-up that must NOT be implemented here.**

use std::sync::RwLock;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// BlobSigningSecret
// ---------------------------------------------------------------------------

/// A rotatable HMAC-SHA256 signing secret used exclusively for blob
/// signed-URL tokens.
///
/// Kept separate from the JWT secret so each can be rotated independently.
///
/// # Thread safety
///
/// `BlobSigningSecret` is `Send + Sync`.  The inner key is protected by a
/// `RwLock` so reads (MAC computation) take a shared lock while rotation
/// takes an exclusive lock.  Lock contention is negligible: rotation is a
/// rare administrative operation and MAC computation is sub-microsecond.
///
/// # Usage
///
/// ```no_run
/// use basin_blob::signing::BlobSigningSecret;
///
/// let secret = BlobSigningSecret::new(b"initial-key-32-bytes-or-longer!!");
///
/// // Mint a token.
/// let mac = secret.compute_mac("proj", "bucket", "path/to/file.txt", 9_999_999);
///
/// // Verify a token (constant-time).
/// let ok = secret.verify_mac("proj", "bucket", "path/to/file.txt", 9_999_999, &mac);
/// assert!(ok);
///
/// // Rotate — old MACs are immediately invalid.
/// secret.rotate(b"new-key-32-bytes-or-longer-here!!");
/// let still_ok = secret.verify_mac("proj", "bucket", "path/to/file.txt", 9_999_999, &mac);
/// assert!(!still_ok);
/// ```
#[derive(Debug)]
pub struct BlobSigningSecret {
    inner: RwLock<Vec<u8>>,
}

impl BlobSigningSecret {
    /// Create a new `BlobSigningSecret` seeded with `key`.
    ///
    /// `key` must be non-empty.  Any length is accepted (HMAC-SHA256 accepts
    /// arbitrary key lengths), but ≥32 bytes of high-entropy random material
    /// is recommended.
    ///
    /// # Panics
    ///
    /// Panics if `key` is empty.
    pub fn new(key: &[u8]) -> Self {
        assert!(!key.is_empty(), "BlobSigningSecret: key must not be empty");
        Self {
            inner: RwLock::new(key.to_vec()),
        }
    }

    /// Replace the active signing key.
    ///
    /// After this call:
    /// - All **new** tokens are minted with `new_key`.
    /// - All **old** tokens (minted with the previous key) will fail
    ///   [`Self::verify_mac`].
    ///
    /// This is the rotation hook.  Callers are responsible for ensuring that
    /// no in-flight signed-URL requests are in the middle of a verify call
    /// when rotation occurs; the `RwLock` guarantees memory-safety but not
    /// application-level atomicity across mint/verify pairs that span the
    /// rotation instant.  The recommended deployment pattern is to rotate
    /// on a low-traffic maintenance window or to accept the brief window of
    /// ~outstanding-URL TTL where old URLs silently fail.
    ///
    /// # Panics
    ///
    /// Panics if `new_key` is empty.
    pub fn rotate(&self, new_key: &[u8]) {
        assert!(
            !new_key.is_empty(),
            "BlobSigningSecret::rotate: key must not be empty"
        );
        let mut guard = self.inner.write().expect("BlobSigningSecret lock poisoned");
        guard.clear();
        guard.extend_from_slice(new_key);
    }

    /// Compute an HMAC-SHA256 token over the canonical
    /// `(project, bucket, path, expires_unix_secs)` tuple.
    ///
    /// Returns the raw MAC bytes (32 bytes for SHA-256).
    pub fn compute_mac(&self, project: &str, bucket: &str, path: &str, expires: i64) -> Vec<u8> {
        let guard = self.inner.read().expect("BlobSigningSecret lock poisoned");
        Self::hmac_inner(&guard, project, bucket, path, expires)
    }

    /// Verify a candidate MAC against the current key (constant-time).
    ///
    /// Returns `true` iff:
    /// - `candidate` has the correct length (32 bytes for SHA-256).
    /// - `candidate` matches the MAC computed from the current key and the
    ///   given `(project, bucket, path, expires)` tuple.
    ///
    /// Returns `false` — never errors — for any mismatch, including wrong
    /// length or a token minted against a rotated-away key.
    pub fn verify_mac(
        &self,
        project: &str,
        bucket: &str,
        path: &str,
        expires: i64,
        candidate: &[u8],
    ) -> bool {
        let guard = self.inner.read().expect("BlobSigningSecret lock poisoned");
        let expected = Self::hmac_inner(&guard, project, bucket, path, expires);
        if expected.len() != candidate.len() {
            return false;
        }
        expected.as_slice().ct_eq(candidate).into()
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn hmac_inner(key: &[u8], project: &str, bucket: &str, path: &str, expires: i64) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(project.as_bytes());
        mac.update(b"\n");
        mac.update(bucket.as_bytes());
        mac.update(b"\n");
        mac.update(path.as_bytes());
        mac.update(b"\n");
        mac.update(expires.to_string().as_bytes());
        mac.finalize().into_bytes().to_vec()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_A: &[u8] = b"key-A-32-bytes-long-enough------";
    const KEY_B: &[u8] = b"key-B-32-bytes-long-enough------";

    fn make_secret() -> BlobSigningSecret {
        BlobSigningSecret::new(KEY_A)
    }

    #[test]
    fn mac_is_deterministic() {
        let s = make_secret();
        let m1 = s.compute_mac("proj", "bucket", "file.txt", 9999);
        let m2 = s.compute_mac("proj", "bucket", "file.txt", 9999);
        assert_eq!(m1, m2);
    }

    #[test]
    fn verify_succeeds_with_correct_mac() {
        let s = make_secret();
        let mac = s.compute_mac("proj", "bucket", "file.txt", 9999);
        assert!(s.verify_mac("proj", "bucket", "file.txt", 9999, &mac));
    }

    #[test]
    fn verify_fails_with_wrong_expiry() {
        let s = make_secret();
        let mac = s.compute_mac("proj", "bucket", "file.txt", 9999);
        assert!(!s.verify_mac("proj", "bucket", "file.txt", 10000, &mac));
    }

    #[test]
    fn verify_fails_with_wrong_path() {
        let s = make_secret();
        let mac = s.compute_mac("proj", "bucket", "a.txt", 9999);
        assert!(!s.verify_mac("proj", "bucket", "b.txt", 9999, &mac));
    }

    #[test]
    fn verify_fails_with_tampered_mac() {
        let s = make_secret();
        let mut mac = s.compute_mac("proj", "bucket", "file.txt", 9999);
        mac[0] ^= 0xFF;
        assert!(!s.verify_mac("proj", "bucket", "file.txt", 9999, &mac));
    }

    // ── P2-1 rotation ─────────────────────────────────────────────────────

    /// Audit P2-1: signed URL minted before rotation must fail after rotation.
    ///
    /// This test is the unit-level proof that the `BlobSigningSecret::rotate`
    /// hook satisfies the security property described in audit finding P2-1.
    /// The integration test in `tests/integration/tests/security.rs` will be
    /// un-ignored once `basin-rest` is wired to use `BlobSigningSecret`
    /// instead of the JWT secret.
    #[test]
    fn rotation_invalidates_old_macs() {
        let s = make_secret();

        // Mint a token before rotation.
        let old_mac = s.compute_mac("proj", "bucket", "file.txt", 9999);
        assert!(
            s.verify_mac("proj", "bucket", "file.txt", 9999, &old_mac),
            "token must be valid before rotation"
        );

        // Rotate to a new key.
        s.rotate(KEY_B);

        // Old token is now rejected.
        assert!(
            !s.verify_mac("proj", "bucket", "file.txt", 9999, &old_mac),
            "token minted before rotation must be rejected after rotation"
        );
    }

    #[test]
    fn new_macs_work_after_rotation() {
        let s = make_secret();
        s.rotate(KEY_B);

        let new_mac = s.compute_mac("proj", "bucket", "file.txt", 9999);
        assert!(s.verify_mac("proj", "bucket", "file.txt", 9999, &new_mac));
    }

    #[test]
    fn multiple_rotations_work() {
        let s = make_secret();
        s.rotate(KEY_B);
        s.rotate(KEY_A);

        let mac = s.compute_mac("proj", "bucket", "f", 1);
        assert!(s.verify_mac("proj", "bucket", "f", 1, &mac));
    }

    #[test]
    #[should_panic]
    fn new_with_empty_key_panics() {
        BlobSigningSecret::new(b"");
    }

    #[test]
    #[should_panic]
    fn rotate_with_empty_key_panics() {
        let s = make_secret();
        s.rotate(b"");
    }
}
