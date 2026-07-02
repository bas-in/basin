//! Password hashing & verification.
//!
//! Thin wrappers over `bcrypt`. `bcrypt::verify` already runs in constant time
//! relative to the stored hash, so we don't need a separate `subtle` check
//! for password comparison.

use basin_common::{BasinError, Result};

/// Hash a password with the configured bcrypt cost factor.
pub fn hash(password: &str, cost: u32) -> Result<String> {
    bcrypt::hash(password, cost).map_err(|e| BasinError::internal(format!("bcrypt hash: {e}")))
}

/// Verify a password against a previously stored hash. Constant-time relative
/// to the hash structure (bcrypt's own implementation).
pub fn verify(password: &str, hash: &str) -> Result<bool> {
    bcrypt::verify(password, hash).map_err(|e| BasinError::internal(format!("bcrypt verify: {e}")))
}

/// Perform a bcrypt verify against a process-static dummy hash and discard the
/// result. This exists purely for its *timing*: the credential lookup returns
/// early on an unknown username, so without this the "no such user" path would
/// be measurably faster than the "user exists, wrong password" path — letting an
/// attacker enumerate valid `pgwire_user` strings (which embed the project id)
/// by response latency. Calling this on the not-found branch equalizes the two.
///
/// The dummy hash is computed once at `cost` — the same work factor real
/// credentials use — and cached. If that one-time hash somehow fails (only an
/// invalid cost can do that, which config already rejects) the call is a no-op
/// rather than a panic on the auth hot path.
pub fn verify_dummy(password: &str, cost: u32) {
    static DUMMY: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    let dummy = DUMMY.get_or_init(|| hash("bcrypt-timing-equalizer", cost).ok());
    if let Some(h) = dummy {
        let _ = bcrypt::verify(password, h);
    }
}

/// Reject passwords shorter than `min_len` bytes. Length is checked in bytes,
/// not chars, because attackers measure storage too.
pub fn check_length(password: &str, min_len: usize) -> Result<()> {
    if password.len() < min_len {
        return Err(BasinError::InvalidIdent(format!(
            "password must be at least {min_len} characters"
        )));
    }
    // bcrypt truncates anything past 72 bytes silently; warn-fail at 1024 so
    // we don't waste server-side hash compute on absurd inputs.
    if password.len() > 1024 {
        return Err(BasinError::InvalidIdent(
            "password is unreasonably long (>1024 bytes)".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify() {
        let h = hash("hunter2hunter2", 4).unwrap();
        assert!(verify("hunter2hunter2", &h).unwrap());
        assert!(!verify("nope", &h).unwrap());
        assert_ne!(h, "hunter2hunter2");
    }

    #[test]
    fn check_length_enforces_min() {
        assert!(check_length("short", 10).is_err());
        check_length("longenoughpassword", 10).unwrap();
    }

    #[test]
    fn check_length_rejects_huge() {
        let huge = "a".repeat(2048);
        assert!(check_length(&huge, 10).is_err());
    }

    #[test]
    fn verify_dummy_completes_without_error() {
        // The unknown-user auth branch calls this to pay bcrypt's timing cost so
        // response latency can't distinguish it from a wrong-password attempt.
        // cost 4 keeps the test fast; it must run the dummy bcrypt verify (and
        // discard the result) without panicking, for any input including empty.
        verify_dummy("any password", 4);
        verify_dummy("", 4);
    }
}
