//! Per-user session settings. Read by the engine for `current_setting()`.
//!
//! v0.1 ships a hard-coded allowlist: `timezone` and `language`. Anything
//! else is rejected at the API boundary — the engine consumer
//! (basin-engine, separate work item) can rely on the set of valid keys
//! being a closed enum.

use std::collections::HashMap;

use basin_common::{BasinError, Result, ProjectId};

use crate::{Inner, UserId};

/// Allowed session-setting keys. Adding a new key is a v-bump because
/// the engine's `current_setting()` consumer reads this list verbatim.
pub const ALLOWED_KEYS: &[&str] = &["timezone", "language"];

fn validate_key(key: &str) -> Result<()> {
    if !ALLOWED_KEYS.contains(&key) {
        return Err(BasinError::InvalidIdent(format!(
            "unknown session setting {key:?}; allowed: {ALLOWED_KEYS:?}"
        )));
    }
    Ok(())
}

fn validate_value(key: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(BasinError::InvalidIdent(format!(
            "session setting {key:?} value is empty"
        )));
    }
    if value.len() > 256 {
        return Err(BasinError::InvalidIdent(format!(
            "session setting {key:?} value > 256 chars"
        )));
    }
    match key {
        // v0.1 accepts any non-empty string; engine-side validation is the
        // gate (chrono_tz parsing happens at `current_setting('timezone')`
        // resolution, not here, to keep the deps surface narrow).
        "timezone" => Ok(()),
        "language" => {
            // BCP 47 lite: `xx` or `xx-XX`. Rejecting anything else avoids
            // surprising the engine consumer with `Locale::from_str` panics.
            let bytes = value.as_bytes();
            let ok = matches!(bytes.len(), 2 | 5)
                && bytes[..2].iter().all(|c| c.is_ascii_lowercase())
                && (bytes.len() == 2
                    || (bytes[2] == b'-' && bytes[3..].iter().all(|c| c.is_ascii_uppercase())));
            if !ok {
                return Err(BasinError::InvalidIdent(format!(
                    "language must match `xx` or `xx-XX`: {value:?}"
                )));
            }
            Ok(())
        }
        _ => unreachable!("validate_key already gated this"),
    }
}

pub(crate) async fn set(
    inner: &Inner,
    project: &ProjectId,
    user: UserId,
    key: &str,
    value: &str,
) -> Result<()> {
    validate_key(key)?;
    validate_value(key, value)?;
    inner
        .store
        .upsert_session_setting(project, user, key, value)
        .await
}

pub(crate) async fn get_all(
    inner: &Inner,
    project: &ProjectId,
    user: UserId,
) -> Result<HashMap<String, String>> {
    inner.store.list_session_settings(project, user).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_keys() {
        assert!(validate_key("timezone").is_ok());
        assert!(validate_key("language").is_ok());
        assert!(validate_key("nope").is_err());
    }

    #[test]
    fn validate_language_values() {
        validate_value("language", "en").unwrap();
        validate_value("language", "en-US").unwrap();
        validate_value("language", "pt-BR").unwrap();
        assert!(validate_value("language", "en_US").is_err());
        assert!(validate_value("language", "english").is_err());
        assert!(validate_value("language", "EN").is_err());
        assert!(validate_value("language", "en-us").is_err());
        assert!(validate_value("language", "").is_err());
    }

    #[test]
    fn validate_timezone_values() {
        validate_value("timezone", "America/New_York").unwrap();
        validate_value("timezone", "UTC").unwrap();
        assert!(validate_value("timezone", "").is_err());
    }
}
