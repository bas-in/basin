//! Per-user session settings. Read by the engine for `current_setting()`.
//!
//! v0.1 ships a hard-coded allowlist: `timezone` and `language`. Anything
//! else is rejected at the API boundary — the engine consumer
//! (basin-engine, separate work item) can rely on the set of valid keys
//! being a closed enum.

use std::collections::HashMap;

use basin_common::{BasinError, Result, TenantId};

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
    tenant: &TenantId,
    user: UserId,
    key: &str,
    value: &str,
) -> Result<()> {
    validate_key(key)?;
    validate_value(key, value)?;
    let tenant_str = tenant.to_string();
    let client = inner.client.lock().await;
    client
        .execute(
            &format!(
                "INSERT INTO {sch}.user_session_settings
                   (tenant_id, user_id, key, value)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (tenant_id, user_id, key)
                 DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
                sch = inner.schema()
            ),
            &[&tenant_str, &user, &key, &value],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("session setting upsert: {e}")))?;
    Ok(())
}

pub(crate) async fn get_all(
    inner: &Inner,
    tenant: &TenantId,
    user: UserId,
) -> Result<HashMap<String, String>> {
    let tenant_str = tenant.to_string();
    let client = inner.client.lock().await;
    let rows = client
        .query(
            &format!(
                "SELECT key, value
                 FROM {sch}.user_session_settings
                 WHERE tenant_id = $1 AND user_id = $2",
                sch = inner.schema()
            ),
            &[&tenant_str, &user],
        )
        .await
        .map_err(|e| BasinError::catalog(format!("session setting list: {e}")))?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let k: String = row.get(0);
            let v: String = row.get(1);
            (k, v)
        })
        .collect())
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
