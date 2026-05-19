//! `ALTER PROJECT` DDL surface.
//!
//! Phase 5.14.C5 adds one `ALTER PROJECT` form:
//!
//! ```sql
//! ALTER PROJECT <name> SET basin.memtable_hard_cap = <n>
//! ```
//!
//! `<n>` accepts a bare integer (bytes) or a human-readable suffix
//! (`256MB`, `1GB`, etc.). The value is validated (1 MiB ≤ n ≤ 64 GiB) and
//! persisted in the project's `ProjectStorageConfig::provider_extras` under the
//! key `"basin.memtable_hard_cap"`. The engine reads the per-project override
//! at hot-tier registration time (Phase 5.14.C2+ wiring) and passes it to
//! [`basin_hottier::MemTableRegistry`].
//!
//! # Why `provider_extras`?
//!
//! `ProjectStorageConfig::provider_extras` is an opaque `BTreeMap<String,
//! String>` the catalog already persists per-project without any schema
//! migration.  Storing the memtable cap there avoids adding a new catalog
//! column / migration for a single scalar that is read only at session open
//! time.

use basin_common::{BasinError, Result};
use basin_hottier::budget::{parse_byte_size, MemTableConfig};

use crate::ProjectSession;

// ── textual matcher ───────────────────────────────────────────────────────────

/// Pre-screen `sql` for `ALTER PROJECT <name> SET basin.memtable_hard_cap = <n>`.
///
/// Returns `Ok(Some((project_name, bytes)))` on a match, `Ok(None)` if the
/// statement is not this form, or `Err` if it looked like ours but was
/// malformed.
///
/// The check is deliberately conservative: only statements that start with
/// `ALTER PROJECT` (case-insensitive) trigger the path.
pub(crate) fn match_alter_project_memtable_cap(
    sql: &str,
) -> Result<Option<(String, u64)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();

    if !lower.starts_with("alter project") {
        return Ok(None);
    }

    let after = trimmed["alter project".len()..].trim_start();

    // Read the project name (bare identifier; we don't support quoted names here).
    let (project_name, rest) = read_ident(after)?;

    // Expect `SET` next.
    let rest = rest.trim_start();
    let rest_lower = rest.to_ascii_lowercase();
    let after_set = match strip_prefix_ci(rest, &rest_lower, "set") {
        Some(s) => s.trim_start(),
        None => {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER PROJECT {project_name}: expected SET, got {:?}",
                rest.split_whitespace().next().unwrap_or("")
            )));
        }
    };

    // Expect `basin.memtable_hard_cap`.
    let after_set_lower = after_set.to_ascii_lowercase();
    let after_key = match strip_prefix_ci(after_set, &after_set_lower, "basin.memtable_hard_cap") {
        Some(s) => s.trim_start(),
        None => {
            // Unrecognised SET key — fall through (return None so the caller
            // doesn't treat this as a handled statement).
            return Ok(None);
        }
    };

    // Expect `= <value>`.
    if !after_key.starts_with('=') {
        return Err(BasinError::InvalidSchema(
            "ALTER PROJECT … SET basin.memtable_hard_cap: expected '='".into(),
        ));
    }
    let value_str = after_key[1..].trim();

    let bytes = parse_byte_size(value_str).ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "ALTER PROJECT … SET basin.memtable_hard_cap: cannot parse {:?} as a byte size \
             (accepted forms: integer bytes, or NKB / NMB / NGB e.g. 256MB)",
            value_str
        ))
    })?;

    Ok(Some((project_name, bytes)))
}

// ── executor ──────────────────────────────────────────────────────────────────

/// Apply `ALTER PROJECT <name> SET basin.memtable_hard_cap = <bytes>`.
///
/// Validates the cap, then persists it in `ProjectStorageConfig::provider_extras`
/// under the key `"basin.memtable_hard_cap"`.  The registry live in-process is
/// NOT updated here because it is constructed once at engine startup; per-project
/// overrides are picked up at the next session-open (Phase 5.14.C2 wiring).
pub(crate) async fn exec_alter_project_memtable_cap(
    sess: &ProjectSession,
    project_name: &str,
    hard_cap_bytes: u64,
) -> Result<()> {
    // Validate the cap.
    MemTableConfig::validate_hard_cap(hard_cap_bytes).map_err(|msg| {
        BasinError::InvalidSchema(format!(
            "ALTER PROJECT {project_name} SET basin.memtable_hard_cap: {msg}"
        ))
    })?;

    // Load the current config (or default) and update the key.
    let catalog = sess.engine.config().catalog.clone();
    let project = &sess.project;

    let mut config = catalog
        .get_project_storage_config(project)
        .await?
        .unwrap_or_default();

    config.provider_extras.insert(
        "basin.memtable_hard_cap".into(),
        hard_cap_bytes.to_string(),
    );

    catalog
        .set_project_storage_config(project, config)
        .await
        .map_err(|e| {
            BasinError::Internal(format!(
                "ALTER PROJECT {project_name} SET basin.memtable_hard_cap: \
                 failed to persist: {e}"
            ))
        })?;

    Ok(())
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Read a bare SQL identifier from the start of `s`. Returns `(ident, rest)`.
fn read_ident(s: &str) -> Result<(String, &str)> {
    let end = s
        .find(|c: char| !c.is_alphanumeric() && c != '_' && c != '-' && c != '.')
        .unwrap_or(s.len());
    if end == 0 {
        return Err(BasinError::InvalidSchema(
            "ALTER PROJECT: expected project name".into(),
        ));
    }
    Ok((s[..end].to_string(), &s[end..]))
}

/// Strip a case-insensitive prefix `prefix` from `s` (using pre-computed
/// `lower` for the comparison).  Returns the remainder if matched.
fn strip_prefix_ci<'a>(s: &'a str, lower: &str, prefix: &str) -> Option<&'a str> {
    if lower.starts_with(prefix) {
        // Make sure the prefix ends on a token boundary.
        let after = &s[prefix.len()..];
        if after
            .chars()
            .next()
            .map(|c| c.is_alphanumeric() || c == '_')
            .unwrap_or(false)
        {
            // Still inside an identifier; not a whole-word match.
            return None;
        }
        Some(after)
    } else {
        None
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_bare_integer() {
        let r = match_alter_project_memtable_cap(
            "ALTER PROJECT myproject SET basin.memtable_hard_cap = 268435456",
        )
        .unwrap();
        assert_eq!(r, Some(("myproject".into(), 268_435_456)));
    }

    #[test]
    fn match_mb_suffix() {
        let r = match_alter_project_memtable_cap(
            "ALTER PROJECT proj SET basin.memtable_hard_cap = 256MB",
        )
        .unwrap();
        assert_eq!(r, Some(("proj".into(), 256 * 1024 * 1024)));
    }

    #[test]
    fn match_gb_suffix() {
        let r = match_alter_project_memtable_cap(
            "ALTER PROJECT proj SET basin.memtable_hard_cap = 1GB",
        )
        .unwrap();
        assert_eq!(r, Some(("proj".into(), 1024 * 1024 * 1024)));
    }

    #[test]
    fn trailing_semicolon_stripped() {
        let r = match_alter_project_memtable_cap(
            "ALTER PROJECT proj SET basin.memtable_hard_cap = 256MB;",
        )
        .unwrap();
        assert!(r.is_some());
    }

    #[test]
    fn case_insensitive() {
        let r = match_alter_project_memtable_cap(
            "alter project Proj set basin.memtable_hard_cap = 256MB",
        )
        .unwrap();
        assert!(r.is_some());
    }

    #[test]
    fn non_alter_project_returns_none() {
        let r = match_alter_project_memtable_cap("SELECT 1").unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn alter_table_returns_none() {
        let r =
            match_alter_project_memtable_cap("ALTER TABLE t SET cold_after = 3600").unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn unrecognised_set_key_returns_none() {
        let r = match_alter_project_memtable_cap(
            "ALTER PROJECT proj SET basin.some_other_key = 1",
        )
        .unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn invalid_byte_size_returns_err() {
        let r = match_alter_project_memtable_cap(
            "ALTER PROJECT proj SET basin.memtable_hard_cap = notanumber",
        );
        assert!(r.is_err());
    }
}
