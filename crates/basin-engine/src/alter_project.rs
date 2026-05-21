//! `ALTER PROJECT` DDL surface.
//!
//! Phase 5.14.C5 added one `ALTER PROJECT` form (memtable cap); Phase 6.X.B
//! (ADR 0023) adds a second (partition count). Both ride on the same
//! pre-screen + `ProjectStorageConfig::provider_extras` persistence path:
//!
//! ```sql
//! ALTER PROJECT <name> SET basin.memtable_hard_cap = <n>
//! ALTER PROJECT <name> SET partitions          = <n>
//! ```
//!
//! For `basin.memtable_hard_cap` `<n>` accepts a bare integer (bytes) or a
//! human-readable suffix (`256MB`, `1GB`, …); the value is validated
//! (1 MiB ≤ n ≤ 64 GiB) and persisted under the key
//! `"basin.memtable_hard_cap"`. The engine reads the per-project override
//! at hot-tier registration time and passes it to
//! [`basin_hottier::MemTableRegistry`].
//!
//! For `partitions` `<n>` is a bare integer (1 ≤ n ≤ 1024) persisted under
//! `"basin.partitions"`. v1 default is 1 (back-compat: a single partition
//! whose lease lives on whichever replica was first to touch the project;
//! the router maps `(project, "_default") → owner` through the
//! [`basin_catalog::LeaseRegistry`]). Setting N > 1 lets a whale tenant
//! distribute its memtable / WAL / cap state across N replicas via the
//! Phase 6.X.B partition-aware router.
//!
//! # Why `provider_extras`?
//!
//! `ProjectStorageConfig::provider_extras` is an opaque `BTreeMap<String,
//! String>` the catalog already persists per-project without any schema
//! migration. Storing both scalars there avoids adding a new catalog
//! column / migration for a value read once per session open.

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

    let mut config: basin_storage::ProjectStorageConfig = catalog
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

// ── Phase 6.X.B — ALTER PROJECT … SET partitions = N ─────────────────────────

/// Persisted key under [`ProjectStorageConfig::provider_extras`] carrying the
/// per-project partition count. Phase 6.X.B (ADR 0023). Default is 1 when
/// the key is absent (back-compat byte-equivalent to pre-6.X.B).
pub const BASIN_PARTITIONS_KEY: &str = "basin.partitions";

/// Default partition count when the project hasn't opted in via DDL. One
/// partition routes the project's single `(project, "_default")` lease to
/// whichever replica acquires first — byte-equivalent to the pre-6.X.B
/// hashed-router behaviour.
pub const DEFAULT_PARTITION_COUNT: u32 = 1;

/// Maximum partition count accepted by `ALTER PROJECT … SET partitions = N`.
/// 1024 is well past what a single tenant can plausibly need in v1
/// (a 1024-partition project against a 16-replica cluster would already
/// distribute 64 partitions per replica) and bounds the lease-table cost
/// per project at `O(N)` rows.
pub const MAX_PARTITION_COUNT: u32 = 1024;

/// Pre-screen `sql` for `ALTER PROJECT <name> SET partitions = <n>`.
///
/// Mirrors [`match_alter_project_memtable_cap`] in shape. Returns:
/// - `Ok(Some((project_name, partitions)))` on a match.
/// - `Ok(None)` if the statement is not this form. (Notably also for the
///   memtable-cap form; the caller dispatches the memtable arm first.)
/// - `Err` if the statement begins with `ALTER PROJECT … SET partitions`
///   but the value is malformed (so the operator gets a clear failure
///   instead of a silent fallthrough).
pub(crate) fn match_alter_project_partitions(sql: &str) -> Result<Option<(String, u32)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("alter project") {
        return Ok(None);
    }
    let after = trimmed["alter project".len()..].trim_start();
    let (project_name, rest) = read_ident(after)?;
    let rest = rest.trim_start();
    let rest_lower = rest.to_ascii_lowercase();
    let after_set = match strip_prefix_ci(rest, &rest_lower, "set") {
        Some(s) => s.trim_start(),
        None => {
            // Not a SET form; caller arms can decide. Don't fail loudly.
            return Ok(None);
        }
    };

    // Match the `partitions` key. Bail to None if it's some other key the
    // memtable-cap arm or a future arm will handle.
    let after_set_lower = after_set.to_ascii_lowercase();
    let after_key = match strip_prefix_ci(after_set, &after_set_lower, "partitions") {
        Some(s) => s.trim_start(),
        None => return Ok(None),
    };

    if !after_key.starts_with('=') {
        return Err(BasinError::InvalidSchema(
            "ALTER PROJECT … SET partitions: expected '='".into(),
        ));
    }
    let value_str = after_key[1..].trim();
    let n: u32 = value_str.parse().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "ALTER PROJECT {project_name} SET partitions: cannot parse {value_str:?} as a \
             non-negative integer: {e}"
        ))
    })?;
    if n == 0 {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER PROJECT {project_name} SET partitions: must be >= 1 (got 0)"
        )));
    }
    if n > MAX_PARTITION_COUNT {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER PROJECT {project_name} SET partitions: must be <= {MAX_PARTITION_COUNT} \
             (got {n})"
        )));
    }
    Ok(Some((project_name, n)))
}

/// Apply `ALTER PROJECT <name> SET partitions = <n>`. Persists in
/// `ProjectStorageConfig::provider_extras` under [`BASIN_PARTITIONS_KEY`].
///
/// The router (`LeaseAwareShardMap`) reads this on demand on the next
/// `owner_for(project, partition_id)` call. Existing leases are *not*
/// re-balanced here; that is Phase 6.X.C (lease handoff) and is explicitly
/// out of scope for v1 — ops trigger a rebalance manually for now.
pub(crate) async fn exec_alter_project_partitions(
    sess: &ProjectSession,
    project_name: &str,
    partitions: u32,
) -> Result<()> {
    let catalog = sess.engine.config().catalog.clone();
    let project = &sess.project;
    let mut config: basin_storage::ProjectStorageConfig = catalog
        .get_project_storage_config(project)
        .await?
        .unwrap_or_default();
    config
        .provider_extras
        .insert(BASIN_PARTITIONS_KEY.into(), partitions.to_string());
    catalog
        .set_project_storage_config(project, config)
        .await
        .map_err(|e| {
            BasinError::Internal(format!(
                "ALTER PROJECT {project_name} SET partitions: failed to persist: {e}"
            ))
        })?;
    Ok(())
}

/// Read the per-project partition count from a previously-persisted
/// `ProjectStorageConfig`. Falls back to [`DEFAULT_PARTITION_COUNT`] when
/// the key is absent or unparseable (best-effort: the data path stays
/// open with the 1-partition default; an operator wanting to debug a bad
/// value should consult catalog state directly).
pub fn read_partition_count(config: &basin_storage::ProjectStorageConfig) -> u32 {
    config
        .provider_extras
        .get(BASIN_PARTITIONS_KEY)
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|n| (1..=MAX_PARTITION_COUNT).contains(n))
        .unwrap_or(DEFAULT_PARTITION_COUNT)
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

    // ── Phase 6.X.B — partition-count DDL ─────────────────────────────────

    #[test]
    fn partitions_match_simple() {
        let r = match_alter_project_partitions("ALTER PROJECT proj SET partitions = 4").unwrap();
        assert_eq!(r, Some(("proj".into(), 4)));
    }

    #[test]
    fn partitions_match_case_insensitive_with_semicolon() {
        let r = match_alter_project_partitions(
            "alter project Whale set partitions = 8;",
        )
        .unwrap();
        assert_eq!(r, Some(("Whale".into(), 8)));
    }

    #[test]
    fn partitions_zero_rejected() {
        let err =
            match_alter_project_partitions("ALTER PROJECT p SET partitions = 0").unwrap_err();
        match err {
            BasinError::InvalidSchema(msg) => assert!(msg.contains("must be >= 1"), "got: {msg}"),
            other => panic!("expected InvalidSchema, got {other:?}"),
        }
    }

    #[test]
    fn partitions_over_max_rejected() {
        let err =
            match_alter_project_partitions("ALTER PROJECT p SET partitions = 9999").unwrap_err();
        match err {
            BasinError::InvalidSchema(msg) => assert!(msg.contains("must be <="), "got: {msg}"),
            other => panic!("expected InvalidSchema, got {other:?}"),
        }
    }

    #[test]
    fn partitions_garbage_rejected() {
        assert!(
            match_alter_project_partitions("ALTER PROJECT p SET partitions = NaN").is_err()
        );
    }

    #[test]
    fn partitions_does_not_swallow_memtable_form() {
        // The memtable-cap form must return None so the caller can dispatch
        // to the memtable arm.
        let r = match_alter_project_partitions(
            "ALTER PROJECT proj SET basin.memtable_hard_cap = 256MB",
        )
        .unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn partitions_non_alter_project_returns_none() {
        assert!(match_alter_project_partitions("SELECT 1").unwrap().is_none());
        assert!(match_alter_project_partitions("ALTER TABLE t ADD COLUMN x INT")
            .unwrap()
            .is_none());
    }

    #[test]
    fn read_partition_count_default_when_absent() {
        let cfg = basin_storage::ProjectStorageConfig::default();
        assert_eq!(read_partition_count(&cfg), DEFAULT_PARTITION_COUNT);
    }

    #[test]
    fn read_partition_count_returns_persisted_value() {
        let mut cfg = basin_storage::ProjectStorageConfig::default();
        cfg.provider_extras
            .insert(BASIN_PARTITIONS_KEY.into(), "16".into());
        assert_eq!(read_partition_count(&cfg), 16);
    }

    #[test]
    fn read_partition_count_clamps_garbage_to_default() {
        let mut cfg = basin_storage::ProjectStorageConfig::default();
        // Non-integer.
        cfg.provider_extras
            .insert(BASIN_PARTITIONS_KEY.into(), "nope".into());
        assert_eq!(read_partition_count(&cfg), DEFAULT_PARTITION_COUNT);
        // Out-of-range (> MAX).
        cfg.provider_extras
            .insert(BASIN_PARTITIONS_KEY.into(), "99999".into());
        assert_eq!(read_partition_count(&cfg), DEFAULT_PARTITION_COUNT);
        // Zero.
        cfg.provider_extras
            .insert(BASIN_PARTITIONS_KEY.into(), "0".into());
        assert_eq!(read_partition_count(&cfg), DEFAULT_PARTITION_COUNT);
    }
}
