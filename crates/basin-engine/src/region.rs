//! Multi-region write gate, read guard, and the [`WriteForwarder`] seam.
//!
//! Basin runs one independent deployment per region (ADR 0009): a per-region
//! single-writer Raft WAL, per-region shard owners, and a per-region object
//! store kept in sync by S3 cross-region replication at the bucket layer.
//! There is deliberately NO cross-region quorum, 2PC, multi-master, or
//! automatic failover (all explicit non-goals).
//!
//! Each project is pinned to a `home_region`
//! ([`basin_catalog::ProjectMetadata::home_region`]). The home region owns the
//! project's writer lease and hot tier; only it can serve a write or a
//! strongly-consistent read. This module enforces that:
//!
//! * [`region_write_gate`] / [`region_write_gate_insert`] — run on the WRITE
//!   path. For a project homed elsewhere they either delegate to a registered
//!   [`WriteForwarder`] (the home region executes the statement and returns its
//!   result) or, with no forwarder, raise [`BasinError::WrongRegion`] — the
//!   fail-loud default ADR 0009 specifies for forwarding-disabled deployments.
//! * [`region_read_guard`] — run on the READ path. `basin.read_tier = 'primary'`
//!   (the default) requires the home region (only it has the live tail);
//!   `'lagging'` is allowed anywhere (it reads flushed, S3-replicated cold
//!   data and accepts staleness).
//!
//! A `home_region` of `None` (legacy / unpinned project) is treated as homed
//! in the local region everywhere ([`basin_common::is_home_region`]), so
//! single-region deployments and pre-multi-region projects never reject.

use std::sync::Arc;

use basin_common::{is_home_region, local_region, BasinError, Result};

use crate::session::{session_read_tier, ReadTier};
use crate::{ExecResult, ProjectSession};

/// Context describing the statement being forwarded, handed to a
/// [`WriteForwarder`]. Kept deliberately minimal and serializable-in-spirit:
/// the cloud-private forwarder turns it into a remote call to the home region.
///
/// v0.1 forwards the **verbatim SQL text** plus the principal that must
/// resolve `current_user` (for RLS) in the home region. Prepared/bound
/// statements and COPY streams are NOT forwarded in this seam — the forwarder
/// returns `Err(BasinError::WrongRegion(..))` for anything it cannot carry, and
/// the engine surfaces that as the same typed error a no-forwarder deployment
/// would (see [`WriteForwarder`] contract).
pub struct ForwardContext<'a> {
    /// The project whose home region must execute this statement.
    pub project: basin_common::ProjectId,
    /// The project's pinned home region (always `Some` here — the gate only
    /// forwards when the home is known and differs from the local region).
    pub home_region: &'a str,
    /// The region this process runs in (`local_region()`), for the forwarder's
    /// own logging / routing.
    pub local_region: &'a str,
    /// Principal that resolves `current_user` for RLS in the home region.
    pub current_user: &'a str,
    /// The exact SQL text the client submitted, unrewritten.
    pub sql: &'a str,
}

/// The forwarder seam. A non-home write either delegates here (if registered)
/// or fails with [`BasinError::WrongRegion`]. The real implementation lives in
/// the cloud-private repo; the OSS engine ships only this trait + the
/// default-none behaviour, and NO HTTP / network code.
///
/// ## Contract
///
/// * **Idempotency.** The forwarder MUST treat each call as it would a direct
///   client statement: it does not retry internally in a way that could
///   double-apply a non-idempotent write. The engine does NOT retry a
///   forwarded call automatically — a returned `Err` is surfaced to the client
///   unchanged. If the home region executed the statement but the response was
///   lost, that is the same at-least-once exposure a single-region client
///   already has on a dropped connection; callers requiring exactly-once must
///   use an idempotency key in the statement itself (e.g. `INSERT … ON
///   CONFLICT DO NOTHING`). The forwarder must NOT silently swallow-and-ack.
/// * **What is forwarded.** Exactly one autonomous statement
///   ([`ForwardContext::sql`]) executed as [`ForwardContext::current_user`] in
///   [`ForwardContext::home_region`]. Session-local state (open transaction,
///   GUCs, temp tables, prepared statements, cursors, advisory locks) is NOT
///   carried: forwarding is for auto-commit, single-statement writes. A write
///   issued INSIDE an open transaction against a non-home project must NOT be
///   forwarded transparently (cross-region transactional semantics are a
///   non-goal) — the forwarder returns `Err(BasinError::WrongRegion(..))` and
///   the client sees the typed error. The engine enforces this by only calling
///   the forwarder for auto-commit writes (see `region_write_gate`).
/// * **Result shape.** On success the forwarder returns the home region's
///   [`ExecResult`] verbatim (row counts, RETURNING rows, tags) so the client
///   cannot tell the write was forwarded. On any failure it returns a
///   [`BasinError`] — ideally the home region's own typed error, falling back
///   to [`BasinError::WrongRegion`] / [`BasinError::wal`] for transport faults.
/// * **No partial visibility.** A forwarded write commits in the home region or
///   not at all; the local region observes it only via S3 replication later
///   (lagging reads), never as a local WAL append.
#[async_trait::async_trait]
pub trait WriteForwarder: Send + Sync + 'static {
    /// Forward one auto-commit write statement to the project's home region and
    /// return its result. See the trait-level contract for idempotency and
    /// statement-context expectations.
    async fn forward_write(&self, ctx: ForwardContext<'_>) -> Result<ExecResult>;
}

impl ProjectSession {
    /// Register a [`WriteForwarder`] on this session's engine. Process-wide:
    /// once set, every session's non-home write delegates to it instead of
    /// raising [`BasinError::WrongRegion`]. Intended to be called once at
    /// startup by the cloud-private layer. Re-registration replaces the prior
    /// forwarder (last writer wins), matching the engine's other attach-* APIs.
    pub fn attach_write_forwarder(&self, fwd: Arc<dyn WriteForwarder>) {
        self.engine.attach_write_forwarder(fwd);
    }
}

/// Resolve a project's home region from catalog metadata. `None` ⇒ unpinned /
/// legacy ⇒ treated as local everywhere.
async fn project_home_region(sess: &ProjectSession) -> Result<Option<String>> {
    let meta = sess
        .engine
        .config()
        .catalog
        .get_project_metadata(&sess.project())
        .await?;
    Ok(meta.home_region)
}

/// True when this session is inside an explicit (multi-statement) transaction.
/// Cross-region forwarding is only valid for auto-commit single statements, so
/// a non-home write inside a transaction fails loud rather than forwards.
fn in_explicit_transaction(sess: &ProjectSession) -> bool {
    crate::session::tx_is_active(&sess.state)
}

/// Write-path gate for a parsed statement of write kind. Returns:
///
/// * `Ok(None)` — the write may proceed locally (home/legacy/unpinned project).
/// * `Ok(Some(result))` — a [`WriteForwarder`] handled it; this is the home
///   region's result, return it to the client.
/// * `Err(BasinError::WrongRegion(..))` — non-home write, no forwarder (or the
///   write is inside an explicit transaction, which is never forwarded).
///
/// `_kind` is accepted for symmetry / future per-kind policy; the gate is the
/// same for every write kind today.
pub(crate) async fn region_write_gate(
    sess: &ProjectSession,
    _kind: crate::pg_ast::StmtKind,
    sql: &str,
) -> Result<Option<ExecResult>> {
    let home = project_home_region(sess).await?;
    if is_home_region(home.as_deref()) {
        return Ok(None); // local region IS the home (or project is unpinned).
    }
    // Non-home write. `home` is Some(..) and != local_region().
    let home = home.expect("non-home implies Some(home_region)");
    forward_or_reject(sess, &home, sql).await.map(Some)
}

/// Write-path gate for the literal-INSERT fast path (which bypasses the parsed
/// dispatch block). Same semantics as [`region_write_gate`]:
///
/// * `Ok(None)` — proceed into the preparse fast path locally.
/// * `Ok(Some(Ok(result)))` — forwarded; return this result.
/// * `Ok(Some(Err(e)))` — non-home, rejected (WrongRegion) — propagate `e`.
///
/// The double-`Result` shape lets the executor write `return result;` directly,
/// matching how `try_insert_preparse` already returns `Option<Result<..>>`.
pub(crate) async fn region_write_gate_insert(
    sess: &ProjectSession,
    sql: &str,
) -> Result<Option<Result<ExecResult>>> {
    let home = project_home_region(sess).await?;
    if is_home_region(home.as_deref()) {
        return Ok(None);
    }
    let home = home.expect("non-home implies Some(home_region)");
    Ok(Some(forward_or_reject(sess, &home, sql).await))
}

/// Shared tail: forward to the registered forwarder, or raise WrongRegion.
async fn forward_or_reject(
    sess: &ProjectSession,
    home: &str,
    sql: &str,
) -> Result<ExecResult> {
    // A non-home write inside an explicit transaction is never forwarded
    // (cross-region transactional semantics are a non-goal). Fail loud.
    if in_explicit_transaction(sess) {
        return Err(BasinError::wrong_region_for(
            sess.project(),
            "write in an explicit transaction",
            home,
            local_region(),
        ));
    }
    match sess.engine.write_forwarder() {
        Some(fwd) => {
            let ctx = ForwardContext {
                project: sess.project(),
                home_region: home,
                local_region: local_region(),
                current_user: sess.current_user(),
                sql,
            };
            fwd.forward_write(ctx).await
        }
        None => Err(BasinError::wrong_region_for(
            sess.project(),
            "write",
            home,
            local_region(),
        )),
    }
}

/// Read-path guard (commit 9). Call before serving a read for `sess`'s project.
///
/// * `primary` tier (default): the read must be served from the home region
///   (only it has the hot tier + WAL tail for a strongly-consistent answer).
///   A non-home primary read raises [`BasinError::WrongRegion`].
/// * `lagging` tier: allowed in any region. Returns `Ok(())` even off-home; the
///   read goes through the normal cold-tier path over local S3-replicated data
///   and sees flushed-only state (no hot tier / WAL tail outside home).
///
/// `Ok(())` means "proceed with the read here." A home/legacy/unpinned project
/// always returns `Ok(())` regardless of tier.
pub(crate) async fn region_read_guard(sess: &ProjectSession) -> Result<()> {
    let home = project_home_region(sess).await?;
    if is_home_region(home.as_deref()) {
        return Ok(()); // home / unpinned: any tier is fine.
    }
    let home = home.expect("non-home implies Some(home_region)");
    match session_read_tier(&sess.state) {
        // Lagging reads are explicitly cross-region: serve local replicated
        // cold data, accept staleness.
        ReadTier::Lagging => Ok(()),
        // Primary reads need the home region's live tail.
        ReadTier::Primary => Err(BasinError::wrong_region_for(
            sess.project(),
            "primary-tier read",
            &home,
            local_region(),
        )),
    }
}

/// Cheap prefix test: does `sql` look like an `INSERT`? Used at the very top of
/// `execute()` to decide whether the literal-INSERT fast path's region gate
/// needs to run, WITHOUT parsing. Conservative: false negatives (a weird INSERT
/// this misses) simply fall through to the general parsed gate; false positives
/// just cost one project-metadata lookup. Skips leading whitespace and a single
/// leading line/block comment is NOT handled here (the preparse path itself
/// bails on those, so the general gate covers them).
pub(crate) fn looks_like_insert(sql: &str) -> bool {
    let s = sql.trim_start();
    s.len() >= 6 && s.as_bytes()[..6].eq_ignore_ascii_case(b"INSERT")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_insert_basic() {
        assert!(looks_like_insert("INSERT INTO t VALUES (1)"));
        assert!(looks_like_insert("  insert into t values (1)"));
        assert!(looks_like_insert("InSeRt INTO t DEFAULT VALUES"));
        assert!(!looks_like_insert("SELECT 1"));
        assert!(!looks_like_insert("UPDATE t SET a=1"));
        assert!(!looks_like_insert("INS")); // too short
        assert!(!looks_like_insert(""));
    }
}
