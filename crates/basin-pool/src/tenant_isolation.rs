//! Per-project isolation enforcement for pool checkout.
//!
//! ## Purpose
//!
//! The pool keys on `(ProjectId, client_key)`, which means the idle-cache map
//! already prevents sessions from one project from appearing in another
//! project's lookup bucket. However, that structural guarantee is a single
//! point of trust: one map bug (wrong key, a future refactor that drops the
//! project component of the key, etc.) could silently hand a session from
//! project A to a request from project B.
//!
//! This module adds an explicit, defence-in-depth **checkout-time assertion**:
//! before any session is handed to a caller, [`assert_project_tag`] verifies
//! that `session.project() == requested_project`. If they differ the pool
//! returns a hard error rather than allowing cross-project data exposure. This
//! is the load-bearing isolation invariant from Phase 5.27.D.
//!
//! ## Multi-tenant semaphore (per-project cap)
//!
//! Per-tenant resource cost must be O(bytes), not O(pool-size). The
//! [`per_project_cap`] path in [`crate::SessionPool::acquire`] already
//! implements this: each project gets at most `cfg.per_project_cap` concurrent
//! sessions; waiters are parked on a per-project oneshot queue rather than on
//! a global semaphore. One project exhausting its cap cannot starve another
//! project's slot. This file documents that invariant alongside the checkout
//! validation so all isolation-related logic is co-located.
//!
//! ## Error
//!
//! [`ProjectTagMismatch`] is returned as a [`basin_common::BasinError`]
//! (SQLSTATE `58030` — IO error; we reuse this rather than introducing a new
//! pool-specific code) so the caller can surface it to the client without
//! revealing the internal session's project identity.

use basin_common::{BasinError, ProjectId, Result};
use basin_engine::ProjectSession;

/// Verify that the project tag stamped on `session` at open-time matches the
/// `requested` project id.
///
/// Call this immediately before handing a session (cache-hit or fresh open) to
/// a logical client. On mismatch the function returns an error — the caller
/// should **not** hand the session out and should instead destroy it (drop the
/// `Arc`) and release the slot so capacity is not leaked.
///
/// # Panics
///
/// Never panics. Errors are returned via `Result`.
///
/// # Example
///
/// ```rust,ignore
/// // Inside the cache-hit branch of acquire():
/// assert_project_tag(&session, key.project)?;
/// return Ok(pooled_session::build(...));
/// ```
pub(crate) fn assert_project_tag(session: &ProjectSession, requested: ProjectId) -> Result<()> {
    let actual = session.project();
    if actual != requested {
        return Err(BasinError::Internal(format!(
            "pool isolation violation: session project {actual} does not match \
             requested project {requested}; refusing checkout to prevent \
             cross-project data exposure"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use basin_catalog::InMemoryCatalog;
    use basin_common::ProjectId;
    use basin_engine::{Engine, EngineConfig};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::assert_project_tag;

    fn engine_in(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// A session whose project tag matches the requested project must pass.
    #[tokio::test]
    async fn correct_project_tag_passes() {
        let dir = TempDir::new().unwrap();
        let engine = engine_in(&dir);
        let project = ProjectId::new();
        let session = engine.open_session(project).await.unwrap();
        assert!(
            assert_project_tag(&session, project).is_ok(),
            "same project tag must pass validation"
        );
    }

    /// A session whose project tag differs from the requested project must
    /// be rejected — this is the cross-project isolation invariant.
    #[tokio::test]
    async fn mismatched_project_tag_is_rejected() {
        let dir = TempDir::new().unwrap();
        let engine = engine_in(&dir);
        let project_a = ProjectId::new();
        let project_b = ProjectId::new();

        // Open a session for project A.
        let session = engine.open_session(project_a).await.unwrap();

        // Attempting to hand this session out for project B must fail.
        let result = assert_project_tag(&session, project_b);
        assert!(
            result.is_err(),
            "session opened for {project_a} must not be handed to {project_b}"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("isolation violation"),
            "error must mention 'isolation violation'; got: {msg}"
        );
    }
}
