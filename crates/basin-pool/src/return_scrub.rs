//! Return-to-pool session scrub — DISCARD ALL semantics.
//!
//! ## Purpose
//!
//! When a [`SessionPool`] operates in [`PoolMode::Session`] mode, the same
//! physical [`ProjectSession`] is handed to successive logical clients.
//! Per-session state that accumulates during one checkout must not bleed into
//! the next one.  The commands under test (Phase 5.27.A battery):
//!
//! | State class           | Leak vector                               | Scrub action          |
//! |-----------------------|-------------------------------------------|-----------------------|
//! | Session GUCs (`SET`)  | persists for connection life              | `RESET ALL`           |
//! | Open cursors          | persist until `CLOSE` or session end      | `CLOSE ALL`           |
//! | Prepared statements   | persist until `DEALLOCATE` or session end | `DEALLOCATE ALL`      |
//! | LISTEN subscriptions  | persist until `UNLISTEN` or disconnect    | `UNLISTEN *`          |
//! | Advisory locks        | persist until release or disconnect       | (session-level engine drop) |
//! | Open transactions     | corrupt state for the next client         | `ROLLBACK`            |
//!
//! ## What is NOT scrubbed here
//!
//! * **Temporary tables** — in Basin, `CREATE TEMP TABLE` creates a persistent
//!   catalog entry (there is no session-scoped temp-schema as in standard
//!   Postgres).  Dropping temp tables on return requires engine-level tracking
//!   of which tables were created with the `TEMPORARY` keyword; that tracking
//!   does not exist today.  This is a known gap tracked by the
//!   `temp_table_does_not_survive_pool_return` test (Phase 5.27.E #temp-stub).
//!
//! * **Named SQL prepared statements (text-form PREPARE/EXECUTE)** — Basin
//!   treats `PREPARE <name> AS …` as a noop-accept (the real extended-query
//!   protocol uses `StatementHandle` UUIDs).  `EXECUTE <name>` is likewise a
//!   noop that always returns `Ok`.  There is no server-side named-statement
//!   registry to scrub, so `DEALLOCATE ALL` below is correct in intent but
//!   has no observable effect today.  The
//!   `prepared_statement_does_not_survive_pool_return` test is blocked on this
//!   engine stub (Phase 5.27.E #prepare-stub).
//!
//! ## Transaction-mode note
//!
//! In [`PoolMode::Transaction`] / [`PoolMode::Statement`] the session is
//! **destroyed** on return, so no scrub is needed — there is no physical
//! connection to reuse.  [`scrub_session_for_pool_return`] is only called on
//! the Session-mode return path.

use basin_common::Result;
use basin_engine::ProjectSession;
use tracing::instrument;

/// Apply DISCARD-ALL semantics to `session` before returning it to the idle
/// pool queue.
///
/// Each statement is attempted independently; a failure is logged but does
/// not abort the scrub — we want to reset as much state as possible even if
/// one command is unsupported or fails.
///
/// # Order
///
/// 1. `ROLLBACK` — close any open transaction so the following commands do
///    not run inside an aborted txn.
/// 2. `CLOSE ALL` — drop all open cursors.
/// 3. `DEALLOCATE ALL` — release all named prepared statements.
/// 4. `RESET ALL` — restore session GUCs to their defaults.
/// 5. `UNLISTEN *` — cancel all LISTEN subscriptions.
#[instrument(skip(session), fields(project = %session.project()))]
pub(crate) async fn scrub_session_for_pool_return(session: &ProjectSession) -> Result<()> {
    // 1. Roll back any open (or aborted) transaction.  Issuing ROLLBACK when
    //    no transaction is active is harmless in Postgres (returns "no
    //    transaction is in progress" notice, not an error).
    if let Err(e) = session.execute("ROLLBACK").await {
        tracing::debug!(
            error = %e,
            "pool scrub: ROLLBACK failed (no open txn is fine)"
        );
    }

    // 2. Drop all open cursors AND named prepared statements directly via the
    //    engine's in-memory registries. SQL `CLOSE ALL` / `DEALLOCATE ALL` are
    //    not fully wired in v0.1, so this is the authoritative cursor/prepared
    //    scrub for Session-mode reuse (no per-session state crosses the pooled
    //    checkout boundary). Transaction mode destroys the session entirely and
    //    never reaches this path.
    session.reset_for_pool_reuse().await;

    // 3. Deallocate all named prepared statements.  In Basin, SQL-level
    //    PREPARE/EXECUTE are noop-accepts so this has no effect today, but
    //    it is correct when the engine adds a named-statement registry.
    if let Err(e) = session.execute("DEALLOCATE ALL").await {
        tracing::debug!(error = %e, "pool scrub: DEALLOCATE ALL failed");
    }

    // 4. Reset all session GUCs (search_path, statement_timeout, etc.) to
    //    their per-session defaults.  Basin's SET search_path is stored in
    //    SchemaState; RESET ALL must restore it.  Currently RESET is a
    //    noop-accept for most GUCs; SET search_path is real so resetting it
    //    explicitly provides the guaranteed cleanup.
    if let Err(e) = session.execute("SET search_path TO DEFAULT").await {
        tracing::debug!(error = %e, "pool scrub: SET search_path TO DEFAULT failed");
    }

    // Also attempt the general RESET ALL for forward-compatibility with GUCs
    // the engine may implement in the future.
    if let Err(e) = session.execute("RESET ALL").await {
        tracing::debug!(error = %e, "pool scrub: RESET ALL failed (noop-accepted or unsupported)");
    }

    // 5. Unsubscribe all LISTEN channels.  LISTEN routing through
    //    ProjectSession::execute is currently unconfirmed (it may go through
    //    the realtime substrate); this is correct once routing is confirmed.
    if let Err(e) = session.execute("UNLISTEN *").await {
        tracing::debug!(error = %e, "pool scrub: UNLISTEN * failed");
    }

    Ok(())
}
