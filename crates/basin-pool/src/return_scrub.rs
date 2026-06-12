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
//! | Session GUCs (`SET`)  | persists for connection life              | `reset_for_pool_reuse` (`reset_gucs`) |
//! | Open cursors          | persist until `CLOSE` or session end      | `reset_for_pool_reuse` (`cursors.clear_all`) |
//! | Prepared statements   | persist until `DEALLOCATE` or session end | `reset_for_pool_reuse` (`prepared.clear_all`) |
//! | LISTEN subscriptions  | persist until `UNLISTEN` or disconnect    | `reset_for_pool_reuse` (`listen_unsubscribe_all`) |
//! | Advisory locks        | persist until release or disconnect       | `reset_for_pool_reuse` (`release_all_on_session_end`) |
//! | Open transactions     | corrupt state for the next client         | `ROLLBACK`            |
//! | **Temp tables (P0)**  | persist in catalog across checkouts       | `DROP TABLE IF EXISTS` per table |
//!
//! ## Authoritative reset lives in the engine (5.27.E)
//!
//! GUCs, cursors, prepared statements, advisory locks and LISTEN subscriptions
//! are all reset by the engine's [`ProjectSession::reset_for_pool_reuse`], which
//! is `DISCARD ALL` implemented in Rust (the SQL surface for `RESET ALL` /
//! `UNLISTEN *` / `DEALLOCATE ALL` is a noop-accept or only partially wired in
//! v0.1, so we do NOT depend on it). Pool checkin is not session end, so the
//! advisory-lock release is invoked explicitly rather than left to
//! `Drop for SessionState`. The best-effort SQL statements issued below
//! (`ROLLBACK`, `SET search_path TO DEFAULT`, `RESET ALL`, `UNLISTEN *`) are
//! kept as defence-in-depth and to emit the PG-compatible command tags some
//! drivers expect, but correctness no longer rides on them.
//!
//! ## Temp-table scrub (P0 fix)
//!
//! `CREATE TEMP TABLE` in Basin creates a **persistent** catalog entry (there
//! is no session-scoped temp-schema as in standard Postgres).  Without
//! explicit cleanup a temp table created by tenant A during one checkout
//! remains visible to tenant B on the next checkout that reuses the same
//! physical session.
//!
//! The fix: [`PooledSession::execute`] intercepts `CREATE TEMP[ORARY] TABLE`
//! statements and records the table name.  [`scrub_session_for_pool_return`]
//! accepts this list and issues `DROP TABLE IF EXISTS <name>` for each entry
//! before the session is returned to the idle queue.  If any drop fails the
//! session is **destroyed** (the caller receives `Err`) rather than silently
//! reused with the temp table still present.
//!
//! ## Routing-critical scrub failure (P2 fix)
//!
//! `SET search_path` has real storage-routing impact in Basin: the schema
//! resolver uses it to map bare table names to catalog entries.  If
//! `SET search_path TO DEFAULT` fails during scrub we return `Err` so the
//! caller destroys the session rather than returning a potentially mis-routed
//! session to the idle cache.  Other best-effort scrub steps (ROLLBACK,
//! DEALLOCATE, RESET ALL, UNLISTEN) remain warn-and-continue.
//!
//! ## What is NOT scrubbed here
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
/// `temp_tables` is the list of table names created with `CREATE TEMP TABLE`
/// during the preceding checkout (tracked by [`crate::PooledSession::execute`]).
/// Each is dropped with `DROP TABLE IF EXISTS` before any other scrub step.
///
/// ## Error policy
///
/// * **Temp-table drops** — any failure returns `Err` immediately.  The caller
///   must destroy the session to prevent the temp table from leaking into the
///   next checkout (P0 invariant).
/// * **`SET search_path TO DEFAULT`** — failure returns `Err`.  Routing-state
///   leaks are treated as fatal for session reuse (P2 fix).
/// * All other steps (ROLLBACK, cursor/prepared reset, RESET ALL, UNLISTEN) are
///   best-effort: failures are logged at `debug` level and the scrub continues.
///
/// # Order
///
/// 1. `DROP TABLE IF EXISTS <name>` — for each tracked temp table (P0).
/// 2. `ROLLBACK` — close any open transaction.
/// 3. Engine-level cursor + prepared-statement reset.
/// 4. `DEALLOCATE ALL` — release named prepared statements.
/// 5. `SET search_path TO DEFAULT` — restore routing-critical GUC (fatal on failure).
/// 6. `RESET ALL` — restore remaining session GUCs.
/// 7. `UNLISTEN *` — cancel LISTEN subscriptions.
#[instrument(skip(session, temp_tables), fields(project = %session.project()))]
pub(crate) async fn scrub_session_for_pool_return(
    session: &ProjectSession,
    temp_tables: &[String],
) -> Result<()> {
    // 1. Drop every temp table created during the preceding checkout.
    //    P0: any failure is fatal — we return Err so the caller destroys the
    //    session rather than returning it with a leaked temp table.
    for table in temp_tables {
        let drop_sql = format!("DROP TABLE IF EXISTS {table}");
        if let Err(e) = session.execute(&drop_sql).await {
            tracing::warn!(
                error = %e,
                table = %table,
                "pool scrub: DROP TABLE for temp table failed; destroying session to prevent leak"
            );
            return Err(e);
        }
        tracing::debug!(table = %table, "pool scrub: dropped temp table");
    }

    // 2. Roll back any open (or aborted) transaction.  Issuing ROLLBACK when
    //    no transaction is active is harmless in Postgres (returns "no
    //    transaction is in progress" notice, not an error).
    if let Err(e) = session.execute("ROLLBACK").await {
        tracing::debug!(
            error = %e,
            "pool scrub: ROLLBACK failed (no open txn is fine)"
        );
    }

    // 3. Authoritative DISCARD ALL: drop cursors + prepared statements, release
    //    ALL advisory locks (session checkin is not session end, so we can't
    //    wait for `Drop`), clear LISTEN subscriptions, and reset every session
    //    GUC to its process default. SQL `CLOSE ALL` / `DEALLOCATE ALL` /
    //    `RESET ALL` / `UNLISTEN *` are noop-accepts or only partially wired in
    //    v0.1, so this engine method is the source of truth for Session-mode
    //    reuse. Transaction mode destroys the session entirely and never reaches
    //    this path.
    session.reset_for_pool_reuse().await;

    // 4. Deallocate all named prepared statements.  In Basin, SQL-level
    //    PREPARE/EXECUTE are noop-accepts so this has no effect today, but
    //    it is correct when the engine adds a named-statement registry.
    if let Err(e) = session.execute("DEALLOCATE ALL").await {
        tracing::debug!(error = %e, "pool scrub: DEALLOCATE ALL failed");
    }

    // 5. Reset search_path — routing-critical GUC.  Basin's SET search_path is
    //    stored in SchemaState and affects how bare table names resolve to
    //    catalog entries.  Failure here is FATAL: return Err so the caller
    //    destroys the session rather than returning a mis-routed session to
    //    the idle cache (P2 fix).
    if let Err(e) = session.execute("SET search_path TO DEFAULT").await {
        tracing::warn!(
            error = %e,
            "pool scrub: SET search_path TO DEFAULT failed; destroying session \
             to prevent routing-state leak"
        );
        return Err(e);
    }

    // 6. Also attempt the general RESET ALL for forward-compatibility with GUCs
    //    the engine may implement in the future.  Best-effort.
    if let Err(e) = session.execute("RESET ALL").await {
        tracing::debug!(error = %e, "pool scrub: RESET ALL failed (noop-accepted or unsupported)");
    }

    // 7. Unsubscribe all LISTEN channels.  Best-effort.
    if let Err(e) = session.execute("UNLISTEN *").await {
        tracing::debug!(error = %e, "pool scrub: UNLISTEN * failed");
    }

    Ok(())
}
