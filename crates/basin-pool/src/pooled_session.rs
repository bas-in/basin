//! [`PooledSession`] — RAII handle that returns its session to the pool
//! when dropped.
//!
//! ## Scrub-on-checkout (Session mode)
//!
//! Per-session state accumulated during a checkout (GUCs, cursors, LISTEN
//! subscriptions) must not bleed into the next logical client.  In
//! [`PoolMode::Session`] the physical session is reused across checkouts, so
//! we apply the DISCARD-ALL scrub sequence **at checkout time** (in
//! [`SessionPool::acquire`]) rather than at return time.  Scrubbing on
//! checkout rather than on return has two advantages:
//!
//! 1. `Drop` stays synchronous — we return the session to the idle queue
//!    immediately (preserving the try_lock fast path and the
//!    `hit_then_release_then_hit` test invariant).
//! 2. A session that is evicted from the idle queue while waiting is not
//!    scrubbed unnecessarily.
//!
//! The caller of [`build`] is responsible for calling
//! [`crate::return_scrub::scrub_session_for_pool_return`] before handing the
//! session to the logical client when the session came from the idle cache
//! (cache hit path in [`SessionPool::acquire`]).
//!
//! ## Temp-table tracking (P0 fix)
//!
//! `CREATE TEMP[ORARY] TABLE` statements executed through [`PooledSession::execute`]
//! are intercepted by a lightweight regex scan.  Detected table names are pushed
//! into `entry.temp_tables`.  At the next checkout (scrub step) every listed
//! table is dropped with `DROP TABLE IF EXISTS` before the session is handed to
//! the next logical client, ensuring no cross-project temp-table visibility.

use std::sync::Arc;
use std::time::Instant;

use basin_common::{ProjectId, Result};
use basin_engine::{ExecResult, ProjectSession};

use crate::state::{PoolKey, PooledEntry};
use crate::Inner;

/// A leased session. Dropping the handle returns the underlying
/// [`ProjectSession`] to the pool's idle queue.
///
/// The session reference is exposed via [`PooledSession::session`] rather
/// than by `Deref`. We deliberately do not hand out ownership: the pool
/// must see every release.
pub struct PooledSession {
    pub(crate) entry: Option<PooledEntry>,
    pub(crate) key: PoolKey,
    pub(crate) pool: Arc<Inner>,
}

impl PooledSession {
    /// The underlying engine session. Use this exactly the way the router
    /// would use the value returned by `Engine::open_session`.
    pub fn session(&self) -> &ProjectSession {
        &self
            .entry
            .as_ref()
            .expect("PooledSession used after drop")
            .session
    }

    /// The project this session is bound to. Convenience accessor that
    /// matches `ProjectSession::project` so callers don't need to deref.
    pub fn project(&self) -> ProjectId {
        self.key.project
    }

    /// Execute `sql` on the underlying session, intercepting any
    /// `CREATE TEMP[ORARY] TABLE` statements to track the created table name
    /// for pool-return cleanup (P0 temp-table leak fix).
    ///
    /// Only the Session-mode idle cache path carries a populated `entry.temp_tables`
    /// list; Transaction-mode sessions are destroyed on return and need no tracking.
    pub async fn execute(&self, sql: &str) -> Result<ExecResult> {
        let result = self.session().execute(sql).await?;
        // Track temp table names so the scrub can drop them before the next
        // checkout reuses this session.  Only Session-mode sessions enter the
        // idle cache, so tracking is always useful here.
        if let Some(name) = detect_create_temp_table(sql) {
            if let Some(entry) = &self.entry {
                if let Ok(mut tables) = entry.temp_tables.lock() {
                    tables.push(name);
                }
            }
        }
        Ok(result)
    }

}

impl Drop for PooledSession {
    fn drop(&mut self) {
        // `entry` is `None` only if `drop` ran twice, which Rust prevents.
        let entry = match self.entry.take() {
            Some(e) => e,
            None => return,
        };
        let pool = self.pool.clone();
        let key = self.key.clone();

        // Transaction / statement mode: destroy the session rather than
        // returning it to the idle cache.  Dropping `entry` here releases
        // the `Arc<ProjectSession>` (and thus all cursor / prepared-statement
        // state held by that session), then we release the capacity slot so
        // the next waiter can open a fresh session.  This is the load-bearing
        // isolation invariant for transaction-mode: no per-session state
        // (cursors, SET variables, prepared statements) can cross a checkout
        // boundary because the physical session is destroyed at checkout end.
        if pool.cfg.pool_mode.destroys_on_return() {
            // Drop the session Arc here, outside the lock, so its
            // destructors (cursor registry, advisory locks, etc.) run
            // before we re-acquire the pool mutex.
            drop(entry);
            // Release the slot under the lock and wake one waiter.
            // Bind the try_lock result to an owned variable so we don't
            // borrow `pool` across the spawn boundary below.
            let released = pool.state.try_lock().ok().map(|mut state| {
                state.release_slot(key.project);
                state.wake_one_waiter(key.project);
            });
            if released.is_none() {
                tokio::spawn(async move {
                    let mut state = pool.state.lock().await;
                    state.release_slot(key.project);
                    state.wake_one_waiter(key.project);
                });
            }
            return;
        }

        // Session mode: return the session to the idle cache as before.
        //
        // The DISCARD-ALL scrub is applied on the *checkout* side (in
        // `SessionPool::acquire`, on a cache hit) rather than here, so that
        // `Drop` can stay synchronous and the try_lock fast path is preserved.
        // See the module-level doc for the rationale.
        let mut entry = entry;
        entry.last_used = Instant::now();

        // Fast path: no contention on the outer mutex.
        if let Ok(mut state) = pool.state.try_lock() {
            state.return_entry(key, entry);
            return;
        }

        // Slow path: someone else holds the lock right now (almost always
        // another `acquire` in the same runtime). Hand the return off to
        // a fresh task so `Drop` itself stays sync.
        tokio::spawn(async move {
            let mut state = pool.state.lock().await;
            state.return_entry(key, entry);
        });
    }
}

#[cfg(test)]
mod detect_tests {
    use super::detect_create_temp_table;

    #[test]
    fn detects_create_temp_table() {
        assert_eq!(
            detect_create_temp_table("CREATE TEMP TABLE foo (id INT)"),
            Some("foo".to_string())
        );
    }

    #[test]
    fn detects_create_temporary_table() {
        assert_eq!(
            detect_create_temp_table("CREATE TEMPORARY TABLE bar (x TEXT)"),
            Some("bar".to_string())
        );
    }

    #[test]
    fn detects_case_insensitive() {
        assert_eq!(
            detect_create_temp_table("create temp table MyTable (id int)"),
            Some("mytable".to_string())
        );
    }

    #[test]
    fn detects_if_not_exists() {
        assert_eq!(
            detect_create_temp_table("CREATE TEMP TABLE IF NOT EXISTS baz (id INT)"),
            Some("baz".to_string())
        );
    }

    #[test]
    fn strips_schema_qualifier() {
        assert_eq!(
            detect_create_temp_table("CREATE TEMP TABLE public.qux (id INT)"),
            Some("qux".to_string())
        );
    }

    #[test]
    fn ignores_non_temp_create_table() {
        assert_eq!(
            detect_create_temp_table("CREATE TABLE regular (id INT)"),
            None
        );
    }

    #[test]
    fn ignores_non_table_temp() {
        assert_eq!(
            detect_create_temp_table("CREATE TEMP VIEW vw AS SELECT 1"),
            None
        );
    }

    #[test]
    fn ignores_select() {
        assert_eq!(detect_create_temp_table("SELECT * FROM foo"), None);
    }
}

/// Wrap a freshly-acquired entry plus its key in a `PooledSession`. Kept in
/// this module so the field-visibility surface stays small.
pub(crate) fn build(pool: Arc<Inner>, key: PoolKey, session: Arc<ProjectSession>) -> PooledSession {
    PooledSession {
        entry: Some(PooledEntry {
            session,
            last_used: Instant::now(),
            temp_tables: std::sync::Mutex::new(Vec::new()),
        }),
        key,
        pool,
    }
}

/// Detect a `CREATE TEMP[ORARY] TABLE [IF NOT EXISTS] <name>` statement and
/// return the table name (unqualified, lowercase) if found.
///
/// The detection is intentionally conservative: we only recognise the standard
/// forms Postgres and Basin accept.  A false negative (unrecognised syntax) is
/// safe — the worst outcome is a temp table that isn't dropped on return, which
/// is the pre-fix baseline.  A false positive would cause `DROP TABLE IF EXISTS`
/// to run unnecessarily, which is harmless.
///
/// We do not parse the full SQL because that would couple the pool to sqlparser
/// and add latency on every query.  A simple case-insensitive token scan is
/// sufficient for the well-known forms.
pub(crate) fn detect_create_temp_table(sql: &str) -> Option<String> {
    // Normalise to uppercase ASCII for case-insensitive token matching.
    // Allocate only when we have a plausible hit (contains "TEMP").
    let upper = sql.to_ascii_uppercase();
    // Quick pre-screen: must contain CREATE and TEMP somewhere.
    if !upper.contains("CREATE") || !upper.contains("TEMP") {
        return None;
    }
    // Tokenise by whitespace + common delimiters.  We want to find the
    // sequence:
    //   CREATE [OR REPLACE] TEMP[ORARY] TABLE [IF NOT EXISTS] <name>
    let tokens: Vec<&str> = sql
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .filter(|t| !t.is_empty())
        .collect();

    let mut i = 0;
    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("create") {
            let mut j = i + 1;
            // Skip optional OR REPLACE.
            if j + 1 < tokens.len()
                && tokens[j].eq_ignore_ascii_case("or")
                && tokens[j + 1].eq_ignore_ascii_case("replace")
            {
                j += 2;
            }
            // Expect TEMP or TEMPORARY.
            if j < tokens.len()
                && (tokens[j].eq_ignore_ascii_case("temp")
                    || tokens[j].eq_ignore_ascii_case("temporary"))
            {
                j += 1;
                // Expect TABLE.
                if j < tokens.len() && tokens[j].eq_ignore_ascii_case("table") {
                    j += 1;
                    // Skip optional IF NOT EXISTS.
                    if j + 2 < tokens.len()
                        && tokens[j].eq_ignore_ascii_case("if")
                        && tokens[j + 1].eq_ignore_ascii_case("not")
                        && tokens[j + 2].eq_ignore_ascii_case("exists")
                    {
                        j += 3;
                    }
                    // Next token is the table name.
                    if j < tokens.len() {
                        let raw_name = tokens[j];
                        // Strip any schema qualifier — we only track the bare name
                        // because Basin's catalog stores temp tables under (public, name).
                        let bare = raw_name
                            .rsplit('.')
                            .next()
                            .unwrap_or(raw_name)
                            .trim_matches('"')
                            .to_ascii_lowercase();
                        if !bare.is_empty() {
                            return Some(bare);
                        }
                    }
                }
            }
        }
        i += 1;
    }
    None
}
