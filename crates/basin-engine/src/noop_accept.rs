//! No-op-accept dispatch for DBA / RBAC primitives and compatibility
//! statement kinds that Basin accepts syntactically but doesn't enforce or
//! execute. Real permissions come from `auth.uid()` / `auth.role()` per
//! ADRs 0005 / 0013.
//! No-op-accept dispatch for DBA / RBAC primitives that Basin accepts
//! syntactically but doesn't enforce. Real permissions come from
//! `auth.uid()` / `auth.role()` per ADRs 0005 / 0013.
//!
//! # Design
//!
//! These statement kinds are recognised by the canonical libpg_query parser,
//! classified into [`StmtKind`] variants, and intercepted **before**
//! `reject_unsupported` runs. Each returns an `ExecResult::Empty` with the
//! Postgres-style command-complete tag the wire protocol expects. No catalog
//! mutation, no privilege check — the statement is silently accepted.
//!
//! For `EXPLAIN`: we return a synthetic single-row result with a human-readable
//! message rather than an empty tag, so clients that expect a result set
//! (e.g. `psql \e`, JDBC `executeQuery`) don't crash.
//!
//! # LISTEN / NOTIFY / UNLISTEN
//!
//! These are **not** in this accept set. They are explicit non-goals per
//! ADR 0012 ("no pub/sub today"). They remain in `is_unsupported()` and
//! surface SQLSTATE 0A000 to the client.
//!
//! # Cursor lifecycle (DECLARE / FETCH / CLOSE / MOVE)
//!
//! Real cursor lifecycle is implemented by sibling agent a193aadd56ce5cb56.
//! Those kinds are NOT noop-accepted here.

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

use crate::pg_ast::StmtKind;
use crate::ExecResult;

/// Returns `Some(result)` if `kind` is in the no-op-accept set.
/// Returns `None` if the kind is not handled here; caller should continue
/// to `reject_unsupported` and then the normal dispatch path.
pub(crate) fn try_accept_as_noop(kind: StmtKind, sql: &str) -> Option<ExecResult> {
    match kind {
        // Maintenance / utility
        StmtKind::Vacuum => Some(ExecResult::Empty {
            tag: "VACUUM".into(),
        }),
        StmtKind::Analyze => Some(ExecResult::Empty {
            tag: "ANALYZE".into(),
        }),
        StmtKind::Cluster => Some(ExecResult::Empty {
            tag: "CLUSTER".into(),
        }),
        StmtKind::Lock => Some(ExecResult::Empty {
            tag: "LOCK TABLE".into(),
        }),
        // Metadata annotation (no enforcement)
        StmtKind::Comment => Some(ExecResult::Empty {
            tag: "COMMENT".into(),
        }),
        // EXPLAIN — fall through to the real exec_explain (explain.rs) which
        // invokes DataFusion's plan formatter. Earlier this returned a stub
        // single-row result; with `explain.rs` shipping the real path the
        // stub would shadow it.
        // RBAC primitives — accepted syntactically; real auth from auth.uid()/role()
        StmtKind::CreateRole => Some(ExecResult::Empty {
            tag: "CREATE ROLE".into(),
        }),
        StmtKind::DropRole => Some(ExecResult::Empty {
            tag: "DROP ROLE".into(),
        }),
        StmtKind::AlterRole => Some(ExecResult::Empty {
            tag: "ALTER ROLE".into(),
        }),
        StmtKind::Grant => Some(ExecResult::Empty {
            tag: "GRANT".into(),
        }),
        StmtKind::Revoke => Some(ExecResult::Empty {
            tag: "REVOKE".into(),
        }),
        // Foreign Data Wrappers / Foreign tables — accept-only; Basin has no FDW execution
        StmtKind::CreateFdw => Some(ExecResult::Empty {
            tag: "CREATE FOREIGN DATA WRAPPER".into(),
        }),
        StmtKind::DropFdw => Some(ExecResult::Empty {
            tag: "DROP FOREIGN DATA WRAPPER".into(),
        }),
        StmtKind::CreateForeignServer => Some(ExecResult::Empty {
            tag: "CREATE SERVER".into(),
        }),
        StmtKind::DropForeignServer => Some(ExecResult::Empty {
            tag: "DROP SERVER".into(),
        }),
        StmtKind::CreateUserMapping => Some(ExecResult::Empty {
            tag: "CREATE USER MAPPING".into(),
        }),
        StmtKind::DropUserMapping => Some(ExecResult::Empty {
            tag: "DROP USER MAPPING".into(),
        }),
        StmtKind::CreateForeignTable => Some(ExecResult::Empty {
            tag: "CREATE FOREIGN TABLE".into(),
        }),
        StmtKind::DropForeignTable => Some(ExecResult::Empty {
            tag: "DROP FOREIGN TABLE".into(),
        }),
        StmtKind::ImportForeignSchema => Some(ExecResult::Empty {
            tag: "IMPORT FOREIGN SCHEMA".into(),
        }),
        // Ownership / reassignment — accept-only
        StmtKind::ReassignOwned => Some(ExecResult::Empty {
            tag: "REASSIGN OWNED".into(),
        }),
        StmtKind::DropOwned => Some(ExecResult::Empty {
            tag: "DROP OWNED".into(),
        }),
        // Default privileges — accept-only; real auth from auth.uid()/role()
        StmtKind::AlterDefaultPrivileges => Some(ExecResult::Empty {
            tag: "ALTER DEFAULT PRIVILEGES".into(),
        }),
        // Constraint deferrability — accept-only; Basin is auto-commit
        StmtKind::SetConstraints => Some(ExecResult::Empty {
            tag: "SET CONSTRAINTS".into(),
        }),
        // Security labels — accept-only
        StmtKind::SecurityLabel => Some(ExecResult::Empty {
            tag: "SECURITY LABEL".into(),
        }),

        // Extended query protocol from text — PREPARE / EXECUTE / DEALLOCATE.
        // The real extended-query path uses pgwire Parse/Bind/Execute messages;
        // text-form PREPARE is rare (psql \bind, some migration tools) and
        // returning an empty ok tag avoids the 0A000 bounce. v0.1 does not
        // cache or execute the prepared plan.
        StmtKind::Prepare => Some(ExecResult::Empty {
            tag: "PREPARE".into(),
        }),
        StmtKind::Execute => Some(ExecResult::Empty {
            tag: "EXECUTE".into(),
        }),
        StmtKind::Deallocate => Some(ExecResult::Empty {
            tag: "DEALLOCATE".into(),
        }),

        // Transaction control — Basin is auto-commit (no MVCC, no WAL-level
        // transaction isolation in v0.1). Accepting these silently keeps
        // drivers that emit implicit BEGIN/COMMIT happy (e.g. lib/pq in
        // non-autocommit mode). Real single-shard transactions are deferred
        // to Phase 5; once shipped, these arms must be replaced with real
        // transaction state management.
        //
        // CAPABILITIES.md "Transactions" row documents the caveat.
        StmtKind::BeginTransaction => Some(ExecResult::Empty {
            tag: "BEGIN".into(),
        }),
        StmtKind::Commit => Some(ExecResult::Empty {
            tag: "COMMIT".into(),
        }),
        StmtKind::Rollback => Some(ExecResult::Empty {
            tag: "ROLLBACK".into(),
        }),
        // SAVEPOINT and RELEASE SAVEPOINT both arrive as Savepoint — return
        // the canonical tag "SAVEPOINT" for both.
        StmtKind::Savepoint => Some(ExecResult::Empty {
            tag: "SAVEPOINT".into(),
        }),

        // Trigger DDL — accepted syntactically; Basin does not execute trigger
        // bodies. Declarative lifecycle (`AUTO_UPDATE`, `AUDIT TO`, `SOFT DELETE`)
        // and SQL-bodied reactors (`REACT ON`) cover the common trigger use
        // cases without PL/pgSQL. See ADR 0012.
        StmtKind::CreateTrigger => Some(ExecResult::Empty {
            tag: "CREATE TRIGGER".into(),
        }),
        StmtKind::DropTrigger => Some(ExecResult::Empty {
            tag: "DROP TRIGGER".into(),
        }),

        // Extension DDL — accepted; Basin ships its own extension-equivalents
        // natively per ADR 0002 (no upstream `.so` loading). Loading an
        // external extension is therefore a no-op: the equivalent Basin crate
        // is already active. DROP EXTENSION is similarly a no-op.
        StmtKind::CreateExtension => Some(ExecResult::Empty {
            tag: "CREATE EXTENSION".into(),
        }),
        StmtKind::DropExtension => Some(ExecResult::Empty {
            tag: "DROP EXTENSION".into(),
        }),

        // MERGE INTO t USING src ON cond WHEN MATCHED THEN ... WHEN NOT MATCHED THEN ...
        // v0.1 accepts the syntax but does not execute the MERGE logic.
        // The WHEN MATCHED UPDATE and WHEN NOT MATCHED INSERT branches must
        // be run as separate UPDATE / INSERT statements until Phase 5 ships
        // real MERGE execution. Documented in CAPABILITIES.md.
        StmtKind::Merge => Some(ExecResult::Empty {
            tag: "MERGE".into(),
        }),

        // REINDEX — accepted; Basin indexes are managed by DataFusion's
        // adaptive planner (no manual rebuild needed). This keeps tooling
        // scripts that call REINDEX happy without error.
        StmtKind::Reindex => Some(ExecResult::Empty {
            tag: "REINDEX".into(),
        }),

        // Session-variable control.
        // `SET search_path = …`, `RESET role`, etc. are no-op accepted.
        StmtKind::VariableSet => Some(ExecResult::Empty { tag: "SET".into() }),
        // `SHOW search_path`, `SHOW all`, etc. are no-op accepted.
        // IMPORTANT: `SHOW TABLES` is Basin's extension command that returns
        // a real result set (sqlparser handles it). Do NOT intercept it here;
        // return None so it falls through to the existing SHOW TABLES handler.
        StmtKind::VariableShow => {
            let trimmed = sql.trim().to_ascii_uppercase();
            let trimmed = trimmed.trim_end_matches(';').trim_end();
            if trimmed == "SHOW TABLES" {
                None
            } else {
                Some(ExecResult::Empty { tag: "SHOW".into() })
            }
        }
        // Everything else is not in our accept set.
        _ => None,
    }
}
