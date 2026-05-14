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
        // EXPLAIN — return a synthetic single-row result set rather than
        // Empty so that clients expecting a result set (psql, JDBC) don't
        // error. v0.1 does not invoke DataFusion's plan formatter.
        StmtKind::Explain => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "QUERY PLAN",
                DataType::Utf8,
                false,
            )]));
            let msg = format!(
                "EXPLAIN ANALYZE not yet supported in Basin v0.1; query was: {}",
                sql.trim()
            );
            let col: ArrayRef = Arc::new(StringArray::from(vec![msg]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![col]).expect("static schema is valid");
            Some(ExecResult::Rows {
                schema,
                batches: vec![batch],
            })
        }
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
        // Session-variable control
        StmtKind::VariableSet => Some(ExecResult::Empty { tag: "SET".into() }),
        StmtKind::VariableShow => Some(ExecResult::Empty {
            tag: "SHOW".into(),
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
        // Everything else is not in our accept set.
        _ => None,
    }
}
