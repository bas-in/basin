//! Phase 1 of ADR 0014 (pg_query as canonical parser).
//!
//! This module hosts the libpg_query-based parser wrapper and statement-kind
//! classifier. Phase 2 will route SELECT/DML through a PgNode → DataFusion
//! LogicalPlan translator that lives in a sibling module. Phase 3 fills in
//! the long-tail SQL features (window funcs, GROUPING SETS, recursive CTEs,
//! MERGE, LATERAL).
//!
//! Phase 5.13.C: the `BASIN_PG_QUERY` env-gate has been removed. libpg_query
//! now unconditionally parses every incoming statement at the top of
//! [`crate::executor::execute`]; unsupported kinds are rejected with
//! SQLSTATE 0A000 before sqlparser sees them, and all textual prescreens
//! have been migrated to typed AST matches (Phase 5.13.B). sqlparser
//! remains only for DML/DDL that needs its AST downstream.

use basin_common::{BasinError, Result};
use pg_query::protobuf::{node::Node as NodeEnum, Node};

/// Phase-1 parse-tree handle.
///
/// Newtype wrapper around [`pg_query::protobuf::ParseResult`] so we can
/// hang Basin-specific helpers off it (e.g. AST matchers in agents 2–4)
/// without bleeding the prost-generated type all over the call sites.
#[derive(Debug)]
pub struct ParseTree(pub pg_query::protobuf::ParseResult);

impl ParseTree {
    /// Top-level statement nodes in source order. Statements without an
    /// inner node (e.g. an empty `;` slot returned by libpg_query) are
    /// skipped.
    pub fn stmts(&self) -> impl Iterator<Item = &Node> {
        self.0.stmts.iter().filter_map(|s| s.stmt.as_deref())
    }
}

/// Coarse top-level statement classification. We dispatch on this before
/// reaching for the full pg_query NodeEnum so the executor's routing
/// table stays readable and the unsupported-kind reject set is one
/// pattern match.
///
/// Anything we haven't yet enumerated lands in [`StmtKind::Other`]; that
/// covers things like `CREATE SCHEMA`, `GRANT`, `COMMENT`, `COPY`, etc.
/// which Phase 1 doesn't need to discriminate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StmtKind {
    // DML
    Select,
    Insert,
    Update,
    Delete,
    // Table DDL
    CreateTable,
    AlterTable,
    DropTable,
    CreateIndex,
    // Type / domain DDL
    CreateType,
    AlterType,
    CreateDomain,
    DropDomain,
    // Materialized view DDL
    CreateMatView,
    RefreshMatView,
    DropMatView,
    // Routine DDL
    CreateFunction,
    AlterFunction,
    /// `ALTER FUNCTION <name>(<args>) RENAME TO <new>` — libpg_query
    /// routes this as `RenameStmt(rename_type = ObjectFunction)`, distinct
    /// from the other `AlterFunctionStmt` actions (OWNER TO, SET …, etc.).
    AlterFunctionRename,
    DropFunction,
    CreateProcedure,
    Call,
    DropProcedure,
    // Schema DDL (CREATE / DROP handled by sqlparser; RENAME pre-screened)
    /// `ALTER SCHEMA <old> RENAME TO <new>` — libpg_query routes this as
    /// `RenameStmt(rename_type = ObjectSchema)`.
    AlterSchemaRename,
    // Sequence DDL
    CreateSequence,
    AlterSequence,
    DropSequence,
    // Policy DDL
    CreatePolicy,
    DropPolicy,
    // Maintenance / utility
    Vacuum,
    Analyze,
    Cluster,
    Lock,
    // DBA / RBAC primitives (syntactic-accept-only; real auth from auth.uid/role)
    Comment,
    Explain,
    CreateRole,
    DropRole,
    AlterRole,
    Grant,
    Revoke,
    VariableSet,
    VariableShow,
    // Async notification (not on roadmap per ADR 0012 — LISTEN/NOTIFY are
    // explicit non-goals; remain in is_unsupported() to surface 0A000)
    Listen,
    Notify,
    Unlisten,
    // Prepared / cursor (syntactic-accept-only; see noop_accept.rs)
    Prepare,
    Execute,
    Deallocate,
    // Cursor lifecycle — real implementation in sibling agent (a193aadd)
    DeclareCursor,
    Fetch,
    /// `MOVE <direction> [FROM|IN] <cursor>` — libpg_query routes this as
    /// `FetchStmt(ismove = true)`, distinct from plain FETCH (`ismove = false`).
    Move,
    Close,
    // Extension / trigger (syntactic-accept-only; see noop_accept.rs)
    CreateExtension,
    DropExtension,
    CreateTrigger,
    DropTrigger,
    // Transaction control (syntactic-accept-only; Basin is auto-commit)
    BeginTransaction,
    Commit,
    Rollback,
    Savepoint,
    // TRUNCATE — real operation (delete all rows + optionally restart seqs)
    Truncate,
    // Foreign Data Wrappers / Foreign tables (accept-only; Basin has no FDW execution)
    CreateFdw,
    DropFdw,
    CreateForeignServer,
    DropForeignServer,
    CreateUserMapping,
    DropUserMapping,
    CreateForeignTable,
    DropForeignTable,
    ImportForeignSchema,
    // Ownership / reassignment (accept-only)
    ReassignOwned,
    DropOwned,
    // Default privileges (accept-only)
    AlterDefaultPrivileges,
    // Constraint deferrability (accept-only)
    SetConstraints,
    // Security labels (accept-only)
    SecurityLabel,
    // MERGE INTO … USING … ON … WHEN MATCHED / NOT MATCHED
    // (syntactic-accept-only in v0.1; see noop_accept.rs)
    Merge,
    // REINDEX — Basin indexes managed by DataFusion's adaptive planner
    // (syntactic-accept-only; see noop_accept.rs)
    Reindex,
    // PL/pgSQL anonymous block — ADR 0012 design exclusion; no PL/pgSQL runtime.
    DoBlock,
    // CREATE TABLE … PARTITION OF — declarative partitioning is a design
    // exclusion (flat-storage model); surface 0A000 per ADR 0012.
    CreateTablePartitionOf,
    /// Anything we don't yet dispatch on. Routed to the existing
    /// sqlparser pipeline by the executor.
    Other,
}

impl StmtKind {
    /// Human-readable name, used in error messages.
    pub fn as_label(&self) -> &'static str {
        match self {
            StmtKind::Select => "SELECT",
            StmtKind::Insert => "INSERT",
            StmtKind::Update => "UPDATE",
            StmtKind::Delete => "DELETE",
            StmtKind::CreateTable => "CREATE TABLE",
            StmtKind::AlterTable => "ALTER TABLE",
            StmtKind::DropTable => "DROP TABLE",
            StmtKind::CreateIndex => "CREATE INDEX",
            StmtKind::CreateType => "CREATE TYPE",
            StmtKind::AlterType => "ALTER TYPE",
            StmtKind::CreateDomain => "CREATE DOMAIN",
            StmtKind::DropDomain => "DROP DOMAIN",
            StmtKind::CreateMatView => "CREATE MATERIALIZED VIEW",
            StmtKind::RefreshMatView => "REFRESH MATERIALIZED VIEW",
            StmtKind::DropMatView => "DROP MATERIALIZED VIEW",
            StmtKind::CreateFunction => "CREATE FUNCTION",
            StmtKind::AlterFunction => "ALTER FUNCTION",
            StmtKind::AlterFunctionRename => "ALTER FUNCTION … RENAME TO",
            StmtKind::DropFunction => "DROP FUNCTION",
            StmtKind::CreateProcedure => "CREATE PROCEDURE",
            StmtKind::Call => "CALL",
            StmtKind::DropProcedure => "DROP PROCEDURE",
            StmtKind::AlterSchemaRename => "ALTER SCHEMA … RENAME TO",
            StmtKind::CreateSequence => "CREATE SEQUENCE",
            StmtKind::AlterSequence => "ALTER SEQUENCE",
            StmtKind::DropSequence => "DROP SEQUENCE",
            StmtKind::CreatePolicy => "CREATE POLICY",
            StmtKind::DropPolicy => "DROP POLICY",
            StmtKind::Vacuum => "VACUUM",
            StmtKind::Analyze => "ANALYZE",
            StmtKind::Cluster => "CLUSTER",
            StmtKind::Lock => "LOCK",
            StmtKind::Comment => "COMMENT",
            StmtKind::Explain => "EXPLAIN",
            StmtKind::CreateRole => "CREATE ROLE",
            StmtKind::DropRole => "DROP ROLE",
            StmtKind::AlterRole => "ALTER ROLE",
            StmtKind::Grant => "GRANT",
            StmtKind::Revoke => "REVOKE",
            StmtKind::VariableSet => "SET",
            StmtKind::VariableShow => "SHOW",
            StmtKind::Listen => "LISTEN",
            StmtKind::Notify => "NOTIFY",
            StmtKind::Unlisten => "UNLISTEN",
            StmtKind::Prepare => "PREPARE",
            StmtKind::Execute => "EXECUTE",
            StmtKind::Deallocate => "DEALLOCATE",
            StmtKind::DeclareCursor => "DECLARE CURSOR",
            StmtKind::Fetch => "FETCH",
            StmtKind::Move => "MOVE",
            StmtKind::Close => "CLOSE",
            StmtKind::CreateExtension => "CREATE EXTENSION",
            StmtKind::DropExtension => "DROP EXTENSION",
            StmtKind::CreateTrigger => "CREATE TRIGGER",
            StmtKind::DropTrigger => "DROP TRIGGER",
            StmtKind::BeginTransaction => "BEGIN",
            StmtKind::Commit => "COMMIT",
            StmtKind::Rollback => "ROLLBACK",
            StmtKind::Savepoint => "SAVEPOINT",
            StmtKind::Truncate => "TRUNCATE",
            StmtKind::CreateFdw => "CREATE FOREIGN DATA WRAPPER",
            StmtKind::DropFdw => "DROP FOREIGN DATA WRAPPER",
            StmtKind::CreateForeignServer => "CREATE SERVER",
            StmtKind::DropForeignServer => "DROP SERVER",
            StmtKind::CreateUserMapping => "CREATE USER MAPPING",
            StmtKind::DropUserMapping => "DROP USER MAPPING",
            StmtKind::CreateForeignTable => "CREATE FOREIGN TABLE",
            StmtKind::DropForeignTable => "DROP FOREIGN TABLE",
            StmtKind::ImportForeignSchema => "IMPORT FOREIGN SCHEMA",
            StmtKind::ReassignOwned => "REASSIGN OWNED",
            StmtKind::DropOwned => "DROP OWNED",
            StmtKind::AlterDefaultPrivileges => "ALTER DEFAULT PRIVILEGES",
            StmtKind::SetConstraints => "SET CONSTRAINTS",
            StmtKind::SecurityLabel => "SECURITY LABEL",
            StmtKind::Merge => "MERGE",
            StmtKind::Reindex => "REINDEX",
            StmtKind::DoBlock => "DO",
            StmtKind::CreateTablePartitionOf => "CREATE TABLE … PARTITION OF",
            StmtKind::Other => "<other>",
        }
    }

    /// `true` if this kind is in the "known-but-not-yet-shipped" set
    /// rejected by [`reject_unsupported`] with SQLSTATE 0A000.
    ///
    /// Kinds that have been moved to the syntactic-accept (noop) set in
    /// `noop_accept::try_accept_as_noop` must be removed from here so
    /// `reject_unsupported` does not block them before noop dispatch runs.
    ///
    /// LISTEN / NOTIFY / UNLISTEN are explicit non-goals per ADR 0012 and
    /// remain here — they surface 0A000 intentionally.
    /// rejected by [`reject_unsupported`]. Centralised so the executor
    /// and the test suite stay in sync.
    pub fn is_unsupported(&self) -> bool {
        // Only ADR 0012 design exclusions are unconditionally rejected.
        // Everything else (BEGIN/COMMIT/PREPARE/cursor lifecycle/extensions/
        // triggers) is either noop-accepted via noop_accept.rs or has a real
        // implementation; reject_unsupported must let them through.
        matches!(
            self,
            StmtKind::Listen
                | StmtKind::Notify
                | StmtKind::Unlisten
                // ADR 0012 design exclusions — no PL/pgSQL runtime
                | StmtKind::DoBlock
                // Declarative partitioning — flat-storage design exclusion
                | StmtKind::CreateTablePartitionOf
        )
    }
}

/// Parse `sql` with libpg_query (the real Postgres parser).
///
/// Maps `pg_query::Error::Parse(_)` to [`BasinError::InvalidSchema`],
/// matching the existing convention in `executor.rs` for sqlparser-side
/// parse failures. libpg_query embeds the column position in the error
/// message text (e.g. `"syntax error at or near \"FROM\" at character 8"`),
/// so we don't need a separate `position` field on the variant.
///
/// The Postgres SQLSTATE for `syntax_error` is `42601`. The router
/// renders [`BasinError::InvalidSchema`] under that class today; once
/// ADR 0014 lands a dedicated `BasinError::Parse` variant Phase 2 will
/// switch over.
pub fn parse(sql: &str) -> Result<ParseTree> {
    match pg_query::parse(sql) {
        Ok(result) => Ok(ParseTree(result.protobuf)),
        Err(pg_query::Error::Parse(msg)) => Err(BasinError::InvalidSchema(format!(
            "pg_query parse error (SQLSTATE 42601): {msg}"
        ))),
        Err(other) => Err(BasinError::internal(format!(
            "pg_query internal error: {other}"
        ))),
    }
}

/// Shape of a plain `CREATE TABLE … AS <query>` statement, as classified
/// by libpg_query (the real PostgreSQL parser).
#[derive(Debug, Clone)]
pub struct CtasShape {
    /// `true` for `WITH NO DATA` (schema-only clone); `false` for
    /// `WITH DATA` / no clause (populate from the query result).
    pub skip_data: bool,
    /// Explicit `(col, …)` LHS column-name list, if present. PostgreSQL
    /// renames the query's output columns positionally with this list.
    pub col_names: Vec<String>,
}

/// Detect a plain `CREATE TABLE [(col, …)] … AS <query> [WITH [NO] DATA]`
/// statement and lift the bits sqlparser 0.61 cannot represent.
///
/// libpg_query understands both the trailing `WITH [NO] DATA` clause
/// (exposed as `into.skip_data`) and the bare LHS column-name list
/// (`into.col_names`). sqlparser 0.61's `CreateTable` AST has a field
/// for neither: `WITH [NO] DATA` makes its statement parser fail with
/// "Expected: end of statement, found: WITH", and the bare column list
/// makes it fail with "Expected: a data type name". The executor uses
/// this classification to strip both forms textually before sqlparser
/// sees the SQL, then re-applies them from the captured metadata.
///
/// Returns `None` for anything that is not a plain CTAS
/// (`CREATE MATERIALIZED VIEW`, the `SELECT … INTO` form, or any
/// non-CTAS statement) — caller leaves the SQL untouched.
pub fn ctas_shape(node: &Node) -> Option<CtasShape> {
    let inner = node.node.as_ref()?;
    let NodeEnum::CreateTableAsStmt(ctas) = inner else {
        return None;
    };
    // `CREATE MATERIALIZED VIEW` also lands on CreateTableAsStmt; that
    // form has its own dedicated path. `SELECT … INTO foo` is the
    // is_select_into shape and is likewise out of scope here.
    if ctas.objtype == pg_query::protobuf::ObjectType::ObjectMatview as i32 || ctas.is_select_into {
        return None;
    }
    let into = ctas.into.as_ref();
    // `into` is always present for a real CTAS; default to "populate"
    // if the field is somehow absent rather than silently dropping rows.
    let skip_data = into.map(|i| i.skip_data).unwrap_or(false);
    let col_names = into
        .map(|i| {
            i.col_names
                .iter()
                .filter_map(|c| match c.node.as_ref() {
                    Some(NodeEnum::String(s)) => Some(s.sval.clone()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    Some(CtasShape {
        skip_data,
        col_names,
    })
}

/// Classify a top-level statement node.
///
/// `node` must be the inner `RawStmt.stmt.node` — i.e. the per-statement
/// value yielded by [`ParseTree::stmts`]. Returns [`StmtKind::Other`]
/// for any variant we haven't enumerated yet; callers should treat
/// `Other` as "fall through to the legacy pipeline", not "reject".
pub fn stmt_kind(node: &Node) -> StmtKind {
    let Some(inner) = node.node.as_ref() else {
        return StmtKind::Other;
    };
    match inner {
        NodeEnum::SelectStmt(_) => StmtKind::Select,
        NodeEnum::InsertStmt(_) => StmtKind::Insert,
        NodeEnum::UpdateStmt(_) => StmtKind::Update,
        NodeEnum::DeleteStmt(_) => StmtKind::Delete,

        NodeEnum::CreateStmt(s) => {
            // A `CREATE TABLE … PARTITION OF parent FOR VALUES …` carries a
            // non-None `partbound`; treat it as a distinct design-exclusion kind.
            if s.partbound.is_some() {
                StmtKind::CreateTablePartitionOf
            } else {
                StmtKind::CreateTable
            }
        }
        NodeEnum::AlterTableStmt(_) => StmtKind::AlterTable,
        NodeEnum::IndexStmt(_) => StmtKind::CreateIndex,

        NodeEnum::CreateEnumStmt(_) => StmtKind::CreateType,
        NodeEnum::CompositeTypeStmt(_) => StmtKind::CreateType,
        NodeEnum::CreateRangeStmt(_) => StmtKind::CreateType,
        NodeEnum::AlterEnumStmt(_) => StmtKind::AlterType,
        NodeEnum::AlterTypeStmt(_) => StmtKind::AlterType,
        NodeEnum::CreateDomainStmt(_) => StmtKind::CreateDomain,

        NodeEnum::ViewStmt(_) => StmtKind::Other,
        NodeEnum::CreateTableAsStmt(s) => {
            // CREATE MATERIALIZED VIEW arrives here with objtype =
            // ObjectMatview (24). Plain CREATE TABLE AS is a different
            // animal and stays as Other for Phase 1.
            if s.objtype == pg_query::protobuf::ObjectType::ObjectMatview as i32 {
                StmtKind::CreateMatView
            } else {
                StmtKind::Other
            }
        }
        NodeEnum::RefreshMatViewStmt(_) => StmtKind::RefreshMatView,

        NodeEnum::CreateFunctionStmt(s) => {
            if s.is_procedure {
                StmtKind::CreateProcedure
            } else {
                StmtKind::CreateFunction
            }
        }
        NodeEnum::AlterFunctionStmt(_) => StmtKind::AlterFunction,
        NodeEnum::CallStmt(_) => StmtKind::Call,

        // `ALTER FUNCTION … RENAME TO` and `ALTER SCHEMA … RENAME TO`
        // both arrive as RenameStmt; the rename_type discriminates them.
        NodeEnum::RenameStmt(s) => {
            use pg_query::protobuf::ObjectType as O;
            match O::try_from(s.rename_type) {
                Ok(O::ObjectFunction) | Ok(O::ObjectProcedure) | Ok(O::ObjectRoutine) => {
                    StmtKind::AlterFunctionRename
                }
                Ok(O::ObjectSchema) => StmtKind::AlterSchemaRename,
                _ => StmtKind::Other,
            }
        }

        NodeEnum::CreateSeqStmt(_) => StmtKind::CreateSequence,
        NodeEnum::AlterSeqStmt(_) => StmtKind::AlterSequence,

        NodeEnum::CreatePolicyStmt(_) => StmtKind::CreatePolicy,

        NodeEnum::VacuumStmt(s) => {
            // libpg_query maps both VACUUM and ANALYZE to VacuumStmt; the
            // option list (or the `is_vacuumcmd` bit on PG16+) distinguishes
            // them. is_vacuumcmd is false for bare ANALYZE.
            if s.is_vacuumcmd {
                StmtKind::Vacuum
            } else {
                StmtKind::Analyze
            }
        }
        NodeEnum::ClusterStmt(_) => StmtKind::Cluster,
        NodeEnum::LockStmt(_) => StmtKind::Lock,
        NodeEnum::CommentStmt(_) => StmtKind::Comment,
        NodeEnum::ExplainStmt(_) => StmtKind::Explain,
        NodeEnum::CreateRoleStmt(_) => StmtKind::CreateRole,
        NodeEnum::DropRoleStmt(_) => StmtKind::DropRole,
        NodeEnum::AlterRoleStmt(_) => StmtKind::AlterRole,
        NodeEnum::GrantStmt(s) => {
            // libpg_query uses GrantStmt for both GRANT and REVOKE;
            // `is_grant` discriminates them.
            if s.is_grant {
                StmtKind::Grant
            } else {
                StmtKind::Revoke
            }
        }
        NodeEnum::VariableSetStmt(_) => StmtKind::VariableSet,
        NodeEnum::VariableShowStmt(_) => StmtKind::VariableShow,

        NodeEnum::ListenStmt(_) => StmtKind::Listen,
        NodeEnum::NotifyStmt(_) => StmtKind::Notify,
        NodeEnum::UnlistenStmt(_) => StmtKind::Unlisten,

        NodeEnum::PrepareStmt(_) => StmtKind::Prepare,
        NodeEnum::ExecuteStmt(_) => StmtKind::Execute,
        NodeEnum::DeallocateStmt(_) => StmtKind::Deallocate,
        NodeEnum::DeclareCursorStmt(_) => StmtKind::DeclareCursor,
        NodeEnum::FetchStmt(s) => {
            // MOVE and FETCH both arrive as FetchStmt; `ismove` discriminates.
            if s.ismove {
                StmtKind::Move
            } else {
                StmtKind::Fetch
            }
        }
        NodeEnum::ClosePortalStmt(_) => StmtKind::Close,

        NodeEnum::CreateExtensionStmt(_) => StmtKind::CreateExtension,
        NodeEnum::CreateTrigStmt(_) => StmtKind::CreateTrigger,

        NodeEnum::TruncateStmt(_) => StmtKind::Truncate,

        // Foreign Data Wrappers / Foreign tables
        NodeEnum::CreateFdwStmt(_) => StmtKind::CreateFdw,
        NodeEnum::CreateForeignServerStmt(_) => StmtKind::CreateForeignServer,
        NodeEnum::CreateUserMappingStmt(_) => StmtKind::CreateUserMapping,
        NodeEnum::DropUserMappingStmt(_) => StmtKind::DropUserMapping,
        NodeEnum::CreateForeignTableStmt(_) => StmtKind::CreateForeignTable,
        NodeEnum::ImportForeignSchemaStmt(_) => StmtKind::ImportForeignSchema,

        // Ownership / reassignment
        NodeEnum::ReassignOwnedStmt(_) => StmtKind::ReassignOwned,
        NodeEnum::DropOwnedStmt(_) => StmtKind::DropOwned,

        // Default privileges
        NodeEnum::AlterDefaultPrivilegesStmt(_) => StmtKind::AlterDefaultPrivileges,

        // Constraint deferrability
        NodeEnum::ConstraintsSetStmt(_) => StmtKind::SetConstraints,

        // Security labels
        NodeEnum::SecLabelStmt(_) => StmtKind::SecurityLabel,

        NodeEnum::TransactionStmt(s) => {
            use pg_query::protobuf::TransactionStmtKind as Tk;
            match Tk::try_from(s.kind) {
                Ok(Tk::TransStmtBegin) | Ok(Tk::TransStmtStart) => StmtKind::BeginTransaction,
                Ok(Tk::TransStmtCommit) | Ok(Tk::TransStmtCommitPrepared) => StmtKind::Commit,
                Ok(Tk::TransStmtRollback)
                | Ok(Tk::TransStmtRollbackTo)
                | Ok(Tk::TransStmtRollbackPrepared) => StmtKind::Rollback,
                Ok(Tk::TransStmtSavepoint) | Ok(Tk::TransStmtRelease) => StmtKind::Savepoint,
                Ok(Tk::TransStmtPrepare) => StmtKind::Prepare,
                _ => StmtKind::Other,
            }
        }

        // DROP <kind> ... is one node — DropStmt — with an ObjectType
        // discriminator on `remove_type`. Pull the kind out so we can
        // dispatch the same way as the corresponding CREATE.
        NodeEnum::DropStmt(s) => {
            use pg_query::protobuf::ObjectType as O;
            match O::try_from(s.remove_type) {
                Ok(O::ObjectTable) => StmtKind::DropTable,
                Ok(O::ObjectMatview) => StmtKind::DropMatView,
                Ok(O::ObjectDomain) => StmtKind::DropDomain,
                Ok(O::ObjectFunction) => StmtKind::DropFunction,
                Ok(O::ObjectProcedure) | Ok(O::ObjectRoutine) => StmtKind::DropProcedure,
                Ok(O::ObjectSequence) => StmtKind::DropSequence,
                Ok(O::ObjectPolicy) => StmtKind::DropPolicy,
                Ok(O::ObjectTrigger) => StmtKind::DropTrigger,
                Ok(O::ObjectFdw) => StmtKind::DropFdw,
                Ok(O::ObjectForeignServer) => StmtKind::DropForeignServer,
                Ok(O::ObjectForeignTable) => StmtKind::DropForeignTable,
                Ok(O::ObjectExtension) => StmtKind::DropExtension,
                _ => StmtKind::Other,
            }
        }

        NodeEnum::MergeStmt(_) => StmtKind::Merge,
        NodeEnum::ReindexStmt(_) => StmtKind::Reindex,

        // PL/pgSQL anonymous block — ADR 0012 design exclusion
        NodeEnum::DoStmt(_) => StmtKind::DoBlock,

        _ => StmtKind::Other,
    }
}

/// Precise classification of a transaction-control statement, with the
/// savepoint name lifted straight from libpg_query's authoritative parse
/// (`TransactionStmt.savepoint_name`) rather than re-scraped from the SQL
/// text.  This is what the executor uses to tell `ROLLBACK` apart from
/// `ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] <name>` (PostgreSQL makes
/// every one of `WORK`, `TRANSACTION`, and `SAVEPOINT` optional, so the
/// old `contains("SAVEPOINT")` text heuristic mis-routed `ROLLBACK TO s`
/// into a full-transaction rollback).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnStmt {
    /// `BEGIN` / `START TRANSACTION`.
    Begin,
    /// `COMMIT` / `COMMIT PREPARED`.
    Commit,
    /// Bare `ROLLBACK` / `ABORT` / `ROLLBACK PREPARED` — undo whole txn.
    Rollback,
    /// `ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] <name>`.
    RollbackToSavepoint(String),
    /// `SAVEPOINT <name>`.
    Savepoint(String),
    /// `RELEASE [SAVEPOINT] <name>`.
    ReleaseSavepoint(String),
    /// Any other transaction node (PREPARE TRANSACTION, etc.).
    Other,
}

/// Classify `node` as a [`TxnStmt`] when it is a `TransactionStmt`.
/// Returns `None` for non-transaction statements.  The savepoint name
/// comes from libpg_query's parsed `savepoint_name` field, so it is
/// exact regardless of optional `WORK`/`TRANSACTION`/`SAVEPOINT` noise
/// words, comments, or identifier quoting.
pub fn txn_stmt(node: &Node) -> Option<TxnStmt> {
    use pg_query::protobuf::TransactionStmtKind as Tk;
    let NodeEnum::TransactionStmt(s) = node.node.as_ref()? else {
        return None;
    };
    Some(match Tk::try_from(s.kind) {
        Ok(Tk::TransStmtBegin) | Ok(Tk::TransStmtStart) => TxnStmt::Begin,
        Ok(Tk::TransStmtCommit) | Ok(Tk::TransStmtCommitPrepared) => TxnStmt::Commit,
        Ok(Tk::TransStmtRollback) | Ok(Tk::TransStmtRollbackPrepared) => TxnStmt::Rollback,
        Ok(Tk::TransStmtRollbackTo) => TxnStmt::RollbackToSavepoint(s.savepoint_name.clone()),
        Ok(Tk::TransStmtSavepoint) => TxnStmt::Savepoint(s.savepoint_name.clone()),
        Ok(Tk::TransStmtRelease) => TxnStmt::ReleaseSavepoint(s.savepoint_name.clone()),
        _ => TxnStmt::Other,
    })
}

/// Rewrite the `@@` tsvector-match operator to a `tsvector_match_udf(lhs, rhs)`
/// function call before sqlparser / DataFusion see the SQL.
///
/// DataFusion has no built-in handler for the `@@` binary operator that
/// PostgreSQL uses for full-text search (`tsvector @@ tsquery`).  Basin
/// registers `tsvector_match_udf` as a stub UDF (always returns `true` for
/// v0.1 — semantics deferred to `basin-fts`).  This function rewrites every
/// bare `@@` occurrence in the SQL string to `tsvector_match_udf(lhs, rhs)`.
///
/// # Strategy
///
/// The rewrite is text-level rather than parse-tree-level.  The `@@` token
/// cannot appear inside string literals (`'...'`) as anything other than two
/// `@` characters, but to be safe we skip past single-quoted literals when
/// scanning.  The token `@@@` (three `@`s) is *not* rewritten — it appears in
/// test SQL as an intentionally invalid form and must remain unchanged.
///
/// The left-hand operand is extracted by walking backward from the `@@`
/// position using the same brace-balanced algorithm as the vector-operator
/// rewriter in `udf.rs`.  The right-hand operand is the balanced expression
/// after the `@@`.
pub fn rewrite_tsvector_at_at(sql: &str) -> String {
    // Two-stage rewrite:
    //   1. Strip `::tsvector` / `::tsquery` casts (Basin stores both as Utf8).
    //      DataFusion's planner rejects these as unsupported cast targets.
    //   2. Lower the `@@` operator to `tsvector_match_udf(lhs, rhs)`.
    //
    // The cast strip must happen BEFORE the `@@` rewrite so that the
    // operand-capture logic sees a stripped (and thus shorter) operand to
    // capture. Without this ordering the operand captures the bare value
    // and the trailing `::type` becomes a syntax error.
    let stripped = strip_fts_casts(sql);
    if !stripped.contains("@@") {
        return stripped;
    }
    let mut out = stripped;
    loop {
        // Find the next `@@` that is not part of `@@@`.
        let Some(pos) = find_at_at(&out) else {
            break;
        };
        let lhs_end = pos;
        let rhs_start = pos + 2; // skip `@@`

        let (lhs_from, lhs_to) = extract_left_operand_pg(&out, lhs_end);
        let (rhs_from, rhs_to) = extract_right_operand_pg(&out, rhs_start);

        let lhs = out[lhs_from..lhs_to].to_string();
        let rhs = out[rhs_from..rhs_to].to_string();
        let replacement = format!("tsvector_match_udf({lhs}, {rhs})");

        out.replace_range(lhs_from..rhs_to, &replacement);
        // No need to advance scan_pos — if there is another `@@` the loop
        // finds it on the next iteration. The replacement text contains no
        // `@@` so we cannot loop infinitely.
    }
    out
}

/// Strip `::tsvector` and `::tsquery` casts from expressions.
///
/// Basin stores both tsvector and tsquery as Utf8 internally (stub — no
/// real tokenisation). DataFusion's planner doesn't recognise these as
/// cast targets at expression level, so `'a fox'::tsvector` would fail
/// with "Unsupported SQL type" before the executor even runs. Stripping
/// the cast is safe because the source expression is already Utf8.
///
/// String literals, double-quoted identifiers, and SQL comments are
/// honoured so we don't mangle text data or commented-out code.
fn strip_fts_casts(sql: &str) -> String {
    if !sql.contains("::") {
        return sql.to_string();
    }
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < len {
        let b = bytes[i];
        // Pass through single-quoted literals untouched.
        if b == b'\'' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    if i + 1 < len && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Pass through double-quoted identifiers untouched.
        if b == b'"' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Pass through `--` line comments.
        if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            let start = i;
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Pass through `/* */` block comments.
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Look for `::tsvector` / `::tsquery`.
        if b == b':' && i + 1 < len && bytes[i + 1] == b':' {
            // Peek the type name (alnum/`_`).
            let mut j = i + 2;
            while j < len && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            let name_start = j;
            while j < len && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            let name = &sql[name_start..j];
            if name.eq_ignore_ascii_case("tsvector") || name.eq_ignore_ascii_case("tsquery") {
                // Drop the `::<name>` entirely. Skip any trailing `(...)`
                // type modifier (defensive — neither type takes a modifier
                // today but PG allows e.g. `varchar(255)`).
                let mut k = j;
                while k < len && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if k < len && bytes[k] == b'(' {
                    let mut depth = 1i32;
                    k += 1;
                    while k < len && depth > 0 {
                        match bytes[k] {
                            b'(' => depth += 1,
                            b')' => depth -= 1,
                            _ => {}
                        }
                        k += 1;
                    }
                    i = k;
                } else {
                    i = j;
                }
                continue;
            }
        }
        // Pass through one UTF-8 character (could be multi-byte).
        let char_len = utf8_char_len(b);
        let end = (i + char_len).min(len);
        out.push_str(&sql[i..end]);
        i = end;
    }
    out
}

/// Length in bytes of a UTF-8 codepoint given its lead byte.
fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b < 0xC0 {
        1 // continuation byte (defensive — shouldn't be the lead)
    } else if b < 0xE0 {
        2
    } else if b < 0xF0 {
        3
    } else {
        4
    }
}

/// Find the byte position of the first `@@` that is:
///   1. Not part of `@@@` (three `@`s).
///   2. Not inside a single-quoted string literal.
///   3. Not inside a double-quoted identifier.
///   4. Not inside an `--` line comment or `/* */` block comment.
///
/// Returns the position of the first `@` in the pair.
fn find_at_at(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0usize;
    while i + 1 < len {
        // Skip single-quoted string literals.
        if bytes[i] == b'\'' {
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    if i + 1 < len && bytes[i + 1] == b'\'' {
                        i += 2; // `''` escape
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        // Skip double-quoted identifiers.
        if bytes[i] == b'"' {
            i += 1;
            while i < len {
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Skip `--` line comments to end of line.
        if bytes[i] == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Skip `/* ... */` block comments (no nesting in v0.1).
        if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2;
            }
            continue;
        }
        if bytes[i] == b'@' && bytes[i + 1] == b'@' {
            let preceding_at = i > 0 && bytes[i - 1] == b'@';
            let following_at = i + 2 < len && bytes[i + 2] == b'@';
            if !preceding_at && !following_at {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Walk backward from `end` (exclusive) to capture one operand token.
/// Mirrors the logic in `udf::extract_left_operand` but lives here so we
/// don't depend on the private `udf` helpers.
///
/// Handles, in order:
///   * `)` — balanced paren group + preceding function-name identifier.
///   * `'...'` single-quoted literal (with `''` escapes).
///   * `"..."` double-quoted identifier.
///   * identifier / number (alnum, `_`, `.`).
///
/// After capturing a base unit, walks back over any number of `::cast`
/// suffixes (each followed by another base unit on the left).
fn extract_left_operand_pg(s: &str, end: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let mut i = end;
    // Skip trailing whitespace.
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    let operand_end = i;
    // Repeatedly capture one "unit", then if preceded by `::` keep going.
    loop {
        let new_i = capture_left_unit(bytes, i);
        if new_i == i {
            // Made no progress — bail out.
            break;
        }
        i = new_i;
        // Skip whitespace then check for `::`.
        let mut j = i;
        while j > 0 && bytes[j - 1].is_ascii_whitespace() {
            j -= 1;
        }
        if j >= 2 && bytes[j - 1] == b':' && bytes[j - 2] == b':' {
            // Consume the `::` and continue to capture the value on the
            // left of the cast.
            i = j - 2;
            // Skip whitespace again before the next unit.
            while i > 0 && bytes[i - 1].is_ascii_whitespace() {
                i -= 1;
            }
            continue;
        }
        break;
    }
    (i, operand_end)
}

/// Capture one "unit" walking backwards: paren-group + preceding identifier,
/// quoted string, quoted identifier, or plain identifier / number.
/// Returns the new (smaller) `i` after consumption. If no consumption is
/// possible returns the input `i` unchanged.
fn capture_left_unit(bytes: &[u8], i_in: usize) -> usize {
    let mut i = i_in;
    if i == 0 {
        return i;
    }
    let last = bytes[i - 1];
    if last == b')' {
        // Balanced paren group. Track string literals so unbalanced parens
        // inside `'...'` don't throw off the depth counter.
        let mut depth = 1i32;
        i -= 1;
        while i > 0 && depth > 0 {
            i -= 1;
            match bytes[i] {
                b')' => depth += 1,
                b'(' => depth -= 1,
                b'\'' => {
                    // Walk back over the entire single-quoted literal.
                    while i > 0 {
                        i -= 1;
                        if bytes[i] == b'\'' {
                            // Could be the start, or part of an escape `''`.
                            if i > 0 && bytes[i - 1] == b'\'' {
                                i -= 1; // consume the escape pair
                                continue;
                            }
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
        // Include a preceding identifier (function name), if any.
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    } else if last == b'\'' {
        // Single-quoted literal — walk back to opening quote, honouring `''` escapes.
        i -= 1;
        while i > 0 {
            i -= 1;
            if bytes[i] == b'\'' {
                if i > 0 && bytes[i - 1] == b'\'' {
                    i -= 1; // consume `''` escape, keep going
                    continue;
                }
                break;
            }
        }
    } else if last == b'"' {
        // Double-quoted identifier — walk back to opening quote.
        i -= 1;
        while i > 0 {
            i -= 1;
            if bytes[i] == b'"' {
                break;
            }
        }
    } else if last.is_ascii_alphanumeric() || last == b'_' || last == b'.' {
        // Plain identifier / number.
        while i > 0
            && (bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.')
        {
            i -= 1;
        }
    }
    i
}

/// Walk forward from `start` to capture one operand token.
///
/// After the base unit, consumes any number of trailing `::cast` suffixes
/// (each followed by another base unit).
fn extract_right_operand_pg(s: &str, start: usize) -> (usize, usize) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = start;
    // Skip leading whitespace.
    while i < len && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let operand_start = i;
    loop {
        let new_i = capture_right_unit(bytes, i);
        if new_i == i {
            break;
        }
        i = new_i;
        // Look ahead for `::cast`.
        let mut j = i;
        while j < len && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j + 1 < len && bytes[j] == b':' && bytes[j + 1] == b':' {
            i = j + 2;
            while i < len && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }
        break;
    }
    (operand_start, i)
}

/// Capture one "unit" walking forward from `i_in`. Returns the new `i`.
fn capture_right_unit(bytes: &[u8], i_in: usize) -> usize {
    let mut i = i_in;
    let len = bytes.len();
    if i >= len {
        return i;
    }
    let first = bytes[i];
    if first == b'(' {
        // Balanced paren group — string-literal aware.
        let mut depth = 1i32;
        i += 1;
        while i < len && depth > 0 {
            match bytes[i] {
                b'(' => {
                    depth += 1;
                    i += 1;
                }
                b')' => {
                    depth -= 1;
                    i += 1;
                }
                b'\'' => {
                    // Skip single-quoted literal (with `''` escape).
                    i += 1;
                    while i < len {
                        if bytes[i] == b'\'' {
                            i += 1;
                            if i < len && bytes[i] == b'\'' {
                                i += 1;
                                continue;
                            }
                            break;
                        }
                        i += 1;
                    }
                }
                _ => i += 1,
            }
        }
    } else if first == b'\'' {
        // Single-quoted literal.
        i += 1;
        while i < len {
            if bytes[i] == b'\'' {
                i += 1;
                if i < len && bytes[i] == b'\'' {
                    i += 1; // escaped `''`
                } else {
                    break;
                }
            } else {
                i += 1;
            }
        }
    } else if first == b'"' {
        // Double-quoted identifier.
        i += 1;
        while i < len {
            if bytes[i] == b'"' {
                i += 1;
                break;
            }
            i += 1;
        }
    } else if first.is_ascii_alphanumeric() || first == b'_' {
        // Identifier / number (optional schema-qualified) with optional
        // function-call argument list.
        while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.')
        {
            i += 1;
        }
        // Optional function-call args.
        let mut j = i;
        while j < len && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j < len && bytes[j] == b'(' {
            i = capture_right_unit(bytes, j);
        }
    }
    i
}

/// Reject every top-level statement whose kind is in the "known but
/// not yet shipped" set with SQLSTATE 0A000. Stops at the first
/// unsupported statement. Returns `Ok(())` if every statement is
/// either supported or [`StmtKind::Other`].
///
/// Called unconditionally from the executor entry (Phase 5.13.C).
/// Extends the reject set as design-exclusion features are confirmed.
pub fn reject_unsupported(tree: &ParseTree) -> Result<()> {
    for node in tree.stmts() {
        let kind = stmt_kind(node);
        if kind.is_unsupported() {
            return Err(BasinError::FeatureNotSupported(format!(
                "{} is not supported (SQLSTATE 0A000)",
                kind.as_label()
            )));
        }

        // Fine-grained sub-statement checks that cannot be expressed as a single
        // StmtKind because the top-level node is shared with supported variants.

        let Some(inner) = node.node.as_ref() else {
            continue;
        };

        // CREATE TABLE with EXCLUDE constraint — Basin has no GiST / exclusion
        // index support. This is a design exclusion (flat-storage model).
        if let NodeEnum::CreateStmt(cs) = inner {
            use pg_query::protobuf::ConstrType;
            let has_exclusion = cs.table_elts.iter().any(|elt| {
                if let Some(NodeEnum::Constraint(c)) = elt.node.as_ref() {
                    c.contype == ConstrType::ConstrExclusion as i32
                } else {
                    false
                }
            });
            if has_exclusion {
                return Err(BasinError::FeatureNotSupported(
                    "EXCLUDE constraint is not supported (SQLSTATE 0A000): \
                     Basin has no GiST index backend"
                        .into(),
                ));
            }
        }

        // BUG #132 — CREATE TRIGGER / CREATE CONSTRAINT TRIGGER honest-reject.
        // Basin has no PL/pgSQL trigger runtime (ADR 0012). A silent no-op
        // here is a correctness bomb (audit rows / derived columns /
        // validation never fire). Reject loudly with SQLSTATE 0A000. Both
        // `CREATE TRIGGER` and `CREATE CONSTRAINT TRIGGER` parse to
        // `CreateTrigStmt` (the latter sets `isconstraint = true`).
        if let NodeEnum::CreateTrigStmt(cts) = inner {
            let what = if cts.isconstraint {
                "CREATE CONSTRAINT TRIGGER"
            } else {
                "CREATE TRIGGER"
            };
            return Err(BasinError::FeatureNotSupported(format!(
                "{what} is not supported (SQLSTATE 0A000): Basin has no \
                 PL/pgSQL trigger runtime. Use declarative AUTO_UPDATE / \
                 AUDIT TO / SOFT DELETE columns or a SQL-bodied REACT ON \
                 reactor (ADR 0012) instead of a row/statement trigger."
            )));
        }

        // BUG #132 — DROP TRIGGER asymmetry. No trigger can ever exist in
        // Basin, so `DROP TRIGGER ... IF EXISTS` is correctly a silent no-op
        // (noop-accepted before this gate runs) while a bare `DROP TRIGGER`
        // must error exactly as PG does ("trigger ... does not exist"). Only
        // the bare (missing_ok == false) form reaches here — the IF-EXISTS
        // form was already accepted by `noop_accept::try_accept_as_noop`.
        if let NodeEnum::DropStmt(ds) = inner {
            use pg_query::protobuf::ObjectType as O;
            if ds.remove_type == O::ObjectTrigger as i32 && !ds.missing_ok {
                let name = ds
                    .objects
                    .iter()
                    .find_map(|o| match o.node.as_ref() {
                        Some(NodeEnum::List(l)) => l.items.last().and_then(|n| {
                            if let Some(NodeEnum::String(s)) = n.node.as_ref() {
                                Some(s.sval.clone())
                            } else {
                                None
                            }
                        }),
                        _ => None,
                    })
                    .unwrap_or_else(|| "?".to_string());
                return Err(BasinError::FeatureNotSupported(format!(
                    "trigger \"{name}\" does not exist (SQLSTATE 0A000): \
                     Basin has no trigger runtime so no trigger can exist; \
                     use DROP TRIGGER IF EXISTS for an idempotent no-op."
                )));
            }
        }

        // ALTER TABLE … DETACH PARTITION — declarative partitioning design exclusion.
        if let NodeEnum::AlterTableStmt(at) = inner {
            use pg_query::protobuf::AlterTableType;
            let has_detach = at.cmds.iter().any(|cmd| {
                if let Some(NodeEnum::AlterTableCmd(c)) = cmd.node.as_ref() {
                    c.subtype == AlterTableType::AtDetachPartition as i32
                        || c.subtype == AlterTableType::AtDetachPartitionFinalize as i32
                } else {
                    false
                }
            });
            if has_detach {
                return Err(BasinError::FeatureNotSupported(
                    "ALTER TABLE … DETACH PARTITION is not supported (SQLSTATE 0A000): \
                     Basin uses flat storage without declarative partitioning"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

/// Strip the `ONLY` table-inheritance modifier from `FROM ONLY <table>` and
/// `JOIN ONLY <table>` expressions.
///
/// Basin has no table inheritance (flat-storage design); `ONLY` is syntactically
/// accepted by the real PostgreSQL parser but has no effect in Basin — every
/// query already implicitly targets the named table only. Removing the keyword
/// makes the SQL parseable by sqlparser and executable by DataFusion.
///
/// # Strategy
///
/// Text-level scan: find every occurrence of `FROM ONLY ` or `JOIN ONLY ` (case-
/// insensitive) that is followed by an identifier character, and remove the
/// `ONLY ` (5 chars). Single-quoted literals are skipped to avoid mangling
/// string values that happen to contain the phrase.
///
/// Returns the original `sql` as `Cow::Borrowed` when no rewrite is needed.
pub fn strip_only_modifier(sql: &str) -> std::borrow::Cow<'_, str> {
    // Fast path: the word ONLY doesn't appear at all.
    if !sql.to_ascii_uppercase().contains("ONLY") {
        return std::borrow::Cow::Borrowed(sql);
    }

    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::new();
    let mut i = 0;
    let mut modified = false;

    while i < len {
        // Skip single-quoted string literals.
        if bytes[i] == b'\'' {
            if modified {
                out.push('\'');
            }
            i += 1;
            while i < len {
                let ch = bytes[i];
                if modified {
                    out.push(ch as char);
                }
                i += 1;
                if ch == b'\'' {
                    // Handle escaped quotes ('').
                    if i < len && bytes[i] == b'\'' {
                        if modified {
                            out.push('\'');
                        }
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        // Check for `FROM ONLY ` or `JOIN ONLY ` (case-insensitive).
        // We match the pattern: (FROM|JOIN) followed by whitespace and ONLY and whitespace.
        // More precisely: the keyword FROM/JOIN, at least one space, then ONLY, then at
        // least one space or the end.
        let remaining = &sql[i..];
        let upper_rem = remaining.to_ascii_uppercase();

        let keyword_len = if upper_rem.starts_with("FROM") && i + 4 <= len {
            Some(4usize)
        } else if upper_rem.starts_with("JOIN") && i + 4 <= len {
            Some(4usize)
        } else {
            None
        };

        if let Some(kw_len) = keyword_len {
            // Ensure character before is a word boundary (start or non-ident).
            let prev_is_boundary = i == 0 || {
                let prev = bytes[i - 1];
                !prev.is_ascii_alphanumeric() && prev != b'_'
            };
            if prev_is_boundary {
                // Scan past the keyword and any whitespace.
                let mut j = i + kw_len;
                while j < len && bytes[j] == b' ' {
                    j += 1;
                }
                // Check for ONLY keyword.
                let rem2 = sql[j..].to_ascii_uppercase();
                if rem2.starts_with("ONLY") {
                    let after_only = j + 4;
                    // ONLY must be followed by whitespace (not part of a longer word).
                    if after_only < len
                        && (bytes[after_only] == b' '
                            || bytes[after_only] == b'\t'
                            || bytes[after_only] == b'\n'
                            || bytes[after_only] == b'\r')
                    {
                        // We have FROM/JOIN <spaces> ONLY <space>.
                        // Copy up through FROM/JOIN and the single space, then skip ONLY + one space.
                        if !modified {
                            out = sql[..i].to_string();
                            modified = true;
                        }
                        // Emit the FROM/JOIN keyword.
                        out.push_str(&sql[i..i + kw_len]);
                        // Emit ONE space (collapse any run of spaces before ONLY).
                        out.push(' ');
                        // Skip past `ONLY ` (the keyword plus its one following space).
                        i = after_only + 1;
                        continue;
                    }
                }
            }
        }

        if modified {
            out.push(bytes[i] as char);
        }
        i += 1;
    }

    if modified {
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn first_kind(sql: &str) -> StmtKind {
        let tree = parse(sql).expect("parse should succeed");
        let node = tree.stmts().next().expect("at least one statement");
        stmt_kind(node)
    }

    #[test]
    fn parse_select_one() {
        let tree = parse("SELECT 1").expect("parse SELECT 1");
        let stmts: Vec<_> = tree.stmts().collect();
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmt_kind(stmts[0]), StmtKind::Select);
    }

    #[test]
    fn parse_syntax_error_maps_to_42601() {
        let err = parse("SELECT FROM WHERE").expect_err("malformed SQL must fail");
        match err {
            BasinError::InvalidSchema(msg) => {
                assert!(
                    msg.contains("42601"),
                    "expected SQLSTATE 42601 in message, got: {msg}"
                );
            }
            other => panic!("expected InvalidSchema, got {other:?}"),
        }
    }

    #[test]
    fn stmt_kind_dml() {
        assert_eq!(first_kind("SELECT * FROM t"), StmtKind::Select);
        assert_eq!(first_kind("INSERT INTO t (a) VALUES (1)"), StmtKind::Insert);
        assert_eq!(first_kind("UPDATE t SET a = 1"), StmtKind::Update);
        assert_eq!(first_kind("DELETE FROM t"), StmtKind::Delete);
    }

    #[test]
    fn stmt_kind_table_ddl() {
        assert_eq!(first_kind("CREATE TABLE t (a int)"), StmtKind::CreateTable);
        assert_eq!(
            first_kind("ALTER TABLE t ADD COLUMN b int"),
            StmtKind::AlterTable
        );
        assert_eq!(first_kind("DROP TABLE t"), StmtKind::DropTable);
    }

    #[test]
    fn stmt_kind_type_and_domain() {
        assert_eq!(
            first_kind("CREATE DOMAIN d AS INT CHECK (VALUE > 0)"),
            StmtKind::CreateDomain
        );
        assert_eq!(
            first_kind("ALTER TYPE colour ADD VALUE 'red'"),
            StmtKind::AlterType
        );
    }

    #[test]
    fn stmt_kind_matview_refresh() {
        assert_eq!(
            first_kind("REFRESH MATERIALIZED VIEW mv"),
            StmtKind::RefreshMatView
        );
    }

    #[test]
    fn stmt_kind_async_and_maintenance() {
        assert_eq!(first_kind("LISTEN ch"), StmtKind::Listen);
        assert_eq!(first_kind("NOTIFY ch"), StmtKind::Notify);
        assert_eq!(first_kind("VACUUM"), StmtKind::Vacuum);
    }

    #[test]
    fn stmt_kind_trigger_and_tx() {
        assert_eq!(
            first_kind("CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()"),
            StmtKind::CreateTrigger
        );
        assert_eq!(first_kind("BEGIN"), StmtKind::BeginTransaction);
    }

    #[test]
    fn reject_unsupported_blocks_each_kind() {
        // Every kind in `is_unsupported()` must produce a
        // FeatureNotSupported with SQLSTATE 0A000. Per ADR 0012 only
        // pub/sub statements are unconditionally rejected; everything else
        // is either noop-accepted or has a real implementation.
        let cases = [
            ("LISTEN ch", "LISTEN"),
            ("NOTIFY ch", "NOTIFY"),
            ("UNLISTEN ch", "UNLISTEN"),
        ];

        for (sql, label) in cases {
            let tree = parse(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
            let err = reject_unsupported(&tree).expect_err(&format!("{sql:?} should be rejected"));
            match err {
                BasinError::FeatureNotSupported(msg) => {
                    assert!(
                        msg.contains("0A000"),
                        "{sql:?}: expected SQLSTATE 0A000 in message, got: {msg}"
                    );
                    assert!(
                        msg.contains(label),
                        "{sql:?}: expected label {label:?} in message, got: {msg}"
                    );
                }
                other => panic!("{sql:?}: expected FeatureNotSupported, got {other:?}"),
            }
        }
    }

    #[test]
    fn reject_unsupported_blocks_triggers_bug_132() {
        // BUG #132 — CREATE TRIGGER / CREATE CONSTRAINT TRIGGER and a bare
        // DROP TRIGGER (no IF EXISTS) must be honestly rejected with SQLSTATE
        // 0A000 instead of silently swallowed. `DROP TRIGGER ... IF EXISTS`
        // is a faithful PG no-op and is asserted elsewhere to pass.
        let create_cases = [
            (
                "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
                "CREATE TRIGGER",
            ),
            (
                "CREATE CONSTRAINT TRIGGER ct AFTER INSERT ON t \
                 FOR EACH ROW EXECUTE FUNCTION f()",
                "CREATE CONSTRAINT TRIGGER",
            ),
        ];
        for (sql, label) in create_cases {
            let tree = parse(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
            let err = reject_unsupported(&tree).expect_err(&format!("{sql:?} should be rejected"));
            match err {
                BasinError::FeatureNotSupported(msg) => {
                    assert!(
                        msg.contains("0A000"),
                        "{sql:?}: expected SQLSTATE 0A000, got: {msg}"
                    );
                    assert!(
                        msg.contains(label) && msg.contains("not supported"),
                        "{sql:?}: expected {label:?}/not supported, got: {msg}"
                    );
                }
                other => panic!("{sql:?}: expected FeatureNotSupported, got {other:?}"),
            }
        }

        // Bare DROP TRIGGER — PG raises "trigger \"x\" does not exist"; Basin
        // mirrors that (no trigger can exist) with SQLSTATE 0A000.
        let tree = parse("DROP TRIGGER tr ON t").unwrap();
        let err = reject_unsupported(&tree).expect_err("bare DROP TRIGGER should be rejected");
        match err {
            BasinError::FeatureNotSupported(msg) => {
                assert!(msg.contains("0A000"), "expected 0A000, got: {msg}");
                assert!(
                    msg.contains("does not exist") && msg.contains("tr"),
                    "expected 'does not exist'/name, got: {msg}"
                );
            }
            other => panic!("expected FeatureNotSupported, got {other:?}"),
        }

        // DROP TRIGGER ... IF EXISTS must pass reject_unsupported (faithful
        // PG no-op; silently noop-accepted upstream).
        let tree = parse("DROP TRIGGER IF EXISTS tr ON t").unwrap();
        reject_unsupported(&tree).expect("DROP TRIGGER IF EXISTS should be allowed");
    }

    #[test]
    fn reject_unsupported_allows_supported_kinds() {
        // Every supported / noop-accepted / Other kind must pass through
        // `reject_unsupported` cleanly. Noop-accepted kinds are intercepted
        // before the sqlparser stage in executor.rs; here we only test that
        // reject_unsupported does not block them.
        // Every supported / Other kind must pass through cleanly.
        // LOCK, VACUUM, ANALYZE, CLUSTER are now in the noop_accept set and
        // also pass reject_unsupported (they are not in is_unsupported()).
        let cases = [
            "SELECT 1",
            "INSERT INTO t (a) VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "CREATE TABLE t (a int)",
            "ALTER TABLE t ADD COLUMN b int",
            "DROP TABLE t",
            "CREATE DOMAIN d AS INT",
            "REFRESH MATERIALIZED VIEW mv",
            "ALTER TYPE colour ADD VALUE 'red'",
            "LOCK TABLE t",
            "VACUUM",
            "ANALYZE t",
            "CLUSTER t",
            "GRANT SELECT ON t TO u",
            "REVOKE SELECT ON t FROM u",
            "SET search_path = public",
            "SHOW search_path",
            "COMMENT ON TABLE t IS 'desc'",
            "CREATE ROLE r",
            "DROP ROLE r",
            "ALTER ROLE r WITH LOGIN",
            // noop-accepted since this commit
            "PREPARE p AS SELECT 1",
            "EXECUTE p",
            "DEALLOCATE p",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT s",
            // BUG #132: `DROP TRIGGER ... IF EXISTS` is a faithful PG no-op
            // (nothing to drop) — `missing_ok = true`, so reject_unsupported
            // must let it through (it is silently noop-accepted upstream).
            // The bare `DROP TRIGGER` and `CREATE TRIGGER` forms are now
            // honestly rejected; see `reject_unsupported_blocks_each_kind`.
            "DROP TRIGGER IF EXISTS tr ON t",
            "CREATE EXTENSION pgcrypto",
            "DROP EXTENSION pgcrypto",
        ];
        for sql in cases {
            let tree = parse(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
            reject_unsupported(&tree)
                .unwrap_or_else(|e| panic!("{sql:?} should be allowed, got {e}"));
        }
    }

    // -----------------------------------------------------------------
    // rewrite_tsvector_at_at — text-level operator lowering
    // -----------------------------------------------------------------

    #[test]
    fn rewrite_at_at_no_op_when_absent() {
        let sql = "SELECT 1";
        assert_eq!(rewrite_tsvector_at_at(sql), sql);
    }

    #[test]
    fn rewrite_at_at_basic_column_vs_call() {
        let got = rewrite_tsvector_at_at("SELECT * FROM doc WHERE ts @@ to_tsquery('fox')");
        assert_eq!(
            got,
            "SELECT * FROM doc WHERE tsvector_match_udf(ts, to_tsquery('fox'))"
        );
    }

    #[test]
    fn rewrite_at_at_paren_operands() {
        // Both sides parenthesised.
        let got = rewrite_tsvector_at_at("SELECT (to_tsvector('a b')) @@ (to_tsquery('a')) FROM t");
        assert_eq!(
            got,
            "SELECT tsvector_match_udf((to_tsvector('a b')), (to_tsquery('a'))) FROM t"
        );
    }

    #[test]
    fn rewrite_at_at_function_call_lhs_and_rhs() {
        let got =
            rewrite_tsvector_at_at("SELECT to_tsvector('a b') @@ to_tsquery('english', 'a') AS m");
        assert_eq!(
            got,
            "SELECT tsvector_match_udf(to_tsvector('a b'), to_tsquery('english', 'a')) AS m"
        );
    }

    #[test]
    fn rewrite_at_at_cast_lhs() {
        // 'a quick fox'::tsvector @@ q
        // The `::tsvector` cast is stripped (Basin stores tsvector as Utf8)
        // before the @@ rewrite captures the operand.
        let got = rewrite_tsvector_at_at("SELECT 'a quick fox'::tsvector @@ to_tsquery('fox')");
        assert_eq!(
            got,
            "SELECT tsvector_match_udf('a quick fox', to_tsquery('fox'))"
        );
    }

    #[test]
    fn rewrite_at_at_cast_rhs() {
        // `'fox'::tsquery` → `'fox'` after the cast strip.
        let got = rewrite_tsvector_at_at("SELECT ts @@ 'fox'::tsquery FROM doc");
        assert_eq!(got, "SELECT tsvector_match_udf(ts, 'fox') FROM doc");
    }

    #[test]
    fn strip_fts_casts_preserves_other_casts() {
        // Non-FTS casts (e.g. ::int, ::float) must NOT be stripped.
        let sql = "SELECT '42'::int AS n";
        assert_eq!(rewrite_tsvector_at_at(sql), sql);
    }

    #[test]
    fn strip_fts_casts_in_string_literal_left_alone() {
        // `::tsvector` inside a string literal is data.
        let sql = "SELECT 'foo::tsvector' AS lit";
        assert_eq!(rewrite_tsvector_at_at(sql), sql);
    }

    #[test]
    fn rewrite_at_at_multiple_in_one_statement() {
        let got = rewrite_tsvector_at_at("SELECT * FROM t WHERE a @@ q1 OR b @@ q2");
        assert_eq!(
            got,
            "SELECT * FROM t WHERE tsvector_match_udf(a, q1) OR tsvector_match_udf(b, q2)"
        );
    }

    #[test]
    fn rewrite_at_at_triple_at_passthrough() {
        // `@@@` is not the FTS match operator; it must not be rewritten.
        // It is allowed to fail at the parser, but the rewriter itself
        // must leave the source text alone.
        let sql = "SELECT a @@@ b FROM t";
        assert_eq!(rewrite_tsvector_at_at(sql), sql);
    }

    #[test]
    fn rewrite_at_at_inside_string_literal_left_alone() {
        // `@@` inside a single-quoted literal is data, not an operator.
        let sql = "SELECT 'a @@ b' AS lit";
        assert_eq!(rewrite_tsvector_at_at(sql), sql);
    }

    #[test]
    fn rewrite_at_at_inside_quoted_identifier_left_alone() {
        let sql = "SELECT \"a@@b\" FROM t";
        assert_eq!(rewrite_tsvector_at_at(sql), sql);
    }

    #[test]
    fn rewrite_at_at_inside_line_comment_left_alone() {
        let sql = "SELECT 1 -- a @@ b\nFROM t";
        assert_eq!(rewrite_tsvector_at_at(sql), sql);
    }

    #[test]
    fn rewrite_at_at_inside_block_comment_left_alone() {
        let sql = "SELECT 1 /* a @@ b */ FROM t";
        assert_eq!(rewrite_tsvector_at_at(sql), sql);
    }

    #[test]
    fn rewrite_at_at_qualified_column_lhs() {
        let got = rewrite_tsvector_at_at("SELECT * FROM doc d WHERE d.ts @@ to_tsquery('fox')");
        assert_eq!(
            got,
            "SELECT * FROM doc d WHERE tsvector_match_udf(d.ts, to_tsquery('fox'))"
        );
    }

    #[test]
    fn rewrite_at_at_does_not_rewrite_other_at_operators() {
        // `<@`, `@>`, `?@`, `@?` etc. should all pass through.
        let cases = [
            "SELECT a <@ b FROM t",
            "SELECT a @> b FROM t",
            "SELECT a @? b FROM t",
            "SELECT a ?@ b FROM t",
            "SELECT @x FROM t",
        ];
        for sql in cases {
            assert_eq!(rewrite_tsvector_at_at(sql), sql, "case: {sql}");
        }
    }

    // -----------------------------------------------------------------
    // strip_only_modifier — FROM ONLY / JOIN ONLY table-inheritance strip
    // -----------------------------------------------------------------

    #[test]
    fn strip_only_no_op_when_absent() {
        let sql = "SELECT * FROM t";
        assert_eq!(strip_only_modifier(sql), sql);
    }

    #[test]
    fn strip_only_from_only() {
        let got = strip_only_modifier("SELECT * FROM ONLY t");
        assert_eq!(got, "SELECT * FROM t");
    }

    #[test]
    fn strip_only_join_only() {
        let got = strip_only_modifier("SELECT * FROM a JOIN ONLY b ON a.id = b.id");
        assert_eq!(got, "SELECT * FROM a JOIN b ON a.id = b.id");
    }

    #[test]
    fn strip_only_case_insensitive() {
        let got = strip_only_modifier("select * from only t");
        assert_eq!(got, "select * from t");
    }

    #[test]
    fn strip_only_does_not_touch_string_literals() {
        // 'FROM ONLY foo' inside a string literal must not be rewritten.
        let sql = "SELECT 'FROM ONLY foo' FROM t";
        assert_eq!(strip_only_modifier(sql), sql);
    }

    #[test]
    fn strip_only_does_not_mangle_only_as_identifier() {
        // A column named `only` should not be affected.
        let sql = "SELECT only FROM t";
        assert_eq!(strip_only_modifier(sql), sql);
    }

    #[test]
    fn strip_only_detects_do_block_as_design_exclusion() {
        let tree = parse("DO $$ BEGIN RAISE NOTICE 'hi'; END; $$ LANGUAGE plpgsql")
            .expect("pg_query parses DO");
        let node = tree.stmts().next().expect("one stmt");
        assert_eq!(stmt_kind(node), StmtKind::DoBlock);
        let err = reject_unsupported(&tree).expect_err("DoBlock should be rejected");
        match err {
            BasinError::FeatureNotSupported(msg) => {
                assert!(msg.contains("0A000"), "expected SQLSTATE 0A000 in: {msg}");
            }
            other => panic!("expected FeatureNotSupported, got {other:?}"),
        }
    }

    #[test]
    fn strip_only_detects_partition_of_as_design_exclusion() {
        let tree = parse("CREATE TABLE t_2024 PARTITION OF t FOR VALUES FROM (2024) TO (2025)")
            .expect("pg_query parses PARTITION OF");
        let node = tree.stmts().next().expect("one stmt");
        assert_eq!(stmt_kind(node), StmtKind::CreateTablePartitionOf);
        let err = reject_unsupported(&tree).expect_err("CreateTablePartitionOf should be rejected");
        match err {
            BasinError::FeatureNotSupported(msg) => {
                assert!(msg.contains("0A000"), "expected SQLSTATE 0A000 in: {msg}");
            }
            other => panic!("expected FeatureNotSupported, got {other:?}"),
        }
    }

    #[test]
    fn strip_only_detects_exclude_constraint_as_design_exclusion() {
        let tree = parse("CREATE TABLE t (id INT, EXCLUDE USING gist (id WITH =))")
            .expect("pg_query parses EXCLUDE");
        let err = reject_unsupported(&tree).expect_err("EXCLUDE should be rejected");
        match err {
            BasinError::FeatureNotSupported(msg) => {
                assert!(msg.contains("0A000"), "expected SQLSTATE 0A000 in: {msg}");
            }
            other => panic!("expected FeatureNotSupported, got {other:?}"),
        }
    }

    #[test]
    fn strip_only_detects_detach_partition_as_design_exclusion() {
        let tree =
            parse("ALTER TABLE t DETACH PARTITION p").expect("pg_query parses DETACH PARTITION");
        let err = reject_unsupported(&tree).expect_err("DETACH PARTITION should be rejected");
        match err {
            BasinError::FeatureNotSupported(msg) => {
                assert!(msg.contains("0A000"), "expected SQLSTATE 0A000 in: {msg}");
            }
            other => panic!("expected FeatureNotSupported, got {other:?}"),
        }
    }
}

/// sqlparser 0.61 changed `ObjectName` to `ObjectName(Vec<ObjectNamePart>)`
/// where `ObjectNamePart` is an enum (`Identifier(Ident)` | `Function(..)`).
/// Pre-0.61 callers accessed the `.value` String directly on the part.
/// This extension restores that access shape with a single `&String` getter
/// so existing call sites keep working (`.value` -> `.id_val()`).
pub trait ObjectNamePartExt {
    /// The identifier string for this name part. For function-style parts
    /// (rare, dialect-specific) returns a reference to an empty string,
    /// matching the previous "no value" behavior conservatively.
    fn id_val(&self) -> &String;
}

static EMPTY_IDENT: String = String::new();

impl ObjectNamePartExt for sqlparser::ast::ObjectNamePart {
    fn id_val(&self) -> &String {
        match self {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => &ident.value,
            _ => &EMPTY_IDENT,
        }
    }
}

/// sqlparser 0.61 collapsed `Query.limit` / `Query.offset` / `Query.limit_by`
/// into a single optional `Query.limit_clause: Option<LimitClause>`. These
/// helpers restore the old per-field accessors so pre-0.61 call sites keep
/// their existing logic with a minimal `q.limit` -> `query_limit(q)` swap.
pub trait QueryClauseExt {
    fn ext_limit(&self) -> Option<&sqlparser::ast::Expr>;
    fn ext_limit_mut(&mut self) -> Option<&mut sqlparser::ast::Expr>;
    fn ext_offset(&self) -> Option<&sqlparser::ast::Offset>;
    fn ext_limit_by(&self) -> &[sqlparser::ast::Expr];
}

impl QueryClauseExt for sqlparser::ast::Query {
    fn ext_limit(&self) -> Option<&sqlparser::ast::Expr> {
        match self.limit_clause.as_ref()? {
            sqlparser::ast::LimitClause::LimitOffset { limit, .. } => limit.as_ref(),
            sqlparser::ast::LimitClause::OffsetCommaLimit { limit, .. } => Some(limit),
        }
    }
    fn ext_limit_mut(&mut self) -> Option<&mut sqlparser::ast::Expr> {
        match self.limit_clause.as_mut()? {
            sqlparser::ast::LimitClause::LimitOffset { limit, .. } => limit.as_mut(),
            sqlparser::ast::LimitClause::OffsetCommaLimit { limit, .. } => Some(limit),
        }
    }
    fn ext_offset(&self) -> Option<&sqlparser::ast::Offset> {
        match self.limit_clause.as_ref()? {
            sqlparser::ast::LimitClause::LimitOffset { offset, .. } => offset.as_ref(),
            sqlparser::ast::LimitClause::OffsetCommaLimit { .. } => None,
        }
    }
    fn ext_limit_by(&self) -> &[sqlparser::ast::Expr] {
        match self.limit_clause.as_ref() {
            Some(sqlparser::ast::LimitClause::LimitOffset { limit_by, .. }) => limit_by,
            _ => &[],
        }
    }
}

/// sqlparser 0.61 replaced `OrderBy.exprs: Vec<OrderByExpr>` with
/// `OrderBy.kind: OrderByKind`. These helpers expose the expression list
/// for the common `ORDER BY <expr>, ...` form (the `ALL` form yields an
/// empty slice / no-op iteration, matching prior behavior closely).
pub trait OrderByExt {
    fn ext_exprs(&self) -> &[sqlparser::ast::OrderByExpr];
    fn ext_exprs_mut(&mut self) -> &mut [sqlparser::ast::OrderByExpr];
}

impl OrderByExt for sqlparser::ast::OrderBy {
    fn ext_exprs(&self) -> &[sqlparser::ast::OrderByExpr] {
        match &self.kind {
            sqlparser::ast::OrderByKind::Expressions(v) => v,
            sqlparser::ast::OrderByKind::All(_) => &[],
        }
    }
    fn ext_exprs_mut(&mut self) -> &mut [sqlparser::ast::OrderByExpr] {
        match &mut self.kind {
            sqlparser::ast::OrderByKind::Expressions(v) => v,
            sqlparser::ast::OrderByKind::All(_) => &mut [],
        }
    }
}

/// sqlparser 0.61 replaced `Insert.table_name: ObjectName` with
/// `Insert.table: TableObject` (an enum of `TableName(ObjectName)` |
/// `TableFunction(Function)`). This helper restores the old accessor for
/// the only form Basin supports (a plain table name), erroring cleanly on
/// the table-function form rather than silently mis-handling it.
pub fn insert_object_name(ins: &sqlparser::ast::Insert) -> Result<&sqlparser::ast::ObjectName> {
    match &ins.table {
        sqlparser::ast::TableObject::TableName(name) => Ok(name),
        sqlparser::ast::TableObject::TableFunction(_) => Err(BasinError::FeatureNotSupported(
            "INSERT INTO TABLE FUNCTION(...) is not supported".into(),
        )),
    }
}

/// sqlparser 0.61 changed PK/UNIQUE constraint column lists from
/// `Vec<Ident>` to `Vec<IndexColumn>`, where each `IndexColumn` wraps an
/// `OrderByExpr` whose `.expr` is normally an `Expr::Identifier`. This
/// extracts the bare column name for the common (only supported) case;
/// non-identifier index expressions stringify (best-effort, matches the
/// prior "whatever the ident text was" behavior for simple columns).
pub fn index_column_name(col: &sqlparser::ast::IndexColumn) -> String {
    match &col.column.expr {
        sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
        other => other.to_string(),
    }
}
