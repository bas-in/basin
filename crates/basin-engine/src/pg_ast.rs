//! Phase 1 of ADR 0014 (pg_query as canonical parser).
//!
//! This module hosts the libpg_query-based parser wrapper and statement-kind
//! classifier. Phase 2 will route SELECT/DML through a PgNode → DataFusion
//! LogicalPlan translator that lives in a sibling module. Phase 3 fills in
//! the long-tail SQL features (window funcs, GROUPING SETS, recursive CTEs,
//! MERGE, LATERAL).
//!
//! Today this module is wired into [`crate::executor::execute`] behind the
//! `BASIN_PG_QUERY` env-gate: when set, every incoming statement is parsed
//! with libpg_query and any kind in the "known-but-unsupported" set is
//! rejected with SQLSTATE 0A000 *before* sqlparser sees it. Other kinds
//! fall through to the existing sqlparser-based pipeline. The gate is
//! intentional — agents 2–4 migrate the textual pre-screens to AST
//! matchers against the same parse tree before agent 5 flips this
//! unconditional and deletes the textual pre-screens.

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
    DropFunction,
    CreateProcedure,
    Call,
    DropProcedure,
    // Sequence DDL
    CreateSequence,
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
    // Async notification (unsupported in v0.1)
    Listen,
    Notify,
    // Prepared / cursor (unsupported in v0.1)
    Prepare,
    Execute,
    Deallocate,
    DeclareCursor,
    Fetch,
    Close,
    // Extension / trigger (unsupported in v0.1)
    CreateExtension,
    CreateTrigger,
    DropTrigger,
    // Transaction control (unsupported in v0.1 — Basin is auto-commit)
    BeginTransaction,
    Commit,
    Rollback,
    Savepoint,
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
            StmtKind::DropFunction => "DROP FUNCTION",
            StmtKind::CreateProcedure => "CREATE PROCEDURE",
            StmtKind::Call => "CALL",
            StmtKind::DropProcedure => "DROP PROCEDURE",
            StmtKind::CreateSequence => "CREATE SEQUENCE",
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
            StmtKind::Prepare => "PREPARE",
            StmtKind::Execute => "EXECUTE",
            StmtKind::Deallocate => "DEALLOCATE",
            StmtKind::DeclareCursor => "DECLARE CURSOR",
            StmtKind::Fetch => "FETCH",
            StmtKind::Close => "CLOSE",
            StmtKind::CreateExtension => "CREATE EXTENSION",
            StmtKind::CreateTrigger => "CREATE TRIGGER",
            StmtKind::DropTrigger => "DROP TRIGGER",
            StmtKind::BeginTransaction => "BEGIN",
            StmtKind::Commit => "COMMIT",
            StmtKind::Rollback => "ROLLBACK",
            StmtKind::Savepoint => "SAVEPOINT",
            StmtKind::Other => "<other>",
        }
    }

    /// `true` if this kind is in the "known-but-not-yet-shipped" set
    /// rejected by [`reject_unsupported`]. Centralised so the executor
    /// and the test suite stay in sync.
    pub fn is_unsupported(&self) -> bool {
        matches!(
            self,
            StmtKind::Listen
                | StmtKind::Notify
                | StmtKind::Prepare
                | StmtKind::Execute
                | StmtKind::Deallocate
                | StmtKind::DeclareCursor
                | StmtKind::Fetch
                | StmtKind::Close
                | StmtKind::CreateExtension
                | StmtKind::CreateTrigger
                | StmtKind::DropTrigger
                | StmtKind::BeginTransaction
                | StmtKind::Commit
                | StmtKind::Rollback
                | StmtKind::Savepoint
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

        NodeEnum::CreateStmt(_) => StmtKind::CreateTable,
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

        NodeEnum::CreateSeqStmt(_) => StmtKind::CreateSequence,

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
        NodeEnum::UnlistenStmt(_) => StmtKind::Listen,

        NodeEnum::PrepareStmt(_) => StmtKind::Prepare,
        NodeEnum::ExecuteStmt(_) => StmtKind::Execute,
        NodeEnum::DeallocateStmt(_) => StmtKind::Deallocate,
        NodeEnum::DeclareCursorStmt(_) => StmtKind::DeclareCursor,
        NodeEnum::FetchStmt(_) => StmtKind::Fetch,
        NodeEnum::ClosePortalStmt(_) => StmtKind::Close,

        NodeEnum::CreateExtensionStmt(_) => StmtKind::CreateExtension,
        NodeEnum::CreateTrigStmt(_) => StmtKind::CreateTrigger,

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
                _ => StmtKind::Other,
            }
        }

        _ => StmtKind::Other,
    }
}

/// Reject every top-level statement whose kind is in the "known but
/// not yet shipped" set with SQLSTATE 0A000. Stops at the first
/// unsupported statement. Returns `Ok(())` if every statement is
/// either supported or [`StmtKind::Other`].
///
/// Routed by the executor's `BASIN_PG_QUERY` env-gate today. Phase 2
/// makes this unconditional and extends the reject set as features
/// land.
pub fn reject_unsupported(tree: &ParseTree) -> Result<()> {
    for node in tree.stmts() {
        let kind = stmt_kind(node);
        if kind.is_unsupported() {
            return Err(BasinError::FeatureNotSupported(format!(
                "{} is not supported (SQLSTATE 0A000)",
                kind.as_label()
            )));
        }
    }
    Ok(())
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
        assert_eq!(
            first_kind("INSERT INTO t (a) VALUES (1)"),
            StmtKind::Insert
        );
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
        // FeatureNotSupported with SQLSTATE 0A000.
        let cases = [
            ("LISTEN ch", "LISTEN"),
            ("NOTIFY ch", "NOTIFY"),
            ("PREPARE p AS SELECT 1", "PREPARE"),
            ("EXECUTE p", "EXECUTE"),
            ("DEALLOCATE p", "DEALLOCATE"),
            ("DECLARE c CURSOR FOR SELECT 1", "DECLARE CURSOR"),
            ("FETCH c", "FETCH"),
            ("CLOSE c", "CLOSE"),
            ("CREATE EXTENSION pgcrypto", "CREATE EXTENSION"),
            (
                "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
                "CREATE TRIGGER",
            ),
            ("DROP TRIGGER tr ON t", "DROP TRIGGER"),
            ("BEGIN", "BEGIN"),
            ("COMMIT", "COMMIT"),
            ("ROLLBACK", "ROLLBACK"),
            ("SAVEPOINT s", "SAVEPOINT"),
        ];

        for (sql, label) in cases {
            let tree = parse(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
            let err =
                reject_unsupported(&tree).expect_err(&format!("{sql:?} should be rejected"));
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
    fn reject_unsupported_allows_supported_kinds() {
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
        ];
        for sql in cases {
            let tree = parse(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
            reject_unsupported(&tree)
                .unwrap_or_else(|e| panic!("{sql:?} should be allowed, got {e}"));
        }
    }
}
