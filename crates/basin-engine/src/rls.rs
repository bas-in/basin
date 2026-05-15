//! Row-level security: DDL parsing + logical-plan predicate injection.
//!
//! The contract we honour from `TASK.md` Phase 5.6:
//!
//! 1. `CREATE POLICY` / `ALTER POLICY` / `DROP POLICY` and `ALTER TABLE …
//!    ENABLE/DISABLE ROW LEVEL SECURITY` are accepted as Postgres syntax via
//!    the existing sqlparser fork. We translate the AST into the catalog's
//!    [`Policy`] / `rls_enabled` fields and persist via `set_rls_state`.
//!
//! 2. The `SELECT` planner consults the catalog *only* when the table has
//!    `rls_enabled = true`. The cost on the no-RLS hot path is one
//!    `AtomicBool` load per session. Tables with RLS off see the engine's
//!    behaviour as it was before this module existed.
//!
//! 3. Predicate injection is at the DataFusion logical-plan level. We do
//!    **not** rewrite the user's SQL textually. The strategy:
//!
//!      a. The user's SQL is parsed and planned by DataFusion as usual,
//!         producing a `LogicalPlan` whose leaves are `TableScan` nodes.
//!      b. For each leaf scan against an RLS-enabled table, we rebuild the
//!         policy's USING expression as a DataFusion `Expr` (by planning a
//!         tiny synthetic `SELECT 1 FROM <t> WHERE <using>` against the
//!         same `SessionContext`) and wrap the scan in a `Filter(expr, …)`
//!         via `LogicalPlanBuilder::filter` — exactly the API the spec
//!         names.
//!      c. INSERT / UPDATE WITH CHECK enforcement reuses the same policy
//!         translation path; v0.1 ships the SELECT/UPDATE/DELETE filter
//!         and leaves WITH CHECK as a TODO (see end of file).
//!
//! 4. `current_user` is resolved from the calling [`crate::ProjectSession`].
//!    With auth disabled the engine stamps `'anonymous'`. Substitution into
//!    the policy expression is by SQL-text replacement of the
//!    `current_user` identifier with a single-quoted string literal — this
//!    is principal-text substitution (not user-SQL rewriting) and runs
//!    only on the policy's own definition.
//!
//! 5. RLS is a *layer above* project prefix isolation. The catalog calls
//!    that materialise table state are project-scoped; a policy registered
//!    on project A's `orders` is invisible to project B's session. The
//!    integration test `viability_rls_isolation` is the back-stop.

use std::collections::HashMap;
use std::sync::Arc;

use basin_catalog::{Policy, PolicyCommand};
use basin_common::{BasinError, Result, TableName};
use datafusion::logical_expr::{Filter, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{DataFrame, SessionContext};
use sqlparser::ast::{
    AlterPolicyOperation, AlterTableOperation, CreatePolicyCommand, ObjectName, Owner, Statement,
};

/// Compile `Statement::CreatePolicy` (and friends) into a catalog mutation.
///
/// Returns:
/// - `Ok(Some((table, mutation)))` — caller applies the mutation to the
///   loaded `TableMetadata`'s `(rls_enabled, policies)` and persists via
///   `Catalog::set_rls_state`.
/// - `Ok(None)` — statement isn't an RLS-related DDL; caller falls through.
/// - `Err(...)` — RLS DDL with shape we don't (yet) support; surface as a
///   user error.
pub(crate) fn match_rls_ddl(stmt: &Statement) -> Result<Option<RlsDdl>> {
    match stmt {
        Statement::CreatePolicy {
            name,
            table_name,
            policy_type: _, // PERMISSIVE only in v0.1; RESTRICTIVE deferred.
            command,
            to,
            using,
            with_check,
        } => {
            let table = single_part_object_name(table_name)?;
            let cmd = command
                .as_ref()
                .map(map_create_policy_command)
                .unwrap_or(PolicyCommand::All);
            let roles = to
                .as_ref()
                .map(|owners| owners.iter().map(owner_to_role_string).collect::<Vec<_>>())
                .unwrap_or_default();
            // Filter out the literal "PUBLIC" role; an explicit `TO PUBLIC`
            // is identical to "no role list" for our matcher.
            let roles: Vec<String> = roles
                .into_iter()
                .filter(|r| !r.eq_ignore_ascii_case("public"))
                .collect();
            let using_expr = using.as_ref().map(|e| e.to_string()).ok_or_else(|| {
                BasinError::InvalidSchema(
                    "CREATE POLICY without USING is not supported in v0.1".into(),
                )
            })?;
            let with_check_expr = with_check.as_ref().map(|e| e.to_string());
            Ok(Some(RlsDdl::CreatePolicy {
                table,
                policy: Policy {
                    name: name.value.clone(),
                    applies_to_roles: roles,
                    command: cmd,
                    using_expr,
                    with_check_expr,
                },
            }))
        }
        Statement::AlterPolicy {
            name,
            table_name,
            operation,
        } => {
            let table = single_part_object_name(table_name)?;
            let op = match operation {
                AlterPolicyOperation::Rename { new_name } => {
                    AlterPolicyOp::Rename(new_name.value.clone())
                }
                AlterPolicyOperation::Apply {
                    to,
                    using,
                    with_check,
                } => {
                    let roles = to.as_ref().map(|owners| {
                        owners
                            .iter()
                            .map(owner_to_role_string)
                            .filter(|r| !r.eq_ignore_ascii_case("public"))
                            .collect::<Vec<_>>()
                    });
                    AlterPolicyOp::Apply {
                        roles,
                        using_expr: using.as_ref().map(|e| e.to_string()),
                        with_check_expr: with_check.as_ref().map(|e| e.to_string()),
                    }
                }
            };
            Ok(Some(RlsDdl::AlterPolicy {
                table,
                name: name.value.clone(),
                op,
            }))
        }
        Statement::DropPolicy {
            if_exists,
            name,
            table_name,
            option: _,
        } => {
            let table = single_part_object_name(table_name)?;
            Ok(Some(RlsDdl::DropPolicy {
                table,
                name: name.value.clone(),
                if_exists: *if_exists,
            }))
        }
        Statement::AlterTable {
            name, operations, ..
        } => {
            // We only intercept `ENABLE/DISABLE ROW LEVEL SECURITY`; every
            // other ALTER TABLE op is out of scope for this module and the
            // executor surfaces a clean error from its own dispatch path.
            for op in operations {
                match op {
                    AlterTableOperation::EnableRowLevelSecurity => {
                        let table = single_part_object_name(name)?;
                        return Ok(Some(RlsDdl::SetRlsEnabled {
                            table,
                            enabled: true,
                        }));
                    }
                    AlterTableOperation::DisableRowLevelSecurity => {
                        let table = single_part_object_name(name)?;
                        return Ok(Some(RlsDdl::SetRlsEnabled {
                            table,
                            enabled: false,
                        }));
                    }
                    _ => {}
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Mutation extracted from an RLS DDL statement. The executor applies these
/// to a freshly loaded `TableMetadata` and persists with
/// [`basin_catalog::Catalog::set_rls_state`].
pub(crate) enum RlsDdl {
    SetRlsEnabled {
        table: TableName,
        enabled: bool,
    },
    CreatePolicy {
        table: TableName,
        policy: Policy,
    },
    AlterPolicy {
        table: TableName,
        name: String,
        op: AlterPolicyOp,
    },
    DropPolicy {
        table: TableName,
        name: String,
        if_exists: bool,
    },
}

pub(crate) enum AlterPolicyOp {
    Rename(String),
    Apply {
        roles: Option<Vec<String>>,
        using_expr: Option<String>,
        with_check_expr: Option<String>,
    },
}

impl RlsDdl {
    pub(crate) fn table(&self) -> &TableName {
        match self {
            RlsDdl::SetRlsEnabled { table, .. }
            | RlsDdl::CreatePolicy { table, .. }
            | RlsDdl::AlterPolicy { table, .. }
            | RlsDdl::DropPolicy { table, .. } => table,
        }
    }

    /// Apply this mutation to `(rls_enabled, policies)` in place.
    pub(crate) fn apply(
        self,
        rls_enabled: &mut bool,
        policies: &mut Vec<Policy>,
    ) -> Result<&'static str> {
        match self {
            RlsDdl::SetRlsEnabled { enabled, .. } => {
                *rls_enabled = enabled;
                Ok(if enabled {
                    "ALTER TABLE"
                } else {
                    "ALTER TABLE"
                })
            }
            RlsDdl::CreatePolicy { policy, .. } => {
                if policies.iter().any(|p| p.name == policy.name) {
                    return Err(BasinError::InvalidSchema(format!(
                        "policy {:?} already exists on this table",
                        policy.name
                    )));
                }
                policies.push(policy);
                Ok("CREATE POLICY")
            }
            RlsDdl::AlterPolicy { name, op, .. } => {
                let p = policies
                    .iter_mut()
                    .find(|p| p.name == name)
                    .ok_or_else(|| BasinError::not_found(format!("policy {name:?}")))?;
                match op {
                    AlterPolicyOp::Rename(new) => p.name = new,
                    AlterPolicyOp::Apply {
                        roles,
                        using_expr,
                        with_check_expr,
                    } => {
                        if let Some(r) = roles {
                            p.applies_to_roles = r;
                        }
                        if let Some(u) = using_expr {
                            p.using_expr = u;
                        }
                        if let Some(w) = with_check_expr {
                            p.with_check_expr = Some(w);
                        }
                    }
                }
                Ok("ALTER POLICY")
            }
            RlsDdl::DropPolicy {
                name, if_exists, ..
            } => {
                let pos = policies.iter().position(|p| p.name == name);
                match (pos, if_exists) {
                    (Some(i), _) => {
                        policies.remove(i);
                        Ok("DROP POLICY")
                    }
                    (None, true) => Ok("DROP POLICY"),
                    (None, false) => Err(BasinError::not_found(format!("policy {name:?}"))),
                }
            }
        }
    }
}

fn map_create_policy_command(c: &CreatePolicyCommand) -> PolicyCommand {
    match c {
        CreatePolicyCommand::All => PolicyCommand::All,
        CreatePolicyCommand::Select => PolicyCommand::Select,
        CreatePolicyCommand::Insert => PolicyCommand::Insert,
        CreatePolicyCommand::Update => PolicyCommand::Update,
        CreatePolicyCommand::Delete => PolicyCommand::Delete,
    }
}

fn owner_to_role_string(o: &Owner) -> String {
    match o {
        Owner::Ident(i) => i.value.clone(),
        Owner::CurrentRole | Owner::CurrentUser | Owner::SessionUser => {
            // These three only appear in OWNER TO; for `CREATE POLICY ... TO`
            // sqlparser parses bare identifiers as `Owner::Ident` so we won't
            // encounter the variants in practice. Fall back to a sentinel
            // string that won't match any real role.
            "__current__".into()
        }
    }
}

fn single_part_object_name(name: &ObjectName) -> Result<TableName> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified policy table not supported in PoC: {name}"
        )));
    }
    TableName::new(name.0[0].value.clone())
}

// --- Predicate selection ----------------------------------------------------

/// Pick the policies that apply to `current_user` for the given DML kind.
/// Empty `applies_to_roles` means PUBLIC (everyone). For `command`, `All`
/// matches every kind.
pub(crate) fn select_applicable<'a>(
    policies: &'a [Policy],
    current_user: &str,
    kind: PolicyCommand,
) -> Vec<&'a Policy> {
    policies
        .iter()
        .filter(|p| match p.command {
            PolicyCommand::All => true,
            other => other == kind,
        })
        .filter(|p| {
            p.applies_to_roles.is_empty() || p.applies_to_roles.iter().any(|r| r == current_user)
        })
        .collect()
}

// --- Logical-plan injection -------------------------------------------------

/// Inject row-level filters into `df`'s logical plan for every TableScan
/// against an RLS-enabled table.
///
/// `policies_by_table` maps the *unqualified* table name (sqlparser's bare
/// identifier — what registers in DataFusion's catalog) to the list of
/// policies that apply to the current principal in SELECT context.
///
/// We use [`datafusion::common::tree_node::TreeNode::transform_up`] to walk
/// the plan bottom-up and wrap each matching `TableScan` in a `Filter`.
/// Because the rewrite is plan-local (no SQL string is touched), DataFusion's
/// own optimizer downstream still pushes the predicate down through projects,
/// joins, and limits.
pub(crate) async fn inject_select_predicates(
    ctx: &SessionContext,
    df: DataFrame,
    policies_by_table: &HashMap<String, Vec<Policy>>,
    current_user: &str,
) -> Result<DataFrame> {
    use datafusion::common::tree_node::{Transformed, TreeNode};

    if policies_by_table.is_empty() {
        return Ok(df);
    }

    // Pre-build the (table -> Vec<Expr>) map. Building the Expr inside
    // `transform_up` would force us to async/await mid-traversal, which
    // TreeNode doesn't support directly.
    let mut filter_exprs: HashMap<String, Vec<datafusion::logical_expr::Expr>> =
        HashMap::with_capacity(policies_by_table.len());
    for (table, policies) in policies_by_table {
        let mut exprs = Vec::with_capacity(policies.len());
        for p in policies {
            let expr = build_predicate_expr(ctx, table, &p.using_expr, current_user).await?;
            exprs.push(expr);
        }
        if !exprs.is_empty() {
            filter_exprs.insert(table.clone(), exprs);
        }
    }
    if filter_exprs.is_empty() {
        return Ok(df);
    }

    let plan = df.logical_plan().clone();
    let transformed = plan
        .transform_up(|node: LogicalPlan| {
            if let LogicalPlan::TableScan(ref ts) = node {
                let bare = ts.table_name.table().to_string();
                if let Some(exprs) = filter_exprs.get(&bare) {
                    // OR-merge multiple policies (Postgres permissive
                    // semantics). v0.1 ships PERMISSIVE only.
                    let combined = combine_or(exprs.clone());
                    let new_plan = LogicalPlanBuilder::from(node)
                        .filter(combined)
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "rls filter wrap: {e}"
                            ))
                        })?
                        .build()
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "rls filter build: {e}"
                            ))
                        })?;
                    return Ok(Transformed::yes(new_plan));
                }
            }
            Ok(Transformed::no(node))
        })
        .map_err(|e| BasinError::internal(format!("rls plan rewrite: {e}")))?;

    // Sanity: at least one TableScan was wrapped; otherwise the user is
    // querying a table that's not in our catalog and DataFusion would have
    // already errored — let it through unchanged.
    let new_plan = transformed.data;
    let session_state = ctx.state();
    let new_df = DataFrame::new(session_state, new_plan);
    Ok(new_df)
}

/// Build a single DataFusion `Expr` for one policy's USING clause against
/// `table`'s schema. We use a synthetic SQL probe (`SELECT 1 FROM <t> WHERE
/// <using>`) and pull the predicate out of the resulting `Filter` plan. The
/// probe is internal — it never sees the user's SQL — so this is not the
/// "string-rewriting" the spec forbids.
async fn build_predicate_expr(
    ctx: &SessionContext,
    table: &str,
    using_expr: &str,
    current_user: &str,
) -> Result<datafusion::logical_expr::Expr> {
    let prepared = substitute_current_user(using_expr, current_user);
    // Wrap the table identifier in double quotes so a name that happens to
    // be a SQL keyword still parses. The `current_user` substitution above
    // has already escaped the literal.
    let probe_sql = format!("SELECT 1 FROM \"{table}\" WHERE {prepared}");
    let df = ctx.sql(&probe_sql).await.map_err(|e| {
        BasinError::InvalidSchema(format!(
            "rls policy USING failed to plan ({probe_sql:?}): {e}"
        ))
    })?;
    // The plan is `Projection(SELECT 1) -> Filter(<using>) -> TableScan(t)`.
    // Walk it once and pull the first Filter's predicate.
    let plan = df.logical_plan().clone();
    extract_first_filter(&plan).ok_or_else(|| {
        BasinError::internal(format!(
            "rls: probe plan has no Filter (table={table}, using={using_expr})"
        ))
    })
}

fn extract_first_filter(plan: &LogicalPlan) -> Option<datafusion::logical_expr::Expr> {
    match plan {
        LogicalPlan::Filter(Filter { predicate, .. }) => Some(predicate.clone()),
        other => {
            for child in other.inputs() {
                if let Some(p) = extract_first_filter(child) {
                    return Some(p);
                }
            }
            None
        }
    }
}

fn combine_or(mut exprs: Vec<datafusion::logical_expr::Expr>) -> datafusion::logical_expr::Expr {
    let mut acc = exprs.pop().expect("combine_or: empty");
    for e in exprs {
        acc = acc.or(e);
    }
    acc
}

/// Replace bare `current_user` / `current_role` identifiers in a policy's
/// USING expression with a single-quoted SQL literal of the principal name.
/// This is intentionally narrow: we don't try to handle arbitrary expressions
/// that compute `current_user` indirectly (e.g. `CONCAT(current_user, 'x')`)
/// — those would require AST-level substitution and are out of scope for v0.1.
///
/// The substitution preserves identifier boundaries: `mycurrent_user_col`
/// is left alone, `current_user` (as a whole token) is replaced.
fn substitute_current_user(using_expr: &str, current_user: &str) -> String {
    let mut out = String::with_capacity(using_expr.len());
    let escaped = current_user.replace('\'', "''");
    let literal = format!("'{}'", escaped);

    let bytes = using_expr.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if !is_ident_start(c) {
            out.push(c);
            i += 1;
            continue;
        }
        // Read a full identifier token.
        let start = i;
        while i < bytes.len() && is_ident_part(bytes[i] as char) {
            i += 1;
        }
        let tok = &using_expr[start..i];
        if tok.eq_ignore_ascii_case("current_user") || tok.eq_ignore_ascii_case("current_role") {
            out.push_str(&literal);
        } else {
            out.push_str(tok);
        }
    }
    out
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_part(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

// --- Build the per-query policy map ----------------------------------------

/// Look up RLS state for every table referenced in `tables` and assemble the
/// per-table predicate list applicable to `current_user` under `kind`.
///
/// Returns an empty map if none of the tables have RLS enabled — that's the
/// signal to the caller (the SELECT executor) to take the no-RLS fast path.
pub(crate) async fn build_policies_for_query(
    catalog: &Arc<dyn basin_catalog::Catalog>,
    project: &basin_common::ProjectId,
    tables: &[TableName],
    current_user: &str,
    kind: PolicyCommand,
) -> Result<HashMap<String, Vec<Policy>>> {
    let mut out = HashMap::new();
    for table in tables {
        let meta = match catalog.load_table(project, table).await {
            Ok(m) => m,
            Err(_) => continue, // user query refers to unknown table; let DF
                                // surface the error.
        };
        if !meta.rls_enabled {
            continue;
        }
        let applicable = select_applicable(&meta.policies, current_user, kind)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        if !applicable.is_empty() {
            out.insert(table.to_string(), applicable);
        } else {
            // RLS is on but no policy applies → deny-all. Postgres'
            // documented default is "no row visible" when RLS is on and
            // no permissive policy matches. Express that as a literal
            // FALSE predicate on this table.
            out.insert(
                table.to_string(),
                vec![Policy {
                    name: "__deny_all__".into(),
                    applies_to_roles: Vec::new(),
                    command: kind,
                    using_expr: "FALSE".into(),
                    with_check_expr: None,
                }],
            );
        }
    }
    Ok(out)
}

// TODO(v0.2): WITH CHECK enforcement on INSERT/UPDATE. The catalog already
// stores `with_check_expr`; the engine's `dml.rs` (INSERT) and
// `dml_mutate.rs` (UPDATE) need a hook that materialises the predicate
// against the about-to-be-written batch and rejects rows that fail. The
// hook lives at the same layer as predicate injection (logical-plan), so
// the right shape is to wrap the staged batch in a `MemTable`, build a
// `SELECT * FROM staged WHERE NOT (<with_check>)` plan, and reject the
// write if that plan returns >0 rows.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitute_current_user_replaces_whole_token() {
        let out = substitute_current_user("owner_id = current_user", "alice");
        assert_eq!(out, "owner_id = 'alice'");
    }

    #[test]
    fn substitute_current_user_leaves_substrings_alone() {
        // a column named `mycurrent_user_col` should be left intact.
        let out = substitute_current_user("mycurrent_user_col = 'x'", "alice");
        assert_eq!(out, "mycurrent_user_col = 'x'");
    }

    #[test]
    fn substitute_current_user_escapes_quotes() {
        let out = substitute_current_user("a = current_user", "o'malley");
        assert_eq!(out, "a = 'o''malley'");
    }

    #[test]
    fn substitute_current_role_also_replaced() {
        let out = substitute_current_user("r = current_role", "alice");
        assert_eq!(out, "r = 'alice'");
    }

    #[test]
    fn select_applicable_filters_by_role_and_command() {
        let pols = vec![
            Policy {
                name: "p_all".into(),
                applies_to_roles: vec![],
                command: PolicyCommand::All,
                using_expr: "true".into(),
                with_check_expr: None,
            },
            Policy {
                name: "p_alice".into(),
                applies_to_roles: vec!["alice".into()],
                command: PolicyCommand::Select,
                using_expr: "owner='alice'".into(),
                with_check_expr: None,
            },
            Policy {
                name: "p_bob_insert".into(),
                applies_to_roles: vec!["bob".into()],
                command: PolicyCommand::Insert,
                using_expr: "true".into(),
                with_check_expr: None,
            },
        ];
        let v = select_applicable(&pols, "alice", PolicyCommand::Select);
        assert_eq!(v.len(), 2);
        assert!(v.iter().any(|p| p.name == "p_all"));
        assert!(v.iter().any(|p| p.name == "p_alice"));
        let v = select_applicable(&pols, "alice", PolicyCommand::Insert);
        // p_all matches any kind, p_alice is SELECT-only, p_bob_insert is bob's.
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].name, "p_all");
    }
}
