//! CREATE TABLE: sqlparser AST → Arrow [`Schema`].

use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::{CheckConstraint, ForeignKeyDef, PartitionSpec, RefAction, UniqueConstraint};
use basin_common::{BasinError, Result};
use sqlparser::ast::{
    ColumnDef, ColumnOption, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GeneratedAs,
    GeneratedExpressionMode, ReferentialAction, TableConstraint,
};

use crate::lifecycle::CreateTableLifecycle;
use crate::types::{
    arrow_data_type, serial_kind, BASIN_AUDIT_TABLE_KEY, BASIN_AUTO_UPDATE_KEY,
    BASIN_COLUMN_DEFAULT, BASIN_GENERATED_AS, BASIN_SOFT_DELETE_KEY, BASIN_TYPE_JSONB,
    BASIN_TYPE_KEY, BASIN_TYPE_TSQUERY, BASIN_TYPE_TSVECTOR, BASIN_TYPE_UUID,
};

/// One implicit sequence promised by a `SERIAL` / `BIGSERIAL` /
/// `SMALLSERIAL` column. The executor walks this list after
/// `Catalog::create_table` and registers each sequence; the DEFAULT
/// metadata on the field already names this sequence via
/// `nextval('<table>_<col>_seq')` so subsequent INSERTs find it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImplicitSequence {
    /// Tenant-scoped sequence name (`<table>_<col>_seq`, PG convention).
    pub name: String,
    /// Column the sequence backs — kept for diagnostics / future
    /// owned-by tracking.
    pub column: String,
}

/// Parsed PK / CHECK / FK extracted from a `CREATE TABLE` AST. The
/// engine persists this onto the `TableMetadata` after the table has
/// been created. Empty fields mean "no constraint of that kind".
#[derive(Debug, Default, Clone)]
pub(crate) struct ExtractedConstraints {
    pub pk_columns: Vec<String>,
    pub checks: Vec<CheckConstraint>,
    pub foreign_keys: Vec<ForeignKeyDef>,
    /// `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` columns each promise a
    /// catalog sequence named `<table>_<col>_seq`. The executor
    /// registers these after the table create succeeds.
    pub implicit_sequences: Vec<ImplicitSequence>,
    /// UNIQUE constraints lifted out of column-level `col TYPE UNIQUE`
    /// and table-level `UNIQUE (col1, col2)` clauses. Persisted onto
    /// `TableMetadata::unique_constraints` via
    /// `Catalog::set_unique_constraints` after the table is created.
    pub uniques: Vec<UniqueConstraint>,
}

/// Inspect `sql` to decide whether the user wrote JSONB (or plain JSON;
/// for v0.1 we treat them as the same logical type — see the comment in
/// `crate::types::arrow_data_type`). JSONB has no dedicated Arrow
/// `DataType`, so `schema_from_columns` tags the resulting `Field` with
/// metadata the rest of the engine reads to recover the logical type.
fn is_jsonb_sql(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    match sql {
        SqlDataType::JSONB | SqlDataType::JSON => true,
        SqlDataType::Custom(name, modifiers) => {
            name.0.len() == 1
                && name.0[0].value.eq_ignore_ascii_case("jsonb")
                && modifiers.is_empty()
        }
        _ => false,
    }
}

/// Mirror of `is_jsonb_sql` for UUID columns. UUID rides on
/// `FixedSizeBinary(16)` at the Arrow level; the metadata marker tells the
/// pgwire encoder + REST layer to render bytes as the canonical hyphenated
/// text form (and to advertise OID 2950 instead of `bytea`).
fn is_uuid_sql(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    match sql {
        SqlDataType::Uuid => true,
        SqlDataType::Custom(name, modifiers) => {
            name.0.len() == 1
                && name.0[0].value.eq_ignore_ascii_case("uuid")
                && modifiers.is_empty()
        }
        _ => false,
    }
}

/// Returns `true` if the SQL column type is `TSVECTOR`. The Arrow physical
/// type is `Utf8`; the `BASIN_TYPE=TSVECTOR` marker on the field tells
/// downstream layers to advertise PG OID 3614.
fn is_tsvector_sql(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    if let SqlDataType::Custom(name, modifiers) = sql {
        name.0.len() == 1
            && name.0[0].value.eq_ignore_ascii_case("tsvector")
            && modifiers.is_empty()
    } else {
        false
    }
}

/// Returns `true` if the SQL column type is `TSQUERY`. The Arrow physical
/// type is `Utf8`; the `BASIN_TYPE=TSQUERY` marker on the field tells
/// downstream layers to advertise PG OID 3615.
fn is_tsquery_sql(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    if let SqlDataType::Custom(name, modifiers) = sql {
        name.0.len() == 1
            && name.0[0].value.eq_ignore_ascii_case("tsquery")
            && modifiers.is_empty()
    } else {
        false
    }
}

/// Build an Arrow [`Schema`] from sqlparser column definitions plus the
/// PRIMARY KEY / CHECK / FOREIGN KEY constraints extracted from
/// column-level and table-level constraint clauses.
///
/// Nullability defaults to `true`; `NOT NULL` flips it. Column-level
/// `PRIMARY KEY` / `CHECK (<expr>)` / `REFERENCES <table>(<col>)` are
/// hoisted into `ExtractedConstraints` rather than rejected.
/// `UNIQUE` (when not `PRIMARY KEY`) is out of scope for v0.1 and
/// rejected.
///
/// `lifecycle` folds in declarative-lifecycle markers extracted by the
/// pre-screener (`AUTO_UPDATE`, `SOFT DELETE`, `AUDIT TO`). At most one
/// `SOFT DELETE` column is allowed; a second declaration is rejected.
///
/// `table_name` is used to mint default constraint names.
pub(crate) fn schema_and_constraints_from_columns(
    columns: &[ColumnDef],
    table_constraints: &[TableConstraint],
    table_name: &str,
    lifecycle: &CreateTableLifecycle,
) -> Result<(Schema, ExtractedConstraints)> {
    if columns.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE TABLE requires at least one column".into(),
        ));
    }
    if lifecycle.soft_delete_columns.len() > 1 {
        return Err(BasinError::InvalidSchema(
            "a table may declare at most one SOFT DELETE column".into(),
        ));
    }
    // Reject lifecycle attributes that name a column that doesn't exist on
    // the table (typo guard).
    for c in lifecycle
        .auto_update_columns
        .iter()
        .chain(lifecycle.soft_delete_columns.iter())
    {
        if !columns
            .iter()
            .any(|cd| cd.name.value.eq_ignore_ascii_case(c))
        {
            return Err(BasinError::InvalidSchema(format!(
                "lifecycle attribute references unknown column {c:?}"
            )));
        }
    }

    let mut fields = Vec::with_capacity(columns.len());
    let column_names_lc: Vec<String> = columns
        .iter()
        .map(|c| c.name.value.to_ascii_lowercase())
        .collect();
    let mut extracted = ExtractedConstraints::default();
    let mut check_counter: usize = 0;
    let mut col_pk_columns: Vec<String> = Vec::new();
    let mut explicit_null: std::collections::HashSet<String> = std::collections::HashSet::new();
    for col in columns {
        let dt = arrow_data_type(&col.data_type)?;
        // SERIAL / BIGSERIAL / SMALLSERIAL expand to an integer column +
        // an implicit sequence + `DEFAULT nextval(...)` + `NOT NULL`.
        // We capture the kind here; the option loop below picks it up
        // *after* honouring user-written `DEFAULT` (PG-shaped: explicit
        // DEFAULT on a SERIAL silently overrides the auto one but the
        // sequence still gets created so future inserts can reach it).
        let serial = serial_kind(&col.data_type);
        let mut nullable = true;
        let mut generated_expr: Option<String> = None;
        let mut default_text: Option<String> = None;
        for opt in &col.options {
            match &opt.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Null => {
                    nullable = true;
                    explicit_null.insert(col.name.value.to_ascii_lowercase());
                }
                // Column-level `PRIMARY KEY` / `UNIQUE`. PG / sqlparser
                // collapse the two; we route `is_primary == true` to the
                // PK column list and `is_primary == false` to the unique
                // constraint set (synthesising a PG-shaped name).
                ColumnOption::Unique { is_primary, .. } => {
                    if *is_primary {
                        col_pk_columns.push(col.name.value.clone());
                    } else {
                        let uname = format!("{table_name}_{}_key", col.name.value);
                        extracted.uniques.push(UniqueConstraint {
                            name: uname,
                            columns: vec![col.name.value.clone()],
                        });
                    }
                }
                ColumnOption::Check(expr) => {
                    let name = format!("{table_name}_{}_check", col.name.value);
                    extracted.checks.push(CheckConstraint {
                        name,
                        predicate: expr.to_string(),
                    });
                }
                ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                } => {
                    if foreign_table.0.len() != 1 {
                        return Err(BasinError::InvalidSchema(format!(
                            "FOREIGN KEY references must be a bare table name; got {foreign_table}"
                        )));
                    }
                    let ref_table = foreign_table.0[0].value.clone();
                    if referred_columns.is_empty() {
                        return Err(BasinError::InvalidSchema(format!(
                            "FOREIGN KEY column {:?} REFERENCES {ref_table}: \
                             specify the referenced column(s) explicitly in v0.1",
                            col.name.value
                        )));
                    }
                    let ref_columns: Vec<String> =
                        referred_columns.iter().map(|i| i.value.clone()).collect();
                    let on_delete_act = referential_action_from_ast(*on_delete)?;
                    let on_update_act = referential_action_from_ast(*on_update)?;
                    let name = format!("{table_name}_{}_fkey", col.name.value);
                    extracted.foreign_keys.push(ForeignKeyDef {
                        name,
                        columns: vec![col.name.value.clone()],
                        ref_table,
                        ref_columns,
                        on_delete: on_delete_act,
                        on_update: on_update_act,
                    });
                }
                ColumnOption::Default(expr) => {
                    // Store the DEFAULT expression text on the column's
                    // field metadata. INSERT-time evaluation lives in
                    // `crate::seq_ddl::evaluate_default_expression`,
                    // which routes the text through
                    // `rewrite_sequence_calls` before parsing it back
                    // into an Expr — so `DEFAULT nextval('s')` advances
                    // the sequence on each row.
                    default_text = Some(expr.to_string());
                }
                ColumnOption::Generated {
                    generated_as,
                    sequence_options,
                    generation_expr,
                    generation_expr_mode,
                    generated_keyword: _,
                } => {
                    if sequence_options.is_some() {
                        return Err(BasinError::InvalidSchema(
                            "GENERATED ... AS IDENTITY is not supported in v0.1; use a SEQUENCE \
                             (5.11.K3) or a plain column"
                                .to_string(),
                        ));
                    }
                    match generated_as {
                        GeneratedAs::Always | GeneratedAs::ExpStored => {}
                        GeneratedAs::ByDefault => {
                            return Err(BasinError::InvalidSchema(
                                "GENERATED BY DEFAULT AS is not supported; use \
                                 GENERATED ALWAYS AS (<expr>) STORED"
                                    .to_string(),
                            ));
                        }
                    }
                    let expr = generation_expr.as_ref().ok_or_else(|| {
                        BasinError::InvalidSchema(format!(
                            "GENERATED column {:?} requires AS (<expression>) STORED",
                            col.name.value
                        ))
                    })?;
                    match generation_expr_mode {
                        Some(GeneratedExpressionMode::Stored) => {}
                        // `ExpStored` style omits an explicit STORED token in
                        // some dialects but the parser already classified the
                        // expression as stored; treat it the same.
                        None if matches!(generated_as, GeneratedAs::ExpStored) => {}
                        Some(GeneratedExpressionMode::Virtual) => {
                            return Err(BasinError::FeatureNotSupported(
                                "VIRTUAL generated columns deferred to v0.2; use STORED"
                                    .to_string(),
                            ));
                        }
                        None => {
                            return Err(BasinError::FeatureNotSupported(
                                "VIRTUAL generated columns deferred to v0.2; use STORED"
                                    .to_string(),
                            ));
                        }
                    }
                    let expr_text = expr.to_string();
                    // Reject self-reference at registration. PG forbids
                    // self-reference and forward-reference; v0.1 only catches
                    // the self-reference case (forward-reference is rare).
                    if expr_references_identifier(expr, &col.name.value) {
                        return Err(BasinError::InvalidSchema(format!(
                            "generated column {:?} cannot reference itself in its expression",
                            col.name.value
                        )));
                    }
                    // All identifier references in the expression must
                    // resolve to a column on this table; otherwise we'd
                    // accept a typo silently and surface it on first INSERT.
                    let referenced = collect_identifier_refs(expr);
                    for r in &referenced {
                        if !column_names_lc.iter().any(|c| c == &r.to_ascii_lowercase()) {
                            return Err(BasinError::InvalidSchema(format!(
                                "generated column {:?} references unknown column {:?}",
                                col.name.value, r
                            )));
                        }
                    }
                    generated_expr = Some(expr_text);
                }
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "unsupported column option in PoC: {other}"
                    )));
                }
            }
        }
        // SERIAL post-processing. PG semantics:
        //   * `NOT NULL` is implicit (we force it).
        //   * Sequence is auto-created as `<table>_<col>_seq`.
        //   * `DEFAULT nextval('<seq>')` is added unless the user wrote
        //     their own DEFAULT (in which case PG keeps the user's).
        //   * `NULL` on the column is rejected (PG does the same).
        if let Some(_kind) = serial {
            if explicit_null.contains(&col.name.value.to_ascii_lowercase()) {
                return Err(BasinError::InvalidSchema(format!(
                    "SERIAL column {:?} cannot also be declared NULL",
                    col.name.value
                )));
            }
            if generated_expr.is_some() {
                return Err(BasinError::InvalidSchema(format!(
                    "SERIAL column {:?} cannot also be GENERATED ALWAYS AS",
                    col.name.value
                )));
            }
            nullable = false;
            let seq_name = format!(
                "{}_{}_seq",
                table_name.to_ascii_lowercase(),
                col.name.value.to_ascii_lowercase()
            );
            if default_text.is_none() {
                default_text = Some(format!("nextval('{seq_name}')"));
            }
            extracted.implicit_sequences.push(ImplicitSequence {
                name: seq_name,
                column: col.name.value.clone(),
            });
        }
        let mut md: HashMap<String, String> = HashMap::new();
        if is_jsonb_sql(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_JSONB.to_string());
        } else if is_uuid_sql(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_UUID.to_string());
        } else if is_tsvector_sql(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_TSVECTOR.to_string());
        } else if is_tsquery_sql(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_TSQUERY.to_string());
        }
        if let Some(expr_text) = generated_expr.as_ref() {
            md.insert(BASIN_GENERATED_AS.to_string(), expr_text.clone());
        }
        if let Some(default_expr) = default_text.as_ref() {
            md.insert(BASIN_COLUMN_DEFAULT.to_string(), default_expr.clone());
        }
        let col_name = &col.name.value;
        if lifecycle
            .auto_update_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(col_name))
        {
            // AUTO_UPDATE only makes sense on a TIMESTAMPTZ-shaped column.
            if !matches!(dt, DataType::Timestamp(_, _)) {
                return Err(BasinError::InvalidSchema(format!(
                    "AUTO_UPDATE column {col_name:?} must be TIMESTAMPTZ"
                )));
            }
            md.insert(BASIN_AUTO_UPDATE_KEY.to_string(), "1".into());
        }
        if lifecycle
            .soft_delete_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(col_name))
        {
            if !matches!(dt, DataType::Timestamp(_, _)) {
                return Err(BasinError::InvalidSchema(format!(
                    "SOFT DELETE column {col_name:?} must be TIMESTAMPTZ"
                )));
            }
            md.insert(BASIN_SOFT_DELETE_KEY.to_string(), "1".into());
        }
        let mut field = Field::new(col_name.clone(), dt, nullable);
        if !md.is_empty() {
            field = field.with_metadata(md);
        }
        fields.push(field);
    }

    // Process table-level constraints. A `PRIMARY KEY (a, b)` clause
    // wins over column-level inline `PRIMARY KEY` (PG semantics).
    let mut table_pk: Vec<String> = Vec::new();
    for tc in table_constraints {
        match tc {
            TableConstraint::PrimaryKey { columns, name, .. } => {
                if !table_pk.is_empty() {
                    return Err(BasinError::InvalidSchema(
                        "multiple table-level PRIMARY KEY clauses".into(),
                    ));
                }
                let _ = name;
                table_pk = columns.iter().map(|i| i.value.clone()).collect();
            }
            TableConstraint::Unique { name, columns, .. } => {
                if columns.is_empty() {
                    return Err(BasinError::InvalidSchema(
                        "UNIQUE: column list cannot be empty".into(),
                    ));
                }
                let cols: Vec<String> = columns.iter().map(|i| i.value.clone()).collect();
                // Reject duplicates inside the constraint's own column
                // list — PG accepts this but it's almost always a typo
                // and the dedup check on enforcement makes the dup a
                // no-op anyway.
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                for c in &cols {
                    if !seen.insert(c.to_ascii_lowercase()) {
                        return Err(BasinError::InvalidSchema(format!(
                            "UNIQUE: column {c:?} listed twice"
                        )));
                    }
                }
                let cname = match name {
                    Some(n) => n.value.clone(),
                    None => format!("{table_name}_{}_key", cols.join("_")),
                };
                extracted.uniques.push(UniqueConstraint {
                    name: cname,
                    columns: cols,
                });
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                if foreign_table.0.len() != 1 {
                    return Err(BasinError::InvalidSchema(format!(
                        "FOREIGN KEY references must be a bare table name; got {foreign_table}"
                    )));
                }
                let ref_table = foreign_table.0[0].value.clone();
                if columns.is_empty() {
                    return Err(BasinError::InvalidSchema(
                        "FOREIGN KEY: empty column list".into(),
                    ));
                }
                if referred_columns.is_empty() {
                    return Err(BasinError::InvalidSchema(format!(
                        "FOREIGN KEY ({}) REFERENCES {ref_table}: \
                         specify the referenced column(s) explicitly in v0.1",
                        columns
                            .iter()
                            .map(|i| i.value.clone())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
                let local_cols: Vec<String> = columns.iter().map(|i| i.value.clone()).collect();
                let ref_cols: Vec<String> =
                    referred_columns.iter().map(|i| i.value.clone()).collect();
                if local_cols.len() != ref_cols.len() {
                    return Err(BasinError::InvalidSchema(
                        "FOREIGN KEY: local and referenced column counts differ".into(),
                    ));
                }
                let on_delete_act = referential_action_from_ast(*on_delete)?;
                let on_update_act = referential_action_from_ast(*on_update)?;
                let fk_name = match name {
                    Some(n) => n.value.clone(),
                    None => format!("{table_name}_{}_fkey", local_cols[0]),
                };
                extracted.foreign_keys.push(ForeignKeyDef {
                    name: fk_name,
                    columns: local_cols,
                    ref_table,
                    ref_columns: ref_cols,
                    on_delete: on_delete_act,
                    on_update: on_update_act,
                });
            }
            TableConstraint::Check { name, expr } => {
                check_counter += 1;
                let cname = match name {
                    Some(n) => n.value.clone(),
                    None => format!("{table_name}_check_{check_counter}"),
                };
                extracted.checks.push(CheckConstraint {
                    name: cname,
                    predicate: expr.to_string(),
                });
            }
            TableConstraint::Index { .. } | TableConstraint::FulltextOrSpatial { .. } => {
                return Err(BasinError::FeatureNotSupported(
                    "INDEX / FULLTEXT / SPATIAL table constraints are not supported in v0.1".into(),
                ));
            }
        }
    }

    let pk_columns = if !table_pk.is_empty() {
        if !col_pk_columns.is_empty() {
            return Err(BasinError::InvalidSchema(
                "table cannot mix column-level PRIMARY KEY and table-level PRIMARY KEY".into(),
            ));
        }
        table_pk
    } else {
        col_pk_columns
    };

    if !pk_columns.is_empty() {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for c in &pk_columns {
            if !seen.insert(c.to_ascii_lowercase()) {
                return Err(BasinError::InvalidSchema(format!(
                    "PRIMARY KEY column {c:?} listed twice"
                )));
            }
            let pos = fields
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(c))
                .ok_or_else(|| {
                    BasinError::InvalidSchema(format!(
                        "PRIMARY KEY column {c:?} is not in the table"
                    ))
                })?;
            // PG auto-promotes PK columns to NOT NULL. We mirror — but
            // reject the case where the user *explicitly* wrote `NULL`
            // on a PK column (the AST distinguishes `NULL` from absent
            // via the `column_explicitly_nullable` set we track in the
            // first pass... actually, sqlparser collapses both into a
            // single `nullable` bit and we don't have the visibility
            // here). We use `was_explicitly_null` only for the column
            // we just authored, falling back to silent promotion
            // otherwise — same effective shape PG ships.
            let f = &fields[pos];
            if f.is_nullable() && explicit_null.contains(&f.name().to_ascii_lowercase()) {
                return Err(BasinError::InvalidSchema(format!(
                    "PRIMARY KEY column {c:?} must be NOT NULL (explicit NULL is incompatible)"
                )));
            }
            if f.is_nullable() {
                let promoted =
                    arrow_schema::Field::new(f.name().clone(), f.data_type().clone(), false)
                        .with_metadata(f.metadata().clone());
                fields[pos] = promoted;
            }
        }
    }

    // Rebuild schema with possibly-promoted PK columns.
    let schema = if let Some(audit) = lifecycle.audit_table.as_ref() {
        let mut md = std::collections::HashMap::new();
        md.insert(BASIN_AUDIT_TABLE_KEY.to_string(), audit.clone());
        Schema::new_with_metadata(fields, md)
    } else {
        Schema::new(fields)
    };

    // Validate UNIQUE column references against the (final) schema.
    // Duplicate constraint names across the table are rejected here too
    // so the catalog's lookup-by-name stays unambiguous. PG names PK
    // and UNIQUE constraints in the same namespace, so a collision
    // between the auto-named `<table>_pkey` and a user-written
    // `CONSTRAINT <table>_pkey UNIQUE (...)` is also a conflict.
    if !extracted.uniques.is_empty() {
        let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();
        for u in &extracted.uniques {
            for c in &u.columns {
                if schema.field_with_name(c).is_err() {
                    return Err(BasinError::InvalidSchema(format!(
                        "UNIQUE constraint {:?}: column {c:?} is not in the table",
                        u.name
                    )));
                }
            }
            if !seen_names.insert(u.name.to_ascii_lowercase()) {
                return Err(BasinError::InvalidSchema(format!(
                    "duplicate constraint name {:?} on this table",
                    u.name
                )));
            }
        }
    }

    extracted.pk_columns = pk_columns;
    Ok((schema, extracted))
}

fn referential_action_from_ast(action: Option<ReferentialAction>) -> Result<RefAction> {
    match action {
        None | Some(ReferentialAction::NoAction) => Ok(RefAction::NoAction),
        Some(ReferentialAction::Cascade) => Ok(RefAction::Cascade),
        Some(ReferentialAction::Restrict) => Err(BasinError::FeatureNotSupported(
            "ON DELETE/UPDATE RESTRICT is not supported in v0.1; use NO ACTION (default)".into(),
        )),
        Some(ReferentialAction::SetNull) => Err(BasinError::FeatureNotSupported(
            "ON DELETE/UPDATE SET NULL is not supported in v0.1".into(),
        )),
        Some(ReferentialAction::SetDefault) => Err(BasinError::FeatureNotSupported(
            "ON DELETE/UPDATE SET DEFAULT is not supported in v0.1".into(),
        )),
    }
}

/// Translate the AST's `PARTITION BY ...` expression (when present) into a
/// [`PartitionSpec`]. Validates the partition column exists in `schema` and
/// has a type the partition pruner knows how to bucket. We support exactly
/// one shape today: `RANGE (col)` over `TIMESTAMPTZ` or `BIGINT`-as-epoch
/// columns. Anything else surfaces as `InvalidSchema` so a user can't
/// silently end up with an unpartitioned-but-claimed-partitioned table.
pub(crate) fn partition_spec_from_ast(
    partition_by: Option<&Expr>,
    schema: &Schema,
) -> Result<PartitionSpec> {
    let Some(expr) = partition_by else {
        return Ok(PartitionSpec::Unpartitioned);
    };
    // sqlparser parses `PARTITION BY RANGE (ts)` as `Expr::Function {
    // name: "RANGE", args: (ts) }`. Reach in and pull out the column name.
    let func = match expr {
        Expr::Function(f) => f,
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "PARTITION BY expression must be RANGE(col); got {other}"
            )));
        }
    };
    let strategy = func
        .name
        .0
        .last()
        .map(|i| i.value.to_ascii_uppercase())
        .unwrap_or_default();
    if strategy != "RANGE" {
        return Err(BasinError::InvalidSchema(format!(
            "only PARTITION BY RANGE is supported in v0.1; got {strategy}"
        )));
    }
    let args = match &func.args {
        FunctionArguments::List(list) => &list.args,
        _ => {
            return Err(BasinError::InvalidSchema(
                "PARTITION BY RANGE requires a column list".into(),
            ));
        }
    };
    if args.len() != 1 {
        return Err(BasinError::InvalidSchema(format!(
            "PARTITION BY RANGE requires exactly one column, got {}",
            args.len()
        )));
    }
    let col_name = match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => ident.value.clone(),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => parts
            .last()
            .map(|p| p.value.clone())
            .ok_or_else(|| BasinError::InvalidSchema("empty compound identifier".into()))?,
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "PARTITION BY RANGE column must be a bare identifier, got {other}"
            )));
        }
    };
    let field = schema
        .fields()
        .iter()
        .find(|f| f.name() == &col_name)
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "PARTITION BY column {col_name:?} is not in the table schema"
            ))
        })?;
    match field.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::Timestamp(TimeUnit::Second, _)
        | DataType::Int64 => Ok(PartitionSpec::RangeMonthly { column: col_name }),
        other => Err(BasinError::InvalidSchema(format!(
            "PARTITION BY RANGE column {col_name} must be TIMESTAMPTZ or BIGINT-as-epoch; got {other:?}"
        ))),
    }
}

/// Phase 5.7 B2: Pre-screen `CREATE TABLE … CLUSTER BY (col1, col2)` and
/// strip the trailing clause before sqlparser sees the SQL.
///
/// `sqlparser` 0.52's `PostgreSqlDialect` only recognises `CLUSTER BY` for
/// BigQuery, so the engine handles it the same way it handles other Basin
/// extension DDL: a small string-literal-aware scan that lifts the clause
/// out of the input. Returns `(stripped_sql, Some(columns))` on a match,
/// or `(original_sql, None)` if no `CLUSTER BY` clause is present at the
/// tail of the statement.
///
/// Only the trailing `CLUSTER BY (...)` immediately preceding any
/// `;`/whitespace tail is considered — `CLUSTER BY` deeper inside the
/// statement (e.g. inside a string literal or a comment, or theoretically
/// inside a future-supported subquery) is left alone.
pub(crate) fn extract_create_table_cluster_by(sql: &str) -> Result<(String, Option<Vec<String>>)> {
    // Cheap rejection: skip the scan unless the statement starts with
    // CREATE TABLE. We don't need a perfect leading-keyword check — the
    // tail scan won't match anything else anyway, but this keeps the
    // hot path (every non-CREATE statement) at one ASCII compare.
    let leading = sql.trim_start();
    if !leading
        .get(..6)
        .map(|s| s.eq_ignore_ascii_case("create"))
        .unwrap_or(false)
    {
        return Ok((sql.to_string(), None));
    }

    // Scan forward, skipping over string literals and comments, recording
    // the position of every top-level `CLUSTER BY` keyword pair. The tail
    // match is the last one whose closing `)` is followed only by
    // whitespace and an optional `;`.
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut last_match: Option<(usize, Vec<String>, usize)> = None; // (start, cols, end_after_paren)

    while i < bytes.len() {
        let b = bytes[i];
        // String literal: `'...'` with `''` escapes.
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Quoted identifier: `"..."` (no escape handling beyond doubled quotes).
        if b == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Line comment: `-- ...` to end-of-line.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Block comment: `/* ... */`.
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            }
            continue;
        }
        // Match the keyword pair `CLUSTER BY` only at a word boundary.
        if (b == b'C' || b == b'c')
            && bytes_match_kw(&bytes[i..], b"CLUSTER")
            && at_word_boundary(bytes, i)
            && at_word_boundary_end(bytes, i + 7)
        {
            // Skip whitespace between CLUSTER and BY.
            let mut j = i + 7;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len()
                && bytes_match_kw(&bytes[j..], b"BY")
                && at_word_boundary_end(bytes, j + 2)
            {
                let after_by = j + 2;
                // Skip whitespace, then expect `(`.
                let mut k = after_by;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if k < bytes.len() && bytes[k] == b'(' {
                    // Find the matching `)`.
                    let open = k;
                    let mut depth = 1i32;
                    let mut m = k + 1;
                    while m < bytes.len() && depth > 0 {
                        match bytes[m] {
                            b'(' => depth += 1,
                            b')' => depth -= 1,
                            _ => {}
                        }
                        if depth == 0 {
                            break;
                        }
                        m += 1;
                    }
                    if depth == 0 && m < bytes.len() {
                        let inside = &sql[open + 1..m];
                        let cols = parse_paren_ident_list(inside)?;
                        last_match = Some((i, cols, m + 1));
                        i = m + 1;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }

    // Only accept the match if it's at the tail (whitespace + optional `;` only).
    if let Some((start, cols, end)) = last_match {
        let tail = sql[end..].trim();
        if tail.is_empty() || tail == ";" {
            // Strip the clause; preserve everything up to `start`. Re-add
            // a trailing `;` if the original ended with one.
            let mut stripped = sql[..start].trim_end().to_string();
            if sql.trim_end().ends_with(';') {
                stripped.push(';');
            }
            return Ok((stripped, Some(cols)));
        }
    }
    Ok((sql.to_string(), None))
}

/// Parse a `(col1, col2, …)` body (without the surrounding parens) into a
/// vec of bare identifiers. Rejects empty lists and non-identifier
/// tokens. Mirrors the helper in `crate::alter`.
fn parse_paren_ident_list(inside: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for raw in inside.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        // Reject anything that isn't a bare identifier.
        if !token.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || token
                .chars()
                .next()
                .map(|c| c.is_ascii_digit())
                .unwrap_or(true)
        {
            return Err(BasinError::InvalidSchema(format!(
                "CLUSTER BY: bad identifier {token:?}"
            )));
        }
        out.push(token.to_string());
    }
    if out.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CLUSTER BY: column list cannot be empty".into(),
        ));
    }
    Ok(out)
}

/// True if `expr` mentions an `Identifier` whose name matches `name`
/// case-insensitively. Used to catch self-reference in `GENERATED ALWAYS
/// AS (...)` expressions at registration time.
fn expr_references_identifier(expr: &Expr, name: &str) -> bool {
    let mut hit = false;
    walk_expr(expr, &mut |e| {
        if hit {
            return;
        }
        match e {
            Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case(name) => hit = true,
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last() {
                    if last.value.eq_ignore_ascii_case(name) {
                        hit = true;
                    }
                }
            }
            _ => {}
        }
    });
    hit
}

/// Pull every bare-identifier reference (and the trailing component of a
/// compound identifier) out of `expr`. Used at CREATE TABLE time to validate
/// that a `GENERATED ALWAYS AS` expression only mentions columns that
/// exist on this table.
fn collect_identifier_refs(expr: &Expr) -> Vec<String> {
    let mut out = Vec::new();
    walk_expr(expr, &mut |e| match e {
        Expr::Identifier(ident) => out.push(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                out.push(last.value.clone());
            }
        }
        _ => {}
    });
    out
}

/// Pre-order walk over a sqlparser expression tree, calling `visit` on every
/// node. Coverage is the small-but-useful subset we expect in generated-column
/// expressions: binary/unary ops, function calls, casts, IS NULL / IS NOT NULL,
/// IN, BETWEEN, CASE. Anything we don't recognise terminates that branch
/// silently — over-collecting refs is harmless because they'll show up in the
/// referenced-column scan and just get rejected if unknown, while
/// under-collecting only matters if the user wrote something exotic that
/// happens to self-reference, and PG rejects that on first INSERT anyway.
fn walk_expr(expr: &Expr, visit: &mut dyn FnMut(&Expr)) {
    visit(expr);
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            walk_expr(left, visit);
            walk_expr(right, visit);
        }
        Expr::UnaryOp { expr, .. } => walk_expr(expr, visit),
        Expr::Nested(inner) => walk_expr(inner, visit),
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => walk_expr(inner, visit),
        Expr::Cast { expr, .. } => walk_expr(expr, visit),
        Expr::Between {
            expr, low, high, ..
        } => {
            walk_expr(expr, visit);
            walk_expr(low, visit);
            walk_expr(high, visit);
        }
        Expr::InList { expr, list, .. } => {
            walk_expr(expr, visit);
            for it in list {
                walk_expr(it, visit);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(o) = operand.as_deref() {
                walk_expr(o, visit);
            }
            for c in conditions {
                walk_expr(c, visit);
            }
            for r in results {
                walk_expr(r, visit);
            }
            if let Some(e) = else_result.as_deref() {
                walk_expr(e, visit);
            }
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = a {
                        walk_expr(inner, visit);
                    } else if let FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(inner),
                        ..
                    } = a
                    {
                        walk_expr(inner, visit);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Case-insensitive prefix match on an ASCII keyword.
fn bytes_match_kw(bytes: &[u8], kw: &[u8]) -> bool {
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    true
}

fn at_word_boundary(bytes: &[u8], i: usize) -> bool {
    if i == 0 {
        return true;
    }
    let prev = bytes[i - 1];
    !(prev.is_ascii_alphanumeric() || prev == b'_')
}

fn at_word_boundary_end(bytes: &[u8], i: usize) -> bool {
    if i >= bytes.len() {
        return true;
    }
    let c = bytes[i];
    !(c.is_ascii_alphanumeric() || c == b'_')
}

#[cfg(test)]
mod cluster_by_tests {
    use super::*;

    #[test]
    fn extract_single_col() {
        let (stripped, cols) =
            extract_create_table_cluster_by("CREATE TABLE foo (id BIGINT) CLUSTER BY (id)")
                .unwrap();
        assert_eq!(stripped.trim(), "CREATE TABLE foo (id BIGINT)");
        assert_eq!(cols, Some(vec!["id".into()]));
    }

    #[test]
    fn extract_multiple_cols() {
        let (stripped, cols) = extract_create_table_cluster_by(
            "CREATE TABLE foo (id BIGINT, ts BIGINT) CLUSTER BY (id, ts)",
        )
        .unwrap();
        assert_eq!(stripped.trim(), "CREATE TABLE foo (id BIGINT, ts BIGINT)");
        assert_eq!(cols, Some(vec!["id".into(), "ts".into()]));
    }

    #[test]
    fn extract_with_trailing_semicolon() {
        let (stripped, cols) =
            extract_create_table_cluster_by("CREATE TABLE foo (id BIGINT) CLUSTER BY (id);")
                .unwrap();
        assert_eq!(
            stripped.trim_end_matches(|c: char| c.is_whitespace()),
            "CREATE TABLE foo (id BIGINT);"
        );
        assert_eq!(cols, Some(vec!["id".into()]));
    }

    #[test]
    fn no_cluster_by() {
        let (stripped, cols) =
            extract_create_table_cluster_by("CREATE TABLE foo (id BIGINT)").unwrap();
        assert_eq!(stripped, "CREATE TABLE foo (id BIGINT)");
        assert_eq!(cols, None);
    }

    #[test]
    fn cluster_by_inside_string_literal_ignored() {
        // A literal containing the substring `CLUSTER BY (x)` must NOT
        // confuse the extractor.
        let sql = "CREATE TABLE foo (id BIGINT, payload TEXT DEFAULT 'CLUSTER BY (xx)')";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }

    #[test]
    fn case_insensitive() {
        let (_stripped, cols) =
            extract_create_table_cluster_by("create table foo (id BIGINT) cluster by (id)")
                .unwrap();
        assert_eq!(cols, Some(vec!["id".into()]));
    }

    #[test]
    fn empty_column_list_rejected() {
        let err = extract_create_table_cluster_by("CREATE TABLE foo (id BIGINT) CLUSTER BY ()")
            .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn non_create_statement_returns_none() {
        let (stripped, cols) =
            extract_create_table_cluster_by("SELECT * FROM events WHERE id = 1").unwrap();
        assert_eq!(stripped, "SELECT * FROM events WHERE id = 1");
        assert_eq!(cols, None);
    }

    #[test]
    fn cluster_by_in_block_comment_ignored() {
        let sql = "CREATE TABLE foo (id BIGINT) /* CLUSTER BY (x) */";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }

    #[test]
    fn cluster_by_in_line_comment_ignored() {
        let sql = "CREATE TABLE foo (id BIGINT)\n-- CLUSTER BY (x)\n";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }

    #[test]
    fn rejects_word_prefix_match() {
        // `CLUSTERED` should NOT match `CLUSTER`.
        let sql = "CREATE TABLE foo (id BIGINT) /* CLUSTERED INDEX placeholder */";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }

    #[test]
    fn cluster_by_not_at_tail_left_alone() {
        // CLUSTER BY (x) followed by extra non-whitespace tokens isn't
        // the trailing clause shape we accept.
        let sql = "CREATE TABLE foo (id BIGINT) CLUSTER BY (id) EXTRA";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }
}
