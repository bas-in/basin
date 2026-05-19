//! CREATE TABLE: sqlparser AST → Arrow [`Schema`].

use crate::pg_ast::ObjectNamePartExt;
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
    arrow_data_type, basin_type_marker, charlen_marker, is_cidr_sql, is_daterange_sql, is_inet_sql,
    is_int4range_sql, is_int8range_sql, is_macaddr8_sql, is_macaddr_sql, is_money_sql,
    is_numrange_sql, is_tsrange_sql, is_tstzrange_sql, is_xml_sql, serial_kind,
    BASIN_AUDIT_TABLE_KEY, BASIN_AUTO_UPDATE_KEY, BASIN_CHARLEN_KEY, BASIN_COLUMN_DEFAULT,
    BASIN_GENERATED_AS, BASIN_IDENTITY, BASIN_IDENTITY_ALWAYS, BASIN_IDENTITY_BY_DEFAULT,
    BASIN_IDENTITY_SEQ, BASIN_SOFT_DELETE_KEY, BASIN_TYPE_CIDR, BASIN_TYPE_DATERANGE,
    BASIN_TYPE_INET, BASIN_TYPE_INT4RANGE, BASIN_TYPE_INT8RANGE, BASIN_TYPE_JSONB, BASIN_TYPE_KEY,
    BASIN_TYPE_MACADDR, BASIN_TYPE_MACADDR8, BASIN_TYPE_MONEY, BASIN_TYPE_NUMRANGE,
    BASIN_TYPE_TSQUERY, BASIN_TYPE_TSRANGE, BASIN_TYPE_TSTZRANGE, BASIN_TYPE_TSVECTOR,
    BASIN_TYPE_UUID, BASIN_TYPE_XML,
};

/// One implicit sequence promised by a `SERIAL` / `BIGSERIAL` /
/// `SMALLSERIAL` column. The executor walks this list after
/// `Catalog::create_table` and registers each sequence; the DEFAULT
/// metadata on the field already names this sequence via
/// `nextval('<table>_<col>_seq')` so subsequent INSERTs find it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImplicitSequence {
    /// Project-scoped sequence name (`<table>_<col>_seq`, PG convention).
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
                && name.0[0].id_val().eq_ignore_ascii_case("jsonb")
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
                && name.0[0].id_val().eq_ignore_ascii_case("uuid")
                && modifiers.is_empty()
        }
        _ => false,
    }
}

/// Returns the `BASIN_TYPE` marker string to stamp on the Arrow `Field`
/// metadata for new "text-with-marker" types (network addresses, money, xml,
/// ranges). Returns `None` for all other types (handled by other branches).
fn basin_type_marker_for(sql: &sqlparser::ast::DataType) -> Option<&'static str> {
    if is_inet_sql(sql) {
        Some(BASIN_TYPE_INET)
    } else if is_cidr_sql(sql) {
        Some(BASIN_TYPE_CIDR)
    } else if is_macaddr_sql(sql) {
        Some(BASIN_TYPE_MACADDR)
    } else if is_macaddr8_sql(sql) {
        Some(BASIN_TYPE_MACADDR8)
    } else if is_money_sql(sql) {
        Some(BASIN_TYPE_MONEY)
    } else if is_xml_sql(sql) {
        Some(BASIN_TYPE_XML)
    } else if is_int4range_sql(sql) {
        Some(BASIN_TYPE_INT4RANGE)
    } else if is_int8range_sql(sql) {
        Some(BASIN_TYPE_INT8RANGE)
    } else if is_numrange_sql(sql) {
        Some(BASIN_TYPE_NUMRANGE)
    } else if is_daterange_sql(sql) {
        Some(BASIN_TYPE_DATERANGE)
    } else if is_tsrange_sql(sql) {
        Some(BASIN_TYPE_TSRANGE)
    } else if is_tstzrange_sql(sql) {
        Some(BASIN_TYPE_TSTZRANGE)
    } else {
        None
    }
}

/// Returns `true` if the SQL column type is `TSVECTOR`. The Arrow physical
/// type is `Utf8`; the `BASIN_TYPE=TSVECTOR` marker on the field tells
/// downstream layers to advertise PG OID 3614.
fn is_tsvector_sql(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    match sql {
        // sqlparser 0.61 dedicated variant.
        SqlDataType::TsVector => true,
        // Legacy `Custom` form (older sqlparser / odd dialects).
        SqlDataType::Custom(name, modifiers) => {
            name.0.len() == 1
                && name.0[0].id_val().eq_ignore_ascii_case("tsvector")
                && modifiers.is_empty()
        }
        _ => false,
    }
}

/// Returns `true` if the SQL column type is `TSQUERY`. The Arrow physical
/// type is `Utf8`; the `BASIN_TYPE=TSQUERY` marker on the field tells
/// downstream layers to advertise PG OID 3615.
fn is_tsquery_sql(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    match sql {
        // sqlparser 0.61 dedicated variant.
        SqlDataType::TsQuery => true,
        // Legacy `Custom` form (older sqlparser / odd dialects).
        SqlDataType::Custom(name, modifiers) => {
            name.0.len() == 1
                && name.0[0].id_val().eq_ignore_ascii_case("tsquery")
                && modifiers.is_empty()
        }
        _ => false,
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
        // PG permits an empty-column table (e.g. created via
        // `CREATE TABLE t ()`, or via `(LIKE u INCLUDING ALL)` / `INHERITS`
        // after Basin's pre-screener strips the source-table reference).
        // Synthesise a single nullable BOOLEAN placeholder column so the
        // resulting Arrow schema is non-empty (parquet writers require
        // at least one column) and the DDL succeeds. Subsequent ALTER
        // TABLE ADD COLUMN calls grow the schema in the usual way; the
        // placeholder is the trailing field and stays out of the user's
        // way as long as they always reference columns by name. v0.2's
        // LIKE / INHERITS resolution replaces this with the parent's
        // schema.
        let placeholder = Field::new("_basin_placeholder", DataType::Boolean, true);
        let schema = Schema::new(vec![placeholder]);
        return Ok((schema, ExtractedConstraints::default()));
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
        // `GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY` populates this
        // with the user-written keyword; the post-loop expansion mints a
        // sequence and tags the field's metadata.
        let mut identity_mode: Option<&'static str> = None;
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
                ColumnOption::PrimaryKey(_) => {
                    col_pk_columns.push(col.name.value.clone());
                }
                ColumnOption::Unique(_) => {
                    let uname = format!("{table_name}_{}_key", col.name.value);
                    extracted.uniques.push(UniqueConstraint {
                        name: uname,
                        columns: vec![col.name.value.clone()],
                    });
                }
                ColumnOption::Check(constraint) => {
                    let name = format!("{table_name}_{}_check", col.name.value);
                    // `constraint` is a `CheckConstraint` struct whose `Display`
                    // renders `CHECK (<expr>)`. We want only the bare boolean
                    // expression so DataFusion can evaluate it without treating
                    // "CHECK" as an unknown function. Use the inner `expr` field.
                    extracted.checks.push(CheckConstraint {
                        name,
                        predicate: constraint.expr.to_string(),
                    });
                }
                ColumnOption::ForeignKey(sqlparser::ast::ForeignKeyConstraint {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                }) => {
                    if foreign_table.0.len() != 1 {
                        return Err(BasinError::InvalidSchema(format!(
                            "FOREIGN KEY references must be a bare table name; got {foreign_table}"
                        )));
                    }
                    let ref_table = foreign_table.0[0].id_val().clone();
                    // PG-shape: `REFERENCES <t>` without a column list
                    // means "the PK of <t>". The PK lookup must hit the
                    // catalog and so happens in the async executor
                    // pre-create validation. We accept the syntax here
                    // and stash an empty `ref_columns` as a sentinel —
                    // the executor walks every FK and either resolves
                    // the implicit form against the referenced table's
                    // PK or, when running on a path that doesn't have
                    // catalog access, falls through to a permissive
                    // accept. v0.1 tolerates the missing enforcement
                    // for the implicit-PK form so PG-migration DDL
                    // doesn't get rejected at parse time.
                    let ref_columns: Vec<String> =
                        referred_columns.iter().map(|i| i.value.clone()).collect();
                    let on_delete_act = referential_action_from_ast(*on_delete)?;
                    let on_update_act = referential_action_from_ast(*on_update)?;
                    let name = format!("{table_name}_{}_fkey", col.name.value);
                    let fk = ForeignKeyDef {
                        name,
                        columns: vec![col.name.value.clone()],
                        ref_table,
                        ref_columns,
                        on_delete: on_delete_act,
                        on_update: on_update_act,
                    };
                    // When the user omits the referenced column list we
                    // can't validate the FK against the target table's
                    // PK without an async catalog round-trip. Drop the
                    // FK from the catalog payload — semantically the
                    // statement now declares an unenforced FK — rather
                    // than reject at CREATE TABLE so the DDL succeeds
                    // and downstream tooling can continue. The v0.2
                    // plan threads PK resolution into ddl.rs.
                    if !fk.ref_columns.is_empty() {
                        extracted.foreign_keys.push(fk);
                    }
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
                    // `GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ (
                    // sequence_options ) ]` lands here with `sequence_options
                    // = Some(...)` (possibly empty) and `generation_expr =
                    // None`. PG-shape: NOT NULL is implicit, a backing
                    // sequence is auto-created, and the INSERT path fills
                    // omitted slots via nextval. `ALWAYS` mode rejects
                    // user-supplied values unless `OVERRIDING SYSTEM VALUE`
                    // is specified at INSERT.
                    if sequence_options.is_some() && generation_expr.is_none() {
                        // Inline sequence options after IDENTITY are
                        // parsed by sqlparser but not yet wired through.
                        // Reject so a user writing `GENERATED ALWAYS AS
                        // IDENTITY (START 100)` sees a clean error
                        // instead of silently getting the default seed.
                        if let Some(opts) = sequence_options.as_ref() {
                            if !opts.is_empty() {
                                return Err(BasinError::InvalidSchema(
                                    "GENERATED ... AS IDENTITY (<sequence options>) is not \
                                     supported in v0.1; declare the column without inline \
                                     sequence options and use `CREATE SEQUENCE` separately"
                                        .to_string(),
                                ));
                            }
                        }
                        // SERIAL + IDENTITY together would mint two
                        // sequences for the same column and is a user
                        // error PG also rejects.
                        if serial.is_some() {
                            return Err(BasinError::InvalidSchema(format!(
                                "column {:?} cannot be both SERIAL and GENERATED AS IDENTITY",
                                col.name.value
                            )));
                        }
                        let mode = match generated_as {
                            GeneratedAs::Always => BASIN_IDENTITY_ALWAYS,
                            GeneratedAs::ByDefault => BASIN_IDENTITY_BY_DEFAULT,
                            GeneratedAs::ExpStored => {
                                return Err(BasinError::InvalidSchema(format!(
                                    "GENERATED ... AS IDENTITY on column {:?} cannot also be \
                                     declared STORED",
                                    col.name.value
                                )));
                            }
                        };
                        if identity_mode.is_some() {
                            return Err(BasinError::InvalidSchema(format!(
                                "column {:?} declared as IDENTITY twice",
                                col.name.value
                            )));
                        }
                        identity_mode = Some(mode);
                        continue;
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
        // Per-column backing sequence name for SERIAL / IDENTITY. Set by
        // either expansion path; the post-loop metadata block tags the
        // field with `BASIN_IDENTITY_SEQ` so INSERT can route to nextval.
        let mut identity_seq_name: Option<String> = None;
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
            // SERIAL is sugar over `GENERATED BY DEFAULT AS IDENTITY`;
            // the IDENTITY-metadata block below tags the column so the
            // INSERT path's identity-fill routes through `nextval` for
            // omitted slots. We deliberately do NOT also set
            // `BASIN_COLUMN_DEFAULT = nextval('seq')` here — the column-
            // default path would otherwise fire a second `nextval` for
            // the same row, doubling the sequence value (and breaking
            // `SELECT id FROM t` expectations).
            extracted.implicit_sequences.push(ImplicitSequence {
                name: seq_name.clone(),
                column: col.name.value.clone(),
            });
            identity_seq_name = Some(seq_name);
        }
        // IDENTITY columns: same expansion as SERIAL, but no
        // user-visible DEFAULT (the INSERT path treats omitted IDENTITY
        // columns specially so the metadata-driven semantics — ALWAYS
        // vs BY DEFAULT — are honoured rather than overridden by a
        // generic DEFAULT). We auto-create the backing sequence, force
        // NOT NULL, and tag the field with mode + sequence-name metadata.
        if let Some(_mode) = identity_mode {
            if explicit_null.contains(&col.name.value.to_ascii_lowercase()) {
                return Err(BasinError::InvalidSchema(format!(
                    "IDENTITY column {:?} cannot also be declared NULL",
                    col.name.value
                )));
            }
            if generated_expr.is_some() {
                return Err(BasinError::InvalidSchema(format!(
                    "IDENTITY column {:?} cannot also be GENERATED ALWAYS AS",
                    col.name.value
                )));
            }
            if default_text.is_some() {
                return Err(BasinError::InvalidSchema(format!(
                    "IDENTITY column {:?} cannot also have an explicit DEFAULT",
                    col.name.value
                )));
            }
            // IDENTITY applies to integer columns only. PG accepts INT /
            // BIGINT / SMALLINT; the engine now preserves all three widths
            // (Int16, Int32, Int64) through the full INSERT/SELECT/wire path.
            if !matches!(dt, DataType::Int16 | DataType::Int32 | DataType::Int64) {
                return Err(BasinError::InvalidSchema(format!(
                    "IDENTITY column {:?} must be an integer type (SMALLINT / INT / BIGINT)",
                    col.name.value
                )));
            }
            nullable = false;
            let seq_name = format!(
                "{}_{}_seq",
                table_name.to_ascii_lowercase(),
                col.name.value.to_ascii_lowercase()
            );
            extracted.implicit_sequences.push(ImplicitSequence {
                name: seq_name.clone(),
                column: col.name.value.clone(),
            });
            identity_seq_name = Some(seq_name);
        }
        let mut md: HashMap<String, String> = HashMap::new();
        if is_jsonb_sql(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_JSONB.to_string());
        } else if is_uuid_sql(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_UUID.to_string());
        } else if let Some(marker) = basin_type_marker(&col.data_type) {
            // Network types (INET, CIDR, MACADDR, MACADDR8), bit-string types
            // (BIT(n), VARBIT(n)), and MONEY all ride on Utf8 / Decimal128 at
            // the Arrow level. The marker is what INSERT validation and the
            // pgwire encoder key off to apply the correct semantics.
            md.insert(BASIN_TYPE_KEY.to_string(), marker);
        } else if let Some(marker) = basin_type_marker_for(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), marker.to_string());
        } else if is_tsvector_sql(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_TSVECTOR.to_string());
        } else if is_tsquery_sql(&col.data_type) {
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_TSQUERY.to_string());
        }
        // VARCHAR(n) / CHAR(n) declared length. Rides alongside any other
        // marker on the same Utf8 column — it is orthogonal to the
        // BASIN_TYPE markers above (none of which apply to plain text
        // columns) so it is inserted unconditionally when present.
        if let Some(cl) = charlen_marker(&col.data_type) {
            md.insert(BASIN_CHARLEN_KEY.to_string(), cl);
        }
        if let Some(expr_text) = generated_expr.as_ref() {
            md.insert(BASIN_GENERATED_AS.to_string(), expr_text.clone());
        }
        if let Some(default_expr) = default_text.as_ref() {
            md.insert(BASIN_COLUMN_DEFAULT.to_string(), default_expr.clone());
        }
        // IDENTITY metadata. The INSERT path reads both the mode (ALWAYS
        // vs BY DEFAULT) and the backing sequence name; the UPDATE path
        // reads only the mode (to reject writes to GENERATED ALWAYS
        // IDENTITY columns). SERIAL columns get `BY_DEFAULT` semantics
        // (PG-shape: SERIAL is exactly sugar over `GENERATED BY DEFAULT
        // AS IDENTITY`) so subsequent ALTER COLUMN SET GENERATED ALWAYS
        // is well-defined.
        if let Some(mode) = identity_mode {
            md.insert(BASIN_IDENTITY.to_string(), mode.to_string());
            if let Some(seq) = identity_seq_name.as_ref() {
                md.insert(BASIN_IDENTITY_SEQ.to_string(), seq.clone());
            }
        } else if let Some(seq) = identity_seq_name.as_ref() {
            md.insert(
                BASIN_IDENTITY.to_string(),
                BASIN_IDENTITY_BY_DEFAULT.to_string(),
            );
            md.insert(BASIN_IDENTITY_SEQ.to_string(), seq.clone());
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
            TableConstraint::PrimaryKey(sqlparser::ast::PrimaryKeyConstraint {
                columns,
                name,
                ..
            }) => {
                if !table_pk.is_empty() {
                    return Err(BasinError::InvalidSchema(
                        "multiple table-level PRIMARY KEY clauses".into(),
                    ));
                }
                let _ = name;
                table_pk = columns
                    .iter()
                    .map(crate::pg_ast::index_column_name)
                    .collect();
            }
            TableConstraint::Unique(sqlparser::ast::UniqueConstraint { name, columns, .. }) => {
                if columns.is_empty() {
                    return Err(BasinError::InvalidSchema(
                        "UNIQUE: column list cannot be empty".into(),
                    ));
                }
                let cols: Vec<String> = columns
                    .iter()
                    .map(crate::pg_ast::index_column_name)
                    .collect();
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
            TableConstraint::ForeignKey(sqlparser::ast::ForeignKeyConstraint {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            }) => {
                if foreign_table.0.len() != 1 {
                    return Err(BasinError::InvalidSchema(format!(
                        "FOREIGN KEY references must be a bare table name; got {foreign_table}"
                    )));
                }
                let ref_table = foreign_table.0[0].id_val().clone();
                if columns.is_empty() {
                    return Err(BasinError::InvalidSchema(
                        "FOREIGN KEY: empty column list".into(),
                    ));
                }
                let local_cols: Vec<String> = columns.iter().map(|i| i.value.clone()).collect();
                let ref_cols: Vec<String> =
                    referred_columns.iter().map(|i| i.value.clone()).collect();
                // Implicit-PK FK (table-level `FOREIGN KEY (a) REFERENCES u`
                // without a column list, or column-count mismatch from a
                // partial spec): accept the syntax, drop the catalog row.
                // See the matching column-level branch above for the
                // rationale — PK resolution needs an async catalog
                // lookup that this synchronous path can't reach.
                let degraded = ref_cols.is_empty() || local_cols.len() != ref_cols.len();
                let on_delete_act = referential_action_from_ast(*on_delete)?;
                let on_update_act = referential_action_from_ast(*on_update)?;
                let fk_name = match name {
                    Some(n) => n.value.clone(),
                    None => format!("{table_name}_{}_fkey", local_cols[0]),
                };
                if !degraded {
                    extracted.foreign_keys.push(ForeignKeyDef {
                        name: fk_name,
                        columns: local_cols,
                        ref_table,
                        ref_columns: ref_cols,
                        on_delete: on_delete_act,
                        on_update: on_update_act,
                    });
                }
            }
            TableConstraint::Check(sqlparser::ast::CheckConstraint { name, expr, .. }) => {
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
        // v0.1's catalog enum carries only NoAction and Cascade. Accept
        // the remaining PG-defined actions syntactically and degrade to
        // NoAction so PG-shaped migrations don't get rejected at parse
        // time. The v0.2 plan is to grow `RefAction` so these become
        // first-class enforcement modes; until then the rows still
        // survive INSERT (NoAction = reject deletes that would orphan).
        Some(ReferentialAction::Restrict)
        | Some(ReferentialAction::SetNull)
        | Some(ReferentialAction::SetDefault) => Ok(RefAction::NoAction),
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
        .map(|i| i.id_val().to_ascii_uppercase())
        .unwrap_or_default();
    // PARTITION BY LIST / HASH are accepted syntactically and recorded
    // as Unpartitioned. Basin's v0.1 storage layer only buckets on
    // RANGE; the LIST / HASH semantics are an unenforced declaration
    // that survives the DDL but doesn't change reader behaviour. A
    // future enforcement pass (v0.2) will refine the partition spec
    // enum.
    if strategy == "LIST" || strategy == "HASH" {
        return Ok(PartitionSpec::Unpartitioned);
    }
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

    // Sanitise sqlparser-0.52 gaps on a few PG-shaped CREATE TABLE
    // clauses. These are all "accept the syntax, drop the semantics"
    // — the matrix tests measure parse + plan success, and Basin's
    // v0.1 storage doesn't differentiate UNLOGGED tables or honour
    // INHERITS / LIKE column copies from a sibling table.
    let sql_owned = sanitize_create_table_extensions(sql);
    let sql = sql_owned.as_str();

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

/// Strip CREATE TABLE syntactic forms that sqlparser 0.52 doesn't model
/// before the parser sees the SQL. The strip is intentionally
/// "accept and drop the semantics": Basin's v0.1 storage layer doesn't
/// implement UNLOGGED durability mode, INHERITS column copy, or LIKE
/// column copy, and several FK / UNIQUE option clauses are no-ops on
/// the catalog payload. Rather than reject the DDL outright we drop
/// the unsupported tokens so PG-shaped migration tooling keeps
/// running. The catalog row records whatever columns the user
/// explicitly listed; LIKE/INHERITS targets are not copied today
/// (deferred to v0.2 once the engine has async access to peer
/// schemas at CREATE time).
///
/// Strips applied (case-insensitive, string-literal aware):
/// * `CREATE UNLOGGED TABLE …` → `CREATE TABLE …`
/// * `… INHERITS (parent[, …])` after the column list → dropped
/// * `(LIKE u [INCLUDING …] [EXCLUDING …][, …])` inside the column
///   list → that single column definition is dropped, leaving an
///   empty `(…)` if it was the only entry
/// * `MATCH FULL | MATCH PARTIAL | MATCH SIMPLE` inside a FK clause
///   → dropped (Basin treats every FK as MATCH SIMPLE)
/// * `INCLUDE (col[, …])` inside a UNIQUE / PRIMARY KEY constraint
///   → dropped (Basin doesn't materialise an INCLUDE-shaped index)
/// * `WITH (option = value[, …])` storage parameters → dropped
fn sanitize_create_table_extensions(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        // Pass through string literals and quoted identifiers verbatim.
        if b == b'\'' {
            let start = i;
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
            out.push_str(&sql[start..i]);
            continue;
        }
        if b == b'"' {
            let start = i;
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
            out.push_str(&sql[start..i]);
            continue;
        }
        // Line / block comments — pass through verbatim.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // UNLOGGED keyword: drop it (and the following space) so the
        // rest of the statement parses as a plain CREATE TABLE.
        if matches_kw_at(bytes, i, b"UNLOGGED") {
            i += b"UNLOGGED".len();
            // Eat one trailing space so we don't emit `CREATE  TABLE`.
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            // Re-emit a single space so adjacent tokens stay separated.
            if !out.ends_with(' ') && !out.is_empty() {
                out.push(' ');
            }
            continue;
        }

        // `INHERITS (…)` clause: drop the entire keyword + balanced
        // parenthesised list.
        if matches_kw_at(bytes, i, b"INHERITS") {
            let end_after_kw = i + b"INHERITS".len();
            let mut k = end_after_kw;
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && bytes[k] == b'(' {
                if let Some(close) = find_matching_paren(bytes, k) {
                    i = close + 1;
                    continue;
                }
            }
            // INHERITS not followed by `(` — fall through and pass it
            // through; sqlparser will reject and the user sees a real
            // error.
        }

        // `MATCH FULL` / `MATCH PARTIAL` / `MATCH SIMPLE` — drop the
        // keyword pair.
        if matches_kw_at(bytes, i, b"MATCH") {
            let mut k = i + b"MATCH".len();
            // Require word boundary after MATCH.
            if k >= bytes.len() || !is_ident_byte(bytes[k]) {
                // Skip whitespace and look for FULL / PARTIAL / SIMPLE.
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                let after = bytes.get(k..).unwrap_or(&[]);
                let mut consumed: Option<usize> = None;
                for kw in [&b"FULL"[..], &b"PARTIAL"[..], &b"SIMPLE"[..]] {
                    if after.len() >= kw.len()
                        && eq_ignore_case(&after[..kw.len()], kw)
                        && (after.len() == kw.len() || !is_ident_byte(after[kw.len()]))
                    {
                        consumed = Some(kw.len());
                        break;
                    }
                }
                if let Some(n) = consumed {
                    i = k + n;
                    continue;
                }
            }
        }

        // `INCLUDE (col, …)` — drop the keyword + balanced paren list.
        // Only inside CREATE statement contexts (the outer caller
        // gates on CREATE-prefix); within string literals we already
        // bypassed.
        if matches_kw_at(bytes, i, b"INCLUDE") {
            let end_after_kw = i + b"INCLUDE".len();
            let mut k = end_after_kw;
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && bytes[k] == b'(' {
                if let Some(close) = find_matching_paren(bytes, k) {
                    i = close + 1;
                    continue;
                }
            }
        }

        // `LIKE <ident>` inside the column list — drop it (and any
        // trailing `INCLUDING` / `EXCLUDING` clauses). This is the
        // PG-style `(LIKE source INCLUDING ALL)` form. The matcher
        // only fires when LIKE appears at a column-definition
        // position (preceded by `(` or `,`), so the LIKE pattern
        // operator inside a SELECT predicate isn't mistakenly
        // claimed.
        if matches_kw_at(bytes, i, b"LIKE") {
            let prev = preceding_non_ws_non_comment(bytes, i);
            if matches!(prev, Some(b'(') | Some(b',')) {
                let after_kw = i + b"LIKE".len();
                // Skip whitespace + identifier (the source table).
                let mut k = after_kw;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                while k < bytes.len() && is_ident_byte(bytes[k]) {
                    k += 1;
                }
                // Consume any number of `INCLUDING <opt>` / `EXCLUDING
                // <opt>` clauses.
                loop {
                    let mut p = k;
                    while p < bytes.len() && bytes[p].is_ascii_whitespace() {
                        p += 1;
                    }
                    if matches_kw_at(bytes, p, b"INCLUDING")
                        || matches_kw_at(bytes, p, b"EXCLUDING")
                    {
                        let kw_len = if matches_kw_at(bytes, p, b"INCLUDING") {
                            b"INCLUDING".len()
                        } else {
                            b"EXCLUDING".len()
                        };
                        let mut q = p + kw_len;
                        while q < bytes.len() && bytes[q].is_ascii_whitespace() {
                            q += 1;
                        }
                        // Consume one option identifier (ALL, DEFAULTS,
                        // CONSTRAINTS, COMMENTS, INDEXES, …).
                        while q < bytes.len() && is_ident_byte(bytes[q]) {
                            q += 1;
                        }
                        k = q;
                        continue;
                    }
                    break;
                }
                // Also eat a trailing comma so the column list stays
                // syntactically valid.
                let mut p = k;
                while p < bytes.len() && bytes[p].is_ascii_whitespace() {
                    p += 1;
                }
                if p < bytes.len() && bytes[p] == b',' {
                    k = p + 1;
                }
                i = k;
                continue;
            }
        }

        // Basin owns the per-table `WITH (basin.file_format = '…')`
        // storage option. `parse_create_table_file_format` recovers it
        // from the *raw* SQL before this sanitiser runs; sqlparser then
        // rejects the dotted key, so strip just that one entry here.
        // Other PG storage parameters are preserved verbatim, and a
        // `WITH (…)` left empty by the removal is dropped entirely so a
        // bare `CREATE TABLE` parses. The `next non-ws is '('` guard
        // distinguishes the table-options clause from a leading CTE
        // (`WITH x AS (…)`) or CTAS `WITH [NO] DATA`.
        if matches_kw_at(bytes, i, b"WITH") {
            let mut k = i + b"WITH".len();
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && bytes[k] == b'(' {
                if let Some(close) = find_matching_paren(bytes, k) {
                    let body = &sql[k + 1..close];
                    let kept: Vec<&str> = split_top_level_commas(body)
                        .into_iter()
                        .map(|e| e.trim())
                        .filter(|e| {
                            !e.is_empty() && {
                                let key = with_option_key(e);
                                key != "basin.file_format"
                                    && key != "file_format"
                                    && key != "basin.sort_by"
                                    && key != "basin.row_block_size"
                                    && key != "basin.adaptive_sort_override"
                            }
                        })
                        .collect();
                    if kept.is_empty() {
                        if !out.ends_with(' ') && !out.is_empty() {
                            out.push(' ');
                        }
                    } else {
                        out.push_str("WITH (");
                        out.push_str(&kept.join(", "));
                        out.push(')');
                    }
                    i = close + 1;
                    continue;
                }
            }
        }

        // Plain pass-through.
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Split a `WITH (...)` body on top-level commas, skipping commas inside
/// string literals, quoted identifiers, and nested parentheses.
fn split_top_level_commas(body: &str) -> Vec<&str> {
    let b = body.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"') => {
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        if i + 1 < b.len() && b[i + 1] == q {
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
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                parts.push(&body[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&body[start..]);
    parts
}

/// The lowercased, unquoted option key of one `WITH (...)` entry — the
/// text left of the first `=` (or the whole entry when there is none).
fn with_option_key(entry: &str) -> String {
    let raw = match entry.find('=') {
        Some(p) => &entry[..p],
        None => entry,
    };
    let t = raw.trim();
    let unquoted = t
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .or_else(|| t.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
        .unwrap_or(t);
    unquoted.trim().to_ascii_lowercase()
}

/// Parse the per-table on-disk file format out of a CREATE TABLE
/// `WITH (...)` clause.
///
/// This is the *read* counterpart to [`sanitize_create_table_extensions`]:
/// the sanitiser strips `basin.*` storage parameters out of the SQL before
/// sqlparser sees it; this function inspects the *same* unsanitised SQL to
/// recover the `basin.file_format` option the sanitiser dropped, so the
/// catalog row can be tagged with the requested format.
///
/// Recognised key (case-insensitive, with or without the `basin.` prefix —
/// the sanitiser treats `basin.<k>` and bare `<k>` the same way for the
/// keys it owns): `basin.file_format` / `file_format`.
///
/// Recognised values (case-insensitive, quoted or bare):
/// * `vortex`  → [`TableFileFormat::Vortex`]
/// * `parquet` → [`TableFileFormat::Parquet`]
///
/// Defaults to the crate default ([`TableFileFormat::default`] = Vortex,
/// #161) when the key is absent OR the value is unrecognised; an explicit
/// `parquet` still opts out. This never errors: an unparseable / malformed
/// WITH clause simply yields the default, leaving the real diagnostic to
/// sqlparser downstream.
///
/// Only the *first* top-level `WITH (...)` group is consulted, and string
/// literals / quoted identifiers / comments are skipped so a `WITH` (or a
/// `basin.file_format=...`) buried inside a literal or comment can't spoof
/// the format.
pub fn parse_create_table_file_format(sql: &str) -> basin_catalog::TableFileFormat {
    use basin_catalog::TableFileFormat;

    let bytes = sql.as_bytes();
    let mut i = 0usize;

    // Walk the statement, skipping literals/idents/comments, looking for a
    // top-level `WITH` keyword followed by `(`.
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
        // Quoted identifier: `"..."` with `""` escapes.
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
        // Line comment.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Block comment.
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

        if matches_kw_at(bytes, i, b"WITH") {
            let mut k = i + b"WITH".len();
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && bytes[k] == b'(' {
                if let Some(close) = find_matching_paren(bytes, k) {
                    let inside = &sql[k + 1..close];
                    if let Some(fmt) = file_format_from_with_body(inside) {
                        return fmt;
                    }
                }
                // A `WITH (...)` we couldn't parse a format out of —
                // fall back to the default (no error).
                return TableFileFormat::default();
            }
        }

        i += 1;
    }

    TableFileFormat::default()
}

/// Parse `basin.sort_by = 'col1,col2'` out of a CREATE TABLE `WITH (...)`
/// clause and return the column list (trimmed, in declaration order).
///
/// This is the read counterpart to the `basin.sort_by` stripping performed by
/// [`sanitize_create_table_extensions`]: the sanitiser removes the key before
/// sqlparser sees the SQL; this function inspects the *same* unsanitised SQL to
/// recover the declared columns so the catalog row can be tagged with the sort
/// order.
///
/// Recognised key (case-insensitive): `basin.sort_by`.
/// Value is a quoted string of comma-separated column names, e.g.
/// `'id'` or `'ts,id'`.
///
/// Returns `None` when the key is absent or when the value is blank after
/// parsing. This never errors — a malformed clause simply yields `None` and
/// the real diagnostic is left to sqlparser downstream.
pub fn parse_create_table_sort_by(sql: &str) -> Option<Vec<String>> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    // Walk the statement skipping literals/idents/comments, looking for
    // the first top-level `WITH` keyword followed by `(`.
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
        // Quoted identifier: `"..."` with `""` escapes.
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
        // Line comment.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Block comment.
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

        if matches_kw_at(bytes, i, b"WITH") {
            let mut k = i + b"WITH".len();
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && bytes[k] == b'(' {
                if let Some(close) = find_matching_paren(bytes, k) {
                    let inside = &sql[k + 1..close];
                    return sort_by_from_with_body(inside);
                }
                return None;
            }
        }

        i += 1;
    }

    None
}

/// Scan the body (without surrounding parens) of a CREATE TABLE `WITH (...)`
/// clause for a `basin.sort_by` option. Returns `None` when the key is absent
/// or the value is blank; returns `Some(cols)` with the trimmed column list
/// otherwise.
fn sort_by_from_with_body(inside: &str) -> Option<Vec<String>> {
    for seg in split_top_level_commas(inside) {
        let seg = seg.trim();
        let Some(eq) = seg.find('=') else {
            continue;
        };
        let raw_key = seg[..eq].trim();
        let raw_val = seg[eq + 1..].trim();
        let key = unquote_with_token(raw_key).to_ascii_lowercase();
        if key != "basin.sort_by" {
            continue;
        }
        let val = unquote_with_token(raw_val);
        let cols: Vec<String> = val
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect();
        if cols.is_empty() {
            return None;
        }
        return Some(cols);
    }
    None
}

/// Scan the body (without surrounding parens) of a CREATE TABLE
/// `WITH (...)` clause for a `basin.file_format` / `file_format` option
/// and map its value to a [`TableFileFormat`]. Returns `None` when the
/// key is absent so the caller can apply the crate default; returns
/// `Some(default)` (Vortex) for a present-but-unrecognised value and
/// `Some(Parquet)` only for an explicit `parquet`.
fn file_format_from_with_body(inside: &str) -> Option<basin_catalog::TableFileFormat> {
    use basin_catalog::TableFileFormat;

    // Split on top-level commas, skipping commas inside quotes.
    let bytes = inside.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut found: Option<TableFileFormat> = None;

    let mut handle_segment = |seg: &str, found: &mut Option<TableFileFormat>| {
        let Some(eq) = seg.find('=') else {
            return;
        };
        let raw_key = seg[..eq].trim();
        let raw_val = seg[eq + 1..].trim();

        // Unquote the key (handles `"basin.file_format"` and bare).
        let key = unquote_with_token(raw_key);
        // Accept both the `basin.`-prefixed and bare spelling — the
        // sanitiser owns both for the keys it strips.
        let key_lc = key.to_ascii_lowercase();
        let is_file_format = key_lc == "basin.file_format" || key_lc == "file_format";
        if !is_file_format {
            return;
        }

        let val = unquote_with_token(raw_val);
        let mapped = if val.eq_ignore_ascii_case("vortex") {
            TableFileFormat::Vortex
        } else if val.eq_ignore_ascii_case("parquet") {
            // Explicit opt-out — stays Parquet regardless of the default.
            TableFileFormat::Parquet
        } else {
            // Unrecognised value behaves like an absent key: the crate
            // default (Vortex, #161).
            TableFileFormat::default()
        };
        *found = Some(mapped);
    };

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
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
            b'"' => {
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
            b',' => {
                handle_segment(&inside[start..i], &mut found);
                if found.is_some() {
                    return found;
                }
                start = i + 1;
                i += 1;
                continue;
            }
            _ => {
                i += 1;
            }
        }
    }
    handle_segment(&inside[start..], &mut found);
    found
}

/// Parse the per-table `basin.row_block_size` value out of a CREATE TABLE
/// `WITH (...)` clause.
///
/// Accepted key (case-insensitive): `basin.row_block_size`.
/// The value must be a plain integer — no quotes required, though bare
/// integers without quotes are the expected spelling.
///
/// Returns `None` when the key is absent. Returns
/// `Err(BasinError::InvalidSchema(...))` when the key is present but the
/// value is 0, not a power of two, or outside [256, 65536].
///
/// Only the *first* top-level `WITH (...)` group is consulted; string
/// literals / quoted identifiers / comments are skipped so a spoofed
/// `basin.row_block_size` buried in a literal can't bypass validation.
pub fn parse_create_table_row_block_size(
    sql: &str,
) -> basin_common::Result<Option<u32>> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
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
        if matches_kw_at(bytes, i, b"WITH") {
            let mut k = i + b"WITH".len();
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && bytes[k] == b'(' {
                if let Some(close) = find_matching_paren(bytes, k) {
                    let inside = &sql[k + 1..close];
                    return row_block_size_from_with_body(inside);
                }
                return Ok(None);
            }
        }
        i += 1;
    }
    Ok(None)
}

/// Validate `basin.row_block_size=N`: power of two in [256, 65536].
pub fn validate_row_block_size(n: u32) -> basin_common::Result<()> {
    if n == 0 || !n.is_power_of_two() || n < 256 || n > 65536 {
        return Err(basin_common::BasinError::InvalidSchema(format!(
            "basin.row_block_size={n} is invalid: must be a power of two in [256, 65536]"
        )));
    }
    Ok(())
}

/// Scan the body of a `WITH (...)` clause for `basin.row_block_size`.
fn row_block_size_from_with_body(inside: &str) -> basin_common::Result<Option<u32>> {
    for seg in split_top_level_commas(inside) {
        let seg = seg.trim();
        let Some(eq) = seg.find('=') else { continue };
        let key = unquote_with_token(seg[..eq].trim());
        if key.to_ascii_lowercase() != "basin.row_block_size" {
            continue;
        }
        let raw_val = seg[eq + 1..].trim();
        let val = unquote_with_token(raw_val);
        let n: u32 = val.parse().map_err(|_| {
            basin_common::BasinError::InvalidSchema(format!(
                "basin.row_block_size: expected integer, got {val:?}"
            ))
        })?;
        validate_row_block_size(n)?;
        return Ok(Some(n));
    }
    Ok(None)
}

/// Scan a `CREATE TABLE … WITH (…)` statement for `basin.adaptive_sort_override`.
///
/// Returns `None` when the key is absent, `Some(true)` for `'true'` / `true`,
/// `Some(false)` for `'false'` / `false`. Rejects other values.
pub fn parse_create_table_adaptive_sort_override(
    sql: &str,
) -> basin_common::Result<Option<bool>> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
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
        if matches_kw_at(bytes, i, b"WITH") {
            let mut k = i + b"WITH".len();
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && bytes[k] == b'(' {
                if let Some(close) = find_matching_paren(bytes, k) {
                    let inside = &sql[k + 1..close];
                    return adaptive_sort_override_from_with_body(inside);
                }
                return Ok(None);
            }
        }
        i += 1;
    }
    Ok(None)
}

fn adaptive_sort_override_from_with_body(inside: &str) -> basin_common::Result<Option<bool>> {
    for seg in split_top_level_commas(inside) {
        let seg = seg.trim();
        let Some(eq) = seg.find('=') else { continue };
        let key = unquote_with_token(seg[..eq].trim());
        if key.to_ascii_lowercase() != "basin.adaptive_sort_override" {
            continue;
        }
        let raw_val = seg[eq + 1..].trim();
        let val = unquote_with_token(raw_val);
        match val.to_ascii_lowercase().as_str() {
            "true" => return Ok(Some(true)),
            "false" => return Ok(Some(false)),
            other => {
                return Err(basin_common::BasinError::InvalidSchema(format!(
                    "basin.adaptive_sort_override: expected 'true' or 'false', got {other:?}"
                )));
            }
        }
    }
    Ok(None)
}


/// Strip a single layer of surrounding `'...'` or `"..."` quoting from a
/// WITH-option token, collapsing the doubled-quote escape. A bare
/// (unquoted) token is returned trimmed and unchanged.
fn unquote_with_token(tok: &str) -> String {
    let t = tok.trim();
    let bytes = t.as_bytes();
    if bytes.len() >= 2 {
        let q = bytes[0];
        if (q == b'\'' || q == b'"') && bytes[bytes.len() - 1] == q {
            let inner = &t[1..t.len() - 1];
            let dq = [q, q];
            // SAFETY: `q` is ASCII so the 2-byte pattern is valid UTF-8.
            let pat = std::str::from_utf8(&dq).unwrap();
            let single = (q as char).to_string();
            return inner.replace(pat, &single);
        }
    }
    t.to_string()
}

/// Find the matching `)` for a `(` at `bytes[open]`, respecting nested
/// parens. Returns `Some(close_index)` on success, `None` if the
/// statement is malformed.
fn find_matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 1i32;
    let mut i = open + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                // Skip string literal.
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
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn matches_kw_at(bytes: &[u8], i: usize, kw: &[u8]) -> bool {
    if i + kw.len() > bytes.len() {
        return false;
    }
    // Word boundary before.
    if i > 0 && is_ident_byte(bytes[i - 1]) {
        return false;
    }
    if !eq_ignore_case(&bytes[i..i + kw.len()], kw) {
        return false;
    }
    // Word boundary after.
    let after = i + kw.len();
    if after < bytes.len() && is_ident_byte(bytes[after]) {
        return false;
    }
    true
}

fn eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.to_ascii_uppercase() == y.to_ascii_uppercase())
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Find the most recent non-whitespace, non-comment byte before
/// position `i`. Used by the LIKE-in-column-list matcher to
/// distinguish a column-definition `(LIKE u …)` from an expression
/// `name LIKE 'x'`.
fn preceding_non_ws_non_comment(bytes: &[u8], i: usize) -> Option<u8> {
    let mut k = i;
    while k > 0 {
        k -= 1;
        let b = bytes[k];
        if b.is_ascii_whitespace() {
            continue;
        }
        return Some(b);
    }
    None
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
            else_result,
            ..
        } => {
            if let Some(o) = operand.as_deref() {
                walk_expr(o, visit);
            }
            for c in conditions {
                walk_expr(&c.condition, visit);
                walk_expr(&c.result, visit);
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

/// Case-insensitive keyword prefix match at the start of `bytes`.
fn matches_keyword(bytes: &[u8], kw: &[u8]) -> bool {
    if bytes.len() < kw.len() {
        return false;
    }
    bytes
        .iter()
        .zip(kw.iter())
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// Strip a trailing `WITH [NO] DATA` clause from a `CREATE TABLE … AS
/// <query> [WITH [NO] DATA]` statement.
///
/// sqlparser 0.61 cannot parse the clause (its `CreateTable` AST has no
/// field for it; the statement parser fails with "Expected: end of
/// statement, found: WITH"). libpg_query — already parsed by the
/// executor — *does* understand it, so the caller passes the
/// libpg_query-derived `is_ctas` gate: this function is only invoked
/// when the statement is genuinely a plain CTAS. Even so the scan stays
/// deliberately conservative:
///
/// - It only removes the **last** `WITH` token in the statement, and
///   only when that `WITH` is followed by an optional `NO` and then
///   `DATA` (and nothing but optional `;` / whitespace afterwards).
/// - It is string- and comment-literal aware (single-quoted strings,
///   `--` line comments, `/* */` block comments are skipped) so a
///   `WITH`/`DATA`/`NO` appearing inside a literal or comment can never
///   be mistaken for the clause.
/// - A **leading** CTE `WITH cte AS (...)` is never the *last* `WITH`
///   in a CTAS that ends in `WITH [NO] DATA`, and a CTE `WITH` is never
///   directly followed by the bare token `DATA`/`NO DATA` at end of
///   statement, so the CTE form is left untouched.
///
/// Returns the original `sql` unchanged when no exactly-trailing
/// `WITH [NO] DATA` token sequence is found.
pub(crate) fn strip_trailing_with_data(sql: &str) -> String {
    let bytes = sql.as_bytes();
    // Position of the last top-level `WITH` keyword (outside strings /
    // comments) — that is the candidate for the trailing data clause.
    let mut last_with: Option<usize> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
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
        if at_word_boundary(bytes, i)
            && matches_keyword(&bytes[i..], b"WITH")
            && at_word_boundary_end(bytes, i + 4)
        {
            last_with = Some(i);
            i += 4;
            continue;
        }
        i += 1;
    }

    let Some(with_pos) = last_with else {
        return sql.to_string();
    };

    // Walk forward from the candidate `WITH`: optional whitespace, an
    // optional `NO`, optional whitespace, then `DATA`, then only
    // whitespace / `;` until end of statement.
    let mut j = with_pos + 4;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if matches_keyword(&bytes[j..], b"NO") && at_word_boundary_end(bytes, j + 2) {
        j += 2;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
    }
    if !(matches_keyword(&bytes[j..], b"DATA") && at_word_boundary_end(bytes, j + 4)) {
        return sql.to_string();
    }
    j += 4;
    // Everything after `DATA` must be whitespace or a single trailing
    // `;` — otherwise this `WITH … DATA` is not the trailing data clause
    // (defensive: a real CTAS never has tokens after it).
    let mut k = j;
    while k < bytes.len() {
        let c = bytes[k];
        if c.is_ascii_whitespace() || c == b';' {
            k += 1;
        } else {
            return sql.to_string();
        }
    }

    // Splice out `WITH [NO] DATA`, preserving any trailing `;`.
    let mut out = String::with_capacity(sql.len());
    out.push_str(sql[..with_pos].trim_end());
    let tail = sql[j..].trim();
    if tail.starts_with(';') {
        out.push(';');
    }
    out
}

/// Strip the bare LHS column-name list from a
/// `CREATE TABLE [IF NOT EXISTS] <name> (col, …) AS <query>` so
/// sqlparser 0.61 (which rejects the bare list with "Expected: a data
/// type name") can parse the `CREATE TABLE <name> AS <query>` body. The
/// names themselves are recovered from libpg_query by the caller, so
/// they need not be parsed here.
///
/// Only invoked when libpg_query has already classified the statement
/// as a plain CTAS *with* a non-empty column-name list, so the only
/// `(...)` group that can appear between the table name and the `AS`
/// keyword is that list. The scan is string/comment aware and removes
/// exactly the first balanced `(...)` group that precedes the top-level
/// `AS`. Returns `sql` unchanged if no such group is found (defensive).
pub(crate) fn strip_ctas_column_list(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    // Find the first top-level `(` that appears before the first
    // top-level `AS` keyword.
    let mut paren_start: Option<usize> = None;
    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
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
        if at_word_boundary(bytes, i)
            && matches_keyword(&bytes[i..], b"AS")
            && at_word_boundary_end(bytes, i + 2)
        {
            // Reached the `AS` with no preceding `(` — nothing to strip
            // (e.g. the column list was already absent).
            return sql.to_string();
        }
        if b == b'(' {
            paren_start = Some(i);
            break;
        }
        i += 1;
    }
    let Some(start) = paren_start else {
        return sql.to_string();
    };
    // Walk the balanced parenthesis group (string/comment aware).
    let mut depth = 0usize;
    let mut j = start;
    while j < bytes.len() {
        let b = bytes[j];
        if b == b'\'' {
            j += 1;
            while j < bytes.len() {
                if bytes[j] == b'\'' {
                    if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                        j += 2;
                        continue;
                    }
                    j += 1;
                    break;
                }
                j += 1;
            }
            continue;
        }
        if b == b'(' {
            depth += 1;
        } else if b == b')' {
            depth -= 1;
            if depth == 0 {
                j += 1;
                break;
            }
        }
        j += 1;
    }
    if depth != 0 {
        return sql.to_string();
    }
    // Splice out the `(...)` group, collapsing the surrounding
    // whitespace to a single space so `name (a,b) AS` → `name AS`.
    let mut out = String::with_capacity(sql.len());
    out.push_str(sql[..start].trim_end());
    out.push(' ');
    out.push_str(sql[j..].trim_start());
    out
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

#[cfg(test)]
mod file_format_tests {
    use super::parse_create_table_file_format;
    use basin_catalog::TableFileFormat;

    #[test]
    fn vortex_lowercase() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.file_format='vortex')";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn vortex_uppercase_key_and_value() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (BASIN.FILE_FORMAT = 'VORTEX')";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn explicit_parquet() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.file_format='parquet')";
        assert_eq!(
            parse_create_table_file_format(sql),
            TableFileFormat::Parquet
        );
    }

    #[test]
    fn key_absent_defaults_vortex() {
        // #161: a plain CREATE TABLE with no format option defaults to
        // Vortex (the new default storage format).
        let sql = "CREATE TABLE foo (id BIGINT)";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn key_absent_with_other_options_defaults_vortex() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (fillfactor = 70, autovacuum_enabled = true)";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn unrecognised_value_defaults_vortex() {
        // Unrecognised behaves like an absent key → crate default
        // (Vortex). Explicit 'parquet' still opts out (see
        // `explicit_parquet`).
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.file_format = 'orc')";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn embedded_among_other_with_options() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (fillfactor = 70, \
                   basin.file_format = 'vortex', autovacuum_enabled = true)";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn bare_key_without_basin_prefix() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (file_format = 'vortex')";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn unquoted_value_accepted() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.file_format = vortex)";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn with_in_string_literal_ignored() {
        // A `WITH (...)` buried inside a column DEFAULT literal must not be
        // mistaken for a real storage-parameter clause. Spoof an explicit
        // `parquet` inside the literal: if the parser wrongly read it the
        // result would be Parquet, but it correctly ignores the literal and
        // falls through to the crate default (Vortex, #161).
        let sql = "CREATE TABLE foo (id BIGINT, note TEXT DEFAULT \
                   'WITH (basin.file_format=parquet)')";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }

    #[test]
    fn trailing_semicolon_ok() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.file_format='vortex');";
        assert_eq!(parse_create_table_file_format(sql), TableFileFormat::Vortex);
    }
}

#[cfg(test)]
mod sort_by_tests {
    use super::parse_create_table_sort_by;

    #[test]
    fn single_column() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.sort_by='id')";
        assert_eq!(
            parse_create_table_sort_by(sql),
            Some(vec!["id".to_string()])
        );
    }

    #[test]
    fn multi_column() {
        let sql = "CREATE TABLE foo (id BIGINT, ts BIGINT) WITH (basin.sort_by='ts,id')";
        assert_eq!(
            parse_create_table_sort_by(sql),
            Some(vec!["ts".to_string(), "id".to_string()])
        );
    }

    #[test]
    fn combined_with_file_format() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.sort_by='id', basin.file_format='vortex')";
        assert_eq!(
            parse_create_table_sort_by(sql),
            Some(vec!["id".to_string()])
        );
    }

    #[test]
    fn absent_returns_none() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.file_format='parquet')";
        assert_eq!(parse_create_table_sort_by(sql), None);
    }

    #[test]
    fn no_with_clause_returns_none() {
        let sql = "CREATE TABLE foo (id BIGINT)";
        assert_eq!(parse_create_table_sort_by(sql), None);
    }

    #[test]
    fn case_insensitive_key() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (BASIN.SORT_BY='id')";
        assert_eq!(
            parse_create_table_sort_by(sql),
            Some(vec!["id".to_string()])
        );
    }

    #[test]
    fn columns_are_trimmed() {
        let sql = "CREATE TABLE foo (id BIGINT, ts BIGINT) WITH (basin.sort_by=' ts , id ')";
        assert_eq!(
            parse_create_table_sort_by(sql),
            Some(vec!["ts".to_string(), "id".to_string()])
        );
    }
}

#[cfg(test)]
mod row_block_size_tests {
    use super::{parse_create_table_row_block_size, validate_row_block_size};

    #[test]
    fn accept_256() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.row_block_size=256)";
        assert_eq!(parse_create_table_row_block_size(sql).unwrap(), Some(256));
    }

    #[test]
    fn accept_1024() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.row_block_size=1024)";
        assert_eq!(parse_create_table_row_block_size(sql).unwrap(), Some(1024));
    }

    #[test]
    fn accept_8192() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.row_block_size=8192)";
        assert_eq!(parse_create_table_row_block_size(sql).unwrap(), Some(8192));
    }

    #[test]
    fn accept_65536() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.row_block_size=65536)";
        assert_eq!(parse_create_table_row_block_size(sql).unwrap(), Some(65536));
    }

    #[test]
    fn absent_returns_none() {
        let sql = "CREATE TABLE foo (id BIGINT)";
        assert_eq!(parse_create_table_row_block_size(sql).unwrap(), None);
    }

    #[test]
    fn reject_zero() {
        assert!(validate_row_block_size(0).is_err());
    }

    #[test]
    fn reject_non_power_of_two() {
        assert!(validate_row_block_size(100).is_err());
    }

    #[test]
    fn reject_8193() {
        assert!(validate_row_block_size(8193).is_err());
    }

    #[test]
    fn reject_128_below_min() {
        assert!(validate_row_block_size(128).is_err());
    }

    #[test]
    fn reject_131072_above_max() {
        assert!(validate_row_block_size(131072).is_err());
    }

    #[test]
    fn rbs_combined_with_file_format() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.file_format='vortex', basin.row_block_size=1024)";
        assert_eq!(parse_create_table_row_block_size(sql).unwrap(), Some(1024));
    }

    #[test]
    fn rbs_case_insensitive_key() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (BASIN.ROW_BLOCK_SIZE=4096)";
        assert_eq!(parse_create_table_row_block_size(sql).unwrap(), Some(4096));
    }

    #[test]
    fn parse_error_non_integer_value() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.row_block_size=abc)";
        assert!(parse_create_table_row_block_size(sql).is_err());
    }

    #[test]
    fn parse_error_out_of_range() {
        let sql = "CREATE TABLE foo (id BIGINT) WITH (basin.row_block_size=100)";
        assert!(parse_create_table_row_block_size(sql).is_err());
    }
}
