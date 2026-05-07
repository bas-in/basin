//! CREATE TABLE: sqlparser AST → Arrow [`Schema`].

use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::PartitionSpec;
use basin_common::{BasinError, Result};
use sqlparser::ast::{ColumnDef, ColumnOption, Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::types::{arrow_data_type, BASIN_TYPE_JSONB, BASIN_TYPE_KEY, BASIN_TYPE_UUID};

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

/// Build an Arrow [`Schema`] from sqlparser column definitions.
///
/// Nullability defaults to `true`; `NOT NULL` flips it. Other column options
/// (DEFAULT, UNIQUE, PRIMARY KEY, FOREIGN KEY, etc.) are explicitly out of
/// scope for the PoC and trigger `InvalidSchema` so we don't silently drop
/// constraints the user expected to hold.
pub(crate) fn schema_from_columns(columns: &[ColumnDef]) -> Result<Schema> {
    if columns.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE TABLE requires at least one column".into(),
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    for col in columns {
        let dt = arrow_data_type(&col.data_type)?;
        let mut nullable = true;
        for opt in &col.options {
            match &opt.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Null => nullable = true,
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "unsupported column option in PoC: {other}"
                    )));
                }
            }
        }
        let mut field = Field::new(col.name.value.clone(), dt, nullable);
        if is_jsonb_sql(&col.data_type) {
            // Tag the field so downstream layers (INSERT coercion, the
            // pgwire row encoder) know the bytes are canonical JSON, not
            // arbitrary `bytea`.
            let mut md = HashMap::with_capacity(1);
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_JSONB.to_string());
            field = field.with_metadata(md);
        } else if is_uuid_sql(&col.data_type) {
            // UUID rides on `FixedSizeBinary(16)`; the marker lets the
            // pgwire encoder and REST layer render the canonical
            // hyphenated form rather than 16 raw bytes.
            let mut md = HashMap::with_capacity(1);
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_UUID.to_string());
            field = field.with_metadata(md);
        }
        fields.push(field);
    }
    Ok(Schema::new(fields))
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
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            ident.value.clone()
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
            parts
                .last()
                .map(|p| p.value.clone())
                .ok_or_else(|| {
                    BasinError::InvalidSchema("empty compound identifier".into())
                })?
        }
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
