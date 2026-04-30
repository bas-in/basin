//! CREATE TABLE: sqlparser AST → Arrow [`Schema`].

use arrow_schema::{Field, Schema};
use basin_common::{BasinError, Result};
use sqlparser::ast::{ColumnDef, ColumnOption};

use crate::types::arrow_data_type;

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
        fields.push(Field::new(col.name.value.clone(), dt, nullable));
    }
    Ok(Schema::new(fields))
}
