//! sqlparser → Arrow type bridging for the PoC.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use basin_common::{BasinError, Result};
use sqlparser::ast::DataType as SqlDataType;

/// Map a sqlparser column type to an Arrow [`DataType`]. Only the small set
/// listed in the engine's PoC SQL contract is accepted; anything else is an
/// `InvalidSchema` error so callers see exactly which type they tripped on.
pub(crate) fn arrow_data_type(sql: &SqlDataType) -> Result<DataType> {
    match sql {
        SqlDataType::Int(_)
        | SqlDataType::Integer(_)
        | SqlDataType::Int4(_)
        | SqlDataType::BigInt(_)
        | SqlDataType::Int8(_) => Ok(DataType::Int64),

        SqlDataType::Text
        | SqlDataType::Varchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::String(_) => Ok(DataType::Utf8),

        SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),

        SqlDataType::Double
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float(_) => Ok(DataType::Float64),

        SqlDataType::Bytea => Ok(DataType::Binary),

        // sqlparser's Postgres dialect parses unknown parameterised types
        // (e.g. `vector(N)`) as `Custom`. We recognise the `vector(N)` form
        // and map it to the Arrow physical layout the rest of the engine
        // already understands: a `FixedSizeList<Float32>` of length N.
        SqlDataType::Custom(name, modifiers) => {
            if name.0.len() == 1 && name.0[0].value.eq_ignore_ascii_case("vector") {
                let dim = parse_vector_dim(modifiers)?;
                // Child field is nullable=true to match what the Arrow
                // builder helpers produce (`FixedSizeListArray::
                // from_iter_primitive` defaults its child to nullable). The
                // distinction is irrelevant in practice because vector(N) at
                // the user level never carries per-element NULLs.
                Ok(DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim,
                ))
            } else {
                Err(BasinError::InvalidSchema(format!(
                    "unsupported custom type: {name}"
                )))
            }
        }

        other => Err(BasinError::InvalidSchema(format!(
            "unsupported column type in PoC: {other}"
        ))),
    }
}

/// Pull the dimensionality out of `vector(N)`'s modifier list. sqlparser
/// stores the parenthesised modifiers as raw strings; we accept exactly one
/// positive integer.
fn parse_vector_dim(modifiers: &[String]) -> Result<i32> {
    if modifiers.len() != 1 {
        return Err(BasinError::InvalidSchema(format!(
            "vector type requires one dimension argument, got {}",
            modifiers.len()
        )));
    }
    let n: i32 = modifiers[0].trim().parse().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "vector dimension must be a positive integer, got {:?}: {e}",
            modifiers[0]
        ))
    })?;
    if n <= 0 {
        return Err(BasinError::InvalidSchema(format!(
            "vector dimension must be > 0, got {n}"
        )));
    }
    Ok(n)
}

/// Decode a `'[a, b, c]'` literal into a `Vec<f32>`. Used both at INSERT time
/// and at query time when the engine sees a vector literal in a UDF call.
pub(crate) fn parse_vector_literal(s: &str) -> Result<Vec<f32>> {
    let trimmed = s.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "vector literal must be bracketed `[...]`, got {trimmed:?}"
            ))
        })?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for piece in inner.split(',') {
        let p = piece.trim();
        let v: f32 = p.parse().map_err(|e| {
            BasinError::InvalidSchema(format!(
                "bad vector element {p:?}: {e}"
            ))
        })?;
        out.push(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_col_type(sql: &str) -> SqlDataType {
        let ddl = format!("CREATE TABLE t (c {sql})");
        let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, &ddl).unwrap();
        match stmts.pop().unwrap() {
            sqlparser::ast::Statement::CreateTable(ct) => ct.columns[0].data_type.clone(),
            other => panic!("not a CREATE TABLE: {other:?}"),
        }
    }

    #[test]
    fn vector_n_parses_to_fixed_size_list_float32() {
        let dt = arrow_data_type(&parse_col_type("vector(4)")).unwrap();
        match dt {
            DataType::FixedSizeList(field, n) => {
                assert_eq!(*field.data_type(), DataType::Float32);
                assert!(field.is_nullable());
                assert_eq!(n, 4);
            }
            other => panic!("expected FixedSizeList, got {other:?}"),
        }
    }

    #[test]
    fn vector_dim_must_be_positive() {
        let err = arrow_data_type(&parse_col_type("vector(0)")).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn vector_literal_parse_basic() {
        let v = parse_vector_literal("[0.1, 0.2, -0.3]").unwrap();
        assert_eq!(v, vec![0.1f32, 0.2, -0.3]);
    }

    #[test]
    fn vector_literal_empty() {
        let v = parse_vector_literal("[]").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn vector_literal_must_be_bracketed() {
        let err = parse_vector_literal("0.1, 0.2").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }
}
