//! A pushed-down predicate, and the value type it compares against.
//!
//! This is intentionally tiny — an equality and a membership test, not a
//! general expression AST. Building a full `Expr` pushdown story is the
//! planner's job (`basin-plan`); this crate only needs enough to prove the
//! pushdown *shape* works and to let real relations like `pg_type` narrow on
//! the columns a catalog client actually filters on in practice (`WHERE oid =
//! $1`, `WHERE typname = 'int4'`, psql's `attrelid = 16384`).

use crate::Oid;

/// One column's value, as a predicate compares it.
///
/// Deliberately not `PgType`-aware — a catalog relation's columns are things
/// like `oid`, `name`, `smallint`, `"char"`, not arbitrary user data types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Oid(Oid),
    Text(String),
    Int(i64),
    Bool(bool),
}

impl From<Oid> for Value {
    fn from(o: Oid) -> Self {
        Value::Oid(o)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

/// A predicate a caller has decided it can push into [`crate::SystemView::scan`].
///
/// `column` names a column of the relation's own [`crate::SystemView::schema`]
/// — `scan` implementations reject a predicate naming a column they do not
/// have (see [`crate::Error::UnknownColumn`]) rather than silently ignoring
/// it, and every predicate naming a real column is guaranteed to be applied.
/// That guarantee is the entire point of this crate: contrast with today's
/// `TableProvider` shims, which accept `_filters` and then discard them (see
/// the crate docs).
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `column = value`.
    Eq { column: &'static str, value: Value },
    /// `column IN (values)`.
    In {
        column: &'static str,
        values: Vec<Value>,
    },
}

impl Predicate {
    /// Convenience constructor for `column = value`.
    pub fn eq(column: &'static str, value: impl Into<Value>) -> Self {
        Predicate::Eq {
            column,
            value: value.into(),
        }
    }

    /// Convenience constructor for `column IN (values)`.
    pub fn in_list(column: &'static str, values: impl IntoIterator<Item = Value>) -> Self {
        Predicate::In {
            column,
            values: values.into_iter().collect(),
        }
    }

    /// The column this predicate constrains.
    pub fn column(&self) -> &'static str {
        match self {
            Predicate::Eq { column, .. } => column,
            Predicate::In { column, .. } => column,
        }
    }

    /// Whether a row whose value for [`Self::column`] is `actual` satisfies
    /// this predicate. `actual` is `None` when the row cannot produce a
    /// value for the column at all, which never matches.
    pub fn matches(&self, actual: Option<&Value>) -> bool {
        let Some(actual) = actual else {
            return false;
        };
        match self {
            Predicate::Eq { value, .. } => actual == value,
            Predicate::In { values, .. } => values.contains(actual),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_matches_only_the_equal_value() {
        let p = Predicate::eq("oid", Value::Oid(Oid(23)));
        assert!(p.matches(Some(&Value::Oid(Oid(23)))));
        assert!(!p.matches(Some(&Value::Oid(Oid(25)))));
        assert!(!p.matches(None));
    }

    #[test]
    fn in_list_matches_any_member() {
        let p = Predicate::in_list("oid", [Value::Oid(Oid(23)), Value::Oid(Oid(25))]);
        assert!(p.matches(Some(&Value::Oid(Oid(23)))));
        assert!(p.matches(Some(&Value::Oid(Oid(25)))));
        assert!(!p.matches(Some(&Value::Oid(Oid(16)))));
    }

    #[test]
    fn column_reports_the_constrained_column() {
        assert_eq!(Predicate::eq("typname", "int4").column(), "typname");
    }
}
