//! Standalone predicate evaluator for webhook subscriptions.
//!
//! `ALTER TABLE ... SUBSCRIBE WEBHOOK ... WHERE (<predicate>)` lets a
//! subscriber filter the stream: only events for which the predicate
//! evaluates to `true` against the row payload are delivered. This
//! module evaluates that predicate **without re-entering the SQL
//! engine** so the post-commit sink does not depend on the engine
//! crate (would create a cycle: `basin-engine -> basin-webhooks ->
//! basin-engine`).
//!
//! ## Supported predicate shapes
//!
//! - Identifier references `NEW.col` / `OLD.col` resolve against the
//!   event's `after` / `before` JSON payloads.
//! - Identifiers `TG_OP` and `TG_TABLE_NAME` resolve to `'insert'` /
//!   `'update'` / `'delete'` and the table name.
//! - Comparisons `=` `<>` / `!=` `<` `<=` `>` `>=` between scalar
//!   literals and the resolved bind values.
//! - Boolean composition `AND` / `OR` / `NOT`.
//! - `IS NULL` / `IS NOT NULL`.
//!
//! Anything else (function calls, arithmetic, subqueries) returns
//! `Err(_)`, which the sink treats as "don't deliver" — failing closed
//! is the right default for a subscription whose predicate the
//! operator can't sanitise.
//!
//! ## Why a hand-rolled mini-evaluator
//!
//! Engine reentry would let us reuse the full SQL evaluator at the
//! cost of a `basin-engine` dep here. That dependency cycle is
//! exactly what the crate-doc's per-tenant cost discipline calls out
//! — the engine attaches the sink, so the sink cannot own a back-ref
//! to the engine crate. The mini-evaluator is contained to this file
//! and covers the predicate shapes the SQL surface accepts in v0.1
//! (validated by sqlparser at registration time).

use basin_common::ChangeEvent;
use serde_json::Value;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Parse `predicate_sql` as a single SQL expression and evaluate it
/// against `event`. Returns the boolean result (NULL collapses to
/// `false`, matching PG's `WHERE` semantics on NULL). Parse / unsupported
/// shapes surface as `Err`.
pub(crate) fn eval(predicate_sql: &str, event: &ChangeEvent) -> std::result::Result<bool, String> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(predicate_sql)
        .map_err(|e| format!("parse: {e}"))?;
    let expr = parser.parse_expr().map_err(|e| format!("parse: {e}"))?;
    let v = eval_expr(&expr, event)?;
    Ok(matches!(v, Lit::Bool(true)))
}

/// Tiny scalar-literal universe the evaluator works in. We deliberately
/// avoid pulling Arrow / DataFusion types into webhooks; this enum is
/// just enough to compare a JSON-payload value against a SQL literal.
#[derive(Clone, Debug)]
enum Lit {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

fn eval_expr(expr: &Expr, event: &ChangeEvent) -> std::result::Result<Lit, String> {
    match expr {
        Expr::Nested(inner) => eval_expr(inner, event),
        Expr::Value(v) => Ok(value_to_lit(v)),
        Expr::Identifier(ident) => {
            let v = ident.value.to_ascii_uppercase();
            match v.as_str() {
                "TG_OP" => Ok(Lit::Str(op_text(event.op).to_string())),
                "TG_TABLE_NAME" => Ok(Lit::Str(event.table.as_str().to_string())),
                "NULL" => Ok(Lit::Null),
                other => Err(format!("unknown identifier {other:?}")),
            }
        }
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            let head = idents[0].value.to_ascii_uppercase();
            let col = idents[1].value.as_str();
            match head.as_str() {
                "NEW" => Ok(json_field_to_lit(event.after.as_ref(), col)),
                "OLD" => Ok(json_field_to_lit(event.before.as_ref(), col)),
                other => Err(format!("unknown bind variable {other}.{col}")),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, event)?;
            match op {
                UnaryOperator::Not => Ok(Lit::Bool(!truthy(&v))),
                UnaryOperator::Plus => Ok(v),
                UnaryOperator::Minus => match v {
                    Lit::Int(n) => Ok(Lit::Int(-n)),
                    Lit::Float(n) => Ok(Lit::Float(-n)),
                    Lit::Null => Ok(Lit::Null),
                    other => Err(format!("unary minus on {other:?}")),
                },
                other => Err(format!("unsupported unary op {other:?}")),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, event)?;
            let r = eval_expr(right, event)?;
            eval_binary(op, &l, &r)
        }
        Expr::IsNull(inner) => Ok(Lit::Bool(matches!(eval_expr(inner, event)?, Lit::Null))),
        Expr::IsNotNull(inner) => Ok(Lit::Bool(!matches!(eval_expr(inner, event)?, Lit::Null))),
        Expr::IsTrue(inner) => Ok(Lit::Bool(truthy(&eval_expr(inner, event)?))),
        Expr::IsFalse(inner) => Ok(Lit::Bool(!truthy(&eval_expr(inner, event)?))),
        other => Err(format!("unsupported expression: {other}")),
    }
}

fn eval_binary(op: &BinaryOperator, l: &Lit, r: &Lit) -> std::result::Result<Lit, String> {
    use BinaryOperator::*;
    match op {
        And => Ok(Lit::Bool(truthy(l) && truthy(r))),
        Or => Ok(Lit::Bool(truthy(l) || truthy(r))),
        Eq => Ok(Lit::Bool(cmp_eq(l, r))),
        NotEq => Ok(Lit::Bool(!cmp_eq(l, r))),
        Lt | LtEq | Gt | GtEq => {
            let order = cmp_ord(l, r)?;
            let result = match op {
                Lt => order == std::cmp::Ordering::Less,
                LtEq => order != std::cmp::Ordering::Greater,
                Gt => order == std::cmp::Ordering::Greater,
                GtEq => order != std::cmp::Ordering::Less,
                _ => unreachable!(),
            };
            Ok(Lit::Bool(result))
        }
        other => Err(format!("unsupported binary op {other:?}")),
    }
}

fn cmp_eq(l: &Lit, r: &Lit) -> bool {
    match (l, r) {
        (Lit::Null, _) | (_, Lit::Null) => false,
        (Lit::Bool(a), Lit::Bool(b)) => a == b,
        (Lit::Int(a), Lit::Int(b)) => a == b,
        (Lit::Float(a), Lit::Float(b)) => a == b,
        (Lit::Int(a), Lit::Float(b)) | (Lit::Float(b), Lit::Int(a)) => (*a as f64) == *b,
        (Lit::Str(a), Lit::Str(b)) => a == b,
        _ => false,
    }
}

fn cmp_ord(l: &Lit, r: &Lit) -> std::result::Result<std::cmp::Ordering, String> {
    match (l, r) {
        (Lit::Null, _) | (_, Lit::Null) => Err("ordering comparison against NULL".into()),
        (Lit::Int(a), Lit::Int(b)) => Ok(a.cmp(b)),
        (Lit::Float(a), Lit::Float(b)) => a.partial_cmp(b).ok_or_else(|| "NaN compare".into()),
        (Lit::Int(a), Lit::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| "NaN compare".into()),
        (Lit::Float(a), Lit::Int(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| "NaN compare".into()),
        (Lit::Str(a), Lit::Str(b)) => Ok(a.cmp(b)),
        (Lit::Bool(a), Lit::Bool(b)) => Ok(a.cmp(b)),
        (a, b) => Err(format!("incomparable types: {a:?} vs {b:?}")),
    }
}

fn truthy(v: &Lit) -> bool {
    matches!(v, Lit::Bool(true))
}

fn value_to_lit(v: &SqlValue) -> Lit {
    match v {
        SqlValue::Null => Lit::Null,
        SqlValue::Boolean(b) => Lit::Bool(*b),
        SqlValue::Number(s, _) => {
            if let Ok(n) = s.parse::<i64>() {
                Lit::Int(n)
            } else if let Ok(n) = s.parse::<f64>() {
                Lit::Float(n)
            } else {
                Lit::Str(s.clone())
            }
        }
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::EscapedStringLiteral(s)
        | SqlValue::NationalStringLiteral(s)
        | SqlValue::HexStringLiteral(s) => Lit::Str(s.clone()),
        SqlValue::DollarQuotedString(s) => Lit::Str(s.value.clone()),
        _ => Lit::Null,
    }
}

fn json_field_to_lit(payload: Option<&Value>, col: &str) -> Lit {
    let Some(payload) = payload else {
        return Lit::Null;
    };
    let Some(field) = payload.get(col) else {
        return Lit::Null;
    };
    match field {
        Value::Null => Lit::Null,
        Value::Bool(b) => Lit::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Lit::Int(i)
            } else if let Some(f) = n.as_f64() {
                Lit::Float(f)
            } else {
                Lit::Null
            }
        }
        Value::String(s) => Lit::Str(s.clone()),
        // Arrays / objects render as their canonical JSON text so a
        // predicate can string-compare against them. Use rare; v0.1
        // scope.
        other => Lit::Str(other.to_string()),
    }
}

fn op_text(op: basin_common::ChangeOp) -> &'static str {
    use basin_common::ChangeOp;
    match op {
        ChangeOp::Insert => "insert",
        ChangeOp::Update => "update",
        ChangeOp::Delete => "delete",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::{ChangeOp, TableName, TenantId};
    use chrono::Utc;
    use serde_json::json;

    fn ev(after: serde_json::Value) -> ChangeEvent {
        ChangeEvent {
            tenant: TenantId::new(),
            table: TableName::new("t").unwrap(),
            op: ChangeOp::Update,
            before: Some(json!({})),
            after: Some(after),
            committed_at: Utc::now(),
            seq: 1,
            causation_user: None,
        }
    }

    #[test]
    fn eq_string_match() {
        let e = ev(json!({"status": "paid"}));
        assert!(eval("NEW.status = 'paid'", &e).unwrap());
        assert!(!eval("NEW.status = 'pending'", &e).unwrap());
    }

    #[test]
    fn integer_compare() {
        let e = ev(json!({"amount": 100}));
        assert!(eval("NEW.amount > 50", &e).unwrap());
        assert!(!eval("NEW.amount > 200", &e).unwrap());
        assert!(eval("NEW.amount = 100", &e).unwrap());
    }

    #[test]
    fn and_or_compose() {
        let e = ev(json!({"status": "paid", "amount": 100}));
        assert!(eval("NEW.status = 'paid' AND NEW.amount > 50", &e).unwrap());
        assert!(!eval("NEW.status = 'failed' OR NEW.amount < 50", &e).unwrap());
    }

    #[test]
    fn is_null_works() {
        let e = ev(json!({"deleted_at": Value::Null, "ts": "2026-01-01"}));
        assert!(eval("NEW.deleted_at IS NULL", &e).unwrap());
        assert!(eval("NEW.ts IS NOT NULL", &e).unwrap());
    }

    #[test]
    fn unsupported_returns_err() {
        let e = ev(json!({"x": 1}));
        assert!(eval("upper(NEW.x) = 'a'", &e).is_err());
    }

    #[test]
    fn missing_field_is_null() {
        let e = ev(json!({"x": 1}));
        // Comparing NULL to anything is false, not error.
        assert!(!eval("NEW.missing = 1", &e).unwrap());
    }

    #[test]
    fn tg_op_resolves() {
        let e = ev(json!({}));
        assert!(eval("TG_OP = 'update'", &e).unwrap());
        assert!(!eval("TG_OP = 'insert'", &e).unwrap());
    }
}
