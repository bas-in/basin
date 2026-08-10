//! Subscriber-side predicate filter for realtime subscriptions (Phase 5.11.R5).
//!
//! A [`Filter`] holds a SQL `WHERE` predicate that is parsed *once* at
//! subscription registration time and evaluated on each [`ChangeEvent`] at
//! fanout time. Events that do not satisfy the predicate are dropped before
//! they hit the wire, saving network bandwidth and client-side CPU.
//!
//! # Predicate language
//!
//! Same surface as the webhook `WHERE` clause (Phase 5.11.I): column
//! references use `NEW.<col>` / `OLD.<col>`, comparisons `=`, `<>`, `<`,
//! `<=`, `>`, `>=`, boolean composition `AND` / `OR` / `NOT`, and `IS NULL` /
//! `IS NOT NULL`. Anything unsupported returns `Err` from [`Filter::matches`],
//! which the publish path treats as "don't deliver" (fail-closed semantics).
//!
//! # Compile-once design
//!
//! [`Filter::new`] parses the SQL string into a `sqlparser` AST and stores it
//! in an `Arc`. Every `filter.matches(event)` call reuses that AST without
//! re-parsing. This keeps per-event evaluation in the low-microsecond range
//! for typical predicates (see `benches/filter_eval.rs`).

use std::sync::Arc;

use basin_common::ChangeEvent;
use sqlparser::ast::Expr;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// A compiled SQL predicate that can be evaluated against [`ChangeEvent`]
/// payloads. Cheap to clone — the AST is `Arc`-shared.
#[derive(Clone, Debug)]
pub struct Filter {
    /// The original SQL text, kept for display / logging purposes.
    predicate_sql: Arc<str>,
    /// Parsed AST — allocated once at construction, reused per event.
    expr: Arc<Expr>,
}

impl Filter {
    /// Parse `predicate_sql` as a SQL `WHERE` expression.
    ///
    /// Returns `Err` if the string cannot be parsed as a single SQL
    /// expression. The error carries a human-readable description suitable
    /// for returning to the client at subscribe time.
    pub fn new(predicate_sql: &str) -> Result<Self, String> {
        let dialect = PostgreSqlDialect {};
        let mut parser = Parser::new(&dialect)
            .try_with_sql(predicate_sql)
            .map_err(|e| format!("predicate parse error: {e}"))?;
        let expr = parser
            .parse_expr()
            .map_err(|e| format!("predicate parse error: {e}"))?;
        Ok(Self {
            predicate_sql: Arc::from(predicate_sql),
            expr: Arc::new(expr),
        })
    }

    /// Evaluate this filter against `event`.
    ///
    /// Returns `Ok(true)` if the event satisfies the predicate, `Ok(false)`
    /// if it does not, and `Err` if evaluation fails (unsupported expression
    /// shape). The fanout path treats `Err` as "don't deliver" (fail-closed).
    pub fn matches(&self, event: &ChangeEvent) -> Result<bool, String> {
        // Delegate to the shared evaluator that the webhook sink (5.11.I) also
        // uses. This function accepts a pre-built `Expr` reference directly to
        // avoid re-parsing on every event.
        eval_expr(&self.expr, event).map(|lit| matches!(lit, Lit::Bool(true)))
    }

    /// The original predicate SQL string (for logging / debugging).
    pub fn as_sql(&self) -> &str {
        &self.predicate_sql
    }
}

impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.predicate_sql)
    }
}

// ---------------------------------------------------------------------------
// Internal evaluator — mirrors basin-webhooks predicate_eval but operates on
// a pre-parsed Expr so that Filter::matches never re-parses.
// ---------------------------------------------------------------------------

/// Tiny scalar-literal universe the evaluator works in. Same set as the
/// webhook evaluator so comparison semantics are identical.
#[derive(Clone, Debug)]
enum Lit {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

fn eval_expr(expr: &Expr, event: &ChangeEvent) -> Result<Lit, String> {
    use sqlparser::ast::UnaryOperator;
    match expr {
        Expr::Nested(inner) => eval_expr(inner, event),
        Expr::Value(v) => Ok(value_to_lit(&v.value)),
        Expr::Identifier(ident) => {
            let upper = ident.value.to_ascii_uppercase();
            match upper.as_str() {
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

fn eval_binary(op: &sqlparser::ast::BinaryOperator, l: &Lit, r: &Lit) -> Result<Lit, String> {
    use sqlparser::ast::BinaryOperator::*;
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

fn cmp_ord(l: &Lit, r: &Lit) -> Result<std::cmp::Ordering, String> {
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

fn value_to_lit(v: &sqlparser::ast::Value) -> Lit {
    use sqlparser::ast::Value as SqlValue;
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

fn json_field_to_lit(payload: Option<&serde_json::Value>, col: &str) -> Lit {
    let Some(payload) = payload else {
        return Lit::Null;
    };
    let Some(field) = payload.get(col) else {
        return Lit::Null;
    };
    match field {
        serde_json::Value::Null => Lit::Null,
        serde_json::Value::Bool(b) => Lit::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Lit::Int(i)
            } else if let Some(f) = n.as_f64() {
                Lit::Float(f)
            } else {
                Lit::Null
            }
        }
        serde_json::Value::String(s) => Lit::Str(s.clone()),
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::{ChangeOp, ProjectId, TableName};
    use chrono::Utc;
    use serde_json::json;

    fn make_event(after: serde_json::Value) -> ChangeEvent {
        ChangeEvent {
            project: ProjectId::new(),
            table: TableName::new("orders").unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(after),
            committed_at: Utc::now(),
            seq: 1,
            causation_user: None,
        }
    }

    // ---- parse-time errors ------------------------------------------------

    #[test]
    fn filter_new_rejects_invalid_sql() {
        // An unmatched parenthesis cannot be parsed as a SQL expression.
        // Note: sqlparser's parse_expr() is permissive and accepts keywords
        // like SELECT as identifiers, so we use a structurally invalid input.
        assert!(Filter::new("(NEW.status = 'paid'").is_err());
    }

    #[test]
    fn filter_new_accepts_valid_predicate() {
        assert!(Filter::new("NEW.status = 'paid'").is_ok());
    }

    // ---- core acceptance gate (spec: status = 'paid') ---------------------

    #[test]
    fn paid_status_filter_accepts_matching_event() {
        let f = Filter::new("NEW.status = 'paid'").unwrap();
        let event = make_event(json!({"status": "paid", "amount": 99}));
        assert!(f.matches(&event).unwrap(), "paid order should pass filter");
    }

    #[test]
    fn paid_status_filter_rejects_non_matching_event() {
        let f = Filter::new("NEW.status = 'paid'").unwrap();
        let event = make_event(json!({"status": "pending", "amount": 99}));
        assert!(
            !f.matches(&event).unwrap(),
            "pending order must not pass filter"
        );
    }

    // ---- numeric comparison -----------------------------------------------

    #[test]
    fn numeric_gt_filter() {
        let f = Filter::new("NEW.amount > 100").unwrap();
        let pass = make_event(json!({"amount": 200}));
        let fail = make_event(json!({"amount": 50}));
        assert!(f.matches(&pass).unwrap());
        assert!(!f.matches(&fail).unwrap());
    }

    // ---- boolean composition -----------------------------------------------

    #[test]
    fn and_composition() {
        let f = Filter::new("NEW.status = 'paid' AND NEW.amount > 50").unwrap();
        let pass = make_event(json!({"status": "paid", "amount": 100}));
        let fail = make_event(json!({"status": "paid", "amount": 10}));
        assert!(f.matches(&pass).unwrap());
        assert!(!f.matches(&fail).unwrap());
    }

    #[test]
    fn or_composition() {
        let f = Filter::new("NEW.status = 'paid' OR NEW.status = 'refunded'").unwrap();
        let pass1 = make_event(json!({"status": "paid"}));
        let pass2 = make_event(json!({"status": "refunded"}));
        let fail = make_event(json!({"status": "pending"}));
        assert!(f.matches(&pass1).unwrap());
        assert!(f.matches(&pass2).unwrap());
        assert!(!f.matches(&fail).unwrap());
    }

    // ---- NULL handling -----------------------------------------------------

    #[test]
    fn is_null_filter() {
        let f = Filter::new("NEW.deleted_at IS NULL").unwrap();
        let pass = make_event(json!({"deleted_at": null}));
        let fail = make_event(json!({"deleted_at": "2026-01-01"}));
        assert!(f.matches(&pass).unwrap());
        assert!(!f.matches(&fail).unwrap());
    }

    #[test]
    fn missing_field_is_null() {
        let f = Filter::new("NEW.nonexistent IS NULL").unwrap();
        let event = make_event(json!({"status": "paid"}));
        // A missing field resolves to NULL, so IS NULL should be true.
        assert!(f.matches(&event).unwrap());
    }

    // ---- unsupported expression returns Err --------------------------------

    #[test]
    fn unsupported_expression_returns_err() {
        let f = Filter::new("upper(NEW.status) = 'PAID'").unwrap();
        let event = make_event(json!({"status": "paid"}));
        assert!(f.matches(&event).is_err());
    }

    // ---- compile-once reuse -----------------------------------------------

    #[test]
    fn filter_can_be_evaluated_multiple_times() {
        let f = Filter::new("NEW.status = 'paid'").unwrap();
        for _ in 0..100 {
            let event = make_event(json!({"status": "paid"}));
            assert!(f.matches(&event).unwrap());
        }
    }

    #[test]
    fn filter_clone_shares_arc() {
        let f1 = Filter::new("NEW.amount > 0").unwrap();
        let f2 = f1.clone();
        // Both clones should produce the same result.
        let event = make_event(json!({"amount": 42}));
        assert_eq!(f1.matches(&event).unwrap(), f2.matches(&event).unwrap());
    }
}
