//! `ALTER TABLE … SUBSCRIBE/UNSUBSCRIBE WEBHOOK` SQL surface (5.11.I).
//!
//! Two statement shapes route through this module:
//!
//! 1. ```sql
//!    ALTER TABLE <table> SUBSCRIBE WEBHOOK
//!        [ <subscription_name> ]
//!        TO '<url>'
//!        ON {INSERT|UPDATE|DELETE}[, ...]
//!        [ WHERE (<predicate>) ]
//!        [ WITH (max_retries = N, ...) ]
//!    ```
//!
//! 2. ```sql
//!    ALTER TABLE <table> UNSUBSCRIBE WEBHOOK <subscription_name>
//!    ```
//!
//! sqlparser 0.52's `ALTER TABLE` AST has no `SUBSCRIBE` /
//! `UNSUBSCRIBE` arms, so we recognise the shape textually before
//! sqlparser sees the statement (mirrors `cv_ddl.rs` and
//! `function_ddl.rs`). The recogniser produces a [`SubscribeIntent`]
//! / [`UnsubscribeIntent`]; the executor functions consume an intent
//! plus a [`WebhookRegistry`] handle.
//!
//! This module lives in `basin-engine` (alongside `webhook_registry`)
//! so the executor's pre-screen pipeline can route `ALTER TABLE …
//! SUBSCRIBE WEBHOOK …` directly. `basin-webhooks` re-exports the
//! intent + registry types via `pub use basin_engine::…`, so HTTP
//! delivery (worker / sink) keeps reading the same shared registry.
//!
//! ## v0.1 limitations
//!
//! - Subscription label storage. The `<subscription_name>` is parsed
//!   and used by `UNSUBSCRIBE` but not currently persisted on the
//!   `WebhookSubscription` row (the registry stores `(project, table,
//!   url)` as the unique key). The exec function looks up by
//!   `(project, table)` for now; a follow-up adds an explicit `label`
//!   field when catalog-persistence lands.
//! - Schema-qualified table names are out of scope.

use basin_common::{BasinError, ProjectId, Result, TableName};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::webhook_registry::{
    SubscriptionError, WebhookOps, WebhookRegistry, WebhookSubscription, WebhookSubscriptionId,
};

/// Parsed `ALTER TABLE ... SUBSCRIBE WEBHOOK ...` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscribeIntent {
    /// `<table>` from the `ALTER TABLE <table>` head.
    pub table: String,
    /// Optional `<subscription_name>` after `SUBSCRIBE WEBHOOK`. v0.1
    /// is informational; the unsubscribe path uses `(project, table)`
    /// match.
    pub label: Option<String>,
    /// Quote-stripped URL.
    pub url: String,
    /// At least one of INSERT / UPDATE / DELETE.
    pub ops: WebhookOps,
    /// Optional `WHERE (<predicate>)` text. Validated to parse as a
    /// SQL expression.
    pub predicate: Option<String>,
    /// `WITH (...)` options. Currently only `max_retries` is
    /// recognised.
    pub max_retries: u32,
}

/// Parsed `ALTER TABLE ... UNSUBSCRIBE WEBHOOK <name>` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsubscribeIntent {
    pub table: String,
    pub label: String,
}

/// Default for `max_retries` when `WITH (...)` omits it. Mirrors
/// [`WebhookRegistry::DEFAULT_MAX_RETRIES`].
pub const DEFAULT_MAX_RETRIES: u32 = WebhookRegistry::DEFAULT_MAX_RETRIES;

/// Recognise `ALTER TABLE <table> SUBSCRIBE WEBHOOK ...`. Returns the
/// parsed intent on a match, `None` when the statement is something
/// else, or an `Err` on a malformed match.
pub fn match_alter_table_subscribe_webhook(sql: &str) -> Result<Option<SubscribeIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "ALTER") {
        return Ok(None);
    }
    let after_alter = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_alter, "TABLE") {
        return Ok(None);
    }
    let after_table = skip_word(after_alter).trim_start();
    let (table, after_table_name) = read_simple_identifier(after_table)?;
    let after_table_name = after_table_name.trim_start();
    if !starts_with_kw(after_table_name, "SUBSCRIBE") {
        return Ok(None);
    }
    let after_subscribe = skip_word(after_table_name).trim_start();
    if !starts_with_kw(after_subscribe, "WEBHOOK") {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE SUBSCRIBE: expected WEBHOOK".into(),
        ));
    }
    let mut rest = skip_word(after_subscribe).trim_start();

    // Optional subscription label: a bare identifier *not* followed by `TO`.
    let label = if !starts_with_kw(rest, "TO") && peek_is_identifier_start(rest) {
        let (name, after) = read_simple_identifier(rest)?;
        rest = after.trim_start();
        Some(name)
    } else {
        None
    };

    if !starts_with_kw(rest, "TO") {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE SUBSCRIBE WEBHOOK: expected TO '<url>'".into(),
        ));
    }
    let after_to = skip_word(rest).trim_start();
    let (url, after_url) = read_string_literal(after_to)?;
    if url.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE SUBSCRIBE WEBHOOK: URL must not be empty".into(),
        ));
    }
    let after_url = after_url.trim_start();

    if !starts_with_kw(after_url, "ON") {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE SUBSCRIBE WEBHOOK: expected ON <op_list>".into(),
        ));
    }
    let after_on = skip_word(after_url).trim_start();
    let (ops, after_ops) = read_op_list(after_on)?;
    if ops.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE SUBSCRIBE WEBHOOK: ON list must include at least one of \
             INSERT / UPDATE / DELETE"
                .into(),
        ));
    }
    let mut tail = after_ops.trim_start();

    // Optional `WHERE (<predicate>)`.
    let predicate = if starts_with_kw(tail, "WHERE") {
        let after_where = skip_word(tail).trim_start();
        let (pred, after_pred) = read_paren_block(after_where)?;
        validate_predicate(&pred)?;
        tail = after_pred.trim_start();
        Some(pred.trim().to_string())
    } else {
        None
    };

    // Optional `WITH (...)` options.
    let max_retries = if starts_with_kw(tail, "WITH") {
        let after_with = skip_word(tail).trim_start();
        let (opts_body, after_opts) = read_paren_block(after_with)?;
        tail = after_opts.trim_start();
        parse_with_options(&opts_body)?
    } else {
        DEFAULT_MAX_RETRIES
    };

    if !tail.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE SUBSCRIBE WEBHOOK: unexpected trailing input {tail:?}"
        )));
    }

    Ok(Some(SubscribeIntent {
        table,
        label,
        url,
        ops,
        predicate,
        max_retries,
    }))
}

/// Recognise `ALTER TABLE <table> UNSUBSCRIBE WEBHOOK <name>`.
pub fn match_alter_table_unsubscribe_webhook(sql: &str) -> Result<Option<UnsubscribeIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "ALTER") {
        return Ok(None);
    }
    let after_alter = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_alter, "TABLE") {
        return Ok(None);
    }
    let after_table = skip_word(after_alter).trim_start();
    let (table, after_table_name) = read_simple_identifier(after_table)?;
    let after_table_name = after_table_name.trim_start();
    if !starts_with_kw(after_table_name, "UNSUBSCRIBE") {
        return Ok(None);
    }
    let after_unsubscribe = skip_word(after_table_name).trim_start();
    if !starts_with_kw(after_unsubscribe, "WEBHOOK") {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE UNSUBSCRIBE: expected WEBHOOK".into(),
        ));
    }
    let after_webhook = skip_word(after_unsubscribe).trim_start();
    let (label, rest) = read_simple_identifier(after_webhook)?;
    let rest = rest.trim();
    if !rest.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE UNSUBSCRIBE WEBHOOK: unexpected trailing input {rest:?}"
        )));
    }
    Ok(Some(UnsubscribeIntent { table, label }))
}

/// Apply a [`SubscribeIntent`] against `registry`. Returns the
/// freshly-minted subscription id.
pub async fn exec_subscribe_webhook(
    intent: SubscribeIntent,
    project: &ProjectId,
    registry: &WebhookRegistry,
) -> Result<WebhookSubscriptionId> {
    let table = TableName::new(&intent.table)?;
    let sub = WebhookSubscription {
        id: WebhookSubscriptionId(uuid::Uuid::nil()),
        project: *project,
        table,
        url: intent.url.clone(),
        ops: intent.ops,
        when_predicate: intent.predicate.clone(),
        max_retries: intent.max_retries,
        paused: false,
    };
    let id = if intent.predicate.is_some() {
        registry
            .add_with_predicate(sub)
            .await
            .map_err(map_subscription_err)?
    } else {
        registry.add(sub).await.map_err(map_subscription_err)?
    };
    Ok(id)
}

/// Apply an [`UnsubscribeIntent`] against `registry`. v0.1 looks up
/// by `(project, table)` because the registry row does not yet carry
/// a label column; the first matching subscription is removed.
/// Returns `Err(NotFound)` when no matching subscription exists.
pub async fn exec_unsubscribe_webhook(
    intent: UnsubscribeIntent,
    project: &ProjectId,
    registry: &WebhookRegistry,
) -> Result<()> {
    let table = TableName::new(&intent.table)?;
    let subs = registry.list(project).await;
    let candidate = subs.into_iter().find(|s| s.table == table).ok_or_else(|| {
        BasinError::not_found(format!(
            "subscription {:?} on {:?} does not exist",
            intent.label, intent.table
        ))
    })?;
    let _ = registry.remove(candidate.id).await;
    Ok(())
}

// --- Helpers ----------------------------------------------------------

fn map_subscription_err(e: SubscriptionError) -> BasinError {
    match e {
        SubscriptionError::PredicateNotSupported => BasinError::InvalidSchema(
            "ALTER TABLE SUBSCRIBE WEBHOOK: WHERE predicate not supported on this entry point"
                .into(),
        ),
        SubscriptionError::InvalidUrl(u) => {
            BasinError::InvalidSchema(format!("invalid webhook URL: {u}"))
        }
        SubscriptionError::NoOps => {
            BasinError::InvalidSchema("ALTER TABLE SUBSCRIBE WEBHOOK: ON list is empty".into())
        }
        SubscriptionError::InvalidMaxRetries => BasinError::InvalidSchema(
            "ALTER TABLE SUBSCRIBE WEBHOOK: max_retries must be >= 1".into(),
        ),
    }
}

fn validate_predicate(pred: &str) -> Result<()> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(pred).map_err(|e| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE SUBSCRIBE WEBHOOK: WHERE predicate parse error: {e}"
        ))
    })?;
    parser.parse_expr().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE SUBSCRIBE WEBHOOK: WHERE predicate parse error: {e}"
        ))
    })?;
    Ok(())
}

fn parse_with_options(body: &str) -> Result<u32> {
    let mut max_retries: u32 = DEFAULT_MAX_RETRIES;
    for raw in split_top_level_commas(body) {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        let Some(eq) = token.find('=') else {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER TABLE SUBSCRIBE WEBHOOK: option {token:?} has no '=' value"
            )));
        };
        let key = token[..eq].trim();
        let value = token[eq + 1..].trim();
        match key.to_ascii_lowercase().as_str() {
            "max_retries" => {
                let n: u32 = value.parse().map_err(|_| {
                    BasinError::InvalidSchema(format!(
                        "max_retries must be a positive integer, got {value:?}"
                    ))
                })?;
                max_retries = n;
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "ALTER TABLE SUBSCRIBE WEBHOOK: unknown WITH option {other:?}"
                )));
            }
        }
    }
    Ok(max_retries)
}

fn read_op_list(s: &str) -> Result<(WebhookOps, &str)> {
    let mut ops = WebhookOps::empty();
    let mut rest = s.trim_start();
    loop {
        let (token, after) = match peek_word(rest) {
            Some(t) => t,
            None => break,
        };
        let bit = match token.to_ascii_uppercase().as_str() {
            "INSERT" => WebhookOps::INSERT,
            "UPDATE" => WebhookOps::UPDATE,
            "DELETE" => WebhookOps::DELETE,
            _ => break,
        };
        ops |= bit;
        rest = after.trim_start();
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else {
            break;
        }
    }
    Ok((ops, rest))
}

fn read_paren_block(s: &str) -> Result<(String, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'(' {
        return Err(BasinError::InvalidSchema(format!(
            "expected '(' at {:?}",
            s.chars().take(8).collect::<String>()
        )));
    }
    let mut depth: i32 = 1;
    let mut i = 1;
    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return Err(BasinError::InvalidSchema(
            "unterminated '(' in ALTER TABLE SUBSCRIBE WEBHOOK clause".into(),
        ));
    }
    Ok((s[1..i - 1].to_string(), &s[i..]))
}

fn read_string_literal(s: &str) -> Result<(String, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'\'' {
        return Err(BasinError::InvalidSchema(format!(
            "expected string literal at {:?}",
            s.chars().take(16).collect::<String>()
        )));
    }
    let mut out = String::new();
    let mut i = 1;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                out.push('\'');
                i += 2;
                continue;
            }
            return Ok((out, &s[i + 1..]));
        }
        out.push(c as char);
        i += 1;
    }
    Err(BasinError::InvalidSchema(
        "unterminated string literal".into(),
    ))
}

fn split_top_level_commas(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b',' if depth == 0 => {
                out.push(&text[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if start <= bytes.len() {
        out.push(&text[start..]);
    }
    out
}

fn starts_with_kw(s: &str, kw: &str) -> bool {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let kw = kw.as_bytes();
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    if bytes.len() == kw.len() {
        return true;
    }
    let next = bytes[kw.len()];
    !(next.is_ascii_alphanumeric() || next == b'_')
}

fn skip_word(s: &str) -> &str {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    &s[i..]
}

fn read_simple_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidIdent(
            "expected an identifier, got end of statement".into(),
        ));
    }
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return Err(BasinError::InvalidIdent(format!(
            "expected an identifier at {:?}",
            s.chars().take(8).collect::<String>()
        )));
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Ok((s[..i].to_string(), &s[i..]))
}

fn peek_is_identifier_start(s: &str) -> bool {
    let s = s.trim_start();
    s.as_bytes()
        .first()
        .copied()
        .map(|b| b.is_ascii_alphabetic() || b == b'_')
        .unwrap_or(false)
}

fn peek_word(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() || !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Some((s[..i].to_string(), &s[i..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_subscribe_minimal() {
        let intent = match_alter_table_subscribe_webhook(
            "ALTER TABLE orders SUBSCRIBE WEBHOOK TO 'https://example.com/h' ON INSERT",
        )
        .unwrap()
        .unwrap();
        assert_eq!(intent.table, "orders");
        assert_eq!(intent.label, None);
        assert_eq!(intent.url, "https://example.com/h");
        assert_eq!(intent.ops, WebhookOps::INSERT);
        assert_eq!(intent.predicate, None);
        assert_eq!(intent.max_retries, DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn match_subscribe_full() {
        let intent = match_alter_table_subscribe_webhook(
            "ALTER TABLE orders SUBSCRIBE WEBHOOK my_hook TO 'https://example.com/h' \
             ON INSERT, UPDATE WHERE (NEW.status = 'paid') WITH (max_retries = 4);",
        )
        .unwrap()
        .unwrap();
        assert_eq!(intent.table, "orders");
        assert_eq!(intent.label.as_deref(), Some("my_hook"));
        assert_eq!(intent.ops, WebhookOps::INSERT | WebhookOps::UPDATE);
        assert_eq!(intent.predicate.as_deref(), Some("NEW.status = 'paid'"));
        assert_eq!(intent.max_retries, 4);
    }

    #[test]
    fn match_subscribe_rejects_empty_url() {
        let err =
            match_alter_table_subscribe_webhook("ALTER TABLE t SUBSCRIBE WEBHOOK TO '' ON INSERT")
                .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_subscribe_rejects_empty_ops() {
        let err =
            match_alter_table_subscribe_webhook("ALTER TABLE t SUBSCRIBE WEBHOOK TO 'http://h' ON")
                .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_subscribe_rejects_malformed_predicate() {
        let err = match_alter_table_subscribe_webhook(
            "ALTER TABLE t SUBSCRIBE WEBHOOK TO 'http://h' ON INSERT WHERE (@@@)",
        )
        .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_unsubscribe() {
        let intent = match_alter_table_unsubscribe_webhook(
            "ALTER TABLE orders UNSUBSCRIBE WEBHOOK my_hook;",
        )
        .unwrap()
        .unwrap();
        assert_eq!(intent.table, "orders");
        assert_eq!(intent.label, "my_hook");
    }

    #[test]
    fn match_subscribe_non_match_returns_none() {
        assert!(match_alter_table_subscribe_webhook("SELECT 1")
            .unwrap()
            .is_none());
        assert!(
            match_alter_table_subscribe_webhook("ALTER TABLE t ADD COLUMN x INT")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn match_unsubscribe_non_match_returns_none() {
        assert!(match_alter_table_unsubscribe_webhook("SELECT 1")
            .unwrap()
            .is_none());
    }
}
