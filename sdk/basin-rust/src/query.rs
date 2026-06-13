//! Query builder for `/rest/v1/:table`.
//!
//! Route verified: `crates/basin-rest/src/server.rs:295-301` —
//! `GET|POST|PATCH|DELETE /rest/v1/:table`.
//!
//! The filter grammar is Basin's PostgREST-*style* dialect (`parser.rs`):
//! - `select=<cols,csv>`
//! - `<col>=<op>.<value>` with ops `eq|neq|gt|gte|lt|lte|in|is`
//!   (`in.(a,b,c)` parenthesised; `is.null` / `is.notnull`)
//! - `order=<col>[.asc|.desc][,...]`, `limit=N`, `offset=N`
//! - `cursor=<token>` keyset pagination, `stream=true` NDJSON
//!
//! It is **not** full PostgREST: no `or=`, `not.`, `like|ilike`, embedded
//! resource selects, or `Prefer` headers. Filters AND together.
//!
//! Response shapes (`crates/basin-rest/src/routes/data.rs`):
//! - plain GET → JSON array of rows
//! - GET with `limit` or `cursor` → `{ rows, next_cursor }`
//! - POST → 201 `{ ok, tag }` (or rows); PATCH/DELETE → `{ ok, tag }`
//! - DELETE may surface 501 `E_ENGINE_UNSUPPORTED`

use reqwest::Method;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::BasinError;
use crate::http::Transport;

/// A scalar filter value. Rendered to the wire form expected by `parser.rs`.
#[derive(Debug, Clone)]
pub enum Scalar {
    /// A string value (rendered verbatim).
    Str(String),
    /// An integer value.
    Int(i64),
    /// A floating-point value.
    Float(f64),
    /// A boolean (`true` / `false`).
    Bool(bool),
    /// SQL `NULL` (rendered as `null`).
    Null,
}

impl Scalar {
    fn literal(&self) -> String {
        match self {
            Scalar::Str(s) => s.clone(),
            Scalar::Int(i) => i.to_string(),
            Scalar::Float(f) => f.to_string(),
            Scalar::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            Scalar::Null => "null".to_string(),
        }
    }
}

impl From<&str> for Scalar {
    fn from(s: &str) -> Self {
        Scalar::Str(s.to_string())
    }
}
impl From<String> for Scalar {
    fn from(s: String) -> Self {
        Scalar::Str(s)
    }
}
impl From<i64> for Scalar {
    fn from(i: i64) -> Self {
        Scalar::Int(i)
    }
}
impl From<i32> for Scalar {
    fn from(i: i32) -> Self {
        Scalar::Int(i as i64)
    }
}
impl From<f64> for Scalar {
    fn from(f: f64) -> Self {
        Scalar::Float(f)
    }
}
impl From<bool> for Scalar {
    fn from(b: bool) -> Self {
        Scalar::Bool(b)
    }
}

/// The normalised result of a GET: rows plus the pagination cursor.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    /// The returned rows as raw JSON values.
    pub rows: Vec<Value>,
    /// Opaque keyset-pagination cursor, present when more pages exist.
    pub next_cursor: Option<String>,
}

impl QueryResult {
    /// Deserialise the rows into a typed `Vec<T>`.
    pub fn into_typed<T: DeserializeOwned>(self) -> Result<Vec<T>, BasinError> {
        self.rows
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(|e| BasinError::Decode(e.to_string())))
            .collect()
    }
}

/// Normalise the two GET response shapes (bare array vs `{ rows, next_cursor }`).
fn normalize_get(body: Value) -> QueryResult {
    match body {
        Value::Array(rows) => QueryResult {
            rows,
            next_cursor: None,
        },
        Value::Object(map) => {
            if let Some(Value::Array(rows)) = map.get("rows") {
                QueryResult {
                    rows: rows.clone(),
                    next_cursor: map
                        .get("next_cursor")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                }
            } else {
                // `{ ok, tag }` empty-result shape.
                QueryResult::default()
            }
        }
        _ => QueryResult::default(),
    }
}

/// Fluent query builder for `/rest/v1/:table`.
///
/// Build a query by chaining filter methods, then call a terminal method
/// ([`run`](Self::run), [`rows`](Self::rows), [`insert`](Self::insert), …).
///
/// ```no_run
/// # use basin::Client;
/// # async fn ex(client: Client) -> Result<(), basin::BasinError> {
/// let result = client
///     .table("orders")
///     .select("id,total")
///     .eq("status", "paid")
///     .gte("total", 100i64)
///     .order("total", false)
///     .limit(50)
///     .run()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct QueryBuilder {
    transport: Transport,
    table: String,
    query: Vec<(String, String)>,
}

impl QueryBuilder {
    pub(crate) fn new(transport: Transport, table: impl Into<String>) -> Self {
        Self {
            transport,
            table: table.into(),
            query: Vec::new(),
        }
    }

    fn path(&self) -> String {
        format!("/rest/v1/{}", crate::http::urlencode(&self.table))
    }

    // -- projection / filters ----------------------------------------------

    /// `select=<cols>` projection. Pass `"*"` (or use [`select_all`](Self::select_all))
    /// for all columns.
    pub fn select(mut self, columns: &str) -> Self {
        self.query.push(("select".into(), columns.to_string()));
        self
    }

    /// `select=*` — project all columns.
    pub fn select_all(self) -> Self {
        self.select("*")
    }

    /// `<col>=eq.<value>`.
    pub fn eq(mut self, column: &str, value: impl Into<Scalar>) -> Self {
        self.query
            .push((column.to_string(), format!("eq.{}", value.into().literal())));
        self
    }

    /// `<col>=neq.<value>`.
    pub fn neq(mut self, column: &str, value: impl Into<Scalar>) -> Self {
        self.query
            .push((column.to_string(), format!("neq.{}", value.into().literal())));
        self
    }

    /// `<col>=gt.<value>`.
    pub fn gt(mut self, column: &str, value: impl Into<Scalar>) -> Self {
        self.query
            .push((column.to_string(), format!("gt.{}", value.into().literal())));
        self
    }

    /// `<col>=gte.<value>`.
    pub fn gte(mut self, column: &str, value: impl Into<Scalar>) -> Self {
        self.query
            .push((column.to_string(), format!("gte.{}", value.into().literal())));
        self
    }

    /// `<col>=lt.<value>`.
    pub fn lt(mut self, column: &str, value: impl Into<Scalar>) -> Self {
        self.query
            .push((column.to_string(), format!("lt.{}", value.into().literal())));
        self
    }

    /// `<col>=lte.<value>`.
    pub fn lte(mut self, column: &str, value: impl Into<Scalar>) -> Self {
        self.query
            .push((column.to_string(), format!("lte.{}", value.into().literal())));
        self
    }

    /// `<col>=in.(a,b,c)` — parenthesised list per `parser.rs parse_in_list`.
    pub fn r#in(mut self, column: &str, values: impl IntoIterator<Item = Scalar>) -> Self {
        let joined = values
            .into_iter()
            .map(|v| v.literal())
            .collect::<Vec<_>>()
            .join(",");
        self.query
            .push((column.to_string(), format!("in.({joined})")));
        self
    }

    /// `<col>=is.<value>` — typically `"null"` or `"notnull"`.
    pub fn is(mut self, column: &str, value: &str) -> Self {
        self.query
            .push((column.to_string(), format!("is.{value}")));
        self
    }

    /// `order=<col>.asc|desc` (repeatable). `ascending = false` for descending.
    pub fn order(mut self, column: &str, ascending: bool) -> Self {
        let dir = if ascending { "asc" } else { "desc" };
        self.query
            .push(("order".into(), format!("{column}.{dir}")));
        self
    }

    /// `limit=N`. Switches the GET response to `{ rows, next_cursor }`.
    pub fn limit(mut self, n: u64) -> Self {
        self.query.push(("limit".into(), n.to_string()));
        self
    }

    /// `offset=N`.
    pub fn offset(mut self, n: u64) -> Self {
        self.query.push(("offset".into(), n.to_string()));
        self
    }

    /// `cursor=<token>` — resume keyset pagination from a `next_cursor`.
    pub fn cursor(mut self, token: &str) -> Self {
        self.query.push(("cursor".into(), token.to_string()));
        self
    }

    /// Expose the accumulated query pairs (for inspection / testing).
    pub fn query_pairs(&self) -> &[(String, String)] {
        &self.query
    }

    // -- execution ----------------------------------------------------------

    /// Execute as GET and normalise both response shapes.
    pub async fn run(self) -> Result<QueryResult, BasinError> {
        let body: Value = self
            .transport
            .request_json(Method::GET, &self.path(), &self.query, None, true)
            .await?;
        Ok(normalize_get(body))
    }

    /// Execute as GET and return the rows deserialised into `Vec<T>`.
    pub async fn rows<T: DeserializeOwned>(self) -> Result<Vec<T>, BasinError> {
        self.run().await?.into_typed()
    }

    /// `POST /rest/v1/:table` — insert one row or an array of rows. The
    /// argument is any JSON-serialisable value (object or array of objects).
    pub async fn insert<T: serde::Serialize>(self, values: &T) -> Result<Value, BasinError> {
        let body = serde_json::to_value(values).map_err(|e| BasinError::Decode(e.to_string()))?;
        Ok(self
            .transport
            .request_json_opt(Method::POST, &self.path(), &[], Some(&body), true)
            .await?
            .unwrap_or(Value::Null))
    }

    /// `PATCH /rest/v1/:table?<filters>` — update rows matching the accumulated
    /// filters with the given partial row.
    pub async fn update<T: serde::Serialize>(self, values: &T) -> Result<Value, BasinError> {
        let body = serde_json::to_value(values).map_err(|e| BasinError::Decode(e.to_string()))?;
        Ok(self
            .transport
            .request_json_opt(Method::PATCH, &self.path(), &self.query, Some(&body), true)
            .await?
            .unwrap_or(Value::Null))
    }

    /// `DELETE /rest/v1/:table?<filters>`.
    ///
    /// May error with `E_ENGINE_UNSUPPORTED` (501) on engines without DELETE
    /// support.
    pub async fn delete(self) -> Result<Value, BasinError> {
        Ok(self
            .transport
            .request_json_opt(Method::DELETE, &self.path(), &self.query, None, true)
            .await?
            .unwrap_or(Value::Null))
    }

    #[cfg(feature = "arrow")]
    pub(crate) fn transport_ref(&self) -> &Transport {
        &self.transport
    }

    #[cfg(feature = "arrow")]
    pub(crate) fn arrow_path(&self) -> String {
        self.path()
    }

    #[cfg(feature = "arrow")]
    pub(crate) fn arrow_query(&self) -> &[(String, String)] {
        &self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;

    fn b() -> QueryBuilder {
        QueryBuilder::new(Transport::new("http://h".into(), None, Client::new()), "orders")
    }

    #[test]
    fn builds_filter_pairs_in_order() {
        let q = b()
            .select("id,total")
            .eq("status", "paid")
            .gte("total", 100i64)
            .order("total", false)
            .limit(10);
        let pairs = q.query_pairs();
        assert_eq!(pairs[0], ("select".into(), "id,total".into()));
        assert_eq!(pairs[1], ("status".into(), "eq.paid".into()));
        assert_eq!(pairs[2], ("total".into(), "gte.100".into()));
        assert_eq!(pairs[3], ("order".into(), "total.desc".into()));
        assert_eq!(pairs[4], ("limit".into(), "10".into()));
    }

    #[test]
    fn in_list_is_parenthesised() {
        let q = b().r#in("id", [Scalar::Int(1), Scalar::Int(2), Scalar::Int(3)]);
        assert_eq!(q.query_pairs()[0], ("id".into(), "in.(1,2,3)".into()));
    }

    #[test]
    fn is_null_form() {
        let q = b().is("deleted_at", "null");
        assert_eq!(q.query_pairs()[0], ("deleted_at".into(), "is.null".into()));
    }

    #[test]
    fn bool_and_null_literals() {
        let q = b().eq("active", true).eq("note", Scalar::Null);
        assert_eq!(q.query_pairs()[0].1, "eq.true");
        assert_eq!(q.query_pairs()[1].1, "eq.null");
    }

    #[test]
    fn normalize_bare_array() {
        let r = normalize_get(serde_json::json!([{"id":1},{"id":2}]));
        assert_eq!(r.rows.len(), 2);
        assert!(r.next_cursor.is_none());
    }

    #[test]
    fn normalize_paged_object() {
        let r = normalize_get(serde_json::json!({"rows":[{"id":1}],"next_cursor":"c1"}));
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.next_cursor.as_deref(), Some("c1"));
    }

    #[test]
    fn normalize_exec_tag_is_empty() {
        let r = normalize_get(serde_json::json!({"ok":true,"tag":"INSERT 0 1"}));
        assert!(r.rows.is_empty());
        assert!(r.next_cursor.is_none());
    }
}
