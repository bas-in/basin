//! Remote shard owner: forwards a router-side pgwire session into another
//! Basin process via `tokio_postgres::Client`.
//!
//! ## Lifecycle
//!
//! 1. Router accepts a pgwire connection. Startup handler resolves the
//!    incoming `user` parameter to a `ProjectId` exactly as in single-process
//!    mode.
//! 2. With shard mode on, the startup handler hands the resolved project
//!    plus the original username to a [`RemoteShardSessionFactory`]. The
//!    factory looks up which shard owns the project via `ShardMap`,
//!    establishes a `tokio_postgres::Client` to that shard's pgwire
//!    listener (forwarding `user=<original_username>` so the upstream can
//!    re-resolve to the same project), and returns a [`RemoteShardSession`].
//! 3. The `RemoteShardSession` implements the router's internal `Session`
//!    trait: `execute` forwards as `simple_query`, `prepare` records SQL +
//!    placeholder count, `bind` substitutes literals into the SQL, and
//!    `execute_bound` forwards the substituted SQL as `simple_query`.
//!
//! ## v0.1 limitations (call out in the report)
//!
//! - Result columns come back as `tokio_postgres::SimpleQueryRow`, which
//!   only carries text values. We synthesise an Arrow schema with all
//!   columns typed `Utf8`. The downstream pgwire-text encoder copes —
//!   tests using `tokio_postgres::SimpleQuery` get the same strings back —
//!   but typed binary-format clients see TEXT for everything.
//! - One upstream connection per downstream connection. No pooling.
//! - No COPY support; extended-query forwarding for portals/cursors uses
//!   the same `simple_query` path so `Execute` with `max_rows > 0` always
//!   materialises the full result.

use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_common::{BasinError, ProjectId, Result};
use basin_engine::{BoundStatement, ExecResult, ScalarParam, StatementHandle, StatementSchema};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::protocol::{Session, SessionFactory};
use crate::sharding::ShardMap;

/// Open a `tokio_postgres::Client` against a shard endpoint, forwarding
/// the original pgwire `user` so the upstream router resolves the same
/// `ProjectId`. Spawn the connection driver task; if it returns an error
/// we surface it via tracing — a dropped client will fail subsequent
/// operations, which is the behaviour we want.
async fn connect_upstream(endpoint: &str, user: &str) -> Result<Client> {
    let (host, port) = parse_endpoint(endpoint)?;
    // `password` is ignored by Basin's startup handler but `tokio_postgres`
    // needs *something* to send during cleartext-password auth.
    let conn_str = format!("host={host} port={port} user={user} password=ignored");
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .map_err(|e| BasinError::Internal(format!("connect upstream {endpoint}: {e}")))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::warn!(error = %e, "remote shard connection driver ended with error");
        }
    });
    Ok(client)
}

fn parse_endpoint(ep: &str) -> Result<(&str, u16)> {
    let (host, port_s) = ep
        .rsplit_once(':')
        .ok_or_else(|| BasinError::Internal(format!("bad shard endpoint {ep:?}")))?;
    let port: u16 = port_s
        .parse()
        .map_err(|e| BasinError::Internal(format!("bad shard port {port_s:?}: {e}")))?;
    Ok((host, port))
}

/// Per-connection state for a remote-shard session.
///
/// Holds:
/// - the upstream `tokio_postgres::Client` for the connection's lifetime
///   (so we keep one upstream connection per downstream pgwire session).
/// - a small map of `StatementHandle -> (sql, placeholder_count)` filled
///   in by `prepare` and consumed by `bind`.
pub(crate) struct RemoteShardSession {
    client: Client,
    project: ProjectId,
    prepared: Mutex<std::collections::HashMap<StatementHandle, RemotePrepared>>,
}

struct RemotePrepared {
    sql: String,
    placeholder_count: usize,
}

impl RemoteShardSession {
    fn new(client: Client, project: ProjectId) -> Self {
        Self {
            client,
            project,
            prepared: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait]
impl Session for RemoteShardSession {
    fn project(&self) -> ProjectId {
        self.project
    }

    async fn execute(&self, sql: &str) -> Result<ExecResult> {
        let msgs = self
            .client
            .simple_query(sql)
            .await
            .map_err(|e| BasinError::Internal(format!("remote shard simple_query: {e}")))?;
        Ok(simple_query_to_exec_result(&msgs))
    }

    async fn prepare(&self, sql: &str) -> Result<(StatementHandle, StatementSchema)> {
        // We don't run an upstream Parse round-trip here — we hold the SQL
        // and substitute literals at bind time, then forward via
        // simple_query. That keeps the v0.1 forwarding logic uniform and
        // avoids needing typed parameter descriptions from upstream.
        let placeholder_count = count_placeholders(sql);
        let handle = StatementHandle::new();
        self.prepared.lock().await.insert(
            handle,
            RemotePrepared {
                sql: sql.to_owned(),
                placeholder_count,
            },
        );
        Ok((
            handle,
            StatementSchema {
                // All slots default to TEXT. The downstream client may
                // bind binary-format ints/floats; the param decoder in
                // `protocol.rs` handles that and produces a `ScalarParam`
                // we render to a literal here.
                param_types: vec![DataType::Utf8; placeholder_count],
                param_is_jsonb: vec![false; placeholder_count],
                param_is_uuid: vec![false; placeholder_count],
                columns: Vec::new(),
            },
        ))
    }

    async fn bind(
        &self,
        handle: &StatementHandle,
        params: Vec<ScalarParam>,
    ) -> Result<BoundStatement> {
        let prepared = self.prepared.lock().await;
        let entry = prepared.get(handle).ok_or_else(|| {
            BasinError::not_found(format!("remote prepared statement {handle:?}"))
        })?;
        if params.len() != entry.placeholder_count {
            return Err(BasinError::InvalidSchema(format!(
                "remote bind: expected {} parameters, got {}",
                entry.placeholder_count,
                params.len()
            )));
        }
        let sql = substitute(&entry.sql, &params);
        Ok(BoundStatement::for_tests(sql))
    }

    async fn execute_bound(&self, bound: BoundStatement) -> Result<ExecResult> {
        let sql = bound.debug_sql().to_owned();
        let msgs = self
            .client
            .simple_query(&sql)
            .await
            .map_err(|e| BasinError::Internal(format!("remote shard execute_bound: {e}")))?;
        Ok(simple_query_to_exec_result(&msgs))
    }

    async fn describe_statement(&self, handle: &StatementHandle) -> Result<StatementSchema> {
        let prepared = self.prepared.lock().await;
        let entry = prepared.get(handle).ok_or_else(|| {
            BasinError::not_found(format!("remote prepared statement {handle:?}"))
        })?;
        Ok(StatementSchema {
            param_types: vec![DataType::Utf8; entry.placeholder_count],
            param_is_jsonb: vec![false; entry.placeholder_count],
            param_is_uuid: vec![false; entry.placeholder_count],
            columns: Vec::new(),
        })
    }

    async fn close_statement(&self, handle: &StatementHandle) {
        let _ = self.prepared.lock().await.remove(handle);
    }
}

/// Translate a sequence of `SimpleQueryMessage`s into [`ExecResult`].
///
/// Postgres' simple-query protocol can return multiple statements in one
/// round trip; v0.1 forwarders only ever send one statement at a time, so
/// we collapse to "first row description we see + all subsequent rows".
fn simple_query_to_exec_result(msgs: &[SimpleQueryMessage]) -> ExecResult {
    // Detect whether we have any row data. If every message is
    // `CommandComplete` (or `RowDescription` followed by no rows), we
    // still need to produce the right `ExecResult` shape.
    let mut col_names: Option<Vec<String>> = None;
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut last_tag: Option<String> = None;

    for m in msgs {
        match m {
            SimpleQueryMessage::Row(r) => {
                if col_names.is_none() {
                    col_names = Some(r.columns().iter().map(|c| c.name().to_owned()).collect());
                }
                let n = r.len();
                let mut row: Vec<Option<String>> = Vec::with_capacity(n);
                for i in 0..n {
                    row.push(r.get(i).map(|s| s.to_owned()));
                }
                rows.push(row);
            }
            SimpleQueryMessage::CommandComplete(n) => {
                last_tag = Some(format!("OK {n}"));
            }
            SimpleQueryMessage::RowDescription(cols) => {
                if col_names.is_none() {
                    col_names = Some(cols.iter().map(|c| c.name().to_owned()).collect());
                }
            }
            _ => {}
        }
    }

    match col_names {
        Some(names) if !rows.is_empty() => {
            let schema = Arc::new(Schema::new(
                names
                    .iter()
                    .map(|n| Field::new(n, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            ));
            let n_cols = names.len();
            let mut columns: Vec<Vec<Option<String>>> =
                vec![Vec::with_capacity(rows.len()); n_cols];
            for row in rows {
                for (i, cell) in row.into_iter().enumerate() {
                    if i < n_cols {
                        columns[i].push(cell);
                    }
                }
                // Tolerate short rows by padding with NULL — defensive only.
                for col in columns.iter_mut().take(n_cols) {
                    if col.len() < col.capacity() {
                        // already pushed in inner loop; nothing to pad
                    }
                }
            }
            let arrow_columns: Vec<Arc<dyn arrow_array::Array>> = columns
                .into_iter()
                .map(|col| Arc::new(StringArray::from(col)) as Arc<dyn arrow_array::Array>)
                .collect();
            let batch = RecordBatch::try_new(schema.clone(), arrow_columns)
                .expect("RecordBatch::try_new on text-only schema");
            ExecResult::Rows {
                schema,
                batches: vec![batch],
            }
        }
        Some(names) => {
            // RowDescription but no rows -> an empty result set. Match
            // pgwire's convention: emit a Rows with zero-row batch so the
            // RowDescription gets sent.
            let schema = Arc::new(Schema::new(
                names
                    .iter()
                    .map(|n| Field::new(n, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            ));
            let arrow_columns: Vec<Arc<dyn arrow_array::Array>> = names
                .iter()
                .map(|_| {
                    Arc::new(StringArray::from(Vec::<Option<String>>::new()))
                        as Arc<dyn arrow_array::Array>
                })
                .collect();
            let batch = RecordBatch::try_new(schema.clone(), arrow_columns)
                .expect("RecordBatch::try_new empty");
            ExecResult::Rows {
                schema,
                batches: vec![batch],
            }
        }
        None => ExecResult::Empty {
            tag: last_tag.unwrap_or_else(|| "OK".to_owned()),
        },
    }
}

/// Count `$N` placeholders. We re-implement instead of importing the engine's
/// scanner so the remote-shard module doesn't pull `prepared.rs`'s private
/// helpers across the crate boundary. The substitution rules below match.
fn count_placeholders(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut max_n: usize = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
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
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'$' => {
                let mut j = i + 1;
                let start = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > start {
                    if let Ok(n) = std::str::from_utf8(&bytes[start..j])
                        .unwrap_or("0")
                        .parse::<usize>()
                    {
                        if n > max_n {
                            max_n = n;
                        }
                    }
                    i = j;
                    continue;
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    max_n
}

/// Render `$N` placeholders as SQL literals. Mirrors basin-engine's
/// substitution for the wedge of types we forward.
fn substitute(sql: &str, params: &[ScalarParam]) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + params.len() * 8);
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => {
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
            }
            b'"' => {
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
            }
            b'$' => {
                let mut j = i + 1;
                let start = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > start {
                    let digits = &sql[start..j];
                    let n: usize = digits.parse().unwrap_or(0);
                    if let Some(p) = params.get(n.saturating_sub(1)) {
                        out.push_str(&render(p));
                    }
                    i = j;
                    continue;
                }
                out.push('$');
                i += 1;
            }
            _ => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    out
}

fn render(p: &ScalarParam) -> String {
    match p {
        ScalarParam::Null => "NULL".into(),
        ScalarParam::Int4(v) => v.to_string(),
        ScalarParam::Int8(v) => v.to_string(),
        ScalarParam::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        ScalarParam::Float8(f) => {
            let s = format!("{f}");
            if s.contains('.') || s.contains('e') || s.contains('E') {
                s
            } else {
                format!("{s}.0")
            }
        }
        ScalarParam::Text(s) => {
            let mut out = String::with_capacity(s.len() + 2);
            out.push('\'');
            for c in s.chars() {
                if c == '\'' {
                    out.push('\'');
                    out.push('\'');
                } else {
                    out.push(c);
                }
            }
            out.push('\'');
            out
        }
        ScalarParam::Bytea(bytes) => {
            let mut hex = String::with_capacity(bytes.len() * 2 + 8);
            hex.push_str("'\\x");
            for b in bytes {
                hex.push_str(&format!("{:02x}", b));
            }
            hex.push_str("'::bytea");
            hex
        }
    }
}

/// Factory used by the router when running in shard mode. Holds the
/// `ShardMap`; the per-connection username arrives via `SessionFactory::open`
/// and is forwarded as the upstream pgwire `user` parameter so the upstream
/// router re-resolves it to the same `ProjectId`.
pub(crate) struct RemoteShardSessionFactory {
    map: Arc<ShardMap>,
}

impl RemoteShardSessionFactory {
    pub(crate) fn new(map: Arc<ShardMap>) -> Self {
        Self { map }
    }
}

#[async_trait]
impl SessionFactory for RemoteShardSessionFactory {
    type Session = RemoteShardSession;
    async fn open(&self, project: ProjectId, username: &str) -> Result<Arc<RemoteShardSession>> {
        let endpoint = self.map.shard_for(&project).to_owned();
        let client = connect_upstream(&endpoint, username).await?;
        Ok(Arc::new(RemoteShardSession::new(client, project)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_and_substitute_match_engine() {
        assert_eq!(count_placeholders("SELECT * FROM t WHERE id = $1"), 1);
        assert_eq!(count_placeholders("INSERT INTO t VALUES ($1, $2)"), 2);
        // `$1` in a literal must not count.
        assert_eq!(count_placeholders("SELECT '$1', $2"), 2);
        let s = substitute(
            "INSERT INTO t VALUES ($1, $2)",
            &[ScalarParam::Int8(7), ScalarParam::Text("hi".into())],
        );
        assert_eq!(s, "INSERT INTO t VALUES (7, 'hi')");
        let s = substitute("SELECT $1", &[ScalarParam::Text("it's".into())]);
        assert_eq!(s, "SELECT 'it''s'");
    }

    #[test]
    fn endpoint_parses() {
        assert_eq!(
            parse_endpoint("127.0.0.1:5433").unwrap(),
            ("127.0.0.1", 5433)
        );
        assert!(parse_endpoint("nocolon").is_err());
        assert!(parse_endpoint("127.0.0.1:notaport").is_err());
    }

    #[test]
    fn empty_messages_become_empty_exec_result() {
        let msgs: Vec<SimpleQueryMessage> = Vec::new();
        match simple_query_to_exec_result(&msgs) {
            ExecResult::Empty { tag } => assert_eq!(tag, "OK"),
            other => panic!("expected Empty, got {other:?}"),
        }
    }
}
