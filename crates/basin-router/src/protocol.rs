//! pgwire simple-query and extended-query handling.
//!
//! The structure is:
//!
//! 1. A small internal trait `Session` abstracts whatever runs the SQL. The
//!    real implementation forwards to `basin_engine::ProjectSession`; tests
//!    inject a fake. We *do not* add a trait to basin-engine — the abstraction
//!    lives entirely in this crate.
//! 2. `simple_query_messages` produces the full sequence of backend messages
//!    for one simple-query exchange (Row description -> data rows ->
//!    CommandComplete -> ReadyForQuery, or ErrorResponse -> ReadyForQuery).
//!    It's pure data, which makes it easy to unit-test against a fake session.
//! 3. `BasinSimpleQueryHandler` is the pgwire glue that owns the per-connection
//!    `Session` and feeds the produced messages through the `Sink`.
//! 4. `BasinExtendedQueryHandler` implements the full
//!    `Parse`/`Bind`/`Describe`/`Execute`/`Close`/`Sync` flow against the
//!    engine's prepared-statement API. Per-connection statement and portal
//!    maps live on the handler instance (one handler per connection).
//!
//! ### Format-code policy
//!
//! **Result columns**: per-column binary format codes from the `Bind` message
//! are respected. `encode_batches_with_formats` emits binary when the client
//! requests format code 1, text (format code 0) otherwise. Simple-query
//! results are always text (matching the PG spec).
//!
//! **Parameters**: binary input parameters are decoded for the common scalar
//! types (int2/4/8, float4/8, bool, bytea, text, UUID, JSONB, NUMERIC,
//! timestamp, date, one-dimensional arrays). Unknown binary types fall back
//! to a best-effort UTF-8 decode so the engine's text-substitution path can
//! still coerce them.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use basin_common::Result;
use basin_engine::{
    AuthContext, BoundStatement, ExecResult, ProjectSession, ScalarParam, StatementHandle,
    StatementSchema,
};
use chrono::TimeZone as _; // for Utc.timestamp_opt()
use futures::sink::{Sink, SinkExt};
use pgwire::api::auth::{
    finish_authentication, save_startup_parameters_to_metadata, DefaultServerParameterProvider,
    StartupHandler,
};
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeResponse, DescribeStatementResponse, Response,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireConnectionState, PgWireServerHandlers,
    Type, METADATA_USER,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::data::{NoData, ParameterDescription};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Flush, Parse, ParseComplete,
    Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::messages::response::{
    CommandComplete, EmptyQueryResponse, ErrorResponse, ReadyForQuery, TransactionStatus,
};
use pgwire::messages::simplequery::Query;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use sqlparser::ast::{FromTable, SelectItem, Statement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use tokio::sync::Mutex;

use crate::error::error_response;
use crate::resolver::ProjectResolver;
use crate::types::{arrow_to_pg_type, encode_batches, row_description, row_description_with_formats};

/// The PostgreSQL `server_version` Basin advertises in the pgwire startup
/// ParameterStatus. MUST stay a real PG-style version whose major component is
/// the major Basin claims wire-compatibility with — clients (Django, psycopg,
/// node-postgres, JDBC, …) gate features and even the initial connection on it.
/// pgwire's `DefaultServerParameterProvider` defaults this to its OWN crate
/// version, which a client reads as "PostgreSQL 0.28" and rejects; we override
/// it here. Keep it consistent with `pg_settings.server_version`
/// (`basin-catalog` info_schema, currently 15.0).
const BASIN_PG_SERVER_VERSION: &str = "15.0";

// ── Commit-conflict retry policy ──────────────────────────────────────────────
//
// When a write DML (INSERT / UPDATE / DELETE) lands a `CommitConflict` the
// router retries it transparently with exponential backoff + jitter before
// surfacing SQLSTATE 40001 to the client.  The retry re-executes the *exact*
// same `BoundStatement` that was already Bind-substituted: params are baked
// into the SQL string, so the semantics are identical on every attempt.
//
// Correctness constraints:
//  * Only bare DML portals (not SELECT) enter the retry path.
//  * `UniqueViolation` (23505) and other non-snapshot errors propagate
//    immediately — they are schema conflicts, not snapshot races.
//  * On exhaustion the caller receives the original `CommitConflict` error,
//    which the router maps to SQLSTATE 40001 (`serialization_failure`).
//  * No enclosing transaction awareness needed for v0.1 (auto-commit only).

/// Maximum number of retry attempts after the initial execution failure.
/// After this many retries the error is returned to the client.
const COMMIT_CONFLICT_MAX_RETRIES: u32 = 6;

/// Base delay in milliseconds for the first retry.  Each subsequent attempt
/// doubles the base before jitter is added (1, 2, 4, 8, 16, 32 ms).
const COMMIT_CONFLICT_BASE_MS: u64 = 1;

/// Global observable counters — readable by tests / metrics scrapers.
pub static COMMIT_CONFLICT_RETRY_COUNT: AtomicU64 = AtomicU64::new(0);
pub static COMMIT_CONFLICT_EXHAUSTED_COUNT: AtomicU64 = AtomicU64::new(0);

/// Internal trait abstracting whatever runs SQL. `ProjectSession` implements it
/// transparently; tests substitute a fake.
///
/// The prepared-statement methods mirror the engine API one-for-one. Default
/// bodies for the engine impl forward straight through; tests only need to
/// implement what they exercise.
#[async_trait]
pub(crate) trait Session: Send + Sync {
    /// Project this session belongs to. Stable for the session's lifetime.
    /// Used by the per-project rate limiter (see [`crate::rate_limit`])
    /// to key the token bucket without re-resolving the project on every
    /// query.
    fn project(&self) -> basin_common::ProjectId;

    async fn execute(&self, sql: &str) -> Result<ExecResult>;

    async fn prepare(&self, sql: &str) -> Result<(StatementHandle, StatementSchema)>;

    async fn bind(
        &self,
        handle: &StatementHandle,
        params: Vec<ScalarParam>,
    ) -> Result<BoundStatement>;

    async fn execute_bound(&self, bound: BoundStatement) -> Result<ExecResult>;

    /// Mirror of [`basin_engine::ProjectSession::describe_statement`]. The
    /// router caches schemas at parse time so this is unused on the hot path,
    /// but the trait keeps it for parity with the engine API.
    #[allow(dead_code)]
    async fn describe_statement(&self, handle: &StatementHandle) -> Result<StatementSchema>;

    async fn close_statement(&self, handle: &StatementHandle);

    /// Ingest a pre-parsed CSV batch into `table_name`, bypassing SQL parse.
    ///
    /// Returns the number of rows ingested.  On
    /// `BasinError::FeatureNotSupported` the caller should fall back to the
    /// per-row INSERT path (e.g. partitioned tables).  The default
    /// implementation returns `Err(FeatureNotSupported)` so test fakes that
    /// don't implement the fast path compile and fail gracefully.
    async fn ingest_csv_batch(
        &self,
        _table_name: &str,
        _full_schema: std::sync::Arc<arrow_schema::Schema>,
        _column_names: Option<&[String]>,
        _rows: Vec<Vec<Option<String>>>,
    ) -> Result<u64> {
        Err(basin_common::BasinError::FeatureNotSupported(
            "ingest_csv_batch not implemented for this session type".into(),
        ))
    }
}

#[async_trait]
impl Session for ProjectSession {
    fn project(&self) -> basin_common::ProjectId {
        ProjectSession::project(self)
    }

    async fn execute(&self, sql: &str) -> Result<ExecResult> {
        ProjectSession::execute(self, sql).await
    }

    async fn prepare(&self, sql: &str) -> Result<(StatementHandle, StatementSchema)> {
        ProjectSession::prepare(self, sql).await
    }

    async fn bind(
        &self,
        handle: &StatementHandle,
        params: Vec<ScalarParam>,
    ) -> Result<BoundStatement> {
        ProjectSession::bind(self, handle, params).await
    }

    async fn execute_bound(&self, bound: BoundStatement) -> Result<ExecResult> {
        ProjectSession::execute_bound(self, bound).await
    }

    async fn describe_statement(&self, handle: &StatementHandle) -> Result<StatementSchema> {
        ProjectSession::describe_statement(self, handle).await
    }

    async fn close_statement(&self, handle: &StatementHandle) {
        ProjectSession::close_statement(self, handle).await
    }

    async fn ingest_csv_batch(
        &self,
        table_name: &str,
        full_schema: std::sync::Arc<arrow_schema::Schema>,
        column_names: Option<&[String]>,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<u64> {
        ProjectSession::ingest_csv_batch(self, table_name, full_schema, column_names, rows).await
    }
}

/// Split a simple-query message body into individual statement texts.
///
/// PG's pgwire spec allows a single `Q` message to carry multiple
/// semicolon-separated statements (`tokio_postgres::batch_execute`,
/// `psql -f setup.sql`). The engine accepts one statement per call, so the
/// router splits on top-level `;` here.
///
/// Splits using sqlparser's tokenizer so dollar-quoted bodies, single-/
/// double-quoted literals, and `--` / `/* ... */` comments don't fool the
/// separator search. Each returned string is the raw substring of the
/// original SQL between separators (whitespace trimmed); empty fragments
/// (e.g. trailing `;`, comment-only) are skipped so they don't trigger
/// engine pre-screens.
///
/// On a tokenizer error we fall back to returning the full SQL as a single
/// statement; the engine will re-tokenize and surface a properly-shaped
/// parse error.
pub(crate) fn split_simple_query(sql: &str) -> Vec<String> {
    let dialect = PostgreSqlDialect {};
    let tokens = match Tokenizer::new(&dialect, sql).tokenize_with_location() {
        Ok(t) => t,
        Err(_) => {
            let trimmed = sql.trim();
            return if trimmed.is_empty() {
                Vec::new()
            } else {
                vec![sql.to_string()]
            };
        }
    };

    // Pre-compute byte offsets for every line start. sqlparser locations
    // are 1-indexed line / 1-indexed column on chars (NOT bytes).
    let mut line_starts = vec![0usize];
    for (i, b) in sql.bytes().enumerate() {
        if b == b'\n' {
            line_starts.push(i + 1);
        }
    }
    let to_byte_offset = |line: u64, column: u64| -> usize {
        let li = (line as usize)
            .saturating_sub(1)
            .min(line_starts.len().saturating_sub(1));
        let line_start = line_starts[li];
        let col = (column as usize).saturating_sub(1);
        // Walk `col` chars from `line_start`. If we run off the end of the
        // string, return its length (one-past-the-end is a valid Rust slice
        // index and matches "EOF column").
        sql[line_start..]
            .char_indices()
            .nth(col)
            .map(|(byte_in_line, _)| line_start + byte_in_line)
            .unwrap_or(sql.len())
    };

    let mut out = Vec::new();
    let mut start = 0usize;
    // Tracks whether any "real" token (not whitespace, not comment) has been
    // seen in the current segment. Comment-only / whitespace-only segments
    // are dropped so the engine's textual pre-screens never see them.
    let mut has_real_token = false;
    for twl in &tokens {
        match &twl.token {
            Token::SemiColon => {
                let off = to_byte_offset(twl.span.start.line, twl.span.start.column);
                if has_real_token {
                    let segment = sql[start..off].trim();
                    if !segment.is_empty() {
                        out.push(segment.to_string());
                    }
                }
                start = off + 1;
                has_real_token = false;
            }
            Token::Whitespace(_) => {}
            _ => has_real_token = true,
        }
    }
    if has_real_token {
        let tail = sql[start..].trim();
        if !tail.is_empty() {
            out.push(tail.to_string());
        }
    }
    out
}

/// Produce the full backend-message sequence for one simple-query exchange.
///
/// This is the pure-data heart of the simple-query protocol; the pgwire
/// integration just feeds these bytes to the wire. Splitting it out lets us
/// unit-test against a `FakeSession` without standing up a TCP socket or a
/// real `Engine`.
///
/// Multi-statement messages are split into individual statements before
/// dispatch; each statement produces its own response group
/// (`RowDescription` + `DataRow`s + `CommandComplete`, or `CommandComplete`
/// for non-row results). Exactly one trailing `ReadyForQuery` closes the
/// exchange. On the first failing statement we emit `ErrorResponse` and
/// stop processing further statements (PG's non-transactional semicolon
/// batch behaviour).
pub(crate) async fn simple_query_messages<S>(session: &S, sql: &str) -> Vec<PgWireBackendMessage>
where
    S: Session + ?Sized,
{
    // ── ADR 0028 Phase 5: logical-replication command path (FLAGGED) ──────
    //
    // A `replication=database` consumer (Debezium / pg_recvlogical / DMS)
    // issues replication commands — not SQL — over this same simple-query
    // channel. When the flag is on, recognise them here BEFORE the SQL split
    // (they are not valid SQL and would otherwise syntax-error). When off,
    // fall through so a default build behaves exactly like a non-replication
    // backend. See `crate::replication` for the parse + response shapes and
    // the documented START_REPLICATION streaming wiring.
    if crate::replication::replication_enabled() {
        if let Some(msgs) = replication_command_messages(session, sql) {
            return msgs;
        }
    }

    let mut out = Vec::new();
    let stmts = split_simple_query(sql);

    if stmts.is_empty() {
        out.push(PgWireBackendMessage::EmptyQueryResponse(
            EmptyQueryResponse::new(),
        ));
        out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
            TransactionStatus::Idle,
        )));
        return out;
    }

    for stmt_sql in &stmts {
        match session.execute(stmt_sql).await {
            Ok(ExecResult::Empty { tag }) => {
                out.push(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    tag,
                )));
            }
            Ok(ExecResult::Rows { schema, batches }) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                let rd = row_description(schema.as_ref());
                out.push(PgWireBackendMessage::RowDescription(rd));
                for row in encode_batches(&schema, &batches) {
                    out.push(PgWireBackendMessage::DataRow(row));
                }
                out.push(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    format!("SELECT {row_count}"),
                )));
            }
            Err(e) => {
                tracing::warn!(error = %e, "query failed");
                out.push(PgWireBackendMessage::ErrorResponse(error_response(&e)));
                // PG stops dispatching the rest of a simple-query batch after
                // the first failure (non-transactional semicolon-separated
                // statements; each succeeds independently up to the failure).
                break;
            }
        }
    }

    out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
        TransactionStatus::Idle,
    )));
    out
}

/// ADR 0028 Phase 5 — handle a recognised logical-replication command, or
/// return `None` if `sql` is not one (so the caller falls through to SQL).
///
/// The query-protocol commands (`IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT`,
/// `DROP_REPLICATION_SLOT`) answer with their real message shapes.
/// `START_REPLICATION` requires CopyBoth streaming mode, which the framework's
/// `SimpleQueryHandler` path does not model: it returns a structured
/// `ErrorResponse` naming the exact remaining wiring rather than faking a
/// stream (an honest deferral; see `crate::replication::START_REPLICATION_WIRING`).
fn replication_command_messages<S>(session: &S, sql: &str) -> Option<Vec<PgWireBackendMessage>>
where
    S: Session + ?Sized,
{
    use crate::replication::{
        create_slot_response, drop_slot_response, identify_system_response,
        parse_replication_command, ReplicationCommand, START_REPLICATION_WIRING,
    };

    let cmd = parse_replication_command(sql)?;
    let project = session.project();
    // `confirmed_seq` (the live ring high-water mark) is the remaining wiring:
    // a working stream reads it from the CDC ring / slot catalog. At the
    // command layer we report a fresh `0` xlogpos, which is protocol-valid
    // (a slot created at 0/... streams from the start of the ring).
    let confirmed_seq: u64 = 0;
    let dbname = project.to_string();

    let msgs = match cmd {
        ReplicationCommand::IdentifySystem => {
            identify_system_response(&dbname, confirmed_seq, &dbname)
        }
        ReplicationCommand::CreateSlot {
            slot_name, plugin, ..
        } => {
            // The durable slot row is the additive catalog model
            // (`basin_catalog::cdc_slots`); persisting it here is the wiring
            // that turns this from a stateless ack into a held cursor.
            create_slot_response(&slot_name, confirmed_seq, plugin)
        }
        ReplicationCommand::DropSlot { .. } => drop_slot_response(),
        ReplicationCommand::StartReplication { .. } => {
            // Honest deferral — never fake a CopyBoth stream.
            vec![
                PgWireBackendMessage::ErrorResponse(pgwire::messages::response::ErrorResponse::from(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(), // feature_not_supported
                        format!("START_REPLICATION streaming not yet wired: {START_REPLICATION_WIRING}"),
                    ),
                )),
                PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
            ]
        }
        ReplicationCommand::TimelineHistory { .. } => {
            // Basin has a single synthetic timeline (1); no history file.
            vec![
                PgWireBackendMessage::ErrorResponse(pgwire::messages::response::ErrorResponse::from(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_owned(),
                        "0A000".to_owned(),
                        "TIMELINE_HISTORY: Basin has a single synthetic timeline (1)".to_owned(),
                    ),
                )),
                PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
            ]
        }
    };
    Some(msgs)
}

/// pgwire `SimpleQueryHandler` that drives one connection's session.
///
/// Owns the resolved `Session` for the connection. We override `on_query`
/// directly (not `do_query`) because we want full control over the response
/// sequence and we already produce a well-formed message stream from
/// `simple_query_messages`.
pub(crate) struct BasinSimpleQueryHandler<S: Session + 'static> {
    session: Arc<S>,
}

impl<S: Session + 'static> BasinSimpleQueryHandler<S> {
    pub(crate) fn new(session: Arc<S>) -> Self {
        Self { session }
    }
}

#[async_trait]
impl<S: Session + 'static> SimpleQueryHandler for BasinSimpleQueryHandler<S> {
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        client.set_state(PgWireConnectionState::QueryInProgress);

        tracing::info!(sql = %query.query.lines().next().unwrap_or(""), "simple query");
        let msgs = simple_query_messages(self.session.as_ref(), &query.query).await;
        for m in msgs {
            client.feed(m).await?;
        }
        client.flush().await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    // We bypass the framework's `do_query` -> `Response` plumbing because
    // `on_query` does the entire job. The trait still requires the method, so
    // we provide a stub that's never called.
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        _query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        unreachable!("on_query is overridden and never delegates to do_query")
    }
}

/// Per-connection state for the extended-query handler.
///
/// The pgwire crate provides its own `PortalStore` for storing parsed
/// statements and portals; we deliberately do *not* use it because we keep
/// the engine-side [`StatementHandle`] association ourselves and the engine
/// is the source of truth for the substituted SQL. Storing the same key in
/// both pgwire's store and ours doubles the work and creates an opportunity
/// for them to disagree, which we don't want.
struct ExtendedState {
    /// `name` (empty string == unnamed) → engine handle + cached schema.
    statements: HashMap<String, StatementEntry>,
    /// portal name → bound statement + cached schema.
    portals: HashMap<String, PortalEntry>,
    /// Parallel maps for `COPY FROM STDIN` / `COPY TO STDOUT`. Kept
    /// separate from `statements` / `portals` because COPY has no engine
    /// handle, no parameters, and no result rows — the regular tables
    /// don't fit.
    copy_statements: HashMap<String, crate::copy::CopyCommand>,
    copy_portals: HashMap<String, crate::copy::CopyCommand>,
}

#[derive(Clone)]
struct StatementEntry {
    handle: StatementHandle,
    schema: StatementSchema,
    /// SQL string we sent to `prepare`. Useful for error logs.
    #[allow(dead_code)]
    sql: String,
    /// For `INSERT/UPDATE/DELETE … RETURNING …` statements: the DML verb
    /// (`"INSERT"`, `"UPDATE"`, `"DELETE"`) so `Execute` can emit the
    /// correct Postgres-style command tag (`"INSERT 0 N"`, `"UPDATE N"`,
    /// `"DELETE N"`) instead of the generic `"SELECT N"` tag the row-path
    /// would otherwise produce.
    dml_tag_prefix: Option<String>,
}

#[derive(Clone)]
struct PortalEntry {
    bound: BoundStatement,
    /// Schema cached at bind time so `Describe portal` can answer without
    /// re-running the planner.
    schema: StatementSchema,
    /// Result-column format codes from the `Bind` message. One entry per
    /// column, or one entry that applies to all, or empty (default text).
    /// We need these at `Execute` time so binary-format requests get binary
    /// row data back; otherwise drivers like `tokio-postgres` (which
    /// hard-code binary) misdecode the bytes.
    result_format_codes: Vec<i16>,
    /// Propagated from `StatementEntry::dml_tag_prefix` at bind time.
    /// Present for RETURNING DML portals; `None` for SELECT portals and
    /// plain DML without RETURNING.
    dml_tag_prefix: Option<String>,
    /// True when the statement is a bare write DML (INSERT / UPDATE / DELETE).
    /// Used by the commit-conflict retry loop in `handle_execute` to gate
    /// the transparent retry: reads (SELECT) never need a retry, and retrying
    /// them would be semantically wrong if they had UDF side-effects.
    is_write_dml: bool,
}

impl ExtendedState {
    fn new() -> Self {
        Self {
            statements: HashMap::new(),
            portals: HashMap::new(),
            copy_statements: HashMap::new(),
            copy_portals: HashMap::new(),
        }
    }
}

/// Returns `true` if `sql` is a bare write-DML statement (INSERT, UPDATE, or
/// DELETE) that may produce a `CommitConflict` under concurrent load and should
/// therefore enter the transparent retry path.
///
/// Detection is intentionally simple: skip leading whitespace / SQL comments
/// (`--` line-comment, `/* */` block-comment) and check the first keyword.
/// This is cheaper than a full parse and correct for the shapes Basin emits
/// (the prepared-statement substitution always produces a well-formed
/// statement where the verb is the very first token after optional comments).
///
/// SELECT, COPY, DDL, etc. all return `false` and bypass the retry.
fn sql_is_write_dml(sql: &str) -> bool {
    // Skip whitespace and SQL-style comments to find the first keyword.
    let mut chars = sql.trim_start().chars().peekable();
    // Consume block comments `/* ... */` and line comments `-- ...`.
    loop {
        match (chars.peek().copied(), {
            let mut it = chars.clone();
            it.next();
            it.peek().copied()
        }) {
            (Some('/'), Some('*')) => {
                // Block comment: scan until `*/`.
                chars.next(); // '/'
                chars.next(); // '*'
                loop {
                    match chars.next() {
                        None => return false,
                        Some('*') if chars.peek() == Some(&'/') => {
                            chars.next(); // '/'
                            break;
                        }
                        _ => {}
                    }
                }
                // Consume any whitespace after the comment.
                while chars.peek().map(|c| c.is_whitespace()).unwrap_or(false) {
                    chars.next();
                }
            }
            (Some('-'), Some('-')) => {
                // Line comment: scan until newline.
                while chars.peek().map(|c| *c != '\n').unwrap_or(false) {
                    chars.next();
                }
                // Consume any whitespace after the comment.
                while chars.peek().map(|c| c.is_whitespace()).unwrap_or(false) {
                    chars.next();
                }
            }
            _ => break,
        }
    }
    // Collect the first keyword (ASCII alpha chars).
    let keyword: String = chars
        .take_while(|c| c.is_ascii_alphabetic())
        .flat_map(|c| c.to_uppercase())
        .collect();
    matches!(keyword.as_str(), "INSERT" | "UPDATE" | "DELETE")
}

/// Decode a parameter byte slice into a [`ScalarParam`] using its declared
/// pgwire `Type` and the wire format code (0 = text, 1 = binary).
///
/// v1 supports both text and binary for the common scalar types; everything
/// else falls back to text-decoded UTF-8 and the engine's substitution does
/// the final SQL-escaping.
fn decode_param(
    bytes: &[u8],
    declared: &Type,
    is_binary: bool,
) -> std::result::Result<ScalarParam, PgWireError> {
    if is_binary {
        decode_param_binary(bytes, declared)
    } else {
        decode_param_text(bytes, declared)
    }
}

fn decode_param_text(
    bytes: &[u8],
    declared: &Type,
) -> std::result::Result<ScalarParam, PgWireError> {
    let s = std::str::from_utf8(bytes).map_err(|e| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22P02".to_owned(),
            format!("text-format parameter is not UTF-8: {e}"),
        )))
    })?;

    let parse_err = |what: &str, e: &dyn std::fmt::Display| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22P02".to_owned(), // invalid_text_representation
            format!("invalid {what} parameter {s:?}: {e}"),
        )))
    };

    match *declared {
        Type::INT2 | Type::INT4 => {
            let v: i32 = s
                .parse()
                .map_err(|e: std::num::ParseIntError| parse_err("int4", &e))?;
            Ok(ScalarParam::Int4(v))
        }
        Type::INT8 => {
            let v: i64 = s
                .parse()
                .map_err(|e: std::num::ParseIntError| parse_err("int8", &e))?;
            Ok(ScalarParam::Int8(v))
        }
        Type::FLOAT4 | Type::FLOAT8 => {
            let v: f64 = s
                .parse()
                .map_err(|e: std::num::ParseFloatError| parse_err("float", &e))?;
            Ok(ScalarParam::Float8(v))
        }
        Type::BOOL => {
            let lower = s.to_ascii_lowercase();
            let b = match lower.as_str() {
                "t" | "true" | "1" | "yes" | "y" | "on" => true,
                "f" | "false" | "0" | "no" | "n" | "off" => false,
                _ => {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22P02".to_owned(),
                        format!("invalid bool parameter {s:?}"),
                    ))));
                }
            };
            Ok(ScalarParam::Bool(b))
        }
        // PG one-dimensional array text format: `{1,2,3}` for numeric
        // arrays, `{a,"b,c"}` for text arrays.  We hand-roll a small
        // tokeniser that respects double-quoted elements and escaped
        // characters (the only escape PG uses is `\"` / `\\`). NULL
        // elements (`NULL` unquoted) become `ScalarParam::Null`.
        Type::INT2_ARRAY | Type::INT4_ARRAY | Type::INT8_ARRAY => {
            let elems = parse_pg_array_text(s)?;
            let mut out = Vec::with_capacity(elems.len());
            for raw in elems {
                match raw {
                    None => out.push(ScalarParam::Null),
                    Some(t) => {
                        let v: i64 = t
                            .parse()
                            .map_err(|e: std::num::ParseIntError| parse_err("int array element", &e))?;
                        out.push(ScalarParam::Int8(v));
                    }
                }
            }
            Ok(ScalarParam::Array(out))
        }
        Type::FLOAT4_ARRAY | Type::FLOAT8_ARRAY => {
            let elems = parse_pg_array_text(s)?;
            let mut out = Vec::with_capacity(elems.len());
            for raw in elems {
                match raw {
                    None => out.push(ScalarParam::Null),
                    Some(t) => {
                        let v: f64 = t.parse().map_err(|e: std::num::ParseFloatError| {
                            parse_err("float array element", &e)
                        })?;
                        out.push(ScalarParam::Float8(v));
                    }
                }
            }
            Ok(ScalarParam::Array(out))
        }
        Type::BOOL_ARRAY => {
            let elems = parse_pg_array_text(s)?;
            let mut out = Vec::with_capacity(elems.len());
            for raw in elems {
                match raw {
                    None => out.push(ScalarParam::Null),
                    Some(t) => {
                        let b = match t.to_ascii_lowercase().as_str() {
                            "t" | "true" | "1" => true,
                            "f" | "false" | "0" => false,
                            _ => {
                                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "22P02".to_owned(),
                                    format!("invalid bool array element {t:?}"),
                                ))))
                            }
                        };
                        out.push(ScalarParam::Bool(b));
                    }
                }
            }
            Ok(ScalarParam::Array(out))
        }
        Type::TEXT_ARRAY => {
            let elems = parse_pg_array_text(s)?;
            Ok(ScalarParam::Array(
                elems
                    .into_iter()
                    .map(|e| match e {
                        None => ScalarParam::Null,
                        Some(t) => ScalarParam::Text(t),
                    })
                    .collect(),
            ))
        }
        // TIMESTAMPTZ text format: ISO-8601 / PG-shaped literals, with or
        // without an explicit zone (JDBC and psycopg2 both send text-format
        // timestamptz). Parse to the typed Unix-epoch-micros variant so the
        // engine renders a canonical `'…+00'::timestamptz` literal; a shape
        // we can't parse falls back to verbatim text (the engine's
        // coercion then raises its canonical error — exactly the
        // pre-variant behaviour).
        Type::TIMESTAMPTZ => match parse_timestamptz_text_to_unix_micros(s) {
            Some(us) => Ok(ScalarParam::Timestamptz(us)),
            None => Ok(ScalarParam::Text(s.to_owned())),
        },
        // JSONB / UUID text format passes through verbatim — the engine's
        // INSERT path parses string literals back to the canonical Arrow
        // representation. Same default as every other unmapped type.
        _ => Ok(ScalarParam::Text(s.to_owned())),
    }
}

/// Parse a text-format TIMESTAMPTZ parameter into microseconds since the
/// Unix epoch (UTC). Accepts RFC3339 (`2026-01-02T03:04:05.123456Z`), the
/// PG-shaped zoned forms (`2026-01-02 03:04:05.123456+00`, `+00:00`,
/// space- or `T`-separated, with or without fraction), and the naive forms
/// (assumed UTC — Basin sessions are UTC-only). Returns `None` for anything
/// else so the caller can degrade to verbatim text.
fn parse_timestamptz_text_to_unix_micros(s: &str) -> Option<i64> {
    let t = s.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(t) {
        return Some(dt.with_timezone(&chrono::Utc).timestamp_micros());
    }
    // Zoned, PG-shaped (`%#z` matches `+00`, `+0000`, and `+00:00`).
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%#z",
    ] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(t, fmt) {
            return Some(dt.with_timezone(&chrono::Utc).timestamp_micros());
        }
    }
    // Naive (assume UTC). `%.f` also matches an empty fraction.
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(t, fmt) {
            return Some(
                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc)
                    .timestamp_micros(),
            );
        }
    }
    None
}

/// Parse a PostgreSQL one-dimensional array literal in text form:
/// `{a, b, NULL, "c,d"}`.  Returns `Some(element)` per cell or `None` for
/// the unquoted NULL keyword.  Multi-dimensional / explicit-bounds forms
/// (`[1:3]={…}`) are not supported — drivers rarely emit them in extended-
/// protocol binds.
fn parse_pg_array_text(s: &str) -> std::result::Result<Vec<Option<String>>, PgWireError> {
    let err = |msg: String| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22P02".to_owned(),
            msg,
        )))
    };
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'{' || bytes[bytes.len() - 1] != b'}' {
        return Err(err(format!(
            "text-format array must be wrapped in braces, got {s:?}"
        )));
    }
    let inner = &s[1..s.len() - 1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut out: Vec<Option<String>> = Vec::new();
    let bytes = inner.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Skip leading whitespace.
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        if bytes[i] == b'"' {
            // Quoted element — collect until the matching unescaped quote.
            i += 1;
            let mut buf = String::new();
            while i < bytes.len() {
                let b = bytes[i];
                if b == b'\\' && i + 1 < bytes.len() {
                    buf.push(bytes[i + 1] as char);
                    i += 2;
                    continue;
                }
                if b == b'"' {
                    i += 1;
                    break;
                }
                buf.push(b as char);
                i += 1;
            }
            out.push(Some(buf));
        } else {
            // Unquoted — terminator is `,` or end-of-string.
            let start = i;
            while i < bytes.len() && bytes[i] != b',' {
                i += 1;
            }
            let raw = inner[start..i].trim().to_owned();
            if raw.eq_ignore_ascii_case("null") {
                out.push(None);
            } else {
                out.push(Some(raw));
            }
        }
        // Skip whitespace, then the comma (or end).
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i < bytes.len() {
            if bytes[i] == b',' {
                i += 1;
            } else {
                return Err(err(format!(
                    "expected ',' or '}}' at byte {i} of array body {inner:?}"
                )));
            }
        }
    }
    Ok(out)
}

/// Decode the Postgres binary wire format for the common scalar types. The
/// formats are documented in
/// <https://www.postgresql.org/docs/current/protocol-message-formats.html>;
/// integers are big-endian fixed-width, floats are IEEE 754 big-endian, bool
/// is one byte, text/varchar/bytea are raw bytes (no length prefix — the
/// surrounding `Bind` message already framed them).
pub(crate) fn decode_param_binary(
    bytes: &[u8],
    declared: &Type,
) -> std::result::Result<ScalarParam, PgWireError> {
    let bad_len = |expected: usize| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22P03".to_owned(), // invalid_binary_representation
            format!(
                "binary parameter for {} expected {expected} bytes, got {}",
                declared.name(),
                bytes.len()
            ),
        )))
    };
    match *declared {
        Type::INT2 => {
            if bytes.len() != 2 {
                return Err(bad_len(2));
            }
            let v = i16::from_be_bytes([bytes[0], bytes[1]]);
            Ok(ScalarParam::Int4(v as i32))
        }
        Type::INT4 => {
            if bytes.len() != 4 {
                return Err(bad_len(4));
            }
            let v = i32::from_be_bytes(bytes.try_into().unwrap());
            Ok(ScalarParam::Int4(v))
        }
        Type::INT8 => {
            if bytes.len() != 8 {
                return Err(bad_len(8));
            }
            let v = i64::from_be_bytes(bytes.try_into().unwrap());
            Ok(ScalarParam::Int8(v))
        }
        Type::FLOAT4 => {
            if bytes.len() != 4 {
                return Err(bad_len(4));
            }
            let v = f32::from_be_bytes(bytes.try_into().unwrap());
            Ok(ScalarParam::Float8(v as f64))
        }
        Type::FLOAT8 => {
            if bytes.len() != 8 {
                return Err(bad_len(8));
            }
            let v = f64::from_be_bytes(bytes.try_into().unwrap());
            Ok(ScalarParam::Float8(v))
        }
        Type::BOOL => {
            if bytes.len() != 1 {
                return Err(bad_len(1));
            }
            Ok(ScalarParam::Bool(bytes[0] != 0))
        }
        Type::BYTEA => Ok(ScalarParam::Bytea(bytes.to_vec())),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            let s = std::str::from_utf8(bytes).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22021".to_owned(), // character_not_in_repertoire
                    format!("binary text parameter is not valid UTF-8: {e}"),
                )))
            })?;
            Ok(ScalarParam::Text(s.to_owned()))
        }
        // JSONB binary wire format: leading `0x01` version byte, then JSON
        // text. Strip it and hand the JSON to the engine as a string literal;
        // INSERT canonicalises and stores. Future versions would pick a new
        // byte; reject anything other than `0x01` with `0A000`.
        Type::JSONB => {
            if bytes.is_empty() {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22P03".to_owned(),
                    "binary JSONB parameter is empty (missing version byte)".into(),
                ))));
            }
            if bytes[0] != 1 {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "0A000".to_owned(), // feature_not_supported
                    format!(
                        "unsupported JSONB binary version 0x{:02x}; only v1 is supported",
                        bytes[0]
                    ),
                ))));
            }
            let s = std::str::from_utf8(&bytes[1..]).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22P02".to_owned(),
                    format!("binary JSONB body is not valid UTF-8: {e}"),
                )))
            })?;
            Ok(ScalarParam::Text(s.to_owned()))
        }
        // One-dimensional array binary wire format
        // (`int4[]` / `int8[]` / `text[]` / `bool[]` / `float8[]`):
        //   ndim:i32 has_nulls:i32 elem_oid:i32
        //   { dim_len:i32 dim_lbound:i32 } * ndim
        //   { elem_len:i32 (-1=NULL) elem_data:bytes(elem_len) } * total
        // tokio-postgres encodes `Vec<i64>` / `&[i64]` etc. exactly this way.
        // We decode by re-using the same `decode_param_binary` for the
        // element OID, dispatching off `elem_oid` from the header rather
        // than the declared array type so a client that disagrees with our
        // ParameterDescription doesn't silently misalign.
        Type::INT2_ARRAY
        | Type::INT4_ARRAY
        | Type::INT8_ARRAY
        | Type::FLOAT4_ARRAY
        | Type::FLOAT8_ARRAY
        | Type::BOOL_ARRAY
        | Type::TEXT_ARRAY => decode_pg_array_binary(bytes),
        // UUID binary wire format: 16 raw RFC 4122 bytes; render canonical
        // hyphenated form so the engine's text-substitution path (which
        // accepts string-quoted UUID literals) coerces back to bytes.
        Type::UUID => {
            if bytes.len() != 16 {
                return Err(bad_len(16));
            }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);
            let s = uuid::Uuid::from_bytes(arr).hyphenated().to_string();
            Ok(ScalarParam::Text(s))
        }
        // TIMESTAMP / TIMESTAMPTZ binary wire format: 8-byte big-endian i64
        // microseconds since the Postgres epoch 2000-01-01 00:00:00 UTC
        // (NOT the Unix epoch 1970-01-01). The rebase constant is shared
        // with the encode direction in `types.rs`.
        //
        // TIMESTAMPTZ uses the same encoding and always represents UTC; it
        // decodes to the typed `ScalarParam::Timestamptz` (Unix-epoch
        // micros) so the engine's bind-direct / AST / text substitution
        // routes all render it as a `'…+00'::timestamptz` literal. Naive
        // TIMESTAMP keeps the historical ISO-8601 text rendering (the
        // engine's text-substitution path parses it back as a naive UTC
        // timestamp).
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            if bytes.len() != 8 {
                return Err(bad_len(8));
            }
            let pg_usec = i64::from_be_bytes(bytes.try_into().unwrap());
            let unix_usec = pg_usec
                .checked_add(crate::types::PG_EPOCH_MICROS_FROM_UNIX)
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22P03".to_owned(),
                        "timestamp binary value overflows i64 microseconds".to_owned(),
                    )))
                })?;
            let secs = unix_usec.div_euclid(1_000_000);
            let nanos = (unix_usec.rem_euclid(1_000_000) * 1_000) as u32;
            let dt = chrono::Utc
                .timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22P03".to_owned(),
                        format!("timestamp binary value out of range: pg_usec={pg_usec}"),
                    )))
                })?;
            if *declared == Type::TIMESTAMPTZ {
                Ok(ScalarParam::Timestamptz(unix_usec))
            } else {
                let s = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
                Ok(ScalarParam::Text(s))
            }
        }
        // NUMERIC binary wire format (#118 — mirror of `encode_numeric_binary`
        // in `types.rs`). Wire body (big-endian, NO outer length prefix here
        // because the `Bind` framer already stripped it):
        //
        //   u16 ndigits   — number of base-10000 digits
        //   i16 weight    — positional weight of the first digit (units = 0)
        //   u16 sign      — 0x0000 positive, 0x4000 negative, 0xC000 NaN,
        //                   0xD000 -Infinity, 0xF000 +Infinity
        //   u16 dscale    — display scale (decimal digits after the point)
        //   u16 digits[ndigits]
        //
        // We materialise to a decimal string (`ScalarParam::Text`) because
        // `ScalarParam` has no Numeric variant; the engine's text-substitution
        // path quotes/coerces it as a NUMERIC literal. NaN / ±Infinity are
        // rejected with `22P03` because the encoder never emits them and the
        // engine's NUMERIC type cannot represent them.
        Type::NUMERIC => {
            const HEADER_LEN: usize = 8;
            if bytes.len() < HEADER_LEN {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22P03".to_owned(),
                    format!(
                        "binary NUMERIC parameter shorter than 8-byte header: got {} bytes",
                        bytes.len()
                    ),
                ))));
            }
            let ndigits = u16::from_be_bytes([bytes[0], bytes[1]]);
            let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
            let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
            let dscale = u16::from_be_bytes([bytes[6], bytes[7]]);
            let expected = HEADER_LEN + (ndigits as usize) * 2;
            if bytes.len() != expected {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22P03".to_owned(),
                    format!(
                        "binary NUMERIC body length mismatch: ndigits={ndigits} expects {expected} bytes, got {}",
                        bytes.len()
                    ),
                ))));
            }
            // Reject NaN / ±Infinity — the engine has no representation for
            // them and the encoder never emits them. Pos = 0x0000, Neg = 0x4000.
            const NUMERIC_POS: u16 = 0x0000;
            const NUMERIC_NEG: u16 = 0x4000;
            if sign != NUMERIC_POS && sign != NUMERIC_NEG {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22P03".to_owned(),
                    format!(
                        "binary NUMERIC sign 0x{sign:04x} (NaN / Infinity) not representable as a NUMERIC literal"
                    ),
                ))));
            }
            // Parse the base-10000 digit groups.
            let mut digits: Vec<u16> = Vec::with_capacity(ndigits as usize);
            for i in 0..ndigits as usize {
                let off = HEADER_LEN + i * 2;
                let d = u16::from_be_bytes([bytes[off], bytes[off + 1]]);
                if d >= 10000 {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22P03".to_owned(),
                        format!("binary NUMERIC digit group out of range: {d}"),
                    ))));
                }
                digits.push(d);
            }

            // Render to a decimal string. Each digit covers exactly 4 decimal
            // places. The digit at index `i` represents `digit * 10^((weight - i) * 4)`.
            // Strategy: build an integer-digit string from groups whose position
            // (`weight - i`) is >= 0, padding leading zeros for missing positions,
            // and a fractional-digit string from groups whose position is < 0
            // (also padded). Finally, trim or extend the fractional part to
            // exactly `dscale` decimal places to honour the declared scale.
            let mut int_part = String::new();
            let mut frac_part = String::new();
            // The most-significant integer position is `weight` (0 means units).
            // If `weight < 0` there are no integer digits at all.
            // The integer digit groups cover positions weight, weight-1, …, 0.
            if weight >= 0 {
                let int_groups_needed = (weight as usize) + 1;
                for pos in (0..int_groups_needed).rev() {
                    // Group index whose positional weight is `pos`:
                    //   i = weight - pos   (i is the index into `digits`)
                    let i_signed = weight as i32 - pos as i32;
                    let group = if (0..ndigits as i32).contains(&i_signed) {
                        digits[i_signed as usize]
                    } else {
                        0
                    };
                    if int_part.is_empty() {
                        // Most-significant group: no leading-zero pad.
                        int_part.push_str(&format!("{group}"));
                    } else {
                        int_part.push_str(&format!("{group:04}"));
                    }
                }
            }
            if int_part.is_empty() {
                int_part.push('0');
            }
            // Fractional digit groups cover positions -1, -2, … each group is
            // 4 decimal places. The number of frac groups we need to render to
            // cover `dscale` decimal places is `ceil(dscale / 4)`.
            let frac_groups_needed = (dscale as usize).div_ceil(4);
            for k in 0..frac_groups_needed {
                // Positional index of this frac group (negative): -1 - k.
                // Corresponding index into `digits`: i = weight - (-1 - k)
                //                                     = weight + 1 + k
                let i_signed = weight as i32 + 1 + k as i32;
                let group = if (0..ndigits as i32).contains(&i_signed) {
                    digits[i_signed as usize]
                } else {
                    0
                };
                frac_part.push_str(&format!("{group:04}"));
            }
            // Truncate / extend the frac string to exactly `dscale` digits.
            if frac_part.len() > dscale as usize {
                frac_part.truncate(dscale as usize);
            } else {
                while frac_part.len() < dscale as usize {
                    frac_part.push('0');
                }
            }
            let mut s = String::new();
            if sign == NUMERIC_NEG && !(int_part == "0" && frac_part.chars().all(|c| c == '0')) {
                s.push('-');
            }
            s.push_str(&int_part);
            if !frac_part.is_empty() {
                s.push('.');
                s.push_str(&frac_part);
            }
            Ok(ScalarParam::Text(s))
        }
        // DATE binary wire format: 4-byte big-endian i32 days since the
        // Postgres epoch 2000-01-01 (NOT Unix epoch 1970-01-01).
        Type::DATE => {
            if bytes.len() != 4 {
                return Err(bad_len(4));
            }
            let pg_days = i32::from_be_bytes(bytes.try_into().unwrap());
            // PG epoch is 2000-01-01; add/subtract pg_days to get the date.
            let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let date = if pg_days >= 0 {
                pg_epoch.checked_add_days(chrono::Days::new(pg_days as u64))
            } else {
                pg_epoch.checked_sub_days(chrono::Days::new((-pg_days) as u64))
            }
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "22P03".to_owned(),
                    format!("date binary value out of range: pg_days={pg_days}"),
                )))
            })?;
            let s = date.format("%Y-%m-%d").to_string();
            Ok(ScalarParam::Text(s))
        }
        _ => {
            // Unknown binary type: best-effort treat as raw text bytes. The
            // engine's text substitution will then SQL-escape them. If the
            // bytes are not UTF-8 we fail loudly rather than emit invalid SQL.
            let s = std::str::from_utf8(bytes).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "0A000".to_owned(),
                    format!(
                        "binary format for type {} not supported in v1: {e}",
                        declared.name()
                    ),
                )))
            })?;
            Ok(ScalarParam::Text(s.to_owned()))
        }
    }
}

/// Decode the PG binary wire format for a one-dimensional array.  The
/// element OID is read from the array header itself, so a client that
/// disagrees with our ParameterDescription (e.g. binds `int4[]` against a
/// slot we surfaced as `int8[]`) still decodes correctly rather than
/// silently misaligning.  Multi-dimensional arrays are rejected as
/// unsupported (no ORM emits them in extended-protocol binds in practice).
fn decode_pg_array_binary(bytes: &[u8]) -> std::result::Result<ScalarParam, PgWireError> {
    let short = || {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22P03".to_owned(),
            "binary array parameter is truncated".to_owned(),
        )))
    };
    if bytes.len() < 12 {
        return Err(short());
    }
    let ndim = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let _has_nulls = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
    let elem_oid = u32::from_be_bytes(bytes[8..12].try_into().unwrap());

    // Empty array: ndim == 0, no dim block, no elements.
    if ndim == 0 {
        return Ok(ScalarParam::Array(Vec::new()));
    }
    if ndim != 1 {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "0A000".to_owned(),
            format!("multi-dimensional arrays (ndim={ndim}) not supported"),
        ))));
    }

    let dim_block_end = 12 + 8; // 1 dim * (length:i32 + lbound:i32)
    if bytes.len() < dim_block_end {
        return Err(short());
    }
    let dim_len = i32::from_be_bytes(bytes[12..16].try_into().unwrap());
    if dim_len < 0 {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22P03".to_owned(),
            format!("binary array has negative length {dim_len}"),
        ))));
    }

    let elem_type = Type::from_oid(elem_oid).unwrap_or(Type::TEXT);

    let mut out = Vec::with_capacity(dim_len as usize);
    let mut pos = dim_block_end;
    for _ in 0..dim_len {
        if pos + 4 > bytes.len() {
            return Err(short());
        }
        let elen = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
        pos += 4;
        if elen < 0 {
            out.push(ScalarParam::Null);
            continue;
        }
        let elen = elen as usize;
        if pos + elen > bytes.len() {
            return Err(short());
        }
        let elem_bytes = &bytes[pos..pos + elen];
        pos += elen;
        // Recursively decode each element using the element OID from the
        // array header.  Nested arrays would re-enter this same path but
        // we already rejected ndim != 1 above.
        let v = decode_param_binary(elem_bytes, &elem_type)?;
        out.push(v);
    }
    Ok(ScalarParam::Array(out))
}

/// Detect whether `sql` is a DML statement with a `RETURNING` clause and, if
/// so, return the projected column schema plus the DML verb (for the
/// CommandComplete tag).
///
/// Strategy: parse the SQL with sqlparser, check for Insert / Update / Delete
/// with a non-empty `returning` vec, synthesise a safe
/// `SELECT <returning_items> FROM <table> WHERE false` probe query, execute it
/// against the engine (zero rows touched), and extract the Arrow schema from
/// the result. Returns `None` on any error so callers fall back to an empty
/// column list — the worst case is we fall back to the old `NoData` behaviour
/// (not a regression).
async fn probe_returning_schema<S>(
    session: &S,
    sql: &str,
) -> Option<(Vec<arrow_schema::Field>, String)>
where
    S: Session + ?Sized,
{
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    let stmt = stmts.into_iter().next()?;

    // Extract (table_name_sql, returning_items, dml_verb).
    let (table_sql, returning_items, dml_verb): (String, Vec<SelectItem>, &'static str) = match stmt
    {
        Statement::Insert(ins) => {
            let ret = ins.returning.clone()?; // None → no RETURNING → return None
            if ret.is_empty() {
                return None;
            }
            let table_sql = match &ins.table {
                sqlparser::ast::TableObject::TableName(name) => name.to_string(),
                _ => return None,
            };
            (table_sql, ret, "INSERT")
        }
        Statement::Update(upd) => {
            let ret = upd.returning.clone()?;
            if ret.is_empty() {
                return None;
            }
            let table_sql = match &upd.table.relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return None,
            };
            (table_sql, ret, "UPDATE")
        }
        Statement::Delete(del) => {
            let ret = del.returning.clone()?;
            if ret.is_empty() {
                return None;
            }
            let tables = match &del.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            let twj = tables.first()?;
            let table_sql = match &twj.relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return None,
            };
            (table_sql, ret, "DELETE")
        }
        _ => return None,
    };

    // Build a safe probe: SELECT <returning_items> FROM <table> WHERE false.
    // This never modifies any row; it only plans and executes a schema-discovery
    // query that returns 0 rows.
    let projection = returning_items
        .iter()
        .map(|it| it.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let probe_sql = format!("SELECT {projection} FROM {table_sql} WHERE false");

    match session.execute(&probe_sql).await {
        Ok(ExecResult::Rows { schema, .. }) => {
            let fields: Vec<arrow_schema::Field> =
                schema.fields().iter().map(|f| (**f).clone()).collect();
            if fields.is_empty() {
                None
            } else {
                Some((fields, dml_verb.to_owned()))
            }
        }
        _ => None,
    }
}

/// Compute the [`Response`] sequence for one Parse + Bind + Sync exchange.
/// Lives outside the pgwire trait so tests can pin its behaviour.
///
/// Returns the sequence of backend messages that should reach the client. On
/// any engine error the sequence collapses to a single `ErrorResponse`
/// followed by `ReadyForQuery` (the parser is not always asked for one — we
/// emit it only at sync points).
async fn handle_parse<S>(
    state: &Mutex<ExtendedState>,
    session: &S,
    msg: Parse,
) -> Vec<PgWireBackendMessage>
where
    S: Session + ?Sized,
{
    let name = msg.name.clone().unwrap_or_default();
    // PG protocol §55.2.3: extended-query Parse accepts exactly one
    // statement. Multi-statement SQL (e.g. "SELECT 1; SELECT 2") must be
    // rejected with SQLSTATE 42601 before any Bind/Execute so drivers
    // (pgx, JDBC, asyncpg) do not de-sync on an unexpected extra
    // RowDescription. Simple-query multi-statement (split before dispatch)
    // is unaffected — this guard is extended-query only.
    {
        let stmts = split_simple_query(&msg.query);
        if stmts.len() > 1 {
            let info = ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(), // syntax_error
                "cannot insert multiple commands into a prepared statement".to_owned(),
            );
            return vec![PgWireBackendMessage::ErrorResponse(info.into())];
        }
    }
    // COPY is intercepted before the engine; the engine's parser would
    // reject it, but we want it to flow through Bind/Execute so
    // tokio-postgres' `copy_in` / `copy_out` (which always use the
    // extended-query path) work.
    match crate::copy::parse_copy(&msg.query) {
        Ok(Some(cmd)) => {
            // Front-load engine-dependent validation (column list against the
            // table schema, file-path allowlist) at Parse time so failures
            // surface BEFORE Bind/Execute. Real Postgres parses COPY fully at
            // Parse time and never reaches Execute on bad input. tokio-
            // postgres' `copy_in` returns Err mid-Bind+Execute+Sync, then
            // emits CopyFail+Sync onto the wire (its CopyInReceiver does this
            // when the user-side sender drops). That CopyFail+Sync would
            // produce a second ReadyForQuery and confuse the driver
            // ("unexpected message"). Failing at Parse short-circuits before
            // Bind/Execute is ever sent.
            if let Err(emsg) = validate_copy_at_parse(session, &cmd).await {
                let (sqlstate, message) = decode_copy_error(&emsg);
                let info =
                    ErrorInfo::new("ERROR".to_owned(), sqlstate.to_owned(), message.to_owned());
                return vec![PgWireBackendMessage::ErrorResponse(info.into())];
            }
            let mut g = state.lock().await;
            g.copy_statements.insert(name.clone(), cmd);
            // Erase any prior engine-side statement under the same name
            // so Bind/Execute see the COPY entry, not a stale handle.
            g.statements.remove(&name);
            return vec![PgWireBackendMessage::ParseComplete(ParseComplete::new())];
        }
        Ok(None) => {}
        Err(emsg) => {
            let (sqlstate, message) = decode_copy_error(&emsg);
            let info = ErrorInfo::new("ERROR".to_owned(), sqlstate.to_owned(), message.to_owned());
            return vec![PgWireBackendMessage::ErrorResponse(info.into())];
        }
    }
    match session.prepare(&msg.query).await {
        Ok((handle, mut schema)) => {
            // basin-engine's `probe_schema` returns an empty column list for
            // DML statements (INSERT / UPDATE / DELETE) because DataFusion
            // cannot plan them as SELECT-style queries at prepare time. When
            // the statement carries a RETURNING clause the router must bridge
            // this gap: without a column list, `handle_describe` emits
            // `NoData` instead of a `RowDescription`, and drivers like
            // `tokio-postgres` then cache a 0-column schema — producing the
            // observed "0-column DataRow" bug even though the engine correctly
            // emits the RETURNING rows at execute time.
            //
            // Fix: if the engine returned no columns, check whether the SQL
            // has a RETURNING clause. If it does, synthesise a safe
            // `SELECT <returning_items> FROM <table> WHERE false` probe query,
            // execute it (0 rows affected / returned), and use its schema to
            // populate `schema.columns`. The probe is side-effect-free.
            let mut dml_tag_prefix: Option<String> = None;
            if schema.columns.is_empty() {
                if let Some((fields, verb)) = probe_returning_schema(session, &msg.query).await {
                    schema.columns = fields;
                    dml_tag_prefix = Some(verb);
                    tracing::debug!(
                        dml_verb = %dml_tag_prefix.as_deref().unwrap_or(""),
                        cols = schema.columns.len(),
                        "RETURNING schema probed at parse time"
                    );
                }
            }
            let mut g = state.lock().await;
            g.copy_statements.remove(&name);
            g.statements.insert(
                name,
                StatementEntry {
                    handle,
                    schema,
                    sql: msg.query,
                    dml_tag_prefix,
                },
            );
            vec![PgWireBackendMessage::ParseComplete(ParseComplete::new())]
        }
        Err(e) => {
            tracing::warn!(error = %e, "parse failed");
            vec![PgWireBackendMessage::ErrorResponse(error_response(&e))]
        }
    }
}

/// Validate a COPY command's table/column list / file-path against the engine
/// at Parse time. Surfaces SQLSTATE 42601 errors so they never reach
/// Bind/Execute, which keeps the wire-protocol exchange clean (no
/// CopyFail+Sync follow-up from tokio-postgres' CopyInReceiver). For STDIN/
/// STDOUT-only commands without a column list this is a no-op.
///
/// `PROGRAM` and file-path-gate errors are encoded in the returned string with
/// a `"\x0042501\x00"` prefix to signal SQLSTATE 42501 to the caller.
async fn validate_copy_at_parse<S>(
    session: &S,
    cmd: &crate::copy::CopyCommand,
) -> std::result::Result<(), String>
where
    S: Session + ?Sized,
{
    match cmd {
        crate::copy::CopyCommand::QueryTo { .. } => {
            // Query-source COPY: no table/column validation at parse time.
            return Ok(());
        }
        crate::copy::CopyCommand::From {
            table,
            columns,
            path,
            opts,
            ..
        } => {
            if let Some(p) = path {
                crate::copy::validate_copy_path(p)?;
            }
            let is_binary = opts.format == crate::copy::CopyFormat::Binary;
            if columns.is_some() || is_binary {
                let table_cols = crate::copy::resolve_table_columns(session, table)
                    .await
                    .map_err(|e| format!("COPY: {e}"))?;
                if table_cols.is_empty() {
                    return Err(format!("COPY: cannot resolve schema of {table:?}"));
                }
                let cols =
                    crate::copy::select_copy_in_columns(table, &table_cols, columns.as_deref())?;
                if is_binary {
                    // Prefix with the 0A000 sqlstate marker so the caller
                    // surfaces feature_not_supported rather than 42601.
                    crate::copy::validate_binary_copy_columns(cols.iter())
                        .map_err(|m| format!("\x000A000\x00{m}"))?;
                }
            }
        }
        crate::copy::CopyCommand::To {
            table,
            columns,
            path,
            opts,
            ..
        } => {
            if let Some(p) = path {
                crate::copy::validate_copy_path(p)?;
            }
            let is_binary = opts.format == crate::copy::CopyFormat::Binary;
            if columns.is_some() || is_binary {
                let table_cols = crate::copy::resolve_table_columns(session, table)
                    .await
                    .map_err(|e| format!("COPY: {e}"))?;
                if table_cols.is_empty() {
                    return Err(format!("COPY: cannot resolve schema of {table:?}"));
                }
                let cols =
                    crate::copy::select_copy_out_columns(table, &table_cols, columns.as_deref())?;
                if is_binary {
                    // Prefix with the 0A000 sqlstate marker so the caller
                    // surfaces feature_not_supported rather than 42601.
                    crate::copy::validate_binary_copy_columns(cols.iter())
                        .map_err(|m| format!("\x000A000\x00{m}"))?;
                }
            }
        }
    }
    Ok(())
}

/// Decode an error string that may carry a `"\x0042501\x00<msg>"` prefix
/// emitted by `parse_copy` / `validate_copy_path` for SQLSTATE 42501 errors.
/// Returns `(sqlstate, message)`.
fn decode_copy_error(msg: &str) -> (&str, &str) {
    if let Some(rest) = msg.strip_prefix('\x00') {
        // Format: "<sqlstate>\x00<message>"
        if let Some(idx) = rest.find('\x00') {
            return (&rest[..idx], &rest[idx + 1..]);
        }
    }
    ("42601", msg)
}

async fn handle_bind<S>(
    state: &Mutex<ExtendedState>,
    session: &S,
    msg: Bind,
) -> Vec<PgWireBackendMessage>
where
    S: Session + ?Sized,
{
    let stmt_name = msg.statement_name.clone().unwrap_or_default();
    let portal_name = msg.portal_name.clone().unwrap_or_default();

    // COPY-shape statements have no parameters; skip the param-decoding
    // path and stash the command on the COPY-portal map. Either way, the
    // bind for a portal name overwrites whatever was there before — so
    // clear both maps under that portal name to keep them disjoint.
    {
        let mut g = state.lock().await;
        if let Some(cmd) = g.copy_statements.get(&stmt_name).cloned() {
            g.portals.remove(&portal_name);
            g.copy_portals.insert(portal_name, cmd);
            return vec![PgWireBackendMessage::BindComplete(BindComplete::new())];
        }
        // Non-COPY rebind under this portal name: drop any stale COPY
        // entry so Execute doesn't accidentally route to the COPY path.
        g.copy_portals.remove(&portal_name);
    }

    let entry = {
        let g = state.lock().await;
        match g.statements.get(&stmt_name) {
            Some(e) => e.clone(),
            None => {
                let info = ErrorInfo::new(
                    "ERROR".to_owned(),
                    "26000".to_owned(), // invalid_sql_statement_name
                    format!("prepared statement {stmt_name:?} not found"),
                );
                return vec![PgWireBackendMessage::ErrorResponse(info.into())];
            }
        }
    };

    if msg.parameters.len() != entry.schema.param_types.len() {
        let info = ErrorInfo::new(
            "ERROR".to_owned(),
            "08P01".to_owned(), // protocol_violation
            format!(
                "bind: prepared statement {stmt_name:?} expects {} parameters, got {}",
                entry.schema.param_types.len(),
                msg.parameters.len()
            ),
        );
        return vec![PgWireBackendMessage::ErrorResponse(info.into())];
    }

    // Pick the format code per parameter. If the client supplied a single
    // code it applies to all parameters; otherwise one code per parameter;
    // empty list means text for every slot.
    let mut params: Vec<ScalarParam> = Vec::with_capacity(msg.parameters.len());
    for (i, raw) in msg.parameters.iter().enumerate() {
        // Mirror the OID surfaced in ParameterDescription: JSONB / UUID
        // flags take precedence so `decode_param` sees the right type and
        // routes the binary wire format through the proper path (rather
        // than the BYTEA / TEXT catch-all that would corrupt the bytes).
        let declared = if entry.schema.param_is_jsonb.get(i).copied().unwrap_or(false) {
            Type::JSONB
        } else if entry.schema.param_is_uuid.get(i).copied().unwrap_or(false) {
            Type::UUID
        } else {
            arrow_to_pg_type(&entry.schema.param_types[i])
        };
        let is_binary = match msg.parameter_format_codes.len() {
            0 => false,
            1 => msg.parameter_format_codes[0] == 1,
            _ => msg.parameter_format_codes.get(i).copied().unwrap_or(0) == 1,
        };
        match raw {
            None => params.push(ScalarParam::Null),
            Some(bytes) => match decode_param(bytes.as_ref(), &declared, is_binary) {
                Ok(p) => params.push(p),
                Err(PgWireError::UserError(info)) => {
                    return vec![PgWireBackendMessage::ErrorResponse((*info).into())];
                }
                Err(_) => {
                    let info = ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22P02".to_owned(),
                        format!("could not decode parameter {}", i + 1),
                    );
                    return vec![PgWireBackendMessage::ErrorResponse(info.into())];
                }
            },
        }
    }

    // Detect write-DML at bind time so `handle_execute` can gate the
    // commit-conflict retry without a keyword scan on every Execute.
    let is_write_dml = sql_is_write_dml(&entry.sql);

    match session.bind(&entry.handle, params).await {
        Ok(bound) => {
            state.lock().await.portals.insert(
                portal_name,
                PortalEntry {
                    bound,
                    schema: entry.schema,
                    result_format_codes: msg.result_column_format_codes,
                    dml_tag_prefix: entry.dml_tag_prefix,
                    is_write_dml,
                },
            );
            vec![PgWireBackendMessage::BindComplete(BindComplete::new())]
        }
        Err(e) => {
            tracing::warn!(error = %e, "bind failed");
            vec![PgWireBackendMessage::ErrorResponse(error_response(&e))]
        }
    }
}

async fn handle_describe(state: &Mutex<ExtendedState>, msg: Describe) -> Vec<PgWireBackendMessage> {
    let name = msg.name.clone().unwrap_or_default();
    let g = state.lock().await;
    match msg.target_type {
        TARGET_TYPE_BYTE_STATEMENT => {
            // COPY: no parameters, no result rows. ParameterDescription
            // (empty) + NoData keeps strict drivers like JDBC happy.
            if g.copy_statements.contains_key(&name) {
                return vec![
                    PgWireBackendMessage::ParameterDescription(ParameterDescription::new(
                        Vec::new(),
                    )),
                    PgWireBackendMessage::NoData(NoData::new()),
                ];
            }
            let entry = match g.statements.get(&name) {
                Some(e) => e.clone(),
                None => {
                    let info = ErrorInfo::new(
                        "ERROR".to_owned(),
                        "26000".to_owned(),
                        format!("prepared statement {name:?} not found"),
                    );
                    return vec![PgWireBackendMessage::ErrorResponse(info.into())];
                }
            };
            drop(g);
            let mut out = Vec::with_capacity(2);
            // Always send ParameterDescription for a statement-describe so
            // drivers that expect it (e.g. JDBC) keep going. JSONB / UUID
            // slots take their dedicated OIDs from the parallel flag vecs;
            // strict drivers like asyncpg + pgx refuse to bind native
            // `uuid.UUID` / `dict` values into BYTEA / TEXT slots, so we
            // must advertise OID 3802 / 2950 here, not the Arrow-derived
            // BYTEA / TEXT.
            let oids: Vec<u32> = entry
                .schema
                .param_types
                .iter()
                .enumerate()
                .map(|(i, dt)| {
                    if entry.schema.param_is_jsonb.get(i).copied().unwrap_or(false) {
                        Type::JSONB.oid()
                    } else if entry.schema.param_is_uuid.get(i).copied().unwrap_or(false) {
                        Type::UUID.oid()
                    } else {
                        arrow_to_pg_type(dt).oid()
                    }
                })
                .collect();
            out.push(PgWireBackendMessage::ParameterDescription(
                ParameterDescription::new(oids),
            ));
            if entry.schema.columns.is_empty() {
                out.push(PgWireBackendMessage::NoData(NoData::new()));
            } else {
                let schema = arrow_schema::Schema::new(entry.schema.columns.clone());
                out.push(PgWireBackendMessage::RowDescription(row_description(
                    &schema,
                )));
            }
            out
        }
        TARGET_TYPE_BYTE_PORTAL => {
            // COPY portals have no row description.
            if g.copy_portals.contains_key(&name) {
                return vec![PgWireBackendMessage::NoData(NoData::new())];
            }
            let entry = match g.portals.get(&name) {
                Some(e) => e.clone(),
                None => {
                    let info = ErrorInfo::new(
                        "ERROR".to_owned(),
                        "34000".to_owned(), // invalid_cursor_name
                        format!("portal {name:?} not found"),
                    );
                    return vec![PgWireBackendMessage::ErrorResponse(info.into())];
                }
            };
            drop(g);
            if entry.schema.columns.is_empty() {
                vec![PgWireBackendMessage::NoData(NoData::new())]
            } else {
                let schema = arrow_schema::Schema::new(entry.schema.columns.clone());
                // Advertise the format codes this portal's rows will actually be
                // encoded in (the Bind result-format codes), so the client
                // decodes binary/text columns correctly — see
                // `row_description_with_formats`.
                vec![PgWireBackendMessage::RowDescription(
                    row_description_with_formats(&schema, &entry.result_format_codes),
                )]
            }
        }
        other => {
            let info = ErrorInfo::new(
                "ERROR".to_owned(),
                "08P01".to_owned(),
                format!("invalid Describe target byte: {other}"),
            );
            vec![PgWireBackendMessage::ErrorResponse(info.into())]
        }
    }
}

/// Outcome of an `Execute`. `messages` are fed verbatim. `copy_in_active`
/// signals the simple-query / extended path to flip into
/// `CopyInProgress(true)` so subsequent `CopyData` chunks route to the
/// `CopyHandler`. `copy_in_state`, when present, is the staging state to
/// hand to the shared per-connection slot. `is_error` is set when the
/// outcome carries an `ErrorResponse`; the caller flips the connection
/// into `AwaitingSync` so subsequent extended-query messages are
/// discarded until the client's `Sync` arrives (Postgres FE/BE protocol
/// requirement: "discard messages until Sync is reached").
pub(crate) struct ExecuteOutcome {
    pub(crate) messages: Vec<PgWireBackendMessage>,
    pub(crate) start_copy_in: Option<crate::copy::CopyInState>,
    pub(crate) is_error: bool,
}

impl ExecuteOutcome {
    /// Single ErrorResponse, no copy-in start; flips caller into
    /// `AwaitingSync` so the protocol matches PG's extended-query error
    /// recovery rules.
    fn error(err: pgwire::messages::response::ErrorResponse) -> Self {
        Self {
            messages: vec![PgWireBackendMessage::ErrorResponse(err)],
            start_copy_in: None,
            is_error: true,
        }
    }

    fn error_info(info: ErrorInfo) -> Self {
        Self::error(info.into())
    }
}

async fn handle_execute<S>(
    state: &Mutex<ExtendedState>,
    session: &S,
    msg: Execute,
) -> ExecuteOutcome
where
    S: Session + ?Sized,
{
    let portal_name = msg.name.clone().unwrap_or_default();
    // COPY portals: route to the dedicated path. We don't talk to the
    // engine for the "execute" itself — the row work happens later in
    // CopyHandler::on_copy_data (FROM) or right here, synchronously (TO).
    let copy_cmd = {
        let g = state.lock().await;
        g.copy_portals.get(&portal_name).cloned()
    };
    if let Some(cmd) = copy_cmd {
        return execute_copy(session, cmd).await;
    }
    let entry = {
        let g = state.lock().await;
        match g.portals.get(&portal_name) {
            Some(e) => e.clone(),
            None => {
                let info = ErrorInfo::new(
                    "ERROR".to_owned(),
                    "34000".to_owned(),
                    format!("portal {portal_name:?} not found"),
                );
                return ExecuteOutcome::error_info(info);
            }
        }
    };

    // ── Execute with commit-conflict retry ───────────────────────────────────
    // For write-DML portals (INSERT / UPDATE / DELETE) the engine may return
    // `BasinError::CommitConflict` when two writers race on the same snapshot.
    // We retry transparently with exponential backoff + jitter rather than
    // immediately surfacing SQLSTATE 40001 to the client.  The `BoundStatement`
    // already contains the fully parameter-substituted SQL, so each retry is
    // semantically identical to the first execution.
    //
    // Correctness guard: only `CommitConflict` triggers retry.  All other
    // errors — including `UniqueViolation` (23505), `CheckViolation`,
    // `ForeignKeyViolation`, `NotFound`, etc. — surface immediately, preserving
    // their original SQLSTATE.  `UniqueViolation` is a schema constraint, not a
    // snapshot race, and must NOT be retried.
    let exec_result = if entry.is_write_dml {
        execute_with_conflict_retry(session, entry.bound.clone()).await
    } else {
        session.execute_bound(entry.bound).await
    };

    let mut out = Vec::new();
    let mut is_error = false;
    match exec_result {
        Ok(ExecResult::Empty { tag }) => {
            out.push(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                tag,
            )));
        }
        Ok(ExecResult::Rows { schema, batches }) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            let cap = if msg.max_rows > 0 {
                msg.max_rows as usize
            } else {
                usize::MAX
            };
            // Encode rows respecting the per-column format codes from the
            // Bind. A driver like `tokio-postgres` defaults to all-binary;
            // psql defaults to all-text. Doing this right is the difference
            // between green and "garbled bytes."
            let rows_result = crate::types::encode_batches_with_formats(
                &schema,
                &batches,
                &entry.result_format_codes,
            );
            let rows = match rows_result {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error = %e, "encode failed");
                    return ExecuteOutcome::error(error_response(&e));
                }
            };
            let mut emitted = 0usize;
            for row in rows.into_iter() {
                if emitted >= cap {
                    break;
                }
                out.push(PgWireBackendMessage::DataRow(row));
                emitted += 1;
            }
            if msg.max_rows > 0 && emitted < total {
                out.push(PgWireBackendMessage::PortalSuspended(
                    pgwire::messages::extendedquery::PortalSuspended::new(),
                ));
            } else {
                // For DML … RETURNING portals emit the correct Postgres-style
                // command tag so drivers that inspect it (JDBC, libpq checks)
                // see "INSERT 0 N" / "UPDATE N" / "DELETE N" rather than the
                // generic "SELECT N" this branch would otherwise produce.
                let tag = match entry.dml_tag_prefix.as_deref() {
                    Some("INSERT") => format!("INSERT 0 {emitted}"),
                    Some(verb) => format!("{verb} {emitted}"),
                    None => format!("SELECT {emitted}"),
                };
                out.push(PgWireBackendMessage::CommandComplete(CommandComplete::new(
                    tag,
                )));
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "execute failed");
            out.push(PgWireBackendMessage::ErrorResponse(error_response(&e)));
            is_error = true;
        }
    }
    ExecuteOutcome {
        messages: out,
        start_copy_in: None,
        is_error,
    }
}

/// Execute a write-DML [`BoundStatement`] with transparent exponential-backoff
/// retry on `CommitConflict`.
///
/// Backoff schedule (before jitter): 1 ms, 2 ms, 4 ms, 8 ms, 16 ms, 32 ms.
/// Jitter: uniform random 0–50 % of the base delay added to each interval to
/// avoid thundering-herd re-synchronisation between concurrent writers.
///
/// On exhaustion returns the last `CommitConflict` error, which the router
/// maps to SQLSTATE 40001 (`serialization_failure`).  The caller should not
/// call this for non-write-DML statements.
async fn execute_with_conflict_retry<S>(
    session: &S,
    bound: BoundStatement,
) -> basin_common::Result<ExecResult>
where
    S: Session + ?Sized,
{
    use basin_common::BasinError;
    use rand::Rng as _;

    let mut attempt = 0u32;
    loop {
        // Clone the BoundStatement for all but the last attempt so the last
        // one can consume by-value (avoids a redundant clone on success).
        let stmt = if attempt < COMMIT_CONFLICT_MAX_RETRIES {
            bound.clone()
        } else {
            // Final attempt: consume the original.
            bound.clone()
        };
        match session.execute_bound(stmt).await {
            // ── Success ───────────────────────────────────────────────────────
            Ok(result) => return Ok(result),

            // ── Snapshot conflict — eligible for retry ─────────────────────
            Err(BasinError::CommitConflict(_)) if attempt < COMMIT_CONFLICT_MAX_RETRIES => {
                attempt += 1;
                COMMIT_CONFLICT_RETRY_COUNT.fetch_add(1, Ordering::Relaxed);

                // Exponential base: 1, 2, 4, 8, 16, 32 ms.
                let base_ms = COMMIT_CONFLICT_BASE_MS << (attempt - 1);
                // Jitter: 0..=50 % of base_ms.
                let jitter_ms = rand::thread_rng().gen_range(0..=(base_ms / 2).max(1));
                let delay = Duration::from_millis(base_ms + jitter_ms);

                tracing::debug!(
                    attempt,
                    delay_ms = delay.as_millis(),
                    "commit conflict; retrying write-DML"
                );
                tokio::time::sleep(delay).await;
            }

            // ── Conflict exhausted — give up ───────────────────────────────
            Err(e @ BasinError::CommitConflict(_)) => {
                COMMIT_CONFLICT_EXHAUSTED_COUNT.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    attempt,
                    max = COMMIT_CONFLICT_MAX_RETRIES,
                    "commit conflict: max retries exhausted, returning 40001"
                );
                return Err(e);
            }

            // ── Any other error — propagate immediately ────────────────────
            // This includes UniqueViolation (23505), CheckViolation, etc.
            // None of those are snapshot races; retrying would be wrong.
            Err(other) => return Err(other),
        }
    }
}

/// Execute a COPY portal: COPY FROM seeds the per-connection staging
/// state and asks the caller to flip into `CopyInProgress(true)`; COPY
/// TO emits the entire CopyOut sequence inline.
async fn execute_copy<S>(session: &S, cmd: crate::copy::CopyCommand) -> ExecuteOutcome
where
    S: Session + ?Sized,
{
    match cmd {
        crate::copy::CopyCommand::From {
            table,
            with_header,
            columns,
            path,
            opts,
        } => {
            let table_cols = match crate::copy::resolve_table_columns(session, &table).await {
                Ok(c) if !c.is_empty() => c,
                Ok(_) => {
                    let info = ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42P01".to_owned(),
                        format!("COPY FROM: cannot resolve schema of {table:?}"),
                    );
                    return ExecuteOutcome::error_info(info);
                }
                Err(e) => {
                    return ExecuteOutcome::error(error_response(&e));
                }
            };
            let cols = match crate::copy::select_copy_in_columns(
                &table,
                &table_cols,
                columns.as_deref(),
            ) {
                Ok(c) => c,
                Err(msg) => {
                    let info = ErrorInfo::new("ERROR".to_owned(), "42601".to_owned(), msg);
                    return ExecuteOutcome::error_info(info);
                }
            };
            if opts.format == crate::copy::CopyFormat::Binary {
                if let Err(msg) = crate::copy::validate_binary_copy_columns(cols.iter()) {
                    let info = ErrorInfo::new("ERROR".to_owned(), "0A000".to_owned(), msg);
                    return ExecuteOutcome::error_info(info);
                }
            }
            let full_schema = std::sync::Arc::new(arrow_schema::Schema::new(
                table_cols.iter().cloned().collect::<Vec<_>>(),
            ));
            let mut state = crate::copy::CopyInState::new(
                table.clone(),
                full_schema,
                cols,
                with_header,
                opts,
            );
            if let Some(list) = columns.clone() {
                state = state.with_column_list(list);
            }
            // File-path COPY FROM: read+drive synchronously, no CopyIn wire flip.
            if let Some(p) = path {
                let resolved = match crate::copy::validate_copy_path(&p) {
                    Ok(pb) => pb,
                    Err(emsg) => {
                        let (sqlstate, message) = decode_copy_error(&emsg);
                        let info = ErrorInfo::new(
                            "ERROR".to_owned(),
                            sqlstate.to_owned(),
                            message.to_owned(),
                        );
                        return ExecuteOutcome::error_info(info);
                    }
                };
                if let Err(e) = crate::copy::copy_from_file(session, &mut state, &resolved).await {
                    return ExecuteOutcome::error(error_response(&e));
                }
                if let Some(err) = state.error.take() {
                    let info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), err);
                    return ExecuteOutcome::error_info(info);
                }
                return ExecuteOutcome {
                    messages: vec![PgWireBackendMessage::CommandComplete(
                        pgwire::messages::response::CommandComplete::new(format!(
                            "COPY {}",
                            state.row_count
                        )),
                    )],
                    start_copy_in: None,
                    is_error: false,
                };
            }
            let n = state.columns.len();
            let format = state.opts.format;
            ExecuteOutcome {
                messages: crate::copy::copy_from_stdin_messages(n, format),
                start_copy_in: Some(state),
                is_error: false,
            }
        }
        crate::copy::CopyCommand::To {
            table,
            with_header,
            columns,
            path,
            opts,
        } => {
            if let Some(p) = path {
                let resolved = match crate::copy::validate_copy_path(&p) {
                    Ok(pb) => pb,
                    Err(emsg) => {
                        let (sqlstate, message) = decode_copy_error(&emsg);
                        let info = ErrorInfo::new(
                            "ERROR".to_owned(),
                            sqlstate.to_owned(),
                            message.to_owned(),
                        );
                        return ExecuteOutcome::error_info(info);
                    }
                };
                let count = match crate::copy::copy_to_file(
                    session,
                    &table,
                    columns.as_deref(),
                    &resolved,
                    with_header,
                    &opts,
                )
                .await
                {
                    Ok(c) => c,
                    Err(e) => return ExecuteOutcome::error(error_response(&e)),
                };
                return ExecuteOutcome {
                    messages: vec![PgWireBackendMessage::CommandComplete(
                        pgwire::messages::response::CommandComplete::new(format!("COPY {count}")),
                    )],
                    start_copy_in: None,
                    is_error: false,
                };
            }
            // Reuse the simple-query path's emitter, but strip the
            // trailing ReadyForQuery — Sync sends it in extended mode.
            let mut msgs = crate::copy::copy_to_stdout_messages(
                session,
                &table,
                columns.as_deref(),
                with_header,
                &opts,
            )
            .await;
            if matches!(msgs.last(), Some(PgWireBackendMessage::ReadyForQuery(_))) {
                msgs.pop();
            }
            // If the result starts with ErrorResponse (e.g. column-list
            // validation against the engine's resolved schema failed)
            // surface that as an error outcome so the framework flips
            // the connection into AwaitingSync.
            let errored = matches!(msgs.first(), Some(PgWireBackendMessage::ErrorResponse(_)));
            ExecuteOutcome {
                messages: msgs,
                start_copy_in: None,
                is_error: errored,
            }
        }
        crate::copy::CopyCommand::QueryTo {
            query,
            with_header,
            opts,
        } => {
            // Run the subquery and stream the result as CSV to STDOUT.
            let mut msgs =
                crate::copy::copy_query_to_stdout_messages(session, &query, with_header, &opts)
                    .await;
            if matches!(msgs.last(), Some(PgWireBackendMessage::ReadyForQuery(_))) {
                msgs.pop();
            }
            let errored = matches!(msgs.first(), Some(PgWireBackendMessage::ErrorResponse(_)));
            ExecuteOutcome {
                messages: msgs,
                start_copy_in: None,
                is_error: errored,
            }
        }
    }
}

async fn handle_close<S>(
    state: &Mutex<ExtendedState>,
    session: &S,
    msg: Close,
) -> Vec<PgWireBackendMessage>
where
    S: Session + ?Sized,
{
    let name = msg.name.clone().unwrap_or_default();
    match msg.target_type {
        TARGET_TYPE_BYTE_STATEMENT => {
            let mut g = state.lock().await;
            g.copy_statements.remove(&name);
            let removed = g.statements.remove(&name);
            drop(g);
            if let Some(e) = removed {
                session.close_statement(&e.handle).await;
            }
        }
        TARGET_TYPE_BYTE_PORTAL => {
            let mut g = state.lock().await;
            g.copy_portals.remove(&name);
            g.portals.remove(&name);
        }
        _ => {}
    }
    vec![PgWireBackendMessage::CloseComplete(CloseComplete::new())]
}

/// Extended-query handler. One instance per connection. The handler owns the
/// per-connection statement / portal map and forwards SQL to its session.
pub(crate) struct BasinExtendedQueryHandler<S: Session + 'static> {
    session_slot: Arc<Mutex<Option<Arc<S>>>>,
    state: Arc<Mutex<ExtendedState>>,
    /// See [`BasinSimpleQueryHandlerSlot::rate_limit`]; identical
    /// semantics — `None` disables, `Some` enforces per-project qps.
    rate_limit: Option<Arc<crate::rate_limit::PgRateLimit>>,
    /// Per-connection in-flight `COPY FROM STDIN` slot. Shared with
    /// `BasinSimpleQueryHandlerSlot` so the framework's `CopyHandler`
    /// (the simple-query slot impl) sees the same staging state whether
    /// COPY came in over simple or extended protocol. `None` always for
    /// non-COPY statements.
    pub(crate) copy_state: crate::copy::CopyStateSlot,
}

impl<S: Session + 'static> BasinExtendedQueryHandler<S> {
    pub(crate) fn new(
        session_slot: Arc<Mutex<Option<Arc<S>>>>,
        rate_limit: Option<Arc<crate::rate_limit::PgRateLimit>>,
        copy_state: crate::copy::CopyStateSlot,
    ) -> Self {
        Self {
            session_slot,
            state: Arc::new(Mutex::new(ExtendedState::new())),
            rate_limit,
            copy_state,
        }
    }

    async fn require_session(&self) -> PgWireResult<Arc<S>> {
        let g = self.session_slot.lock().await;
        g.clone().ok_or_else(|| {
            PgWireError::ApiError(
                "session not bound to connection (startup did not complete)".into(),
            )
        })
    }
}

#[async_trait]
impl<S: Session + 'static> ExtendedQueryHandler for BasinExtendedQueryHandler<S> {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.require_session().await?;
        for m in handle_parse(self.state.as_ref(), session.as_ref(), message).await {
            client.feed(m).await?;
        }
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.require_session().await?;
        for m in handle_bind(self.state.as_ref(), session.as_ref(), message).await {
            client.feed(m).await?;
        }
        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        client.set_state(PgWireConnectionState::QueryInProgress);
        let session = self.require_session().await?;
        // Per-project rate limit. Same shape as the simple-query path:
        // emit one ErrorResponse + transition back to Idle when the
        // bucket is dry. We don't emit ReadyForQuery here because the
        // pgwire extended-query state machine will emit one after Sync.
        if let Some(rl) = &self.rate_limit {
            if rl.check(&session.project()).is_err() {
                client
                    .feed(PgWireBackendMessage::ErrorResponse(
                        crate::error::rate_limit_exceeded_response(),
                    ))
                    .await?;
                client.set_state(PgWireConnectionState::ReadyForQuery);
                return Ok(());
            }
        }
        let outcome = handle_execute(self.state.as_ref(), session.as_ref(), message).await;
        let errored = outcome.is_error;
        for m in outcome.messages {
            client.feed(m).await?;
        }
        if let Some(st) = outcome.start_copy_in {
            // COPY FROM STDIN over the extended path: stash the staging
            // state on the slot the CopyHandler reads from, then flip
            // into CopyInProgress(true). The framework will route the
            // following CopyData / CopyDone through `on_copy_data`
            // / `on_copy_done`. The trailing Sync is handled later
            // (after `on_copy_done` resets state to ReadyForQuery).
            *self.copy_state.lock().await = Some(st);
            client.flush().await?;
            client.set_state(PgWireConnectionState::CopyInProgress(true));
        } else if errored {
            // Postgres FE/BE protocol: after an ErrorResponse on the
            // extended path the backend must "discard messages until a
            // Sync is reached". Flush so the client sees the
            // ErrorResponse promptly, then sit in `AwaitingSync`; the
            // framework's main loop dispatches the eventual `Sync` to
            // `on_sync`, which will emit the trailing `ReadyForQuery`
            // and return us to `ReadyForQuery` state.
            client.flush().await?;
            client.set_state(PgWireConnectionState::AwaitingSync);
        } else {
            client.set_state(PgWireConnectionState::ReadyForQuery);
        }
        Ok(())
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        for m in handle_describe(self.state.as_ref(), message).await {
            client.feed(m).await?;
        }
        Ok(())
    }

    async fn on_flush<C>(&self, client: &mut C, _message: Flush) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.flush().await?;
        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                client.transaction_status(),
            )))
            .await?;
        client.flush().await?;
        Ok(())
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = self.require_session().await?;
        for m in handle_close(self.state.as_ref(), session.as_ref(), message).await {
            client.feed(m).await?;
        }
        Ok(())
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // We override `on_describe` directly, so this default-path entry is
        // never reached. Provide a `no_data` stub to satisfy the trait.
        Ok(DescribeStatementResponse::no_data())
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(DescribePortalResponse::no_data())
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        _portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // We override `on_execute` directly; this trait path is unused.
        unreachable!("on_execute is overridden and never delegates to do_query")
    }
}

/// Startup handler:
/// 1. Acks `Startup`, replies with `CleartextPassword`.
/// 2. Accepts any password (PoC).
/// 3. Resolves the username -> project via the resolver.
/// 4. Opens an engine session and stores it in the per-connection slot, so
///    the simple-query handler can reach it later. If anything fails an
///    `ErrorResponse` is sent and the connection is closed.
pub(crate) struct BasinStartupHandler<F: SessionFactory + 'static> {
    factory: Arc<F>,
    resolver: Arc<dyn ProjectResolver>,
    /// Per-connection session slot. Filled in once the user authenticates.
    slot: Arc<Mutex<Option<Arc<F::Session>>>>,
    /// Optional per-project connection ceiling. When `Some`, the startup
    /// handler checks the live count against the project's `max_connections`
    /// limit (resolved fresh each connect) and rejects with SQLSTATE 53300
    /// if the limit is reached. `None` means unlimited.
    connection_limiter: Option<Arc<crate::ConnectionLimiter>>,
    /// Holds the `ConnectionGuard` for the duration of this connection.
    /// Filled exactly once after successful authentication (when the limiter
    /// admits the connection). Dropped automatically when the handler is dropped
    /// at connection close, which decrements the per-project live count.
    ///
    /// No `Arc` wrapper is needed: the guard is set once during `on_startup`
    /// and is never shared with any other struct. Removing the `Arc` saves one
    /// heap allocation per connection on the accept hot-path.
    conn_guard: Mutex<Option<crate::ConnectionGuard>>,
}

impl<F: SessionFactory + 'static> BasinStartupHandler<F> {
    pub(crate) fn new(
        factory: Arc<F>,
        resolver: Arc<dyn ProjectResolver>,
        slot: Arc<Mutex<Option<Arc<F::Session>>>>,
        connection_limiter: Option<Arc<crate::ConnectionLimiter>>,
    ) -> Self {
        Self {
            factory,
            resolver,
            slot,
            connection_limiter,
            conn_guard: Mutex::new(None),
        }
    }
}

#[async_trait]
impl<F: SessionFactory + 'static> StartupHandler for BasinStartupHandler<F> {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let user = client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .ok_or(PgWireError::UserNameRequired)?;

                // Decode the cleartext password from the message and hand
                // it to the resolver. Resolvers that don't care about
                // passwords (JWT, API-key, static) ignore it via the
                // default `resolve_credentials` impl. The
                // `ProjectCredentialsResolver` consults it to bcrypt-verify.
                let password = pwd.into_password().map(|p| p.password).unwrap_or_default();
                let project = self
                    .resolver
                    .resolve_credentials(&user, &password)
                    .await
                    .map_err(|e| {
                        // SQLSTATE 28P01 (`invalid_password`) — the canonical
                        // Postgres code. The underlying reason (user not
                        // found vs. bcrypt mismatch vs. JWT expired) is logged
                        // at WARN with the username so operators can debug
                        // misrouted clients without exposing it to the
                        // attacker on the wire.
                        tracing::warn!(error = %e, %user, "pgwire auth rejected");
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "FATAL".to_owned(),
                            "28P01".to_owned(),
                            "invalid pgwire credentials".to_owned(),
                        )))
                    })?;

                // Per-project connection ceiling check. The limit is resolved
                // dynamically (fresh call per connect — not a lifetime cache)
                // so a plan upgrade propagates to new connections immediately.
                // None limiter ⇒ unlimited (correct for self-hosted OSS with
                // no control plane wired).
                if let Some(limiter) = &self.connection_limiter {
                    match limiter.try_admit(project).await {
                        Ok(guard) => {
                            *self.conn_guard.lock().await = Some(guard);
                        }
                        Err(live) => {
                            tracing::warn!(
                                %project,
                                live_connections = live,
                                "pgwire connection rejected: max_connections reached",
                            );
                            client
                                .send(PgWireBackendMessage::ErrorResponse(
                                    crate::error::connection_limit_reached_response(),
                                ))
                                .await?;
                            client.flush().await?;
                            return Ok(());
                        }
                    }
                }

                let session = self.factory.open(project, &user).await.map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "XX000".to_owned(),
                        format!("open session failed: {e}"),
                    )))
                })?;

                *self.slot.lock().await = Some(session);

                tracing::info!(%project, %user, "project authenticated");

                // pgwire's `DefaultServerParameterProvider` reports its OWN
                // crate version (e.g. "0.28.0", from `env!("CARGO_PKG_VERSION")`)
                // as the `server_version` startup ParameterStatus. Clients that
                // gate on the major version parse that as PostgreSQL 0.28 and
                // refuse to connect entirely — Django: "PostgreSQL 14 or later
                // is required (found 0.2800)". Report a PG-compatible version
                // that matches `pg_settings.server_version` (15.0) so version
                // checks in Django / SQLAlchemy / TypeORM / etc. pass.
                let mut server_params = DefaultServerParameterProvider::default();
                server_params.server_version = BASIN_PG_SERVER_VERSION.to_owned();
                finish_authentication(client, &server_params).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// Engine-shaped trait the startup handler can call to materialize a session
/// for an authenticated project. We define this rather than depend on `Engine`
/// directly so the test path can substitute a fake without touching the real
/// engine (whose body is still `unimplemented!()`).
///
/// `_username` is the original pgwire `user` parameter (after JWT/static
/// resolution to the same `ProjectId`). The single-process and pool factories
/// ignore it; the remote-shard factory uses it as the upstream `user`
/// parameter so the upstream router resolves the same project. New
/// implementations should default-impl-ignore it for backward compatibility.
#[async_trait]
pub(crate) trait SessionFactory: Send + Sync {
    type Session: Session + 'static;
    async fn open(
        &self,
        project: basin_common::ProjectId,
        _username: &str,
    ) -> Result<Arc<Self::Session>>;
}

/// Bridge to `basin_engine::Engine`.
///
/// When `auth` is `Some`, the factory attempts to decode the `username`
/// as a JWT on every new connection. If decoding succeeds the session is
/// opened with the decoded `AuthContext` (populating `auth.uid()` /
/// `auth.role()` / `auth.jwt()`). Non-JWT usernames (static resolver
/// scenarios) fall back to an anonymous `AuthContext` gracefully.
pub(crate) struct EngineSessionFactory {
    pub engine: basin_engine::Engine,
    /// Optional auth service for JWT decoding at session-open time.
    pub auth: Option<Arc<basin_auth::AuthService>>,
}

impl EngineSessionFactory {
    pub fn new(engine: basin_engine::Engine) -> Self {
        Self { engine, auth: None }
    }

    #[allow(dead_code)]
    pub fn with_auth(engine: basin_engine::Engine, auth: Arc<basin_auth::AuthService>) -> Self {
        Self {
            engine,
            auth: Some(auth),
        }
    }
}

/// Build an `AuthContext` from a pgwire `username` string, attempting JWT
/// decoding when an `AuthService` is available. Returns `anonymous` context
/// on any failure (wrong format, expired token, no auth service).
fn auth_context_from_username(
    username: &str,
    auth: Option<&basin_auth::AuthService>,
) -> AuthContext {
    let Some(auth_svc) = auth else {
        return AuthContext::anonymous();
    };
    // Strip optional `Bearer ` prefix — same as `JwtProjectResolver`.
    let token = username.strip_prefix("Bearer ").unwrap_or(username);
    match auth_svc.verify_jwt(token) {
        Ok(claims) => {
            // Role: first entry in `claims.roles`, or `"authenticated"` as default.
            let role = claims
                .roles
                .first()
                .cloned()
                .unwrap_or_else(|| "authenticated".to_string());
            // Build the full claims JSON so `auth.jwt()` can return it.
            let claims_json = serde_json::json!({
                "sub": claims.user_id.to_string(),
                "user_id": claims.user_id.to_string(),
                "email": claims.email,
                "roles": claims.roles,
                "exp": claims.exp,
                "iat": claims.iat,
            });
            AuthContext::from_jwt(claims.user_id, role, claims_json)
        }
        Err(_) => {
            // Not a JWT or expired — anonymous context.
            AuthContext::anonymous()
        }
    }
}

#[async_trait]
impl SessionFactory for EngineSessionFactory {
    type Session = ProjectSession;
    async fn open(
        &self,
        project: basin_common::ProjectId,
        username: &str,
    ) -> Result<Arc<ProjectSession>> {
        let auth_ctx = auth_context_from_username(username, self.auth.as_deref());
        let s = self
            .engine
            .open_session_with_auth(project, username, auth_ctx)
            .await?;
        Ok(Arc::new(s))
    }
}

/// Bridge to `basin_pool::SessionPool`. Acquires a `PooledSession` whose
/// `Drop` returns the underlying `ProjectSession` to the pool when the
/// connection's session slot is cleared (i.e. when the per-connection state
/// goes away).
///
/// `client_key` is `None` for now; the JWT-aware path that derives a
/// per-user key from the bearer token is layered on by `JwtProjectResolver`
/// elsewhere — keeping the pool key here `None` matches today's "username
/// is the project" world.
pub(crate) struct PooledSessionFactory {
    pool: Arc<basin_pool::SessionPool>,
}

impl PooledSessionFactory {
    pub(crate) fn new(pool: Arc<basin_pool::SessionPool>) -> Self {
        Self { pool }
    }
}

/// Per-connection wrapper that owns the `PooledSession`. We forward the
/// `Session` trait through to the underlying `ProjectSession`. Holding the
/// `PooledSession` for the lifetime of the connection's slot is what keeps
/// the engine session checked-out; when the slot drops, the wrapper drops,
/// the `PooledSession` drops, and the engine session goes back to the pool.
pub(crate) struct PooledSessionWrapper {
    pooled: basin_pool::PooledSession,
}

#[async_trait]
impl Session for PooledSessionWrapper {
    fn project(&self) -> basin_common::ProjectId {
        self.pooled.project()
    }

    async fn execute(&self, sql: &str) -> Result<ExecResult> {
        self.pooled.session().execute(sql).await
    }

    async fn prepare(&self, sql: &str) -> Result<(StatementHandle, StatementSchema)> {
        self.pooled.session().prepare(sql).await
    }

    async fn bind(
        &self,
        handle: &StatementHandle,
        params: Vec<ScalarParam>,
    ) -> Result<BoundStatement> {
        self.pooled.session().bind(handle, params).await
    }

    async fn execute_bound(&self, bound: BoundStatement) -> Result<ExecResult> {
        self.pooled.session().execute_bound(bound).await
    }

    async fn describe_statement(&self, handle: &StatementHandle) -> Result<StatementSchema> {
        self.pooled.session().describe_statement(handle).await
    }

    async fn close_statement(&self, handle: &StatementHandle) {
        self.pooled.session().close_statement(handle).await
    }
}

#[async_trait]
impl SessionFactory for PooledSessionFactory {
    type Session = PooledSessionWrapper;
    async fn open(
        &self,
        project: basin_common::ProjectId,
        _username: &str,
    ) -> Result<Arc<PooledSessionWrapper>> {
        let pooled = self.pool.acquire(project, None).await?;
        Ok(Arc::new(PooledSessionWrapper { pooled }))
    }
}

/// Glue struct passed to `pgwire::tokio::process_socket`. One instance per
/// connection; owns the session slot the simple-query handler reads from.
pub(crate) struct BasinHandlers<F: SessionFactory + 'static> {
    pub(crate) startup: Arc<BasinStartupHandler<F>>,
    pub(crate) simple: Arc<BasinSimpleQueryHandlerSlot<F::Session>>,
    pub(crate) extended: Arc<BasinExtendedQueryHandler<F::Session>>,
}

impl<F: SessionFactory + 'static> PgWireServerHandlers for BasinHandlers<F> {
    type StartupHandler = BasinStartupHandler<F>;
    type SimpleQueryHandler = BasinSimpleQueryHandlerSlot<F::Session>;
    type ExtendedQueryHandler = BasinExtendedQueryHandler<F::Session>;
    // Same struct as the simple-query handler — `BasinSimpleQueryHandlerSlot`
    // implements both traits so the per-connection COPY-IN staging state
    // (column types, half-buffered CSV bytes, sticky error) is shared
    // between `on_query` (which seeds it) and `on_copy_data` /
    // `on_copy_done` (which drain it).
    type CopyHandler = BasinSimpleQueryHandlerSlot<F::Session>;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.simple.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.extended.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.startup.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        self.simple.clone()
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

/// Adapter that reads the session out of the per-connection slot at query
/// time. The slot is filled in by the startup handler; if a query arrives
/// before the slot is populated, that's a bug on the pgwire side (state
/// machine should refuse), so we treat it as an internal error.
///
/// Also carries the per-connection `COPY FROM STDIN` staging state
/// ([`crate::copy::CopyInState`]). The struct is constructed via
/// [`Self::new`] so the COPY slot stays an implementation detail; lib.rs
/// hands in `slot` and `rate_limit` only.
pub(crate) struct BasinSimpleQueryHandlerSlot<S: Session + 'static> {
    pub(crate) slot: Arc<Mutex<Option<Arc<S>>>>,
    /// Optional per-project rate limiter shared across every connection
    /// for the lifetime of the router. `None` (the default) disables
    /// throttling so existing demos/benches keep their unbounded
    /// throughput; `Some` is set when `BASIN_PGWIRE_RATE_LIMIT_QPS > 0`.
    pub(crate) rate_limit: Option<Arc<crate::rate_limit::PgRateLimit>>,
    /// Holds an in-flight `COPY FROM STDIN`'s staging state for this
    /// connection: the resolved column types, the half-buffered CSV input,
    /// the running row count, and any sticky error captured mid-stream.
    /// `None` whenever no COPY is active. The same struct is the
    /// `CopyHandler` impl, so the on_copy_data / on_copy_done callbacks
    /// reach the same slot.
    pub(crate) copy_state: crate::copy::CopyStateSlot,
}

impl<S: Session + 'static> BasinSimpleQueryHandlerSlot<S> {
    pub(crate) fn new(
        slot: Arc<Mutex<Option<Arc<S>>>>,
        rate_limit: Option<Arc<crate::rate_limit::PgRateLimit>>,
    ) -> Self {
        Self {
            slot,
            rate_limit,
            copy_state: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

#[async_trait]
impl<S: Session + 'static> SimpleQueryHandler for BasinSimpleQueryHandlerSlot<S> {
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = {
            let guard = self.slot.lock().await;
            guard.clone().ok_or_else(|| {
                PgWireError::ApiError(
                    "session not bound to connection (startup did not complete)".into(),
                )
            })?
        };
        // Per-project rate limit (Phase 6): when enabled, drop the query
        // before it touches the engine if the project has burned their
        // budget. Map to SQLSTATE 53400 (configuration_limit_exceeded).
        if let Some(rl) = &self.rate_limit {
            if rl.check(&session.project()).is_err() {
                if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
                    return Err(PgWireError::NotReadyForQuery);
                }
                client.set_state(PgWireConnectionState::QueryInProgress);
                client
                    .feed(PgWireBackendMessage::ErrorResponse(
                        crate::error::rate_limit_exceeded_response(),
                    ))
                    .await?;
                client
                    .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                        TransactionStatus::Idle,
                    )))
                    .await?;
                client.flush().await?;
                client.set_state(PgWireConnectionState::ReadyForQuery);
                return Ok(());
            }
        }
        // COPY hooks here, before normal routing. `parse_copy` returns
        // `Ok(None)` for non-COPY SQL so this is zero-cost on the hot path.
        match crate::copy::parse_copy(&query.query) {
            Ok(Some(cmd)) => {
                return self.handle_copy(client, session, cmd).await;
            }
            Ok(None) => {}
            Err(emsg) => {
                if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
                    return Err(PgWireError::NotReadyForQuery);
                }
                client.set_state(PgWireConnectionState::QueryInProgress);
                let (sqlstate, message) = decode_copy_error(&emsg);
                let info = pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(),
                    sqlstate.to_owned(),
                    message.to_owned(),
                );
                client
                    .feed(PgWireBackendMessage::ErrorResponse(info.into()))
                    .await?;
                client
                    .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                        TransactionStatus::Idle,
                    )))
                    .await?;
                client.flush().await?;
                client.set_state(PgWireConnectionState::ReadyForQuery);
                return Ok(());
            }
        }
        let inner = BasinSimpleQueryHandler::new(session);
        inner.on_query(client, query).await
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        _query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        unreachable!("on_query is overridden and never delegates to do_query")
    }
}

impl<S: Session + 'static> BasinSimpleQueryHandlerSlot<S> {
    /// Drive one COPY exchange end-to-end.
    ///
    /// `From` flips the connection into `CopyInProgress`; the rest is
    /// driven by the framework via the [`CopyHandler`] impl below.
    /// `To` runs entirely in this method — we synchronously query and
    /// emit the full message sequence.
    async fn handle_copy<C>(
        &self,
        client: &mut C,
        session: Arc<S>,
        cmd: crate::copy::CopyCommand,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        client.set_state(PgWireConnectionState::QueryInProgress);

        match cmd {
            crate::copy::CopyCommand::From {
                table,
                with_header,
                columns,
                path,
                opts,
            } => {
                let table_cols = match crate::copy::resolve_table_columns(session.as_ref(), &table)
                    .await
                {
                    Ok(c) if !c.is_empty() => c,
                    Ok(_) => {
                        let info = pgwire::error::ErrorInfo::new(
                            "ERROR".to_owned(),
                            "42P01".to_owned(),
                            format!("COPY FROM: cannot resolve schema of {table:?}"),
                        );
                        return finish_copy_error(client, info.into()).await;
                    }
                    Err(e) => {
                        return finish_copy_error(client, crate::error::error_response(&e)).await;
                    }
                };
                let cols = match crate::copy::select_copy_in_columns(
                    &table,
                    &table_cols,
                    columns.as_deref(),
                ) {
                    Ok(c) => c,
                    Err(msg) => {
                        let info = pgwire::error::ErrorInfo::new(
                            "ERROR".to_owned(),
                            "42601".to_owned(),
                            msg,
                        );
                        return finish_copy_error(client, info.into()).await;
                    }
                };
                if opts.format == crate::copy::CopyFormat::Binary {
                    if let Err(msg) = crate::copy::validate_binary_copy_columns(cols.iter()) {
                        let info = pgwire::error::ErrorInfo::new(
                            "ERROR".to_owned(),
                            "0A000".to_owned(),
                            msg,
                        );
                        return finish_copy_error(client, info.into()).await;
                    }
                }
                let full_schema = std::sync::Arc::new(arrow_schema::Schema::new(
                    table_cols.iter().cloned().collect::<Vec<_>>(),
                ));
                let mut state = crate::copy::CopyInState::new(
                    table.clone(),
                    full_schema,
                    cols,
                    with_header,
                    opts,
                );
                if let Some(list) = columns.clone() {
                    state = state.with_column_list(list);
                }
                if let Some(p) = path {
                    let resolved = match crate::copy::validate_copy_path(&p) {
                        Ok(pb) => pb,
                        Err(emsg) => {
                            let (sqlstate, message) = decode_copy_error(&emsg);
                            let info = pgwire::error::ErrorInfo::new(
                                "ERROR".to_owned(),
                                sqlstate.to_owned(),
                                message.to_owned(),
                            );
                            return finish_copy_error(client, info.into()).await;
                        }
                    };
                    if let Err(e) =
                        crate::copy::copy_from_file(session.as_ref(), &mut state, &resolved).await
                    {
                        return finish_copy_error(client, crate::error::error_response(&e)).await;
                    }
                    if let Some(err) = state.error.take() {
                        let info = pgwire::error::ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            err,
                        );
                        return finish_copy_error(client, info.into()).await;
                    }
                    client
                        .feed(PgWireBackendMessage::CommandComplete(
                            pgwire::messages::response::CommandComplete::new(format!(
                                "COPY {}",
                                state.row_count
                            )),
                        ))
                        .await?;
                    client
                        .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                            TransactionStatus::Idle,
                        )))
                        .await?;
                    client.flush().await?;
                    client.set_state(PgWireConnectionState::ReadyForQuery);
                    return Ok(());
                }
                let n = state.columns.len();
                let format = state.opts.format;
                {
                    let mut g = self.copy_state.lock().await;
                    *g = Some(state);
                }
                for m in crate::copy::copy_from_stdin_messages(n, format) {
                    client.feed(m).await?;
                }
                client.flush().await?;
                client.set_state(PgWireConnectionState::CopyInProgress(false));
                Ok(())
            }
            crate::copy::CopyCommand::To {
                table,
                with_header,
                columns,
                path,
                opts,
            } => {
                if let Some(p) = path {
                    let resolved = match crate::copy::validate_copy_path(&p) {
                        Ok(pb) => pb,
                        Err(emsg) => {
                            let (sqlstate, message) = decode_copy_error(&emsg);
                            let info = pgwire::error::ErrorInfo::new(
                                "ERROR".to_owned(),
                                sqlstate.to_owned(),
                                message.to_owned(),
                            );
                            return finish_copy_error(client, info.into()).await;
                        }
                    };
                    let count = match crate::copy::copy_to_file(
                        session.as_ref(),
                        &table,
                        columns.as_deref(),
                        &resolved,
                        with_header,
                        &opts,
                    )
                    .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            return finish_copy_error(client, crate::error::error_response(&e))
                                .await;
                        }
                    };
                    client
                        .feed(PgWireBackendMessage::CommandComplete(
                            pgwire::messages::response::CommandComplete::new(format!(
                                "COPY {count}"
                            )),
                        ))
                        .await?;
                    client
                        .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                            TransactionStatus::Idle,
                        )))
                        .await?;
                    client.flush().await?;
                    client.set_state(PgWireConnectionState::ReadyForQuery);
                    return Ok(());
                }
                for m in crate::copy::copy_to_stdout_messages(
                    session.as_ref(),
                    &table,
                    columns.as_deref(),
                    with_header,
                    &opts,
                )
                .await
                {
                    client.feed(m).await?;
                }
                client.flush().await?;
                client.set_state(PgWireConnectionState::ReadyForQuery);
                Ok(())
            }
            crate::copy::CopyCommand::QueryTo {
                query,
                with_header,
                opts,
            } => {
                for m in crate::copy::copy_query_to_stdout_messages(
                    session.as_ref(),
                    &query,
                    with_header,
                    &opts,
                )
                .await
                {
                    client.feed(m).await?;
                }
                client.flush().await?;
                client.set_state(PgWireConnectionState::ReadyForQuery);
                Ok(())
            }
        }
    }
}

/// Common tail for COPY-handler errors on the simple-query path: feed the
/// `ErrorResponse` and a `ReadyForQuery(Idle)`, flush, return to
/// ReadyForQuery state. Mirrors the inline boilerplate the previous
/// implementation duplicated at every error-exit.
async fn finish_copy_error<C>(client: &mut C, err: ErrorResponse) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    client
        .feed(PgWireBackendMessage::ErrorResponse(err))
        .await?;
    client
        .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
            TransactionStatus::Idle,
        )))
        .await?;
    client.flush().await?;
    client.set_state(PgWireConnectionState::ReadyForQuery);
    Ok(())
}

#[async_trait]
impl<S: Session + 'static> CopyHandler for BasinSimpleQueryHandlerSlot<S> {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = {
            let g = self.slot.lock().await;
            g.clone().ok_or_else(|| {
                PgWireError::ApiError("session not bound to connection at CopyData time".into())
            })?
        };
        let mut g = self.copy_state.lock().await;
        let state = match g.as_mut() {
            Some(s) => s,
            None => {
                // Stray CopyData with no active COPY — silently ignore so
                // we don't desync. Framework will eventually time out or
                // the client will send CopyDone.
                return Ok(());
            }
        };
        state.buffer.extend_from_slice(&copy_data.data);
        crate::copy::process_buffered_rows(state, session.as_ref(), false).await;
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Capture the original state-machine flag before we touch it. The
        // framework calls us while still in `CopyInProgress(<flag>)`; for
        // simple-protocol it transitions out itself afterwards, but for
        // extended-protocol the comment in pgwire 0.28's server.rs says
        // "let the on_sync handler send the ReadyForQuery" — which only
        // works if WE move out of CopyInProgress here, otherwise the
        // following `Sync` falls into the catch-all `_ => {}` branch and
        // is silently dropped, deadlocking the client.
        let is_extended = matches!(client.state(), PgWireConnectionState::CopyInProgress(true));

        let session = {
            let g = self.slot.lock().await;
            g.clone().ok_or_else(|| {
                PgWireError::ApiError("session not bound to connection at CopyDone time".into())
            })?
        };
        let state = {
            let mut g = self.copy_state.lock().await;
            g.take()
        };
        let mut state = match state {
            Some(s) => s,
            None => {
                if is_extended {
                    client.set_state(PgWireConnectionState::ReadyForQuery);
                }
                return Ok(());
            }
        };
        // Flush any final unterminated row.
        crate::copy::process_buffered_rows(&mut state, session.as_ref(), true).await;

        // For the simple-protocol case: framework appends ReadyForQuery
        // itself after we return; we just emit the close tag.
        // For the extended case: caller will send Sync next; we emit the
        // close tag and transition out of CopyInProgress so Sync routes
        // to `on_sync`, which sends ReadyForQuery.
        let msg = if let Some(err) = state.error {
            PgWireBackendMessage::ErrorResponse(
                pgwire::error::ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), err).into(),
            )
        } else {
            PgWireBackendMessage::CommandComplete(pgwire::messages::response::CommandComplete::new(
                format!("COPY {}", state.row_count),
            ))
        };
        client.feed(msg).await?;
        client.flush().await?;
        if is_extended {
            client.set_state(PgWireConnectionState::ReadyForQuery);
        }
        Ok(())
    }

    async fn on_copy_fail<C>(&self, _client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Client cancelled COPY mid-stream. Drop any staged state.
        {
            let mut g = self.copy_state.lock().await;
            *g = None;
        }
        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            "ERROR".to_owned(),
            "57014".to_owned(), // query_canceled
            format!("COPY FROM STDIN cancelled by client: {}", fail.message),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use async_trait::async_trait;
    use basin_common::{BasinError, Result};
    use basin_engine::ExecResult;

    use super::*;

    // Regression: the startup `server_version` Basin advertises must be a real
    // PG-style version whose MAJOR is >= 14, and it must NOT be pgwire's own
    // crate-version default (which a client reads as "PostgreSQL 0.x" and
    // refuses — Django: "PostgreSQL 14 or later is required (found 0.2800)").
    // Also pin it equal to the value the `DefaultServerParameterProvider` we
    // build for `finish_authentication` actually emits, so a future revert to
    // `::default()` (crate version) trips this test.
    #[test]
    fn startup_server_version_is_pg_compatible() {
        // The literal Basin advertises.
        let major: u32 = BASIN_PG_SERVER_VERSION
            .split('.')
            .next()
            .and_then(|s| s.parse().ok())
            .expect("server_version must start with a numeric major");
        assert!(
            major >= 14,
            "advertised server_version {BASIN_PG_SERVER_VERSION:?} has major {major} (< 14); \
             version-gating clients (Django et al.) will refuse to connect"
        );
        // Guard against silently shipping pgwire's crate-version default.
        let pgwire_default = DefaultServerParameterProvider::default().server_version;
        assert_ne!(
            BASIN_PG_SERVER_VERSION, pgwire_default,
            "server_version must be overridden away from pgwire's crate-version default"
        );
    }

    enum FakeOutcome {
        Empty(String),
        Rows {
            schema: Arc<ArrowSchema>,
            batches: Vec<RecordBatch>,
        },
        Err(BasinError),
    }

    struct FakeSession {
        outcome: tokio::sync::Mutex<Option<FakeOutcome>>,
    }

    impl FakeSession {
        fn new(o: FakeOutcome) -> Self {
            Self {
                outcome: tokio::sync::Mutex::new(Some(o)),
            }
        }
    }

    #[async_trait]
    impl Session for FakeSession {
        fn project(&self) -> basin_common::ProjectId {
            basin_common::ProjectId::new()
        }

        async fn execute(&self, _sql: &str) -> Result<ExecResult> {
            let mut g = self.outcome.lock().await;
            let taken = g.take().expect("FakeSession used twice");
            match taken {
                FakeOutcome::Empty(tag) => Ok(ExecResult::Empty { tag }),
                FakeOutcome::Rows { schema, batches } => Ok(ExecResult::Rows { schema, batches }),
                FakeOutcome::Err(e) => Err(e),
            }
        }

        async fn prepare(&self, _sql: &str) -> Result<(StatementHandle, StatementSchema)> {
            unimplemented!("simple-query test fake")
        }

        async fn bind(
            &self,
            _handle: &StatementHandle,
            _params: Vec<ScalarParam>,
        ) -> Result<BoundStatement> {
            unimplemented!("simple-query test fake")
        }

        async fn execute_bound(&self, _bound: BoundStatement) -> Result<ExecResult> {
            unimplemented!("simple-query test fake")
        }

        async fn describe_statement(&self, _handle: &StatementHandle) -> Result<StatementSchema> {
            unimplemented!("simple-query test fake")
        }

        async fn close_statement(&self, _handle: &StatementHandle) {}
    }

    fn assert_command_complete(msg: &PgWireBackendMessage, expected_tag: &str) {
        match msg {
            PgWireBackendMessage::CommandComplete(cc) => assert_eq!(cc.tag, expected_tag),
            other => panic!("expected CommandComplete, got {other:?}"),
        }
    }

    fn assert_ready_for_query(msg: &PgWireBackendMessage) {
        match msg {
            PgWireBackendMessage::ReadyForQuery(r) => {
                assert!(matches!(r.status, TransactionStatus::Idle))
            }
            other => panic!("expected ReadyForQuery, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn protocol_handles_empty_result() {
        let s = FakeSession::new(FakeOutcome::Empty("CREATE TABLE".into()));
        let msgs = simple_query_messages(&s, "CREATE TABLE foo(...)").await;
        assert_eq!(msgs.len(), 2, "got {msgs:?}");
        assert_command_complete(&msgs[0], "CREATE TABLE");
        assert_ready_for_query(&msgs[1]);
    }

    #[tokio::test]
    async fn protocol_handles_rows() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();
        let s = FakeSession::new(FakeOutcome::Rows {
            schema,
            batches: vec![batch],
        });
        let msgs = simple_query_messages(&s, "SELECT id,name FROM t").await;
        // Expect: RowDescription, 3x DataRow, CommandComplete("SELECT 3"), ReadyForQuery.
        assert_eq!(msgs.len(), 1 + 3 + 1 + 1, "got {msgs:?}");
        match &msgs[0] {
            PgWireBackendMessage::RowDescription(rd) => assert_eq!(rd.fields.len(), 2),
            other => panic!("expected RowDescription, got {other:?}"),
        }
        for m in &msgs[1..4] {
            match m {
                PgWireBackendMessage::DataRow(dr) => assert_eq!(dr.field_count, 2),
                other => panic!("expected DataRow, got {other:?}"),
            }
        }
        assert_command_complete(&msgs[4], "SELECT 3");
        assert_ready_for_query(&msgs[5]);
    }

    #[tokio::test]
    async fn protocol_handles_error() {
        let s = FakeSession::new(FakeOutcome::Err(BasinError::Internal("boom".into())));
        let msgs = simple_query_messages(&s, "SELECT 1").await;
        assert_eq!(msgs.len(), 2, "got {msgs:?}");
        match &msgs[0] {
            PgWireBackendMessage::ErrorResponse(er) => {
                assert_eq!(er.fields[0], (b'S', "ERROR".to_owned()));
                assert_eq!(er.fields[1], (b'C', "XX000".to_owned()));
                assert!(er.fields[2].1.contains("boom"));
            }
            other => panic!("expected ErrorResponse, got {other:?}"),
        }
        assert_ready_for_query(&msgs[1]);
    }

    #[test]
    fn split_basic_two_statements() {
        let parts = split_simple_query("CREATE TABLE t (id BIGINT); CREATE TABLE s (val TEXT);");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "CREATE TABLE t (id BIGINT)");
        assert_eq!(parts[1], "CREATE TABLE s (val TEXT)");
    }

    #[test]
    fn split_single_statement_no_trailing_semicolon() {
        let parts = split_simple_query("SELECT 1");
        assert_eq!(parts, vec!["SELECT 1".to_string()]);
    }

    #[test]
    fn split_trailing_semicolon_dropped() {
        let parts = split_simple_query("SELECT 1;");
        assert_eq!(parts, vec!["SELECT 1".to_string()]);
    }

    #[test]
    fn split_empty_input() {
        assert!(split_simple_query("").is_empty());
        assert!(split_simple_query("   \n\t  ").is_empty());
        assert!(split_simple_query(";;").is_empty());
    }

    #[test]
    fn split_preserves_dollar_quoted_bodies() {
        // `;` inside the dollar-quoted body must NOT split.
        let sql =
            "CREATE FUNCTION f() RETURNS INT LANGUAGE sql AS $$ SELECT 1; SELECT 2 $$; SELECT 3";
        let parts = split_simple_query(sql);
        assert_eq!(parts.len(), 2, "got {parts:?}");
        assert!(parts[0].contains("$$ SELECT 1; SELECT 2 $$"));
        assert_eq!(parts[1], "SELECT 3");
    }

    #[test]
    fn split_preserves_single_quoted_strings() {
        let parts = split_simple_query("SELECT 'a;b'; SELECT 'c'");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "SELECT 'a;b'");
        assert_eq!(parts[1], "SELECT 'c'");
    }

    #[test]
    fn split_skips_comment_only_segments() {
        let parts = split_simple_query("-- just a comment\n;SELECT 1");
        assert_eq!(parts, vec!["SELECT 1".to_string()]);
    }

    #[test]
    fn split_handles_newline_separated_statements() {
        let parts =
            split_simple_query("CREATE TABLE a (id BIGINT);\nCREATE TABLE b (id BIGINT);\n");
        assert_eq!(parts.len(), 2, "got {parts:?}");
        assert_eq!(parts[0], "CREATE TABLE a (id BIGINT)");
        assert_eq!(parts[1], "CREATE TABLE b (id BIGINT)");
    }

    #[test]
    fn split_handles_leading_semicolon() {
        let parts = split_simple_query(";SELECT 1");
        assert_eq!(parts, vec!["SELECT 1".to_string()]);
    }

    /// Session shim that returns a queue of pre-canned outcomes, one per
    /// `execute()`. Enables multi-statement protocol assertions.
    struct ScriptedSession {
        outcomes: tokio::sync::Mutex<std::collections::VecDeque<FakeOutcome>>,
        executed: tokio::sync::Mutex<Vec<String>>,
    }

    impl ScriptedSession {
        fn new(outcomes: Vec<FakeOutcome>) -> Self {
            Self {
                outcomes: tokio::sync::Mutex::new(outcomes.into_iter().collect()),
                executed: tokio::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Session for ScriptedSession {
        fn project(&self) -> basin_common::ProjectId {
            basin_common::ProjectId::new()
        }
        async fn execute(&self, sql: &str) -> Result<ExecResult> {
            self.executed.lock().await.push(sql.to_string());
            let mut g = self.outcomes.lock().await;
            let taken = g.pop_front().expect("ScriptedSession exhausted");
            match taken {
                FakeOutcome::Empty(tag) => Ok(ExecResult::Empty { tag }),
                FakeOutcome::Rows { schema, batches } => Ok(ExecResult::Rows { schema, batches }),
                FakeOutcome::Err(e) => Err(e),
            }
        }
        async fn prepare(&self, _sql: &str) -> Result<(StatementHandle, StatementSchema)> {
            unimplemented!()
        }
        async fn bind(&self, _h: &StatementHandle, _p: Vec<ScalarParam>) -> Result<BoundStatement> {
            unimplemented!()
        }
        async fn execute_bound(&self, _b: BoundStatement) -> Result<ExecResult> {
            unimplemented!()
        }
        async fn describe_statement(&self, _h: &StatementHandle) -> Result<StatementSchema> {
            unimplemented!()
        }
        async fn close_statement(&self, _h: &StatementHandle) {}
    }

    #[tokio::test]
    async fn protocol_handles_multi_statement_two_empties() {
        let s = ScriptedSession::new(vec![
            FakeOutcome::Empty("CREATE TABLE".into()),
            FakeOutcome::Empty("CREATE TABLE".into()),
        ]);
        let msgs =
            simple_query_messages(&s, "CREATE TABLE t (id BIGINT); CREATE TABLE s (val TEXT)")
                .await;
        // Expect: CommandComplete, CommandComplete, ReadyForQuery.
        assert_eq!(msgs.len(), 3, "got {msgs:?}");
        assert_command_complete(&msgs[0], "CREATE TABLE");
        assert_command_complete(&msgs[1], "CREATE TABLE");
        assert_ready_for_query(&msgs[2]);

        let executed = s.executed.lock().await.clone();
        assert_eq!(executed.len(), 2);
        assert_eq!(executed[0], "CREATE TABLE t (id BIGINT)");
        assert_eq!(executed[1], "CREATE TABLE s (val TEXT)");
    }

    #[tokio::test]
    async fn protocol_handles_multi_statement_error_stops_dispatch() {
        // Three statements but the second errors. The third must NOT be
        // dispatched; the first's success stands (no rollback in
        // simple-query batches).
        let s = ScriptedSession::new(vec![
            FakeOutcome::Empty("CREATE TABLE".into()),
            FakeOutcome::Err(BasinError::Internal("dup".into())),
            // Third outcome is a sentinel — if the dispatcher reaches it,
            // it'll show up in `executed` and the assertion below catches
            // it. We still need an entry so `execute` doesn't panic if the
            // bug exists.
            FakeOutcome::Empty("UNREACHABLE".into()),
        ]);
        let msgs = simple_query_messages(
            &s,
            "CREATE TABLE t (id BIGINT); CREATE TABLE t (val TEXT); SELECT 1",
        )
        .await;
        // Expect: CommandComplete, ErrorResponse, ReadyForQuery.
        assert_eq!(msgs.len(), 3, "got {msgs:?}");
        assert_command_complete(&msgs[0], "CREATE TABLE");
        match &msgs[1] {
            PgWireBackendMessage::ErrorResponse(_) => {}
            other => panic!("expected ErrorResponse, got {other:?}"),
        }
        assert_ready_for_query(&msgs[2]);

        let executed = s.executed.lock().await.clone();
        assert_eq!(
            executed.len(),
            2,
            "third statement must not be dispatched: {executed:?}"
        );
    }

    #[tokio::test]
    async fn protocol_handles_empty_input() {
        let s = ScriptedSession::new(vec![]);
        let msgs = simple_query_messages(&s, "").await;
        assert_eq!(msgs.len(), 2);
        match &msgs[0] {
            PgWireBackendMessage::EmptyQueryResponse(_) => {}
            other => panic!("expected EmptyQueryResponse, got {other:?}"),
        }
        assert_ready_for_query(&msgs[1]);
        assert!(s.executed.lock().await.is_empty());
    }

    /// `Session` shim that performs real `$N` substitution at bind time but
    /// does not stand up the full DataFusion stack. Lets us assert that the
    /// router, given a `Parse`+`Bind` for `INSERT INTO t VALUES ($1, $2)`,
    /// hands the engine a SQL string with the literals already substituted.
    struct SubstSession {
        prepared: tokio::sync::Mutex<HashMap<StatementHandle, (String, usize)>>,
        last_executed_sql: tokio::sync::Mutex<Vec<String>>,
        param_types: Vec<arrow_schema::DataType>,
    }

    impl SubstSession {
        fn new(param_types: Vec<arrow_schema::DataType>) -> Self {
            Self {
                prepared: tokio::sync::Mutex::new(HashMap::new()),
                last_executed_sql: tokio::sync::Mutex::new(Vec::new()),
                param_types,
            }
        }

        async fn last_sql(&self) -> Vec<String> {
            self.last_executed_sql.lock().await.clone()
        }
    }

    #[async_trait]
    impl Session for SubstSession {
        fn project(&self) -> basin_common::ProjectId {
            basin_common::ProjectId::new()
        }

        async fn execute(&self, _sql: &str) -> Result<ExecResult> {
            Ok(ExecResult::Empty { tag: "OK".into() })
        }

        async fn prepare(&self, sql: &str) -> Result<(StatementHandle, StatementSchema)> {
            let h = StatementHandle::new();
            // Crude placeholder count for the test: scan once for `$N`s.
            let n = self.param_types.len();
            self.prepared.lock().await.insert(h, (sql.to_owned(), n));
            Ok((
                h,
                StatementSchema {
                    param_types: self.param_types.clone(),
                    param_is_jsonb: vec![false; n],
                    param_is_uuid: vec![false; n],
                    columns: Vec::new(),
                },
            ))
        }

        async fn bind(
            &self,
            handle: &StatementHandle,
            params: Vec<ScalarParam>,
        ) -> Result<BoundStatement> {
            let g = self.prepared.lock().await;
            let (sql, _n) = g
                .get(handle)
                .cloned()
                .ok_or_else(|| BasinError::not_found("test statement"))?;
            // Reuse the engine's substitution by going through the public
            // engine API directly. Simpler than re-implementing here.
            // Build by string manipulation matching the engine's rules.
            let mut out = sql.clone();
            for (i, p) in params.iter().enumerate() {
                let placeholder = format!("${}", i + 1);
                let rendered = match p {
                    ScalarParam::Null => "NULL".to_string(),
                    ScalarParam::Int4(v) => v.to_string(),
                    ScalarParam::Int8(v) => v.to_string(),
                    ScalarParam::Bool(b) => {
                        if *b {
                            "TRUE".into()
                        } else {
                            "FALSE".into()
                        }
                    }
                    ScalarParam::Float8(f) => format!("{f}"),
                    ScalarParam::Text(s) => format!("'{}'", s.replace('\'', "''")),
                    // Mock rendering only — no test asserts on the exact
                    // timestamptz literal shape here (the engine's
                    // render_param owns the real one).
                    ScalarParam::Timestamptz(us) => format!("'{us}'::timestamptz"),
                    ScalarParam::Bytea(b) => format!("'{:?}'", b),
                    // Tests don't exercise array params on the mock router
                    // session today, but the variant must be exhaustive. A
                    // simple debug rendering is enough — no test asserts on
                    // it.
                    ScalarParam::Array(elems) => format!("{:?}", elems),
                };
                out = out.replace(&placeholder, &rendered);
            }
            Ok(BoundStatement::for_tests(out))
        }

        async fn execute_bound(&self, bound: BoundStatement) -> Result<ExecResult> {
            self.last_executed_sql
                .lock()
                .await
                .push(bound.debug_sql().to_owned());
            Ok(ExecResult::Empty { tag: "OK".into() })
        }

        async fn describe_statement(&self, _handle: &StatementHandle) -> Result<StatementSchema> {
            let n = self.param_types.len();
            Ok(StatementSchema {
                param_types: self.param_types.clone(),
                param_is_jsonb: vec![false; n],
                param_is_uuid: vec![false; n],
                columns: Vec::new(),
            })
        }

        async fn close_statement(&self, _handle: &StatementHandle) {}
    }

    #[tokio::test]
    async fn prepare_then_bind_substitutes_int_param() {
        let sess = SubstSession::new(vec![arrow_schema::DataType::Int64]);
        let state = Mutex::new(ExtendedState::new());

        let parse_msgs = handle_parse(
            &state,
            &sess,
            Parse::new(
                Some("st1".into()),
                "SELECT * FROM t WHERE id = $1".into(),
                Vec::new(),
            ),
        )
        .await;
        assert!(matches!(
            parse_msgs.first(),
            Some(PgWireBackendMessage::ParseComplete(_))
        ));

        let bind = Bind::new(
            Some("p1".into()),
            Some("st1".into()),
            Vec::new(),
            vec![Some(bytes::Bytes::from_static(b"42"))],
            Vec::new(),
        );
        let bind_msgs = handle_bind(&state, &sess, bind).await;
        assert!(matches!(
            bind_msgs.first(),
            Some(PgWireBackendMessage::BindComplete(_))
        ));

        let exec_msgs = handle_execute(&state, &sess, Execute::new(Some("p1".into()), 0)).await;
        assert!(matches!(
            exec_msgs.messages.first(),
            Some(PgWireBackendMessage::CommandComplete(_))
        ));

        let captured = sess.last_sql().await;
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "SELECT * FROM t WHERE id = 42");
    }

    #[tokio::test]
    async fn prepare_then_bind_handles_string_param() {
        // Default is Text type for unspecified placeholders.
        let sess = SubstSession::new(vec![arrow_schema::DataType::Utf8]);
        let state = Mutex::new(ExtendedState::new());

        handle_parse(
            &state,
            &sess,
            Parse::new(
                Some("s".into()),
                "INSERT INTO t VALUES ($1)".into(),
                Vec::new(),
            ),
        )
        .await;
        // Send a value that contains an apostrophe to confirm escaping.
        handle_bind(
            &state,
            &sess,
            Bind::new(
                Some("p".into()),
                Some("s".into()),
                Vec::new(),
                vec![Some(bytes::Bytes::from_static(b"it's me"))],
                Vec::new(),
            ),
        )
        .await;
        handle_execute(&state, &sess, Execute::new(Some("p".into()), 0)).await;

        let captured = sess.last_sql().await;
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], "INSERT INTO t VALUES ('it''s me')");
    }

    #[tokio::test]
    async fn extended_protocol_emits_parsecomplete_bindcomplete() {
        let sess = SubstSession::new(vec![arrow_schema::DataType::Int64]);
        let state = Mutex::new(ExtendedState::new());

        let parse_msgs = handle_parse(
            &state,
            &sess,
            Parse::new(None, "SELECT $1".into(), Vec::new()),
        )
        .await;
        assert_eq!(parse_msgs.len(), 1);
        assert!(matches!(
            parse_msgs[0],
            PgWireBackendMessage::ParseComplete(_)
        ));

        let bind_msgs = handle_bind(
            &state,
            &sess,
            Bind::new(
                None,
                None,
                Vec::new(),
                vec![Some(bytes::Bytes::from_static(b"7"))],
                Vec::new(),
            ),
        )
        .await;
        assert_eq!(bind_msgs.len(), 1);
        assert!(matches!(
            bind_msgs[0],
            PgWireBackendMessage::BindComplete(_)
        ));
    }

    #[tokio::test]
    async fn extended_protocol_close_statement_drops_state() {
        let sess = SubstSession::new(Vec::new());
        let state = Mutex::new(ExtendedState::new());

        handle_parse(
            &state,
            &sess,
            Parse::new(Some("s".into()), "SELECT 1".into(), Vec::new()),
        )
        .await;
        assert_eq!(state.lock().await.statements.len(), 1);

        let msgs = handle_close(
            &state,
            &sess,
            Close::new(TARGET_TYPE_BYTE_STATEMENT, Some("s".into())),
        )
        .await;
        assert!(matches!(msgs[0], PgWireBackendMessage::CloseComplete(_)));
        assert!(state.lock().await.statements.is_empty());
    }

    /// Drive `PooledSessionFactory::open` end-to-end against a real `Engine` +
    /// `SessionPool`. Two sequential `open`s for the same project should hit
    /// the pool's cached session on the second call (one miss, one hit) — this
    /// is what `BasinStartupHandler` will exercise once it's running over a
    /// pool.
    #[tokio::test]
    async fn pool_session_factory_reuses_session_across_opens() {
        use std::sync::Arc as StdArc;

        let dir = tempfile::TempDir::new().unwrap();
        let fs = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: StdArc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: StdArc<dyn basin_catalog::Catalog> =
            StdArc::new(basin_catalog::InMemoryCatalog::new());
        let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
            storage,
            catalog,
            shard: None,
        });
        let pool = StdArc::new(basin_pool::SessionPool::new(
            engine,
            basin_pool::PoolConfig::default(),
        ));
        let factory = super::PooledSessionFactory::new(pool.clone());
        let project = basin_common::ProjectId::new();

        // First open is a cold miss, releases on Drop, so the second sees the
        // cached session.
        let s1 = factory.open(project, "alice").await.unwrap();
        drop(s1);
        let s2 = factory.open(project, "alice").await.unwrap();
        drop(s2);

        let stats = pool.stats();
        assert_eq!(
            stats.misses, 1,
            "first open is the only miss, got {stats:?}"
        );
        assert!(stats.hits >= 1, "second open should hit, got {stats:?}");
    }

    /// Binary JSONB wire format = 0x01 version byte + canonical-form JSON
    /// text. The decoder strips the version byte and lifts the rest to
    /// `ScalarParam::Text` so the engine's text-substitution path can quote
    /// it as a SQL literal.
    #[test]
    fn decode_param_binary_jsonb_strips_version_byte() {
        let mut wire = vec![0x01u8];
        wire.extend_from_slice(br#"{"k":"v"}"#);
        let p = super::decode_param_binary(&wire, &Type::JSONB).expect("decode");
        match p {
            ScalarParam::Text(s) => assert_eq!(s, r#"{"k":"v"}"#),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn decode_param_binary_jsonb_rejects_unknown_version() {
        let wire = vec![0x02u8, b'{', b'}'];
        let err = super::decode_param_binary(&wire, &Type::JSONB)
            .expect_err("unknown version must error");
        match err {
            PgWireError::UserError(info) => {
                // SQLSTATE 0A000 = feature_not_supported.
                assert_eq!(info.code, "0A000");
            }
            other => panic!("expected UserError, got {other:?}"),
        }
    }

    #[test]
    fn decode_param_binary_jsonb_empty_is_invalid() {
        let err =
            super::decode_param_binary(&[], &Type::JSONB).expect_err("empty JSONB must error");
        match err {
            PgWireError::UserError(info) => assert_eq!(info.code, "22P03"),
            other => panic!("expected UserError, got {other:?}"),
        }
    }

    /// Binary UUID = 16 raw bytes; we render canonical hyphenated form
    /// because the engine's INSERT path accepts string-quoted UUID literals.
    #[test]
    fn decode_param_binary_uuid_renders_hyphenated() {
        // Test vector: 8400e29b-41d4-4716-a716-446655440000.
        let wire: [u8; 16] = [
            0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0x47, 0x16, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let p = super::decode_param_binary(&wire, &Type::UUID).expect("decode");
        match p {
            ScalarParam::Text(s) => {
                assert_eq!(s, "8400e29b-41d4-4716-a716-446655440000");
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn decode_param_binary_uuid_wrong_length_is_22p03() {
        let err =
            super::decode_param_binary(&[0u8; 15], &Type::UUID).expect_err("short UUID must error");
        match err {
            PgWireError::UserError(info) => assert_eq!(info.code, "22P03"),
            other => panic!("expected UserError, got {other:?}"),
        }
    }

    // ── TIMESTAMPTZ param decode ─────────────────────────────────────────────

    /// Binary TIMESTAMPTZ = i64 micros since the PG epoch (2000-01-01 UTC);
    /// decodes to the typed `Timestamptz` variant rebased to Unix-epoch
    /// micros. Naive TIMESTAMP keeps the historical naive text rendering.
    #[test]
    fn decode_param_binary_timestamptz_is_typed_unix_micros() {
        // 2026-01-02 03:04:05.123456 UTC == 1_767_323_045_123_456 µs Unix.
        let unix_us = 1_767_323_045_123_456_i64;
        let pg_us = unix_us - crate::types::PG_EPOCH_MICROS_FROM_UNIX;
        let wire = pg_us.to_be_bytes();
        let p = super::decode_param_binary(&wire, &Type::TIMESTAMPTZ).expect("decode");
        assert_eq!(p, ScalarParam::Timestamptz(unix_us));
        // Same wire bytes declared as naive TIMESTAMP stay text-rendered.
        let p = super::decode_param_binary(&wire, &Type::TIMESTAMP).expect("decode");
        match p {
            ScalarParam::Text(s) => assert_eq!(s, "2026-01-02 03:04:05.123456"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    /// Text TIMESTAMPTZ accepts RFC3339 / PG-shaped zoned / naive forms;
    /// non-UTC offsets normalise to UTC; garbage degrades to verbatim Text.
    #[test]
    fn decode_param_text_timestamptz_forms() {
        let unix_us = 1_767_323_045_123_456_i64; // 2026-01-02 03:04:05.123456Z
        for s in [
            "2026-01-02T03:04:05.123456Z",
            "2026-01-02 03:04:05.123456+00",
            "2026-01-02 03:04:05.123456+00:00",
            "2026-01-02T03:04:05.123456+00",
            "2026-01-02 03:04:05.123456", // naive → UTC
            "2026-01-02 05:04:05.123456+02",
        ] {
            let p = super::decode_param_text(s.as_bytes(), &Type::TIMESTAMPTZ)
                .unwrap_or_else(|e| panic!("decode {s:?}: {e}"));
            assert_eq!(p, ScalarParam::Timestamptz(unix_us), "input {s:?}");
        }
        let p = super::decode_param_text(b"not-a-timestamp", &Type::TIMESTAMPTZ).expect("decode");
        assert_eq!(p, ScalarParam::Text("not-a-timestamp".to_owned()));
    }

    // ── NUMERIC binary decode (#118) ─────────────────────────────────────────
    //
    // Wire body (no outer length prefix — the Bind framer already stripped it):
    //   u16 ndigits | i16 weight | u16 sign | u16 dscale | u16 digits[ndigits]
    //
    // Helper: assemble the body for given (ndigits, weight, sign, dscale, digits).
    fn numeric_body(weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
        let mut v = Vec::with_capacity(8 + digits.len() * 2);
        v.extend_from_slice(&(digits.len() as u16).to_be_bytes());
        v.extend_from_slice(&weight.to_be_bytes());
        v.extend_from_slice(&sign.to_be_bytes());
        v.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    /// Zero: ndigits=0, weight=0, sign=0x0000, dscale=0 → "0".
    #[test]
    fn decode_param_binary_numeric_zero() {
        let body = numeric_body(0, 0x0000, 0, &[]);
        let p = super::decode_param_binary(&body, &Type::NUMERIC).expect("decode");
        match p {
            ScalarParam::Text(s) => assert_eq!(s, "0"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    /// Negative scale-zero integer: -1 → ndigits=1, weight=0, sign=0x4000,
    /// dscale=0, digits=[1] → "-1".
    #[test]
    fn decode_param_binary_numeric_negative_one() {
        let body = numeric_body(0, 0x4000, 0, &[1]);
        let p = super::decode_param_binary(&body, &Type::NUMERIC).expect("decode");
        match p {
            ScalarParam::Text(s) => assert_eq!(s, "-1"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    /// Large positive with fractional part: 1234567890.1234 →
    ///   int_part = 1234567890 → groups [12, 3456, 7890], weight=2
    ///   frac_part = .1234 → group [1234], dscale=4
    ///   ndigits=4, weight=2, sign=0, dscale=4, digits=[12,3456,7890,1234]
    /// Expected output: "1234567890.1234".
    #[test]
    fn decode_param_binary_numeric_large_positive_with_fraction() {
        let body = numeric_body(2, 0x0000, 4, &[12, 3456, 7890, 1234]);
        let p = super::decode_param_binary(&body, &Type::NUMERIC).expect("decode");
        match p {
            ScalarParam::Text(s) => assert_eq!(s, "1234567890.1234"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    /// Round-trip through types::encode_numeric_binary: encode raw=12345,
    /// scale=2 (= 123.45) and re-decode. The encoder prefixes a 4-byte body
    /// length; skip it to get the decoder's input.
    #[test]
    fn decode_param_binary_numeric_round_trip_123_45() {
        // Body (hand-assembled to match encode_numeric_binary's output):
        // raw=12345, scale=2 → int=123, frac=45
        // int_digits=[123], frac_groups=1, padded=4500 → digits=[123,4500]
        // weight=0, sign=0, dscale=2.
        let body = numeric_body(0, 0x0000, 2, &[123, 4500]);
        let p = super::decode_param_binary(&body, &Type::NUMERIC).expect("decode");
        match p {
            ScalarParam::Text(s) => assert_eq!(s, "123.45"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    /// NaN sign (0xC000) is rejected with 22P03 — the encoder never emits it
    /// and the engine has no NUMERIC NaN representation.
    #[test]
    fn decode_param_binary_numeric_nan_rejected() {
        let body = numeric_body(0, 0xC000, 0, &[]);
        let err = super::decode_param_binary(&body, &Type::NUMERIC)
            .expect_err("NaN must be rejected");
        match err {
            PgWireError::UserError(info) => assert_eq!(info.code, "22P03"),
            other => panic!("expected UserError, got {other:?}"),
        }
    }

    /// Malformed: header says 2 digits but only 1 digit follows → 22P03.
    #[test]
    fn decode_param_binary_numeric_short_body_rejected() {
        let mut body = numeric_body(0, 0x0000, 0, &[1, 2]);
        body.truncate(body.len() - 2); // drop the last digit
        let err = super::decode_param_binary(&body, &Type::NUMERIC)
            .expect_err("short body must be rejected");
        match err {
            PgWireError::UserError(info) => assert_eq!(info.code, "22P03"),
            other => panic!("expected UserError, got {other:?}"),
        }
    }

    #[test]
    fn decode_param_text_jsonb_passes_through() {
        let p = super::decode_param_text(br#"{"k":"v"}"#, &Type::JSONB).expect("decode");
        match p {
            ScalarParam::Text(s) => assert_eq!(s, r#"{"k":"v"}"#),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn decode_param_text_uuid_passes_through() {
        let p = super::decode_param_text(b"8400e29b-41d4-4716-a716-446655440000", &Type::UUID)
            .expect("decode");
        match p {
            ScalarParam::Text(s) => {
                assert_eq!(s, "8400e29b-41d4-4716-a716-446655440000");
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    // ── sql_is_write_dml ─────────────────────────────────────────────────────

    #[test]
    fn write_dml_detects_insert() {
        assert!(super::sql_is_write_dml("INSERT INTO t (a) VALUES (1)"));
        assert!(super::sql_is_write_dml("  insert into t (a) values (1)"));
        assert!(super::sql_is_write_dml("INSERT INTO t SELECT * FROM src"));
    }

    #[test]
    fn write_dml_detects_update() {
        assert!(super::sql_is_write_dml("UPDATE t SET a = 1 WHERE id = 2"));
        assert!(super::sql_is_write_dml("update t set a = 1"));
    }

    #[test]
    fn write_dml_detects_delete() {
        assert!(super::sql_is_write_dml("DELETE FROM t WHERE id = 3"));
        assert!(super::sql_is_write_dml("delete from t"));
    }

    #[test]
    fn write_dml_rejects_select() {
        assert!(!super::sql_is_write_dml("SELECT * FROM t"));
        assert!(!super::sql_is_write_dml("  select id from t where id = 1"));
    }

    #[test]
    fn write_dml_rejects_ddl() {
        assert!(!super::sql_is_write_dml("CREATE TABLE t (id BIGINT)"));
        assert!(!super::sql_is_write_dml("ALTER TABLE t ADD COLUMN x INT"));
        assert!(!super::sql_is_write_dml("DROP TABLE t"));
    }

    #[test]
    fn write_dml_handles_leading_comment() {
        // Block comment before the verb.
        assert!(super::sql_is_write_dml(
            "/* write data */ INSERT INTO t (a) VALUES (1)"
        ));
        // Line comment before the verb.
        assert!(super::sql_is_write_dml(
            "-- seed row\nINSERT INTO t (a) VALUES (1)"
        ));
        // SELECT after a comment is still not a write.
        assert!(!super::sql_is_write_dml("/* read */ SELECT * FROM t"));
    }

    #[test]
    fn write_dml_rejects_empty() {
        assert!(!super::sql_is_write_dml(""));
        assert!(!super::sql_is_write_dml("   "));
    }
}
