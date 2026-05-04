//! pgwire simple-query and extended-query handling.
//!
//! The structure is:
//!
//! 1. A small internal trait `Session` abstracts whatever runs the SQL. The
//!    real implementation forwards to `basin_engine::TenantSession`; tests
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
//! ### Format-code policy (v1)
//!
//! Both parameters and result columns are handled as Postgres text format.
//! If the client requests binary format codes for parameters we still decode
//! them as UTF-8 text — most drivers send text by default and the rest fall
//! back when the response advertises text. Result columns are always text,
//! matching the simple-query behaviour.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use basin_common::Result;
use basin_engine::{
    BoundStatement, ExecResult, ScalarParam, StatementHandle, StatementSchema, TenantSession,
};
use futures::sink::{Sink, SinkExt};
use pgwire::api::auth::{
    finish_authentication, save_startup_parameters_to_metadata, DefaultServerParameterProvider,
    StartupHandler,
};
use pgwire::api::copy::NoopCopyHandler;
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
use pgwire::messages::data::{NoData, ParameterDescription};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Flush, Parse, ParseComplete,
    Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::messages::response::{
    CommandComplete, ReadyForQuery, TransactionStatus,
};
use pgwire::messages::simplequery::Query;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use tokio::sync::Mutex;

use crate::error::error_response;
use crate::resolver::TenantResolver;
use crate::types::{arrow_to_pg_type, encode_batches, row_description};

/// Internal trait abstracting whatever runs SQL. `TenantSession` implements it
/// transparently; tests substitute a fake.
///
/// The prepared-statement methods mirror the engine API one-for-one. Default
/// bodies for the engine impl forward straight through; tests only need to
/// implement what they exercise.
#[async_trait]
pub(crate) trait Session: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<ExecResult>;

    async fn prepare(
        &self,
        sql: &str,
    ) -> Result<(StatementHandle, StatementSchema)>;

    async fn bind(
        &self,
        handle: &StatementHandle,
        params: Vec<ScalarParam>,
    ) -> Result<BoundStatement>;

    async fn execute_bound(&self, bound: BoundStatement) -> Result<ExecResult>;

    /// Mirror of [`basin_engine::TenantSession::describe_statement`]. The
    /// router caches schemas at parse time so this is unused on the hot path,
    /// but the trait keeps it for parity with the engine API.
    #[allow(dead_code)]
    async fn describe_statement(
        &self,
        handle: &StatementHandle,
    ) -> Result<StatementSchema>;

    async fn close_statement(&self, handle: &StatementHandle);
}

#[async_trait]
impl Session for TenantSession {
    async fn execute(&self, sql: &str) -> Result<ExecResult> {
        TenantSession::execute(self, sql).await
    }

    async fn prepare(
        &self,
        sql: &str,
    ) -> Result<(StatementHandle, StatementSchema)> {
        TenantSession::prepare(self, sql).await
    }

    async fn bind(
        &self,
        handle: &StatementHandle,
        params: Vec<ScalarParam>,
    ) -> Result<BoundStatement> {
        TenantSession::bind(self, handle, params).await
    }

    async fn execute_bound(&self, bound: BoundStatement) -> Result<ExecResult> {
        TenantSession::execute_bound(self, bound).await
    }

    async fn describe_statement(
        &self,
        handle: &StatementHandle,
    ) -> Result<StatementSchema> {
        TenantSession::describe_statement(self, handle).await
    }

    async fn close_statement(&self, handle: &StatementHandle) {
        TenantSession::close_statement(self, handle).await
    }
}

/// Produce the full backend-message sequence for one simple-query exchange.
///
/// This is the pure-data heart of the simple-query protocol; the pgwire
/// integration just feeds these bytes to the wire. Splitting it out lets us
/// unit-test against a `FakeSession` without standing up a TCP socket or a
/// real `Engine`.
pub(crate) async fn simple_query_messages<S>(
    session: &S,
    sql: &str,
) -> Vec<PgWireBackendMessage>
where
    S: Session + ?Sized,
{
    let mut out = Vec::new();
    match session.execute(sql).await {
        Ok(ExecResult::Empty { tag }) => {
            out.push(PgWireBackendMessage::CommandComplete(
                pgwire::messages::response::CommandComplete::new(tag),
            ));
        }
        Ok(ExecResult::Rows { schema, batches }) => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            let rd = row_description(schema.as_ref());
            out.push(PgWireBackendMessage::RowDescription(rd));
            for row in encode_batches(&schema, &batches) {
                out.push(PgWireBackendMessage::DataRow(row));
            }
            out.push(PgWireBackendMessage::CommandComplete(
                pgwire::messages::response::CommandComplete::new(format!("SELECT {row_count}")),
            ));
        }
        Err(e) => {
            tracing::warn!(error = %e, "query failed");
            out.push(PgWireBackendMessage::ErrorResponse(error_response(&e)));
        }
    }
    out.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
        TransactionStatus::Idle,
    )));
    out
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
}

#[derive(Clone)]
struct StatementEntry {
    handle: StatementHandle,
    schema: StatementSchema,
    /// SQL string we sent to `prepare`. Useful for error logs.
    #[allow(dead_code)]
    sql: String,
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
}

impl ExtendedState {
    fn new() -> Self {
        Self {
            statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }
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
            let v: i32 = s.parse().map_err(|e: std::num::ParseIntError| parse_err("int4", &e))?;
            Ok(ScalarParam::Int4(v))
        }
        Type::INT8 => {
            let v: i64 = s.parse().map_err(|e: std::num::ParseIntError| parse_err("int8", &e))?;
            Ok(ScalarParam::Int8(v))
        }
        Type::FLOAT4 | Type::FLOAT8 => {
            let v: f64 = s.parse().map_err(|e: std::num::ParseFloatError| parse_err("float", &e))?;
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
        _ => Ok(ScalarParam::Text(s.to_owned())),
    }
}

/// Decode the Postgres binary wire format for the common scalar types. The
/// formats are documented in
/// <https://www.postgresql.org/docs/current/protocol-message-formats.html>;
/// integers are big-endian fixed-width, floats are IEEE 754 big-endian, bool
/// is one byte, text/varchar/bytea are raw bytes (no length prefix — the
/// surrounding `Bind` message already framed them).
fn decode_param_binary(
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
    match session.prepare(&msg.query).await {
        Ok((handle, schema)) => {
            state.lock().await.statements.insert(
                name,
                StatementEntry {
                    handle,
                    schema,
                    sql: msg.query,
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
        let declared = arrow_to_pg_type(&entry.schema.param_types[i]);
        let is_binary = match msg.parameter_format_codes.len() {
            0 => false,
            1 => msg.parameter_format_codes[0] == 1,
            _ => msg
                .parameter_format_codes
                .get(i)
                .copied()
                .unwrap_or(0)
                == 1,
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

    match session.bind(&entry.handle, params).await {
        Ok(bound) => {
            state.lock().await.portals.insert(
                portal_name,
                PortalEntry {
                    bound,
                    schema: entry.schema,
                    result_format_codes: msg.result_column_format_codes,
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

async fn handle_describe(
    state: &Mutex<ExtendedState>,
    msg: Describe,
) -> Vec<PgWireBackendMessage> {
    let name = msg.name.clone().unwrap_or_default();
    let g = state.lock().await;
    match msg.target_type {
        TARGET_TYPE_BYTE_STATEMENT => {
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
            // drivers that expect it (e.g. JDBC) keep going.
            let oids: Vec<u32> = entry
                .schema
                .param_types
                .iter()
                .map(|dt| arrow_to_pg_type(dt).oid())
                .collect();
            out.push(PgWireBackendMessage::ParameterDescription(
                ParameterDescription::new(oids),
            ));
            if entry.schema.columns.is_empty() {
                out.push(PgWireBackendMessage::NoData(NoData::new()));
            } else {
                let schema = arrow_schema::Schema::new(entry.schema.columns.clone());
                out.push(PgWireBackendMessage::RowDescription(row_description(&schema)));
            }
            out
        }
        TARGET_TYPE_BYTE_PORTAL => {
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
                vec![PgWireBackendMessage::RowDescription(row_description(&schema))]
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

async fn handle_execute<S>(
    state: &Mutex<ExtendedState>,
    session: &S,
    msg: Execute,
) -> Vec<PgWireBackendMessage>
where
    S: Session + ?Sized,
{
    let portal_name = msg.name.clone().unwrap_or_default();
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
                return vec![PgWireBackendMessage::ErrorResponse(info.into())];
            }
        }
    };

    let mut out = Vec::new();
    match session.execute_bound(entry.bound).await {
        Ok(ExecResult::Empty { tag }) => {
            out.push(PgWireBackendMessage::CommandComplete(
                CommandComplete::new(tag),
            ));
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
                    out.push(PgWireBackendMessage::ErrorResponse(error_response(&e)));
                    return out;
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
                out.push(PgWireBackendMessage::CommandComplete(
                    CommandComplete::new(format!("SELECT {emitted}")),
                ));
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "execute failed");
            out.push(PgWireBackendMessage::ErrorResponse(error_response(&e)));
        }
    }
    out
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
            let removed = state.lock().await.statements.remove(&name);
            if let Some(e) = removed {
                session.close_statement(&e.handle).await;
            }
        }
        TARGET_TYPE_BYTE_PORTAL => {
            state.lock().await.portals.remove(&name);
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
}

impl<S: Session + 'static> BasinExtendedQueryHandler<S> {
    pub(crate) fn new(session_slot: Arc<Mutex<Option<Arc<S>>>>) -> Self {
        Self {
            session_slot,
            state: Arc::new(Mutex::new(ExtendedState::new())),
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
        for m in handle_execute(self.state.as_ref(), session.as_ref(), message).await {
            client.feed(m).await?;
        }
        client.set_state(PgWireConnectionState::ReadyForQuery);
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
/// 3. Resolves the username -> tenant via the resolver.
/// 4. Opens an engine session and stores it in the per-connection slot, so
///    the simple-query handler can reach it later. If anything fails an
///    `ErrorResponse` is sent and the connection is closed.
pub(crate) struct BasinStartupHandler<F: SessionFactory + 'static> {
    factory: Arc<F>,
    resolver: Arc<dyn TenantResolver>,
    /// Per-connection session slot. Filled in once the user authenticates.
    slot: Arc<Mutex<Option<Arc<F::Session>>>>,
}

impl<F: SessionFactory + 'static> BasinStartupHandler<F> {
    pub(crate) fn new(
        factory: Arc<F>,
        resolver: Arc<dyn TenantResolver>,
        slot: Arc<Mutex<Option<Arc<F::Session>>>>,
    ) -> Self {
        Self {
            factory,
            resolver,
            slot,
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
            PgWireFrontendMessage::PasswordMessageFamily(_pwd) => {
                // PoC: passwords are accepted unconditionally. Documented in
                // the crate doc.
                let user = client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .ok_or(PgWireError::UserNameRequired)?;

                let tenant = self.resolver.resolve(&user).await.map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28000".to_owned(), // invalid_authorization_specification
                        format!("tenant resolution failed: {e}"),
                    )))
                })?;

                let session = self.factory.open(tenant).await.map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "XX000".to_owned(),
                        format!("open session failed: {e}"),
                    )))
                })?;

                *self.slot.lock().await = Some(session);

                tracing::info!(%tenant, %user, "tenant authenticated");

                finish_authentication(client, &DefaultServerParameterProvider::default()).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// Engine-shaped trait the startup handler can call to materialize a session
/// for an authenticated tenant. We define this rather than depend on `Engine`
/// directly so the test path can substitute a fake without touching the real
/// engine (whose body is still `unimplemented!()`).
#[async_trait]
pub(crate) trait SessionFactory: Send + Sync {
    type Session: Session + 'static;
    async fn open(&self, tenant: basin_common::TenantId) -> Result<Arc<Self::Session>>;
}

/// Bridge to `basin_engine::Engine`.
pub(crate) struct EngineSessionFactory(pub basin_engine::Engine);

#[async_trait]
impl SessionFactory for EngineSessionFactory {
    type Session = TenantSession;
    async fn open(&self, tenant: basin_common::TenantId) -> Result<Arc<TenantSession>> {
        let s = self.0.open_session(tenant).await?;
        Ok(Arc::new(s))
    }
}

/// Bridge to `basin_pool::SessionPool`. Acquires a `PooledSession` whose
/// `Drop` returns the underlying `TenantSession` to the pool when the
/// connection's session slot is cleared (i.e. when the per-connection state
/// goes away).
///
/// `client_key` is `None` for now; the JWT-aware path that derives a
/// per-user key from the bearer token is layered on by `JwtTenantResolver`
/// elsewhere — keeping the pool key here `None` matches today's "username
/// is the tenant" world.
pub(crate) struct PooledSessionFactory {
    pool: Arc<basin_pool::SessionPool>,
}

impl PooledSessionFactory {
    pub(crate) fn new(pool: Arc<basin_pool::SessionPool>) -> Self {
        Self { pool }
    }
}

/// Per-connection wrapper that owns the `PooledSession`. We forward the
/// `Session` trait through to the underlying `TenantSession`. Holding the
/// `PooledSession` for the lifetime of the connection's slot is what keeps
/// the engine session checked-out; when the slot drops, the wrapper drops,
/// the `PooledSession` drops, and the engine session goes back to the pool.
pub(crate) struct PooledSessionWrapper {
    pooled: basin_pool::PooledSession,
}

#[async_trait]
impl Session for PooledSessionWrapper {
    async fn execute(&self, sql: &str) -> Result<ExecResult> {
        self.pooled.session().execute(sql).await
    }

    async fn prepare(
        &self,
        sql: &str,
    ) -> Result<(StatementHandle, StatementSchema)> {
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

    async fn describe_statement(
        &self,
        handle: &StatementHandle,
    ) -> Result<StatementSchema> {
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
        tenant: basin_common::TenantId,
    ) -> Result<Arc<PooledSessionWrapper>> {
        let pooled = self.pool.acquire(tenant, None).await?;
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
    type CopyHandler = NoopCopyHandler;
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
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

/// Adapter that reads the session out of the per-connection slot at query
/// time. The slot is filled in by the startup handler; if a query arrives
/// before the slot is populated, that's a bug on the pgwire side (state
/// machine should refuse), so we treat it as an internal error.
pub(crate) struct BasinSimpleQueryHandlerSlot<S: Session + 'static> {
    pub(crate) slot: Arc<Mutex<Option<Arc<S>>>>,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use async_trait::async_trait;
    use basin_common::{BasinError, Result};
    use basin_engine::ExecResult;

    use super::*;

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
        async fn execute(&self, _sql: &str) -> Result<ExecResult> {
            let mut g = self.outcome.lock().await;
            let taken = g.take().expect("FakeSession used twice");
            match taken {
                FakeOutcome::Empty(tag) => Ok(ExecResult::Empty { tag }),
                FakeOutcome::Rows { schema, batches } => Ok(ExecResult::Rows { schema, batches }),
                FakeOutcome::Err(e) => Err(e),
            }
        }

        async fn prepare(
            &self,
            _sql: &str,
        ) -> Result<(StatementHandle, StatementSchema)> {
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

        async fn describe_statement(
            &self,
            _handle: &StatementHandle,
        ) -> Result<StatementSchema> {
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
        async fn execute(&self, _sql: &str) -> Result<ExecResult> {
            Ok(ExecResult::Empty {
                tag: "OK".into(),
            })
        }

        async fn prepare(
            &self,
            sql: &str,
        ) -> Result<(StatementHandle, StatementSchema)> {
            let h = StatementHandle::new();
            // Crude placeholder count for the test: scan once for `$N`s.
            let n = self.param_types.len();
            self.prepared
                .lock()
                .await
                .insert(h, (sql.to_owned(), n));
            Ok((
                h,
                StatementSchema {
                    param_types: self.param_types.clone(),
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
                    ScalarParam::Bool(b) => if *b { "TRUE".into() } else { "FALSE".into() },
                    ScalarParam::Float8(f) => format!("{f}"),
                    ScalarParam::Text(s) => format!("'{}'", s.replace('\'', "''")),
                    ScalarParam::Bytea(b) => format!("'{:?}'", b),
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

        async fn describe_statement(
            &self,
            _handle: &StatementHandle,
        ) -> Result<StatementSchema> {
            Ok(StatementSchema {
                param_types: self.param_types.clone(),
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

        let exec_msgs = handle_execute(
            &state,
            &sess,
            Execute::new(Some("p1".into()), 0),
        )
        .await;
        assert!(matches!(
            exec_msgs.first(),
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
    /// `SessionPool`. Two sequential `open`s for the same tenant should hit
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
        let tenant = basin_common::TenantId::new();

        // First open is a cold miss, releases on Drop, so the second sees the
        // cached session.
        let s1 = factory.open(tenant).await.unwrap();
        drop(s1);
        let s2 = factory.open(tenant).await.unwrap();
        drop(s2);

        let stats = pool.stats();
        assert_eq!(stats.misses, 1, "first open is the only miss, got {stats:?}");
        assert!(stats.hits >= 1, "second open should hit, got {stats:?}");
    }
}
