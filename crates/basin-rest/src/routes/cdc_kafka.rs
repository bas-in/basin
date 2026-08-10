//! `/v1/cdc/:project/kafka-sinks` — CDC Kafka sink config CRUD (ADR 0028
//! Phase 3).
//!
//! ## Surface
//!
//! | Verb + Path                                 | Action                          | Status |
//! |---------------------------------------------|---------------------------------|--------|
//! | `POST   /v1/cdc/:project/kafka-sinks`       | register a Kafka sink           | 201    |
//! | `GET    /v1/cdc/:project/kafka-sinks`       | list sinks + delivery state     | 200    |
//! | `DELETE /v1/cdc/:project/kafka-sinks/:id`   | drop a sink                     | 204    |
//!
//! ## Auth
//!
//! Project-scoped (same JWT path as the CDC SSE stream + the webhook CRUD). The
//! bearer's `project_id` claim must equal the `:project` path segment.
//!
//! ## Wire shape
//!
//! `POST` body:
//! ```json
//! { "brokers": "host:9092,host2:9092",
//!   "topic": "basin.{project}.{table}",   // {project}/{table} placeholders
//!   "tables": ["orders"],                  // optional; omit/null ⇒ all tables
//!   "ops": ["insert","update"],            // optional; omit/null ⇒ all ops
//!   "partition_key": "row_pk" }            // optional; default row_pk
//! ```
//! → `201 Created` with the minted id:
//! ```json
//! { "id": "01J…" }
//! ```
//!
//! `GET` response — observability for each sink (ADR Phase 3): the delivery
//! cursor (`last_seq`), last status, retry count, and lag are exposed.
//! `lag` is `null` until the worker has reported a cursor; it is the count of
//! events behind the project's ring high-water mark when known (best-effort).
//! ```json
//! { "kafka_sinks": [
//!   { "id":"01J…","brokers":"…","topic":"basin.{project}.{table}",
//!     "tables":["orders"],"ops":null,"partition_key":"row_pk","active":true,
//!     "last_seq":42,"last_status":"ok","retry_count":0,"disabled_at":null }
//! ] }
//! ```

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_catalog::{validate_cdc_kafka_sink, CdcKafkaError, CdcKafkaSinkDef, PartitionKey};
use basin_common::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

/// `POST /v1/cdc/:project/kafka-sinks` body.
#[derive(Debug, Deserialize)]
pub(crate) struct RegisterKafkaSinkRequest {
    pub brokers: String,
    pub topic: String,
    #[serde(default)]
    pub tables: Option<Vec<String>>,
    #[serde(default)]
    pub ops: Option<Vec<String>>,
    /// `row_pk` (default) / `project` / `table`.
    #[serde(default)]
    pub partition_key: Option<String>,
}

/// One row in the `GET` response — the subscription plus delivery state.
#[derive(Debug, Serialize)]
struct KafkaSinkView {
    id: String,
    brokers: String,
    topic: String,
    tables: Option<Vec<String>>,
    ops: Option<Vec<String>>,
    partition_key: String,
    active: bool,
    last_seq: u64,
    last_status: Option<String>,
    retry_count: u32,
    disabled_at: Option<String>,
}

fn assert_project_match(
    claims: &basin_auth::Claims,
    path_project: &ProjectId,
) -> Result<(), ApiError> {
    if claims.project_id != *path_project {
        return Err(ApiError::forbidden(format!(
            "token scoped to project {} cannot manage kafka sinks for project {}",
            claims.project_id, path_project,
        )));
    }
    Ok(())
}

fn parse_project(s: &str) -> Result<ProjectId, ApiError> {
    s.parse()
        .map_err(|e| ApiError::invalid(format!("invalid project id in path: {e}")))
}

/// `POST /v1/cdc/:project/kafka-sinks`
#[axum::debug_handler]
pub(crate) async fn register_kafka_sink(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_str): Path<String>,
    Json(req): Json<RegisterKafkaSinkRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = parse_project(&project_str)?;
    assert_project_match(&claims, &project)?;

    validate_cdc_kafka_sink(
        &req.brokers,
        &req.topic,
        req.ops.as_deref(),
        req.partition_key.as_deref(),
    )
    .map_err(cdc_kafka_err)?;

    let ops = req.ops.map(|v| {
        v.into_iter()
            .map(|o| o.trim().to_ascii_lowercase())
            .collect::<Vec<_>>()
    });
    let tables = req.tables.map(|v| {
        v.into_iter()
            .map(|t| t.trim().to_string())
            .collect::<Vec<_>>()
    });
    let partition_key = req
        .partition_key
        .as_deref()
        .and_then(PartitionKey::parse)
        .unwrap_or_default();

    let id = basin_cdc::mint_sink_id();
    let def = CdcKafkaSinkDef {
        id: id.clone(),
        project,
        brokers: req.brokers.trim().to_string(),
        topic: req.topic.trim().to_string(),
        tables,
        ops,
        partition_key,
        active: true,
    };

    state
        .cfg
        .engine
        .config()
        .catalog
        .register_cdc_kafka_sink(def)
        .await
        .map_err(ApiError::from)?;

    tracing::info!(%project, sink = %id, "cdc kafka sink registered");
    Ok((StatusCode::CREATED, Json(json!({ "id": id }))).into_response())
}

/// `GET /v1/cdc/:project/kafka-sinks`
#[axum::debug_handler]
pub(crate) async fn list_kafka_sinks(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_str): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = parse_project(&project_str)?;
    assert_project_match(&claims, &project)?;

    let rows = state
        .cfg
        .engine
        .config()
        .catalog
        .list_cdc_kafka_sinks(&project)
        .await;

    let views: Vec<KafkaSinkView> = rows
        .into_iter()
        .map(|r| KafkaSinkView {
            id: r.def.id,
            brokers: r.def.brokers,
            topic: r.def.topic,
            tables: r.def.tables,
            ops: r.def.ops,
            partition_key: r.def.partition_key.as_str().to_string(),
            active: r.def.active,
            last_seq: r.state.last_seq,
            last_status: r.state.last_status,
            retry_count: r.state.retry_count,
            disabled_at: r.state.disabled_at,
        })
        .collect();

    Ok(Json(json!({ "kafka_sinks": views })).into_response())
}

/// `DELETE /v1/cdc/:project/kafka-sinks/:id`
#[axum::debug_handler]
pub(crate) async fn delete_kafka_sink(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path((project_str, id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let project = parse_project(&project_str)?;
    assert_project_match(&claims, &project)?;

    state
        .cfg
        .engine
        .config()
        .catalog
        .drop_cdc_kafka_sink(&project, &id)
        .await
        .map_err(ApiError::from)?;

    tracing::info!(%project, sink = %id, "cdc kafka sink dropped");
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Map a [`CdcKafkaError`] to the REST error shape.
fn cdc_kafka_err(e: CdcKafkaError) -> ApiError {
    match e {
        CdcKafkaError::InvalidBrokers(m) => ApiError::invalid(format!("invalid brokers: {m}")),
        CdcKafkaError::InvalidTopic(m) => ApiError::invalid(format!("invalid topic: {m}")),
        CdcKafkaError::InvalidOp(m) => ApiError::invalid(format!(
            "invalid op filter {m:?} (allowed: insert, update, delete)"
        )),
        CdcKafkaError::InvalidPartitionKey(m) => ApiError::invalid(format!(
            "invalid partition_key {m:?} (allowed: row_pk, project, table)"
        )),
        CdcKafkaError::NotFound => ApiError::not_found("cdc kafka sink not found"),
    }
}
