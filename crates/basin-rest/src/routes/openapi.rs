//! `GET /rest/v1/_openapi.json` — per-tenant OpenAPI 3.0 spec generated from
//! the live catalog. The spec lists every table the calling tenant owns with
//! the four CRUD operations and a `components.schemas` entry derived from the
//! Arrow `Schema` for that table.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use basin_common::TableName;
use serde_json::{json, Map, Value};

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

const BASIN_TYPE_KEY: &str = "BASIN_TYPE";
const BASIN_TYPE_JSONB: &str = "JSONB";
const BASIN_TYPE_UUID: &str = "UUID";

#[axum::debug_handler]
pub(crate) async fn openapi(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let claims = authorize(&state, &headers).await?;
    let catalog = &state.cfg.engine.config().catalog;

    let table_names: Vec<TableName> = catalog
        .list_tables(&claims.tenant_id)
        .await
        .map_err(ApiError::from)?;

    let mut paths = Map::new();
    let mut schemas = Map::new();
    for tn in &table_names {
        let meta = match catalog.load_table(&claims.tenant_id, tn).await {
            Ok(m) => m,
            // Racing dropper between list_tables and load_table — skip.
            Err(basin_common::BasinError::NotFound(_)) => continue,
            Err(e) => return Err(ApiError::from(e)),
        };
        let name = tn.as_str();
        let component_ref = format!("#/components/schemas/{name}");
        paths.insert(
            format!("/rest/v1/{name}"),
            table_path_item(name, &component_ref),
        );
        schemas.insert(
            name.to_string(),
            table_component_schema(meta.schema.fields()),
        );
    }

    let spec = json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Basin REST",
            "version": "v0.1",
            "description": "Auto-generated per-tenant OpenAPI spec for the Basin REST surface. Every table the tenant owns is exposed under /rest/v1/{table} with GET / POST / PATCH / DELETE.",
        },
        "paths": Value::Object(paths),
        "components": { "schemas": Value::Object(schemas) },
    });

    Ok(Json(spec))
}

fn table_path_item(name: &str, component_ref: &str) -> Value {
    let object_schema = json!({ "$ref": component_ref });
    let array_schema = json!({ "type": "array", "items": { "$ref": component_ref } });
    json!({
        "get": {
            "summary": format!("List rows from {name}"),
            "responses": {
                "200": {
                    "description": "Matching rows.",
                    "content": { "application/json": { "schema": array_schema } },
                },
            },
        },
        "post": {
            "summary": format!("Insert rows into {name}"),
            "requestBody": {
                "required": true,
                "content": { "application/json": { "schema": {
                    "oneOf": [object_schema.clone(), array_schema.clone()],
                } } },
            },
            "responses": {
                "201": { "description": "Rows inserted." },
            },
        },
        "patch": {
            "summary": format!("Update rows in {name} matching the query filter"),
            "requestBody": {
                "required": true,
                "content": { "application/json": { "schema": object_schema } },
            },
            "responses": {
                "200": { "description": "Rows updated." },
            },
        },
        "delete": {
            "summary": format!("Delete rows from {name} matching the query filter"),
            "responses": {
                "200": { "description": "Rows deleted." },
            },
        },
    })
}

fn table_component_schema(fields: &arrow_schema::Fields) -> Value {
    let mut props = Map::new();
    let mut required: Vec<Value> = Vec::new();
    for f in fields.iter() {
        props.insert(f.name().clone(), arrow_field_to_openapi(f));
        if !f.is_nullable() {
            required.push(Value::String(f.name().clone()));
        }
    }
    let mut obj = Map::new();
    obj.insert("type".into(), Value::String("object".into()));
    obj.insert("properties".into(), Value::Object(props));
    if !required.is_empty() {
        obj.insert("required".into(), Value::Array(required));
    }
    Value::Object(obj)
}

fn arrow_field_to_openapi(field: &Field) -> Value {
    let basin_type = field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str());
    match field.data_type() {
        DataType::Boolean => json!({ "type": "boolean" }),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => {
            json!({ "type": "integer", "format": "int32" })
        }
        DataType::Int64 | DataType::UInt64 => {
            json!({ "type": "integer", "format": "int64" })
        }
        DataType::Float32 => json!({ "type": "number", "format": "float" }),
        DataType::Float64 => json!({ "type": "number", "format": "double" }),
        DataType::Utf8 | DataType::LargeUtf8 => {
            if basin_type == Some(BASIN_TYPE_UUID) {
                json!({ "type": "string", "format": "uuid" })
            } else {
                json!({ "type": "string" })
            }
        }
        DataType::LargeBinary | DataType::Binary => {
            if basin_type == Some(BASIN_TYPE_JSONB) {
                json!({ "type": "object", "additionalProperties": true })
            } else {
                json!({ "type": "string", "format": "binary" })
            }
        }
        DataType::FixedSizeBinary(16) => json!({ "type": "string", "format": "uuid" }),
        DataType::FixedSizeBinary(_) => json!({ "type": "string", "format": "binary" }),
        DataType::FixedSizeList(inner, _) => match inner.data_type() {
            DataType::Float32 | DataType::Float64 => {
                json!({ "type": "array", "items": { "type": "number" } })
            }
            _ => json!({ "type": "array", "items": arrow_field_to_openapi(inner) }),
        },
        DataType::List(inner) | DataType::LargeList(inner) => {
            json!({ "type": "array", "items": arrow_field_to_openapi(inner) })
        }
        DataType::Timestamp(_, _) => json!({ "type": "string", "format": "date-time" }),
        _ => json!({}),
    }
}
