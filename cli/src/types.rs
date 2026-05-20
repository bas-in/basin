//! types — the permissive decode structs the CLI renders. Every field
//! is `#[serde(default)]`-friendly (Option or skip_serializing_if) so a
//! backend tweak that adds a field never breaks the CLI.

use serde::{Deserialize, Serialize};

/// User mirrors `models.PublicUser` as far as the CLI cares.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct User {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub email: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub email_verified: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_superuser: bool,
}

/// Org is the read shape for `/v1/orgs/{slug}`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Org {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub slug: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plan: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub billing_email: String,
}

/// Project is the read shape for `/v1/projects/{ref}` and the list
/// envelope under `/v1/orgs/{slug}/projects`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Project {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub org_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub slug: String,
    #[serde(default, rename = "ref", skip_serializing_if = "String::is_empty")]
    pub r#ref: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub region: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default_branch: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub engine_tenant_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

/// Pgwire mirrors the create-project response's "pgwire" envelope.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Pgwire {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pgwire_user: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dbname: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub connection_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub engine_tenant: String,
}

/// CreateProjectResponse is the create envelope.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CreateProjectResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<Project>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgwire: Option<Pgwire>,
}

/// QueryResult mirrors `engine.RunQueryResult` for SQL passthrough.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<i64>,
    #[serde(default)]
    pub elapsed_ms: i64,
}

/// TableInfo is one row of `GET /v1/projects/:ref/tables`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, rename = "row_count")]
    pub rows: i64,
}

/// CloudVersion is the shape of `GET /v1/version`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CloudVersion {
    #[serde(default)]
    pub version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub commit: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub go: String,
}

/// CLIRelease is the slice of GitHub's releases/latest response we use.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CLIRelease {
    #[serde(default)]
    pub tag_name: String,
    #[serde(default)]
    pub html_url: String,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(b: &bool) -> bool {
    !*b
}
