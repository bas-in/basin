//! Per-project plain-view catalog.
//!
//! Stores SQL view definitions registered via `CREATE VIEW` / `CREATE OR
//! REPLACE VIEW`. Each `ViewDef` is keyed by `(ProjectId, name)` and holds
//! the SELECT body the executor inlines at query time.
//!
//! This is intentionally distinct from the continuous-aggregate / matview
//! path: plain views have no backing table, no physical data files, and no
//! refresh cycle. They are pure query-rewrite objects.

use basin_common::ProjectId;
use serde::{Deserialize, Serialize};

/// One plain-view catalog row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewDef {
    pub project: ProjectId,
    /// View name, as the user wrote it (mixed case preserved for display;
    /// lookups fold to lower-case).
    pub name: String,
    /// The SELECT body stored at `CREATE VIEW … AS <query>`.
    pub query_sql: String,
}
