//! Phase 5.21.E — publication registry (CREATE/ALTER/DROP PUBLICATION).
//!
//! A publication declares which tables (and optional row-filters) should be
//! included in the logical replication change stream. The pgoutput encoder
//! (Phase 5.21.C) consults the active publications when deciding whether to
//! emit a change frame for a given table.
//!
//! ## Design
//!
//! - One `PublicationRegistry` per process, keyed by `(ProjectId, pubname)`.
//! - `CREATE PUBLICATION name FOR TABLE t1, t2` → `create_publication`.
//! - `CREATE PUBLICATION name FOR ALL TABLES` → `create_publication` with empty
//!   `tables` list and `all_tables = true`.
//! - `ALTER PUBLICATION name ADD TABLE t` → `add_table_to_publication`.
//! - `DROP PUBLICATION name` → `drop_publication`.
//! - `pg_publication` / `pg_publication_tables` view → `list_publications` /
//!   `list_publication_tables`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use basin_common::ProjectId;

/// Per-table entry in a publication. Stores the table name and an optional
/// SQL WHERE clause (row filter) as a string.
#[derive(Clone, Debug)]
pub struct PublicationTable {
    pub table_name: String,
    /// Optional row-filter expression, e.g. `(region = 'EU')`.
    pub row_filter: Option<String>,
}

/// State of one publication.
#[derive(Clone, Debug)]
pub struct Publication {
    pub pubname: String,
    /// Whether this publication covers ALL tables (FOR ALL TABLES).
    pub all_tables: bool,
    /// Explicit table list (used when `all_tables = false`).
    pub tables: Vec<PublicationTable>,
    /// Which DML operations are published (currently always all three).
    pub pub_insert: bool,
    pub pub_update: bool,
    pub pub_delete: bool,
}

impl Publication {
    /// Check if `table_name` is covered by this publication.
    pub fn covers_table(&self, table_name: &str) -> bool {
        if self.all_tables {
            return true;
        }
        self.tables.iter().any(|t| t.table_name == table_name)
    }
}

/// Project-scoped map of pubname → Publication.
type ProjectPublications = HashMap<String, Publication>;

/// Process-wide publication registry keyed by project.
/// Cheap to clone (`Arc<Mutex<…>>` inside).
#[derive(Clone, Debug, Default)]
pub struct PublicationRegistry {
    inner: Arc<Mutex<HashMap<ProjectId, ProjectPublications>>>,
}

impl PublicationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new publication. Returns an error if a publication with that
    /// name already exists for the project.
    pub fn create_publication(
        &self,
        project: &ProjectId,
        pubname: &str,
        all_tables: bool,
        tables: Vec<PublicationTable>,
    ) -> Result<(), String> {
        let mut guard = self
            .inner
            .lock()
            .expect("PublicationRegistry lock poisoned");
        let project_pubs = guard.entry(*project).or_default();
        if project_pubs.contains_key(pubname) {
            return Err(format!("publication \"{pubname}\" already exists"));
        }
        let pub_ = Publication {
            pubname: pubname.to_string(),
            all_tables,
            tables,
            pub_insert: true,
            pub_update: true,
            pub_delete: true,
        };
        project_pubs.insert(pubname.to_string(), pub_);
        Ok(())
    }

    /// Add a table to an existing publication. Returns an error if the
    /// publication does not exist.
    pub fn add_table_to_publication(
        &self,
        project: &ProjectId,
        pubname: &str,
        table_name: &str,
        row_filter: Option<String>,
    ) -> Result<(), String> {
        let mut guard = self
            .inner
            .lock()
            .expect("PublicationRegistry lock poisoned");
        let project_pubs = guard
            .get_mut(project)
            .ok_or_else(|| format!("publication \"{pubname}\" does not exist"))?;
        let pub_ = project_pubs
            .get_mut(pubname)
            .ok_or_else(|| format!("publication \"{pubname}\" does not exist"))?;
        // Idempotent add: skip if already present.
        if !pub_.tables.iter().any(|t| t.table_name == table_name) {
            pub_.tables.push(PublicationTable {
                table_name: table_name.to_string(),
                row_filter,
            });
        }
        Ok(())
    }

    /// Drop an existing publication. Returns an error if it doesn't exist.
    pub fn drop_publication(&self, project: &ProjectId, pubname: &str) -> Result<(), String> {
        let mut guard = self
            .inner
            .lock()
            .expect("PublicationRegistry lock poisoned");
        let project_pubs = guard
            .get_mut(project)
            .ok_or_else(|| format!("publication \"{pubname}\" does not exist"))?;
        if project_pubs.remove(pubname).is_none() {
            return Err(format!("publication \"{pubname}\" does not exist"));
        }
        Ok(())
    }

    /// List all publications for a project (for `pg_publication` view).
    pub fn list_publications(&self, project: &ProjectId) -> Vec<Publication> {
        let guard = self
            .inner
            .lock()
            .expect("PublicationRegistry lock poisoned");
        let Some(project_pubs) = guard.get(project) else {
            return Vec::new();
        };
        project_pubs.values().cloned().collect()
    }

    /// List all (pubname, tablename) pairs for a project (for `pg_publication_tables` view).
    pub fn list_publication_tables(&self, project: &ProjectId) -> Vec<(String, String)> {
        let guard = self
            .inner
            .lock()
            .expect("PublicationRegistry lock poisoned");
        let Some(project_pubs) = guard.get(project) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for pub_ in project_pubs.values() {
            for t in &pub_.tables {
                out.push((pub_.pubname.clone(), t.table_name.clone()));
            }
        }
        out
    }

    /// Find the first publication that covers `table_name` (for slot/pgoutput filtering).
    pub fn find_publications_for_table<'a>(
        &self,
        project: &ProjectId,
        table_name: &str,
    ) -> Vec<String> {
        let guard = self
            .inner
            .lock()
            .expect("PublicationRegistry lock poisoned");
        let Some(project_pubs) = guard.get(project) else {
            return Vec::new();
        };
        project_pubs
            .values()
            .filter(|p| p.covers_table(table_name))
            .map(|p| p.pubname.clone())
            .collect()
    }
}
