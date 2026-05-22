//! Phase 5.21.E — `CREATE / ALTER / DROP PUBLICATION` SQL dispatch.
//!
//! sqlparser 0.52 does not recognise publication DDL statements (they are
//! PostgreSQL-specific extension syntax). This module provides text-pattern
//! pre-screens (consistent with how `CREATE REACTOR`, `CREATE SEQUENCE`, etc.
//! are handled) and executor functions for the three DDL verbs.
//!
//! ## Grammar supported
//!
//!   `CREATE PUBLICATION name FOR TABLE t1[, t2, …] [WHERE (filter)]`
//!   `CREATE PUBLICATION name FOR ALL TABLES`
//!   `ALTER PUBLICATION name ADD TABLE t1[, t2, …]`
//!   `DROP PUBLICATION [IF EXISTS] name`

use basin_catalog::PublicationTable;
use basin_common::{BasinError, Result};

use crate::{ExecResult, ProjectSession};

// ─── Intents ──────────────────────────────────────────────────────────────────

/// Parsed intent for `CREATE PUBLICATION`.
pub(crate) struct CreatePublicationIntent {
    pub pubname: String,
    pub all_tables: bool,
    /// `(table_name, row_filter_sql)` pairs.
    pub tables: Vec<(String, Option<String>)>,
}

/// Parsed intent for `ALTER PUBLICATION … ADD TABLE`.
pub(crate) struct AlterPublicationAddTable {
    pub pubname: String,
    pub tables: Vec<String>,
}

// ─── Matchers ─────────────────────────────────────────────────────────────────

/// Match `CREATE PUBLICATION <name> FOR TABLE t1[, …] [WHERE (…)]` or
/// `CREATE PUBLICATION <name> FOR ALL TABLES`.
///
/// Returns `Some(CreatePublicationIntent)` when matched.
pub(crate) fn match_create_publication(sql: &str) -> Option<CreatePublicationIntent> {
    let lower = sql.to_lowercase();
    // Must start with CREATE PUBLICATION (whitespace-normalised).
    let trimmed = lower.trim_start();
    if !trimmed.starts_with("create publication") {
        return None;
    }

    // Extract the publication name — the token immediately after the keyword.
    let rest = trimmed["create publication".len()..].trim_start();
    let (pubname, after_name) = first_word(rest)?;

    let after_name = after_name.trim_start();
    if !after_name.starts_with("for") {
        return None;
    }
    let after_for = after_name["for".len()..].trim_start();

    // Use the original (non-lower-cased) SQL to preserve table names.
    // Re-derive `after_for` position from original SQL.
    let orig_pos = {
        // Find "FOR" keyword position in original sql (case-insensitive).
        let lsql = sql.to_lowercase();
        // find "create publication" first
        let p0 = lsql.find("create publication")?;
        let p1 = lsql[p0..].find(" for ")? + p0 + 1; // skip the ' for ' in original
        p1 + "for ".len()
    };
    let orig_after_for = sql[orig_pos..].trim_start();

    if after_for.starts_with("all tables") {
        return Some(CreatePublicationIntent {
            pubname,
            all_tables: true,
            tables: Vec::new(),
        });
    }

    if !after_for.starts_with("table") {
        return None;
    }
    let after_table = &orig_after_for["table".len()..].trim_start();

    // Parse the table list: `t1 [WHERE (filter)][, t2 [WHERE (filter)], …]`
    let tables = parse_publication_table_list(after_table);

    Some(CreatePublicationIntent {
        pubname,
        all_tables: false,
        tables,
    })
}

/// Match `ALTER PUBLICATION <name> ADD TABLE t1[, …]`.
pub(crate) fn match_alter_publication(sql: &str) -> Option<AlterPublicationAddTable> {
    let lower = sql.trim_start().to_lowercase();
    if !lower.starts_with("alter publication") {
        return None;
    }
    let rest = lower["alter publication".len()..].trim_start();
    let (pubname, after_name) = first_word(rest)?;
    let after_name = after_name.trim_start();
    if !after_name.starts_with("add table") {
        return None;
    }
    // Re-derive table list from original SQL.
    let orig_lower = sql.to_lowercase();
    let add_table_pos = orig_lower.find("add table")? + "add table".len();
    let table_list_str = sql[add_table_pos..].trim_start();
    let tables: Vec<String> = table_list_str
        .split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect();

    Some(AlterPublicationAddTable { pubname, tables })
}

/// Match `DROP PUBLICATION [IF EXISTS] <name>`.
pub(crate) fn match_drop_publication(sql: &str) -> Option<String> {
    let lower = sql.trim_start().to_lowercase();
    if !lower.starts_with("drop publication") {
        return None;
    }
    let rest = lower["drop publication".len()..].trim_start();
    // Optional IF EXISTS
    let rest = if rest.starts_with("if exists") {
        rest["if exists".len()..].trim_start()
    } else {
        rest
    };
    let (name, _) = first_word(rest)?;
    Some(name)
}

// ─── Executors ────────────────────────────────────────────────────────────────

pub(crate) async fn exec_create_publication(
    sess: &ProjectSession,
    intent: CreatePublicationIntent,
) -> Result<ExecResult> {
    let registry = sess.engine.inner.publication_registry.clone();
    let tables: Vec<PublicationTable> = intent
        .tables
        .into_iter()
        .map(|(name, filter)| PublicationTable {
            table_name: name,
            row_filter: filter,
        })
        .collect();
    registry
        .create_publication(&sess.project, &intent.pubname, intent.all_tables, tables)
        .map_err(|e| BasinError::Catalog(e))?;
    Ok(ExecResult::Empty {
        tag: "CREATE PUBLICATION".to_string(),
    })
}

pub(crate) async fn exec_alter_publication(
    sess: &ProjectSession,
    intent: AlterPublicationAddTable,
) -> Result<ExecResult> {
    let registry = sess.engine.inner.publication_registry.clone();
    for table in intent.tables {
        registry
            .add_table_to_publication(&sess.project, &intent.pubname, &table, None)
            .map_err(|e| BasinError::Catalog(e))?;
    }
    Ok(ExecResult::Empty {
        tag: "ALTER PUBLICATION".to_string(),
    })
}

pub(crate) async fn exec_drop_publication(
    sess: &ProjectSession,
    pubname: &str,
) -> Result<ExecResult> {
    sess.engine
        .inner
        .publication_registry
        .drop_publication(&sess.project, pubname)
        .map_err(|e| BasinError::Catalog(e))?;
    Ok(ExecResult::Empty {
        tag: "DROP PUBLICATION".to_string(),
    })
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Return the first whitespace-delimited token and the remainder.
fn first_word(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }
    let end = s.find(|c: char| c.is_whitespace()).unwrap_or(s.len());
    let word = s[..end].to_string();
    let rest = &s[end..];
    Some((word, rest))
}

/// Parse a comma-separated list of `table_name [WHERE (filter)]` entries.
/// Returns `(table_name, row_filter)` pairs.
fn parse_publication_table_list(s: &str) -> Vec<(String, Option<String>)> {
    // Split on commas that are not inside parentheses.
    let mut entries = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    let chars: Vec<char> = s.chars().collect();
    for (i, &ch) in chars.iter().enumerate() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                entries.push(s[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = s[start..].trim().to_string();
    if !last.is_empty() {
        entries.push(last);
    }

    entries
        .into_iter()
        .filter_map(|entry| {
            let lower = entry.to_lowercase();
            let where_pos = lower.find(" where ");
            if let Some(pos) = where_pos {
                let table_name = entry[..pos].trim().to_string();
                let filter_raw = entry[pos + " where ".len()..].trim();
                // Strip surrounding parens if present.
                let filter = if filter_raw.starts_with('(') && filter_raw.ends_with(')') {
                    filter_raw[1..filter_raw.len() - 1].to_string()
                } else {
                    filter_raw.to_string()
                };
                Some((table_name, Some(filter)))
            } else {
                let table_name = entry.trim().to_string();
                if table_name.is_empty() {
                    None
                } else {
                    Some((table_name, None))
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_create_publication_for_table() {
        let sql = "CREATE PUBLICATION orders_pub FOR TABLE pub_orders";
        let intent = match_create_publication(sql).unwrap();
        assert_eq!(intent.pubname, "orders_pub");
        assert!(!intent.all_tables);
        assert_eq!(intent.tables.len(), 1);
        assert_eq!(intent.tables[0].0, "pub_orders");
        assert!(intent.tables[0].1.is_none());
    }

    #[test]
    fn match_create_publication_for_all_tables() {
        let sql = "CREATE PUBLICATION all_pub FOR ALL TABLES";
        let intent = match_create_publication(sql).unwrap();
        assert_eq!(intent.pubname, "all_pub");
        assert!(intent.all_tables);
    }

    #[test]
    fn match_create_publication_with_where_filter() {
        let sql = "CREATE PUBLICATION region_pub FOR TABLE pub_orders WHERE (region = 'EU')";
        let intent = match_create_publication(sql).unwrap();
        assert_eq!(intent.pubname, "region_pub");
        assert_eq!(intent.tables.len(), 1);
        assert!(intent.tables[0].1.is_some());
    }

    #[test]
    fn match_alter_publication_add_table() {
        let sql = "ALTER PUBLICATION orders_pub ADD TABLE pub_customers";
        let intent = match_alter_publication(sql).unwrap();
        assert_eq!(intent.pubname, "orders_pub");
        assert_eq!(intent.tables, vec!["pub_customers".to_string()]);
    }

    #[test]
    fn match_drop_publication_basic() {
        assert_eq!(
            match_drop_publication("DROP PUBLICATION orders_pub"),
            Some("orders_pub".to_string())
        );
    }

    #[test]
    fn match_drop_publication_if_exists() {
        assert_eq!(
            match_drop_publication("DROP PUBLICATION IF EXISTS orders_pub"),
            Some("orders_pub".to_string())
        );
    }

    #[test]
    fn no_match_for_non_publication_sql() {
        assert!(match_create_publication("SELECT 1").is_none());
        assert!(match_alter_publication("ALTER TABLE t ADD COLUMN x INT").is_none());
        assert!(match_drop_publication("DROP TABLE t").is_none());
    }
}
