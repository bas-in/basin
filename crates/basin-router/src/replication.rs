//! Postgres logical-replication command path (ADR 0028 Phase 5 — router wiring).
//!
//! # What this is
//!
//! A consumer that speaks Postgres **logical replication** (Debezium, AWS DMS,
//! `pg_recvlogical`) opens a connection with the startup parameter
//! `replication=database` and then issues a small set of **replication
//! commands** over the simple-query protocol — not SQL, but the dedicated
//! commands the walsender understands:
//!
//! | Command                                      | Response shape                  |
//! |----------------------------------------------|---------------------------------|
//! | `IDENTIFY_SYSTEM`                            | one row: systemid/timeline/xlogpos/dbname |
//! | `CREATE_REPLICATION_SLOT <name> LOGICAL <p>` | one row: slot_name/consistent_point/snapshot_name/output_plugin |
//! | `DROP_REPLICATION_SLOT <name>`              | CommandComplete                 |
//! | `START_REPLICATION SLOT <name> LOGICAL <lsn>`| **CopyBothResponse** + a stream of `CopyData(XLogData(pgoutput))`, with `StandbyStatusUpdate` feedback |
//! | `TIMELINE_HISTORY <tli>`                    | one row (or error for tli 1)    |
//!
//! This module recognises and parses those commands and builds the response
//! **message shapes** for the query-protocol ones (`IDENTIFY_SYSTEM`,
//! `CREATE_REPLICATION_SLOT`, `DROP_REPLICATION_SLOT`). `START_REPLICATION` is
//! the one that requires switching the connection into **CopyBoth streaming
//! mode** — a protocol mode the framework's `SimpleQueryHandler` path does not
//! model — so it is recognised here and the streaming loop is the documented
//! remaining wiring (see [`START_REPLICATION_WIRING`]).
//!
//! # Flagged, off by default
//!
//! The whole path is gated on [`replication_enabled`] (env
//! `BASIN_PGWIRE_REPLICATION`, default off). When disabled, a replication
//! command falls through to the normal SQL path (which rejects it as a syntax
//! error, exactly as a non-replication PG connection would). When enabled, the
//! query-protocol commands answer and `START_REPLICATION` returns a structured
//! "not yet streaming" error naming the wiring point — it never fakes a stream.
//!
//! # LSN + slot model
//!
//! The synthetic-LSN mapping and the durable slot/cursor model live in
//! `basin_cdc::pgoutput` ([`seq_to_lsn`](basin_cdc::pgoutput::seq_to_lsn),
//! [`ReplicationSlot`](basin_cdc::pgoutput::ReplicationSlot)) and the additive
//! catalog slot table (`basin_catalog::cdc_slots`). This module is the wire
//! recogniser only; it borrows the LSN text format and plugin parse from there
//! so the router and the encoder agree on one mapping.

use basin_cdc::pgoutput::{commit_lsn, format_lsn, OutputPlugin};
use pgwire::api::Type;
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription};
use pgwire::messages::response::{CommandComplete, ReadyForQuery, TransactionStatus};
use pgwire::messages::PgWireBackendMessage;

/// Env var: enable the pgwire logical-replication command path. Default OFF —
/// a default Basin build answers no replication commands (they syntax-error in
/// the SQL path, exactly as a non-`replication=database` PG backend does).
pub const REPLICATION_ENABLED_ENV: &str = "BASIN_PGWIRE_REPLICATION";

/// The exact remaining wiring for a working `START_REPLICATION` stream, quoted
/// in the deferral error so the integration point is unambiguous.
pub const START_REPLICATION_WIRING: &str =
    "START_REPLICATION requires CopyBoth streaming mode: emit CopyBothResponse, \
     then loop CopyData(XLogData('w', wal_start, wal_end, ts, pgoutput-bytes)) \
     from basin_cdc::pgoutput::encode_record over ProjectRing::replay_after(\
     slot.restart_seq), reading client StandbyStatusUpdate ('r') feedback to \
     advance the slot cursor via ReplicationSlot::confirm_lsn. The pgwire \
     SimpleQueryHandler path does not model CopyBoth; this needs a dedicated \
     post-query streaming hook on the connection.";

/// Is the pgwire replication command path enabled?
pub fn replication_enabled() -> bool {
    std::env::var(REPLICATION_ENABLED_ENV)
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes"))
        .unwrap_or(false)
}

/// A recognised replication command (the subset Debezium/DMS issue).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationCommand {
    IdentifySystem,
    CreateSlot {
        slot_name: String,
        plugin: OutputPlugin,
        /// `true` when the command carried `TEMPORARY`.
        temporary: bool,
    },
    DropSlot {
        slot_name: String,
    },
    StartReplication {
        slot_name: String,
        /// Start LSN the consumer requested (`0/0` ⇒ from the slot's restart).
        start_lsn: u64,
    },
    TimelineHistory {
        timeline: u32,
    },
}

/// Strip a trailing semicolon + surrounding whitespace, leaving the bare
/// replication command for keyword matching.
fn normalise(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim()
}

/// Strip optional single/double quotes around an identifier (slot names may be
/// quoted: `CREATE_REPLICATION_SLOT "my_slot" LOGICAL pgoutput`).
fn unquote(s: &str) -> String {
    let t = s.trim();
    let t = t.strip_prefix('"').and_then(|x| x.strip_suffix('"')).unwrap_or(t);
    let t = t.strip_prefix('\'').and_then(|x| x.strip_suffix('\'')).unwrap_or(t);
    t.to_string()
}

/// Recognise a replication command in `sql`. Returns `None` for anything that
/// is not one of the known commands (so the caller falls through to SQL).
///
/// Parsing is keyword-based (replication commands are not SQL and `sqlparser`
/// does not accept them); we tokenise on whitespace and match the leading
/// keyword, then pull the documented operands.
pub fn parse_replication_command(sql: &str) -> Option<ReplicationCommand> {
    let s = normalise(sql);
    let upper = s.to_ascii_uppercase();

    if upper == "IDENTIFY_SYSTEM" {
        return Some(ReplicationCommand::IdentifySystem);
    }

    if let Some(rest) = strip_kw(&upper, s, "CREATE_REPLICATION_SLOT") {
        // CREATE_REPLICATION_SLOT <name> [TEMPORARY] LOGICAL <plugin> [opts...]
        let toks: Vec<&str> = rest.split_whitespace().collect();
        let slot_name = unquote(toks.first()?);
        let rest_upper = rest.to_ascii_uppercase();
        let temporary = rest_upper.contains(" TEMPORARY ")
            || rest_upper.contains(" TEMPORARY")
            || rest_upper.starts_with("TEMPORARY ");
        // The token after the LOGICAL keyword is the output plugin.
        let plugin = toks
            .iter()
            .position(|t| t.eq_ignore_ascii_case("LOGICAL"))
            .and_then(|i| toks.get(i + 1))
            .and_then(|p| OutputPlugin::parse(&unquote(p)))?;
        return Some(ReplicationCommand::CreateSlot {
            slot_name,
            plugin,
            temporary,
        });
    }

    if let Some(rest) = strip_kw(&upper, s, "DROP_REPLICATION_SLOT") {
        let slot_name = unquote(rest.split_whitespace().next()?);
        return Some(ReplicationCommand::DropSlot { slot_name });
    }

    if let Some(rest) = strip_kw(&upper, s, "START_REPLICATION") {
        // START_REPLICATION SLOT <name> LOGICAL <lsn> [ ( opts ) ]
        let toks: Vec<&str> = rest.split_whitespace().collect();
        let slot_idx = toks.iter().position(|t| t.eq_ignore_ascii_case("SLOT"))?;
        let slot_name = unquote(toks.get(slot_idx + 1)?);
        // The LSN follows the LOGICAL keyword (X/Y form, e.g. 0/0).
        let start_lsn = toks
            .iter()
            .position(|t| t.eq_ignore_ascii_case("LOGICAL"))
            .and_then(|i| toks.get(i + 1))
            .and_then(|l| basin_cdc::pgoutput::parse_lsn(l))
            .unwrap_or(0);
        return Some(ReplicationCommand::StartReplication {
            slot_name,
            start_lsn,
        });
    }

    if let Some(rest) = strip_kw(&upper, s, "TIMELINE_HISTORY") {
        let timeline = rest.split_whitespace().next()?.parse::<u32>().ok()?;
        return Some(ReplicationCommand::TimelineHistory { timeline });
    }

    None
}

/// If `upper` starts with `kw` (as a whole leading keyword), return the
/// original-case remainder after the keyword. Guards against false prefixes
/// (`IDENTIFY_SYSTEMX`) by requiring a whitespace/end boundary.
fn strip_kw<'a>(upper: &str, original: &'a str, kw: &str) -> Option<&'a str> {
    if !upper.starts_with(kw) {
        return None;
    }
    let after = &original[kw.len()..];
    if after.is_empty() || after.starts_with(char::is_whitespace) {
        Some(after.trim())
    } else {
        None
    }
}

/// pgwire text result format code (`0`), matching `crate::types`.
const FORMAT_CODE_TEXT: i16 = 0;

/// A single text-format field descriptor for a replication-command result row.
fn text_field(name: &str, ty: Type) -> FieldDescription {
    FieldDescription::new(
        name.to_string(),
        0,
        0,
        ty.oid(),
        -1,
        -1,
        FORMAT_CODE_TEXT,
    )
}

/// Encode a one-row text-format result: a `RowDescription`, one `DataRow`, and
/// a `CommandComplete` with `tag`, followed by `ReadyForQuery`.
fn one_row_result(
    fields: Vec<FieldDescription>,
    values: Vec<Option<String>>,
    tag: &str,
) -> Vec<PgWireBackendMessage> {
    let rd = RowDescription::new(fields);
    let mut buf = bytes::BytesMut::new();
    let ncols = values.len() as i16;
    for v in &values {
        match v {
            Some(s) => {
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
            None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    let row = DataRow::new(buf, ncols);
    vec![
        PgWireBackendMessage::RowDescription(rd),
        PgWireBackendMessage::DataRow(row),
        PgWireBackendMessage::CommandComplete(CommandComplete::new(tag.to_string())),
        PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
    ]
}

/// Build the `IDENTIFY_SYSTEM` response: systemid, timeline, xlogpos, dbname.
/// `confirmed_seq` is the current high-water ring `seq` for the project (the
/// xlogpos is its synthetic commit LSN); `system_id` is a stable per-project
/// identifier string; `dbname` is the project id.
pub fn identify_system_response(
    system_id: &str,
    confirmed_seq: u64,
    dbname: &str,
) -> Vec<PgWireBackendMessage> {
    let fields = vec![
        text_field("systemid", Type::TEXT),
        text_field("timeline", Type::INT4),
        text_field("xlogpos", Type::TEXT),
        text_field("dbname", Type::TEXT),
    ];
    let values = vec![
        Some(system_id.to_string()),
        Some("1".to_string()),
        Some(format_lsn(commit_lsn(confirmed_seq))),
        Some(dbname.to_string()),
    ];
    one_row_result(fields, values, "IDENTIFY_SYSTEM")
}

/// Build the `CREATE_REPLICATION_SLOT` response: slot_name, consistent_point,
/// snapshot_name (NULL — Basin exports no snapshot), output_plugin.
pub fn create_slot_response(
    slot_name: &str,
    consistent_seq: u64,
    plugin: OutputPlugin,
) -> Vec<PgWireBackendMessage> {
    let fields = vec![
        text_field("slot_name", Type::TEXT),
        text_field("consistent_point", Type::TEXT),
        text_field("snapshot_name", Type::TEXT),
        text_field("output_plugin", Type::TEXT),
    ];
    let values = vec![
        Some(slot_name.to_string()),
        Some(format_lsn(commit_lsn(consistent_seq))),
        None, // snapshot_name: Basin does not export a snapshot
        Some(plugin.as_str().to_string()),
    ];
    one_row_result(fields, values, "CREATE_REPLICATION_SLOT")
}

/// Build the `DROP_REPLICATION_SLOT` response: a bare CommandComplete +
/// ReadyForQuery (the command returns no rows).
pub fn drop_slot_response() -> Vec<PgWireBackendMessage> {
    vec![
        PgWireBackendMessage::CommandComplete(CommandComplete::new(
            "DROP_REPLICATION_SLOT".to_string(),
        )),
        PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_identify_system() {
        assert_eq!(
            parse_replication_command("IDENTIFY_SYSTEM"),
            Some(ReplicationCommand::IdentifySystem)
        );
        assert_eq!(
            parse_replication_command("identify_system;"),
            Some(ReplicationCommand::IdentifySystem)
        );
    }

    #[test]
    fn parses_create_slot_pgoutput() {
        let cmd = parse_replication_command(
            "CREATE_REPLICATION_SLOT debezium LOGICAL pgoutput",
        )
        .unwrap();
        assert_eq!(
            cmd,
            ReplicationCommand::CreateSlot {
                slot_name: "debezium".into(),
                plugin: OutputPlugin::Pgoutput,
                temporary: false,
            }
        );
    }

    #[test]
    fn parses_create_temporary_slot_wal2json_quoted() {
        // Slot names are single-token identifiers (optionally quoted); the
        // whitespace tokeniser does not support spaces inside a quoted name.
        let cmd = parse_replication_command(
            "CREATE_REPLICATION_SLOT \"my_slot\" TEMPORARY LOGICAL wal2json",
        )
        .unwrap();
        match cmd {
            ReplicationCommand::CreateSlot {
                slot_name,
                plugin,
                temporary,
            } => {
                assert_eq!(slot_name, "my_slot");
                assert_eq!(plugin, OutputPlugin::Wal2json);
                assert!(temporary);
            }
            other => panic!("expected CreateSlot, got {other:?}"),
        }
    }

    #[test]
    fn parses_start_replication_with_lsn() {
        let cmd = parse_replication_command(
            "START_REPLICATION SLOT debezium LOGICAL 16/B374D848 (proto_version '1', publication_names 'p')",
        )
        .unwrap();
        match cmd {
            ReplicationCommand::StartReplication {
                slot_name,
                start_lsn,
            } => {
                assert_eq!(slot_name, "debezium");
                assert_eq!(start_lsn, (0x16u64 << 32) | 0xB374_D848);
            }
            other => panic!("expected StartReplication, got {other:?}"),
        }
    }

    #[test]
    fn parses_drop_slot() {
        assert_eq!(
            parse_replication_command("DROP_REPLICATION_SLOT debezium"),
            Some(ReplicationCommand::DropSlot {
                slot_name: "debezium".into()
            })
        );
    }

    #[test]
    fn parses_timeline_history() {
        assert_eq!(
            parse_replication_command("TIMELINE_HISTORY 1"),
            Some(ReplicationCommand::TimelineHistory { timeline: 1 })
        );
    }

    #[test]
    fn non_replication_sql_is_not_recognised() {
        assert_eq!(parse_replication_command("SELECT 1"), None);
        assert_eq!(parse_replication_command("CREATE TABLE t (id int)"), None);
        // False-prefix guard.
        assert_eq!(parse_replication_command("IDENTIFY_SYSTEMX"), None);
    }

    #[test]
    fn create_slot_without_known_plugin_is_unrecognised() {
        // An unknown plugin token means we don't claim the command (falls
        // through to SQL, which errors honestly).
        assert_eq!(
            parse_replication_command("CREATE_REPLICATION_SLOT s LOGICAL bogus"),
            None
        );
    }

    #[test]
    fn identify_system_response_shape() {
        let msgs = identify_system_response("basin-proj-1", 42, "proj1");
        // RowDescription, DataRow, CommandComplete, ReadyForQuery.
        assert_eq!(msgs.len(), 4);
        assert!(matches!(msgs[0], PgWireBackendMessage::RowDescription(_)));
        assert!(matches!(msgs[1], PgWireBackendMessage::DataRow(_)));
        assert!(matches!(
            msgs[3],
            PgWireBackendMessage::ReadyForQuery(_)
        ));
    }

    #[test]
    fn create_slot_response_shape() {
        let msgs = create_slot_response("debezium", 7, OutputPlugin::Pgoutput);
        assert_eq!(msgs.len(), 4);
        assert!(matches!(msgs[0], PgWireBackendMessage::RowDescription(_)));
    }

    #[test]
    fn replication_disabled_by_default() {
        // Default env: off (the test process must not set the var).
        std::env::remove_var(REPLICATION_ENABLED_ENV);
        assert!(!replication_enabled());
    }
}
