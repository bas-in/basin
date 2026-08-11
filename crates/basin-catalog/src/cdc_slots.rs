//! Per-project durable logical-replication slots (ADR 0028 Phase 5).
//!
//! A replication slot is the **durable cursor** a `pgoutput` / `wal2json`
//! logical-replication consumer (Debezium, AWS DMS, `pg_recvlogical`) resumes
//! from. Where the Phase 5.21.B in-process `SlotRegistry`
//! ([`crate::replication_slots`]) holds a slot's *pending frame queue* in
//! memory for the SQL-function consumer path (`pg_logical_slot_get_changes`),
//! **this** catalog table persists the slot's *resume cursor* so a streaming
//! consumer that disconnects and reconnects resumes where it left off, and so
//! the CDC retention GC can hold back pruning below an active slot's cursor
//! (ADR §5 slot-hold).
//!
//! The model mirrors the Phase 3 Kafka sink exactly: an immutable-ish
//! **definition** ([`CdcSlotDef`]) — slot name, output plugin, the `active`
//! flag — and a mutable **delivery cursor / state** ([`CdcSlotState`]) —
//! `confirmed_seq` (the highest ring `seq` the consumer acked via a Standby
//! Status Update LSN → `seq`), `restart_seq` (the retention-hold watermark),
//! `last_status`. The streaming worker resumes from `replay_after(restart_seq)`
//! and advances `confirmed_seq` on each acked LSN.
//!
//! As with every per-project catalog entity, the trait methods carry
//! legacy-tolerant default impls so a backend (or an old schema) that predates
//! this table behaves as "no slots registered" rather than erroring.

use basin_common::ProjectId;
use serde::{Deserialize, Serialize};

/// The output plugin a slot streams with. Mirrors
/// `basin_cdc::pgoutput::OutputPlugin` but lives here so the catalog has no
/// dependency on basin-cdc (basin-cdc depends on basin-catalog, not the
/// reverse). The wire token is `pgoutput` / `wal2json`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotPlugin {
    Pgoutput,
    Wal2json,
}

impl Default for SlotPlugin {
    fn default() -> Self {
        SlotPlugin::Pgoutput
    }
}

impl SlotPlugin {
    /// Parse the plugin token a consumer names in `CREATE_REPLICATION_SLOT`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "pgoutput" => Some(SlotPlugin::Pgoutput),
            "wal2json" => Some(SlotPlugin::Wal2json),
            _ => None,
        }
    }

    /// The canonical wire token.
    pub fn as_str(&self) -> &'static str {
        match self {
            SlotPlugin::Pgoutput => "pgoutput",
            SlotPlugin::Wal2json => "wal2json",
        }
    }
}

/// One registered logical-replication slot (the durable definition).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcSlotDef {
    /// Slot name — the consumer-chosen identifier in CREATE_REPLICATION_SLOT.
    /// Unique per project (the catalog primary key is `(project, slot_name)`).
    pub slot_name: String,
    pub project: ProjectId,
    /// The output plugin the consumer requested.
    #[serde(default)]
    pub plugin: SlotPlugin,
    /// `true` while the slot should hold back retention GC and be streamable.
    /// A dropped slot removes the row; an inactive slot retains the row + cursor
    /// but releases the retention hold.
    pub active: bool,
}

/// Mutable per-slot cursor + observability.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcSlotState {
    /// Highest ring `seq` the consumer has confirmed (acked). The stream's
    /// progress marker. Monotonic (GREATEST upsert). `0` ⇒ never confirmed.
    pub confirmed_seq: u64,
    /// The `seq` the consumer must restart from on reconnect, and the retention
    /// hold watermark (GC must not prune below the min `restart_seq` of any
    /// active slot). Advances toward `confirmed_seq`.
    pub restart_seq: u64,
    /// Last delivery / ack outcome, for the status route.
    #[serde(default)]
    pub last_status: Option<String>,
}

/// A slot plus its current cursor state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcSlotRow {
    #[serde(flatten)]
    pub def: CdcSlotDef,
    pub state: CdcSlotState,
}

/// Errors specific to CDC slot registration.
#[derive(Debug)]
pub enum CdcSlotError {
    /// `slot_name` is empty or malformed.
    InvalidName(String),
    /// The plugin token is not `pgoutput` / `wal2json`.
    InvalidPlugin(String),
    /// No slot with the given `(project, slot_name)` exists.
    NotFound,
}

/// Validate a slot registration: non-empty name, known plugin token.
pub fn validate_registration(slot_name: &str, plugin: &str) -> Result<(), CdcSlotError> {
    if slot_name.trim().is_empty() {
        return Err(CdcSlotError::InvalidName(
            "slot name must be non-empty".into(),
        ));
    }
    // Postgres slot names are lower-case alnum + underscore; we accept the same
    // so a name minted here round-trips through a PG-shaped consumer.
    if !slot_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(CdcSlotError::InvalidName(format!(
            "slot name {slot_name:?} must be [a-z0-9_]"
        )));
    }
    if SlotPlugin::parse(plugin).is_none() {
        return Err(CdcSlotError::InvalidPlugin(plugin.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_round_trips_and_defaults_to_pgoutput() {
        assert_eq!(SlotPlugin::default(), SlotPlugin::Pgoutput);
        for p in [SlotPlugin::Pgoutput, SlotPlugin::Wal2json] {
            assert_eq!(SlotPlugin::parse(p.as_str()), Some(p));
        }
        assert_eq!(SlotPlugin::parse("PGOUTPUT"), Some(SlotPlugin::Pgoutput));
        assert_eq!(SlotPlugin::parse("bogus"), None);
    }

    #[test]
    fn validate_accepts_pg_style_names() {
        assert!(validate_registration("debezium_slot1", "pgoutput").is_ok());
        assert!(validate_registration("wal2json_s", "wal2json").is_ok());
    }

    #[test]
    fn validate_rejects_bad_name_and_plugin() {
        assert!(matches!(
            validate_registration("", "pgoutput"),
            Err(CdcSlotError::InvalidName(_))
        ));
        assert!(matches!(
            validate_registration("bad name", "pgoutput"),
            Err(CdcSlotError::InvalidName(_))
        ));
        assert!(matches!(
            validate_registration("ok", "bogus"),
            Err(CdcSlotError::InvalidPlugin(_))
        ));
    }
}
