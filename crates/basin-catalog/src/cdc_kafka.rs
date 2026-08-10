//! Per-project CDC Kafka/Redpanda sink configs (ADR 0028 Phase 3).
//!
//! A Kafka sink is a registered route from a project's committed change stream
//! to a Kafka-compatible cluster (Confluent Cloud, MSK, Redpanda). It reuses
//! the exact subscription + delivery-cursor model the Phase 2 webhook sink
//! established ([`crate::cdc_webhooks`]): the catalog stores the immutable-ish
//! **subscription** ([`CdcKafkaSinkDef`]) — brokers, topic pattern, optional
//! `(table, op)` filters, partition-key strategy, the `active` flag — and the
//! mutable **delivery cursor / state** ([`CdcKafkaSinkState`]) — `last_seq`,
//! `last_status`, `retry_count`, `disabled_at`. The delivery worker (in
//! `basin-cdc`'s `kafka` module) resumes from the durable ring at `last_seq`,
//! produces records to Kafka, and advances `last_seq` on producer-ack. Only the
//! transport differs from the webhook sink; the discipline is identical.
//!
//! Like every other per-project catalog entity, the trait methods carry
//! **legacy-tolerant default impls** so a backend (or an old schema) that
//! predates this table behaves as "no Kafka sinks registered" rather than
//! erroring.

use basin_common::ProjectId;
use serde::{Deserialize, Serialize};

/// Partition-key strategy: how a produced record's Kafka key is derived.
///
/// Kafka guarantees ordering only **within a partition**; the key selects the
/// partition. The ADR (Phase 3 "Ordering") maps each project to a topic and
/// requires that events keep their per-project order. The default
/// [`PartitionKey::RowPk`] keys by the row's primary-key digest so all events
/// for one row land on one partition (per-row total order); [`PartitionKey::Project`]
/// keys every event by the project id (whole-project single-partition total
/// order); [`PartitionKey::Table`] keys by table name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionKey {
    /// Key by a stable digest of the row's primary-key columns (default). All
    /// events for one logical row are ordered; different rows may parallelise
    /// across partitions. Falls back to the project id when no PK is derivable.
    RowPk,
    /// Key by project id — every event for the project goes to one partition,
    /// giving whole-project total order at the cost of single-partition
    /// throughput.
    Project,
    /// Key by table name — per-table ordering, tables parallelise.
    Table,
}

impl Default for PartitionKey {
    fn default() -> Self {
        PartitionKey::RowPk
    }
}

impl PartitionKey {
    /// Parse a wire token (`row_pk` / `project` / `table`). Case-insensitive.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "row_pk" | "rowpk" | "pk" => Some(PartitionKey::RowPk),
            "project" => Some(PartitionKey::Project),
            "table" => Some(PartitionKey::Table),
            _ => None,
        }
    }

    /// The canonical wire token.
    pub fn as_str(&self) -> &'static str {
        match self {
            PartitionKey::RowPk => "row_pk",
            PartitionKey::Project => "project",
            PartitionKey::Table => "table",
        }
    }
}

/// One registered CDC Kafka sink subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcKafkaSinkDef {
    /// Opaque per-project sink id (ULID string). Minted at registration.
    pub id: String,
    pub project: ProjectId,
    /// Bootstrap brokers, comma-separated (`host:9092,host2:9092`). Passed
    /// straight to the producer.
    pub brokers: String,
    /// Topic name pattern. Supports `{project}` and `{table}` placeholders,
    /// e.g. `basin.{project}.{table}`. A pattern without placeholders is a
    /// single static topic for all events.
    pub topic: String,
    /// Optional table allow-list. `None`/empty ⇒ all tables.
    #[serde(default)]
    pub tables: Option<Vec<String>>,
    /// Optional op allow-list (`insert`/`update`/`delete`, lowercase). `None` ⇒
    /// all ops.
    #[serde(default)]
    pub ops: Option<Vec<String>>,
    /// Partition-key strategy (default per-row PK digest for ordering).
    #[serde(default)]
    pub partition_key: PartitionKey,
    /// Whether the worker should deliver to this sink. Cleared by the
    /// auto-disable path (max delivery age exceeded) or an explicit operator
    /// action; the row + cursor are retained for observability.
    pub active: bool,
}

impl CdcKafkaSinkDef {
    /// Does an event for `table` / `op` (lowercase op name) pass this sink's
    /// filters? `O(filter_len)`.
    pub fn matches(&self, table: &str, op: &str) -> bool {
        if let Some(tables) = self.tables.as_ref() {
            if !tables.is_empty() && !tables.iter().any(|t| t == table) {
                return false;
            }
        }
        if let Some(ops) = self.ops.as_ref() {
            if !ops.is_empty() && !ops.iter().any(|o| o.eq_ignore_ascii_case(op)) {
                return false;
            }
        }
        true
    }

    /// Render the concrete topic name for one event by expanding `{project}`
    /// and `{table}` placeholders in [`Self::topic`]. Deterministic.
    pub fn render_topic(&self, table: &str) -> String {
        self.topic
            .replace("{project}", &self.project.to_string())
            .replace("{table}", table)
    }
}

/// Mutable per-sink delivery state (the resumable cursor + observability).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcKafkaSinkState {
    /// Highest event `seq` this sink has had acked by the broker. The worker
    /// resumes from `replay_after(last_seq)`. `0` ⇒ never delivered.
    pub last_seq: u64,
    /// Last delivery outcome, for the GET route. Either `"ok"` or a producer
    /// error string.
    #[serde(default)]
    pub last_status: Option<String>,
    /// Consecutive failed attempts on the current (un-acked) batch. Reset to 0
    /// on a successful ack. Drives exponential backoff.
    #[serde(default)]
    pub retry_count: u32,
    /// RFC3339 timestamp at which the sink was auto-disabled. `None` while
    /// healthy.
    #[serde(default)]
    pub disabled_at: Option<String>,
}

/// A sink plus its current delivery state, returned by the GET route and
/// consumed by the delivery worker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcKafkaSinkRow {
    #[serde(flatten)]
    pub def: CdcKafkaSinkDef,
    pub state: CdcKafkaSinkState,
}

/// Errors specific to CDC Kafka sink registration.
#[derive(Debug)]
pub enum CdcKafkaError {
    /// `brokers` is empty or unparseable.
    InvalidBrokers(String),
    /// `topic` is empty.
    InvalidTopic(String),
    /// An op filter token is not one of `insert`/`update`/`delete`.
    InvalidOp(String),
    /// `partition_key` is not a known strategy token.
    InvalidPartitionKey(String),
    /// No sink with the given `(project, id)` exists.
    NotFound,
}

/// Validate a registration request: non-empty brokers + topic, known op
/// tokens, known partition-key token.
pub fn validate_registration(
    brokers: &str,
    topic: &str,
    ops: Option<&[String]>,
    partition_key: Option<&str>,
) -> Result<(), CdcKafkaError> {
    if brokers.trim().is_empty() {
        return Err(CdcKafkaError::InvalidBrokers(
            "brokers must be a non-empty bootstrap list".into(),
        ));
    }
    if topic.trim().is_empty() {
        return Err(CdcKafkaError::InvalidTopic(
            "topic must be non-empty".into(),
        ));
    }
    if let Some(ops) = ops {
        for op in ops {
            match op.trim().to_ascii_lowercase().as_str() {
                "insert" | "update" | "delete" => {}
                other => return Err(CdcKafkaError::InvalidOp(other.to_string())),
            }
        }
    }
    if let Some(pk) = partition_key {
        if PartitionKey::parse(pk).is_none() {
            return Err(CdcKafkaError::InvalidPartitionKey(pk.to_string()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(tables: Option<Vec<String>>, ops: Option<Vec<String>>) -> CdcKafkaSinkDef {
        CdcKafkaSinkDef {
            id: "01J".into(),
            project: ProjectId::new(),
            brokers: "localhost:9092".into(),
            topic: "basin.{project}.{table}".into(),
            tables,
            ops,
            partition_key: PartitionKey::default(),
            active: true,
        }
    }

    #[test]
    fn default_partition_key_is_row_pk() {
        assert_eq!(PartitionKey::default(), PartitionKey::RowPk);
        assert_eq!(def(None, None).partition_key, PartitionKey::RowPk);
    }

    #[test]
    fn partition_key_round_trips() {
        for pk in [
            PartitionKey::RowPk,
            PartitionKey::Project,
            PartitionKey::Table,
        ] {
            assert_eq!(PartitionKey::parse(pk.as_str()), Some(pk));
        }
        assert_eq!(PartitionKey::parse("PK"), Some(PartitionKey::RowPk));
        assert_eq!(PartitionKey::parse("bogus"), None);
    }

    #[test]
    fn render_topic_expands_placeholders() {
        let d = def(None, None);
        let rendered = d.render_topic("orders");
        assert!(rendered.starts_with("basin."));
        assert!(rendered.ends_with(".orders"));
        assert!(rendered.contains(&d.project.to_string()));
    }

    #[test]
    fn render_topic_static_passthrough() {
        let mut d = def(None, None);
        d.topic = "basin_events".into();
        assert_eq!(d.render_topic("orders"), "basin_events");
    }

    #[test]
    fn no_filters_matches_everything() {
        let d = def(None, None);
        assert!(d.matches("orders", "insert"));
        assert!(d.matches("anything", "delete"));
    }

    #[test]
    fn table_and_op_filters_restrict() {
        let d = def(Some(vec!["orders".into()]), Some(vec!["insert".into()]));
        assert!(d.matches("orders", "insert"));
        assert!(!d.matches("orders", "update"));
        assert!(!d.matches("users", "insert"));
    }

    #[test]
    fn empty_filter_vec_means_no_filter() {
        let d = def(Some(vec![]), Some(vec![]));
        assert!(d.matches("anything", "insert"));
    }

    #[test]
    fn validate_rejects_empty_brokers_and_topic() {
        assert!(matches!(
            validate_registration("", "t", None, None),
            Err(CdcKafkaError::InvalidBrokers(_))
        ));
        assert!(matches!(
            validate_registration("b:9092", "", None, None),
            Err(CdcKafkaError::InvalidTopic(_))
        ));
        assert!(validate_registration("b:9092", "t", None, None).is_ok());
    }

    #[test]
    fn validate_rejects_bad_op_and_partition_key() {
        assert!(matches!(
            validate_registration("b:9092", "t", Some(&["frob".into()]), None),
            Err(CdcKafkaError::InvalidOp(_))
        ));
        assert!(matches!(
            validate_registration("b:9092", "t", None, Some("hashmod")),
            Err(CdcKafkaError::InvalidPartitionKey(_))
        ));
        assert!(
            validate_registration("b:9092", "t", Some(&["UPDATE".into()]), Some("row_pk")).is_ok()
        );
    }
}
