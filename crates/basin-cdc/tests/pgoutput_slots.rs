//! ADR 0028 Phase 4/5 integration tests: wal2json byte shape, pgoutput
//! end-to-end message stream, the slot/cursor catalog round-trip on BOTH
//! backends, and LSN-mapping monotonicity.
//!
//! These are NEW tests (not the landed phase1/phase2/kafka files). The
//! per-message byte layouts are unit-tested in the crate's `pgoutput` /
//! `wal2json` modules; here we exercise the cross-component contracts: a whole
//! commit's record stream → an ordered pgoutput message sequence, and the
//! durable slot cursor through the catalog.

use basin_catalog::{Catalog, CdcSlotDef, InMemoryCatalog, SlotPlugin};
use basin_cdc::pgoutput::{
    begin_lsn, commit_lsn, encode_begin, encode_commit, encode_record, lsn_to_seq, row_lsn,
    OutputPlugin, RelationRegistry, ReplicationSlot,
};
use basin_cdc::wal2json::{encode_transaction, Wal2JsonTxn};
use basin_cdc::CdcRecord;
use basin_common::ProjectId;
use serde_json::{json, Value};
use std::sync::Arc;

fn rec(op: u8, seq: u64, table: &str, before: Option<Value>, after: Option<Value>) -> CdcRecord {
    CdcRecord {
        seq,
        wal_lsn: 0,
        project: ProjectId::new(),
        table: table.to_string(),
        op,
        tx_id: None,
        committed_at: 1_700_000_000_000_000, // fixed unix micros
        causation_user: None,
        before,
        after,
    }
}

// ---------------------------------------------------------------------------
// wal2json — whole-transaction shape
// ---------------------------------------------------------------------------

#[test]
fn wal2json_transaction_change_array_in_commit_order() {
    let recs = vec![
        rec(1, 10, "orders", None, Some(json!({"id": 1, "total": 9.99}))),
        rec(
            2,
            11,
            "orders",
            Some(json!({"id": 1, "total": 9.99})),
            Some(json!({"id": 1, "total": 19.99})),
        ),
        rec(3, 12, "orders", Some(json!({"id": 1, "total": 19.99})), None),
    ];
    let v = serde_json::to_value(encode_transaction(&recs)).unwrap();
    let arr = v["change"].as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // INSERT — full columns, no oldkeys.
    assert_eq!(arr[0]["kind"], "insert");
    assert_eq!(arr[0]["columnnames"], json!(["id", "total"]));
    assert!(arr[0].get("oldkeys").is_none());
    // UPDATE — new image + oldkeys (prior image).
    assert_eq!(arr[1]["kind"], "update");
    assert_eq!(arr[1]["columnvalues"], json!([1, 19.99]));
    assert_eq!(arr[1]["oldkeys"]["keyvalues"], json!([1, 9.99]));
    // DELETE — oldkeys only.
    assert_eq!(arr[2]["kind"], "delete");
    assert!(arr[2].get("columnnames").is_none());
    assert_eq!(arr[2]["oldkeys"]["keyvalues"], json!([1, 19.99]));
}

#[test]
fn wal2json_v2_streaming_frames_bracket_a_commit() {
    // A streaming consumer sees B, then one action per row, then C.
    let b = serde_json::to_value(Wal2JsonTxn::begin()).unwrap();
    let i = serde_json::to_value(Wal2JsonTxn::row(&rec(1, 1, "t", None, Some(json!({"id": 1})))))
        .unwrap();
    let c = serde_json::to_value(Wal2JsonTxn::commit()).unwrap();
    assert_eq!(b["action"], "B");
    assert_eq!(i["action"], "I");
    assert_eq!(c["action"], "C");
}

// ---------------------------------------------------------------------------
// pgoutput — whole-commit message stream (Begin, Relation, row, Commit)
// ---------------------------------------------------------------------------

#[test]
fn pgoutput_commit_stream_is_begin_relation_rows_commit() {
    // Build the full ordered message stream for a 2-row commit (one new table).
    let seq = 100u64;
    let commit_ts = 1_700_000_000_000_000i64;
    let mut reg = RelationRegistry::new();

    let mut stream: Vec<Vec<u8>> = Vec::new();
    stream.push(encode_begin(seq, commit_ts));
    for (i, r) in [
        rec(1, seq, "orders", None, Some(json!({"id": 1}))),
        rec(1, seq, "orders", None, Some(json!({"id": 2}))),
    ]
    .iter()
    .enumerate()
    {
        let _ = i;
        stream.extend(encode_record(&mut reg, r));
    }
    stream.push(encode_commit(seq, commit_ts));

    // Tags, in order: B, R (first row mints relation), I, I (second row reuses
    // relation — no second R), C.
    let tags: Vec<u8> = stream.iter().map(|m| m[0]).collect();
    assert_eq!(tags, vec![b'B', b'R', b'I', b'I', b'C']);
}

#[test]
fn pgoutput_relation_emitted_once_across_records() {
    let mut reg = RelationRegistry::new();
    let a = encode_record(&mut reg, &rec(1, 1, "t", None, Some(json!({"id": 1}))));
    let b = encode_record(&mut reg, &rec(1, 2, "t", None, Some(json!({"id": 2}))));
    assert_eq!(a.iter().map(|m| m[0]).collect::<Vec<_>>(), vec![b'R', b'I']);
    assert_eq!(b.iter().map(|m| m[0]).collect::<Vec<_>>(), vec![b'I']);
}

// ---------------------------------------------------------------------------
// LSN mapping — monotonicity (the consumer's resume contract)
// ---------------------------------------------------------------------------

#[test]
fn lsn_mapping_is_strictly_monotone_across_a_stream() {
    // For a sequence of commits each with N rows, the synthetic LSNs of
    // Begin/rows/Commit are strictly increasing across the whole stream — the
    // invariant Debezium enforces.
    let mut last: u64 = 0;
    for seq in 1..=20u64 {
        let b = begin_lsn(seq);
        assert!(b > last, "begin_lsn must exceed prior commit's max");
        let mut prev = b;
        for row_ix in 0..3u64 {
            let r = row_lsn(seq, row_ix);
            assert!(r > prev, "row LSNs increase within a commit");
            prev = r;
        }
        let c = commit_lsn(seq);
        assert!(c > prev, "commit LSN is the commit's max");
        // The acked commit LSN recovers exactly this commit's seq.
        assert_eq!(lsn_to_seq(c), seq);
        last = c;
    }
}

// ---------------------------------------------------------------------------
// Slot / cursor catalog round-trip — IN-MEMORY backend (always runs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slot_round_trip_in_memory_backend() {
    let cat = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();

    // Register a pgoutput slot.
    cat.register_cdc_slot(CdcSlotDef {
        slot_name: "debezium".into(),
        project,
        plugin: SlotPlugin::Pgoutput,
        active: true,
    })
    .await
    .unwrap();

    // Duplicate registration is rejected.
    assert!(cat
        .register_cdc_slot(CdcSlotDef {
            slot_name: "debezium".into(),
            project,
            plugin: SlotPlugin::Pgoutput,
            active: true,
        })
        .await
        .is_err());

    // List shows it at the zero cursor.
    let rows = cat.list_cdc_slots(&project).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].def.slot_name, "debezium");
    assert_eq!(rows[0].state.confirmed_seq, 0);

    // Confirm an LSN → cursor advances monotonically.
    let acked = commit_lsn(42);
    cat.record_cdc_slot_confirm(&project, "debezium", lsn_to_seq(acked), "ok")
        .await
        .unwrap();
    let rows = cat.list_cdc_slots(&project).await;
    assert_eq!(rows[0].state.confirmed_seq, 42);
    assert_eq!(rows[0].state.restart_seq, 42);

    // A stale (lower) confirm never rewinds.
    cat.record_cdc_slot_confirm(&project, "debezium", 7, "ok")
        .await
        .unwrap();
    assert_eq!(
        cat.list_cdc_slots(&project).await[0].state.confirmed_seq,
        42
    );

    // Disable releases the retention hold but retains the row + cursor.
    cat.disable_cdc_slot(&project, "debezium").await.unwrap();
    let rows = cat.list_cdc_slots(&project).await;
    assert!(!rows[0].def.active);
    assert_eq!(rows[0].state.confirmed_seq, 42);

    // Drop removes it.
    cat.drop_cdc_slot(&project, "debezium").await.unwrap();
    assert!(cat.list_cdc_slots(&project).await.is_empty());
    assert!(cat.drop_cdc_slot(&project, "debezium").await.is_err());
}

#[tokio::test]
async fn slot_isolation_across_projects_in_memory() {
    let cat = Arc::new(InMemoryCatalog::new());
    let a = ProjectId::new();
    let b = ProjectId::new();
    cat.register_cdc_slot(CdcSlotDef {
        slot_name: "s".into(),
        project: a,
        plugin: SlotPlugin::Wal2json,
        active: true,
    })
    .await
    .unwrap();
    assert_eq!(cat.list_cdc_slots(&a).await.len(), 1);
    assert!(cat.list_cdc_slots(&b).await.is_empty(), "B never sees A's slot");
}

// ---------------------------------------------------------------------------
// Slot / cursor catalog round-trip — POSTGRES backend.
//
// Runs only when a local Postgres is reachable, mirroring the convention in
// `crates/basin-catalog/tests/leases.rs` (try-connect-then-skip rather than
// `#[ignore]`). A real-Debezium end-to-end is a heavier integration env and is
// out of scope for the unit suite (it would need a live consumer + a streaming
// START_REPLICATION wire path — see the router replication.rs deferral).
// ---------------------------------------------------------------------------

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

#[tokio::test]
async fn slot_round_trip_postgres_backend_if_available() {
    use basin_catalog::PostgresCatalog;

    let schema = format!("cdc_slot_test_{}", ulid::Ulid::new().to_string().to_lowercase());
    let cat = match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PostgresCatalog::connect_with_schema(PG_URL, &schema),
    )
    .await
    {
        Ok(Ok(c)) => c,
        _ => {
            eprintln!("postgres unreachable, skipping slot_round_trip_postgres_backend");
            return;
        }
    };
    if cat.migrate().await.is_err() {
        eprintln!("postgres migrate failed, skipping");
        return;
    }

    let project = ProjectId::new();
    cat.register_cdc_slot(CdcSlotDef {
        slot_name: "pg_slot".into(),
        project,
        plugin: SlotPlugin::Pgoutput,
        active: true,
    })
    .await
    .unwrap();

    // Confirm advances the durable cursor (monotonic GREATEST).
    cat.record_cdc_slot_confirm(&project, "pg_slot", 99, "ok")
        .await
        .unwrap();
    cat.record_cdc_slot_confirm(&project, "pg_slot", 5, "ok") // stale
        .await
        .unwrap();

    let rows = cat.list_cdc_slots(&project).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state.confirmed_seq, 99, "GREATEST cursor on PG");
    assert_eq!(rows[0].state.restart_seq, 99);
    assert_eq!(rows[0].def.plugin, SlotPlugin::Pgoutput);

    cat.drop_cdc_slot(&project, "pg_slot").await.unwrap();
    assert!(cat.list_cdc_slots(&project).await.is_empty());
}

// A documented placeholder making the OutputPlugin↔SlotPlugin token agreement
// explicit (the router parses OutputPlugin; the catalog stores SlotPlugin; they
// must use identical wire tokens so a CREATE_REPLICATION_SLOT round-trips).
#[test]
fn output_plugin_and_slot_plugin_tokens_agree() {
    assert_eq!(OutputPlugin::Pgoutput.as_str(), SlotPlugin::Pgoutput.as_str());
    assert_eq!(OutputPlugin::Wal2json.as_str(), SlotPlugin::Wal2json.as_str());
    assert_eq!(
        ReplicationSlot::new("s", ProjectId::new(), OutputPlugin::Pgoutput)
            .plugin
            .as_str(),
        SlotPlugin::Pgoutput.as_str()
    );
}
