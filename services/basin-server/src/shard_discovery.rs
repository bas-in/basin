//! Fly.io-aware self-identity derivation + dynamic peer discovery for the
//! per-partition write forwarder (#28 multi-node bulk ingest).
//!
//! # The Fly.io problem
//!
//! Two facts about how Basin runs on Fly.io break the static-env model the
//! partition router was first built for:
//!
//! 1. **All machines in a Fly app share the same env vars.** So
//!    `BASIN_REPLICA_ID` cannot be a per-machine constant baked into the app
//!    config — every machine would read the *same* value and they'd all claim
//!    the same identity. The self-id must instead be *derived at runtime* from
//!    something unique to the machine.
//! 2. **The machine set changes under autoscaling.** A static
//!    `BASIN_SHARD_PEERS` list goes stale the moment Fly adds or removes a
//!    machine, so the deterministic owner mapping
//!    (`peers[fnv1a(project,partition) % n]`) would point at dead nodes or
//!    miss live ones. The peer list must be *discovered live*.
//!
//! Both are solved here, and crucially both produce the SAME peer-URL string
//! format so a node recognises itself in the discovered list (`is_self`
//! matches byte-for-byte).
//!
//! ## Self-id derivation ([`derive_replica_id`])
//!
//! When `BASIN_REPLICA_ID` is explicitly set, it is honoured unchanged (tests
//! and non-Fly deploys). Otherwise, when both `FLY_MACHINE_ID` and
//! `FLY_APP_NAME` are present, the replica id is derived as this machine's own
//! routable 6PN REST URL:
//!
//! ```text
//! http://{FLY_MACHINE_ID}.vm.{FLY_APP_NAME}.internal:{rest_port}
//! ```
//!
//! `rest_port` is the port from `BASIN_REST_BIND` (default `5434`). This single
//! string is BOTH the shard lease-holder id AND the partition-router self-id,
//! so a node owns exactly the partitions the deterministic hash assigns to its
//! own URL.
//!
//! ## Peer discovery ([`discover_peers`] / [`DiscoveryConfig`])
//!
//! Precedence (documented gate):
//!
//! - If `BASIN_SHARD_PEERS` is explicitly set, it is used verbatim and
//!   discovery does **not** run (deterministic for tests / fixed-N clusters).
//! - Else, discovery runs when ALL of: `FLY_APP_NAME` is set, a forward secret
//!   (`BASIN_FORWARD_SECRET`) is set, and `BASIN_SHARD_ENABLED=1`. This is the
//!   exact set of conditions under which multi-node forwarding can actually do
//!   anything; gating on them avoids spurious DNS traffic on single-node dev.
//!
//! Discovery resolves Fly's special TXT record `vms.{FLY_APP_NAME}.internal`,
//! which lists every running machine in the app as space-separated
//! `machine_id region` pairs (multiple pairs per record, comma-separated). We
//! parse the machine ids and build each peer's REST URL with the IDENTICAL
//! format used by [`derive_replica_id`], then sort the list (so the fnv1a owner
//! mapping is stable regardless of the order DNS returned records in — two
//! nodes that discover the same machine set in different orders compute the
//! same owners).
//!
//! The TXT query is a hand-rolled DNS-over-UDP request to Fly's internal 6PN
//! resolver (`fdaa::3:53`). We deliberately avoid pulling in a DNS resolver
//! crate (none was in the workspace; `getaddrinfo`/`tokio::lookup_host` cannot
//! query TXT records). The wire-format builder + response parser are pure
//! functions, unit-tested with fixed byte buffers; only the actual socket I/O
//! is untested without a live Fly environment.
//!
//! ## Membership-change behaviour (correctness)
//!
//! When the discovered peer set changes, a partition's deterministic
//! `desired_owner` can move to a different node. The flip is safe WITHOUT data
//! migration:
//!
//! - The new owner acquires the partition's writer lease via CAS; the old
//!   owner's lease expires within the TTL (~15s). No double-write — the lease
//!   CAS serialises ownership.
//! - Writes in flight during the flip may hit a transient `LeaseNotHeld` and
//!   retry (already handled in Wave 2b).
//! - Already-written data is **not** migrated. Reads still see it via the
//!   shared object-store catalog + LIST regardless of which node wrote it.
//!
//! Live data rebalance is explicitly OUT OF SCOPE.

use std::net::SocketAddr;

/// Default REST port when `BASIN_REST_BIND` is unset or unparseable.
const DEFAULT_REST_PORT: u16 = 5434;

/// Fly's internal 6PN DNS resolver address (anycast `fdaa::3`, port 53).
const FLY_RESOLVER: &str = "[fdaa::3]:53";

/// Default refresh interval for the discovery background loop, in seconds.
pub const DEFAULT_DISCOVERY_INTERVAL_SECS: u64 = 15;

/// Build this machine's own routable peer REST URL from a Fly machine id, app
/// name and REST port. The single source of truth for the peer-URL format —
/// used for BOTH self-id derivation and per-peer URL construction in discovery,
/// so a node matches itself in the discovered list byte-for-byte.
pub fn fly_peer_url(machine_id: &str, app_name: &str, rest_port: u16) -> String {
    format!("http://{machine_id}.vm.{app_name}.internal:{rest_port}")
}

/// Extract the port from a `BASIN_REST_BIND` value (`host:port`); falls back to
/// [`DEFAULT_REST_PORT`] when unset/unparseable.
pub fn rest_port_from_bind(bind: Option<&str>) -> u16 {
    bind.and_then(|b| b.parse::<SocketAddr>().ok())
        .map(|s| s.port())
        .unwrap_or(DEFAULT_REST_PORT)
}

/// Resolve this process's replica id (shard lease holder AND partition-router
/// self-id), honouring the all-machines-share-env constraint on Fly.
///
/// Precedence:
/// 1. `explicit` (from `BASIN_REPLICA_ID`) — honoured verbatim if `Some` and
///    non-empty. Used by tests and non-Fly deploys.
/// 2. Derived Fly URL `http://{machine}.vm.{app}.internal:{port}` when both
///    `machine_id` and `app_name` are present.
/// 3. `None` otherwise — the caller keeps whatever default the shard config
///    produced (host:pid:salt).
///
/// Pure so it can be unit-tested without touching the process environment.
pub fn derive_replica_id(
    explicit: Option<&str>,
    machine_id: Option<&str>,
    app_name: Option<&str>,
    rest_port: u16,
) -> Option<String> {
    if let Some(id) = explicit {
        let id = id.trim();
        if !id.is_empty() {
            return Some(id.to_string());
        }
    }
    match (machine_id, app_name) {
        (Some(m), Some(a)) if !m.trim().is_empty() && !a.trim().is_empty() => {
            Some(fly_peer_url(m.trim(), a.trim(), rest_port))
        }
        _ => None,
    }
}

/// Whether Fly DNS peer discovery should run, given the resolved environment.
/// `BASIN_SHARD_PEERS` (the `shard_peers` arg) being set takes precedence and
/// disables discovery — the static list is then deterministic for tests/fixed
/// clusters. Otherwise discovery runs only when forwarding could actually
/// happen (Fly app + forward secret + shard enabled). Pure for testability.
pub fn discovery_enabled(
    shard_peers: Option<&str>,
    app_name: Option<&str>,
    forward_secret_set: bool,
    shard_enabled: bool,
) -> bool {
    let static_set = shard_peers.map(|s| !s.trim().is_empty()).unwrap_or(false);
    if static_set {
        return false;
    }
    app_name.map(|a| !a.trim().is_empty()).unwrap_or(false) && forward_secret_set && shard_enabled
}

/// Parse the payload of Fly's `vms.{app}.internal` TXT record into the list of
/// machine ids. The record lists running machines as `machine_id region`
/// pairs; pairs are separated by commas and/or whitespace, and a single TXT
/// resource record may itself be split into multiple character-strings (which
/// the DNS parser concatenates before calling this).
///
/// Example payload: `"3d8d9eldng21 fra,1781570c024d18 jnb"` →
/// `["1781570c024d18", "3d8d9eldng21"]` (deduped, NOT yet URL-ified or sorted
/// here — see [`peer_urls_from_machine_ids`]).
pub fn parse_vms_txt(payload: &str) -> Vec<String> {
    let mut ids = Vec::new();
    // Split on commas first (pair separator), then take the first whitespace
    // token of each pair as the machine id (the region follows it).
    for pair in payload.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some(id) = pair.split_whitespace().next() {
            let id = id.trim();
            if !id.is_empty() && !ids.contains(&id.to_string()) {
                ids.push(id.to_string());
            }
        }
    }
    ids
}

/// Build the sorted, deduplicated peer-URL list from a set of machine ids.
/// Sorting makes the fnv1a owner mapping identical across nodes that discovered
/// the same machine set in different orders. The self URL (derived from
/// `self_machine_id`, when given) is guaranteed to be present so the router
/// always recognises this node.
pub fn peer_urls_from_machine_ids(
    machine_ids: &[String],
    app_name: &str,
    rest_port: u16,
    self_machine_id: Option<&str>,
) -> Vec<String> {
    let mut urls: Vec<String> = machine_ids
        .iter()
        .map(|m| fly_peer_url(m.trim(), app_name, rest_port))
        .collect();
    if let Some(self_id) = self_machine_id {
        let self_id = self_id.trim();
        if !self_id.is_empty() {
            urls.push(fly_peer_url(self_id, app_name, rest_port));
        }
    }
    urls.sort();
    urls.dedup();
    urls
}

/// Resolved discovery inputs (read once from the environment by the server).
#[derive(Clone, Debug)]
pub struct DiscoveryConfig {
    /// `FLY_APP_NAME` — the 6PN app whose machines we resolve.
    pub app_name: String,
    /// `FLY_MACHINE_ID` for this node, so its own URL is always in the list.
    pub self_machine_id: Option<String>,
    /// REST port for every peer URL (this app is homogeneous).
    pub rest_port: u16,
    /// Background refresh interval.
    pub interval_secs: u64,
}

/// Whether the peer set materially changed (membership add/remove). Both lists
/// are assumed already sorted+deduped (as produced by
/// [`peer_urls_from_machine_ids`]); a plain inequality is then exact.
pub fn peer_set_changed(old: &[String], new: &[String]) -> bool {
    old != new
}

/// The added / removed peers between two sorted peer lists (for logging).
pub fn peer_set_diff(old: &[String], new: &[String]) -> (Vec<String>, Vec<String>) {
    let added: Vec<String> = new.iter().filter(|p| !old.contains(p)).cloned().collect();
    let removed: Vec<String> = old.iter().filter(|p| !new.contains(p)).cloned().collect();
    (added, removed)
}

// ---------------------------------------------------------------------------
// DNS-over-UDP TXT query (no resolver-crate dependency).
// ---------------------------------------------------------------------------

/// Build a DNS query message for a TXT record on `name`. Standard query, single
/// question, recursion desired. Pure (transaction id is a parameter) so the
/// wire format is unit-testable.
pub fn build_txt_query(name: &str, txn_id: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + name.len());
    buf.extend_from_slice(&txn_id.to_be_bytes()); // ID
    buf.extend_from_slice(&0x0100u16.to_be_bytes()); // flags: RD set
    buf.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    buf.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT
    buf.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    buf.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
                                                // QNAME: length-prefixed labels.
    for label in name.split('.').filter(|l| !l.is_empty()) {
        buf.push(label.len() as u8);
        buf.extend_from_slice(label.as_bytes());
    }
    buf.push(0); // root label
    buf.extend_from_slice(&16u16.to_be_bytes()); // QTYPE = TXT
    buf.extend_from_slice(&1u16.to_be_bytes()); // QCLASS = IN
    buf
}

/// Parse a DNS response message, concatenating all TXT character-strings across
/// all answer records into one payload string. Best-effort: returns an empty
/// string if no TXT answers are present. Handles name compression in the answer
/// owner field by skipping it via the RDLENGTH (we never need the owner name).
pub fn parse_txt_response(msg: &[u8]) -> String {
    if msg.len() < 12 {
        return String::new();
    }
    let qdcount = u16::from_be_bytes([msg[4], msg[5]]) as usize;
    let ancount = u16::from_be_bytes([msg[6], msg[7]]) as usize;
    let mut pos = 12;

    // Skip the question section.
    for _ in 0..qdcount {
        match skip_name(msg, pos) {
            Some(p) => pos = p,
            None => return String::new(),
        }
        pos += 4; // QTYPE + QCLASS
        if pos > msg.len() {
            return String::new();
        }
    }

    let mut out = String::new();
    for _ in 0..ancount {
        let after_name = match skip_name(msg, pos) {
            Some(p) => p,
            None => break,
        };
        // TYPE(2) CLASS(2) TTL(4) RDLENGTH(2)
        if after_name + 10 > msg.len() {
            break;
        }
        let rtype = u16::from_be_bytes([msg[after_name], msg[after_name + 1]]);
        let rdlength = u16::from_be_bytes([msg[after_name + 8], msg[after_name + 9]]) as usize;
        let rdata_start = after_name + 10;
        let rdata_end = rdata_start + rdlength;
        if rdata_end > msg.len() {
            break;
        }
        if rtype == 16 {
            // TXT RDATA: one or more <length><bytes> character-strings.
            let mut i = rdata_start;
            while i < rdata_end {
                let slen = msg[i] as usize;
                i += 1;
                if i + slen > rdata_end {
                    break;
                }
                out.push_str(&String::from_utf8_lossy(&msg[i..i + slen]));
                i += slen;
            }
        }
        pos = rdata_end;
    }
    out
}

/// Advance past a DNS name starting at `pos`, returning the position just after
/// it. Handles compression pointers (top two bits set): the name ends at the
/// pointer (2 bytes) for the purpose of the *current* record's layout.
fn skip_name(msg: &[u8], mut pos: usize) -> Option<usize> {
    loop {
        if pos >= msg.len() {
            return None;
        }
        let len = msg[pos] as usize;
        if len == 0 {
            return Some(pos + 1);
        }
        if len & 0xc0 == 0xc0 {
            // Compression pointer: 2 bytes, name ends here in this record.
            return Some(pos + 2);
        }
        pos += 1 + len;
    }
}

/// Resolve the live peer-URL list for a Fly app via the `vms.{app}.internal`
/// TXT record. Best-effort: any failure (no resolver, timeout, empty answer)
/// returns `Ok(None)` so the caller stays local-only rather than crashing.
///
/// The socket I/O here is the only part NOT covered by unit tests (it needs a
/// live Fly 6PN resolver). The query builder and response parser it delegates
/// to ([`build_txt_query`] / [`parse_txt_response`] / [`parse_vms_txt`]) ARE
/// unit-tested with fixed buffers.
pub async fn discover_peers(cfg: &DiscoveryConfig) -> std::io::Result<Option<Vec<String>>> {
    use tokio::net::UdpSocket;
    use tokio::time::{timeout, Duration};

    let name = format!("vms.{}.internal", cfg.app_name);
    let txn_id: u16 = 0x4253; // "BS"
    let query = build_txt_query(&name, txn_id);

    let resolver: SocketAddr = FLY_RESOLVER
        .parse()
        .expect("FLY_RESOLVER is a valid socket addr");

    // Bind an ephemeral IPv6 socket (6PN is IPv6-only).
    let sock = UdpSocket::bind("[::]:0").await?;
    sock.connect(resolver).await?;
    sock.send(&query).await?;

    let mut buf = vec![0u8; 4096];
    let n = match timeout(Duration::from_secs(3), sock.recv(&mut buf)).await {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Ok(None), // timeout → stay local-only
    };
    buf.truncate(n);

    let payload = parse_txt_response(&buf);
    let machine_ids = parse_vms_txt(&payload);
    if machine_ids.is_empty() {
        return Ok(None);
    }
    let urls = peer_urls_from_machine_ids(
        &machine_ids,
        &cfg.app_name,
        cfg.rest_port,
        cfg.self_machine_id.as_deref(),
    );
    Ok(Some(urls))
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;
    use basin_engine::partition_router::PartitionRouter;

    // --- self-id derivation -------------------------------------------------

    #[test]
    fn explicit_replica_id_wins() {
        let got = derive_replica_id(
            Some("node-a"),
            Some("3d8d9eldng21"),
            Some("basin-prod"),
            5434,
        );
        assert_eq!(got.as_deref(), Some("node-a"));
    }

    #[test]
    fn derives_fly_url_when_no_explicit_id() {
        let got = derive_replica_id(None, Some("3d8d9eldng21"), Some("basin-prod"), 5434);
        assert_eq!(
            got.as_deref(),
            Some("http://3d8d9eldng21.vm.basin-prod.internal:5434")
        );
    }

    #[test]
    fn derives_with_custom_port() {
        let got = derive_replica_id(None, Some("m1"), Some("app"), 6000);
        assert_eq!(got.as_deref(), Some("http://m1.vm.app.internal:6000"));
    }

    #[test]
    fn no_derivation_without_fly_env() {
        assert_eq!(derive_replica_id(None, None, Some("app"), 5434), None);
        assert_eq!(derive_replica_id(None, Some("m1"), None, 5434), None);
        // Empty explicit falls through to derivation.
        let got = derive_replica_id(Some("   "), Some("m1"), Some("app"), 5434);
        assert_eq!(got.as_deref(), Some("http://m1.vm.app.internal:5434"));
    }

    #[test]
    fn rest_port_parsing() {
        assert_eq!(rest_port_from_bind(Some("127.0.0.1:5434")), 5434);
        assert_eq!(rest_port_from_bind(Some("[::]:6000")), 6000);
        assert_eq!(rest_port_from_bind(None), DEFAULT_REST_PORT);
        assert_eq!(rest_port_from_bind(Some("garbage")), DEFAULT_REST_PORT);
    }

    // --- discovery gate -----------------------------------------------------

    #[test]
    fn static_peers_disable_discovery() {
        // Even with the full Fly env, an explicit BASIN_SHARD_PEERS wins.
        assert!(!discovery_enabled(
            Some("http://a:5434,http://b:5434"),
            Some("app"),
            true,
            true
        ));
    }

    #[test]
    fn discovery_gate_requires_all_conditions() {
        assert!(discovery_enabled(None, Some("app"), true, true));
        assert!(discovery_enabled(Some("  "), Some("app"), true, true)); // empty static = unset
        assert!(!discovery_enabled(None, None, true, true)); // no Fly app
        assert!(!discovery_enabled(None, Some("app"), false, true)); // no secret
        assert!(!discovery_enabled(None, Some("app"), true, false)); // shard disabled
    }

    // --- peer-list build / sort determinism ---------------------------------

    #[test]
    fn peer_urls_sorted_and_order_independent() {
        let a =
            peer_urls_from_machine_ids(&["m3".into(), "m1".into(), "m2".into()], "app", 5434, None);
        let b =
            peer_urls_from_machine_ids(&["m2".into(), "m3".into(), "m1".into()], "app", 5434, None);
        assert_eq!(a, b, "sorted list must be order-independent");
        assert_eq!(
            a,
            vec![
                "http://m1.vm.app.internal:5434",
                "http://m2.vm.app.internal:5434",
                "http://m3.vm.app.internal:5434",
            ]
        );
    }

    #[test]
    fn self_machine_id_always_present_and_deduped() {
        // self already in the discovered set → no duplicate.
        let urls = peer_urls_from_machine_ids(&["m1".into(), "m2".into()], "app", 5434, Some("m1"));
        assert_eq!(urls.len(), 2);
        // self NOT in the discovered set → appended.
        let urls = peer_urls_from_machine_ids(&["m2".into()], "app", 5434, Some("m1"));
        assert_eq!(
            urls,
            vec![
                "http://m1.vm.app.internal:5434",
                "http://m2.vm.app.internal:5434",
            ]
        );
    }

    #[test]
    fn fnv1a_owner_mapping_identical_across_input_orders() {
        // Two routers built from the same machine set discovered in different
        // orders must resolve identical owners (the sort is what guarantees it).
        let set_a =
            peer_urls_from_machine_ids(&["mc".into(), "ma".into(), "mb".into()], "app", 5434, None);
        let set_b =
            peer_urls_from_machine_ids(&["mb".into(), "mc".into(), "ma".into()], "app", 5434, None);
        let ra = PartitionRouter::new(set_a, "http://ma.vm.app.internal:5434");
        let rb = PartitionRouter::new(set_b, "http://mb.vm.app.internal:5434");
        let p = ProjectId::from_ulid(ulid::Ulid(0xdead_beef));
        for part in ["_default", "s1", "s2", "s3", "s4"] {
            assert_eq!(
                ra.desired_owner(&p, part).base_url,
                rb.desired_owner(&p, part).base_url,
                "{part}: owner must be order-independent"
            );
        }
    }

    // --- vms TXT parsing ----------------------------------------------------

    #[test]
    fn parses_vms_txt_pairs() {
        // Fly's documented format: comma-separated `machine_id region` pairs.
        let ids = parse_vms_txt("3d8d9eldng21 fra,1781570c024d18 jnb,9080e1234abc fra");
        assert_eq!(ids, vec!["3d8d9eldng21", "1781570c024d18", "9080e1234abc"]);
    }

    #[test]
    fn parses_vms_txt_whitespace_and_dups() {
        // Tolerate extra whitespace and dedupe repeated ids.
        let ids = parse_vms_txt("  m1 fra , m2 jnb ,  m1 fra ");
        assert_eq!(ids, vec!["m1", "m2"]);
        assert!(parse_vms_txt("").is_empty());
    }

    // --- DNS wire format ----------------------------------------------------

    #[test]
    fn build_txt_query_wire_format() {
        let q = build_txt_query("vms.app.internal", 0x1234);
        // Header.
        assert_eq!(&q[0..2], &[0x12, 0x34]); // ID
        assert_eq!(&q[2..4], &[0x01, 0x00]); // flags RD
        assert_eq!(&q[4..6], &[0x00, 0x01]); // QDCOUNT=1
                                             // QNAME labels: 3"vms" 3"app" 8"internal" 0
        let qname = &q[12..];
        assert_eq!(qname[0], 3);
        assert_eq!(&qname[1..4], b"vms");
        assert_eq!(qname[4], 3);
        assert_eq!(&qname[5..8], b"app");
        assert_eq!(qname[8], 8);
        assert_eq!(&qname[9..17], b"internal");
        assert_eq!(qname[17], 0); // root
                                  // QTYPE=TXT(16), QCLASS=IN(1)
        assert_eq!(&qname[18..20], &[0x00, 0x10]);
        assert_eq!(&qname[20..22], &[0x00, 0x01]);
    }

    #[test]
    fn parse_txt_response_extracts_payload() {
        // Hand-build a minimal DNS response: 1 question + 1 TXT answer whose
        // single character-string is "m1 fra,m2 jnb". Owner name uses a
        // compression pointer to the question name (0xc0 0x0c).
        let payload = b"m1 fra,m2 jnb";
        let mut msg = Vec::new();
        // Header: ID, flags(QR), QD=1, AN=1, NS=0, AR=0.
        msg.extend_from_slice(&[0x42, 0x53]);
        msg.extend_from_slice(&[0x81, 0x80]);
        msg.extend_from_slice(&1u16.to_be_bytes());
        msg.extend_from_slice(&1u16.to_be_bytes());
        msg.extend_from_slice(&0u16.to_be_bytes());
        msg.extend_from_slice(&0u16.to_be_bytes());
        // Question: vms.app.internal TXT IN  (name starts at offset 12).
        for label in ["vms", "app", "internal"] {
            msg.push(label.len() as u8);
            msg.extend_from_slice(label.as_bytes());
        }
        msg.push(0);
        msg.extend_from_slice(&16u16.to_be_bytes()); // TXT
        msg.extend_from_slice(&1u16.to_be_bytes()); // IN
                                                    // Answer: name = pointer to 0x0c.
        msg.extend_from_slice(&[0xc0, 0x0c]);
        msg.extend_from_slice(&16u16.to_be_bytes()); // TYPE TXT
        msg.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
        msg.extend_from_slice(&60u32.to_be_bytes()); // TTL
        let rdlen = (1 + payload.len()) as u16; // 1 length byte + string
        msg.extend_from_slice(&rdlen.to_be_bytes());
        msg.push(payload.len() as u8);
        msg.extend_from_slice(payload);

        let got = parse_txt_response(&msg);
        assert_eq!(got, "m1 fra,m2 jnb");
        assert_eq!(parse_vms_txt(&got), vec!["m1", "m2"]);
    }

    #[test]
    fn parse_txt_response_empty_on_garbage() {
        assert_eq!(parse_txt_response(&[]), "");
        assert_eq!(parse_txt_response(&[0u8; 8]), "");
    }

    // --- membership change detection ----------------------------------------

    #[test]
    fn peer_set_change_detection() {
        let a = vec![
            "http://m1.vm.app.internal:5434".to_string(),
            "http://m2.vm.app.internal:5434".to_string(),
        ];
        let b = a.clone();
        assert!(!peer_set_changed(&a, &b));
        let c = vec![
            "http://m1.vm.app.internal:5434".to_string(),
            "http://m3.vm.app.internal:5434".to_string(),
        ];
        assert!(peer_set_changed(&a, &c));
        let (added, removed) = peer_set_diff(&a, &c);
        assert_eq!(added, vec!["http://m3.vm.app.internal:5434"]);
        assert_eq!(removed, vec!["http://m2.vm.app.internal:5434"]);
    }

    #[test]
    fn membership_change_moves_ownership() {
        // Scaling from 2 → 3 nodes must reassign at least some partition to the
        // new node (proves the router reflects the new set).
        let two = peer_urls_from_machine_ids(&["ma".into(), "mb".into()], "app", 5434, None);
        let three =
            peer_urls_from_machine_ids(&["ma".into(), "mb".into(), "mc".into()], "app", 5434, None);
        assert!(peer_set_changed(&two, &three));
        let r3 = PartitionRouter::new(three, "http://mc.vm.app.internal:5434");
        let mut mc_owns = 0;
        for i in 0..300u128 {
            let p = ProjectId::from_ulid(ulid::Ulid(i.wrapping_mul(0x9e3779b97f4a7c15)));
            if r3.desired_owner(&p, "s1").base_url == "http://mc.vm.app.internal:5434" {
                mc_owns += 1;
            }
        }
        assert!(mc_owns > 0, "new node mc must own some partitions");
    }
}
