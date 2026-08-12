---
title: "DF removal — scan and storage layer"
nav_section: migration
sidebar_position: 6
summary: "Resolves the Vortex blocker: basin-storage already reads Vortex directly, with no DataFusion involvement. The vortex-datafusion coupling is confined to one engine-side ListingTable wrapper that the migration deletes rather than replaces."
tags: [migration, query-engine, storage, vortex]
---

# 06 — Scan and storage layer

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. **Status: partial** — the Vortex blocker question is resolved
below; the Parquet and statistics sections are still outstanding.

## The blocker question

ADR 0030 recorded this as an open negative consequence:

> **Vortex coupling.** `vortex-datafusion` 0.71 implements DataFusion's
> `FileFormat` trait. Removing DataFusion means reading Vortex through the base
> `vortex` crate directly. This is being assessed as a potential blocker.

The framing came from [ADR 0015](../../decisions/0015-vortex-storage-format.md),
which describes the read path as going through `vortex-datafusion`'s
`VortexFormat`:

> Reads go through `vortex-datafusion` 0.70's `VortexFormat`, which implements
> the DataFusion 53 `FileFormat` trait — the same trait the Parquet path
> implements.

**That description is now out of date.** The code has moved on since ADR 0015
was written, and the migration is much better positioned than the ADR implies.

## Finding: there are two Vortex read paths, and only one uses DataFusion

### Path A — `basin-storage`, direct, no DataFusion

`crates/basin-storage/Cargo.toml` (lines 54–60) depends only on base Vortex
crates. **`vortex-datafusion` is not among them:**

```toml
vortex-array   = "0.71"
vortex-file    = { version = "0.71", features = ["zstd"] }
vortex-btrblocks = { version = "0.71", features = ["zstd", "pco"] }
vortex-session = "0.71"
vortex-io      = { version = "0.71", features = ["tokio", "object_store"] }
vortex-layout  = "0.71"
vortex-buffer  = "0.71"
```

`crates/basin-storage/src/vortex_format.rs` uses these directly and already
implements everything a scan needs, without a DataFusion trait in sight:

| Capability | Implementation | Location |
|---|---|---|
| Write / encode | `BtrBlocksCompressorBuilder`, `from_arrow` | `vortex_format.rs:19–20, 87–111` |
| Open file / footer | `vortex_file::VortexFile`, `open_buffer` | `vortex_format.rs:24, 315, 500` |
| Object-store reads | `vortex_io::object_store::ObjectStoreReadAt`, `VortexReadAt` | `vortex_format.rs:299–300` |
| File statistics | `stats_from_vortex_file`, `vortex_array::expr::stats::Stat` | `vortex_format.rs:323–330` |
| Projection + filter | `reader::vortex_project_and_filter`, `vortex_array::expr::Expression` | `vortex_format.rs:426, 439` |
| Footer caching | `crate::vortex_footer_cache::VortexFooterCache` | `vortex_format.rs:486` |

This is a complete, DataFusion-free Vortex reader — including predicate
filtering and its own footer cache — and it is already in production use.

### Path B — `basin-engine`, via DataFusion's ListingTable

`crates/basin-engine/Cargo.toml:46` declares `vortex-datafusion = "0.71"`,
consumed at:

- `crates/basin-engine/src/session.rs:3110` — constructs
  `vortex_datafusion::VortexFormat::new_with_options(...)`
- `crates/basin-engine/src/vortex_listing_format.rs` — a 1,100+ line Basin-local
  wrapper whose own header says it exists to "patch `total_byte_size`" on the
  inner `VortexFormat`

Path B exists only to present Vortex files to DataFusion's `ListingTable`
machinery. It is an adapter to DataFusion, not a Vortex capability.

## Consequence for the migration

**The Vortex read path is not a blocker, and it does not need to be rebuilt.**

Removing DataFusion means:

1. **Delete** `crates/basin-engine/src/vortex_listing_format.rs` outright. It is
   pure DataFusion-adapter code — a wrapper around a `FileFormat` impl,
   patching a field DataFusion's planner reads. With no DataFusion planner,
   nothing consumes it.
2. **Delete** the `vortex-datafusion` dependency from
   `crates/basin-engine/Cargo.toml:46`, and the `session.rs:3110` construction
   site.
3. **Route all Vortex scans through Path A**, the `basin-storage` reader that
   already works. The owned physical scan operator calls
   `vortex_project_and_filter` directly.

This is a **deletion, not a reimplementation** — one of the few places in this
migration where removing DataFusion strictly reduces the code Basin maintains
with no replacement cost. It also removes a leg of the arrow 58 / DataFusion 53
/ vortex 0.71 version lockstep that both ADR 0015 and the root `Cargo.toml`
flag as an ongoing upgrade tax: after this, Vortex needs to track arrow, not
arrow *and* DataFusion.

### One caveat, stated honestly

Path A's `vortex_project_and_filter` is proven for the predicate shapes
`basin-storage` currently drives through it. Whether it covers the full filter
surface Path B's DataFusion pushdown handles has **not** been verified here —
that is a coverage question for the physical-scan work, and it may require
extending Path A's expression translation. ADR 0015 also notes that Vortex
pushdown is type-gated (Vortex panics uncatchably on a mixed-DType compare), so
that guard must be preserved in whichever path survives. The claim proven above
is narrower and sufficient: **no Vortex capability is lost by removing
DataFusion**, because the direct reader already exists.

## Still outstanding in this document

- Parquet: confirm the read path drives `parquet` (arrow-rs 58) directly and
  enumerate what `datafusion-datasource-parquet` (7,140 non-test LOC) adds that
  must be rebuilt — row-group pruning, predicate-to-row-filter conversion,
  projection masks, async buffered reads.
- `TableProvider` inventory (~209 references) and the custom scan bypasses at
  `session.rs:4886` and `:5244`.
- Statistics and pruning: what Basin already owns (catalog stats, bloom filters
  in `fast_select.rs`) versus what comes from `datafusion-pruning` (2,115 LOC).
