//! Glue point for `basin-geo`.
//!
//! `basin-geo` is the PostGIS-compatible spatial layer (Point + Box2d,
//! `ST_Distance` / `ST_DWithin` / `ST_MakePoint` / `ST_X` / `ST_Y`,
//! `ST_Contains` for `BOX2D`/`POINT`). It depends on this crate transitively
//! for column-storage shape, but to keep the dependency graph one-directional
//! we cannot pull `basin-geo` into `basin-engine`. What we can do here is
//! reserve the symbol the engine calls during [`crate::Engine::new`] so a
//! future wiring has an obvious, named landing spot in the source tree.
//!
//! Today's implementation is a deliberate no-op: the v0.1 surface
//! (`ST_Distance`, `ST_DWithin`, `ST_MakePoint`, `ST_X`, `ST_Y`,
//! `ST_Contains`) is exercised by calling `basin_geo::*` directly from
//! application code and viability tests. The same staging pattern is used
//! by `basin-cron` for `cron.schedule(...)`, `basin-net` for
//! `http_get(...)` / `net.http_post(...)`, and `basin-cv` for
//! `CALL basin.refresh_continuous_aggregate(...)`.
//!
//! ## v0.2 plan
//!
//! When the SQL surface is wired up, this stub becomes the registration
//! site for both the column-type recognition and the scalar UDF set:
//!
//! 1. **Column type**. Extend the SQL-planner's type recogniser to map
//!    DDL `POINT` and `BOX2D` onto Arrow `FixedSizeBinary(N)` with
//!    field metadata `BASIN_TYPE=POSTGIS_POINT` / `..._BOX2D`. For
//!    POINT, `N = basin_geo::POINT_WKB_LEN` (21 bytes: 1-byte
//!    endianness + 4-byte type code + 8 bytes X + 8 bytes Y).
//! 2. **ScalarUDFs**. Register one DataFusion `ScalarUDF` per public
//!    function, each thin-wrapping the `basin_geo` Rust API:
//!
//!    | UDF              | Signature                             | Backed by                            |
//!    |------------------|---------------------------------------|--------------------------------------|
//!    | `ST_MakePoint`   | `(DOUBLE, DOUBLE) -> POINT (BYTEA21)` | `basin_geo::make_point` + `encode`   |
//!    | `ST_X`           | `(POINT) -> DOUBLE`                   | `basin_geo::decode_point` + `.x`     |
//!    | `ST_Y`           | `(POINT) -> DOUBLE`                   | `basin_geo::decode_point` + `.y`     |
//!    | `ST_Distance`    | `(POINT, POINT) -> DOUBLE`            | `basin_geo::haversine_meters`        |
//!    | `ST_DWithin`     | `(POINT, POINT, DOUBLE) -> BOOL`      | `basin_geo::dwithin`                 |
//!    | `ST_Contains`    | `(BOX2D, POINT) -> BOOL`              | `basin_geo::contains_box`            |
//!
//!    The registration runs once per `Engine::new` (today's call site)
//!    and per session-open (so RLS-rewritten plan trees keep the UDFs
//!    in their `SessionContext`). Until then, viability tests use the
//!    Rust API directly — same as `viability_basin_cron`.
//!
//! See `basin-geo`'s crate docs for the full surface and the v0.2 TODO
//! list (LINESTRING/POLYGON, R-tree spatial index, EWKB / non-WGS84
//! reprojection).

/// Hook called once from [`crate::Engine::new`]. No-op today; reserved
/// for the future SQL-surface wiring described in the module doc.
#[inline]
pub(crate) fn install() {
    // Intentionally empty. See the module-level doc for why.
}
