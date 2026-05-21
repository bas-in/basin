//! Range type operator UDFs for Basin (Phase 5.24.C).
//!
//! This module re-exports the range-operator UDFs already implemented in
//! `basin_engine::range_udf` and documents the full set of PG range operators
//! that Basin supports:
//!
//! | Operator | Name                        | UDF function            |
//! |----------|-----------------------------|-------------------------|
//! | `@>`     | contains element or range   | `range_contains_elem` / `range_contains_range` |
//! | `<@`     | contained by                | (as above, swapped)     |
//! | `&&`     | overlaps                    | `range_overlaps`        |
//! | `<<`     | strictly left of            | `range_strictly_left`   |
//! | `>>`     | strictly right of           | `range_strictly_right`  |
//! | `-\|-`   | adjacent                    | `range_adjacent`        |
//! | `+`      | union                       | `range_union`           |
//! | `*`      | intersection                | `range_intersection`    |
//! | `-`      | difference                  | `range_diff`            |
//! | `=`      | semantic equality           | `range_eq`              |
//!
//! All operators operate over the Basin range JSON storage format
//! (`{"l":<lo>,"u":<hi>,"li":<bool>,"ui":<bool>}`) and produce results
//! consistent with PostgreSQL's range semantics (half-open intervals,
//! inclusive/exclusive bounds, empty-range rules).
//!
//! ## Registration
//!
//! All UDFs are registered via [`crate::range_udf::register_range_udfs`]
//! which is called from `session.rs` during `SessionContext` construction.
//! This module provides the public re-export surface for documentation and
//! any external crate that needs to reference the UDF names.
//!
//! ## Operator rewriting
//!
//! The pre-parse SQL rewriter in [`crate::range_udf::rewrite_range_operators`]
//! converts PG infix operators to UDF calls before sqlparser sees the SQL.
//! Basin's sqlparser / DataFusion layer does not natively understand these
//! operators, so the rewrite is the correctness-critical step.

/// Names of all range operator UDFs registered by Basin.
pub const RANGE_OPERATOR_UDF_NAMES: &[&str] = &[
    "range_contains_elem",
    "range_contains_range",
    "range_overlaps",
    "range_strictly_left",
    "range_strictly_right",
    "range_adjacent",
    "range_not_extends_right",
    "range_not_extends_left",
    "range_union",
    "range_intersection",
    "range_diff",
    "range_eq",
    "range_merge",
    "multirange_contains_elem",
];
