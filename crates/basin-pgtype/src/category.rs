//! `pg_type.typcategory` and `pg_type.typispreferred` — the two columns
//! Postgres's overload resolution consults once counting exact matches has
//! failed to pick a winner.
//!
//! # Why this exists
//!
//! [`crate::func::resolve`] and [`crate::operator::resolve`] transcribe
//! `func_select_candidate` (`src/backend/parser/parse_func.c`). Three of its
//! four stages ask questions this crate could not answer before this module:
//!
//!   * *is the declared type the preferred type of the input's own category?*
//!     (stage 2, `IsPreferredType`)
//!   * *what category do the candidates' declared types share at this
//!     `unknown` argument position, and does any of them take that category's
//!     preferred type?* (stage 3, `get_type_category_preferred`)
//!
//! Both are pure `pg_type` lookups in Postgres, so they are pure lookups here.
//!
//! # Where these values come from
//!
//! A live PostgreSQL 18.2, not recollection:
//!
//! ```sql
//! SELECT oid, typcategory, typispreferred FROM pg_type WHERE oid = ANY(...);
//! ```
//!
//! `tests/overload_resolution.rs` re-runs exactly that query against whatever
//! `PG_DIFF_TEST_DSN` names and fails on any disagreement, so drift is caught
//! rather than assumed away.
//!
//! # The one fact worth stating out loud
//!
//! **A category can have more than one preferred type.** `typispreferred` is
//! true for *both* `float8` and `oid` inside category `N`, verified live. That
//! is not a quirk to normalize away: it is why `to_hex('42')` is ambiguous in
//! Postgres rather than resolving — stage 3 keeps every candidate taking *a*
//! preferred type, and `to_hex` has an `int4` and an `int8` row, neither of
//! them preferred, so both survive and the call fails. Code that assumed one
//! preferred type per category would resolve that call and be wrong.

use crate::oid;
use crate::Oid;

/// `pg_type.typcategory`, as the enum Postgres spells `TYPCATEGORY_*` in
/// `src/include/catalog/pg_type.h`.
///
/// Only the categories the types this crate tabulates actually use have a
/// variant; [`TypeCategory::Unrecognized`] carries the raw character for
/// anything else, so an untabulated type can never accidentally compare equal
/// to a real category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeCategory {
    /// `A` — array types.
    Array,
    /// `B` — `bool`.
    Boolean,
    /// `D` — `date`, `time`, `timestamp`, `timestamptz`, `timetz`.
    DateTime,
    /// `N` — the numeric types, plus the `reg*` OID-alias types.
    Numeric,
    /// `P` — pseudo-types (`any`, `anyarray`, `record`, `void`).
    Pseudo,
    /// `R` — range types.
    Range,
    /// `S` — `text`, `varchar`, `bpchar`, `name`. The category untyped
    /// literals are biased towards in stage 3 — see [`crate::func::resolve`].
    String,
    /// `T` — `interval`.
    Timespan,
    /// `U` — user-defined/"other" builtins: `bytea`, `json`, `jsonb`, `uuid`,
    /// `xml`.
    UserDefined,
    /// `X` — `unknown`. A real category with exactly one member, and never
    /// consulted, because every stage that looks at a category skips the
    /// `unknown` inputs first.
    Unknown,
    /// `Z` — "internal use", Postgres's `TYPCATEGORY_INTERNAL`: `"char"` and
    /// `pg_node_tree`.
    Internal,
    /// A type this module does not tabulate. Never equal to another
    /// [`TypeCategory::Unrecognized`] unless the raw characters match, and
    /// never preferred — an untabulated type therefore loses every
    /// category-based stage rather than silently joining a category it might
    /// not belong to. `tests/overload_resolution.rs` asserts no type named by
    /// [`crate::func::FUNCS`] or [`crate::operator::OPERATORS`] reaches this.
    Unrecognized,
}

/// `pg_type.typcategory` and `pg_type.typispreferred` for `ty`, together —
/// Postgres's `get_type_category_preferred`, which likewise returns both from
/// one syscache lookup because every caller wants both.
///
/// Returns [`TypeCategory::Unrecognized`] and `false` for a type this module
/// does not tabulate.
pub fn category_and_preferred(ty: Oid) -> (TypeCategory, bool) {
    use TypeCategory::*;
    match ty {
        // ─── B: boolean ─────────────────────────────────────────────────────
        oid::BOOL => (Boolean, true),

        // ─── S: string. `text` is preferred; this is the category stage 3
        //     breaks ties towards, because an untyped literal looks like one.
        oid::TEXT => (String, true),
        oid::NAME | oid::BPCHAR | oid::VARCHAR => (String, false),

        // ─── N: numeric. Two preferred types, not one — see the module docs.
        oid::FLOAT8 => (Numeric, true),
        oid::OID => (Numeric, true),
        oid::INT2 | oid::INT4 | oid::INT8 | oid::FLOAT4 | oid::NUMERIC => (Numeric, false),

        // ─── D: date/time. `timestamptz` is preferred, which is why
        //     `generate_series('a','b',interval)` resolves to the timestamptz
        //     row and not the timestamp one.
        oid::TIMESTAMPTZ => (DateTime, true),
        oid::DATE | oid::TIME | oid::TIMESTAMP | oid::TIMETZ => (DateTime, false),

        // ─── T: timespan. One member, and it is preferred.
        oid::INTERVAL => (Timespan, true),

        // ─── U: "user-defined", i.e. everything builtin that fits nowhere
        //     else. No preferred member.
        oid::BYTEA | oid::JSON | oid::XML | oid::UUID | oid::JSONB => (UserDefined, false),

        // ─── Z: internal use.
        oid::CHAR | oid::PG_NODE_TREE => (Internal, false),

        // ─── X: unknown.
        oid::UNKNOWN => (Unknown, false),

        // ─── R: ranges.
        oid::INT4RANGE
        | oid::NUMRANGE
        | oid::TSRANGE
        | oid::TSTZRANGE
        | oid::DATERANGE
        | oid::INT8RANGE => (Range, false),

        // ─── P: pseudo-types.
        oid::RECORD | oid::ANY | oid::ANYARRAY | oid::VOID => (Pseudo, false),

        // ─── A: arrays. Every array type, including the vector types
        //     (`int2vector`, `oidvector`), which Postgres does file under `A`.
        oid::BOOL_ARRAY
        | oid::BYTEA_ARRAY
        | oid::CHAR_ARRAY
        | oid::NAME_ARRAY
        | oid::INT2_ARRAY
        | oid::INT2VECTOR
        | oid::INT2VECTOR_ARRAY
        | oid::INT4_ARRAY
        | oid::TEXT_ARRAY
        | oid::OIDVECTOR
        | oid::OIDVECTOR_ARRAY
        | oid::BPCHAR_ARRAY
        | oid::VARCHAR_ARRAY
        | oid::INT8_ARRAY
        | oid::FLOAT4_ARRAY
        | oid::FLOAT8_ARRAY
        | oid::OID_ARRAY
        | oid::JSON_ARRAY
        | oid::JSONB_ARRAY
        | oid::UUID_ARRAY
        | oid::DATE_ARRAY
        | oid::TIME_ARRAY
        | oid::TIMESTAMP_ARRAY
        | oid::TIMESTAMPTZ_ARRAY
        | oid::INTERVAL_ARRAY
        | oid::NUMERIC_ARRAY
        | oid::INT4RANGE_ARRAY
        | oid::NUMRANGE_ARRAY
        | oid::TSRANGE_ARRAY
        | oid::TSTZRANGE_ARRAY
        | oid::DATERANGE_ARRAY
        | oid::INT8RANGE_ARRAY => (Array, false),

        _ => (Unrecognized, false),
    }
}

/// `pg_type.typcategory` alone.
pub fn category(ty: Oid) -> TypeCategory {
    category_and_preferred(ty).0
}

/// Postgres's `IsPreferredType(category, type)`: whether `ty` is a preferred
/// type **of that specific category**.
///
/// The category argument is not decoration. Restricting the check to one
/// category is a deliberate 7.4-era restriction (`parse_func.c`: "preferred
/// type must be of same category as input type; give no preference to
/// cross-category conversions to preferred types") — without it, an `int4`
/// input would count a `text` parameter as a preferred match and string
/// overloads would beat numeric ones.
pub fn is_preferred_type(cat: TypeCategory, ty: Oid) -> bool {
    let (ty_cat, preferred) = category_and_preferred(ty);
    ty_cat == cat && preferred
}

/// The raw `pg_type.typcategory` character, for the differential test to
/// compare against the live server without hard-coding the mapping twice.
pub fn category_char(cat: TypeCategory) -> Option<char> {
    Some(match cat {
        TypeCategory::Array => 'A',
        TypeCategory::Boolean => 'B',
        TypeCategory::DateTime => 'D',
        TypeCategory::Numeric => 'N',
        TypeCategory::Pseudo => 'P',
        TypeCategory::Range => 'R',
        TypeCategory::String => 'S',
        TypeCategory::Timespan => 'T',
        TypeCategory::UserDefined => 'U',
        TypeCategory::Unknown => 'X',
        TypeCategory::Internal => 'Z',
        TypeCategory::Unrecognized => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fact the module docs single out: `N` has two preferred types, so
    /// "the preferred type of a category" is not a function.
    #[test]
    fn the_numeric_category_has_two_preferred_types() {
        assert!(is_preferred_type(TypeCategory::Numeric, oid::FLOAT8));
        assert!(is_preferred_type(TypeCategory::Numeric, oid::OID));
        assert!(!is_preferred_type(TypeCategory::Numeric, oid::NUMERIC));
    }

    /// The cross-category restriction: `text` is preferred, but not *for* a
    /// numeric input.
    #[test]
    fn a_preferred_type_of_another_category_is_not_preferred_here() {
        assert!(is_preferred_type(TypeCategory::String, oid::TEXT));
        assert!(!is_preferred_type(TypeCategory::Numeric, oid::TEXT));
        assert!(!is_preferred_type(TypeCategory::String, oid::FLOAT8));
    }

    #[test]
    fn an_untabulated_type_is_unrecognized_and_never_preferred() {
        // 1560 is `bit`, which `crate::oid` deliberately does not carry.
        let (cat, preferred) = category_and_preferred(Oid(1560));
        assert_eq!(cat, TypeCategory::Unrecognized);
        assert!(!preferred);
        assert!(!is_preferred_type(TypeCategory::Unrecognized, Oid(1560)));
        assert_eq!(category_char(TypeCategory::Unrecognized), None);
    }
}
