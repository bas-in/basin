//! Postgres type OIDs.
//!
//! Every value in this module is a **fixed OID from a real PostgreSQL
//! installation**, not an allocation of Basin's choosing. Clients depend on
//! these: a driver decoding a `RowDescription` switches on the type OID, and
//! `23` has to mean `int4` or the client mis-parses the bytes. They are stable
//! across every Postgres release and are effectively part of the wire protocol.
//!
//! Source of truth is `pg_type.dat` in the PostgreSQL source tree. The subset
//! here is the set Basin can currently represent; adding a type means adding
//! its real OID, never inventing one.
//!
//! OIDs for *user-defined* objects (tables, columns, user types) are a
//! different problem with different constraints — see
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md`. This module is
//! builtins only.

/// A Postgres object identifier.
///
/// Postgres's `Oid` is an unsigned 32-bit integer, and the wire protocol
/// transmits it as such. Newtyped so a type OID cannot be silently confused
/// with a relation OID, an attribute number, or a plain integer column value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Oid(pub u32);

impl Oid {
    /// The invalid OID. Postgres spells this `InvalidOid` and uses it as the
    /// "no such object" sentinel; it is never a valid type.
    pub const INVALID: Oid = Oid(0);

    /// Raw value, for wire encoding.
    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for Oid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ─── Scalar builtins ────────────────────────────────────────────────────────

pub const BOOL: Oid = Oid(16);
pub const BYTEA: Oid = Oid(17);
/// `"char"` — the internal single-byte type, NOT `char(n)`. `char(n)` is
/// [`BPCHAR`]. The quoting in Postgres's own docs is load-bearing and this is a
/// classic source of confusion.
pub const CHAR: Oid = Oid(18);
/// `name` — 63-byte identifier type used throughout `pg_catalog`.
pub const NAME: Oid = Oid(19);
pub const INT8: Oid = Oid(20);
pub const INT2: Oid = Oid(21);
pub const INT4: Oid = Oid(23);
pub const TEXT: Oid = Oid(25);
pub const OID: Oid = Oid(26);
pub const JSON: Oid = Oid(114);
pub const XML: Oid = Oid(142);
pub const FLOAT4: Oid = Oid(700);
pub const FLOAT8: Oid = Oid(701);
/// `char(n)`, blank-padded. Postgres's internal name is `bpchar`.
pub const BPCHAR: Oid = Oid(1042);
pub const VARCHAR: Oid = Oid(1043);
pub const DATE: Oid = Oid(1082);
pub const TIME: Oid = Oid(1083);
pub const TIMESTAMP: Oid = Oid(1114);
pub const TIMESTAMPTZ: Oid = Oid(1184);
pub const INTERVAL: Oid = Oid(1186);
pub const TIMETZ: Oid = Oid(1266);
pub const NUMERIC: Oid = Oid(1700);
pub const UUID: Oid = Oid(2950);
pub const JSONB: Oid = Oid(3802);

/// The pseudo-type Postgres assigns to a literal whose type is not yet known.
///
/// This one matters more than its obscurity suggests. In `SELECT 'x' = col`,
/// the literal starts as `unknown` and is resolved *from the column*, not the
/// other way round. An Arrow-first type system has no way to spell this, which
/// is why Basin's logical types cannot be Arrow's. See
/// `docs/migration/df-removal/08-ir-design.md` §2.
pub const UNKNOWN: Oid = Oid(705);

/// `void` — the return type of a function that returns nothing.
pub const VOID: Oid = Oid(2278);

/// `record` — an anonymous composite, e.g. the result of `ROW(...)`.
pub const RECORD: Oid = Oid(2249);

// ─── Catalog-internal builtins ──────────────────────────────────────────────
//
// These never appear in a client's result set, but they are the declared
// types of several `pg_catalog` columns Basin must be able to describe when
// it reports on its own catalog (`pg_proc.proargtypes`, `pg_index.indkey`,
// `pg_cast.castcontext`, `pg_constraint.contype`, `pg_proc.provolatile`, ...).

/// `oidvector` — a fixed-format array of `Oid`, used for e.g.
/// `pg_proc.proargtypes`. Distinct from a real `oid[]` (which is
/// [`OID_ARRAY`]): `oidvector` has no explicit dimensionality/lower-bound
/// info and cannot contain NULLs.
pub const OIDVECTOR: Oid = Oid(30);

/// `int2vector` — same shape as `oidvector` but of `int2`, used for e.g.
/// `pg_index.indkey` and `pg_index.indoption`.
pub const INT2VECTOR: Oid = Oid(22);

/// `pg_node_tree` — a `text`-encoded serialized expression tree, used for
/// columns like `pg_attrdef.adbin` and `pg_constraint.conbin`. Has no array
/// type (`typarray` is 0 in `pg_type`).
pub const PG_NODE_TREE: Oid = Oid(194);

/// `anyarray` — the pseudo-type standing for "some array type", used in the
/// signature of polymorphic functions (`array_append`, etc). Never the
/// runtime type of an actual value or column, and has no array-of-itself
/// type (`typarray` is 0).
pub const ANYARRAY: Oid = Oid(2277);

/// `"char"` — the internal single-byte enum-like type. Already represented
/// as [`CHAR`] (OID 18); see its doc comment. Re-noted here because it is
/// also the declared type of `pg_cast.castcontext`, `pg_constraint.contype`,
/// and `pg_proc.provolatile` — easy to miss since those columns read like
/// they should be `text` or an actual enum.

// ─── Range builtins ─────────────────────────────────────────────────────────
//
// Range types are the one builtin family whose OID's `typtype` is `'r'`, not
// `'b'`; each has a matching array type just like a scalar does.

pub const INT4RANGE: Oid = Oid(3904);
pub const NUMRANGE: Oid = Oid(3906);
pub const TSRANGE: Oid = Oid(3908);
pub const TSTZRANGE: Oid = Oid(3910);
pub const DATERANGE: Oid = Oid(3912);
pub const INT8RANGE: Oid = Oid(3926);

// ─── Array builtins ─────────────────────────────────────────────────────────
//
// Postgres gives every scalar type a matching array type with its own OID, and
// the two are NOT related by any arithmetic — `_int4` is 1007 while `int4` is
// 23. They must be tabulated. `array_of` below is the mapping.

pub const BOOL_ARRAY: Oid = Oid(1000);
pub const BYTEA_ARRAY: Oid = Oid(1001);
pub const CHAR_ARRAY: Oid = Oid(1002);
pub const NAME_ARRAY: Oid = Oid(1003);
pub const INT2_ARRAY: Oid = Oid(1005);
pub const INT4_ARRAY: Oid = Oid(1007);
pub const TEXT_ARRAY: Oid = Oid(1009);
pub const BPCHAR_ARRAY: Oid = Oid(1014);
pub const VARCHAR_ARRAY: Oid = Oid(1015);
pub const INT8_ARRAY: Oid = Oid(1016);
pub const FLOAT4_ARRAY: Oid = Oid(1021);
pub const FLOAT8_ARRAY: Oid = Oid(1022);
pub const OID_ARRAY: Oid = Oid(1028);
pub const DATE_ARRAY: Oid = Oid(1182);
pub const TIME_ARRAY: Oid = Oid(1183);
pub const TIMESTAMP_ARRAY: Oid = Oid(1115);
pub const TIMESTAMPTZ_ARRAY: Oid = Oid(1185);
pub const INTERVAL_ARRAY: Oid = Oid(1187);
pub const NUMERIC_ARRAY: Oid = Oid(1231);
pub const JSON_ARRAY: Oid = Oid(199);
pub const JSONB_ARRAY: Oid = Oid(3807);
pub const UUID_ARRAY: Oid = Oid(2951);

/// `_oidvector`, the (rarely-seen) array-of-`oidvector` type.
pub const OIDVECTOR_ARRAY: Oid = Oid(1013);
/// `_int2vector`, the (rarely-seen) array-of-`int2vector` type.
pub const INT2VECTOR_ARRAY: Oid = Oid(1006);

pub const INT4RANGE_ARRAY: Oid = Oid(3905);
pub const NUMRANGE_ARRAY: Oid = Oid(3907);
pub const TSRANGE_ARRAY: Oid = Oid(3909);
pub const TSTZRANGE_ARRAY: Oid = Oid(3911);
pub const DATERANGE_ARRAY: Oid = Oid(3913);
pub const INT8RANGE_ARRAY: Oid = Oid(3927);

/// The array type whose element type is `elem`, if `elem` is a builtin scalar
/// with a builtin array type.
///
/// Returns `None` for types Basin has no array OID for — including arrays
/// themselves, since Postgres does not nest array *types* (a 2-D `int[]` is
/// still `_int4`; dimensionality lives in the value, not the type).
pub fn array_of(elem: Oid) -> Option<Oid> {
    Some(match elem {
        BOOL => BOOL_ARRAY,
        BYTEA => BYTEA_ARRAY,
        CHAR => CHAR_ARRAY,
        NAME => NAME_ARRAY,
        INT2 => INT2_ARRAY,
        INT4 => INT4_ARRAY,
        INT8 => INT8_ARRAY,
        TEXT => TEXT_ARRAY,
        BPCHAR => BPCHAR_ARRAY,
        VARCHAR => VARCHAR_ARRAY,
        FLOAT4 => FLOAT4_ARRAY,
        FLOAT8 => FLOAT8_ARRAY,
        OID => OID_ARRAY,
        DATE => DATE_ARRAY,
        TIME => TIME_ARRAY,
        TIMESTAMP => TIMESTAMP_ARRAY,
        TIMESTAMPTZ => TIMESTAMPTZ_ARRAY,
        INTERVAL => INTERVAL_ARRAY,
        NUMERIC => NUMERIC_ARRAY,
        JSON => JSON_ARRAY,
        JSONB => JSONB_ARRAY,
        UUID => UUID_ARRAY,
        OIDVECTOR => OIDVECTOR_ARRAY,
        INT2VECTOR => INT2VECTOR_ARRAY,
        INT4RANGE => INT4RANGE_ARRAY,
        NUMRANGE => NUMRANGE_ARRAY,
        TSRANGE => TSRANGE_ARRAY,
        TSTZRANGE => TSTZRANGE_ARRAY,
        DATERANGE => DATERANGE_ARRAY,
        INT8RANGE => INT8RANGE_ARRAY,
        _ => return None,
    })
}

/// The element type of an array type, i.e. the inverse of [`array_of`].
pub fn element_of(array: Oid) -> Option<Oid> {
    Some(match array {
        BOOL_ARRAY => BOOL,
        BYTEA_ARRAY => BYTEA,
        CHAR_ARRAY => CHAR,
        NAME_ARRAY => NAME,
        INT2_ARRAY => INT2,
        INT4_ARRAY => INT4,
        INT8_ARRAY => INT8,
        TEXT_ARRAY => TEXT,
        BPCHAR_ARRAY => BPCHAR,
        VARCHAR_ARRAY => VARCHAR,
        FLOAT4_ARRAY => FLOAT4,
        FLOAT8_ARRAY => FLOAT8,
        OID_ARRAY => OID,
        DATE_ARRAY => DATE,
        TIME_ARRAY => TIME,
        TIMESTAMP_ARRAY => TIMESTAMP,
        TIMESTAMPTZ_ARRAY => TIMESTAMPTZ,
        INTERVAL_ARRAY => INTERVAL,
        NUMERIC_ARRAY => NUMERIC,
        JSON_ARRAY => JSON,
        JSONB_ARRAY => JSONB,
        UUID_ARRAY => UUID,
        OIDVECTOR_ARRAY => OIDVECTOR,
        INT2VECTOR_ARRAY => INT2VECTOR,
        INT4RANGE_ARRAY => INT4RANGE,
        NUMRANGE_ARRAY => NUMRANGE,
        TSRANGE_ARRAY => TSRANGE,
        TSTZRANGE_ARRAY => TSTZRANGE,
        DATERANGE_ARRAY => DATERANGE,
        INT8RANGE_ARRAY => INT8RANGE,
        _ => return None,
    })
}

/// Whether `oid` names a builtin array type.
#[inline]
pub fn is_array(oid: Oid) -> bool {
    element_of(oid).is_some()
}

/// The canonical Postgres name for a builtin OID, as `pg_type.typname` spells
/// it. Note these are the *internal* names — `int4`, not `integer`, and
/// `bpchar`, not `character`. `pg_catalog` reports these, and `format_type()`
/// is what turns them into SQL-standard spellings.
pub fn type_name(oid: Oid) -> Option<&'static str> {
    Some(match oid {
        BOOL => "bool",
        BYTEA => "bytea",
        CHAR => "char",
        NAME => "name",
        INT2 => "int2",
        INT4 => "int4",
        INT8 => "int8",
        TEXT => "text",
        OID => "oid",
        JSON => "json",
        JSONB => "jsonb",
        XML => "xml",
        FLOAT4 => "float4",
        FLOAT8 => "float8",
        BPCHAR => "bpchar",
        VARCHAR => "varchar",
        DATE => "date",
        TIME => "time",
        TIMETZ => "timetz",
        TIMESTAMP => "timestamp",
        TIMESTAMPTZ => "timestamptz",
        INTERVAL => "interval",
        NUMERIC => "numeric",
        UUID => "uuid",
        UNKNOWN => "unknown",
        VOID => "void",
        RECORD => "record",
        OIDVECTOR => "oidvector",
        INT2VECTOR => "int2vector",
        PG_NODE_TREE => "pg_node_tree",
        ANYARRAY => "anyarray",
        INT4RANGE => "int4range",
        NUMRANGE => "numrange",
        TSRANGE => "tsrange",
        TSTZRANGE => "tstzrange",
        DATERANGE => "daterange",
        INT8RANGE => "int8range",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These are wire-protocol constants. A typo here mis-decodes every value
    /// of that type in every client, so they are pinned explicitly rather than
    /// trusted to review.
    #[test]
    fn well_known_oids_match_postgres() {
        assert_eq!(BOOL.get(), 16);
        assert_eq!(INT8.get(), 20);
        assert_eq!(INT2.get(), 21);
        assert_eq!(INT4.get(), 23);
        assert_eq!(TEXT.get(), 25);
        assert_eq!(FLOAT4.get(), 700);
        assert_eq!(FLOAT8.get(), 701);
        assert_eq!(UNKNOWN.get(), 705);
        assert_eq!(BPCHAR.get(), 1042);
        assert_eq!(VARCHAR.get(), 1043);
        assert_eq!(TIMESTAMP.get(), 1114);
        assert_eq!(TIMESTAMPTZ.get(), 1184);
        assert_eq!(NUMERIC.get(), 1700);
        assert_eq!(UUID.get(), 2950);
        assert_eq!(JSONB.get(), 3802);
    }

    #[test]
    fn array_oids_round_trip() {
        for scalar in [
            BOOL,
            BYTEA,
            INT2,
            INT4,
            INT8,
            TEXT,
            VARCHAR,
            BPCHAR,
            FLOAT4,
            FLOAT8,
            DATE,
            TIMESTAMP,
            TIMESTAMPTZ,
            NUMERIC,
            JSONB,
            UUID,
        ] {
            let arr = array_of(scalar).expect("builtin scalar has an array type");
            assert_eq!(element_of(arr), Some(scalar), "round trip for {scalar}");
            assert!(is_array(arr));
            assert!(!is_array(scalar));
        }
    }

    /// Array OIDs are tabulated, not computed — this guards against anyone
    /// "simplifying" `array_of` into arithmetic.
    #[test]
    fn array_oids_are_not_derivable_from_element_oids() {
        assert_eq!(array_of(INT4), Some(Oid(1007)));
        assert_eq!(array_of(INT8), Some(Oid(1016)));
        assert_eq!(array_of(TEXT), Some(Oid(1009)));
        // If these were `elem + k` for any fixed k, these deltas would agree.
        assert_ne!(1007 - 23, 1016 - 20);
    }

    #[test]
    fn arrays_do_not_nest() {
        assert_eq!(array_of(INT4_ARRAY), None);
    }

    #[test]
    fn char_is_not_bpchar() {
        // `"char"` (18) is a single byte; `char(n)` is bpchar (1042).
        assert_ne!(CHAR, BPCHAR);
        assert_eq!(type_name(CHAR), Some("char"));
        assert_eq!(type_name(BPCHAR), Some("bpchar"));
    }

    /// Catalog-internal types added after an audit against a live
    /// PostgreSQL 18.2 (`SELECT oid, typname, typarray FROM pg_type WHERE
    /// oid < 10000`). `oidvector`/`int2vector` back `pg_proc.proargtypes`
    /// and `pg_index.indkey`/`indoption`; `pg_node_tree` backs
    /// `pg_attrdef.adbin`; `anyarray` is the polymorphic pseudo-type.
    #[test]
    fn catalog_internal_oids_match_postgres() {
        assert_eq!(OIDVECTOR.get(), 30);
        assert_eq!(INT2VECTOR.get(), 22);
        assert_eq!(PG_NODE_TREE.get(), 194);
        assert_eq!(ANYARRAY.get(), 2277);
        assert_eq!(type_name(OIDVECTOR), Some("oidvector"));
        assert_eq!(type_name(INT2VECTOR), Some("int2vector"));
        assert_eq!(type_name(PG_NODE_TREE), Some("pg_node_tree"));
        assert_eq!(type_name(ANYARRAY), Some("anyarray"));
    }

    /// `oidvector`/`int2vector` each have a real (if rarely used) array
    /// type in `pg_type` — `_oidvector` / `_int2vector` — confirmed against
    /// the live server's `typarray` column.
    #[test]
    fn vector_array_oids_round_trip() {
        assert_eq!(array_of(OIDVECTOR), Some(OIDVECTOR_ARRAY));
        assert_eq!(element_of(OIDVECTOR_ARRAY), Some(OIDVECTOR));
        assert_eq!(OIDVECTOR_ARRAY.get(), 1013);

        assert_eq!(array_of(INT2VECTOR), Some(INT2VECTOR_ARRAY));
        assert_eq!(element_of(INT2VECTOR_ARRAY), Some(INT2VECTOR));
        assert_eq!(INT2VECTOR_ARRAY.get(), 1006);
    }

    /// Range type OIDs, confirmed against a live PostgreSQL 18.2:
    /// `SELECT oid, typname, typarray FROM pg_type WHERE typtype = 'r' AND
    /// oid < 10000`. `int8range` (3926) is a late addition (Postgres 9.4+)
    /// and is numerically distant from its siblings — easy to fat-finger.
    #[test]
    fn range_oids_match_postgres() {
        assert_eq!(INT4RANGE.get(), 3904);
        assert_eq!(NUMRANGE.get(), 3906);
        assert_eq!(TSRANGE.get(), 3908);
        assert_eq!(TSTZRANGE.get(), 3910);
        assert_eq!(DATERANGE.get(), 3912);
        assert_eq!(INT8RANGE.get(), 3926);

        for (range, expected_array, name) in [
            (INT4RANGE, INT4RANGE_ARRAY, "int4range"),
            (NUMRANGE, NUMRANGE_ARRAY, "numrange"),
            (TSRANGE, TSRANGE_ARRAY, "tsrange"),
            (TSTZRANGE, TSTZRANGE_ARRAY, "tstzrange"),
            (DATERANGE, DATERANGE_ARRAY, "daterange"),
            (INT8RANGE, INT8RANGE_ARRAY, "int8range"),
        ] {
            assert_eq!(array_of(range), Some(expected_array), "array_of({name})");
            assert_eq!(
                element_of(expected_array),
                Some(range),
                "element_of(_{name})"
            );
            assert_eq!(type_name(range), Some(name));
        }
    }

    #[test]
    fn range_array_oids_match_postgres() {
        assert_eq!(INT4RANGE_ARRAY.get(), 3905);
        assert_eq!(NUMRANGE_ARRAY.get(), 3907);
        assert_eq!(TSRANGE_ARRAY.get(), 3909);
        assert_eq!(TSTZRANGE_ARRAY.get(), 3911);
        assert_eq!(DATERANGE_ARRAY.get(), 3913);
        assert_eq!(INT8RANGE_ARRAY.get(), 3927);
    }
}
