//! Basin's Postgres type system.
//!
//! # Why this crate exists
//!
//! Basin's logical types have historically *been* Arrow types, with Postgres
//! semantics bolted on where they diverged. That is backwards, and it is the
//! root of a family of fidelity bugs: Arrow has no `unknown` type, so
//! Postgres's late resolution of untyped literals cannot be expressed; Arrow
//! has no type modifiers, so `varchar(10)` cannot be enforced or reported; and
//! Arrow's `Utf8` cannot distinguish `text` from `varchar` from `name`, which
//! `pg_catalog` must.
//!
//! This crate inverts the relationship:
//!
//! > **Postgres is the logical type system. Arrow is the physical
//! > representation.**
//!
//! A [`PgType`] is what the planner, the catalog, the wire protocol and every
//! UDF signature agree on. [`physical`] maps it down to Arrow when it is time
//! to touch data. The mapping is deliberately many-to-one — `text`,
//! `varchar(n)` and `name` all become `Utf8` — and that is exactly the
//! information Arrow-first typing was throwing away.
//!
//! See `docs/decisions/0030-own-query-engine-remove-datafusion.md` and
//! `docs/migration/df-removal/08-ir-design.md`.
//!
//! # Status
//!
//! Nothing consumes this crate yet. It is the first increment of the owned
//! engine, deliberately built as a leaf so it can be written and tested
//! without touching the running query path.

pub mod cast;
pub mod category;
pub mod func;
pub mod oid;
pub mod operator;
pub mod typmod;

pub use oid::Oid;

use arrow_schema::{DataType, TimeUnit};

/// The logical type of a value, as Postgres understands it.
///
/// Cheap to copy: an OID and an `i32`. It is passed through the planner by
/// value, so it must stay that way — resist the urge to hang a `String` or an
/// `Arc` off it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgType {
    /// The type's OID. For builtins these are Postgres's own fixed values; see
    /// [`oid`].
    pub oid: Oid,
    /// The type modifier, or [`typmod::UNSPECIFIED`] (`-1`). Its meaning is
    /// per-type — decode it with the matching function in [`typmod`], never
    /// generically.
    pub typmod: i32,
}

impl PgType {
    /// A type with no modifier.
    #[inline]
    pub const fn new(oid: Oid) -> Self {
        Self {
            oid,
            typmod: typmod::UNSPECIFIED,
        }
    }

    /// A type with an explicit modifier. The caller is responsible for
    /// encoding `typmod` with the right function from [`typmod`].
    #[inline]
    pub const fn with_typmod(oid: Oid, typmod: i32) -> Self {
        Self { oid, typmod }
    }

    /// Whether this is the `unknown` pseudo-type — an untyped literal awaiting
    /// resolution from its context.
    #[inline]
    pub fn is_unknown(&self) -> bool {
        self.oid == oid::UNKNOWN
    }

    /// Whether this is a builtin array type.
    #[inline]
    pub fn is_array(&self) -> bool {
        oid::is_array(self.oid)
    }

    /// The declared length of a `varchar(n)` / `char(n)`, if one was given.
    pub fn varlena_len(&self) -> Option<i32> {
        match self.oid {
            oid::VARCHAR | oid::BPCHAR => typmod::decode_varlena_len(self.typmod),
            _ => None,
        }
    }

    /// The declared `(precision, scale)` of a `numeric(p,s)`, if one was given.
    pub fn numeric_precision_scale(&self) -> Option<(i32, i32)> {
        match self.oid {
            oid::NUMERIC => typmod::decode_numeric(self.typmod),
            _ => None,
        }
    }
}

// Convenience constructors for the types that appear everywhere.
impl PgType {
    pub const BOOL: PgType = PgType::new(oid::BOOL);
    pub const INT2: PgType = PgType::new(oid::INT2);
    pub const INT4: PgType = PgType::new(oid::INT4);
    pub const INT8: PgType = PgType::new(oid::INT8);
    pub const FLOAT4: PgType = PgType::new(oid::FLOAT4);
    pub const FLOAT8: PgType = PgType::new(oid::FLOAT8);
    pub const TEXT: PgType = PgType::new(oid::TEXT);
    pub const BYTEA: PgType = PgType::new(oid::BYTEA);
    pub const DATE: PgType = PgType::new(oid::DATE);
    pub const TIMESTAMP: PgType = PgType::new(oid::TIMESTAMP);
    pub const TIMESTAMPTZ: PgType = PgType::new(oid::TIMESTAMPTZ);
    pub const UUID: PgType = PgType::new(oid::UUID);
    pub const JSONB: PgType = PgType::new(oid::JSONB);
    pub const UNKNOWN: PgType = PgType::new(oid::UNKNOWN);

    /// `varchar(n)`.
    pub fn varchar(n: i32) -> Self {
        Self::with_typmod(oid::VARCHAR, typmod::encode_varlena_len(n))
    }

    /// `char(n)`, blank-padded.
    pub fn bpchar(n: i32) -> Self {
        Self::with_typmod(oid::BPCHAR, typmod::encode_varlena_len(n))
    }

    /// `numeric(p, s)`.
    pub fn numeric(precision: i32, scale: i32) -> Self {
        Self::with_typmod(oid::NUMERIC, typmod::encode_numeric(precision, scale))
    }
}

/// Why a [`PgType`] has no Arrow representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalError {
    /// The type is a pseudo-type that never reaches storage — `unknown`,
    /// `void`, `record`. Reaching here means a plan escaped type resolution
    /// with an unresolved literal, which is a planner bug rather than a user
    /// error.
    Pseudo(Oid),
    /// `numeric(p,s)` whose precision exceeds what `Decimal128` can hold.
    ///
    /// This is a genuine representational gap, not an oversight: Postgres's
    /// `NUMERIC` is arbitrary-precision up to 1000 digits and Basin's physical
    /// representation is 128-bit. Callers must surface this rather than
    /// silently narrowing, because narrowing loses value.
    NumericPrecisionTooLarge { precision: i32 },
    /// Basin has no physical mapping for this type yet.
    Unsupported(Oid),
}

impl std::fmt::Display for PhysicalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PhysicalError::Pseudo(o) => {
                write!(f, "pseudo-type {o} has no physical representation")
            }
            PhysicalError::NumericPrecisionTooLarge { precision } => write!(
                f,
                "numeric precision {precision} exceeds the 38 digits Decimal128 can represent"
            ),
            PhysicalError::Unsupported(o) => write!(f, "no physical mapping for type {o}"),
        }
    }
}

impl std::error::Error for PhysicalError {}

/// The largest `NUMERIC` precision representable as `Decimal128`.
pub const DECIMAL128_MAX_PRECISION: i32 = 38;

/// Map a logical [`PgType`] to the Arrow type that physically represents it.
///
/// Many-to-one by design. `text`, `varchar(n)` and `name` all land on
/// [`DataType::Utf8`]; the distinction between them lives in the [`PgType`] and
/// is what the catalog and wire protocol report.
///
/// Note this is the **engine's** physical type, which is not always the
/// **storage** encoding. ADR 0024 round-trips `uuid` through `Decimal256(39,0)`
/// at the `basin-storage` ↔ Vortex boundary as a workaround for a missing
/// Vortex encoder; that translation is invisible above storage, and this
/// function deliberately does not know about it.
pub fn physical(ty: PgType) -> Result<DataType, PhysicalError> {
    // Arrays first: an array's physical type is a list of its element's.
    if let Some(elem) = oid::element_of(ty.oid) {
        let inner = physical(PgType::new(elem))?;
        // Postgres arrays are always nullable in their elements — `{1,NULL,3}`
        // is an ordinary value, not an error.
        return Ok(DataType::List(std::sync::Arc::new(
            arrow_schema::Field::new("item", inner, true),
        )));
    }

    Ok(match ty.oid {
        oid::BOOL => DataType::Boolean,
        oid::INT2 => DataType::Int16,
        oid::INT4 => DataType::Int32,
        oid::INT8 => DataType::Int64,
        // `oid` is unsigned 32-bit on the wire. Represented as UInt32 so a high
        // OID does not read back negative.
        oid::OID => DataType::UInt32,
        oid::FLOAT4 => DataType::Float32,
        oid::FLOAT8 => DataType::Float64,

        // Every string-ish type shares one physical form. The logical
        // distinction is preserved in the PgType, which is the whole point.
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR | oid::XML => DataType::Utf8,

        oid::BYTEA => DataType::Binary,

        // JSON is text; JSONB is Basin's binary encoding (ADR 0027), which is
        // opaque bytes at this layer.
        oid::JSON => DataType::Utf8,
        oid::JSONB => DataType::Binary,

        oid::UUID => DataType::FixedSizeBinary(16),

        oid::DATE => DataType::Date32,
        // Postgres stores microseconds. Anything coarser silently truncates a
        // value the user wrote, so the physical unit is fixed at Microsecond
        // regardless of the declared typmod — the typmod constrains what may be
        // *stored*, not how it is laid out.
        oid::TIME => DataType::Time64(TimeUnit::Microsecond),
        oid::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        // A timestamptz is an absolute instant. Postgres stores it in UTC and
        // renders it in the session TimeZone; the zone is display, not data.
        oid::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        oid::INTERVAL => DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),

        oid::NUMERIC => match typmod::decode_numeric(ty.typmod) {
            Some((p, s)) if p <= DECIMAL128_MAX_PRECISION => DataType::Decimal128(p as u8, s as i8),
            Some((p, _)) => return Err(PhysicalError::NumericPrecisionTooLarge { precision: p }),
            // An undecorated NUMERIC is unconstrained in Postgres. Basin picks
            // the widest Decimal128 it can, which is a real and documented
            // narrowing: a value needing more than 38 digits cannot be stored.
            // See docs/migration/df-removal/12-pg-type-fidelity.md.
            None => DataType::Decimal128(DECIMAL128_MAX_PRECISION as u8, 0),
        },

        oid::UNKNOWN | oid::VOID | oid::RECORD => return Err(PhysicalError::Pseudo(ty.oid)),

        other => return Err(PhysicalError::Unsupported(other)),
    })
}

/// Map an Arrow physical [`DataType`] back to the [`PgType`] that produces
/// it — the reverse of [`physical`].
///
/// # This is necessarily lossy — read before calling
///
/// [`physical`] is many-to-one by design (its own doc comment says so):
/// `text`, `varchar(n)`, `bpchar(n)`, `name`, `"char"` and `xml` all collapse
/// to [`DataType::Utf8`], and both `json` and `varlena`-style opaque bytes
/// collapse to [`DataType::Binary`] alongside `jsonb`. Going backwards from a
/// bare `DataType` cannot recover which of those the value actually was —
/// that information lived in the [`PgType`] that produced it and is gone by
/// the time only a `DataType` remains.
///
/// This function therefore returns one **canonical** choice per physical
/// shape — `Utf8` always maps back to [`PgType::TEXT`], `Binary` always maps
/// back to [`PgType::BYTEA`] — not "the" original type, because there isn't
/// one to recover. A caller that has catalog knowledge of what a column was
/// actually declared as (from a `Field`'s metadata, a planner-tracked
/// `PgType`, or `pg_attribute`) **must** prefer that over this function; this
/// exists for the case — chiefly, the wire protocol needing *some*
/// `RowDescription` type oid for a column whose only remaining information is
/// its Arrow type — where no better source is available.
///
/// `typmod` is always [`typmod::UNSPECIFIED`] on the result: a bare
/// `DataType` carries no length/precision-and-scale beyond what `numeric`'s
/// own `Decimal128(p, s)` already encodes (which this function does thread
/// through — see below), so there is nothing to decode a `varchar(n)` or
/// `bpchar(n)` typmod from.
///
/// Arrays recurse through their element type and re-wrap with
/// [`oid::array_of`]; an element type this function cannot map, or one with
/// no builtin array OID, makes the whole array `None` rather than guessing.
/// Anything [`physical`] never produces (`FixedSizeList`, `Struct`,
/// `Decimal256`, ...) is also `None`.
pub fn logical_type(dt: &DataType) -> Option<PgType> {
    Some(match dt {
        DataType::Boolean => PgType::BOOL,
        DataType::Int16 => PgType::INT2,
        DataType::Int32 => PgType::INT4,
        DataType::Int64 => PgType::INT8,
        // The inverse of `physical`'s `oid::OID => DataType::UInt32` arm.
        DataType::UInt32 => PgType::new(oid::OID),
        DataType::Float32 => PgType::FLOAT4,
        DataType::Float64 => PgType::FLOAT8,

        // Canonical choice among text/varchar/bpchar/name/char/xml — see the
        // doc comment above.
        DataType::Utf8 => PgType::TEXT,
        // Canonical choice among bytea/jsonb — see the doc comment above.
        // (`json`, unlike `jsonb`, is `DataType::Utf8` in `physical` and so
        // is already covered by the `Utf8` arm, not this one.)
        DataType::Binary => PgType::BYTEA,

        DataType::FixedSizeBinary(16) => PgType::UUID,

        DataType::Date32 => PgType::DATE,
        DataType::Time64(TimeUnit::Microsecond) => PgType::new(oid::TIME),
        DataType::Timestamp(TimeUnit::Microsecond, None) => PgType::TIMESTAMP,
        // Any timezone-bearing timestamp maps back to timestamptz — the zone
        // string itself is display, not data, exactly as `physical`'s own doc
        // comment notes for the forward direction.
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => PgType::TIMESTAMPTZ,
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => PgType::new(oid::INTERVAL),

        // Decimal128 threads its precision/scale through as a real typmod —
        // unlike every other arm, this one is not lossy, because `physical`
        // itself is the one thing here with no plainer alternative to fall
        // back to.
        DataType::Decimal128(p, s) => PgType::numeric(*p as i32, *s as i32),

        DataType::List(field) => {
            let elem = logical_type(field.data_type())?;
            PgType::new(oid::array_of(elem.oid)?)
        }

        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_types_share_one_physical_form_but_stay_distinct_logically() {
        for t in [
            PgType::TEXT,
            PgType::varchar(10),
            PgType::bpchar(4),
            PgType::new(oid::NAME),
        ] {
            assert_eq!(physical(t).unwrap(), DataType::Utf8);
        }
        // ...and the logical types are NOT equal, which is the information an
        // Arrow-first type system was discarding.
        assert_ne!(PgType::TEXT, PgType::varchar(10));
        assert_ne!(PgType::varchar(10), PgType::varchar(20));
    }

    #[test]
    fn varchar_length_survives_the_round_trip() {
        assert_eq!(PgType::varchar(10).varlena_len(), Some(10));
        assert_eq!(PgType::bpchar(4).varlena_len(), Some(4));
        assert_eq!(PgType::TEXT.varlena_len(), None);
    }

    #[test]
    fn numeric_maps_to_decimal128_within_range() {
        assert_eq!(
            physical(PgType::numeric(10, 2)).unwrap(),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            physical(PgType::numeric(38, 10)).unwrap(),
            DataType::Decimal128(38, 10)
        );
    }

    /// The gap is real and must be loud. Postgres accepts numeric(50,0);
    /// Decimal128 cannot hold it, and silently narrowing would lose value.
    #[test]
    fn numeric_beyond_decimal128_is_an_error_not_a_narrowing() {
        assert_eq!(
            physical(PgType::numeric(50, 0)),
            Err(PhysicalError::NumericPrecisionTooLarge { precision: 50 })
        );
    }

    #[test]
    fn pseudo_types_have_no_physical_form() {
        assert_eq!(
            physical(PgType::UNKNOWN),
            Err(PhysicalError::Pseudo(oid::UNKNOWN))
        );
    }

    #[test]
    fn timestamps_are_microseconds_and_tz_aware_ones_are_utc() {
        assert_eq!(
            physical(PgType::TIMESTAMP).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            physical(PgType::TIMESTAMPTZ).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    /// A declared timestamp(3) still lays out as microseconds — the modifier
    /// constrains what may be stored, not the physical width.
    #[test]
    fn time_typmod_does_not_change_physical_width() {
        let t = PgType::with_typmod(oid::TIMESTAMP, typmod::encode_time_precision(3));
        assert_eq!(physical(t).unwrap(), physical(PgType::TIMESTAMP).unwrap());
    }

    #[test]
    fn arrays_map_to_lists_with_nullable_elements() {
        let DataType::List(field) = physical(PgType::new(oid::INT4_ARRAY)).unwrap() else {
            panic!("int4[] should map to a List");
        };
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(
            field.is_nullable(),
            "'{{1,NULL,3}}' is an ordinary Postgres array value"
        );
    }

    #[test]
    fn uuid_is_sixteen_bytes_at_the_engine_layer() {
        // ADR 0024's Decimal256 dance is a storage-boundary concern and must
        // not leak up here.
        assert_eq!(
            physical(PgType::UUID).unwrap(),
            DataType::FixedSizeBinary(16)
        );
    }

    #[test]
    fn pgtype_stays_cheap_to_copy() {
        // The planner passes these by value. If someone adds a String or an
        // Arc, this catches it.
        assert_eq!(std::mem::size_of::<PgType>(), 8);
    }

    /// The straightforward scalar cases round-trip through `physical` and
    /// back to the type that produced them — no collapsing happens for types
    /// `physical` maps one-to-one.
    #[test]
    fn scalar_types_round_trip_through_physical_and_logical_type() {
        for t in [
            PgType::BOOL,
            PgType::INT2,
            PgType::INT4,
            PgType::INT8,
            PgType::FLOAT4,
            PgType::FLOAT8,
            PgType::DATE,
            PgType::TIMESTAMP,
            PgType::UUID,
        ] {
            let dt = physical(t).unwrap();
            assert_eq!(
                logical_type(&dt),
                Some(t),
                "{t:?} -> {dt:?} did not round trip"
            );
        }
    }

    /// `oid` is `UInt32` at the physical layer specifically so a high OID
    /// does not read back negative — `logical_type` must reverse that arm
    /// too, not just the signed integer widths.
    #[test]
    fn oid_round_trips_through_uint32() {
        let ty = PgType::new(oid::OID);
        assert_eq!(physical(ty).unwrap(), DataType::UInt32);
        assert_eq!(logical_type(&DataType::UInt32), Some(ty));
    }

    /// `Utf8` is genuinely ambiguous — `text`, `varchar(n)`, `bpchar(n)` and
    /// `name` all produce it (see `string_types_share_one_physical_form...`
    /// above). `logical_type` must resolve this to the documented canonical
    /// choice (`text`) rather than, say, whichever of those happened to be
    /// tried last, and must NOT claim to recover `varchar`/`name` — a caller
    /// that assumed it could would silently mis-describe a `name` column
    /// (e.g. in `pg_catalog`) as plain `text`.
    #[test]
    fn utf8_maps_back_to_the_canonical_text_not_varchar_or_name() {
        assert_eq!(logical_type(&DataType::Utf8), Some(PgType::TEXT));
        // Losing the varchar length is the documented, accepted lossiness —
        // pin it explicitly so a future "fix" isn't attempted by surprise.
        assert_ne!(logical_type(&DataType::Utf8), Some(PgType::varchar(10)));
    }

    /// `Binary` is the same ambiguity as `Utf8`, between `bytea` and `jsonb`
    /// — the canonical choice is `bytea`, and a caller must not assume
    /// `logical_type` can recover that a `Binary` column was actually
    /// `jsonb`.
    #[test]
    fn binary_maps_back_to_the_canonical_bytea_not_jsonb() {
        assert_eq!(physical(PgType::JSONB).unwrap(), DataType::Binary);
        assert_eq!(logical_type(&DataType::Binary), Some(PgType::BYTEA));
        assert_ne!(logical_type(&DataType::Binary), Some(PgType::JSONB));
    }

    /// `timestamptz`'s physical zone is always rendered as `"UTC"` by
    /// `physical` (the zone is display, not data — see its doc comment), but
    /// `logical_type` must recognize ANY zone-bearing `Timestamp` as
    /// `timestamptz`, not only the exact `"UTC"` string, since the zone is
    /// not what makes it a timestamptz.
    #[test]
    fn any_timezone_bearing_timestamp_is_timestamptz() {
        assert_eq!(
            logical_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("+05:00".into())
            )),
            Some(PgType::TIMESTAMPTZ)
        );
        assert_eq!(
            logical_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Some(PgType::TIMESTAMP)
        );
    }

    /// `numeric(p, s)` is the one case that is NOT lossy — `Decimal128`
    /// carries its own precision and scale, so the round trip through
    /// `logical_type` must recover the exact `numeric(p, s)`, typmod
    /// included, not just "some numeric".
    #[test]
    fn decimal128_round_trips_precision_and_scale_exactly() {
        let ty = PgType::numeric(12, 3);
        let dt = physical(ty).unwrap();
        assert_eq!(dt, DataType::Decimal128(12, 3));
        assert_eq!(logical_type(&dt), Some(ty));
    }

    /// Arrays recurse through the element type and re-wrap with
    /// `oid::array_of`, so `int4[]` round-trips to `int4[]`, not to a bare
    /// `int4` or to `None`.
    #[test]
    fn arrays_round_trip_through_their_element_type() {
        let ty = PgType::new(oid::INT4_ARRAY);
        let dt = physical(ty).unwrap();
        assert_eq!(logical_type(&dt), Some(ty));
    }

    /// A physical shape `physical` never produces (`Decimal256`, `Struct`,
    /// ...) has no logical type to report — `None`, not a guess.
    #[test]
    fn unrepresentable_physical_types_return_none() {
        assert_eq!(logical_type(&DataType::Decimal256(20, 2)), None);
        assert_eq!(
            logical_type(&DataType::FixedSizeBinary(4)),
            None,
            "only 16-byte FixedSizeBinary (uuid) is a known logical type"
        );
    }
}
