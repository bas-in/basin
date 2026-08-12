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
}
