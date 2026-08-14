//! Function hosting — the registry `basin-exec` did not have.
//!
//! Designed in `docs/migration/df-removal/27-function-hosting-abi.md`; this is
//! the executable half. The trait a scalar function implements, and the
//! registry mapping a `pg_proc` oid to one.
//!
//! # Why this exists
//!
//! DataFusion hosts functions in `SessionContext`'s UDF registry. Basin's
//! counterpart was a hard-coded `match` on oid inside `eval_scalar_fn` — 130
//! constants, no `&self`, no registry parameter, nothing to register into.
//! Function hosting is roughly half the DataFusion removal surface, and none
//! of it could be parallelised while the only way to add a function was to
//! edit the one `match` every agent would have to edit simultaneously.
//!
//! # Keyed on OID, not on name
//!
//! [`basin_pgtype::func::resolve`] maps `(name, arg_types) -> FuncSig`; this
//! maps `FuncSig.oid -> impl`. The seam is deliberate: overload resolution is
//! **not correct yet** — `to_char(unknown, unknown)` still diverges because
//! Basin tabulates two of PostgreSQL's eight rows, and a computed column
//! still types as `PgType::UNKNOWN`. Keying on oid means a resolution bug
//! surfaces as *the wrong function being called*, not as a broken
//! implementation, so ports do not chase a moving target.
//!
//! # Scalar only, on purpose
//!
//! Doc 27 also specifies `AggregateFunc`/`Accumulator`. They are NOT here.
//! Nothing implements them yet, `CellValue` is private to
//! [`crate::aggregate`], and the same document argues — from
//! `basin-pgcatalog`'s 13,231 tested-but-unreachable lines — that inventing a
//! trait with no implementor is a defect rather than preparation. They land
//! with the first aggregate slice that needs them.
//!
//! # Coexistence
//!
//! `eval_scalar_fn` consults this registry FIRST and falls back to its
//! existing `match`. Moving a function is three edits — write the impl,
//! register it, delete its `match` arm — needing no coordination with whoever
//! is moving a different oid. Every intermediate state runs.

use std::collections::HashMap;
use std::sync::OnceLock;

use arrow_array::ArrayRef;
use basin_pgtype::func::FuncSig;
use basin_pgtype::{Oid, PgType};

use crate::eval::EvalSession;
use crate::operator::ExecError;

pub mod dt_fns;
pub mod num_fns;
pub mod str_fns;

/// The `pg_proc` row for `oid`, or `None`.
///
/// A linear scan of [`basin_pgtype::func::FUNCS`]. Called at registration
/// time and from the default [`ScalarFunc::return_type`], neither of which is
/// a hot path; a map is not worth the static-initialisation cost until it is.
pub fn catalog_row(oid: Oid) -> Option<&'static FuncSig> {
    basin_pgtype::func::FUNCS.iter().find(|f| f.oid == oid)
}

/// A scalar function: once per row, one value out.
///
/// Object-safe — the registry stores `Box<dyn ScalarFunc>`.
pub trait ScalarFunc: Send + Sync {
    /// The `pg_proc.oid` this implements. Must match the key it registers
    /// under; [`FuncRegistry::register_scalar`] enforces that.
    fn oid(&self) -> Oid;

    /// Declared argument types in, result type out. Called at PLAN time,
    /// before any data exists.
    ///
    /// The default answers from the catalog, which is right for nearly every
    /// function. Override only where one `pg_proc.prorettype` cannot express
    /// the answer — `extract(... FROM interval)` (oid 6204) is the live
    /// example: it returns `numeric` at a scale that depends on the unit
    /// (measured on 18.2: `second`/`epoch` 6, `milliseconds` 3, everything
    /// else 0), which a single return oid cannot carry.
    fn return_type(&self, _args: &[PgType]) -> Result<PgType, ExecError> {
        catalog_row(self.oid())
            .map(|sig| PgType::new(sig.ret))
            .ok_or_else(|| {
                ExecError::Internal(format!(
                    "function oid {} is registered but has no pg_proc row in \
                     basin_pgtype::func::FUNCS — catalog and registry disagree",
                    self.oid().get()
                ))
            })
    }

    /// Arguments arrive EVALUATED and aligned to the batch length.
    ///
    /// Deliberately narrower than the `&[Expr]` the old free function takes.
    /// No PostgreSQL *function* is lazy — the lazy constructs (`CASE`,
    /// `COALESCE`, `AND`/`OR` short-circuit) are `Expr` variants, not
    /// `pg_proc` rows — so that generality bought nothing while permitting a
    /// real bug: an implementation holding unevaluated arguments can evaluate
    /// one twice, and `now()` changing between two evaluations inside one
    /// statement is issue #151.
    fn invoke(&self, args: &[ArrayRef], session: &EvalSession)
        -> Result<ArrayRef, ExecError>;
}

/// Oid -> implementation.
///
/// Deliberately dull: no priority, no shadowing, no versioning. A duplicate
/// oid panics at construction rather than silently last-wins, because two
/// implementations of one oid is a build bug and resolving it quietly is how
/// a wrong answer becomes impossible to locate.
#[derive(Default)]
pub struct FuncRegistry {
    scalar: HashMap<Oid, Box<dyn ScalarFunc>>,
}

impl std::fmt::Debug for FuncRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuncRegistry")
            .field("scalar", &self.scalar.len())
            .finish()
    }
}

impl FuncRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// # Panics
    /// If `f.oid()` is already registered, or has no row in `FUNCS`. Both are
    /// build-time bugs in the registration list, and both become silent wrong
    /// answers if tolerated — an implementation the planner cannot type is
    /// unreachable code that still looks registered.
    pub fn register_scalar(&mut self, f: Box<dyn ScalarFunc>) {
        let oid = f.oid();
        assert!(
            catalog_row(oid).is_some(),
            "scalar oid {} has no pg_proc row in FUNCS — add the catalog row \
             before the implementation, or nothing can resolve a call to it",
            oid.get()
        );
        assert!(
            self.scalar.insert(oid, f).is_none(),
            "scalar oid {} registered twice",
            oid.get()
        );
    }

    pub fn scalar(&self, oid: Oid) -> Option<&dyn ScalarFunc> {
        self.scalar.get(&oid).map(|b| b.as_ref())
    }

    /// How many functions are hosted — the Phase 1 progress metric.
    ///
    /// Read off the registry itself rather than a maintained constant. A
    /// hardcoded census is exactly what the orphan battery was caught doing
    /// (`d6023b63`), and it reported a number that had stopped being true.
    pub fn len(&self) -> usize {
        self.scalar.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scalar.is_empty()
    }
}

/// The process-wide built-in registry.
///
/// A `OnceLock` rather than a field on every session because
/// `EvalSession::DEFAULT` is a `const` and cannot construct an owning
/// pointer. A session carrying no registry of its own resolves here, so all
/// 303 existing `eval`/`eval_with` call sites keep working untouched — which
/// is the property that lets Phase 1 land one function at a time.
pub fn builtins() -> &'static FuncRegistry {
    static BUILTINS: OnceLock<FuncRegistry> = OnceLock::new();
    BUILTINS.get_or_init(|| {
        #[allow(unused_mut)]
        let mut r = FuncRegistry::new();
        // Phase 1 slices append their registrations here. Append-only, so two
        // agents porting different oid ranges conflict only on adjacent lines.
        r.register_scalar(Box::new(str_fns::Lower));
        // num_fns — ported by the wave-15 numeric slice.
        r.register_scalar(Box::new(num_fns::AbsInt2));
        r.register_scalar(Box::new(num_fns::AbsInt4));
        r.register_scalar(Box::new(num_fns::AbsInt8));
        r.register_scalar(Box::new(num_fns::AbsFloat4));
        r.register_scalar(Box::new(num_fns::AbsFloat8));
        r.register_scalar(Box::new(num_fns::AbsNumeric));
        r.register_scalar(Box::new(num_fns::RoundFloat8));
        r.register_scalar(Box::new(num_fns::CeilFloat8));
        r.register_scalar(Box::new(num_fns::CeilingFloat8));
        r.register_scalar(Box::new(num_fns::FloorFloat8));
        r.register_scalar(Box::new(num_fns::TruncFloat8));
        r.register_scalar(Box::new(num_fns::SignFloat8));
        // dt_fns — ported by the wave-15 date/time slice.
        r.register_scalar(Box::new(dt_fns::Age));
        r.register_scalar(Box::new(dt_fns::DateTruncTimestamp));
        r.register_scalar(Box::new(dt_fns::DateTruncTimestamptz));
        r.register_scalar(Box::new(dt_fns::DateTruncInterval));
        r.register_scalar(Box::new(dt_fns::DatePartTimestamp));
        r.register_scalar(Box::new(dt_fns::DatePartTimestamptz));
        r.register_scalar(Box::new(dt_fns::DatePartDate));
        r.register_scalar(Box::new(dt_fns::DatePartInterval));
        r.register_scalar(Box::new(dt_fns::ToCharTimestamp));
        r.register_scalar(Box::new(dt_fns::ToCharTimestamptz));
        r.register_scalar(Box::new(dt_fns::MakeDate));
        r
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lookup for an oid nobody has ported must MISS, so the caller falls
    /// through to `eval_scalar_fn`'s `match`. This is the property that makes
    /// the migration incremental — an unported function must behave exactly
    /// as it did before this module existed.
    ///
    /// Oid 1571 is one still in the `match`. When a slice ports it, pick another unported oid rather
    /// than deleting this test: the fallthrough is what keeps every
    /// intermediate commit runnable.
    #[test]
    fn an_unported_oid_misses_and_falls_through_to_the_match() {
        assert!(
            builtins().scalar(Oid(1571)).is_none(),
            "an unported oid must fall through to the match"
        );
    }

    /// The hosted count comes off the registry itself. It is the Phase 1
    /// progress metric, so it must not be a maintained constant — that is
    /// exactly the hardcoded census the orphan battery was caught keeping.
    #[test]
    fn the_registry_reports_what_is_actually_hosted() {
        assert_eq!(
            builtins().len(),
            24,
            "24 hosted: lower, the 12 numeric and the 11 date/time ports. Read \
             from the registry, never tracked by hand"
        );
    }

    /// `catalog_row` must agree with the catalog rather than with a
    /// hand-written list.
    #[test]
    fn catalog_row_finds_a_real_row_and_rejects_an_invented_one() {
        // 2100 is avg(int8) — verified against pg_proc on 18.2 in 50e0ac69.
        let avg = catalog_row(Oid(2100)).expect("avg(int8) is tabulated");
        assert_eq!(avg.name, "avg");
        assert!(catalog_row(Oid(4_294_967_290)).is_none());
    }

    /// Registration refuses an oid the catalog does not know.
    #[test]
    #[should_panic(expected = "has no pg_proc row in FUNCS")]
    fn registering_an_oid_absent_from_the_catalog_panics() {
        struct Bogus;
        impl ScalarFunc for Bogus {
            fn oid(&self) -> Oid {
                Oid(4_294_967_290)
            }
            fn invoke(&self, _: &[ArrayRef], _: &EvalSession)
                -> Result<ArrayRef, ExecError> {
                unreachable!("panics before it can be called")
            }
        }
        FuncRegistry::new().register_scalar(Box::new(Bogus));
    }

    /// A duplicate registration is a panic, not a silent overwrite.
    #[test]
    #[should_panic(expected = "registered twice")]
    fn registering_the_same_oid_twice_panics() {
        struct Lower;
        impl ScalarFunc for Lower {
            fn oid(&self) -> Oid {
                Oid(870)
            }
            fn invoke(&self, _: &[ArrayRef], _: &EvalSession)
                -> Result<ArrayRef, ExecError> {
                unreachable!()
            }
        }
        let mut r = FuncRegistry::new();
        r.register_scalar(Box::new(Lower));
        r.register_scalar(Box::new(Lower));
    }
}
