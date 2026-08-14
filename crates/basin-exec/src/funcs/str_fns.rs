//! String functions hosted through [`crate::funcs::ScalarFunc`].
//!
//! # This file is the Phase 1 template
//!
//! `lower` is here as the WORKED EXAMPLE doc 27 said it owed. It is the
//! smallest function that exercises every part of the seam, and it was moved
//! first precisely because it already had tests: three cases in `eval.rs`
//! call it through `eval`, so they now run through the registry unchanged.
//! Existing tests passing over a new path is stronger evidence than any new
//! test written alongside the port.
//!
//! ## Porting a function is three edits
//!
//! 1. Write the impl here (or in the slice module for its family).
//! 2. Register it in [`crate::funcs::builtins`].
//! 3. **Delete its arm from `eval_scalar_fn`'s `match`.**
//!
//! Step 3 is not optional and not cosmetic. The registry is consulted first,
//! so a left-behind arm is unreachable code that still looks live — and this
//! repository has already shipped three features that were committed, tested
//! and completely inert, plus 13,231 unreachable lines in `basin-pgcatalog`.
//! Leaving the arm makes the `match` a graveyard nobody can safely read.
//!
//! ## What the impl must get right
//!
//! * **Arguments arrive evaluated.** Do not evaluate anything yourself; the
//!   call site did it once, deliberately (see `crate::funcs`).
//! * **NULL in, NULL out**, unless PostgreSQL says otherwise for that
//!   function. The `v.map(...)` shape below never calls the body for a NULL,
//!   which is what makes that automatic.
//! * **Length alignment.** The returned array must have the same length as
//!   the inputs. Collecting from the input's iterator gives that for free;
//!   constructing an array some other way does not.
//! * **Verify against live PostgreSQL 18.2**, not against what the function
//!   obviously does. `lower` looks obvious and is not: see the note on
//!   `to_lowercase` below.

use std::sync::Arc;

use arrow_array::{ArrayRef, StringArray};
use basin_pgtype::Oid;

use crate::eval::{downcast_array, EvalSession};
use crate::funcs::ScalarFunc;
use crate::operator::ExecError;

/// Fetch argument `i`, or report a planner bug.
///
/// A missing argument here is never user error: resolution already matched
/// the call against a `pg_proc` row with a fixed arity, so arriving with too
/// few means the planner built something the catalog does not describe.
pub(crate) fn arg<'a>(
    args: &'a [ArrayRef],
    i: usize,
    oid: u32,
) -> Result<&'a ArrayRef, ExecError> {
    args.get(i).ok_or_else(|| {
        ExecError::Internal(format!(
            "function oid {oid} invoked with only {} argument(s) — a planner \
             bug, not user error",
            args.len()
        ))
    })
}

/// `lower(text) -> text`, `pg_proc` oid 870.
pub struct Lower;

impl ScalarFunc for Lower {
    fn oid(&self) -> Oid {
        Oid(870)
    }

    /// `return_type` is not overridden: `lower` returns exactly its
    /// `pg_proc.prorettype`, so the catalog-backed default is correct. Only
    /// functions whose result type is not expressible as one return oid need
    /// to override it — `extract(... FROM interval)` is the live example.
    fn invoke(
        &self,
        args: &[ArrayRef],
        _session: &EvalSession,
    ) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<StringArray>(arg(args, 0, 870)?, "text")?;
        // KNOWN DIVERGENCE, inherited from the `match` arm this replaces and
        // deliberately NOT changed here — a port must move behaviour, not
        // quietly alter it, or a later bisect blames the wrong commit.
        //
        // `str::to_lowercase` implements Unicode's *unconditional* full
        // lowercasing. PostgreSQL lowercases per the database collation.
        // They disagree on U+0130, measured on live 18.2 with
        // datcollate = en_US.UTF-8:
        //
        //   SELECT encode(convert_to(lower('İ'),'UTF8'),'hex')  ->  69
        //   str::to_lowercase("İ")                              ->  69 cc87
        //
        // PostgreSQL returns a bare `i` (length 1); Rust returns `i` plus a
        // COMBINING DOT ABOVE (U+0307), length 2. Every ASCII input agrees,
        // which is why this has never shown up in a test.
        //
        // Filed rather than fixed because the correct fix is collation-aware
        // case mapping for the whole string family, not a special case for
        // one code point — and because `upper`, `initcap`, `citext` and the
        // `LIKE`/pattern paths all share this assumption. See the module doc.
        let out: StringArray = a.iter().map(|v| v.map(str::to_lowercase)).collect();
        Ok(Arc::new(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::funcs::builtins;
    // `is_null`/`len` are `Array` trait methods, not inherent ones.
    use arrow_array::Array;

    /// The registry must actually be reached for this oid. Without this, a
    /// port that forgot step 2 would still pass every behavioural test,
    /// because the `match` arm would answer instead — the port would look
    /// done and have changed nothing.
    #[test]
    fn lower_is_hosted_by_the_registry() {
        assert!(
            builtins().scalar(Oid(870)).is_some(),
            "lower must be reachable through the registry, not only the match"
        );
    }

    /// Behaviour is preserved exactly across the port, INCLUDING the U+0130
    /// divergence from PostgreSQL. The assertion states what Basin does and
    /// what the server does, so the gap is pinned rather than latent.
    #[test]
    fn lower_preserves_behaviour_including_a_known_postgres_divergence() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("HeLLo"),
            Some("İ"),
            Some("ǄUNGLA"),
            None,
        ]));
        let out = Lower
            .invoke(&[input], &EvalSession::DEFAULT)
            .expect("lower over text");
        let s = out
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("text out");

        assert_eq!(s.value(0), "hello");
        assert_eq!(s.value(2), "ǆungla");
        assert!(s.is_null(3), "NULL in, NULL out");
        assert_eq!(s.len(), 4, "output length must match input length");

        // Measured on live PostgreSQL 18.2, datcollate en_US.UTF-8:
        //   encode(convert_to(lower('İ'),'UTF8'),'hex')  ->  69
        // Basin, via str::to_lowercase, produces 69 cc87.
        assert_eq!(
            s.value(1),
            "i\u{307}",
            "Basin appends COMBINING DOT ABOVE where PostgreSQL returns a \
             bare 'i' — a KNOWN divergence, unchanged by this port. Fixing it \
             means collation-aware case mapping for the whole string family, \
             not a special case here; if this assertion ever fails because \
             someone fixed it properly, change it to \"i\"."
        );
    }

    /// Arity is guaranteed by resolution, so a short call is a planner bug
    /// and must say so rather than silently returning something.
    #[test]
    fn a_missing_argument_is_reported_as_a_planner_bug() {
        let err = Lower
            .invoke(&[], &EvalSession::DEFAULT)
            .expect_err("no arguments must fail");
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("planner bug")),
            "expected a planner-bug Internal error, got {err:?}"
        );
    }
}
