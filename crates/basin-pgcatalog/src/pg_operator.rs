//! `pg_catalog.pg_operator`, a view over `basin-pgtype`'s own operator table.
//!
//! Like [`crate::pg_type`], this needs no [`CatalogSource`] at all: every
//! builtin operator Basin resolves lives in
//! `crates/basin-pgtype/src/operator.rs`'s `OPERATORS` table, already checked
//! against a live PostgreSQL 18 `pg_operator` (see that module's own docs for
//! the verifying query). This relation is that table, reshaped into
//! `pg_operator`'s column set.
//!
//! # Why this table, not a private map
//!
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` §2 ranks `pg_operator`
//! first among the missing relations, and not primarily for wire-protocol
//! compatibility: the owned planner must resolve `~`, `@>`, `=` and friends by
//! argument type against *some* table, replacing the textual rewriting in
//! `crates/basin-engine/src/pg_operators.rs`. `basin_pgtype::operator::OPERATORS`
//! already is that table. Making it queryable as `pg_catalog.pg_operator`
//! costs nothing beyond this file and delivers `\do` / driver introspection
//! for free — the same table serves both purposes at once.
//!
//! # Column set and where the values come from
//!
//! `oid`, `oprname`, `oprnamespace`, `oprleft`, `oprright`, `oprresult`,
//! `oprkind` — the columns `docs/migration/df-removal/11-pg-catalog-fidelity.md`
//! calls out as mattering for resolution and introspection. Every `(oid,
//! oprname, oprleft, oprright, oprresult)` tuple is inherited unchanged from
//! `OPERATORS`, whose own module docs record the live-database query used to
//! verify each row. `oprkind` is derived here: `'b'` when `OperatorSig::left`
//! is `Some` (a binary operator), `'l'` when it is `None` (a prefix operator,
//! Postgres's own `'l'` for "left unary"). Spot-checked live for both shapes:
//!
//! ```sql
//! SELECT oid, oprname, oprkind FROM pg_operator WHERE oid IN (96, 558);
//! --  96 | =       | b
//! -- 558 | -       | l
//! ```
//!
//! `oprnamespace` is always [`crate::PG_CATALOG_NAMESPACE`] — every operator
//! this crate knows about is a builtin.
//!
//! # Column audit against live PostgreSQL 18.2 (see crate-level task docs)
//!
//! ```sql
//! SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//! WHERE attrelid = 'pg_catalog.pg_operator'::regclass AND attnum > 0
//! ORDER BY attnum;
//! ```
//!
//! reports **15** columns; this relation previously implemented 7 of them,
//! and — same defect found in every sibling relation audited so far —
//! `oprkind` (real attnum 5) was placed *last*, after `oprleft`/`oprright`/
//! `oprresult` (real attnums 8/9/10), rather than before them. Fixed here.
//! The remaining columns, in real `attnum` order:
//!
//! - `oprowner` (attnum 4, `oid`, `NOT NULL`): the operator's owning role.
//!   `OperatorSig` has no owner field, but every row this crate can produce
//!   is a builtin, and real Postgres reports `oprowner = 10` (the bootstrap
//!   superuser) for all of them — confirmed by querying every oid this
//!   crate's `OPERATORS` table contains:
//!
//!   ```sql
//!   SELECT DISTINCT oprowner FROM pg_operator WHERE oid IN (<all 300 oids
//!   OPERATORS covers>);  -- 10, and only 10
//!   ```
//!
//!   Added as a literal `10` for every row, per this crate's placeholder
//!   convention (see [`crate::pg_index`]'s boolean defaults).
//! - `oprcanmerge`, `oprcanhash` (attnums 6, 7, both `boolean`, `NOT NULL`):
//!   whether the operator is merge-/hash-joinable. Unlike `oprowner` these
//!   are not uniform across every row, but the rule that predicts them
//!   *was* checked against every oid `OPERATORS` covers, not assumed: every
//!   `=` operator is `(true, true)` **except** the six cross-type
//!   date/timestamp/timestamptz equality oids (2347, 2360, 2373, 2386, 2536,
//!   2542), which are `(true, false)` — those compare values that don't
//!   share a single hash domain. Every non-`=` operator is `(false,
//!   false)`. Confirmed live:
//!
//!   ```sql
//!   SELECT oprname, oprcanmerge, oprcanhash, count(*)
//!     FROM pg_operator WHERE oid IN (<all 300 oids>) GROUP BY 1,2,3;
//!   -- every non-'=' row: f,f. '=' rows: 25× t,t and 6× t,f (the cross-type
//!   -- date/timestamp/timestamptz pairs above).
//!   ```
//!
//!   Implemented as a small lookup over that exact 6-oid exception set, not
//!   a guess.
//! - `oprcom`, `oprnegate` (attnums 11, 12, `oid`, `NOT NULL`), `oprcode`,
//!   `oprrest`, `oprjoin` (attnums 13–15, `regproc`, `NOT NULL`): the
//!   operator's commutator, negator, and implementing/restriction/join
//!   functions, from [`OPERATOR_FUNCTIONS`].
//!
//!   An earlier version of this file omitted all five on the grounds that
//!   `OperatorSig` carries none of it and there is no uniform default — a `0`
//!   placeholder would misreport most rows as having no commutator, negator
//!   or estimator when most of them do, and `oprcode` is **never** `0` in a
//!   real server (every operator has an implementing function, by
//!   construction). That reasoning was right about the placeholder and wrong
//!   about the conclusion: these are not values Basin has to invent, they are
//!   fixed properties of the *operator*, assigned by PostgreSQL's own catalog
//!   bootstrap (`genbki`) and identical on every installation, exactly like
//!   [`crate::pg_am`]'s `amhandler` and [`crate::pg_type`]'s `typinput`. So
//!   they are tabulated, one entry per oid `OPERATORS` covers, and
//!   `catalog_fidelity`'s `diff_static_rows` re-verifies every one of the
//!   1,500 cells against a live server on every run — which is the standard
//!   this crate now holds transcribed data to, and the reason transcribing it
//!   is safe where it previously was not.
//!
//!   `oprcom` is `0` for 69 of the 300 (an operator with no commutator),
//!   `oprnegate` for 109, and `oprrest`/`oprjoin` for 101 each — those zeros
//!   are the real values, not gaps. `oprcode` is non-zero for all 300; the
//!   oids it names are `pg_proc` rows [`crate::pg_proc`] mostly does not
//!   have, the same admitted gap `pg_am` and `pg_type` already carry.
//!
//! # `oid` is deduplicated to match a real primary key
//!
//! `pg_operator.oid` is a primary key in real Postgres (confirmed live: `"
//! pg_operator_oid_index" PRIMARY KEY, btree (oid)`), but `OPERATORS` itself
//! deliberately is **not** one row per oid — its own module docs explain that
//! the truly polymorphic array operators (`@>`, `<@`, `&&`, `||` on
//! `anyarray`/`anycompatiblearray`) are monomorphized into several rows
//! (`int4[] @> int4[]`, `text[] @> text[]`, ...) that legitimately share one
//! real oid, because that is the one oid Postgres itself resolves to
//! regardless of concrete element type. Reporting both monomorphizations as
//! separate `pg_operator` rows would violate the real table's primary key and
//! break the "a predicate on oid returns exactly one row" guarantee this
//! crate exists to provide. This relation therefore keeps only the *first*
//! `OPERATORS` row for each oid — the argument types of whichever
//! monomorphization happens to appear first in the source table — and drops
//! the rest. [`crate::operator`]-style resolution (not implemented in this
//! crate) is unaffected, since it consults the full `OPERATORS` table
//! directly, not this projection.
//!
//! # What is deliberately absent
//!
//! Everything `OPERATORS`' own module docs already say is absent: Postgres
//! ships roughly 800 builtin operators, `OPERATORS` covers comparison,
//! arithmetic, text, JSONB and a handful of array operators, and cross-type
//! rows (`int4 = int8`, real oid 416) are not tabulated — that pair resolves
//! by implicit widening onto the `int8 = int8` row (oid 410) instead, so this
//! relation's `pg_operator` will not contain a row for oid 416 at all. This is
//! a real, admitted gap versus a real server: a client that looks up
//! `pg_operator` by oid 416 will find nothing here, though `SELECT ... WHERE
//! int4col = int8col` still resolves correctly via the coercion path.

use std::{collections::HashSet, sync::Arc};

use arrow_array::{BooleanArray, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use basin_pgtype::operator::{OperatorSig, OPERATORS};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    predicate::{Predicate, Value},
};

/// Every builtin operator this crate knows about is owned by the bootstrap
/// superuser — confirmed live for every oid `OPERATORS` covers (see module
/// docs). Not a fabricated guess: there is no other role a builtin operator
/// could be owned by.
const BUILTIN_OPERATOR_OWNER: basin_pgtype::Oid = basin_pgtype::Oid(10);

/// The six cross-type date/timestamp/timestamptz equality oids that are
/// merge-joinable but *not* hash-joinable — see the module docs for the
/// live query that found exactly this set and nothing else.
const MERGEABLE_NOT_HASHABLE: [u32; 6] = [2347, 2360, 2373, 2386, 2536, 2542];

/// `(oprcanmerge, oprcanhash)` for this operator — see the module docs for
/// the live-verified rule this implements.
fn canmerge_canhash(op: &OperatorSig) -> (bool, bool) {
    if op.name != "=" {
        return (false, false);
    }
    if MERGEABLE_NOT_HASHABLE.contains(&op.oid.get()) {
        (true, false)
    } else {
        (true, true)
    }
}

/// `(oid, oprcom, oprnegate, oprcode, oprrest, oprjoin)` for every oid
/// `OPERATORS` covers — the operator's commutator, negator, implementing
/// function, restriction-selectivity estimator and join-selectivity
/// estimator.
///
/// These are real, fixed oids assigned by PostgreSQL's catalog bootstrap
/// (`genbki`), identical on every installation; see the module docs for why
/// transcribing them is not the fabricated placeholder this file previously
/// (correctly) refused. A `0` in any column but `oprcode` is the real value
/// meaning "none"; `oprcode` is never `0`, which
/// `every_operator_has_an_implementing_function` below pins, and which is
/// what makes a *missing* table entry detectable rather than silent.
///
/// The two `^` (exponentiation) rows, 965 and 1038, are the clearest instance
/// of that "none" being real rather than missing: exponentiation is not
/// commutative and has no boolean sense to negate, so `oprcom` and `oprnegate`
/// are genuinely `0`, and it carries no selectivity estimators either — four
/// of the five columns are `0` and only `oprcode` (`dpow`, `numeric_power`) is
/// not. Read off a live PostgreSQL 18.2 like every other row here.
const OPERATOR_FUNCTIONS: &[(u32, u32, u32, u32, u32, u32)] = &[
    (15, 416, 36, 852, 101, 105),       // integer = bigint (int48eq)
    (36, 417, 15, 853, 102, 106),       // integer <> bigint (int48ne)
    (37, 419, 82, 854, 103, 107),       // integer < bigint (int48lt)
    (58, 59, 1695, 56, 103, 107),       // boolean < boolean (boollt)
    (59, 58, 1694, 57, 104, 108),       // boolean > boolean (boolgt)
    (76, 418, 80, 855, 104, 108),       // integer > bigint (int48gt)
    (80, 430, 76, 856, 336, 386),       // integer <= bigint (int48le)
    (82, 420, 37, 857, 337, 398),       // integer >= bigint (int48ge)
    (85, 85, 91, 84, 102, 106),         // boolean <> boolean (boolne)
    (91, 91, 85, 60, 101, 105),         // boolean = boolean (booleq)
    (94, 94, 519, 63, 101, 105),        // smallint = smallint (int2eq)
    (95, 520, 524, 64, 103, 107),       // smallint < smallint (int2lt)
    (96, 96, 518, 65, 101, 105),        // integer = integer (int4eq)
    (97, 521, 525, 66, 103, 107),       // integer < integer (int4lt)
    (98, 98, 531, 67, 101, 105),        // text = text (texteq)
    (375, 0, 0, 383, 0, 0),             // anycompatiblearray || anycompatiblearray (array_cat)
    (410, 410, 411, 467, 101, 105),     // bigint = bigint (int8eq)
    (411, 411, 410, 468, 102, 106),     // bigint <> bigint (int8ne)
    (412, 413, 415, 469, 103, 107),     // bigint < bigint (int8lt)
    (413, 412, 414, 470, 104, 108),     // bigint > bigint (int8gt)
    (414, 415, 413, 471, 336, 386),     // bigint <= bigint (int8le)
    (415, 414, 412, 472, 337, 398),     // bigint >= bigint (int8ge)
    (416, 15, 417, 474, 101, 105),      // bigint = integer (int84eq)
    (417, 36, 416, 475, 102, 106),      // bigint <> integer (int84ne)
    (418, 76, 430, 476, 103, 107),      // bigint < integer (int84lt)
    (419, 37, 420, 477, 104, 108),      // bigint > integer (int84gt)
    (420, 82, 419, 478, 336, 386),      // bigint <= integer (int84le)
    (430, 80, 418, 479, 337, 398),      // bigint >= integer (int84ge)
    (439, 0, 0, 945, 0, 0),             // bigint % bigint (int8mod)
    (484, 0, 0, 462, 0, 0),             // - - bigint (int8um)
    (514, 514, 0, 141, 0, 0),           // integer * integer (int4mul)
    (518, 518, 96, 144, 102, 106),      // integer <> integer (int4ne)
    (519, 519, 94, 145, 102, 106),      // smallint <> smallint (int2ne)
    (520, 95, 522, 146, 104, 108),      // smallint > smallint (int2gt)
    (521, 97, 523, 147, 104, 108),      // integer > integer (int4gt)
    (522, 524, 520, 148, 336, 386),     // smallint <= smallint (int2le)
    (523, 525, 521, 149, 336, 386),     // integer <= integer (int4le)
    (524, 522, 95, 151, 337, 398),      // smallint >= smallint (int2ge)
    (525, 523, 97, 150, 337, 398),      // integer >= integer (int4ge)
    (526, 526, 0, 152, 0, 0),           // smallint * smallint (int2mul)
    (527, 0, 0, 153, 0, 0),             // smallint / smallint (int2div)
    (528, 0, 0, 154, 0, 0),             // integer / integer (int4div)
    (529, 0, 0, 155, 0, 0),             // smallint % smallint (int2mod)
    (530, 0, 0, 156, 0, 0),             // integer % integer (int4mod)
    (531, 531, 98, 157, 102, 106),      // text <> text (textne)
    (532, 533, 538, 158, 101, 105),     // smallint = integer (int24eq)
    (533, 532, 539, 159, 101, 105),     // integer = smallint (int42eq)
    (534, 537, 542, 160, 103, 107),     // smallint < integer (int24lt)
    (535, 536, 543, 161, 103, 107),     // integer < smallint (int42lt)
    (536, 535, 540, 162, 104, 108),     // smallint > integer (int24gt)
    (537, 534, 541, 163, 104, 108),     // integer > smallint (int42gt)
    (538, 539, 532, 164, 102, 106),     // smallint <> integer (int24ne)
    (539, 538, 533, 165, 102, 106),     // integer <> smallint (int42ne)
    (540, 543, 536, 166, 336, 386),     // smallint <= integer (int24le)
    (541, 542, 537, 167, 336, 386),     // integer <= smallint (int42le)
    (542, 541, 534, 168, 337, 398),     // smallint >= integer (int24ge)
    (543, 540, 535, 169, 337, 398),     // integer >= smallint (int42ge)
    (544, 545, 0, 170, 0, 0),           // smallint * integer (int24mul)
    (545, 544, 0, 171, 0, 0),           // integer * smallint (int42mul)
    (546, 0, 0, 172, 0, 0),             // smallint / integer (int24div)
    (547, 0, 0, 173, 0, 0),             // integer / smallint (int42div)
    (550, 550, 0, 176, 0, 0),           // smallint + smallint (int2pl)
    (551, 551, 0, 177, 0, 0),           // integer + integer (int4pl)
    (552, 553, 0, 178, 0, 0),           // smallint + integer (int24pl)
    (553, 552, 0, 179, 0, 0),           // integer + smallint (int42pl)
    (554, 0, 0, 180, 0, 0),             // smallint - smallint (int2mi)
    (555, 0, 0, 181, 0, 0),             // integer - integer (int4mi)
    (556, 0, 0, 182, 0, 0),             // smallint - integer (int24mi)
    (557, 0, 0, 183, 0, 0),             // integer - smallint (int42mi)
    (558, 0, 0, 212, 0, 0),             // - - integer (int4um)
    (559, 0, 0, 213, 0, 0),             // - - smallint (int2um)
    (584, 0, 0, 206, 0, 0),             // - - real (float4um)
    (585, 0, 0, 220, 0, 0),             // - - double precision (float8um)
    (586, 586, 0, 204, 0, 0),           // real + real (float4pl)
    (587, 0, 0, 205, 0, 0),             // real - real (float4mi)
    (588, 0, 0, 203, 0, 0),             // real / real (float4div)
    (589, 589, 0, 202, 0, 0),           // real * real (float4mul)
    (591, 591, 0, 218, 0, 0),           // double precision + double precision (float8pl)
    (592, 0, 0, 219, 0, 0),             // double precision - double precision (float8mi)
    (593, 0, 0, 217, 0, 0),             // double precision / double precision (float8div)
    (594, 594, 0, 216, 0, 0),           // double precision * double precision (float8mul)
    (620, 620, 621, 287, 101, 105),     // real = real (float4eq)
    (621, 621, 620, 288, 102, 106),     // real <> real (float4ne)
    (622, 623, 625, 289, 103, 107),     // real < real (float4lt)
    (623, 622, 624, 291, 104, 108),     // real > real (float4gt)
    (624, 625, 623, 290, 336, 386),     // real <= real (float4le)
    (625, 624, 622, 292, 337, 398),     // real >= real (float4ge)
    (641, 0, 642, 1254, 1818, 1824),    // text ~ text (textregexeq)
    (642, 0, 641, 1256, 1821, 1827),    // text !~ text (textregexne)
    (654, 0, 0, 1258, 0, 0),            // text || text (textcat)
    (664, 666, 667, 740, 103, 107),     // text < text (text_lt)
    (665, 667, 666, 741, 336, 386),     // text <= text (text_le)
    (666, 664, 665, 742, 104, 108),     // text > text (text_gt)
    (667, 665, 664, 743, 337, 398),     // text >= text (text_ge)
    (670, 670, 671, 293, 101, 105),     // double precision = double precision (float8eq)
    (671, 671, 670, 294, 102, 106),     // double precision <> double precision (float8ne)
    (672, 674, 675, 295, 103, 107),     // double precision < double precision (float8lt)
    (673, 675, 674, 296, 336, 386),     // double precision <= double precision (float8le)
    (674, 672, 673, 297, 104, 108),     // double precision > double precision (float8gt)
    (675, 673, 672, 298, 337, 398),     // double precision >= double precision (float8ge)
    (684, 684, 0, 463, 0, 0),           // bigint + bigint (int8pl)
    (685, 0, 0, 464, 0, 0),             // bigint - bigint (int8mi)
    (686, 686, 0, 465, 0, 0),           // bigint * bigint (int8mul)
    (687, 0, 0, 466, 0, 0),             // bigint / bigint (int8div)
    (688, 692, 0, 1274, 0, 0),          // bigint + integer (int84pl)
    (689, 0, 0, 1275, 0, 0),            // bigint - integer (int84mi)
    (690, 694, 0, 1276, 0, 0),          // bigint * integer (int84mul)
    (691, 0, 0, 1277, 0, 0),            // bigint / integer (int84div)
    (692, 688, 0, 1278, 0, 0),          // integer + bigint (int48pl)
    (693, 0, 0, 1279, 0, 0),            // integer - bigint (int48mi)
    (694, 690, 0, 1280, 0, 0),          // integer * bigint (int48mul)
    (695, 0, 0, 1281, 0, 0),            // integer / bigint (int48div)
    (818, 822, 0, 837, 0, 0),           // bigint + smallint (int82pl)
    (819, 0, 0, 838, 0, 0),             // bigint - smallint (int82mi)
    (820, 824, 0, 839, 0, 0),           // bigint * smallint (int82mul)
    (821, 0, 0, 840, 0, 0),             // bigint / smallint (int82div)
    (822, 818, 0, 841, 0, 0),           // smallint + bigint (int28pl)
    (823, 0, 0, 942, 0, 0),             // smallint - bigint (int28mi)
    (824, 820, 0, 943, 0, 0),           // smallint * bigint (int28mul)
    (825, 0, 0, 948, 0, 0),             // smallint / bigint (int28div)
    (965, 0, 0, 232, 0, 0),             // double precision ^ double precision (dpow)
    (1038, 0, 0, 1739, 0, 0),           // numeric ^ numeric (numeric_power)
    (1054, 1054, 1057, 1048, 101, 105), // character = character (bpchareq)
    (1057, 1057, 1054, 1053, 102, 106), // character <> character (bpcharne)
    (1058, 1060, 1061, 1049, 103, 107), // character < character (bpcharlt)
    (1059, 1061, 1060, 1050, 336, 386), // character <= character (bpcharle)
    (1060, 1058, 1059, 1051, 104, 108), // character > character (bpchargt)
    (1061, 1059, 1058, 1052, 337, 398), // character >= character (bpcharge)
    (1076, 2551, 0, 2071, 0, 0),        // date + interval (date_pl_interval)
    (1077, 0, 0, 2072, 0, 0),           // date - interval (date_mi_interval)
    (1093, 1093, 1094, 1086, 101, 105), // date = date (date_eq)
    (1094, 1094, 1093, 1091, 102, 106), // date <> date (date_ne)
    (1095, 1097, 1098, 1087, 103, 107), // date < date (date_lt)
    (1096, 1098, 1097, 1088, 336, 386), // date <= date (date_le)
    (1097, 1095, 1096, 1089, 104, 108), // date > date (date_gt)
    (1098, 1096, 1095, 1090, 337, 398), // date >= date (date_ge)
    (1099, 0, 0, 1140, 0, 0),           // date - date (date_mi)
    (1100, 2555, 0, 1141, 0, 0),        // date + integer (date_pli)
    (1101, 0, 0, 1142, 0, 0),           // date - integer (date_mii)
    (1108, 1108, 1109, 1145, 101, 105), // time without time zone = time without time zone (time_eq)
    (1109, 1109, 1108, 1106, 102, 106), // time without time zone <> time without time zone (time_ne)
    (1110, 1112, 1113, 1102, 103, 107), // time without time zone < time without time zone (time_lt)
    (1111, 1113, 1112, 1103, 336, 386), // time without time zone <= time without time zone (time_le)
    (1112, 1110, 1111, 1104, 104, 108), // time without time zone > time without time zone (time_gt)
    (1113, 1111, 1110, 1105, 337, 398), // time without time zone >= time without time zone (time_ge)
    (1116, 1126, 0, 281, 0, 0),         // real + double precision (float48pl)
    (1117, 0, 0, 282, 0, 0),            // real - double precision (float48mi)
    (1118, 0, 0, 280, 0, 0),            // real / double precision (float48div)
    (1119, 1129, 0, 279, 0, 0),         // real * double precision (float48mul)
    (1120, 1130, 1121, 299, 101, 105),  // real = double precision (float48eq)
    (1121, 1131, 1120, 300, 102, 106),  // real <> double precision (float48ne)
    (1122, 1133, 1125, 301, 103, 107),  // real < double precision (float48lt)
    (1123, 1132, 1124, 303, 104, 108),  // real > double precision (float48gt)
    (1124, 1135, 1123, 302, 336, 386),  // real <= double precision (float48le)
    (1125, 1134, 1122, 304, 337, 398),  // real >= double precision (float48ge)
    (1126, 1116, 0, 285, 0, 0),         // double precision + real (float84pl)
    (1127, 0, 0, 286, 0, 0),            // double precision - real (float84mi)
    (1128, 0, 0, 284, 0, 0),            // double precision / real (float84div)
    (1129, 1119, 0, 283, 0, 0),         // double precision * real (float84mul)
    (1130, 1120, 1131, 305, 101, 105),  // double precision = real (float84eq)
    (1131, 1121, 1130, 306, 102, 106),  // double precision <> real (float84ne)
    (1132, 1123, 1135, 307, 103, 107),  // double precision < real (float84lt)
    (1133, 1122, 1134, 309, 104, 108),  // double precision > real (float84gt)
    (1134, 1125, 1133, 308, 336, 386),  // double precision <= real (float84le)
    (1135, 1124, 1132, 310, 337, 398),  // double precision >= real (float84ge)
    (1209, 0, 1210, 850, 1819, 1825),   // text ~~ text (textlike)
    (1228, 0, 1229, 1238, 1820, 1826),  // text ~* text (texticregexeq)
    (1229, 0, 1228, 1239, 1823, 1829),  // text !~* text (texticregexne)
    (1320, 1320, 1321, 1152, 101, 105), // timestamp with time zone = timestamp with time zone (timestamptz_eq)
    (1321, 1321, 1320, 1153, 102, 106), // timestamp with time zone <> timestamp with time zone (timestamptz_ne)
    (1322, 1324, 1325, 1154, 103, 107), // timestamp with time zone < timestamp with time zone (timestamptz_lt)
    (1323, 1325, 1324, 1155, 336, 386), // timestamp with time zone <= timestamp with time zone (timestamptz_le)
    (1324, 1322, 1323, 1157, 104, 108), // timestamp with time zone > timestamp with time zone (timestamptz_gt)
    (1325, 1323, 1322, 1156, 337, 398), // timestamp with time zone >= timestamp with time zone (timestamptz_ge)
    (1327, 2554, 0, 1189, 0, 0), // timestamp with time zone + interval (timestamptz_pl_interval)
    (1328, 0, 0, 1188, 0, 0), // timestamp with time zone - timestamp with time zone (timestamptz_mi)
    (1329, 0, 0, 1190, 0, 0), // timestamp with time zone - interval (timestamptz_mi_interval)
    (1330, 1330, 1331, 1162, 101, 105), // interval = interval (interval_eq)
    (1331, 1331, 1330, 1163, 102, 106), // interval <> interval (interval_ne)
    (1332, 1334, 1335, 1164, 103, 107), // interval < interval (interval_lt)
    (1333, 1335, 1334, 1165, 336, 386), // interval <= interval (interval_le)
    (1334, 1332, 1333, 1167, 104, 108), // interval > interval (interval_gt)
    (1335, 1333, 1332, 1166, 337, 398), // interval >= interval (interval_ge)
    (1336, 0, 0, 1168, 0, 0), // - - interval (interval_um)
    (1337, 1337, 0, 1169, 0, 0), // interval + interval (interval_pl)
    (1338, 0, 0, 1170, 0, 0), // interval - interval (interval_mi)
    (1399, 0, 0, 1690, 0, 0), // time without time zone - time without time zone (time_mi_time)
    (1550, 1550, 1551, 1352, 101, 105), // time with time zone = time with time zone (timetz_eq)
    (1551, 1551, 1550, 1353, 102, 106), // time with time zone <> time with time zone (timetz_ne)
    (1552, 1554, 1555, 1354, 103, 107), // time with time zone < time with time zone (timetz_lt)
    (1553, 1555, 1554, 1355, 336, 386), // time with time zone <= time with time zone (timetz_le)
    (1554, 1552, 1553, 1357, 104, 108), // time with time zone > time with time zone (timetz_gt)
    (1555, 1553, 1552, 1356, 337, 398), // time with time zone >= time with time zone (timetz_ge)
    (1583, 1584, 0, 1618, 0, 0), // interval * double precision (interval_mul)
    (1585, 0, 0, 1326, 0, 0), // interval / double precision (interval_div)
    (1694, 1695, 59, 1691, 336, 386), // boolean <= boolean (boolle)
    (1695, 1694, 58, 1692, 337, 398), // boolean >= boolean (boolge)
    (1751, 0, 0, 1771, 0, 0), // - - numeric (numeric_uminus)
    (1752, 1752, 1753, 1718, 101, 105), // numeric = numeric (numeric_eq)
    (1753, 1753, 1752, 1719, 102, 106), // numeric <> numeric (numeric_ne)
    (1754, 1756, 1757, 1722, 103, 107), // numeric < numeric (numeric_lt)
    (1755, 1757, 1756, 1723, 336, 386), // numeric <= numeric (numeric_le)
    (1756, 1754, 1755, 1720, 104, 108), // numeric > numeric (numeric_gt)
    (1757, 1755, 1754, 1721, 337, 398), // numeric >= numeric (numeric_ge)
    (1758, 1758, 0, 1724, 0, 0), // numeric + numeric (numeric_add)
    (1759, 0, 0, 1725, 0, 0), // numeric - numeric (numeric_sub)
    (1760, 1760, 0, 1726, 0, 0), // numeric * numeric (numeric_mul)
    (1761, 0, 0, 1727, 0, 0), // numeric / numeric (numeric_div)
    (1762, 0, 0, 1729, 0, 0), // numeric % numeric (numeric_mod)
    (1800, 1849, 0, 1747, 0, 0), // time without time zone + interval (time_pl_interval)
    (1801, 0, 0, 1748, 0, 0), // time without time zone - interval (time_mi_interval)
    (1849, 1800, 0, 1848, 0, 0), // interval + time without time zone (interval_pl_time)
    (1862, 1868, 1863, 1850, 101, 105), // smallint = bigint (int28eq)
    (1863, 1869, 1862, 1851, 102, 106), // smallint <> bigint (int28ne)
    (1864, 1871, 1867, 1852, 103, 107), // smallint < bigint (int28lt)
    (1865, 1870, 1866, 1853, 104, 108), // smallint > bigint (int28gt)
    (1866, 1873, 1865, 1854, 336, 386), // smallint <= bigint (int28le)
    (1867, 1872, 1864, 1855, 337, 398), // smallint >= bigint (int28ge)
    (1868, 1862, 1869, 1856, 101, 105), // bigint = smallint (int82eq)
    (1869, 1863, 1868, 1857, 102, 106), // bigint <> smallint (int82ne)
    (1870, 1865, 1873, 1858, 103, 107), // bigint < smallint (int82lt)
    (1871, 1864, 1872, 1859, 104, 108), // bigint > smallint (int82gt)
    (1872, 1867, 1871, 1860, 336, 386), // bigint <= smallint (int82le)
    (1873, 1866, 1870, 1861, 337, 398), // bigint >= smallint (int82ge)
    (1955, 1955, 1956, 1948, 101, 105), // bytea = bytea (byteaeq)
    (1956, 1956, 1955, 1953, 102, 106), // bytea <> bytea (byteane)
    (1957, 1959, 1960, 1949, 103, 107), // bytea < bytea (bytealt)
    (1958, 1960, 1959, 1950, 336, 386), // bytea <= bytea (byteale)
    (1959, 1957, 1958, 1951, 104, 108), // bytea > bytea (byteagt)
    (1960, 1958, 1957, 1952, 337, 398), // bytea >= bytea (byteage)
    (2018, 0, 0, 2011, 0, 0), // bytea || bytea (byteacat)
    (2060, 2060, 2061, 2052, 101, 105), // timestamp without time zone = timestamp without time zone (timestamp_eq)
    (2061, 2061, 2060, 2053, 102, 106), // timestamp without time zone <> timestamp without time zone (timestamp_ne)
    (2062, 2064, 2065, 2054, 103, 107), // timestamp without time zone < timestamp without time zone (timestamp_lt)
    (2063, 2065, 2064, 2055, 336, 386), // timestamp without time zone <= timestamp without time zone (timestamp_le)
    (2064, 2062, 2063, 2057, 104, 108), // timestamp without time zone > timestamp without time zone (timestamp_gt)
    (2065, 2063, 2062, 2056, 337, 398), // timestamp without time zone >= timestamp without time zone (timestamp_ge)
    (2066, 2553, 0, 2032, 0, 0), // timestamp without time zone + interval (timestamp_pl_interval)
    (2067, 0, 0, 2031, 0, 0), // timestamp without time zone - timestamp without time zone (timestamp_mi)
    (2068, 0, 0, 2033, 0, 0), // timestamp without time zone - interval (timestamp_mi_interval)
    (2345, 2375, 2348, 2338, 103, 107), // date < timestamp without time zone (date_lt_timestamp)
    (2346, 2374, 2349, 2339, 336, 386), // date <= timestamp without time zone (date_le_timestamp)
    (2347, 2373, 2350, 2340, 101, 105), // date = timestamp without time zone (date_eq_timestamp)
    (2348, 2372, 2345, 2342, 337, 398), // date >= timestamp without time zone (date_ge_timestamp)
    (2349, 2371, 2346, 2341, 104, 108), // date > timestamp without time zone (date_gt_timestamp)
    (2350, 2376, 2347, 2343, 102, 106), // date <> timestamp without time zone (date_ne_timestamp)
    (2358, 2388, 2361, 2351, 103, 107), // date < timestamp with time zone (date_lt_timestamptz)
    (2359, 2387, 2362, 2352, 336, 386), // date <= timestamp with time zone (date_le_timestamptz)
    (2360, 2386, 2363, 2353, 101, 105), // date = timestamp with time zone (date_eq_timestamptz)
    (2361, 2385, 2358, 2355, 337, 398), // date >= timestamp with time zone (date_ge_timestamptz)
    (2362, 2384, 2359, 2354, 104, 108), // date > timestamp with time zone (date_gt_timestamptz)
    (2363, 2389, 2360, 2356, 102, 106), // date <> timestamp with time zone (date_ne_timestamptz)
    (2371, 2349, 2374, 2364, 103, 107), // timestamp without time zone < date (timestamp_lt_date)
    (2372, 2348, 2375, 2365, 336, 386), // timestamp without time zone <= date (timestamp_le_date)
    (2373, 2347, 2376, 2366, 101, 105), // timestamp without time zone = date (timestamp_eq_date)
    (2374, 2346, 2371, 2368, 337, 398), // timestamp without time zone >= date (timestamp_ge_date)
    (2375, 2345, 2372, 2367, 104, 108), // timestamp without time zone > date (timestamp_gt_date)
    (2376, 2350, 2373, 2369, 102, 106), // timestamp without time zone <> date (timestamp_ne_date)
    (2384, 2362, 2387, 2377, 103, 107), // timestamp with time zone < date (timestamptz_lt_date)
    (2385, 2361, 2388, 2378, 336, 386), // timestamp with time zone <= date (timestamptz_le_date)
    (2386, 2360, 2389, 2379, 101, 105), // timestamp with time zone = date (timestamptz_eq_date)
    (2387, 2359, 2384, 2381, 337, 398), // timestamp with time zone >= date (timestamptz_ge_date)
    (2388, 2358, 2385, 2380, 104, 108), // timestamp with time zone > date (timestamptz_gt_date)
    (2389, 2363, 2386, 2382, 102, 106), // timestamp with time zone <> date (timestamptz_ne_date)
    (2534, 2544, 2537, 2520, 103, 107), // timestamp without time zone < timestamp with time zone (timestamp_lt_timestamptz)
    (2535, 2543, 2538, 2521, 336, 386), // timestamp without time zone <= timestamp with time zone (timestamp_le_timestamptz)
    (2536, 2542, 2539, 2522, 101, 105), // timestamp without time zone = timestamp with time zone (timestamp_eq_timestamptz)
    (2537, 2541, 2534, 2524, 337, 398), // timestamp without time zone >= timestamp with time zone (timestamp_ge_timestamptz)
    (2538, 2540, 2535, 2523, 104, 108), // timestamp without time zone > timestamp with time zone (timestamp_gt_timestamptz)
    (2539, 2545, 2536, 2525, 102, 106), // timestamp without time zone <> timestamp with time zone (timestamp_ne_timestamptz)
    (2540, 2538, 2543, 2527, 103, 107), // timestamp with time zone < timestamp without time zone (timestamptz_lt_timestamp)
    (2541, 2537, 2544, 2528, 336, 386), // timestamp with time zone <= timestamp without time zone (timestamptz_le_timestamp)
    (2542, 2536, 2545, 2529, 101, 105), // timestamp with time zone = timestamp without time zone (timestamptz_eq_timestamp)
    (2543, 2535, 2540, 2531, 337, 398), // timestamp with time zone >= timestamp without time zone (timestamptz_ge_timestamp)
    (2544, 2534, 2541, 2530, 104, 108), // timestamp with time zone > timestamp without time zone (timestamptz_gt_timestamp)
    (2545, 2539, 2542, 2532, 102, 106), // timestamp with time zone <> timestamp without time zone (timestamptz_ne_timestamp)
    (2551, 1076, 0, 2546, 0, 0),        // interval + date (interval_pl_date)
    (2553, 2066, 0, 2548, 0, 0), // interval + timestamp without time zone (interval_pl_timestamp)
    (2554, 1327, 0, 2549, 0, 0), // interval + timestamp with time zone (interval_pl_timestamptz)
    (2555, 1100, 0, 2550, 0, 0), // integer + date (integer_pl_date)
    (2750, 2750, 0, 2747, 3817, 3818), // anyarray && anyarray (arrayoverlap)
    (2751, 2752, 0, 2748, 3817, 3818), // anyarray @> anyarray (arraycontains)
    (2752, 2751, 0, 2749, 3817, 3818), // anyarray <@ anyarray (arraycontained)
    (2972, 2972, 2973, 2956, 101, 105), // uuid = uuid (uuid_eq)
    (2973, 2973, 2972, 2959, 102, 106), // uuid <> uuid (uuid_ne)
    (2974, 2975, 2977, 2954, 103, 107), // uuid < uuid (uuid_lt)
    (2975, 2974, 2976, 2958, 104, 108), // uuid > uuid (uuid_gt)
    (2976, 2977, 2975, 2955, 336, 386), // uuid <= uuid (uuid_le)
    (2977, 2976, 2974, 2957, 337, 398), // uuid >= uuid (uuid_ge)
    (3206, 0, 0, 3940, 0, 0),    // jsonb #>> text[] (jsonb_extract_path_text)
    (3211, 0, 0, 3478, 0, 0),    // jsonb -> text (jsonb_object_field)
    (3212, 0, 0, 3215, 0, 0),    // jsonb -> integer (jsonb_array_element)
    (3213, 0, 0, 3217, 0, 0),    // jsonb #> text[] (jsonb_extract_path)
    (3246, 3250, 0, 4046, 5040, 5041), // jsonb @> jsonb (jsonb_contains)
    (3247, 0, 0, 4047, 5040, 5041), // jsonb ? text (jsonb_exists)
    (3248, 0, 0, 4048, 5040, 5041), // jsonb ?| text[] (jsonb_exists_any)
    (3249, 0, 0, 4049, 5040, 5041), // jsonb ?& text[] (jsonb_exists_all)
    (3250, 3246, 0, 4050, 5040, 5041), // jsonb <@ jsonb (jsonb_contained)
    (3477, 0, 0, 3214, 0, 0),    // jsonb ->> text (jsonb_object_field_text)
    (3481, 0, 0, 3216, 0, 0),    // jsonb ->> integer (jsonb_array_element_text)
];

/// `(oprcom, oprnegate, oprcode, oprrest, oprjoin)` for this operator — see
/// [`OPERATOR_FUNCTIONS`]. An oid with no entry yields all zeros, which
/// `oprcode = 0` makes visible rather than plausible.
fn operator_functions(op: &OperatorSig) -> (u32, u32, u32, u32, u32) {
    OPERATOR_FUNCTIONS
        .iter()
        .find(|(oid, ..)| *oid == op.oid.get())
        .map(|&(_, com, negate, code, rest, join)| (com, negate, code, rest, join))
        .unwrap_or((0, 0, 0, 0, 0))
}

/// The real, polymorphic signature of each `OPERATORS` oid that table
/// monomorphizes into several concrete rows.
///
/// `operator.rs` deliberately expands `anyarray`/`anycompatiblearray`
/// operators into one row per concrete element type (`int4[] @> int4[]`,
/// `text[] @> text[]`, ...) because that is what its own resolution needs,
/// and — as this module's docs on deduplication already say — all of those
/// rows legitimately share the *one* real oid Postgres resolves to. This
/// relation reports one row per oid, so it must report the **real** row's
/// signature, not whichever monomorphization happened to come first in
/// `OPERATORS`.
///
/// Reporting the monomorphization is not harmless: it told clients that
/// `pg_operator` oid 2751 is `integer[] @> integer[]`, so anything resolving
/// `@>` for `text[]` against this relation would find the operator declared
/// for the wrong operand type and conclude there is none. Found by
/// `catalog_fidelity`'s `diff_static_rows` the first time that oracle was
/// pointed at `pg_operator`; re-verified against a live server every run.
///
/// `(oid, oprleft, oprright, oprresult)`. 2277 = `anyarray`,
/// 5078 = `anycompatiblearray`, 16 = `boolean`.
const POLYMORPHIC_SIGNATURES: &[(u32, u32, u32, u32)] = &[
    (375, 5078, 5078, 5078), // anycompatiblearray || anycompatiblearray
    (2750, 2277, 2277, 16),  // anyarray && anyarray
    (2751, 2277, 2277, 16),  // anyarray @> anyarray
    (2752, 2277, 2277, 16),  // anyarray <@ anyarray
];

/// `(oprleft, oprright, oprresult)` for this operator — the real row's, which
/// differs from `OperatorSig`'s concrete types for the polymorphic array
/// operators. See [`POLYMORPHIC_SIGNATURES`].
fn operand_types(op: &OperatorSig) -> (basin_pgtype::Oid, basin_pgtype::Oid, basin_pgtype::Oid) {
    if let Some((_, left, right, result)) = POLYMORPHIC_SIGNATURES
        .iter()
        .find(|(oid, ..)| *oid == op.oid.get())
    {
        return (
            basin_pgtype::Oid(*left),
            basin_pgtype::Oid(*right),
            basin_pgtype::Oid(*result),
        );
    }
    (
        op.left.unwrap_or(basin_pgtype::Oid::INVALID),
        op.right,
        op.result,
    )
}

/// This operator's value for `column`, or `None` if `column` is not one of
/// this relation's columns.
fn value(op: &OperatorSig, column: &str) -> Option<Value> {
    Some(match column {
        "oid" => Value::Oid(op.oid),
        "oprname" => Value::Text(op.name.to_string()),
        "oprnamespace" => Value::Oid(crate::PG_CATALOG_NAMESPACE),
        "oprowner" => Value::Oid(BUILTIN_OPERATOR_OWNER),
        "oprkind" => Value::Text(if op.left.is_some() { "b" } else { "l" }.to_string()),
        "oprcanmerge" => Value::Bool(canmerge_canhash(op).0),
        "oprcanhash" => Value::Bool(canmerge_canhash(op).1),
        "oprleft" => Value::Oid(operand_types(op).0),
        "oprright" => Value::Oid(operand_types(op).1),
        "oprresult" => Value::Oid(operand_types(op).2),
        "oprcom" => Value::Oid(basin_pgtype::Oid(operator_functions(op).0)),
        "oprnegate" => Value::Oid(basin_pgtype::Oid(operator_functions(op).1)),
        "oprcode" => Value::Oid(basin_pgtype::Oid(operator_functions(op).2)),
        "oprrest" => Value::Oid(basin_pgtype::Oid(operator_functions(op).3)),
        "oprjoin" => Value::Oid(basin_pgtype::Oid(operator_functions(op).4)),
        _ => return None,
    })
}

/// `OPERATORS`, keeping only the first row for each oid — see the module docs
/// on why a real `pg_operator` cannot report the polymorphic array operators'
/// several monomorphizations as separate rows.
fn deduplicated_by_oid() -> Vec<&'static OperatorSig> {
    let mut seen = HashSet::new();
    OPERATORS.iter().filter(|op| seen.insert(op.oid)).collect()
}

/// `pg_catalog.pg_operator`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgOperator;

impl PgOperator {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("oprname", DataType::Utf8, false),
            Field::new("oprnamespace", DataType::UInt32, false),
            Field::new("oprowner", DataType::UInt32, false),
            Field::new("oprkind", DataType::Utf8, false),
            Field::new("oprcanmerge", DataType::Boolean, false),
            Field::new("oprcanhash", DataType::Boolean, false),
            Field::new("oprleft", DataType::UInt32, false),
            Field::new("oprright", DataType::UInt32, false),
            Field::new("oprresult", DataType::UInt32, false),
            // `oprcode`, `oprrest` and `oprjoin` are `regproc` in real
            // Postgres, represented as the `oid` they are at the storage
            // level — this crate's convention throughout (see
            // `crate::pg_cast`'s `castfunc` and `crate::pg_am`'s
            // `amhandler`).
            Field::new("oprcom", DataType::UInt32, false),
            Field::new("oprnegate", DataType::UInt32, false),
            Field::new("oprcode", DataType::UInt32, false),
            Field::new("oprrest", DataType::UInt32, false),
            Field::new("oprjoin", DataType::UInt32, false),
        ]))
    }
}

impl crate::SystemView for PgOperator {
    fn name(&self) -> &str {
        "pg_operator"
    }

    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }

    fn scan(
        &self,
        _catalog: &dyn CatalogSource,
        pushed: &[Predicate],
    ) -> Result<RecordBatch, Error> {
        let schema = Self::arrow_schema();
        for p in pushed {
            if !schema.fields().iter().any(|f| f.name() == p.column()) {
                return Err(Error::UnknownColumn {
                    relation: "pg_operator",
                    column: p.column().to_string(),
                });
            }
        }

        let rows: Vec<&OperatorSig> = deduplicated_by_oid()
            .into_iter()
            .filter(|op| {
                pushed
                    .iter()
                    .all(|p| p.matches(value(op, p.column()).as_ref()))
            })
            .collect();

        let oids: UInt32Array = rows.iter().map(|r| r.oid.get()).collect();
        let oprnames: StringArray = rows.iter().map(|r| Some(r.name)).collect();
        let oprnamespaces: UInt32Array = rows
            .iter()
            .map(|_| crate::PG_CATALOG_NAMESPACE.get())
            .collect();
        // Placeholder — see the module docs for why `10` is the real,
        // live-verified value for every row, not a guess.
        let oprowners: UInt32Array = rows.iter().map(|_| BUILTIN_OPERATOR_OWNER.get()).collect();
        let oprkinds: StringArray = rows
            .iter()
            .map(|r| Some(if r.left.is_some() { "b" } else { "l" }))
            .collect();
        let oprcanmerges: BooleanArray = rows.iter().map(|r| canmerge_canhash(r).0).collect();
        let oprcanhashes: BooleanArray = rows.iter().map(|r| canmerge_canhash(r).1).collect();
        // The real row's operand/result types, which differ from
        // `OperatorSig`'s for the polymorphic array operators — see
        // [`POLYMORPHIC_SIGNATURES`].
        let oprlefts: UInt32Array = rows.iter().map(|r| operand_types(r).0.get()).collect();
        let oprrights: UInt32Array = rows.iter().map(|r| operand_types(r).1.get()).collect();
        let oprresults: UInt32Array = rows.iter().map(|r| operand_types(r).2.get()).collect();
        let oprcoms: UInt32Array = rows.iter().map(|r| operator_functions(r).0).collect();
        let oprnegates: UInt32Array = rows.iter().map(|r| operator_functions(r).1).collect();
        let oprcodes: UInt32Array = rows.iter().map(|r| operator_functions(r).2).collect();
        let oprrests: UInt32Array = rows.iter().map(|r| operator_functions(r).3).collect();
        let oprjoins: UInt32Array = rows.iter().map(|r| operator_functions(r).4).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(oprnames),
                Arc::new(oprnamespaces),
                Arc::new(oprowners),
                Arc::new(oprkinds),
                Arc::new(oprcanmerges),
                Arc::new(oprcanhashes),
                Arc::new(oprlefts),
                Arc::new(oprrights),
                Arc::new(oprresults),
                Arc::new(oprcoms),
                Arc::new(oprnegates),
                Arc::new(oprcodes),
                Arc::new(oprrests),
                Arc::new(oprjoins),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;
    use crate::{mock::MockCatalog, SystemView};

    fn col_u32(batch: &RecordBatch, name: &str) -> Vec<u32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_str(batch: &RecordBatch, name: &str) -> Vec<String> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect()
    }

    fn col_bool(batch: &RecordBatch, name: &str) -> Vec<bool> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .map(|b| b.unwrap())
            .collect()
    }

    fn row_for(batch: &RecordBatch, oid: u32) -> usize {
        col_u32(batch, "oid")
            .into_iter()
            .position(|o| o == oid)
            .unwrap_or_else(|| panic!("no pg_operator row for oid {oid}"))
    }

    #[test]
    fn name_is_pg_operator() {
        assert_eq!(PgOperator.name(), "pg_operator");
    }

    /// Pins the exact column layout (name, type, order, nullability) against
    /// live PostgreSQL 18.2's `pg_attribute` for `pg_operator`, so a future
    /// edit cannot silently reorder, rename, retype or flip nullability on a
    /// column. `oprcom`/`oprnegate`/`oprcode`/`oprrest`/`oprjoin` (real
    /// attnums 11-15) are deliberately absent — see the module docs.
    /// Every operator has an implementing function — `oprcode` is never `0`
    /// in a real server, by construction, so a `0` here means an oid is
    /// missing from [`OPERATOR_FUNCTIONS`] rather than that the operator
    /// genuinely has none. This is what makes that table's coverage
    /// self-checking without a live server.
    #[test]
    fn every_operator_has_an_implementing_function() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let oids = col_u32(&batch, "oid");
        for (i, code) in col_u32(&batch, "oprcode").into_iter().enumerate() {
            assert_ne!(code, 0, "oid {} has no OPERATOR_FUNCTIONS entry", oids[i]);
        }
    }

    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgOperator.schema();
        let got: Vec<(&str, DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("oid", DataType::UInt32, false),
                ("oprname", DataType::Utf8, false),
                ("oprnamespace", DataType::UInt32, false),
                ("oprowner", DataType::UInt32, false),
                ("oprkind", DataType::Utf8, false),
                ("oprcanmerge", DataType::Boolean, false),
                ("oprcanhash", DataType::Boolean, false),
                ("oprleft", DataType::UInt32, false),
                ("oprright", DataType::UInt32, false),
                ("oprresult", DataType::UInt32, false),
                ("oprcom", DataType::UInt32, false),
                ("oprnegate", DataType::UInt32, false),
                ("oprcode", DataType::UInt32, false),
                ("oprrest", DataType::UInt32, false),
                ("oprjoin", DataType::UInt32, false),
            ]
        );
    }

    /// `int4 = int4` (oid 96) is a binary operator: `oprkind = 'b'`, real
    /// `oprleft`/`oprright` both `int4` (23), `oprresult` bool (16).
    /// Confirmed live against PostgreSQL 18.
    #[test]
    fn binary_operator_reports_oprkind_b_and_both_operand_types() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 96);
        assert_eq!(col_str(&batch, "oprname")[i], "=");
        assert_eq!(col_str(&batch, "oprkind")[i], "b");
        assert_eq!(col_u32(&batch, "oprleft")[i], 23);
        assert_eq!(col_u32(&batch, "oprright")[i], 23);
        assert_eq!(col_u32(&batch, "oprresult")[i], 16);
    }

    /// Unary minus (oid 558) is a prefix operator: `oprkind = 'l'`, and real
    /// Postgres reports `oprleft = 0` (no left operand at all), not some
    /// placeholder type. Confirmed live.
    #[test]
    fn prefix_operator_reports_oprkind_l_and_zero_oprleft() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 558);
        assert_eq!(col_str(&batch, "oprname")[i], "-");
        assert_eq!(col_str(&batch, "oprkind")[i], "l");
        assert_eq!(col_u32(&batch, "oprleft")[i], 0);
        assert_eq!(col_u32(&batch, "oprright")[i], 23);
    }

    /// Every row is owned by the bootstrap superuser (oid 10) — confirmed
    /// live for every oid `OPERATORS` covers, per the module docs.
    #[test]
    fn every_row_is_owned_by_the_bootstrap_superuser() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        for owner in col_u32(&batch, "oprowner") {
            assert_eq!(owner, 10);
        }
    }

    /// `int4 = int4` (oid 96) is a same-type equality: merge- and
    /// hash-joinable. Confirmed live.
    #[test]
    fn same_type_equality_is_mergeable_and_hashable() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 96);
        assert!(col_bool(&batch, "oprcanmerge")[i]);
        assert!(col_bool(&batch, "oprcanhash")[i]);
    }

    /// `date = timestamp` (oid 2347) is merge-joinable but NOT
    /// hash-joinable — the one shape of `=` operator in this table's
    /// coverage that is not both. Confirmed live; see module docs for the
    /// full 6-oid exception set this pins.
    #[test]
    fn cross_type_date_timestamp_equality_is_mergeable_not_hashable() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 2347);
        assert!(col_bool(&batch, "oprcanmerge")[i]);
        assert!(!col_bool(&batch, "oprcanhash")[i]);
    }

    /// A non-`=` operator (`<`, oid 97) is neither merge- nor hash-joinable.
    /// Confirmed live.
    #[test]
    fn non_equality_operator_is_neither_mergeable_nor_hashable() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let i = row_for(&batch, 97);
        assert!(!col_bool(&batch, "oprcanmerge")[i]);
        assert!(!col_bool(&batch, "oprcanhash")[i]);
    }

    /// The entire point of this crate: a predicate on `oid` must actually
    /// narrow the row set to exactly one row, mirroring `pg_operator.oid`
    /// being a real primary key.
    #[test]
    fn pushed_oid_predicate_narrows_to_exactly_one_row() {
        let full = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        assert!(
            full.num_rows() > 1,
            "sanity: pg_operator has more than one row"
        );

        let filtered = PgOperator
            .scan(
                &MockCatalog::new(),
                &[Predicate::eq("oid", basin_pgtype::Oid(96))],
            )
            .unwrap();
        assert_eq!(filtered.num_rows(), 1);
        assert_eq!(col_str(&filtered, "oprname"), vec!["=".to_string()]);
    }

    /// The polymorphic array operators (`@>` on `anyarray`) share one real
    /// oid across their monomorphized instantiations in `OPERATORS`; this
    /// relation must report that oid exactly once, not once per
    /// instantiation, since `pg_operator.oid` is a primary key in real
    /// Postgres.
    #[test]
    fn polymorphic_array_operator_oid_appears_exactly_once() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        let count = col_u32(&batch, "oid")
            .into_iter()
            .filter(|&o| o == 2751)
            .count();
        assert_eq!(
            count, 1,
            "oid 2751 (@> on anyarray) must be deduplicated to one row"
        );
    }

    /// Every row lives in `pg_catalog` (namespace 11), confirmed live.
    #[test]
    fn every_row_is_in_pg_catalog_namespace() {
        let batch = PgOperator.scan(&MockCatalog::new(), &[]).unwrap();
        for ns in col_u32(&batch, "oprnamespace") {
            assert_eq!(ns, 11);
        }
    }

    /// A predicate matching nothing returns zero rows, not everything.
    #[test]
    fn pushed_predicate_matching_nothing_returns_empty() {
        let filtered = PgOperator
            .scan(
                &MockCatalog::new(),
                &[Predicate::eq("oid", basin_pgtype::Oid(999_999))],
            )
            .unwrap();
        assert_eq!(filtered.num_rows(), 0);
    }

    /// A predicate naming a column this relation does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgOperator
            .scan(&MockCatalog::new(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_operator",
                column: "nope".to_string(),
            }
        );
    }

    /// `IN` pushdown on `oprname` narrows to exactly the named operators.
    #[test]
    fn pushed_in_predicate_narrows_by_name() {
        let filtered = PgOperator
            .scan(
                &MockCatalog::new(),
                &[Predicate::in_list(
                    "oprname",
                    [Value::Text("~".to_string()), Value::Text("~*".to_string())],
                )],
            )
            .unwrap();
        let mut names = col_str(&filtered, "oprname");
        names.sort();
        assert_eq!(names, vec!["~".to_string(), "~*".to_string()]);
    }
}
