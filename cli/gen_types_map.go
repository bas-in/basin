// gen_types_map — pure lookup table: Postgres column type → target-language type.
//
// This file is the single source of truth for every pg→lang mapping consumed
// by `gen types typescript|go|python`. It is intentionally a leaf: it imports
// nothing from this repository. The dispatcher (`cmd_gen.go`) and each
// language emitter import this package and plug straight in.
//
// Type system reference: ../basin/CAPABILITIES.md §"Types"
// Only types whose status is ✅ in that table are included. Types marked 🚫
// or ◻️ are intentionally absent — emitting a mapping for an unsupported
// engine type would give users false confidence that their schema is portable.
//
// Omissions (with reason):
//   - MONEY      — 🚫 in CAPABILITIES.md; engine rejects the column type.
//   - XML        — 🚫 in CAPABILITIES.md.
//   - INTERVAL   — 🚫 in CAPABILITIES.md (listed alongside MONEY/XML as not
//                  on roadmap). Included conservatively below because
//                  information_schema may surface it from older snapshots;
//                  callers that encounter it get the mapping, callers that
//                  don't never hit it.
//   - POINT      — 🛠 (SQL surface deferred to v0.2); excluded to avoid
//                  mapping an unsupported DDL type.
//   - array types — CREATE TABLE column type `TEXT[]` / `INT[]` not yet
//                  supported in the arrow schema bridge (CAPABILITIES.md).
//   - ENUM       — stored as Utf8 + BASIN_ENUM_TYPE metadata; information_schema
//                  surfaces the underlying base type. Callers introspecting
//                  enums should query pg_catalog.pg_type separately; no row
//                  here avoids silently flattening enum to plain string.
package main

// LangTarget identifies the output language for a type-generation pass.
// The zero value (0) is intentionally invalid so accidental zero-value
// usage produces an ok=false from MapType rather than a silent wrong mapping.
type LangTarget int

const (
	// LangTypeScript targets TypeScript; nullable form is `T | null`,
	// no import statements are needed for primitives, json.RawMessage
	// surfaces as `Record<string, unknown>`.
	LangTypeScript LangTarget = iota + 1

	// LangGo targets Go; nullable form is `*T`, JSONB columns use
	// `json.RawMessage` (import "encoding/json" required), UUID columns
	// use `[16]byte` (no extra import).
	LangGo

	// LangPython targets Python + Pydantic; nullable form is
	// `Optional[T]` (import `from typing import Optional` required),
	// JSONB columns use `Any` (import `from typing import Any`).
	LangPython
)

// PgType is the canonical Postgres column type as returned by
// information_schema.columns.data_type. The Name field holds the
// lower-cased value from that view (e.g. "boolean", "integer",
// "timestamp without time zone"). OID is the Postgres type OID — carried
// for diagnostics and future binary-protocol work; MapType matches on Name,
// not OID, because information_schema always returns text.
type PgType struct {
	Name string
	OID  int
}

// MappedType is the result of a MapType lookup for one (PgType, LangTarget)
// pair.
//
//   - Type     — the non-nullable target type (e.g. "string", "int64",
//                "str"). Used when the column is NOT NULL.
//   - Nullable — the nullable form (e.g. "string | null", "*int64",
//                "Optional[str]"). Used when the column is nullable.
//   - Import   — an optional import statement to prepend to the generated
//                file. Empty for primitives that need no import. Multiple
//                imports for a single file are deduplicated by the emitter.
type MappedType struct {
	Lang     LangTarget
	Type     string
	Nullable string
	Import   string
}

// pgRow holds the three per-language mappings for a single Postgres type.
// All three fields are populated together so the table stays readable as
// a horizontal comparison.
type pgRow struct {
	ts  MappedType // TypeScript
	go_ MappedType // Go
	py  MappedType // Python
}

// typeTable maps every information_schema.columns.data_type value that the
// Basin engine supports (✅ in CAPABILITIES.md) to its three language targets.
//
// Keys are lower-cased data_type strings exactly as information_schema returns
// them. Aliases (e.g. "int8" vs "bigint") share the same pgRow by pointing to
// separate keys with identical values — the extra memory is negligible and
// keeps the lookup O(1) without a normalisation step in hot paths.
//
// vector(N) is stored under the bare key "vector"; the dimension suffix is
// handled by the caller (it strips the "(N)" before the lookup, then
// re-attaches it as a comment in the emitted file).
var typeTable = map[string]pgRow{
	// ── Boolean ─────────────────────────────────────────────────────────────
	"boolean": {
		ts:  MappedType{Lang: LangTypeScript, Type: "boolean", Nullable: "boolean | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "bool", Nullable: "*bool", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "bool", Nullable: "Optional[bool]", Import: "from typing import Optional"},
	},
	"bool": { // pgwire / pg_catalog alias
		ts:  MappedType{Lang: LangTypeScript, Type: "boolean", Nullable: "boolean | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "bool", Nullable: "*bool", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "bool", Nullable: "Optional[bool]", Import: "from typing import Optional"},
	},

	// ── Integer family ───────────────────────────────────────────────────────
	// information_schema returns "smallint" for int2, "integer" for int4,
	// "bigint" for int8. The short aliases (int2/int4/int8) appear in
	// pg_catalog.pg_attribute.atttypid paths and in some older information_schema
	// implementations; we carry them for robustness.
	"smallint": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number", Nullable: "number | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "int16", Nullable: "*int16", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "int", Nullable: "Optional[int]", Import: "from typing import Optional"},
	},
	"int2": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number", Nullable: "number | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "int16", Nullable: "*int16", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "int", Nullable: "Optional[int]", Import: "from typing import Optional"},
	},
	"integer": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number", Nullable: "number | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "int32", Nullable: "*int32", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "int", Nullable: "Optional[int]", Import: "from typing import Optional"},
	},
	"int4": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number", Nullable: "number | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "int32", Nullable: "*int32", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "int", Nullable: "Optional[int]", Import: "from typing import Optional"},
	},
	"bigint": {
		// TypeScript: bigint literal is safe for int8 values; callers that
		// need JSON serialisation may prefer string — document in gen types help.
		ts:  MappedType{Lang: LangTypeScript, Type: "bigint", Nullable: "bigint | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "int64", Nullable: "*int64", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "int", Nullable: "Optional[int]", Import: "from typing import Optional"},
	},
	"int8": {
		ts:  MappedType{Lang: LangTypeScript, Type: "bigint", Nullable: "bigint | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "int64", Nullable: "*int64", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "int", Nullable: "Optional[int]", Import: "from typing import Optional"},
	},

	// ── Floating-point family ─────────────────────────────────────────────────
	"real": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number", Nullable: "number | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "float32", Nullable: "*float32", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "float", Nullable: "Optional[float]", Import: "from typing import Optional"},
	},
	"float4": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number", Nullable: "number | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "float32", Nullable: "*float32", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "float", Nullable: "Optional[float]", Import: "from typing import Optional"},
	},
	"double precision": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number", Nullable: "number | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "float64", Nullable: "*float64", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "float", Nullable: "Optional[float]", Import: "from typing import Optional"},
	},
	"float8": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number", Nullable: "number | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "float64", Nullable: "*float64", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "float", Nullable: "Optional[float]", Import: "from typing import Optional"},
	},

	// ── Exact numeric ─────────────────────────────────────────────────────────
	// information_schema returns "numeric" for both NUMERIC and DECIMAL
	// (DECIMAL is a synonym). We also carry the "decimal" key for drivers
	// that surface it directly.
	"numeric": {
		// TypeScript: string is the safe representation for arbitrary-precision
		// numerics; callers doing math should parseFloat, documented in --help.
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "Decimal", Nullable: "Optional[Decimal]", Import: "from decimal import Decimal\nfrom typing import Optional"},
	},
	"decimal": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "Decimal", Nullable: "Optional[Decimal]", Import: "from decimal import Decimal\nfrom typing import Optional"},
	},

	// ── Text family ───────────────────────────────────────────────────────────
	// information_schema returns "character varying" for VARCHAR, "character"
	// for CHAR(n), and "text" for TEXT. All three map to string.
	"text": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "str", Nullable: "Optional[str]", Import: "from typing import Optional"},
	},
	"character varying": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "str", Nullable: "Optional[str]", Import: "from typing import Optional"},
	},
	"varchar": { // pg_catalog alias
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "str", Nullable: "Optional[str]", Import: "from typing import Optional"},
	},
	"character": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "str", Nullable: "Optional[str]", Import: "from typing import Optional"},
	},
	"char": { // pg_catalog alias for character(1)
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "str", Nullable: "Optional[str]", Import: "from typing import Optional"},
	},

	// ── Binary ───────────────────────────────────────────────────────────────
	// BYTEA: information_schema returns "bytea". Arrow LargeBinary on the wire.
	"bytea": {
		ts:  MappedType{Lang: LangTypeScript, Type: "Uint8Array", Nullable: "Uint8Array | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "[]byte", Nullable: "[]byte", Import: ""},
		// Go: []byte is already a reference type; its zero value is nil,
		// which JSON-marshals as null — so the nullable form is identical.
		py: MappedType{Lang: LangPython, Type: "bytes", Nullable: "Optional[bytes]", Import: "from typing import Optional"},
	},

	// ── UUID ─────────────────────────────────────────────────────────────────
	// CAPABILITIES.md: Arrow FixedSizeBinary(16) + BASIN_TYPE=UUID metadata;
	// pgwire OID 2950. information_schema returns "uuid".
	"uuid": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "[16]byte", Nullable: "*[16]byte", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "UUID", Nullable: "Optional[UUID]", Import: "from uuid import UUID\nfrom typing import Optional"},
	},

	// ── JSON / JSONB ─────────────────────────────────────────────────────────
	// CAPABILITIES.md: JSONB is Arrow LargeBinary + BASIN_TYPE=JSONB; OID 3802.
	// JSON and JSONB share the same mapping — the difference is engine-side
	// validation and indexing, not the wire shape.
	"jsonb": {
		ts:  MappedType{Lang: LangTypeScript, Type: "Record<string, unknown>", Nullable: "Record<string, unknown> | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "json.RawMessage", Nullable: "json.RawMessage", Import: "encoding/json"},
		// Go: json.RawMessage is []byte; nil == null in JSON, so nullable
		// form is identical to non-null form.
		py: MappedType{Lang: LangPython, Type: "Any", Nullable: "Optional[Any]", Import: "from typing import Any, Optional"},
	},
	"json": {
		ts:  MappedType{Lang: LangTypeScript, Type: "Record<string, unknown>", Nullable: "Record<string, unknown> | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "json.RawMessage", Nullable: "json.RawMessage", Import: "encoding/json"},
		py:  MappedType{Lang: LangPython, Type: "Any", Nullable: "Optional[Any]", Import: "from typing import Any, Optional"},
	},

	// ── Timestamps ───────────────────────────────────────────────────────────
	// CAPABILITIES.md: TIMESTAMPTZ = Arrow Timestamp(Microsecond, "UTC").
	// information_schema returns "timestamp with time zone" for TIMESTAMPTZ
	// and "timestamp without time zone" for TIMESTAMP.
	"timestamp with time zone": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "time.Time", Nullable: "*time.Time", Import: "time"},
		py:  MappedType{Lang: LangPython, Type: "datetime", Nullable: "Optional[datetime]", Import: "from datetime import datetime\nfrom typing import Optional"},
	},
	"timestamptz": { // pg_catalog alias
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "time.Time", Nullable: "*time.Time", Import: "time"},
		py:  MappedType{Lang: LangPython, Type: "datetime", Nullable: "Optional[datetime]", Import: "from datetime import datetime\nfrom typing import Optional"},
	},
	"timestamp without time zone": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "time.Time", Nullable: "*time.Time", Import: "time"},
		py:  MappedType{Lang: LangPython, Type: "datetime", Nullable: "Optional[datetime]", Import: "from datetime import datetime\nfrom typing import Optional"},
	},
	"timestamp": { // bare alias used in some drivers
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "time.Time", Nullable: "*time.Time", Import: "time"},
		py:  MappedType{Lang: LangPython, Type: "datetime", Nullable: "Optional[datetime]", Import: "from datetime import datetime\nfrom typing import Optional"},
	},

	// ── Date ─────────────────────────────────────────────────────────────────
	// information_schema returns "date".
	"date": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "time.Time", Nullable: "*time.Time", Import: "time"},
		py:  MappedType{Lang: LangPython, Type: "date", Nullable: "Optional[date]", Import: "from datetime import date\nfrom typing import Optional"},
	},

	// ── Time ─────────────────────────────────────────────────────────────────
	// information_schema returns "time without time zone" for TIME,
	// "time with time zone" for TIMETZ (rarely used).
	"time without time zone": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "time", Nullable: "Optional[time]", Import: "from datetime import time\nfrom typing import Optional"},
	},
	"time with time zone": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "time", Nullable: "Optional[time]", Import: "from datetime import time\nfrom typing import Optional"},
	},
	"time": { // bare alias
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "time", Nullable: "Optional[time]", Import: "from datetime import time\nfrom typing import Optional"},
	},

	// ── Interval ─────────────────────────────────────────────────────────────
	// Listed as 🚫 in CAPABILITIES.md alongside MONEY/XML, but
	// information_schema may surface it from snapshots or future engine builds.
	// We map conservatively to string so callers don't get a missing-key error
	// on a schema that includes an interval column from an older snapshot.
	"interval": {
		ts:  MappedType{Lang: LangTypeScript, Type: "string", Nullable: "string | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "string", Nullable: "*string", Import: ""},
		py:  MappedType{Lang: LangPython, Type: "str", Nullable: "Optional[str]", Import: "from typing import Optional"},
	},

	// ── Vector ───────────────────────────────────────────────────────────────
	// CAPABILITIES.md: Arrow FixedSizeList<Float32>. information_schema
	// returns "vector" (bare, without the dimension suffix "(N)"); the
	// dimension is carried separately in character_maximum_length or via a
	// look-aside to pg_attribute. The caller strips the "(N)" suffix before
	// calling MapType and re-emits it as a comment in the output.
	//
	// TypeScript: number[] (a typed float array; the dimension is a comment).
	// Go: []float32 (dimension enforced only at the DB layer).
	// Python: list[float] (dimension enforced only at the DB layer).
	"vector": {
		ts:  MappedType{Lang: LangTypeScript, Type: "number[]", Nullable: "number[] | null", Import: ""},
		go_: MappedType{Lang: LangGo, Type: "[]float32", Nullable: "[]float32", Import: ""},
		// Go: slice is already a reference type; nil == null in JSON.
		py: MappedType{Lang: LangPython, Type: "list[float]", Nullable: "Optional[list[float]]", Import: "from typing import Optional"},
	},
}

// MapType returns the MappedType for (pg, lang). ok is false when the Postgres
// type name is not in the table — callers should fall back to a "string" /
// "any" / "Any" safe default and emit a warning comment in the generated file
// rather than aborting code generation entirely.
//
// The pg.Name lookup is case-sensitive and expects the lower-cased value that
// information_schema.columns.data_type returns. Callers should strings.ToLower
// the data_type field before calling MapType.
func MapType(pg PgType, lang LangTarget) (MappedType, bool) {
	row, ok := typeTable[pg.Name]
	if !ok {
		return MappedType{}, false
	}
	switch lang {
	case LangTypeScript:
		return row.ts, true
	case LangGo:
		return row.go_, true
	case LangPython:
		return row.py, true
	default:
		return MappedType{}, false
	}
}
