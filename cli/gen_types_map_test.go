// gen_types_map_test — exhaustive table-driven tests for the type-mapping table.
//
// Every row in typeTable gets at least one test per language (TypeScript, Go,
// Python), covering both the non-null Type and the Nullable form. A final
// group of tests covers the ok=false unknown-type path so callers know the
// sentinel is exercised.
package main

import (
	"testing"
)

// mappingCase is one row of the test table.
type mappingCase struct {
	name     string
	pg       PgType
	lang     LangTarget
	wantType string
	wantNull string
	wantImport string
	wantOK   bool
}

func TestMapType(t *testing.T) {
	t.Parallel()

	cases := []mappingCase{
		// ── boolean ─────────────────────────────────────────────────────────
		{
			name: "boolean/TS",
			pg: PgType{Name: "boolean"}, lang: LangTypeScript,
			wantType: "boolean", wantNull: "boolean | null", wantImport: "", wantOK: true,
		},
		{
			name: "boolean/Go",
			pg: PgType{Name: "boolean"}, lang: LangGo,
			wantType: "bool", wantNull: "*bool", wantImport: "", wantOK: true,
		},
		{
			name: "boolean/Python",
			pg: PgType{Name: "boolean"}, lang: LangPython,
			wantType: "bool", wantNull: "Optional[bool]", wantImport: "from typing import Optional", wantOK: true,
		},
		// bool alias
		{
			name: "bool/TS",
			pg: PgType{Name: "bool"}, lang: LangTypeScript,
			wantType: "boolean", wantNull: "boolean | null", wantImport: "", wantOK: true,
		},
		{
			name: "bool/Go",
			pg: PgType{Name: "bool"}, lang: LangGo,
			wantType: "bool", wantNull: "*bool", wantImport: "", wantOK: true,
		},
		{
			name: "bool/Python",
			pg: PgType{Name: "bool"}, lang: LangPython,
			wantType: "bool", wantNull: "Optional[bool]", wantImport: "from typing import Optional", wantOK: true,
		},

		// ── smallint / int2 ──────────────────────────────────────────────────
		{
			name: "smallint/TS",
			pg: PgType{Name: "smallint"}, lang: LangTypeScript,
			wantType: "number", wantNull: "number | null", wantImport: "", wantOK: true,
		},
		{
			name: "smallint/Go",
			pg: PgType{Name: "smallint"}, lang: LangGo,
			wantType: "int16", wantNull: "*int16", wantImport: "", wantOK: true,
		},
		{
			name: "smallint/Python",
			pg: PgType{Name: "smallint"}, lang: LangPython,
			wantType: "int", wantNull: "Optional[int]", wantImport: "from typing import Optional", wantOK: true,
		},
		{
			name: "int2/Go",
			pg: PgType{Name: "int2"}, lang: LangGo,
			wantType: "int16", wantNull: "*int16", wantImport: "", wantOK: true,
		},

		// ── integer / int4 ───────────────────────────────────────────────────
		{
			name: "integer/TS",
			pg: PgType{Name: "integer"}, lang: LangTypeScript,
			wantType: "number", wantNull: "number | null", wantImport: "", wantOK: true,
		},
		{
			name: "integer/Go",
			pg: PgType{Name: "integer"}, lang: LangGo,
			wantType: "int32", wantNull: "*int32", wantImport: "", wantOK: true,
		},
		{
			name: "integer/Python",
			pg: PgType{Name: "integer"}, lang: LangPython,
			wantType: "int", wantNull: "Optional[int]", wantImport: "from typing import Optional", wantOK: true,
		},
		{
			name: "int4/Go",
			pg: PgType{Name: "int4"}, lang: LangGo,
			wantType: "int32", wantNull: "*int32", wantImport: "", wantOK: true,
		},

		// ── bigint / int8 ────────────────────────────────────────────────────
		{
			name: "bigint/TS",
			pg: PgType{Name: "bigint"}, lang: LangTypeScript,
			wantType: "bigint", wantNull: "bigint | null", wantImport: "", wantOK: true,
		},
		{
			name: "bigint/Go",
			pg: PgType{Name: "bigint"}, lang: LangGo,
			wantType: "int64", wantNull: "*int64", wantImport: "", wantOK: true,
		},
		{
			name: "bigint/Python",
			pg: PgType{Name: "bigint"}, lang: LangPython,
			wantType: "int", wantNull: "Optional[int]", wantImport: "from typing import Optional", wantOK: true,
		},
		{
			name: "int8/Go",
			pg: PgType{Name: "int8"}, lang: LangGo,
			wantType: "int64", wantNull: "*int64", wantImport: "", wantOK: true,
		},

		// ── real / float4 ────────────────────────────────────────────────────
		{
			name: "real/TS",
			pg: PgType{Name: "real"}, lang: LangTypeScript,
			wantType: "number", wantNull: "number | null", wantImport: "", wantOK: true,
		},
		{
			name: "real/Go",
			pg: PgType{Name: "real"}, lang: LangGo,
			wantType: "float32", wantNull: "*float32", wantImport: "", wantOK: true,
		},
		{
			name: "real/Python",
			pg: PgType{Name: "real"}, lang: LangPython,
			wantType: "float", wantNull: "Optional[float]", wantImport: "from typing import Optional", wantOK: true,
		},
		{
			name: "float4/Go",
			pg: PgType{Name: "float4"}, lang: LangGo,
			wantType: "float32", wantNull: "*float32", wantImport: "", wantOK: true,
		},

		// ── double precision / float8 ─────────────────────────────────────────
		{
			name: "double precision/TS",
			pg: PgType{Name: "double precision"}, lang: LangTypeScript,
			wantType: "number", wantNull: "number | null", wantImport: "", wantOK: true,
		},
		{
			name: "double precision/Go",
			pg: PgType{Name: "double precision"}, lang: LangGo,
			wantType: "float64", wantNull: "*float64", wantImport: "", wantOK: true,
		},
		{
			name: "double precision/Python",
			pg: PgType{Name: "double precision"}, lang: LangPython,
			wantType: "float", wantNull: "Optional[float]", wantImport: "from typing import Optional", wantOK: true,
		},
		{
			name: "float8/Go",
			pg: PgType{Name: "float8"}, lang: LangGo,
			wantType: "float64", wantNull: "*float64", wantImport: "", wantOK: true,
		},

		// ── numeric / decimal ────────────────────────────────────────────────
		{
			name: "numeric/TS",
			pg: PgType{Name: "numeric"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "numeric/Go",
			pg: PgType{Name: "numeric"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},
		{
			name: "numeric/Python",
			pg: PgType{Name: "numeric"}, lang: LangPython,
			wantType: "Decimal", wantNull: "Optional[Decimal]",
			wantImport: "from decimal import Decimal\nfrom typing import Optional", wantOK: true,
		},
		{
			name: "decimal/Python",
			pg: PgType{Name: "decimal"}, lang: LangPython,
			wantType: "Decimal", wantNull: "Optional[Decimal]",
			wantImport: "from decimal import Decimal\nfrom typing import Optional", wantOK: true,
		},

		// ── text ─────────────────────────────────────────────────────────────
		{
			name: "text/TS",
			pg: PgType{Name: "text"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "text/Go",
			pg: PgType{Name: "text"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},
		{
			name: "text/Python",
			pg: PgType{Name: "text"}, lang: LangPython,
			wantType: "str", wantNull: "Optional[str]", wantImport: "from typing import Optional", wantOK: true,
		},

		// character varying / varchar / character / char
		{
			name: "character varying/TS",
			pg: PgType{Name: "character varying"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "character varying/Go",
			pg: PgType{Name: "character varying"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},
		{
			name: "character varying/Python",
			pg: PgType{Name: "character varying"}, lang: LangPython,
			wantType: "str", wantNull: "Optional[str]", wantImport: "from typing import Optional", wantOK: true,
		},
		{
			name: "varchar/Go",
			pg: PgType{Name: "varchar"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},
		{
			name: "character/Go",
			pg: PgType{Name: "character"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},
		{
			name: "char/Go",
			pg: PgType{Name: "char"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},

		// ── bytea ─────────────────────────────────────────────────────────────
		{
			name: "bytea/TS",
			pg: PgType{Name: "bytea"}, lang: LangTypeScript,
			wantType: "Uint8Array", wantNull: "Uint8Array | null", wantImport: "", wantOK: true,
		},
		{
			name: "bytea/Go",
			pg: PgType{Name: "bytea"}, lang: LangGo,
			wantType: "[]byte", wantNull: "[]byte", wantImport: "", wantOK: true,
		},
		{
			name: "bytea/Python",
			pg: PgType{Name: "bytea"}, lang: LangPython,
			wantType: "bytes", wantNull: "Optional[bytes]", wantImport: "from typing import Optional", wantOK: true,
		},

		// ── uuid ─────────────────────────────────────────────────────────────
		{
			name: "uuid/TS",
			pg: PgType{Name: "uuid"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "uuid/Go",
			pg: PgType{Name: "uuid"}, lang: LangGo,
			wantType: "[16]byte", wantNull: "*[16]byte", wantImport: "", wantOK: true,
		},
		{
			name: "uuid/Python",
			pg: PgType{Name: "uuid"}, lang: LangPython,
			wantType: "UUID", wantNull: "Optional[UUID]",
			wantImport: "from uuid import UUID\nfrom typing import Optional", wantOK: true,
		},

		// ── jsonb ─────────────────────────────────────────────────────────────
		{
			name: "jsonb/TS",
			pg: PgType{Name: "jsonb"}, lang: LangTypeScript,
			wantType: "Record<string, unknown>", wantNull: "Record<string, unknown> | null", wantImport: "", wantOK: true,
		},
		{
			name: "jsonb/Go",
			pg: PgType{Name: "jsonb"}, lang: LangGo,
			wantType: "json.RawMessage", wantNull: "json.RawMessage", wantImport: "encoding/json", wantOK: true,
		},
		{
			name: "jsonb/Python",
			pg: PgType{Name: "jsonb"}, lang: LangPython,
			wantType: "Any", wantNull: "Optional[Any]", wantImport: "from typing import Any, Optional", wantOK: true,
		},

		// ── json ──────────────────────────────────────────────────────────────
		{
			name: "json/TS",
			pg: PgType{Name: "json"}, lang: LangTypeScript,
			wantType: "Record<string, unknown>", wantNull: "Record<string, unknown> | null", wantImport: "", wantOK: true,
		},
		{
			name: "json/Go",
			pg: PgType{Name: "json"}, lang: LangGo,
			wantType: "json.RawMessage", wantNull: "json.RawMessage", wantImport: "encoding/json", wantOK: true,
		},
		{
			name: "json/Python",
			pg: PgType{Name: "json"}, lang: LangPython,
			wantType: "Any", wantNull: "Optional[Any]", wantImport: "from typing import Any, Optional", wantOK: true,
		},

		// ── timestamp with time zone ──────────────────────────────────────────
		{
			name: "timestamp with time zone/TS",
			pg: PgType{Name: "timestamp with time zone"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "timestamp with time zone/Go",
			pg: PgType{Name: "timestamp with time zone"}, lang: LangGo,
			wantType: "time.Time", wantNull: "*time.Time", wantImport: "time", wantOK: true,
		},
		{
			name: "timestamp with time zone/Python",
			pg: PgType{Name: "timestamp with time zone"}, lang: LangPython,
			wantType: "datetime", wantNull: "Optional[datetime]",
			wantImport: "from datetime import datetime\nfrom typing import Optional", wantOK: true,
		},
		{
			name: "timestamptz/Go",
			pg: PgType{Name: "timestamptz"}, lang: LangGo,
			wantType: "time.Time", wantNull: "*time.Time", wantImport: "time", wantOK: true,
		},

		// ── timestamp without time zone ───────────────────────────────────────
		{
			name: "timestamp without time zone/TS",
			pg: PgType{Name: "timestamp without time zone"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "timestamp without time zone/Go",
			pg: PgType{Name: "timestamp without time zone"}, lang: LangGo,
			wantType: "time.Time", wantNull: "*time.Time", wantImport: "time", wantOK: true,
		},
		{
			name: "timestamp without time zone/Python",
			pg: PgType{Name: "timestamp without time zone"}, lang: LangPython,
			wantType: "datetime", wantNull: "Optional[datetime]",
			wantImport: "from datetime import datetime\nfrom typing import Optional", wantOK: true,
		},
		{
			name: "timestamp bare/Go",
			pg: PgType{Name: "timestamp"}, lang: LangGo,
			wantType: "time.Time", wantNull: "*time.Time", wantImport: "time", wantOK: true,
		},

		// ── date ──────────────────────────────────────────────────────────────
		{
			name: "date/TS",
			pg: PgType{Name: "date"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "date/Go",
			pg: PgType{Name: "date"}, lang: LangGo,
			wantType: "time.Time", wantNull: "*time.Time", wantImport: "time", wantOK: true,
		},
		{
			name: "date/Python",
			pg: PgType{Name: "date"}, lang: LangPython,
			wantType: "date", wantNull: "Optional[date]",
			wantImport: "from datetime import date\nfrom typing import Optional", wantOK: true,
		},

		// ── time without time zone ────────────────────────────────────────────
		{
			name: "time without time zone/TS",
			pg: PgType{Name: "time without time zone"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "time without time zone/Go",
			pg: PgType{Name: "time without time zone"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},
		{
			name: "time without time zone/Python",
			pg: PgType{Name: "time without time zone"}, lang: LangPython,
			wantType: "time", wantNull: "Optional[time]",
			wantImport: "from datetime import time\nfrom typing import Optional", wantOK: true,
		},
		{
			name: "time with time zone/Go",
			pg: PgType{Name: "time with time zone"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},
		{
			name: "time bare/Go",
			pg: PgType{Name: "time"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},

		// ── interval ─────────────────────────────────────────────────────────
		{
			name: "interval/TS",
			pg: PgType{Name: "interval"}, lang: LangTypeScript,
			wantType: "string", wantNull: "string | null", wantImport: "", wantOK: true,
		},
		{
			name: "interval/Go",
			pg: PgType{Name: "interval"}, lang: LangGo,
			wantType: "string", wantNull: "*string", wantImport: "", wantOK: true,
		},
		{
			name: "interval/Python",
			pg: PgType{Name: "interval"}, lang: LangPython,
			wantType: "str", wantNull: "Optional[str]", wantImport: "from typing import Optional", wantOK: true,
		},

		// ── vector ───────────────────────────────────────────────────────────
		{
			name: "vector/TS",
			pg: PgType{Name: "vector"}, lang: LangTypeScript,
			wantType: "number[]", wantNull: "number[] | null", wantImport: "", wantOK: true,
		},
		{
			name: "vector/Go",
			pg: PgType{Name: "vector"}, lang: LangGo,
			wantType: "[]float32", wantNull: "[]float32", wantImport: "", wantOK: true,
		},
		{
			name: "vector/Python",
			pg: PgType{Name: "vector"}, lang: LangPython,
			wantType: "list[float]", wantNull: "Optional[list[float]]", wantImport: "from typing import Optional", wantOK: true,
		},

		// ── unknown-type path (ok=false) ──────────────────────────────────────
		// These are types the engine does not support (🚫) or types outside
		// the mapping table. Callers must receive ok=false and fall back safely.
		{
			name: "money/TS — 🚫 engine type",
			pg: PgType{Name: "money"}, lang: LangTypeScript,
			wantOK: false,
		},
		{
			name: "xml/Go — 🚫 engine type",
			pg: PgType{Name: "xml"}, lang: LangGo,
			wantOK: false,
		},
		{
			name: "point/Python — 🛠 sql surface deferred",
			pg: PgType{Name: "point"}, lang: LangPython,
			wantOK: false,
		},
		{
			name: "unknown type entirely",
			pg: PgType{Name: "not_a_real_pg_type"}, lang: LangTypeScript,
			wantOK: false,
		},
		{
			name: "empty string",
			pg: PgType{Name: ""}, lang: LangGo,
			wantOK: false,
		},
		{
			name: "zero LangTarget — invalid sentinel",
			pg: PgType{Name: "text"}, lang: LangTarget(0),
			wantOK: false,
		},
		{
			name: "unknown LangTarget value",
			pg: PgType{Name: "text"}, lang: LangTarget(99),
			wantOK: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := MapType(tc.pg, tc.lang)

			if ok != tc.wantOK {
				t.Fatalf("MapType(%q, %v) ok=%v, want %v", tc.pg.Name, tc.lang, ok, tc.wantOK)
			}
			if !ok {
				// No further assertions when we expect a miss.
				return
			}

			if got.Type != tc.wantType {
				t.Errorf("Type = %q, want %q", got.Type, tc.wantType)
			}
			if got.Nullable != tc.wantNull {
				t.Errorf("Nullable = %q, want %q", got.Nullable, tc.wantNull)
			}
			if got.Import != tc.wantImport {
				t.Errorf("Import = %q, want %q", got.Import, tc.wantImport)
			}
			if got.Lang != tc.lang {
				t.Errorf("Lang = %v, want %v", got.Lang, tc.lang)
			}
		})
	}
}

// TestMapTypeTableCompleteness verifies that every key in typeTable is reachable
// via MapType for all three language targets, so a typo in typeTable can't
// produce a row that's silently dead.
func TestMapTypeTableCompleteness(t *testing.T) {
	t.Parallel()

	langs := []LangTarget{LangTypeScript, LangGo, LangPython}

	for pgName := range typeTable {
		pgName := pgName
		for _, lang := range langs {
			lang := lang
			t.Run(pgName+"/"+langName(lang), func(t *testing.T) {
				t.Parallel()

				pg := PgType{Name: pgName}
				m, ok := MapType(pg, lang)
				if !ok {
					t.Fatalf("MapType(%q, %v) returned ok=false but key exists in typeTable", pgName, lang)
				}
				if m.Type == "" {
					t.Errorf("MapType(%q, %v).Type is empty string", pgName, lang)
				}
				if m.Nullable == "" {
					t.Errorf("MapType(%q, %v).Nullable is empty string", pgName, lang)
				}
				if m.Lang != lang {
					t.Errorf("MapType(%q, %v).Lang = %v, want %v", pgName, lang, m.Lang, lang)
				}
			})
		}
	}
}

// TestLangTargetConstValues ensures the const block is a strict iota+1 sequence
// so no two constants share the same value and the zero value stays invalid.
func TestLangTargetConstValues(t *testing.T) {
	t.Parallel()

	if LangTypeScript == LangGo || LangGo == LangPython || LangTypeScript == LangPython {
		t.Fatal("two LangTarget constants share the same value")
	}
	if LangTypeScript == 0 || LangGo == 0 || LangPython == 0 {
		t.Fatal("a LangTarget constant is zero — zero must remain the invalid sentinel")
	}
}

// langName is a helper for sub-test names only; it does not need to be exported.
func langName(l LangTarget) string {
	switch l {
	case LangTypeScript:
		return "typescript"
	case LangGo:
		return "go"
	case LangPython:
		return "python"
	default:
		return "unknown"
	}
}
