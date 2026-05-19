// fuzz_test — stdlib fuzz tests for critical parsing / serialisation
// boundaries. Each FuzzXxx function includes a seed corpus that covers
// known-valid and known-invalid inputs; the fuzzer then explores the
// surrounding space.
//
// Run as a normal test (seeds only):
//
//	go test -run=FuzzParseSemver ./...
//
// Run as a proper fuzz campaign:
//
//	go test -fuzz=FuzzParseSemver -fuzztime=30s ./...
//
// CI executes the seed-only path via `go test -race -count=1 ./...`.
package main

import (
	"bytes"
	"testing"
)

// FuzzParseSemver exercises parseSemver against the full range of
// string inputs. Invariants:
//  1. The function never panics.
//  2. On success (err == nil) the result round-trips through String()
//     and re-parses to an equivalent semver.
//  3. On error the returned semver is the zero value.
func FuzzParseSemver(f *testing.F) {
	// Valid seeds.
	f.Add("1.2.3")
	f.Add("0.0.0")
	f.Add("v1.0.0")
	f.Add("v0.1.0")
	f.Add("10.20.30")
	f.Add("1.2.3-rc.4")
	f.Add("1.2.3+build5")
	f.Add("v2.0.0-alpha+exp.sha.5114f85")
	// Malformed seeds.
	f.Add("")
	f.Add("1")
	f.Add("1.2")
	f.Add(".")
	f.Add("..")
	f.Add("a.b.c")
	f.Add("1.2.three")
	f.Add("-1.0.0")
	f.Add("1.-2.0")
	f.Add("999999999999999999999.0.0")
	f.Add("v")

	f.Fuzz(func(t *testing.T, input string) {
		sv, err := parseSemver(input)
		if err != nil {
			// On error the struct must be the zero value.
			if sv.Major != 0 || sv.Minor != 0 || sv.Patch != 0 {
				t.Fatalf("parseSemver(%q) returned non-zero semver on error: %+v", input, sv)
			}
			return
		}
		// On success, String() must produce something re-parseable to
		// an equivalent value.
		canonical := sv.String()
		sv2, err2 := parseSemver(canonical)
		if err2 != nil {
			t.Fatalf("parseSemver(%q).String()=%q failed to re-parse: %v", input, canonical, err2)
		}
		if sv != sv2 {
			t.Fatalf("round-trip mismatch for %q: first=%+v, second=%+v", input, sv, sv2)
		}
	})
}

// FuzzWorkingProjectTOMLRoundTrip exercises parseWorkingProjectTOML
// against arbitrary byte sequences. Invariants:
//  1. The function never panics.
//  2. When parsing succeeds, marshalWorkingProjectTOML(result)
//     produces bytes that re-parse to an equivalent workingProject.
func FuzzWorkingProjectTOMLRoundTrip(f *testing.F) {
	// Valid seeds — a few TOML-shaped key=value documents.
	f.Add([]byte("project_ref = myproject\ndefault_branch = main\n"))
	f.Add([]byte("project_ref = \"quoted-ref\"\n"))
	f.Add([]byte("project_ref = 'single-quoted'\nengine_version_pin = 0.1.0\n"))
	f.Add([]byte("# comment only\n\n"))
	f.Add([]byte(""))
	f.Add([]byte("default_branch = feature/my-branch\n"))
	f.Add([]byte("unknown_key = ignored_value\n"))
	f.Add([]byte("project_ref = a\ndefault_branch = b\nengine_version_pin = c\n"))
	// Malformed seeds — should either parse (best-effort) or return an error.
	f.Add([]byte("no-equals-sign\n"))
	f.Add([]byte("= no-key\n"))
	f.Add([]byte("key = value\nnext = "))
	f.Add([]byte("\x00\x01\x02"))
	f.Add([]byte("project_ref = a # inline comment\n"))

	f.Fuzz(func(t *testing.T, b []byte) {
		wp, err := parseWorkingProjectTOML(b)
		if err != nil {
			// Error is acceptable; just verify no panic occurred (we are
			// already here if it didn't).
			return
		}
		if wp == nil {
			// nil result with nil error is valid (empty / comment-only input).
			return
		}
		// Round-trip: marshal then re-parse, check equivalence.
		marshaled := marshalWorkingProjectTOML(*wp)
		wp2, err2 := parseWorkingProjectTOML([]byte(marshaled))
		if err2 != nil {
			t.Fatalf("re-parse of marshaled output failed: %v\nmarshaled=%q", err2, marshaled)
		}
		if wp2 == nil {
			t.Fatalf("re-parse of marshaled output returned nil (want non-nil)\nmarshaled=%q", marshaled)
		}
		if wp.ProjectRef != wp2.ProjectRef ||
			wp.DefaultBranch != wp2.DefaultBranch ||
			wp.EngineVersionPin != wp2.EngineVersionPin {
			t.Fatalf("round-trip mismatch:\n  original  = %+v\n  marshaled = %q\n  re-parsed = %+v",
				*wp, marshaled, *wp2)
		}
		_ = bytes.Equal([]byte(marshaled), []byte(marshaled)) // suppress unused import lint
	})
}

// FuzzMapTypeUnknown exercises MapType with arbitrary pg type names
// and verifies that://
//  1. MapType never panics, regardless of input.
//  2. Known types return (MappedType{...}, true).
//  3. Unknown types return (MappedType{}, false) — the zero value +
//     false sentinel — so callers can rely on the false branch for
//     graceful degradation.
func FuzzMapTypeUnknown(f *testing.F) {
	// Known-valid seeds.
	f.Add("text")
	f.Add("integer")
	f.Add("boolean")
	f.Add("uuid")
	f.Add("jsonb")
	f.Add("vector")
	f.Add("timestamp with time zone")
	f.Add("bigint")
	// Unknown / edge seeds.
	f.Add("")
	f.Add("unknown_type_xyz")
	f.Add("TEXT")
	f.Add("Integer")
	f.Add("VECTOR(1536)")
	f.Add("money")
	f.Add("xml")
	f.Add("\x00")
	f.Add("text[]")
	f.Add("int[]")

	// We test against all three language targets.
	langs := []LangTarget{LangTypeScript, LangGo, LangPython}

	f.Fuzz(func(t *testing.T, typeName string) {
		pg := PgType{Name: typeName}
		for _, lang := range langs {
			mt, ok := MapType(pg, lang)
			if !ok {
				// Must return the zero MappedType on miss.
				if mt.Type != "" || mt.Nullable != "" || mt.Import != "" {
					t.Fatalf("MapType(%q, %v) ok=false but returned non-zero MappedType: %+v", typeName, lang, mt)
				}
				// Lang field of the returned value is also zero on miss.
				if mt.Lang != 0 {
					t.Fatalf("MapType(%q, %v) ok=false but Lang=%v (want 0)", typeName, lang, mt.Lang)
				}
			} else {
				// On a hit, the returned Lang must match what we asked for.
				if mt.Lang != lang {
					t.Fatalf("MapType(%q, %v) returned Lang=%v (mismatch)", typeName, lang, mt.Lang)
				}
				// Type must be non-empty for every known mapping.
				if mt.Type == "" {
					t.Fatalf("MapType(%q, %v) returned empty Type on ok=true", typeName, lang)
				}
			}
		}

		// Zero LangTarget (invalid) must always return ok=false, no panic.
		mt, ok := MapType(pg, LangTarget(0))
		if ok {
			t.Fatalf("MapType(%q, 0) returned ok=true for zero LangTarget (want false)", typeName)
		}
		if mt.Type != "" {
			t.Fatalf("MapType(%q, 0) returned non-empty Type for zero LangTarget", typeName)
		}
	})
}
