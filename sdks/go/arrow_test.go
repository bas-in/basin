package basin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// IPC fixture builder
// ---------------------------------------------------------------------------

// buildArrowFixture returns a minimal Arrow IPC stream with one RecordBatch
// containing the given rows. Schema: {id: int64, name: utf8}.
func buildArrowFixture(t *testing.T, rows []struct{ ID int64; Name string }) []byte {
	t.Helper()
	alloc := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	idBldr := array.NewInt64Builder(alloc)
	nameBldr := array.NewStringBuilder(alloc)
	defer idBldr.Release()
	defer nameBldr.Release()

	for _, r := range rows {
		idBldr.Append(r.ID)
		nameBldr.Append(r.Name)
	}
	idCol := idBldr.NewArray()
	nameCol := nameBldr.NewArray()
	defer idCol.Release()
	defer nameCol.Release()

	rec := array.NewRecord(schema, []arrow.Array{idCol, nameCol}, int64(len(rows)))
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc write record: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc write close: %v", err)
	}
	return buf.Bytes()
}

// ---------------------------------------------------------------------------
// decodeArrowIPC
// ---------------------------------------------------------------------------

func TestDecodeArrowIPC(t *testing.T) {
	fixture := buildArrowFixture(t, []struct{ ID int64; Name string }{
		{1, "Alice"},
		{2, "Bob"},
	})

	records, err := decodeArrowIPC(fixture)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	rec := records[0]
	if rec.NumRows() != 2 {
		t.Errorf("NumRows: got %d want 2", rec.NumRows())
	}
	if rec.NumCols() != 2 {
		t.Errorf("NumCols: got %d want 2", rec.NumCols())
	}
	// Verify schema field names.
	if rec.Schema().Field(0).Name != "id" {
		t.Errorf("Field(0).Name: %q", rec.Schema().Field(0).Name)
	}
	if rec.Schema().Field(1).Name != "name" {
		t.Errorf("Field(1).Name: %q", rec.Schema().Field(1).Name)
	}
	// Verify data.
	ids := rec.Column(0).(*array.Int64)
	if ids.Value(0) != 1 || ids.Value(1) != 2 {
		t.Errorf("id values: %d, %d", ids.Value(0), ids.Value(1))
	}
	names := rec.Column(1).(*array.String)
	if names.Value(0) != "Alice" || names.Value(1) != "Bob" {
		t.Errorf("name values: %q, %q", names.Value(0), names.Value(1))
	}
}

func TestDecodeArrowIPCEmpty(t *testing.T) {
	// An IPC stream with schema but zero rows. The writer produces a schema
	// message and an EOS. Decoding yields zero records (the schema message does
	// not count as a RecordBatch).
	//
	// Note: buildArrowFixture with nil rows writes a schema + empty batch + EOS.
	// We build a raw IPC stream with only a schema message and EOS to test the
	// truly-empty case.
	alloc := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	// Close without writing any batches → schema + EOS only.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	records, err := decodeArrowIPC(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		r.Release()
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records for schema-only IPC stream, got %d", len(records))
	}
}

// ---------------------------------------------------------------------------
// QueryBuilder.Arrow — HTTP mock transport
// ---------------------------------------------------------------------------

func TestQueryBuilderArrowNativePath(t *testing.T) {
	fixture := buildArrowFixture(t, []struct{ ID int64; Name string }{
		{10, "Carol"},
		{11, "Dave"},
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/users", func(w http.ResponseWriter, r *http.Request) {
		// Verify the Accept header.
		if r.Header.Get("Accept") != arrowMIME {
			http.Error(w, "expected Accept: "+arrowMIME, 406)
			return
		}
		w.Header().Set("Content-Type", arrowMIME)
		w.Header().Set("X-Basin-Next-Cursor", "cursor-tok")
		w.Header().Set("X-Basin-Row-Count", "2")
		w.WriteHeader(200)
		_, _ = w.Write(fixture)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, "test-api-key", WithProjectID("proj-01"))
	result, err := client.Table("users").Arrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if len(result.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(result.Records))
	}
	if result.Records[0].NumRows() != 2 {
		t.Errorf("NumRows: got %d want 2", result.Records[0].NumRows())
	}
	if result.NextCursor != "cursor-tok" {
		t.Errorf("NextCursor: got %q want cursor-tok", result.NextCursor)
	}
	if result.RowCount != 2 {
		t.Errorf("RowCount: got %d want 2", result.RowCount)
	}
}

func TestQueryBuilderArrowFallbackToJSON(t *testing.T) {
	// Server returns JSON (older server / fallback path).
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		// Ignore the Accept header; return JSON.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 1, "total": 99.5},
			{"id": 2, "total": 45.0},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, "test-api-key", WithProjectID("proj-01"))
	result, err := client.Table("orders").Arrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	// In the fallback path, Records is nil and FallbackRows is populated.
	if result.Records != nil {
		t.Errorf("expected Records to be nil in fallback path")
	}
	if len(result.FallbackRows) != 2 {
		t.Errorf("FallbackRows len: got %d want 2", len(result.FallbackRows))
	}
}

func TestQueryBuilderArrowAuthHeader(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			http.Error(w, "missing auth", 401)
			return
		}
		fixture := buildArrowFixture(t, nil)
		w.Header().Set("Content-Type", arrowMIME)
		w.WriteHeader(200)
		_, _ = w.Write(fixture)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, "test-key")
	result, err := client.Table("orders").Arrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result.Release()
}

func TestQueryBuilderArrowWithFilters(t *testing.T) {
	// Verify that query params are forwarded on Arrow requests.
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		fixture := buildArrowFixture(t, nil)
		w.Header().Set("Content-Type", arrowMIME)
		w.WriteHeader(200)
		_, _ = w.Write(fixture)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, "key")
	result, err := client.Table("orders").Eq("status", "open").Limit(10).Arrow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result.Release()

	if gotQuery == "" {
		t.Error("expected query params, got empty string")
	}
	// Should contain status=eq.open and limit=10.
	if !containsParam(gotQuery, "status", "eq.open") {
		t.Errorf("query params missing status filter: %q", gotQuery)
	}
	if !containsParam(gotQuery, "limit", "10") {
		t.Errorf("query params missing limit: %q", gotQuery)
	}
}

func TestQueryBuilderArrowErrorResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code":    "E_NOT_FOUND",
			"message": "table not found",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, "key")
	_, err := client.Table("orders").Arrow(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	be, ok := err.(*BasinError)
	if !ok {
		t.Fatalf("expected *BasinError, got %T: %v", err, err)
	}
	if be.Code != "E_NOT_FOUND" {
		t.Errorf("Code: got %q want E_NOT_FOUND", be.Code)
	}
}

// ---------------------------------------------------------------------------
// arrowIsNative / arrowParseRowCount helpers
// ---------------------------------------------------------------------------

func TestArrowIsNative(t *testing.T) {
	cases := []struct {
		ct   string
		want bool
	}{
		{arrowMIME, true},
		{arrowMIME + "; charset=utf-8", true},
		{"application/json", false},
		{"", false},
	}
	for _, tc := range cases {
		got := arrowIsNative(tc.ct)
		if got != tc.want {
			t.Errorf("arrowIsNative(%q): got %v want %v", tc.ct, got, tc.want)
		}
	}
}

func TestArrowParseRowCount(t *testing.T) {
	cases := []struct {
		s    string
		want int64
	}{
		{"42", 42},
		{"0", 0},
		{"", 0},
		{"abc", 0},
	}
	for _, tc := range cases {
		if got := arrowParseRowCount(tc.s); got != tc.want {
			t.Errorf("arrowParseRowCount(%q): got %d want %d", tc.s, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func containsParam(rawQuery, key, value string) bool {
	for _, pair := range splitParams(rawQuery) {
		if pair[0] == key && pair[1] == value {
			return true
		}
	}
	return false
}

func splitParams(q string) [][2]string {
	var out [][2]string
	for _, pair := range bytes.Split([]byte(q), []byte("&")) {
		kv := bytes.SplitN(pair, []byte("="), 2)
		if len(kv) == 2 {
			// URL-decode the value for comparison.
			val := string(kv[1])
			val = urlDecode(val)
			out = append(out, [2]string{string(kv[0]), val})
		}
	}
	return out
}

func urlDecode(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			var b byte
			for _, c := range []byte(s[i+1 : i+3]) {
				b <<= 4
				if c >= '0' && c <= '9' {
					b |= c - '0'
				} else if c >= 'a' && c <= 'f' {
					b |= c - 'a' + 10
				} else if c >= 'A' && c <= 'F' {
					b |= c - 'A' + 10
				}
			}
			out = append(out, b)
			i += 2
		} else if s[i] == '+' {
			out = append(out, ' ')
		} else {
			out = append(out, s[i])
		}
	}
	return string(out)
}
