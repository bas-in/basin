package basin

// Arrow IPC transport for the Basin REST API.
//
// Route source (verified): crates/basin-rest/src/arrow_ipc.rs —
// GET /rest/v1/:table with Accept: application/vnd.apache.arrow.stream.
// Also POST /rest/v1/rpc/:fn with the same Accept header.
//
// Server response contract (data.rs render_get_response):
//   - Content-Type: application/vnd.apache.arrow.stream
//   - X-Basin-Next-Cursor: <opaque token> (absent when no next page)
//   - X-Basin-Row-Count:   <total row count> (absent when unknown)
//   - Body: Arrow IPC stream — zero or more RecordBatches followed by an EOS marker.
//
// Fallback: when the server returns JSON (older server, or 406 Not Acceptable),
// the rows are decoded from JSON and returned as-is in an ArrowResult with no
// Records; the caller can access .FallbackRows instead.
//
// Dependency: github.com/apache/arrow-go/v18 (Apache License 2.0). Added in
// arrow.go; the core REST/auth/storage paths work without it. Users who call
// Table(...).Arrow() pull this dep; others pay no binary cost.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// arrowMIME is the MIME type for the Arrow IPC streaming format.
// Matches the server constant in crates/basin-rest/src/arrow_ipc.rs.
const arrowMIME = "application/vnd.apache.arrow.stream"

// ArrowResult holds decoded Arrow record batches with pagination metadata.
//
// When the server responded with native Arrow IPC, Records is populated.
// When the server returned JSON (fallback path), FallbackRows is populated and
// Records is nil.
type ArrowResult struct {
	// Records contains the decoded Arrow record batches from the IPC stream.
	// All records share the same schema (accessible via Records[0].Schema()).
	// Nil when the server returned JSON (see FallbackRows).
	Records []arrow.Record

	// FallbackRows holds the JSON rows when the server did not serve Arrow IPC.
	// Nil when Records is populated.
	FallbackRows []Row

	// NextCursor is the opaque pagination token from the X-Basin-Next-Cursor
	// response header. Empty when the server returns no next page.
	NextCursor string

	// RowCount is the total row count from the X-Basin-Row-Count response header.
	// Zero when the header is absent or unparseable.
	RowCount int64
}

// Release releases all Arrow record batches in the result. Call this when you
// are done using the records to avoid memory leaks. No-op when FallbackRows is
// populated.
func (a *ArrowResult) Release() {
	for _, r := range a.Records {
		r.Release()
	}
}

// Arrow executes the query and returns the result as Arrow record batches.
//
// Sends Accept: application/vnd.apache.arrow.stream. If the server responds
// with an Arrow IPC stream (Content-Type matches), decodes it natively via
// github.com/apache/arrow-go/v18 — zero JSON round-trip, full i64/timestamp
// fidelity.
//
// If the server returns JSON (e.g. older server without IPC support, or a
// 406 Not Acceptable), falls back transparently to JSON rows: ArrowResult.Records
// will be nil and ArrowResult.FallbackRows will contain the decoded rows.
//
// Pagination: the X-Basin-Next-Cursor response header is returned in
// ArrowResult.NextCursor so callers can page:
//
//	result, err := client.Table("orders").Limit(1000).Arrow(ctx)
//	if result.NextCursor != "" {
//	    result2, err := client.Table("orders").Limit(1000).Cursor(result.NextCursor).Arrow(ctx)
//	    ...
//	}
//
// Call result.Release() when done to free Arrow memory.
func (q *QueryBuilder) Arrow(ctx context.Context) (*ArrowResult, error) {
	rawURL := q.t.buildURL("/rest/v1/"+q.table, q.query)
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("basin arrow: build request: %w", err)
	}
	req.Header.Set("Accept", arrowMIME)
	if tok := q.t.bearer(ctx); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}

	resp, err := q.t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("basin arrow: http: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("basin arrow: read body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, q.t.parseError(raw, resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	nextCursor := resp.Header.Get("X-Basin-Next-Cursor")
	rowCount := arrowParseRowCount(resp.Header.Get("X-Basin-Row-Count"))

	if arrowIsNative(ct) {
		records, err := decodeArrowIPC(raw)
		if err != nil {
			return nil, fmt.Errorf("basin arrow: decode IPC: %w", err)
		}
		return &ArrowResult{
			Records:    records,
			NextCursor: nextCursor,
			RowCount:   rowCount,
		}, nil
	}

	// Fallback: server returned JSON → decode as rows.
	result, err := normalizeGetResponse(raw)
	if err != nil {
		return nil, fmt.Errorf("basin arrow: JSON fallback decode: %w", err)
	}
	return &ArrowResult{
		FallbackRows: result.Rows,
		NextCursor:   result.NextCursor,
	}, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func arrowIsNative(contentType string) bool {
	return strings.Contains(contentType, arrowMIME)
}

func arrowParseRowCount(s string) int64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// decodeArrowIPC decodes an Arrow IPC stream into one record batch per
// message. The caller owns each record and must call Release() when done.
func decodeArrowIPC(data []byte) ([]arrow.Record, error) {
	reader, err := ipc.NewReader(
		bytes.NewReader(data),
		ipc.WithAllocator(memory.DefaultAllocator),
	)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain() // caller owns this; reader will release its own reference
		records = append(records, rec)
	}
	if err := reader.Err(); err != nil {
		for _, r := range records {
			r.Release()
		}
		return nil, err
	}
	return records, nil
}
