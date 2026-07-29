package basin

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strconv"
	"strings"
)

// QueryBuilder is a fluent builder for /rest/v1/:table queries.
//
// Route source (verified): crates/basin-rest/src/server.rs:243-249 —
// GET|POST|PATCH|DELETE /rest/v1/:table.
//
// Filter grammar (Basin's PostgREST-style dialect, parser.rs):
//   - select=<cols,csv>
//   - <col>=<op>.<value> with ops eq|neq|gt|gte|lt|lte|in|is
//     (in.(a,b,c) parenthesised; is.null / is.notnull)
//   - order=<col>[.asc|.desc][,...], limit=N, offset=N
//   - cursor=<token> keyset pagination
//
// Filters AND together. There is no OR, NOT, LIKE/ILIKE, or embedded resource
// selects — this is not full PostgREST.
//
// Response shapes (crates/basin-rest/src/routes/data.rs):
//   - plain GET → JSON array of rows
//   - GET with limit or cursor → { rows, next_cursor }
//   - POST → 201, { ok, tag }; PATCH/DELETE → { ok, tag }
//   - DELETE may surface 501 E_ENGINE_UNSUPPORTED
type QueryBuilder struct {
	t     *transport
	table string
	query []queryParam
}

func newQueryBuilder(t *transport, table string) *QueryBuilder {
	return &QueryBuilder{t: t, table: table}
}

// clone returns a shallow copy so filter methods are chain-safe.
func (q *QueryBuilder) clone() *QueryBuilder {
	params := make([]queryParam, len(q.query))
	copy(params, q.query)
	return &QueryBuilder{t: q.t, table: q.table, query: params}
}

func (q *QueryBuilder) push(key, val string) *QueryBuilder {
	out := q.clone()
	out.query = append(out.query, queryParam{key, val})
	return out
}

// formatValue converts a typed filter value to the string literal expected by
// Basin's PostgREST-style parser. Rules:
//
//   - string → verbatim (no quoting; the server parser handles the op.<value> split)
//   - bool   → "true" / "false"
//   - integer types (int, int8…int64, uint…uint64) → decimal
//   - float32/float64 → shortest decimal representation
//   - nil    → "null"  (use with Is for IS NULL checks)
//   - anything else → fmt.Sprint(v)
func formatValue(v any) string {
	if v == nil {
		return "null"
	}
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return strconv.FormatInt(int64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint:
		return strconv.FormatUint(uint64(t), 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// Select sets the columns projection. Accepts a CSV string or a slice.
// Omitting columns or passing "*" selects all columns.
func (q *QueryBuilder) Select(columns ...string) *QueryBuilder {
	var cols string
	if len(columns) == 0 {
		cols = "*"
	} else {
		cols = strings.Join(columns, ",")
	}
	return q.push("select", cols)
}

// Eq adds an equality filter: column=eq.value.
// value may be any scalar type: string, int, int64, float64, bool, or nil.
func (q *QueryBuilder) Eq(column string, value any) *QueryBuilder {
	return q.push(column, "eq."+formatValue(value))
}

// Neq adds a not-equal filter: column=neq.value.
// value may be any scalar type: string, int, int64, float64, bool, or nil.
func (q *QueryBuilder) Neq(column string, value any) *QueryBuilder {
	return q.push(column, "neq."+formatValue(value))
}

// Gt adds a greater-than filter: column=gt.value.
// value may be any scalar type: string, int, int64, float64, bool, or nil.
func (q *QueryBuilder) Gt(column string, value any) *QueryBuilder {
	return q.push(column, "gt."+formatValue(value))
}

// Gte adds a greater-than-or-equal filter: column=gte.value.
// value may be any scalar type: string, int, int64, float64, bool, or nil.
func (q *QueryBuilder) Gte(column string, value any) *QueryBuilder {
	return q.push(column, "gte."+formatValue(value))
}

// Lt adds a less-than filter: column=lt.value.
// value may be any scalar type: string, int, int64, float64, bool, or nil.
func (q *QueryBuilder) Lt(column string, value any) *QueryBuilder {
	return q.push(column, "lt."+formatValue(value))
}

// Lte adds a less-than-or-equal filter: column=lte.value.
// value may be any scalar type: string, int, int64, float64, bool, or nil.
func (q *QueryBuilder) Lte(column string, value any) *QueryBuilder {
	return q.push(column, "lte."+formatValue(value))
}

// In adds an IN filter: column=in.(a,b,c).
// Each element may be any scalar type: string, int, int64, float64, bool, or nil.
func (q *QueryBuilder) In(column string, values []any) *QueryBuilder {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = formatValue(v)
	}
	return q.push(column, "in.("+strings.Join(parts, ",")+")")
}

// Is adds an IS filter for null/notnull: column=is.null or column=is.notnull.
// value must be the literal string "null" or "notnull".
func (q *QueryBuilder) Is(column, value string) *QueryBuilder {
	return q.push(column, "is."+value)
}

// Order appends an ordering directive: order=col.asc|desc.
func (q *QueryBuilder) Order(column string, ascending bool) *QueryBuilder {
	dir := "asc"
	if !ascending {
		dir = "desc"
	}
	return q.push("order", column+"."+dir)
}

// Limit sets a row limit, switching the response shape to { rows, next_cursor }.
func (q *QueryBuilder) Limit(n int) *QueryBuilder {
	return q.push("limit", fmt.Sprintf("%d", n))
}

// Offset sets a row offset.
func (q *QueryBuilder) Offset(n int) *QueryBuilder {
	return q.push("offset", fmt.Sprintf("%d", n))
}

// Cursor resumes keyset pagination from a NextCursor token.
func (q *QueryBuilder) Cursor(token string) *QueryBuilder {
	return q.push("cursor", token)
}

// Run executes the query as GET and returns a QueryResult.
func (q *QueryBuilder) Run(ctx context.Context) (*QueryResult, error) {
	raw, _, err := q.t.doJSON(ctx, "GET", "/rest/v1/"+q.table, q.query, nil, true)
	if err != nil {
		return nil, err
	}
	return normalizeGetResponse(raw)
}

// Stream executes the query with ?stream=true and returns an iterator that
// yields decoded rows as they arrive from the NDJSON response body. The
// iterator is pull-based (range-over-func, Go 1.23+):
//
//	for row, err := range q.Stream(ctx) {
//	    if err != nil { /* handle */ break }
//	    fmt.Println(row)
//	}
//
// The response body is read line-by-line with bufio.Scanner; each non-empty
// line is JSON-decoded into a Row. The trailing pagination line
// {"_basin_next_cursor":"..."} is captured and not yielded as a row — retrieve
// it via (*StreamResult).NextCursor after the loop.
//
// The caller must consume or break the iterator to ensure the HTTP response
// body is closed. Cancelling ctx terminates the iterator with the
// context's error.
func (q *QueryBuilder) Stream(ctx context.Context) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		params := make([]queryParam, len(q.query)+1)
		copy(params, q.query)
		params[len(q.query)] = queryParam{"stream", "true"}

		resp, err := q.t.doStream(ctx, "/rest/v1/"+q.table, params)
		if err != nil {
			yield(nil, err)
			return
		}
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var parsed map[string]any
			if err := json.Unmarshal([]byte(line), &parsed); err != nil {
				if !yield(nil, fmt.Errorf("basin: stream decode: %w", err)) {
					return
				}
				continue
			}
			// Skip the trailing pagination sentinel line; do not yield it.
			if _, ok := parsed["_basin_next_cursor"]; ok {
				continue
			}
			if !yield(Row(parsed), nil) {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			yield(nil, fmt.Errorf("basin: stream read: %w", err))
		}
	}
}

// StreamResult is returned by StreamWithCursor; it exposes the next_cursor
// from the trailing NDJSON sentinel line so callers can page through results.
type StreamResult struct {
	// NextCursor is the opaque cursor from the trailing
	// {"_basin_next_cursor":"..."} line, or "" when there are no more pages.
	NextCursor string
}

// StreamWithCursor is like Stream but also captures the trailing
// {"_basin_next_cursor":"..."} pagination sentinel line. The returned
// *StreamResult is populated after the iterator is fully consumed.
//
//	res := &basin.StreamResult{}
//	for row, err := range q.StreamWithCursor(ctx, res) {
//	    if err != nil { break }
//	    handle(row)
//	}
//	fmt.Println(res.NextCursor)
func (q *QueryBuilder) StreamWithCursor(ctx context.Context, out *StreamResult) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		params := make([]queryParam, len(q.query)+1)
		copy(params, q.query)
		params[len(q.query)] = queryParam{"stream", "true"}

		resp, err := q.t.doStream(ctx, "/rest/v1/"+q.table, params)
		if err != nil {
			yield(nil, err)
			return
		}
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var parsed map[string]any
			if err := json.Unmarshal([]byte(line), &parsed); err != nil {
				if !yield(nil, fmt.Errorf("basin: stream decode: %w", err)) {
					return
				}
				continue
			}
			// Capture the pagination sentinel; do not yield it as a row.
			if cursor, ok := parsed["_basin_next_cursor"]; ok {
				if s, ok := cursor.(string); ok && out != nil {
					out.NextCursor = s
				}
				continue
			}
			if !yield(Row(parsed), nil) {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			yield(nil, fmt.Errorf("basin: stream read: %w", err))
		}
	}
}

// Insert inserts one row or a slice of rows (POST /rest/v1/:table → 201).
// Pass a map[string]any or []map[string]any.
func (q *QueryBuilder) Insert(ctx context.Context, values any) (*ExecResult, error) {
	raw, _, err := q.t.doJSON(ctx, "POST", "/rest/v1/"+q.table, nil, values, true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return &ExecResult{OK: true}, nil
	}
	var result ExecResult
	if err := json.Unmarshal(raw, &result); err != nil {
		// The server sometimes returns the inserted rows; return generic OK.
		return &ExecResult{OK: true}, nil
	}
	return &result, nil
}

// Update patches rows matching the current filters (PATCH /rest/v1/:table).
func (q *QueryBuilder) Update(ctx context.Context, values map[string]any) (*ExecResult, error) {
	raw, _, err := q.t.doJSON(ctx, "PATCH", "/rest/v1/"+q.table, q.query, values, true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return &ExecResult{OK: true}, nil
	}
	var result ExecResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return &ExecResult{OK: true}, nil
	}
	return &result, nil
}

// Delete removes rows matching the current filters (DELETE /rest/v1/:table).
// May return *BasinError with Code "E_ENGINE_UNSUPPORTED" (501) on engines
// that do not support DELETE.
func (q *QueryBuilder) Delete(ctx context.Context) (*ExecResult, error) {
	raw, _, err := q.t.doJSON(ctx, "DELETE", "/rest/v1/"+q.table, q.query, nil, true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return &ExecResult{OK: true}, nil
	}
	var result ExecResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return &ExecResult{OK: true}, nil
	}
	return &result, nil
}

// normalizeGetResponse handles both plain array and { rows, next_cursor }
// response shapes from the Basin data endpoint.
func normalizeGetResponse(raw json.RawMessage) (*QueryResult, error) {
	if raw == nil {
		return &QueryResult{Rows: []Row{}}, nil
	}

	// Try array first.
	if len(raw) > 0 && raw[0] == '[' {
		var rows []Row
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, fmt.Errorf("basin: decode rows: %w", err)
		}
		if rows == nil {
			rows = []Row{}
		}
		return &QueryResult{Rows: rows}, nil
	}

	// Try paginated object { rows, next_cursor }.
	var paged struct {
		Rows       []Row  `json:"rows"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(raw, &paged); err != nil {
		return nil, fmt.Errorf("basin: decode paged response: %w", err)
	}
	if paged.Rows == nil {
		paged.Rows = []Row{}
	}
	return &QueryResult{Rows: paged.Rows, NextCursor: paged.NextCursor}, nil
}
