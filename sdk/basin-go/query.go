package basin

import (
	"context"
	"encoding/json"
	"fmt"
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
func (q *QueryBuilder) Eq(column, value string) *QueryBuilder {
	return q.push(column, "eq."+value)
}

// Neq adds a not-equal filter: column=neq.value.
func (q *QueryBuilder) Neq(column, value string) *QueryBuilder {
	return q.push(column, "neq."+value)
}

// Gt adds a greater-than filter: column=gt.value.
func (q *QueryBuilder) Gt(column, value string) *QueryBuilder {
	return q.push(column, "gt."+value)
}

// Gte adds a greater-than-or-equal filter: column=gte.value.
func (q *QueryBuilder) Gte(column, value string) *QueryBuilder {
	return q.push(column, "gte."+value)
}

// Lt adds a less-than filter: column=lt.value.
func (q *QueryBuilder) Lt(column, value string) *QueryBuilder {
	return q.push(column, "lt."+value)
}

// Lte adds a less-than-or-equal filter: column=lte.value.
func (q *QueryBuilder) Lte(column, value string) *QueryBuilder {
	return q.push(column, "lte."+value)
}

// In adds an IN filter: column=in.(a,b,c).
func (q *QueryBuilder) In(column string, values []string) *QueryBuilder {
	return q.push(column, "in.("+strings.Join(values, ",")+")")
}

// Is adds an IS filter for null/notnull: column=is.null or column=is.notnull.
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
