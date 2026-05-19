// client — thin HTTP wrapper around the v1 + admin/v1 + auth/v1 API.
//
// Stdlib-only: net/http does the transport, encoding/json shapes the
// envelopes. The wrapper exists so every subcommand has one place
// for: bearer-header injection, content-type negotiation, error
// mapping (the cloud's typed-code envelope decodes into APIError),
// and a tiny ergonomic surface (do, doJSON, doStream).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// APIError is the cloud's typed error envelope. Mirrors the
// `httpserver.WriteError` shape so subcommands can branch on Code
// when they care (e.g. `pat_scope_read_only`, `engine_unconfigured`).
type APIError struct {
	HTTPStatus int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("HTTP %d (%s)", e.HTTPStatus, e.Code)
	}
	if e.Code == "" {
		return fmt.Sprintf("HTTP %d: %s", e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("HTTP %d (%s): %s", e.HTTPStatus, e.Code, e.Message)
}

// AsAPIError unwraps a generic error into *APIError when possible.
// Convenient for `if e, ok := AsAPIError(err); ok && e.Code == "…"`.
func AsAPIError(err error) (*APIError, bool) {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae, true
	}
	return nil, false
}

// Client is a single base-URL + bearer-token combo. Long-lived per
// CLI invocation; safe to reuse for streaming reads.
type Client struct {
	BaseURL    string
	Token      string
	HTTP       *http.Client
	UserAgent  string
	httpStream *http.Client // separate client with no per-request timeout for streams
}

// NewClient binds the base URL and bearer token. Caller is
// responsible for ensuring base URL is non-empty.
func NewClient(base, token string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(base, "/"),
		Token:   token,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
		},
		httpStream: &http.Client{},
		UserAgent:  "basin-cli/" + version,
	}
}

// do executes an HTTP request, decoding a JSON body into out (which
// may be nil to discard the body). Returns *APIError on non-2xx.
func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	u := c.BaseURL + path
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		if out == nil {
			// Drain so connection reuse is happy.
			_, _ = io.Copy(io.Discard, res.Body)
			return nil
		}
		// The cloud's httpserver.WriteJSON wraps every success body
		// as `{"data":{...},"error":null}`. We try that shape first
		// and fall back to a flat unmarshal for forward/back compat
		// (test mocks, public bootstrap routes, future hand-crafted
		// flat emitters). If `data` is null or absent, treat the
		// body as flat — that way a 204-shaped envelope `{"data":null}`
		// doesn't blank-out the caller's pre-zeroed struct.
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		var wrapped struct {
			Data  json.RawMessage `json:"data"`
			Error json.RawMessage `json:"error"`
		}
		if jerr := json.Unmarshal(body, &wrapped); jerr == nil &&
			len(wrapped.Data) > 0 && string(wrapped.Data) != "null" {
			return json.Unmarshal(wrapped.Data, out)
		}
		return json.Unmarshal(body, out)
	}
	// Error path. Try to parse the typed envelope; fall back to a
	// plain HTTP error if the body isn't JSON.
	b, _ := io.ReadAll(res.Body)
	ae := &APIError{HTTPStatus: res.StatusCode}
	if len(b) > 0 {
		// The cloud's httpserver.WriteError shape is
		// {"error":{"code":"…","message":"…"}}. We try that first
		// and fall back to a flat shape for forward compat.
		var nested struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(b, &nested); err == nil && (nested.Error.Code != "" || nested.Error.Message != "") {
			ae.Code = nested.Error.Code
			ae.Message = nested.Error.Message
			return ae
		}
		var flat struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(b, &flat); err == nil && (flat.Code != "" || flat.Message != "") {
			ae.Code = flat.Code
			ae.Message = flat.Message
			return ae
		}
		ae.Message = strings.TrimSpace(string(b))
	}
	return ae
}

// stream opens a long-lived GET request, returning the *http.Response
// for the caller to read. Used by `basin logs --tail`. Caller MUST
// close the body.
func (c *Client) stream(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/x-ndjson, text/event-stream, application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	res, err := c.httpStream.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 400 {
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return nil, &APIError{HTTPStatus: res.StatusCode, Message: strings.TrimSpace(string(b))}
	}
	return res, nil
}

// queryString builds a "?k=v&k=v" suffix for paths that need it.
// Skips entries with empty values so callers can pass a sparse map.
func queryString(kv map[string]string) string {
	if len(kv) == 0 {
		return ""
	}
	v := url.Values{}
	for k, val := range kv {
		if val == "" {
			continue
		}
		v.Set(k, val)
	}
	enc := v.Encode()
	if enc == "" {
		return ""
	}
	return "?" + enc
}

// ── Typed responses ───────────────────────────────────────────────
//
// We decode into permissive structs (json:",omitempty" everywhere) so
// a backend tweak that adds a field doesn't break the CLI. Anything
// the CLI actively renders gets a named field; everything else stays
// in `Extras` (the implementation-defined trailing map).

// User mirrors models.PublicUser as far as the CLI cares.
type User struct {
	ID            string `json:"id,omitempty"`
	Email         string `json:"email,omitempty"`
	Name          string `json:"name,omitempty"`
	EmailVerified bool   `json:"email_verified,omitempty"`
	IsSuperuser   bool   `json:"is_superuser,omitempty"`
}

// Org is the read shape for /v1/orgs/{slug}.
type Org struct {
	ID           string `json:"id,omitempty"`
	Slug         string `json:"slug,omitempty"`
	Name         string `json:"name,omitempty"`
	Plan         string `json:"plan,omitempty"`
	BillingEmail string `json:"billing_email,omitempty"`
}

// Project is the read shape for /v1/projects/{ref} and the list
// envelope under /v1/orgs/{slug}/projects.
type Project struct {
	ID             string `json:"id,omitempty"`
	OrgID          string `json:"org_id,omitempty"`
	Slug           string `json:"slug,omitempty"`
	Ref            string `json:"ref,omitempty"`
	Name           string `json:"name,omitempty"`
	Region         string `json:"region,omitempty"`
	Status         string `json:"status,omitempty"`
	DefaultBranch  string `json:"default_branch,omitempty"`
	EngineTenantID string `json:"engine_tenant_id,omitempty"`
	CreatedAt      string `json:"created_at,omitempty"`
}

// Pgwire mirrors the create-project response's "pgwire" envelope —
// the reveal-once connection string + per-tenant credential.
type Pgwire struct {
	PgwireUser    string `json:"pgwire_user,omitempty"`
	Password      string `json:"password,omitempty"`
	DBName        string `json:"dbname,omitempty"`
	ConnectionURL string `json:"connection_url,omitempty"`
	EngineTenant  string `json:"engine_tenant,omitempty"`
}

// CreateProjectResponse is the create envelope (project + initial
// keys + first-time pgwire credential).
type CreateProjectResponse struct {
	Project *Project       `json:"project,omitempty"`
	Keys    map[string]any `json:"keys,omitempty"`
	Pgwire  *Pgwire        `json:"pgwire,omitempty"`
}

// QueryResult mirrors engine.RunQueryResult for SQL passthrough.
type QueryResult struct {
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
	RowsAffected *int     `json:"rows_affected,omitempty"`
	ElapsedMs    int      `json:"elapsed_ms,omitempty"`
}

// TableInfo is one row of GET /v1/projects/:ref/tables.
type TableInfo struct {
	Name   string `json:"name,omitempty"`
	Schema string `json:"schema,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Rows   int64  `json:"row_count,omitempty"`
}

// Column matches handlers/tables_passthrough.go's columnInfo.
type Column struct {
	Name     string  `json:"name,omitempty"`
	Type     string  `json:"type,omitempty"`
	Nullable bool    `json:"nullable,omitempty"`
	Default  *string `json:"default,omitempty"`
	PK       bool    `json:"pk,omitempty"`
}

// RowsPage is the paginated envelope returned by tables_passthrough's
// GetRows. We accept any shape because the engine returns
// JSON-flexible cells; the CLI just renders everything as strings.
type RowsPage struct {
	Columns  []string `json:"columns,omitempty"`
	Rows     [][]any  `json:"rows,omitempty"`
	Total    int64    `json:"total,omitempty"`
	Page     int      `json:"page,omitempty"`
	PageSize int      `json:"page_size,omitempty"`
}

// MigrationRow mirrors handlers/migrations.go::MigrationRow.
type MigrationRow struct {
	ID      string `json:"id,omitempty"`
	At      string `json:"at,omitempty"`
	Author  string `json:"author,omitempty"`
	Source  string `json:"source,omitempty"`
	Op      string `json:"op,omitempty"`
	Summary string `json:"summary,omitempty"`
	Table   string `json:"table,omitempty"`
}

// Snapshot is the read shape from backups/snapshots.
type Snapshot struct {
	ID          string `json:"id,omitempty"`
	Origin      string `json:"origin,omitempty"`
	Status      string `json:"status,omitempty"`
	Description string `json:"description,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
	ExpiresAt   string `json:"expires_at,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	EngineID    string `json:"engine_snapshot_id,omitempty"`
}

// APIToken mirrors models.APIToken minus the secret material.
type APIToken struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Prefix      string   `json:"prefix,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	Scopes      []string `json:"scopes,omitempty"`
	ProjectIDs  []string `json:"project_ids,omitempty"`
	IPAllowlist []string `json:"ip_allowlist,omitempty"`
	CreatedAt   string   `json:"created_at,omitempty"`
	ExpiresAt   *string  `json:"expires_at,omitempty"`
	LastUsedAt  *string  `json:"last_used_at,omitempty"`
}

// CreateTokenResponse is the once-only reveal envelope from
// POST /v1/orgs/:slug/api-tokens.
type CreateTokenResponse struct {
	Token     *APIToken `json:"token,omitempty"`
	FullToken string    `json:"full_token,omitempty"`
}

// ── /v1/version (CLI↔cloud support-window check) ──────────────────

// CloudVersion is the shape of `GET /v1/version`. Mirror of basin-cloud
// `handlers/health.go` `Version` response. Only `Version` is parsed
// for the semver compare; `Commit` + `Go` are surfaced for diagnostics
// (printed by `basin whoami -v` and reported in bug-report bundles).
type CloudVersion struct {
	Version string `json:"version"`
	Commit  string `json:"commit,omitempty"`
	Go      string `json:"go,omitempty"`
}

// ErrVersionEndpointAbsent signals that the cloud's `/v1/version`
// route returned 404 — the cloud is older than the endpoint. Callers
// treat this as "compatibility unknown" and skip the support-window
// warning rather than aborting.
var ErrVersionEndpointAbsent = errors.New("cloud: /v1/version endpoint absent")

// FetchVersion calls GET /v1/version. The endpoint is public + auth-
// optional; we pass any bearer that happens to be set (cheap, cloud
// ignores it) so the request blends in with normal traffic.
//
// Uses a short 5s request timeout so a sick cloud can't hang
// `basin login`. Caller should treat both transport errors and
// ErrVersionEndpointAbsent as soft-fail — never abort a command on
// the back of a version check.
func (c *Client) FetchVersion(ctx context.Context) (CloudVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var out CloudVersion
	if err := c.do(ctx, http.MethodGet, "/v1/version", nil, &out); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == http.StatusNotFound {
			return CloudVersion{}, ErrVersionEndpointAbsent
		}
		return CloudVersion{}, err
	}
	return out, nil
}

// ── GitHub Releases (self-update check) ─────────────────────────────

// CLIRelease is the slice of GitHub's `/repos/{owner}/{repo}/releases/latest`
// response we care about. Tag is the semver, HTMLURL is the user-facing
// landing page printed in the warning. Ignored fields (body, assets,
// author, etc.) decode into nowhere by virtue of being unnamed.
type CLIRelease struct {
	TagName string `json:"tag_name"`
	HTMLURL string `json:"html_url"`
}

// ErrCLIReleaseEndpointAbsent signals that GitHub returned 404 — the
// repo has no published releases yet. Callers treat it the same as
// ErrVersionEndpointAbsent: silent skip, never block.
var ErrCLIReleaseEndpointAbsent = errors.New("github: no published release for basin-cli")

// githubLatestReleaseURL is the endpoint queried by FetchLatestCLIRelease.
// Exposed as a package-level var so tests can swap in an httptest URL
// without spinning up real DNS / TLS for api.github.com.
var githubLatestReleaseURL = "https://api.github.com/repos/bas-in/basin-cli/releases/latest"

// FetchLatestCLIRelease queries GitHub's public Releases API for the
// most recent non-draft release of basin-cli. Public endpoint, no auth
// required (60/hr unauth quota per IP — well above what a single user's
// CLI sessions hit at the 24h cache TTL).
//
// Uses a short 5s request timeout so a stalled github.com can't hang
// the CLI. Caller should treat ErrCLIReleaseEndpointAbsent (404 — no
// releases yet) and any transport error as silent-skip; never abort a
// command on the back of a self-update check.
//
// Note this is a package-level function, not a method on Client — it
// hits GitHub, not basin-cloud, so it doesn't carry the bearer token
// nor honour the cloud's BaseURL.
func FetchLatestCLIRelease(ctx context.Context) (CLIRelease, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubLatestReleaseURL, nil)
	if err != nil {
		return CLIRelease{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "basin-cli/"+version)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return CLIRelease{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return CLIRelease{}, ErrCLIReleaseEndpointAbsent
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return CLIRelease{}, fmt.Errorf("github: HTTP %d fetching latest release", resp.StatusCode)
	}
	var out CLIRelease
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return CLIRelease{}, fmt.Errorf("github: decode latest release: %w", err)
	}
	return out, nil
}
