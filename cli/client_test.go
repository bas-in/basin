// client_test — table-driven request shape tests.
//
// We spin up a local httptest.Server, point a *Client at it, and
// confirm the wire shape (path, method, headers, body) matches what
// the cloud's v1 + auth/v1 handlers expect.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientDoRequestShape(t *testing.T) {
	type wantReq struct {
		method  string
		path    string
		auth    string
		body    string
		hasBody bool
	}
	cases := []struct {
		name     string
		method   string
		path     string
		body     any
		want     wantReq
		respCode int
		respBody string
		wantErr  bool
	}{
		{
			name: "GET /v1/orgs sends bearer header, no body",
			method: "GET", path: "/v1/orgs",
			respCode: 200, respBody: `{"orgs":[]}`,
			want: wantReq{method: "GET", path: "/v1/orgs", auth: "Bearer test-pat"},
		},
		{
			name: "POST /v1/orgs/foo/projects marshals JSON body",
			method: "POST", path: "/v1/orgs/foo/projects",
			body:     map[string]any{"name": "demo", "region": "jnb"},
			respCode: 201, respBody: `{"project":{"ref":"abc"}}`,
			want: wantReq{
				method: "POST", path: "/v1/orgs/foo/projects", auth: "Bearer test-pat",
				body: `{"name":"demo","region":"jnb"}`, hasBody: true,
			},
		},
		{
			name: "non-2xx surfaces nested envelope as APIError",
			method: "GET", path: "/v1/me",
			respCode: 401,
			respBody: `{"error":{"code":"unauthenticated","message":"Sign in required."}}`,
			want:     wantReq{method: "GET", path: "/v1/me", auth: "Bearer test-pat"},
			wantErr:  true,
		},
		{
			name: "non-2xx surfaces flat envelope as APIError",
			method: "GET", path: "/v1/orgs",
			respCode: 403,
			respBody: `{"code":"forbidden","message":"nope"}`,
			want:     wantReq{method: "GET", path: "/v1/orgs", auth: "Bearer test-pat"},
			wantErr:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tc.want.method {
					t.Errorf("method: got %s, want %s", r.Method, tc.want.method)
				}
				if r.URL.Path != tc.want.path {
					t.Errorf("path: got %s, want %s", r.URL.Path, tc.want.path)
				}
				if got := r.Header.Get("Authorization"); got != tc.want.auth {
					t.Errorf("auth header: got %q, want %q", got, tc.want.auth)
				}
				if tc.want.hasBody {
					b, _ := io.ReadAll(r.Body)
					got := strings.TrimSpace(string(b))
					if got != tc.want.body {
						t.Errorf("body: got %q, want %q", got, tc.want.body)
					}
					if r.Header.Get("Content-Type") != "application/json" {
						t.Errorf("content-type missing on POST")
					}
				}
				w.WriteHeader(tc.respCode)
				_, _ = io.WriteString(w, tc.respBody)
			}))
			defer srv.Close()
			c := NewClient(srv.URL, "test-pat")
			var out any
			err := c.do(context.Background(), tc.method, tc.path, tc.body, &out)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				ae, ok := AsAPIError(err)
				if !ok {
					t.Fatalf("err is not *APIError: %T", err)
				}
				if ae.HTTPStatus != tc.respCode {
					t.Errorf("status: got %d want %d", ae.HTTPStatus, tc.respCode)
				}
				if ae.Code == "" {
					t.Errorf("expected typed code, got %+v", ae)
				}
			}
		})
	}
}

func TestClientStreamHonoursAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-pat" {
			t.Errorf("auth missing on stream")
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"line\":1}\n{\"line\":2}\n")
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "test-pat")
	res, err := c.stream(context.Background(), "/v1/projects/foo/logs/stream")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(b), "line") {
		t.Errorf("stream body unexpected: %q", string(b))
	}
}

func TestQueryString(t *testing.T) {
	cases := []struct {
		in   map[string]string
		want []string // either-or — go map iteration is unordered
	}{
		{nil, []string{""}},
		{map[string]string{}, []string{""}},
		{map[string]string{"a": ""}, []string{""}},
		{map[string]string{"a": "1"}, []string{"?a=1"}},
		{map[string]string{"a": "1", "b": "2"}, []string{"?a=1&b=2", "?b=2&a=1"}},
	}
	for _, tc := range cases {
		got := queryString(tc.in)
		hit := false
		for _, opt := range tc.want {
			if got == opt {
				hit = true
				break
			}
		}
		if !hit {
			t.Errorf("queryString(%v) = %q, want one of %v", tc.in, got, tc.want)
		}
	}
}

// TestAPIErrorString sanity-checks the formatter we surface from
// shared.go's printErr path.
func TestAPIErrorString(t *testing.T) {
	cases := []struct {
		ae   APIError
		want string
	}{
		{APIError{HTTPStatus: 500}, "HTTP 500 ()"},
		{APIError{HTTPStatus: 403, Code: "forbidden"}, "HTTP 403 (forbidden)"},
		{APIError{HTTPStatus: 400, Message: "bad"}, "HTTP 400: bad"},
		{APIError{HTTPStatus: 401, Code: "unauthenticated", Message: "Sign in"}, "HTTP 401 (unauthenticated): Sign in"},
	}
	for _, tc := range cases {
		if got := tc.ae.Error(); got != tc.want {
			t.Errorf("Error: got %q, want %q", got, tc.want)
		}
	}
}

// TestRoundTripDecodes confirms a successful JSON body decodes into
// the typed response struct without losing fields. This is the path
// every `cmd_*.go` uses.
func TestRoundTripDecodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"user": map[string]any{
				"id":             "01H...",
				"email":          "pc@example.com",
				"name":           "pc",
				"email_verified": true,
			},
		})
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "tok")
	var out struct {
		User *User `json:"user"`
	}
	if err := c.do(context.Background(), "GET", "/auth/v1/user", nil, &out); err != nil {
		t.Fatalf("do: %v", err)
	}
	if out.User == nil || out.User.Email != "pc@example.com" || !out.User.EmailVerified {
		t.Errorf("decoded user mismatched: %+v", out.User)
	}
}

func TestFetchVersionDecodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/version" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method: %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"1.5.0","commit":"abc1234","go":"go1.23.4"}`))
	}))
	defer srv.Close()

	cli := NewClient(srv.URL, "")
	got, err := cli.FetchVersion(context.Background())
	if err != nil {
		t.Fatalf("FetchVersion: %v", err)
	}
	if got.Version != "1.5.0" {
		t.Errorf("Version = %q, want 1.5.0", got.Version)
	}
	if got.Commit != "abc1234" {
		t.Errorf("Commit = %q, want abc1234", got.Commit)
	}
	if got.Go != "go1.23.4" {
		t.Errorf("Go = %q, want go1.23.4", got.Go)
	}
}

func TestFetchVersionEndpointAbsent(t *testing.T) {
	// Older clouds (pre-/v1/version) return 404. Caller should get the
	// sentinel, not a generic *APIError.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	cli := NewClient(srv.URL, "")
	_, err := cli.FetchVersion(context.Background())
	if !errors.Is(err, ErrVersionEndpointAbsent) {
		t.Fatalf("expected ErrVersionEndpointAbsent, got %v", err)
	}
}

func TestFetchVersionPropagatesNon404(t *testing.T) {
	// 500 from the cloud is a real failure — propagate, don't swallow.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal","message":"boom"}}`))
	}))
	defer srv.Close()

	cli := NewClient(srv.URL, "")
	_, err := cli.FetchVersion(context.Background())
	if err == nil {
		t.Fatal("expected error from 500")
	}
	if errors.Is(err, ErrVersionEndpointAbsent) {
		t.Fatal("non-404 must not collapse to ErrVersionEndpointAbsent")
	}
	ae, ok := AsAPIError(err)
	if !ok || ae.HTTPStatus != 500 {
		t.Errorf("want APIError 500, got %v", err)
	}
}

func TestDoUnwrapsEnvelope(t *testing.T) {
	// Real cloud responses go through httpserver.WriteJSON which
	// wraps the payload as {"data":{...},"error":null}. The CLI's
	// do() should decode the inner `data` into the caller's struct
	// without the caller knowing about the envelope.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"version":"2.0.0","commit":"def","go":"go1.23"},"error":null}`))
	}))
	defer srv.Close()

	cli := NewClient(srv.URL, "")
	got, err := cli.FetchVersion(context.Background())
	if err != nil {
		t.Fatalf("FetchVersion: %v", err)
	}
	if got.Version != "2.0.0" {
		t.Errorf("unwrapped Version = %q, want 2.0.0", got.Version)
	}
	if got.Commit != "def" {
		t.Errorf("unwrapped Commit = %q, want def", got.Commit)
	}
}

func TestDoFallsBackToFlat(t *testing.T) {
	// Non-wrapped JSON (test mocks, hypothetical future flat-emitting
	// endpoints) still decodes via the flat fallback path. Guards
	// against a future "wrapped-only" refactor that would break
	// every test mock in this file.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"3.0.0","commit":"flat","go":"go1.23"}`))
	}))
	defer srv.Close()

	cli := NewClient(srv.URL, "")
	got, err := cli.FetchVersion(context.Background())
	if err != nil {
		t.Fatalf("FetchVersion: %v", err)
	}
	if got.Version != "3.0.0" {
		t.Errorf("flat Version = %q, want 3.0.0", got.Version)
	}
}

func TestDoHandlesNullData(t *testing.T) {
	// A 200 with {"data":null,"error":null} (some endpoints return
	// this for no-op success) must not zero-out the caller's struct
	// via unmarshal-of-null. Falling back to flat-on-the-body still
	// decodes zero values, which is the right outcome for "no data".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":null,"error":null}`))
	}))
	defer srv.Close()

	cli := NewClient(srv.URL, "")
	got, err := cli.FetchVersion(context.Background())
	if err != nil {
		t.Fatalf("FetchVersion: %v", err)
	}
	if got.Version != "" {
		t.Errorf("expected zero CloudVersion on null data, got %+v", got)
	}
}
