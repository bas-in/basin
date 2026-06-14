package basin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// transport is the shared HTTP layer used by all sub-clients. It injects the
// bearer token on every request and decodes Basin error envelopes uniformly.
type transport struct {
	baseURL    string
	staticKey  string // API key or JWT supplied at construction
	httpClient *http.Client
	// tokenFn, when set, is called to obtain the freshest access token.
	// It takes priority over staticKey so session tokens are used after login.
	tokenFn func(ctx context.Context) (string, error)
}

func newTransport(baseURL, key string, hc *http.Client) *transport {
	return &transport{
		baseURL:    strings.TrimRight(baseURL, "/"),
		staticKey:  key,
		httpClient: hc,
	}
}

// setTokenFn wires the auth client's token-getter so every request uses the
// freshest session token (auto-refreshed when near expiry).
func (t *transport) setTokenFn(fn func(ctx context.Context) (string, error)) {
	t.tokenFn = fn
}

// bearer returns the best available token: session token > static key > "".
func (t *transport) bearer(ctx context.Context) string {
	if t.tokenFn != nil {
		tok, err := t.tokenFn(ctx)
		if err == nil && tok != "" {
			return tok
		}
	}
	return t.staticKey
}

// buildURL concatenates the base URL with a path and optional query params.
func (t *transport) buildURL(path string, query []queryParam) string {
	u := t.baseURL + path
	if len(query) == 0 {
		return u
	}
	vals := make(url.Values)
	for _, p := range query {
		vals.Add(p.key, p.val)
	}
	return u + "?" + vals.Encode()
}

type queryParam struct {
	key string
	val string
}

// doJSON executes an HTTP request and decodes the JSON body.
// On non-2xx responses it attempts to parse Basin's error envelope.
func (t *transport) doJSON(ctx context.Context, method, path string, query []queryParam, body any, useAuth bool) (json.RawMessage, int, error) {
	var bodyR io.Reader
	var contentType string

	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("basin: marshal request body: %w", err)
		}
		bodyR = bytes.NewReader(b)
		contentType = "application/json"
	}

	rawURL := t.buildURL(path, query)
	req, err := http.NewRequestWithContext(ctx, method, rawURL, bodyR)
	if err != nil {
		return nil, 0, fmt.Errorf("basin: build request: %w", err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")

	if useAuth {
		if tok := t.bearer(ctx); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("basin: http: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("basin: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, resp.StatusCode, t.parseError(raw, resp.StatusCode)
	}

	// 204 No Content — return nil body without error
	if resp.StatusCode == 204 || len(raw) == 0 {
		return nil, resp.StatusCode, nil
	}

	return json.RawMessage(raw), resp.StatusCode, nil
}

// doRaw executes an HTTP request and returns the raw response (for binary
// downloads). On non-2xx responses it attempts to parse Basin's error envelope.
func (t *transport) doRaw(ctx context.Context, method, path string, query []queryParam, body any, useAuth bool) (*http.Response, error) {
	var bodyR io.Reader
	var contentType string

	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("basin: marshal request body: %w", err)
		}
		bodyR = bytes.NewReader(b)
		contentType = "application/json"
	}

	rawURL := t.buildURL(path, query)
	req, err := http.NewRequestWithContext(ctx, method, rawURL, bodyR)
	if err != nil {
		return nil, fmt.Errorf("basin: build request: %w", err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if useAuth {
		if tok := t.bearer(ctx); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}

	return t.httpClient.Do(req)
}

// doStream executes a GET with ?stream=true and returns the raw *http.Response
// so the caller can read the NDJSON body incrementally. On non-2xx responses
// the body is consumed and an error is returned. The caller is responsible for
// calling resp.Body.Close() on success.
func (t *transport) doStream(ctx context.Context, path string, query []queryParam) (*http.Response, error) {
	rawURL := t.buildURL(path, query)
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("basin: build request: %w", err)
	}
	req.Header.Set("Accept", "application/x-ndjson")
	if tok := t.bearer(ctx); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("basin: http: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return nil, t.parseError(raw, resp.StatusCode)
	}
	return resp, nil
}

// doUpload performs a POST with a raw byte body (object upload).
func (t *transport) doUpload(ctx context.Context, method, path string, data []byte, contentType string, useAuth bool) (json.RawMessage, int, error) {
	rawURL := t.buildURL(path, nil)
	req, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewReader(data))
	if err != nil {
		return nil, 0, fmt.Errorf("basin: build request: %w", err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")

	if useAuth {
		if tok := t.bearer(ctx); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("basin: http: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("basin: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, resp.StatusCode, t.parseError(raw, resp.StatusCode)
	}

	if len(raw) == 0 {
		return nil, resp.StatusCode, nil
	}
	return json.RawMessage(raw), resp.StatusCode, nil
}

func (t *transport) parseError(raw []byte, status int) *BasinError {
	if len(raw) > 0 {
		var env errorEnvelope
		if json.Unmarshal(raw, &env) == nil && env.Code != "" {
			return &BasinError{
				Code:     env.Code,
				Message:  env.Message,
				Status:   status,
				SQLState: env.SQLState,
			}
		}
	}
	return &BasinError{
		Code:    "E_UNKNOWN",
		Message: fmt.Sprintf("HTTP %d", status),
		Status:  status,
	}
}
