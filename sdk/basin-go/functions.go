package basin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// FunctionsClient provides bindings for Basin's two function surfaces.
//
// Route sources (verified):
//
//	ANY  /fn/v1/:name          server.rs:238 — HTTP-handler-shape Wasm functions;
//	                            the guest's response is proxied back verbatim.
//	POST /rest/v1/rpc/:fn_name server.rs:236 — catalog SQL / Wasm UDFs.
type FunctionsClient struct {
	t *transport
}

func newFunctionsClient(t *transport) *FunctionsClient {
	return &FunctionsClient{t: t}
}

// InvokeOption configures a function invocation.
type InvokeOption func(*invokeConfig)

type invokeConfig struct {
	Method  string
	Headers map[string]string
}

// WithInvokeMethod sets the HTTP method (default: POST).
func WithInvokeMethod(method string) InvokeOption {
	return func(c *invokeConfig) { c.Method = method }
}

// WithInvokeHeaders adds extra request headers.
func WithInvokeHeaders(headers map[string]string) InvokeOption {
	return func(c *invokeConfig) {
		for k, v := range headers {
			c.Headers[k] = v
		}
	}
}

// Invoke calls an HTTP-handler function at ANY /fn/v1/:name.
//
// The function's response is proxied verbatim: non-2xx statuses are NOT
// automatically errors unless the body is Basin's own error envelope (auth
// 401, unknown function 404, invocation failure 500), which is surfaced as
// a *BasinError as usual.
//
// body may be nil, []byte, string, or any JSON-serialisable value.
func (f *FunctionsClient) Invoke(ctx context.Context, name string, body any, opts ...InvokeOption) (*FunctionInvokeResult, error) {
	cfg := &invokeConfig{Method: "POST", Headers: map[string]string{}}
	for _, o := range opts {
		o(cfg)
	}

	var bodyR io.Reader
	if body != nil {
		switch v := body.(type) {
		case []byte:
			bodyR = bytes.NewReader(v)
		case string:
			bodyR = bytes.NewReader([]byte(v))
		default:
			b, err := json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("basin: marshal invoke body: %w", err)
			}
			bodyR = bytes.NewReader(b)
			if _, ok := cfg.Headers["content-type"]; !ok {
				cfg.Headers["content-type"] = "application/json"
			}
		}
	}

	rawURL := f.t.baseURL + "/fn/v1/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, cfg.Method, rawURL, bodyR)
	if err != nil {
		return nil, fmt.Errorf("basin: build request: %w", err)
	}

	if tok := f.t.bearer(ctx); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := f.t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("basin: http: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("basin: read response: %w", err)
	}

	ct := resp.Header.Get("Content-Type")
	var data any = string(raw)
	if len(raw) > 0 && isJSON(ct) {
		var decoded any
		if json.Unmarshal(raw, &decoded) == nil {
			data = decoded
		}
	}

	// Surface Basin-layer failures (not the function's own responses) as
	// typed errors.
	if (resp.StatusCode < 200 || resp.StatusCode >= 300) && isErrorEnvelope(data) {
		m, _ := data.(map[string]any)
		code, _ := m["code"].(string)
		message, _ := m["message"].(string)
		return nil, &BasinError{Code: code, Message: message, Status: resp.StatusCode}
	}

	headers := make(map[string]string)
	for k := range resp.Header {
		headers[k] = resp.Header.Get(k)
	}

	return &FunctionInvokeResult{
		Status:  resp.StatusCode,
		Headers: headers,
		Data:    data,
	}, nil
}

// RPC calls a catalog UDF at POST /rest/v1/rpc/:fn_name.
//
// args is a map of named arguments. Missing declared args become NULL.
// Returns the bare scalar for single-value results, or a []Row for
// RETURNS TABLE functions.
func (f *FunctionsClient) RPC(ctx context.Context, fnName string, args map[string]any) (any, error) {
	if args == nil {
		args = map[string]any{}
	}
	raw, _, err := f.t.doJSON(ctx, "POST", "/rest/v1/rpc/"+url.PathEscape(fnName), nil, args, true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	var result any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("basin: decode rpc result: %w", err)
	}
	return result, nil
}

func isJSON(contentType string) bool {
	return len(contentType) > 0 && (contentType == "application/json" ||
		len(contentType) > 16 && contentType[:16] == "application/json")
}

func isErrorEnvelope(data any) bool {
	m, ok := data.(map[string]any)
	if !ok {
		return false
	}
	code, ok := m["code"].(string)
	if !ok {
		return false
	}
	_, hasMsg := m["message"].(string)
	return hasMsg && len(code) > 2 && code[:2] == "E_"
}
