package basin

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Client is the top-level Basin SDK client. It is safe for concurrent use.
//
// Usage:
//
//	client := basin.New("https://your-project.basin.run", "your-api-key",
//	    basin.WithProjectID("01JWXXX..."))
//
//	result, err := client.Table("orders").
//	    Select("id", "total").
//	    Gte("total", "100").
//	    Run(ctx)
type Client struct {
	// Auth provides the /auth/v1/* surface.
	Auth *AuthClient
	// Storage provides the /storage/v1/* surface.
	Storage *StorageClient
	// Functions provides /fn/v1/* (Invoke) and /rest/v1/rpc/* (RPC).
	Functions *FunctionsClient
	// Realtime provides GET /realtime/v1/ws/:project (WebSocket change streams).
	// Requires the nhooyr.io/websocket dependency declared in go.mod.
	Realtime *RealtimeClient

	t                *transport
	explicitProjectID string
}

// Option is a functional option for Client construction.
type Option func(*clientConfig)

type clientConfig struct {
	token      string
	baseURL    string
	projectID  string
	timeout    time.Duration
	httpClient *http.Client
}

// WithToken sets the bearer token (JWT or API key). Identical to passing the
// key as the second argument to New; provided for option-only construction.
func WithToken(token string) Option {
	return func(c *clientConfig) { c.token = token }
}

// WithBaseURL overrides the server base URL.
func WithBaseURL(u string) Option {
	return func(c *clientConfig) { c.baseURL = u }
}

// WithProjectID sets the default project ID used by auth flows and public
// storage URLs. Optional when the JWT already carries a project_id claim.
func WithProjectID(id string) Option {
	return func(c *clientConfig) { c.projectID = id }
}

// WithTimeout sets the default per-request timeout (default: 30s).
func WithTimeout(d time.Duration) Option {
	return func(c *clientConfig) { c.timeout = d }
}

// WithHTTPClient supplies a custom *http.Client (for testing or proxy config).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *clientConfig) { c.httpClient = hc }
}

// New creates a Basin client.
//
//   - baseURL is the server base URL, e.g. "https://your-project.basin.run".
//   - key is a JWT or raw API key passed as Authorization: Bearer <key>.
//     Pass "" if you will sign in via Auth.SignIn instead.
//   - opts are optional functional options (WithProjectID, WithTimeout, …).
func New(baseURL, key string, opts ...Option) *Client {
	cfg := &clientConfig{
		baseURL: baseURL,
		token:   key,
		timeout: 30 * time.Second,
	}
	for _, o := range opts {
		o(cfg)
	}

	hc := cfg.httpClient
	if hc == nil {
		hc = &http.Client{Timeout: cfg.timeout}
	}

	t := newTransport(cfg.baseURL, cfg.token, hc)

	authClient := newAuthClient(t, cfg.projectID)
	// Wire the transport's token-getter to the auth client so every request
	// uses the freshest session token after a SignIn/RefreshSession.
	t.setTokenFn(authClient.AccessToken)

	c := &Client{
		t:                 t,
		explicitProjectID: cfg.projectID,
		Auth:              authClient,
		Functions:         newFunctionsClient(t),
	}
	// Storage and Realtime need a reference back to c.resolveProjectID; assign
	// after c is created.
	c.Storage = newStorageClient(t, c.resolveProjectID)
	c.Realtime = newRealtimeClient(t, c.resolveProjectID)
	return c
}

// resolveProjectID returns the best available project ID:
//  1. The explicit project ID supplied at construction.
//  2. The project_id claim from the current session's access token.
//  3. The project_id claim from the static API key JWT.
//  4. "" when none can be determined.
func (c *Client) resolveProjectID() string {
	if c.explicitProjectID != "" {
		return c.explicitProjectID
	}
	if s := c.Auth.GetSession(); s != nil {
		if pid := projectIDFromJWT(s.AccessToken); pid != "" {
			return pid
		}
	}
	return projectIDFromJWT(c.t.staticKey)
}

// Table returns a QueryBuilder for /rest/v1/:table. The builder is immutable
// — each filter method returns a new builder, so the original is reusable.
func (c *Client) Table(name string) *QueryBuilder {
	return newQueryBuilder(c.t, name)
}

// From is an alias for Table, matching the JS SDK style.
func (c *Client) From(name string) *QueryBuilder {
	return c.Table(name)
}

// RPC is a convenience alias for Functions.RPC.
func (c *Client) RPC(ctx context.Context, fnName string, args map[string]any) (any, error) {
	return c.Functions.RPC(ctx, fnName, args)
}

// Health calls GET /health → "ok".
func (c *Client) Health(ctx context.Context) (string, error) {
	rawURL := c.t.baseURL + "/health"
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return "", fmt.Errorf("basin: build request: %w", err)
	}
	resp, err := c.t.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("basin: http: %w", err)
	}
	defer resp.Body.Close()

	var buf [16]byte
	n, _ := resp.Body.Read(buf[:])
	return string(buf[:n]), nil
}
