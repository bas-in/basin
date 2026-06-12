// Package basin is the official Go SDK for Basin — a cloud-native HTAP
// database with a PostgreSQL wire-compatible interface and an HTTP REST API.
//
// # Quick start
//
//	client := basin.New("https://your-project.basin.run", "your-api-key",
//	    basin.WithProjectID("01JWXXX..."))
//
//	// Query
//	result, err := client.Table("orders").
//	    Select("id,total").
//	    Gte("total", "100").
//	    Run(ctx)
//
//	// Insert
//	_, err = client.Table("orders").
//	    Insert(ctx, map[string]any{"total": 50, "status": "new"})
//
//	// Storage
//	bkt := client.Storage.FromBucket("avatars")
//	err = bkt.Upload(ctx, "alice/profile.png", data, basin.WithContentType("image/png"))
//
// # Authentication
//
// The client accepts a JWT or a raw API-key string as the bearer token.
// After signing in via Auth.SignIn the session is stored and auto-refreshed
// before expiry; the refreshed token is injected into every subsequent
// request automatically.
//
// # Errors
//
// All API errors are returned as *BasinError, which carries a stable Code
// field (E_UNAUTHENTICATED, E_FORBIDDEN, etc.) and an HTTP Status. Use
// errors.As to extract it:
//
//	var be *basin.BasinError
//	if errors.As(err, &be) {
//	    fmt.Println(be.Code, be.Status)
//	}
//
// # Context and timeouts
//
// Every method accepts a context.Context as its first argument. Pass a
// context with a deadline or cancel to control per-request timeouts:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	result, err := client.Table("events").Run(ctx)
//
// # Deliberate omissions
//
//   - Arrow IPC transport: the Basin server does not expose an Arrow/IPC
//     endpoint. JSON is the only supported transport for query results.
//   - Realtime WebSocket: the realtime WS surface is not included in v1 of
//     this SDK. It is on the roadmap.
//   - Async/concurrent client: Go's standard net/http.Client is safe for
//     concurrent use; no separate async variant is needed.
package basin
