module github.com/bas-in/basin/sdk/basin-go

go 1.21

// Zero external runtime dependencies. The core SDK uses only the Go standard
// library (net/http, encoding/json, context, sync, time, strings, etc.).
// This avoids supply-chain risk, keeps binaries small, and means users of
// this package do not inherit any transitive module graph.
//
// The only file that imports a third-party package is the test binary, which
// uses the stdlib net/http/httptest package (part of the standard library),
// so no require directives are needed.
