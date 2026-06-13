module github.com/bas-in/basin/sdk/basin-go

go 1.25.0

// Dependency note: nhooyr.io/websocket is the only external runtime dependency
// in this module. It was added specifically to support the Realtime WebSocket
// client (realtime.go). gorilla/websocket was considered but is archived and
// unmaintained since 2023; nhooyr/websocket is pure Go (no CGO), MIT-licensed,
// and has a clean minimal API. Users of the core SDK who do not call
// client.Realtime can safely tree-shake this dep away in most build systems.
//
// The test binary uses stdlib net/http/httptest; no additional test deps needed.
require nhooyr.io/websocket v1.8.11

require github.com/apache/arrow-go/v18 v18.6.0

require (
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/sys v0.43.0 // indirect
)
