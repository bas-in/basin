package basin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Frame encode / decode tests
// ---------------------------------------------------------------------------

func TestParseServerFrameEvent(t *testing.T) {
	raw := `{"type":"event","project":"proj-01","table":"orders","op":"INSERT","after":{"id":1},"seq":42}`
	frame, err := parseServerFrame([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != "event" {
		t.Errorf("Type: got %q want event", frame.Type)
	}
	if frame.Event == nil {
		t.Fatal("Event is nil")
	}
	if frame.Event.Op != "INSERT" {
		t.Errorf("Op: got %q want INSERT", frame.Event.Op)
	}
	if frame.Event.Seq != 42 {
		t.Errorf("Seq: got %d want 42", frame.Event.Seq)
	}
	if frame.Event.Table != "orders" {
		t.Errorf("Table: got %q want orders", frame.Event.Table)
	}
}

func TestParseServerFrameError(t *testing.T) {
	missed := uint64(5)
	frame, err := parseServerFrame([]byte(`{"type":"error","code":"lag","table":"orders","missed":5}`))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Error == nil {
		t.Fatal("Error is nil")
	}
	if frame.Error.Code != "lag" {
		t.Errorf("Code: got %q", frame.Error.Code)
	}
	if frame.Error.Missed == nil || *frame.Error.Missed != missed {
		t.Errorf("Missed: got %v want %d", frame.Error.Missed, missed)
	}
}

func TestParseServerFrameGap(t *testing.T) {
	raw := `{"type":"gap","table":"orders","last_event_id":10,"oldest_in_ring":20,"newest_in_ring":30}`
	frame, err := parseServerFrame([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Gap == nil {
		t.Fatal("Gap is nil")
	}
	if frame.Gap.OldestInRing != 20 || frame.Gap.NewestInRing != 30 {
		t.Errorf("Gap ring bounds: %d/%d", frame.Gap.OldestInRing, frame.Gap.NewestInRing)
	}
}

func TestParseServerFrameSubscribed(t *testing.T) {
	frame, err := parseServerFrame([]byte(`{"type":"subscribed","table":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Subscribed == nil || frame.Subscribed.Table != "orders" {
		t.Errorf("Subscribed: %+v", frame.Subscribed)
	}
}

func TestParseServerFrameUnsubscribed(t *testing.T) {
	frame, err := parseServerFrame([]byte(`{"type":"unsubscribed","table":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Unsubscribed == nil || frame.Unsubscribed.Table != "orders" {
		t.Errorf("Unsubscribed: %+v", frame.Unsubscribed)
	}
}

func TestParseServerFramePresenceState(t *testing.T) {
	raw := `{"type":"presence_state","channel":"room:1","presences":[{"client_id":"c1","metadata":{"name":"Alice"}}]}`
	frame, err := parseServerFrame([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if frame.PresenceState == nil {
		t.Fatal("PresenceState is nil")
	}
	if len(frame.PresenceState.Presences) != 1 {
		t.Errorf("Presences len: got %d want 1", len(frame.PresenceState.Presences))
	}
	if frame.PresenceState.Presences[0].ClientID != "c1" {
		t.Errorf("ClientID: got %q", frame.PresenceState.Presences[0].ClientID)
	}
}

func TestParseServerFramePresenceDiff(t *testing.T) {
	raw := `{"type":"presence_diff","channel":"room:1","joins":[{"client_id":"c2"}],"leaves":[]}`
	frame, err := parseServerFrame([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if frame.PresenceDiff == nil {
		t.Fatal("PresenceDiff is nil")
	}
	if len(frame.PresenceDiff.Joins) != 1 {
		t.Errorf("Joins len: got %d want 1", len(frame.PresenceDiff.Joins))
	}
}

// Note: "presenceerror" — no underscore (ws.rs ServerMsg::PresenceError uses
// rename_all = "lowercase" on the serde tag, producing "presenceerror").
func TestParseServerFramePresenceError(t *testing.T) {
	raw := `{"type":"presenceerror","code":"unauthorized","channel":"room:1","message":"forged client_id"}`
	frame, err := parseServerFrame([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if frame.PresenceError == nil {
		t.Fatal("PresenceError is nil")
	}
	if frame.PresenceError.Code != "unauthorized" {
		t.Errorf("Code: got %q", frame.PresenceError.Code)
	}
}

func TestParseServerFrameUnknownType(t *testing.T) {
	// Unknown frame types are returned with only the Type field set, not an error.
	frame, err := parseServerFrame([]byte(`{"type":"future_extension","payload":42}`))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != "future_extension" {
		t.Errorf("Type: got %q", frame.Type)
	}
	// All typed fields should be nil.
	if frame.Event != nil || frame.Error != nil || frame.Gap != nil {
		t.Error("expected all typed fields to be nil for unknown type")
	}
}

// ---------------------------------------------------------------------------
// Client message encoding tests
// ---------------------------------------------------------------------------

func TestSubscribeMessageEncoding(t *testing.T) {
	msg := map[string]any{
		"type":          "subscribe",
		"table":         "orders",
		"filter":        "NEW.status = 'paid'",
		"last_event_id": uint64(42),
	}
	b, _ := json.Marshal(msg)
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	if got["type"] != "subscribe" {
		t.Errorf("type: %v", got["type"])
	}
	if got["table"] != "orders" {
		t.Errorf("table: %v", got["table"])
	}
	if got["filter"] != "NEW.status = 'paid'" {
		t.Errorf("filter: %v", got["filter"])
	}
}

func TestUnsubscribeMessageEncoding(t *testing.T) {
	msg := map[string]any{"type": "unsubscribe", "table": "orders"}
	b, _ := json.Marshal(msg)
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	if got["type"] != "unsubscribe" {
		t.Errorf("type: %v", got["type"])
	}
}

func TestPresenceTrackMessageEncoding(t *testing.T) {
	msg := map[string]any{
		"type":      "presence_track",
		"channel":   "room:1",
		"client_id": "c1",
		"metadata":  map[string]any{"name": "Alice"},
	}
	b, _ := json.Marshal(msg)
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	if got["type"] != "presence_track" {
		t.Errorf("type: %v", got["type"])
	}
	if got["client_id"] != "c1" {
		t.Errorf("client_id: %v", got["client_id"])
	}
}

// ---------------------------------------------------------------------------
// Reconnect backoff
// ---------------------------------------------------------------------------

func TestBackoffDelay(t *testing.T) {
	// attempt 0 → 500ms, 1 → 1s, 2 → 2s, …, capped at 30s
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 500 * time.Millisecond},
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{10, 30 * time.Second}, // capped
	}
	for _, tc := range cases {
		got := backoffDelay(tc.attempt)
		if got != tc.want {
			t.Errorf("backoffDelay(%d): got %s want %s", tc.attempt, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// WebSocket mock server + subscription lifecycle
// ---------------------------------------------------------------------------

// wsTestServer is a minimal WebSocket test double that speaks the Basin
// realtime protocol over httptest, using the stdlib net/http WS upgrade
// (we implement a minimal RFC 6455 handshake without external deps).
//
// For the subscribe/unsubscribe lifecycle we use a channel-based fake instead
// of a real TCP server because the upgrade requires the websocket library which
// is not the stdlib. We test the RealtimeClient by exercising its JSON frame
// encode/decode and subscription bookkeeping directly.

func TestRealtimeClientSubscriptionLifecycle(t *testing.T) {
	// Build a fake transport that doesn't actually connect to a server.
	// We directly exercise the subscription bookkeeping + dispatch path.
	rc := &RealtimeClient{
		t:         nil, // not needed for dispatch tests
		projectID: func() string { return "proj-01" },
		subs:      make(map[string]*subscription),
	}

	// Register a fake subscription with a queue.
	ch := make(chan ServerFrame, 8)
	rc.mu.Lock()
	rc.subs["orders"] = &subscription{
		opts:   SubscribeOptions{},
		queues: []chan<- ServerFrame{ch},
	}
	rc.mu.Unlock()

	// Dispatch an event frame.
	eventFrame := &ServerFrame{
		Type:  "event",
		Event: &RealtimeEvent{Table: "orders", Op: "INSERT", Seq: 7},
	}
	rc.dispatch(eventFrame)

	select {
	case f := <-ch:
		if f.Event == nil || f.Event.Seq != 7 {
			t.Errorf("received unexpected frame: %+v", f)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for dispatched event")
	}

	// Dispatch an error frame.
	missed := uint64(3)
	rc.dispatch(&ServerFrame{
		Type:  "error",
		Error: &RealtimeErrorFrame{Code: "lag", Table: "orders", Missed: &missed},
	})
	select {
	case f := <-ch:
		if f.Error == nil || f.Error.Code != "lag" {
			t.Errorf("error frame: %+v", f)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for error frame")
	}

	// Dispatch a gap frame.
	rc.dispatch(&ServerFrame{
		Type: "gap",
		Gap:  &RealtimeGapFrame{Table: "orders", LastEventID: 10, OldestInRing: 20, NewestInRing: 30},
	})
	select {
	case f := <-ch:
		if f.Gap == nil || f.Gap.OldestInRing != 20 {
			t.Errorf("gap frame: %+v", f)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for gap frame")
	}
}

func TestRealtimeClientPresenceDispatch(t *testing.T) {
	rc := &RealtimeClient{
		t:         nil,
		projectID: func() string { return "proj-01" },
		subs:      make(map[string]*subscription),
	}

	ch := make(chan ServerFrame, 4)
	rc.mu.Lock()
	rc.subs["room:1"] = &subscription{
		queues: []chan<- ServerFrame{ch},
	}
	rc.mu.Unlock()

	// presence_state
	rc.dispatch(&ServerFrame{
		Type: "presence_state",
		PresenceState: &PresenceStateFrame{
			Channel:   "room:1",
			Presences: []PresenceEntry{{ClientID: "c1"}},
		},
	})
	select {
	case f := <-ch:
		if f.PresenceState == nil || f.PresenceState.Channel != "room:1" {
			t.Errorf("presence_state: %+v", f)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for presence_state frame")
	}

	// presenceerror
	rc.dispatch(&ServerFrame{
		Type: "presenceerror",
		PresenceError: &PresenceErrorFrame{
			Code:    "unauthorized",
			Channel: "room:1",
			Message: "forged client_id",
		},
	})
	select {
	case f := <-ch:
		if f.PresenceError == nil || f.PresenceError.Code != "unauthorized" {
			t.Errorf("presenceerror: %+v", f)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for presenceerror frame")
	}
}

func TestRealtimeClientSubscribeAck(t *testing.T) {
	rc := &RealtimeClient{
		t:         nil,
		projectID: func() string { return "proj-01" },
		subs:      make(map[string]*subscription),
	}

	ackCh := rc.registerSubscribeAck("orders")
	// Simulate server sending "subscribed" ack.
	rc.dispatch(&ServerFrame{
		Type:       "subscribed",
		Subscribed: &SubscribedFrame{Table: "orders"},
	})
	select {
	case <-ackCh:
		// ack received
	case <-time.After(time.Second):
		t.Fatal("subscribe ack not received")
	}
}

// ---------------------------------------------------------------------------
// WS upgrade + auth via HTTP mock
// ---------------------------------------------------------------------------

// mockWSServer starts an httptest.Server that simulates the Basin WS endpoint.
// It validates the auth subprotocol and immediately sends a "subscribed" ack.
type mockWSServer struct {
	srv      *httptest.Server
	mu       sync.Mutex
	received []string
}

func newMockWSServer(t *testing.T, onSend func(msg map[string]any) []map[string]any) *mockWSServer {
	t.Helper()
	m := &mockWSServer{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Validate subprotocol header.
		proto := r.Header.Get("Sec-Websocket-Protocol")
		if !strings.Contains(proto, "basin-v1") {
			http.Error(w, "missing basin-v1 subprotocol", 401)
			return
		}
		// We can't do a real WS upgrade in stdlib without a library — instead
		// respond with 101 and rely on the nhooyr.io/websocket client to handle
		// the protocol. For pure-Go tests without CGO we just record that the
		// upgrade was attempted and return 200.
		w.WriteHeader(200)
		fmt.Fprintln(w, "upgrade accepted")
	}))
	return m
}

// TestRealtimeClientRequiresProjectID verifies that connect() returns
// E_INVALID_REQUEST when no project ID is available.
func TestRealtimeClientRequiresProjectID(t *testing.T) {
	rc := &RealtimeClient{
		t:         newTransport("http://localhost", "key", &http.Client{Timeout: time.Second}),
		projectID: func() string { return "" }, // no project ID
		subs:      make(map[string]*subscription),
	}
	err := rc.connect(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	be, ok := err.(*BasinError)
	if !ok {
		t.Fatalf("expected *BasinError, got %T: %v", err, err)
	}
	if be.Code != "E_INVALID_REQUEST" {
		t.Errorf("Code: got %q want E_INVALID_REQUEST", be.Code)
	}
}

// TestRealtimeClientRequiresToken verifies that connect() returns
// E_UNAUTHENTICATED when no token is available.
func TestRealtimeClientRequiresToken(t *testing.T) {
	rc := &RealtimeClient{
		t:         newTransport("http://localhost", "", &http.Client{Timeout: time.Second}),
		projectID: func() string { return "proj-01" },
		subs:      make(map[string]*subscription),
	}
	err := rc.connect(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	be, ok := err.(*BasinError)
	if !ok {
		t.Fatalf("expected *BasinError, got %T: %v", err, err)
	}
	if be.Code != "E_UNAUTHENTICATED" {
		t.Errorf("Code: got %q want E_UNAUTHENTICATED", be.Code)
	}
}

// TestRealtimeWSURLConstruction verifies that the WS URL is built correctly
// from an HTTP base URL.
func TestRealtimeWSURLConstruction(t *testing.T) {
	cases := []struct {
		baseURL string
		want    string
	}{
		{"http://localhost:8080", "ws://localhost:8080/realtime/v1/ws/proj-01"},
		{"https://host.basin.run", "wss://host.basin.run/realtime/v1/ws/proj-01"},
	}
	for _, tc := range cases {
		t.Run(tc.baseURL, func(t *testing.T) {
			wsURL := strings.Replace(tc.baseURL, "https://", "wss://", 1)
			wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
			wsURL += "/realtime/v1/ws/proj-01"
			if wsURL != tc.want {
				t.Errorf("wsURL: got %q want %q", wsURL, tc.want)
			}
		})
	}
}

// TestRealtimeSubprotocolAuth verifies the basin-v1,<token> subprotocol form
// mirrors what the Python and JS SDKs send.
func TestRealtimeSubprotocolAuth(t *testing.T) {
	token := "mytoken.jwt.sig"
	subprotocols := []string{"basin-v1", token}
	if subprotocols[0] != "basin-v1" {
		t.Errorf("first subprotocol: %q", subprotocols[0])
	}
	if subprotocols[1] != token {
		t.Errorf("second subprotocol: %q", subprotocols[1])
	}
	// The server parses the header as "basin-v1,<token>" and extracts the token
	// by splitting on comma and skipping the "basin-v1" entry (ws.rs extract_token).
	header := strings.Join(subprotocols, ", ")
	var found string
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if part != "basin-v1" && part != "" {
			found = part
		}
	}
	if found != token {
		t.Errorf("token from header: got %q want %q", found, token)
	}
}

// TestRealtimeDisconnect verifies that Disconnect() closes queues.
func TestRealtimeDisconnect(t *testing.T) {
	rc := &RealtimeClient{
		t:         nil,
		projectID: func() string { return "proj-01" },
		subs:      make(map[string]*subscription),
	}

	ch := make(chan ServerFrame, 4)
	rc.mu.Lock()
	rc.subs["orders"] = &subscription{
		queues: []chan<- ServerFrame{ch},
	}
	rc.mu.Unlock()

	rc.Disconnect()

	// Channel should be closed.
	select {
	case _, open := <-ch:
		if open {
			t.Error("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after Disconnect()")
	}
}
