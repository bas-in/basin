package basin

// Realtime WebSocket client for Basin.
//
// Connects to GET /realtime/v1/ws/:project (crates/basin-realtime/src/ws.rs).
//
// Protocol source of truth: crates/basin-realtime/src/ws.rs (ClientMsg /
// ServerMsg + presence.rs serialize_presence_* helpers).
//
// Auth: Sec-WebSocket-Protocol: basin-v1,<token> — the server also accepts
// Authorization: Bearer but browsers and most WS libraries make the subprotocol
// form simpler for client-originated connections.
//
// Dependency note: the Go standard library has no WebSocket client. This file
// uses nhooyr.io/websocket (pure Go, MIT, zero CGO, sub-tree dep) rather than
// gorilla/websocket (gorilla is archived/unmaintained since 2023). The
// dependency is declared in go.mod under a `// realtime dep` comment so it is
// auditable and isolated to this file.
//
// Usage:
//
//	client := basin.New("https://host", "key", basin.WithProjectID("01J..."))
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	events, err := client.Realtime.Subscribe(ctx, "orders", SubscribeOptions{})
//	if err != nil { ... }
//	for ev := range events {
//	    fmt.Println(ev.Op, ev.After)
//	}

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// ---------------------------------------------------------------------------
// Frame types — server → client
// ---------------------------------------------------------------------------

// RealtimeEventOp is the operation that caused a change event.
type RealtimeEventOp string

const (
	OpInsert RealtimeEventOp = "INSERT"
	OpUpdate RealtimeEventOp = "UPDATE"
	OpDelete RealtimeEventOp = "DELETE"
)

// RealtimeEvent is a change event delivered by the server as a
// {"type":"event",...} frame.
type RealtimeEvent struct {
	Type    string            `json:"type"`
	Project string            `json:"project"`
	Table   string            `json:"table"`
	Op      RealtimeEventOp   `json:"op"`
	Before  map[string]any    `json:"before,omitempty"`
	After   map[string]any    `json:"after,omitempty"`
	Seq     uint64            `json:"seq"`
}

// RealtimeErrorFrame is {"type":"error","code":...,"table":...,"missed":...}.
type RealtimeErrorFrame struct {
	Type   string `json:"type"`
	Code   string `json:"code"`
	Table  string `json:"table"`
	Missed *uint64 `json:"missed,omitempty"`
}

// RealtimeGapFrame is {"type":"gap",...} — the replay ring has been evicted;
// the client must cold re-sync from the analytical store.
type RealtimeGapFrame struct {
	Type          string `json:"type"`
	Table         string `json:"table"`
	LastEventID   uint64 `json:"last_event_id"`
	OldestInRing  uint64 `json:"oldest_in_ring"`
	NewestInRing  uint64 `json:"newest_in_ring"`
}

// PresenceEntry is one member of a presence channel.
type PresenceEntry struct {
	ClientID string `json:"client_id"`
	Metadata any    `json:"metadata,omitempty"`
	JoinedAt string `json:"joined_at,omitempty"`
}

// PresenceStateFrame is {"type":"presence_state","channel":...,"presences":[...]}
type PresenceStateFrame struct {
	Type      string          `json:"type"`
	Channel   string          `json:"channel"`
	Presences []PresenceEntry `json:"presences"`
}

// PresenceDiffFrame is {"type":"presence_diff","channel":...,"joins":[...],"leaves":[...]}
type PresenceDiffFrame struct {
	Type    string          `json:"type"`
	Channel string          `json:"channel"`
	Joins   []PresenceEntry `json:"joins"`
	Leaves  []PresenceEntry `json:"leaves"`
}

// PresenceErrorFrame is {"type":"presenceerror","code":...,"channel":...,"message":...}
// Note: the server uses rename_all = "lowercase" → wire type is "presenceerror"
// (no underscore). Matches ws.rs ServerMsg::PresenceError.
type PresenceErrorFrame struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Channel string `json:"channel"`
	Message string `json:"message"`
}

// SubscribedFrame is {"type":"subscribed","table":...}
type SubscribedFrame struct {
	Type  string `json:"type"`
	Table string `json:"table"`
}

// UnsubscribedFrame is {"type":"unsubscribed","table":...}
type UnsubscribedFrame struct {
	Type  string `json:"type"`
	Table string `json:"table"`
}

// ServerFrame is a discriminated union of all frames the server sends.
// Inspect the Type field to choose the concrete frame type.
type ServerFrame struct {
	// Type is one of: "event", "error", "gap", "subscribed", "unsubscribed",
	// "presence_state", "presence_diff", "presenceerror".
	Type string

	Event          *RealtimeEvent
	Error          *RealtimeErrorFrame
	Gap            *RealtimeGapFrame
	Subscribed     *SubscribedFrame
	Unsubscribed   *UnsubscribedFrame
	PresenceState  *PresenceStateFrame
	PresenceDiff   *PresenceDiffFrame
	PresenceError  *PresenceErrorFrame
}

func parseServerFrame(raw []byte) (*ServerFrame, error) {
	var top struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, err
	}
	f := &ServerFrame{Type: top.Type}
	switch top.Type {
	case "event":
		var v RealtimeEvent
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		f.Event = &v
	case "error":
		var v RealtimeErrorFrame
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		f.Error = &v
	case "gap":
		var v RealtimeGapFrame
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		f.Gap = &v
	case "subscribed":
		var v SubscribedFrame
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		f.Subscribed = &v
	case "unsubscribed":
		var v UnsubscribedFrame
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		f.Unsubscribed = &v
	case "presence_state":
		var v PresenceStateFrame
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		f.PresenceState = &v
	case "presence_diff":
		var v PresenceDiffFrame
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		f.PresenceDiff = &v
	case "presenceerror":
		var v PresenceErrorFrame
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		f.PresenceError = &v
	default:
		// Unknown frame type — return a frame with just the type set so the caller
		// can decide whether to log and skip.
		return f, nil
	}
	return f, nil
}

// ---------------------------------------------------------------------------
// Reconnect backoff
// ---------------------------------------------------------------------------

const (
	backoffBase   = 500 * time.Millisecond
	backoffMax    = 30 * time.Second
	backoffFactor = 2.0
)

func backoffDelay(attempt int) time.Duration {
	d := time.Duration(float64(backoffBase) * math.Pow(backoffFactor, float64(attempt)))
	if d > backoffMax {
		return backoffMax
	}
	return d
}

// ---------------------------------------------------------------------------
// SubscribeOptions
// ---------------------------------------------------------------------------

// SubscribeOptions configures a Subscribe call.
type SubscribeOptions struct {
	// Filter is an optional server-side SQL predicate, e.g. "NEW.status = 'paid'".
	// The server parses and validates this; invalid predicates arrive as an
	// RealtimeErrorFrame with Code "invalid_filter".
	Filter string

	// LastEventID is a reconnect cursor — the last Seq the caller processed.
	// When non-zero, the server replays missed events from its replay ring, or
	// emits a RealtimeGapFrame if the ring has been evicted.
	LastEventID uint64
}

// ---------------------------------------------------------------------------
// RealtimeClient
// ---------------------------------------------------------------------------

// RealtimeClient is the WebSocket realtime client. Obtain via Client.Realtime.
//
// The primary API is Subscribe, which returns a channel that receives
// ServerFrame values and closes when the context is cancelled.
//
// A single WebSocket connection is shared across all Subscribe calls. The
// connection is opened lazily on the first Subscribe call and reconnects with
// exponential backoff on unexpected disconnects, re-subscribing all active
// channels automatically.
type RealtimeClient struct {
	t         *transport
	projectID func() string

	mu       sync.Mutex
	ws       *websocket.Conn
	subs     map[string]*subscription // table/channel → sub
	closed   bool
	connCtx  context.Context
	connStop context.CancelFunc
}

type subscription struct {
	opts   SubscribeOptions
	queues []chan<- ServerFrame
}

func newRealtimeClient(t *transport, projectID func() string) *RealtimeClient {
	return &RealtimeClient{
		t:         t,
		projectID: projectID,
		subs:      make(map[string]*subscription),
	}
}

// Subscribe subscribes to change events on table and returns a read-only
// channel. The channel receives ServerFrame values and is closed when ctx is
// cancelled or the server closes the connection with a terminal close code.
//
// Each Subscribe call for the same table shares one server-side subscription.
// The server sends a "subscribed" ack before any events; this method blocks
// until the ack is received (with a 30 s timeout).
//
// opts.LastEventID can be supplied to replay missed events after a reconnect.
func (r *RealtimeClient) Subscribe(ctx context.Context, table string, opts SubscribeOptions) (<-chan ServerFrame, error) {
	if err := r.connect(ctx); err != nil {
		return nil, err
	}

	ch := make(chan ServerFrame, 64)

	r.mu.Lock()
	sub, ok := r.subs[table]
	if !ok {
		sub = &subscription{opts: opts}
		r.subs[table] = sub
	}
	sub.queues = append(sub.queues, ch)
	r.mu.Unlock()

	// Send subscribe message and wait for the ack.
	ackCh := r.registerSubscribeAck(table)
	if err := r.sendSubscribe(table, opts); err != nil {
		r.removeQueue(table, ch)
		close(ch)
		return nil, fmt.Errorf("basin realtime: subscribe: %w", err)
	}
	ackCtx, ackCancel := context.WithTimeout(ctx, 30*time.Second)
	defer ackCancel()
	select {
	case <-ackCh:
		// subscribed
	case <-ackCtx.Done():
		r.removeQueue(table, ch)
		close(ch)
		return nil, fmt.Errorf("basin realtime: subscribe ack timeout for table %q", table)
	}

	// Close ch when ctx is cancelled.
	go func() {
		<-ctx.Done()
		r.removeQueue(table, ch)
		close(ch)
		// Send unsubscribe if no other callers are subscribed.
		r.mu.Lock()
		sub2, exists := r.subs[table]
		noMore := exists && len(sub2.queues) == 0
		if noMore {
			delete(r.subs, table)
		}
		r.mu.Unlock()
		if noMore {
			_ = r.sendMsg(map[string]any{"type": "unsubscribe", "table": table})
		}
	}()

	return ch, nil
}

// Unsubscribe sends an explicit unsubscribe message for table and closes all
// subscription channels for it. Callers who used Subscribe with a context
// cancellation instead can simply cancel the context.
func (r *RealtimeClient) Unsubscribe(ctx context.Context, table string) error {
	r.mu.Lock()
	sub, ok := r.subs[table]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	qs := sub.queues
	delete(r.subs, table)
	r.mu.Unlock()

	for _, q := range qs {
		close(q)
	}
	return r.sendMsg(map[string]any{"type": "unsubscribe", "table": table})
}

// PresenceTrack sends {"type":"presence_track","channel":...,"client_id":...,"metadata":...}.
func (r *RealtimeClient) PresenceTrack(ctx context.Context, channel, clientID string, metadata any) error {
	if err := r.connect(ctx); err != nil {
		return err
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	return r.sendMsg(map[string]any{
		"type":      "presence_track",
		"channel":   channel,
		"client_id": clientID,
		"metadata":  metadata,
	})
}

// PresenceUntrack sends {"type":"presence_untrack","channel":...,"client_id":...}.
func (r *RealtimeClient) PresenceUntrack(ctx context.Context, channel, clientID string) error {
	if err := r.connect(ctx); err != nil {
		return err
	}
	return r.sendMsg(map[string]any{
		"type":      "presence_untrack",
		"channel":   channel,
		"client_id": clientID,
	})
}

// PresenceHeartbeat sends {"type":"heartbeat","channel":...,"client_id":...}.
func (r *RealtimeClient) PresenceHeartbeat(ctx context.Context, channel, clientID string) error {
	if err := r.connect(ctx); err != nil {
		return err
	}
	return r.sendMsg(map[string]any{
		"type":      "heartbeat",
		"channel":   channel,
		"client_id": clientID,
	})
}

// Disconnect closes the WebSocket connection. All subscription channels are
// closed. The client can reconnect by calling Subscribe again.
func (r *RealtimeClient) Disconnect() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.connStop != nil {
		r.connStop()
	}
	if r.ws != nil {
		_ = r.ws.Close(websocket.StatusNormalClosure, "client disconnect")
		r.ws = nil
	}
	// Close all subscription queues.
	for table, sub := range r.subs {
		for _, q := range sub.queues {
			close(q)
		}
		delete(r.subs, table)
	}
}

// ---------------------------------------------------------------------------
// Internal connect / send helpers
// ---------------------------------------------------------------------------

// pendingAck is a one-shot channel for subscribe acks keyed by table name.
type pendingAck struct {
	mu   sync.Mutex
	acks map[string]chan struct{}
}

var globalPending = &pendingAck{acks: make(map[string]chan struct{})}

func (r *RealtimeClient) registerSubscribeAck(table string) <-chan struct{} {
	ch := make(chan struct{}, 1)
	globalPending.mu.Lock()
	globalPending.acks[table] = ch
	globalPending.mu.Unlock()
	return ch
}

func (r *RealtimeClient) resolveSubscribeAck(table string) {
	globalPending.mu.Lock()
	ch, ok := globalPending.acks[table]
	if ok {
		delete(globalPending.acks, table)
	}
	globalPending.mu.Unlock()
	if ok {
		select {
		case ch <- struct{}{}:
		default:
	}
	}
}

func (r *RealtimeClient) connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ws != nil {
		return nil
	}
	r.closed = false

	project := r.projectID()
	if project == "" {
		return newBasinError(
			"E_INVALID_REQUEST",
			"realtime requires a project id — pass WithProjectID to New or use a JWT that carries project_id",
			0,
		)
	}

	token := r.t.bearer(ctx)
	if token == "" {
		return newBasinError(
			"E_UNAUTHENTICATED",
			"realtime requires a JWT or API key",
			0,
		)
	}

	wsURL := strings.Replace(r.t.baseURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	wsURL += "/realtime/v1/ws/" + project

	// Auth via subprotocol: "basin-v1,<token>" — mirrors JS and Python SDKs.
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"basin-v1", token},
	})
	if err != nil {
		return fmt.Errorf("basin realtime: dial %s: %w", wsURL, err)
	}
	conn.SetReadLimit(4 << 20) // 4 MiB max frame

	connCtx, connStop := context.WithCancel(context.Background())
	r.ws = conn
	r.connCtx = connCtx
	r.connStop = connStop

	go r.recvLoop(connCtx, connStop)
	return nil
}

func (r *RealtimeClient) sendMsg(msg any) error {
	r.mu.Lock()
	ws := r.ws
	r.mu.Unlock()
	if ws == nil {
		return newBasinError("E_UNKNOWN", "realtime socket is not open", 0)
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("basin realtime: marshal: %w", err)
	}
	return ws.Write(r.connCtx, websocket.MessageText, b)
}

func (r *RealtimeClient) sendSubscribe(table string, opts SubscribeOptions) error {
	msg := map[string]any{"type": "subscribe", "table": table}
	if opts.Filter != "" {
		msg["filter"] = opts.Filter
	}
	if opts.LastEventID != 0 {
		msg["last_event_id"] = opts.LastEventID
	}
	return r.sendMsg(msg)
}

func (r *RealtimeClient) removeQueue(table string, ch chan ServerFrame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.subs[table]
	if !ok {
		return
	}
	qs := sub.queues[:0]
	for _, q := range sub.queues {
		if q != (chan<- ServerFrame)(ch) {
			qs = append(qs, q)
		}
	}
	sub.queues = qs
}

// ---------------------------------------------------------------------------
// Receive loop + reconnect
// ---------------------------------------------------------------------------

func (r *RealtimeClient) recvLoop(ctx context.Context, stop context.CancelFunc) {
	defer stop()
	ws := r.ws
	for {
		_, b, err := ws.Read(ctx)
		if err != nil {
			break
		}
		frame, err := parseServerFrame(b)
		if err != nil {
			log.Printf("basin realtime: parse frame: %v", err)
			continue
		}
		r.dispatch(frame)
	}

	r.mu.Lock()
	r.ws = nil
	closed := r.closed
	subs := make(map[string]*subscription, len(r.subs))
	for k, v := range r.subs {
		subs[k] = v
	}
	r.mu.Unlock()

	if !closed {
		go r.reconnectLoop(subs)
	}
}

func (r *RealtimeClient) dispatch(frame *ServerFrame) {
	switch frame.Type {
	case "event":
		ev := frame.Event
		r.mu.Lock()
		sub := r.subs[ev.Table]
		r.mu.Unlock()
		if sub != nil {
			for _, q := range sub.queues {
				select {
				case q <- *frame:
				default:
				}
			}
		}
	case "error":
		r.mu.Lock()
		sub := r.subs[frame.Error.Table]
		r.mu.Unlock()
		if sub != nil {
			for _, q := range sub.queues {
				select {
				case q <- *frame:
				default:
				}
			}
		}
	case "gap":
		r.mu.Lock()
		sub := r.subs[frame.Gap.Table]
		r.mu.Unlock()
		if sub != nil {
			for _, q := range sub.queues {
				select {
				case q <- *frame:
				default:
				}
			}
		}
	case "subscribed":
		r.resolveSubscribeAck(frame.Subscribed.Table)
	case "unsubscribed":
		// No-op in the dispatch path; cleanup is handled by context cancellation.
	case "presence_state", "presence_diff", "presenceerror":
		// Route presence frames to any subscription queues registered for the channel.
		var channel string
		switch frame.Type {
		case "presence_state":
			channel = frame.PresenceState.Channel
		case "presence_diff":
			channel = frame.PresenceDiff.Channel
		case "presenceerror":
			channel = frame.PresenceError.Channel
		}
		r.mu.Lock()
		sub := r.subs[channel]
		r.mu.Unlock()
		if sub != nil {
			for _, q := range sub.queues {
				select {
				case q <- *frame:
				default:
				}
			}
		}
	}
}

func (r *RealtimeClient) reconnectLoop(subs map[string]*subscription) {
	for attempt := 0; ; attempt++ {
		d := backoffDelay(attempt)
		log.Printf("basin realtime: reconnect in %s (attempt %d)", d, attempt+1)
		time.Sleep(d)

		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		ctx := context.Background()
		if err := r.connect(ctx); err != nil {
			log.Printf("basin realtime: reconnect attempt %d failed: %v", attempt+1, err)
			continue
		}

		// Re-subscribe all active subscriptions.
		for table, sub := range subs {
			ackCh := r.registerSubscribeAck(table)
			if err := r.sendSubscribe(table, sub.opts); err != nil {
				log.Printf("basin realtime: resubscribe %q: %v", table, err)
				continue
			}
			ackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			select {
			case <-ackCh:
			case <-ackCtx.Done():
				log.Printf("basin realtime: resubscribe ack timeout for table %q", table)
			}
			cancel()
		}
		return
	}
}
