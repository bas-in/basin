# Realtime — basin-js guide

Subscribe to live table changes and track online presence with
`basin.channel()`. The SDK chooses the cheapest transport automatically
(SSE or WebSocket) based on what the channel needs.

---

## Quick start

```ts
import { createClient } from "@bas-in/basin-js";

const basin = createClient(process.env.BASIN_URL!, process.env.BASIN_ANON_KEY!);

// Listen for all new orders
const channel = basin
  .channel("orders-feed")
  .on(
    "postgres_changes",
    { event: "INSERT", table: "orders" },
    (payload) => {
      console.log("new order:", payload.new);
    },
  )
  .subscribe();

// Later — clean up
channel.unsubscribe();
```

---

## Subscribing to table changes

`.on('postgres_changes', filter, callback)` registers a listener for
PostgreSQL mutations.

### Filter shape

```ts
interface PostgresChangesFilter {
  event: "INSERT" | "UPDATE" | "DELETE" | "*"; // which operations to receive
  table: string;                                // required — table name
  schema?: string;                              // default "public"
  filter?: string;                              // optional server-side row filter
}
```

### Events

| `event` value | Receives |
|---|---|
| `"INSERT"` | new rows only |
| `"UPDATE"` | updated rows only |
| `"DELETE"` | deleted rows only |
| `"*"` | all three operation types |

### Callback payload

```ts
interface PostgresChangesPayload<T = unknown> {
  schema: string;
  table: string;
  commit_timestamp: string;       // ISO-8601
  eventType: "INSERT" | "UPDATE" | "DELETE";
  new: T;                         // row after the change
  old: Partial<T>;                // row before (UPDATE/DELETE; empty for INSERT)
}
```

### Examples

```ts
// All mutations on a table
basin
  .channel("all-changes")
  .on("postgres_changes", { event: "*", table: "messages" }, (p) => {
    console.log(p.eventType, p.new);
  })
  .subscribe();

// Updates only
basin
  .channel("status-changes")
  .on("postgres_changes", { event: "UPDATE", table: "shipments" }, (p) => {
    console.log("shipment updated:", p.new);
  })
  .subscribe();

// Typed rows
type Order = { id: number; status: string; total: number };

basin
  .channel("typed-orders")
  .on<Order>("postgres_changes", { event: "INSERT", table: "orders" }, (p) => {
    const order = p.new; // typed as Order
    console.log(order.status);
  })
  .subscribe();
```

---

## Filtered subscriptions

Pass a `filter` string (PostgREST row-filter syntax) to push server-side
predicate evaluation down to the engine. Only rows that match the filter are
streamed to the client.

```ts
// Only paid orders
basin
  .channel("paid-orders")
  .on(
    "postgres_changes",
    { event: "INSERT", table: "orders", filter: "status=eq.paid" },
    (payload) => {
      console.log("paid order:", payload.new);
    },
  )
  .subscribe();
```

> Channels with a `filter` always use the WebSocket transport (see
> [Transport selection](#transport-selection) below).

---

## Presence

Track which clients are online in a channel. Presence state is maintained
by the engine and broadcast as a diff whenever a client joins or leaves.

### `track(metadata)` / `untrack()`

```ts
const channel = basin
  .channel("room:lobby")
  // Listen for presence events
  .on("presence", { event: "sync" }, (members) => {
    console.log("current members:", members);
  })
  .on("presence", { event: "join" }, (members) => {
    console.log("joined:", members);
  })
  .on("presence", { event: "leave" }, (members) => {
    console.log("left:", members);
  })
  .subscribe();

// Announce this client to the channel
channel.track({ userId: "u_42", status: "online" });

// Stop announcing (stays subscribed to events)
channel.untrack();
```

### Presence events

| `event` value | Fires when |
|---|---|
| `"sync"` | full presence snapshot arrives (on first join and after reconnect) |
| `"join"` | one or more clients joined |
| `"leave"` | one or more clients left |

### `presenceState()`

Read the current presence map synchronously at any time:

```ts
const members = channel.presenceState();
// PresenceMember[] — each has { client_id: string; metadata: unknown }
```

### Presence member shape

```ts
interface PresenceMember {
  client_id: string; // assigned by the SDK (UUID)
  metadata: unknown; // whatever you passed to track()
}
```

> The heartbeat frame is sent automatically every 30 seconds while tracked.
> The server evicts a client after 90 seconds of silence.

---

## Transport selection

The SDK picks the cheapest transport automatically when `.subscribe()` is called.
You never configure this manually.

| Channel configuration | Transport |
|---|---|
| Exactly one `postgres_changes` binding, no `filter`, no presence | **SSE** |
| Multiple table bindings, any `filter`, or any presence binding | **WebSocket** |

**SSE** (`GET /realtime/v1/sse/:project/:table`) is a uni-directional HTTP
stream. Lower overhead for simple single-table listeners.

**WebSocket** (`GET /realtime/v1/ws/:project`) is a multiplexed bidirectional
socket. Supports multiple tables, per-subscription filters, and presence — all
on a single connection.

---

## Reconnect with replay

Both transports reconnect automatically on disconnect using exponential
backoff (1 s, 2 s, 4 s … capped at 30 s).

- **SSE** sends `Last-Event-Id: <seq>` on reconnect so the server replays
  any events missed during the gap.
- **WebSocket** re-sends all active `subscribe` frames on reconnect. The SDK
  detects sequence gaps (`seq` field on event frames) and surfaces a lag
  notification if events were missed.

---

## Multiple bindings on one channel

```ts
basin
  .channel("multi")
  .on("postgres_changes", { event: "INSERT", table: "orders" }, onOrder)
  .on("postgres_changes", { event: "UPDATE", table: "shipments" }, onShipment)
  .subscribe();
// ^ Two table bindings → WebSocket transport
```

---

## Teardown

```ts
const channel = basin.channel("my-channel").on(/* … */).subscribe();

// Disconnect and clean up all transport resources
channel.unsubscribe();
```

`unsubscribe()` is chainable and idempotent.

---

## API reference

### `basin.channel(topic: string): RealtimeChannel`

Constructs a channel builder. `topic` is a string name you choose — it
identifies the logical channel (used for presence grouping; not a table name).

### `channel.on(type, filter, callback): this`

Registers a binding. Chainable. Must be called before `.subscribe()`.

| `type` | `filter` type | `callback` type |
|---|---|---|
| `"postgres_changes"` | `PostgresChangesFilter` | `(payload: PostgresChangesPayload<T>) => void` |
| `"presence"` | `{ event: "sync" \| "join" \| "leave" }` | `(members: PresenceMember[]) => void` |

### `channel.subscribe(): this`

Opens the transport and starts receiving events. Idempotent — safe to call
multiple times.

### `channel.unsubscribe(): this`

Closes the transport and releases all resources.

### `channel.track(metadata: unknown): void`

Announces this client to the presence channel. Requires a presence binding
registered via `.on('presence', …)`.

### `channel.untrack(): void`

Stops announcing this client. Does not close the channel.

### `channel.presenceState(): PresenceMember[]`

Returns the current presence snapshot.
