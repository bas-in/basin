/**
 * Realtime namespace exports — T-030 live.
 *
 * `basin.realtime.channel('room')` / `basin.channel('room')` return a
 * `RealtimeChannel` that routes to SSE or WS based on the registered
 * bindings.
 */

export { RealtimeClient, RealtimeChannel } from "./client.js";
export type {
  RealtimeEvent,
  PostgresChangesPayload,
  PostgresChangesFilter,
  RealtimeListener,
  RealtimeClientDeps,
  RealtimeChannelDeps,
  PresenceBindingFilter,
  PresenceCallback,
} from "./client.js";
