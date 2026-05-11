/**
 * Realtime namespace exports. Tier 4 placeholder — see TASKS.md.
 *
 * `basin.realtime.channel('room')` / `basin.channel('room')` return
 * the same builder; calling `.subscribe()` throws `not_implemented`
 * until basin engine v0.2 ships logical-replication-driven channels.
 */

export { RealtimeClient, RealtimeChannel } from "./client.js";
export type {
  RealtimeEvent,
  PostgresChangesPayload,
  PostgresChangesFilter,
  RealtimeListener,
  RealtimeClientDeps,
  RealtimeChannelDeps,
} from "./client.js";
