package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/**
 * All server → client frame types delivered via the WebSocket.
 *
 * <p>Protocol source: {@code crates/basin-realtime/src/ws.rs} (ClientMsg/ServerMsg).
 *
 * <p>This is a tagged-union represented as a set of nested static classes.
 * Check {@link RealtimeEvent} for the primary data-event type; the remaining
 * types below cover control, error, gap, and presence frames.
 */
public final class RealtimeFrame {

    private RealtimeFrame() {} // utility namespace

    // ------------------------------------------------------------------
    // Control frames
    // ------------------------------------------------------------------

    /** {@code {"type":"subscribed","table":…}} — server confirmed subscription. */
    public static final class Subscribed {
        @JsonProperty("type") public final String type;
        @JsonProperty("table") public final String table;

        public Subscribed(
                @JsonProperty("type") String type,
                @JsonProperty("table") String table) {
            this.type = type;
            this.table = table;
        }
    }

    /** {@code {"type":"unsubscribed","table":…}} — server confirmed unsubscription. */
    public static final class Unsubscribed {
        @JsonProperty("type") public final String type;
        @JsonProperty("table") public final String table;

        public Unsubscribed(
                @JsonProperty("type") String type,
                @JsonProperty("table") String table) {
            this.type = type;
            this.table = table;
        }
    }

    // ------------------------------------------------------------------
    // Error / gap frames
    // ------------------------------------------------------------------

    /**
     * {@code {"type":"error","code":"lag","table":…,"missed":N}} —
     * protocol-level error for a subscribed table.
     */
    public static final class Error {
        @JsonProperty("type") public final String type;
        @JsonProperty("code") public final String code;
        @JsonProperty("table") public final String table;
        /** Events missed due to slow consumer; present for {@code "lag"} errors. */
        @JsonProperty("missed") public final Integer missed;

        public Error(
                @JsonProperty("type") String type,
                @JsonProperty("code") String code,
                @JsonProperty("table") String table,
                @JsonProperty("missed") Integer missed) {
            this.type = type;
            this.code = code;
            this.table = table;
            this.missed = missed;
        }
    }

    /**
     * {@code {"type":"gap","table":…,"last_event_id":N,"oldest_in_ring":N,"newest_in_ring":N}} —
     * reconnect cursor predates the replay ring; client must cold re-sync.
     */
    public static final class Gap {
        @JsonProperty("type") public final String type;
        @JsonProperty("table") public final String table;
        @JsonProperty("last_event_id") public final long lastEventId;
        @JsonProperty("oldest_in_ring") public final long oldestInRing;
        @JsonProperty("newest_in_ring") public final long newestInRing;

        public Gap(
                @JsonProperty("type") String type,
                @JsonProperty("table") String table,
                @JsonProperty("last_event_id") long lastEventId,
                @JsonProperty("oldest_in_ring") long oldestInRing,
                @JsonProperty("newest_in_ring") long newestInRing) {
            this.type = type;
            this.table = table;
            this.lastEventId = lastEventId;
            this.oldestInRing = oldestInRing;
            this.newestInRing = newestInRing;
        }
    }

    // ------------------------------------------------------------------
    // Presence frames (R4 — Phoenix Channels shape)
    // ------------------------------------------------------------------

    /** A single presence member. Mirrors {@code presence.rs PresenceEntry}. */
    public static final class PresenceEntry {
        @JsonProperty("client_id") public final String clientId;
        @JsonProperty("metadata") public final Object metadata;
        @JsonProperty("joined_at") public final String joinedAt;

        public PresenceEntry(
                @JsonProperty("client_id") String clientId,
                @JsonProperty("metadata") Object metadata,
                @JsonProperty("joined_at") String joinedAt) {
            this.clientId = clientId;
            this.metadata = metadata;
            this.joinedAt = joinedAt;
        }
    }

    /**
     * {@code {"type":"presence_state","channel":…,"presences":[…]}} —
     * snapshot of current channel members sent on join.
     */
    public static final class PresenceState {
        @JsonProperty("type") public final String type;
        @JsonProperty("channel") public final String channel;
        @JsonProperty("presences") public final List<PresenceEntry> presences;

        public PresenceState(
                @JsonProperty("type") String type,
                @JsonProperty("channel") String channel,
                @JsonProperty("presences") List<PresenceEntry> presences) {
            this.type = type;
            this.channel = channel;
            this.presences = presences;
        }
    }

    /**
     * {@code {"type":"presence_diff","channel":…,"joins":[…],"leaves":[…]}} —
     * incremental presence changes.
     */
    public static final class PresenceDiff {
        @JsonProperty("type") public final String type;
        @JsonProperty("channel") public final String channel;
        @JsonProperty("joins") public final List<PresenceEntry> joins;
        @JsonProperty("leaves") public final List<PresenceEntry> leaves;

        public PresenceDiff(
                @JsonProperty("type") String type,
                @JsonProperty("channel") String channel,
                @JsonProperty("joins") List<PresenceEntry> joins,
                @JsonProperty("leaves") List<PresenceEntry> leaves) {
            this.type = type;
            this.channel = channel;
            this.joins = joins;
            this.leaves = leaves;
        }
    }

    /**
     * {@code {"type":"presenceerror","code":…,"channel":…,"message":…}} —
     * rejected presence operation (e.g. identity mismatch).
     *
     * <p>Note: the wire type is {@code "presenceerror"} (no underscore) due to
     * Rust's {@code rename_all = "lowercase"} on the {@code PresenceError} variant.
     */
    public static final class PresenceError {
        @JsonProperty("type") public final String type;
        @JsonProperty("code") public final String code;
        @JsonProperty("channel") public final String channel;
        @JsonProperty("message") public final String message;

        public PresenceError(
                @JsonProperty("type") String type,
                @JsonProperty("code") String code,
                @JsonProperty("channel") String channel,
                @JsonProperty("message") String message) {
            this.type = type;
            this.code = code;
            this.channel = channel;
            this.message = message;
        }
    }
}
