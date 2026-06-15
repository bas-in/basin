package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Server → client event frame: {@code {"type":"event",…}}.
 *
 * <p>Emitted for INSERT, UPDATE, and DELETE operations on subscribed tables.
 */
public final class RealtimeEvent {

    @JsonProperty("type")
    public final String type; // "event"

    @JsonProperty("project")
    public final String project;

    @JsonProperty("table")
    public final String table;

    /** {@code "INSERT"}, {@code "UPDATE"}, or {@code "DELETE"}. */
    @JsonProperty("op")
    public final String op;

    /** Monotonically increasing event sequence number. */
    @JsonProperty("seq")
    public final long seq;

    /** Row state before the change; present for UPDATE/DELETE when enabled. */
    @JsonProperty("before")
    public final Map<String, Object> before;

    /** Row state after the change; present for INSERT/UPDATE. */
    @JsonProperty("after")
    public final Map<String, Object> after;

    public RealtimeEvent(
            @JsonProperty("type") String type,
            @JsonProperty("project") String project,
            @JsonProperty("table") String table,
            @JsonProperty("op") String op,
            @JsonProperty("seq") long seq,
            @JsonProperty("before") Map<String, Object> before,
            @JsonProperty("after") Map<String, Object> after) {
        this.type = type;
        this.project = project;
        this.table = table;
        this.op = op;
        this.seq = seq;
        this.before = before;
        this.after = after;
    }
}
