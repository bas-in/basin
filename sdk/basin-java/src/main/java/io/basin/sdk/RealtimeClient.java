package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WebSocket realtime client for {@code GET /realtime/v1/ws/:project}.
 *
 * <p>Protocol source of truth: {@code crates/basin-realtime/src/ws.rs}
 * (ClientMsg / ServerMsg + {@code presence.rs} serialize_presence_* helpers).
 *
 * <h2>Auth</h2>
 * <p>JWT auth is performed during the WebSocket upgrade handshake.
 * The token is sent as the subprotocol header value:
 * {@code Sec-WebSocket-Protocol: basin-v1, <token>}.
 * The server also accepts {@code Authorization: Bearer} but the subprotocol
 * form is used here for browser-compatibility.
 *
 * <h2>Protocol messages (client → server)</h2>
 * <pre>
 * {"type":"subscribe","table":"orders"}
 * {"type":"subscribe","table":"users","filter":"NEW.status = 'paid'"}
 * {"type":"unsubscribe","table":"orders"}
 * {"type":"presence_track","channel":"room:1","client_id":"c1","metadata":{}}
 * {"type":"presence_untrack","channel":"room:1","client_id":"c1"}
 * {"type":"heartbeat","channel":"room:1","client_id":"c1"}
 * </pre>
 *
 * <h2>Reconnect</h2>
 * <p>On unexpected disconnect, reconnects with exponential backoff
 * (0.5 s → 1 s → 2 s … capped at 30 s) and automatically re-issues all
 * active subscriptions. Pass {@code lastEventId} to {@link #subscribe} to
 * request server-side replay of missed events.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * ChannelHandle handle = client.realtime().subscribe("orders",
 *     event -> System.out.println(event),
 *     null, null);
 * // … later:
 * handle.unsubscribe();
 * client.realtime().close();
 * }</pre>
 *
 * <p>Obtain via {@link BasinClient#realtime()}.
 */
public final class RealtimeClient {

    private static final Logger LOG = Logger.getLogger(RealtimeClient.class.getName());

    private static final double BACKOFF_BASE_MS = 500.0;
    private static final double BACKOFF_FACTOR = 2.0;
    private static final long BACKOFF_MAX_MS = 30_000L;

    private final String baseUrl;
    private final Supplier<String> tokenSupplier;
    private final Supplier<String> projectIdSupplier;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    // -- Mutable state protected by stateLock --
    private final Object stateLock = new Object();
    private WebSocket ws;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Active subscriptions: table → { filter, lastEventId }
    private final Map<String, SubscriptionOpts> subscriptions = new ConcurrentHashMap<>();

    // Per-channel/table event queues: key → list of queues
    private final Map<String, java.util.concurrent.CopyOnWriteArrayList<BlockingQueue<Object>>> queues = new ConcurrentHashMap<>();

    // Pending ack futures
    private final ConcurrentHashMap<String, CompletableFuture<Void>> pendingSubscribed
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> pendingUnsubscribed
            = new ConcurrentHashMap<>();

    // Internal sentinel for closed/disconnected queues
    static final Object SENTINEL = new Object();

    RealtimeClient(String baseUrl, Supplier<String> tokenSupplier,
                   Supplier<String> projectIdSupplier, HttpClient httpClient,
                   ObjectMapper mapper) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.tokenSupplier = tokenSupplier;
        this.projectIdSupplier = projectIdSupplier;
        this.httpClient = httpClient;
        this.mapper = mapper;
    }

    // ------------------------------------------------------------------
    // Connection management
    // ------------------------------------------------------------------

    /**
     * Open the WebSocket connection. No-op if already connected.
     *
     * @return future that completes when the handshake is done
     */
    public CompletableFuture<Void> connect() {
        synchronized (stateLock) {
            if (ws != null) return CompletableFuture.completedFuture(null);
        }
        return doConnect();
    }

    private CompletableFuture<Void> doConnect() {
        String project = projectIdSupplier.get();
        if (project == null || project.isEmpty()) {
            return CompletableFuture.failedFuture(new BasinApiException("E_INVALID_REQUEST",
                    "realtime requires a project id — pass projectId to BasinClient.Builder", 0));
        }
        String token = tokenSupplier.get();
        if (token == null || token.isEmpty()) {
            return CompletableFuture.failedFuture(new BasinApiException("E_UNAUTHENTICATED",
                    "realtime requires a JWT or API key", 0));
        }

        String wsBase = baseUrl
                .replace("https://", "wss://")
                .replace("http://", "ws://");
        URI uri = URI.create(wsBase + "/realtime/v1/ws/" + project);

        // Auth via subprotocol header: "basin-v1, <token>"
        // java.net.http.WebSocket.Builder.subprotocols(String, String...) sets
        // Sec-WebSocket-Protocol. We pass "basin-v1" as the primary and the token
        // as the additional subprotocol value — the server accepts this form.
        WebSocket.Builder builder = httpClient.newWebSocketBuilder()
                .subprotocols("basin-v1", token);

        return builder.buildAsync(uri, new Listener())
                .thenAccept(socket -> {
                    synchronized (stateLock) {
                        ws = socket;
                    }
                    LOG.fine("realtime: connected to " + uri);
                });
    }

    /** Close the WebSocket connection and cancel all active subscriptions. */
    public void close() {
        closed.set(true);
        drainQueues();
        WebSocket socket;
        synchronized (stateLock) {
            socket = ws;
            ws = null;
        }
        if (socket != null) {
            try {
                socket.sendClose(WebSocket.NORMAL_CLOSURE, "client closed").join();
            } catch (Exception ignored) {
            }
        }
    }

    // ------------------------------------------------------------------
    // Send helpers
    // ------------------------------------------------------------------

    private CompletableFuture<Void> send(String json) {
        WebSocket socket;
        synchronized (stateLock) {
            socket = ws;
        }
        if (socket == null) {
            return CompletableFuture.failedFuture(
                    new BasinApiException("E_UNKNOWN", "realtime socket is not open", 0));
        }
        return socket.sendText(json, true).thenApply(ws -> null);
    }

    private CompletableFuture<Void> sendMessage(Map<String, Object> msg) {
        try {
            return send(mapper.writeValueAsString(msg));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new BasinException("Failed to serialize message", e));
        }
    }

    // ------------------------------------------------------------------
    // Subscribe / Unsubscribe
    // ------------------------------------------------------------------

    private CompletableFuture<Void> sendSubscribe(String table, SubscriptionOpts opts) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("type", "subscribe");
        msg.put("table", table);
        if (opts.filter != null) msg.put("filter", opts.filter);
        if (opts.lastEventId != null) msg.put("last_event_id", opts.lastEventId);
        return sendMessage(msg);
    }

    private CompletableFuture<Void> doSubscribe(String table, SubscriptionOpts opts) {
        subscriptions.put(table, opts);
        queues.computeIfAbsent(table, k -> new java.util.concurrent.CopyOnWriteArrayList<>());

        CompletableFuture<Void> ack = new CompletableFuture<>();
        pendingSubscribed.put(table, ack);

        return connect()
                .thenCompose(v -> sendSubscribe(table, opts))
                .thenCompose(v -> ack.orTimeout(30, TimeUnit.SECONDS));
    }

    private CompletableFuture<Void> doUnsubscribe(String table) {
        subscriptions.remove(table);
        CompletableFuture<Void> ack = new CompletableFuture<>();
        pendingUnsubscribed.put(table, ack);
        return sendMessage(Map.of("type", "unsubscribe", "table", table))
                .thenCompose(v -> ack.orTimeout(10, TimeUnit.SECONDS))
                .exceptionally(e -> null); // tolerate timeout
    }

    // ------------------------------------------------------------------
    // Public API — callback-based
    // ------------------------------------------------------------------

    /**
     * Subscribe to {@code table} with a callback.
     *
     * <p>Spawns a background thread that delivers events to the callback.
     * Call {@link ChannelHandle#unsubscribe()} to stop.
     *
     * @param table       table name
     * @param callback    called for each {@link RealtimeEvent}, {@link RealtimeFrame.Error},
     *                    {@link RealtimeFrame.Gap}, or any other frame object
     * @param filter      optional server-side SQL predicate, e.g. {@code "NEW.status='paid'"}
     * @param lastEventId reconnect cursor — last {@code seq} processed; {@code null} to start fresh
     * @return future completing with a {@link ChannelHandle} once subscribed
     */
    public CompletableFuture<ChannelHandle> subscribe(
            String table,
            Consumer<Object> callback,
            String filter,
            Long lastEventId) {

        SubscriptionOpts opts = new SubscriptionOpts(filter, lastEventId);
        BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

        java.util.concurrent.CopyOnWriteArrayList<BlockingQueue<Object>> qs = queues.computeIfAbsent(table, k -> new java.util.concurrent.CopyOnWriteArrayList<>());
        qs.add(queue);

        AtomicBoolean stopped = new AtomicBoolean(false);
        Thread driver = Thread.ofVirtual().start(() -> {
            while (!stopped.get()) {
                try {
                    Object item = queue.poll(1, TimeUnit.SECONDS);
                    if (item == null) continue;
                    if (item == SENTINEL) break;
                    if (!stopped.get()) callback.accept(item);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        return doSubscribe(table, opts).thenApply(v ->
                new ChannelHandle(table, queue, driver, stopped, this));
    }

    // ------------------------------------------------------------------
    // Presence
    // ------------------------------------------------------------------

    /**
     * Send {@code {"type":"presence_track",…}} to register in a presence channel.
     *
     * <p>The server enforces that {@code clientId} matches the authenticated identity.
     */
    public CompletableFuture<Void> presenceTrack(String channel, String clientId, Object metadata) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("type", "presence_track");
        msg.put("channel", channel);
        msg.put("client_id", clientId);
        msg.put("metadata", metadata != null ? metadata : Map.of());
        return connect().thenCompose(v -> sendMessage(msg));
    }

    /** Send {@code {"type":"presence_untrack",…}}. */
    public CompletableFuture<Void> presenceUntrack(String channel, String clientId) {
        return connect().thenCompose(v -> sendMessage(
                Map.of("type", "presence_untrack", "channel", channel, "client_id", clientId)));
    }

    /** Send {@code {"type":"heartbeat",…}} to refresh the presence TTL. */
    public CompletableFuture<Void> presenceHeartbeat(String channel, String clientId) {
        return connect().thenCompose(v -> sendMessage(
                Map.of("type", "heartbeat", "channel", channel, "client_id", clientId)));
    }

    // ------------------------------------------------------------------
    // Frame dispatch
    // ------------------------------------------------------------------

    private void dispatch(Object frame) {
        String key = tableKey(frame);
        if (key == null) return;
        java.util.concurrent.CopyOnWriteArrayList<BlockingQueue<Object>> qs = queues.get(key);
        if (qs != null) {
            for (BlockingQueue<Object> q : qs) {
                q.offer(frame);
            }
        }
    }

    private String tableKey(Object frame) {
        if (frame instanceof RealtimeEvent e) return e.table;
        if (frame instanceof RealtimeFrame.Error e) return e.table;
        if (frame instanceof RealtimeFrame.Gap g) return g.table;
        if (frame instanceof RealtimeFrame.Subscribed s) return s.table;
        if (frame instanceof RealtimeFrame.Unsubscribed u) return u.table;
        if (frame instanceof RealtimeFrame.PresenceState ps) return ps.channel;
        if (frame instanceof RealtimeFrame.PresenceDiff pd) return pd.channel;
        if (frame instanceof RealtimeFrame.PresenceError pe) return pe.channel;
        return null;
    }

    private Object parseFrame(String raw) {
        try {
            JsonNode node = mapper.readTree(raw);
            String type = node.has("type") ? node.get("type").asText() : null;
            if (type == null) return null;
            return switch (type) {
                case "event"         -> mapper.treeToValue(node, RealtimeEvent.class);
                case "error"         -> mapper.treeToValue(node, RealtimeFrame.Error.class);
                case "gap"           -> mapper.treeToValue(node, RealtimeFrame.Gap.class);
                case "subscribed"    -> {
                    var f = mapper.treeToValue(node, RealtimeFrame.Subscribed.class);
                    CompletableFuture<Void> ack = pendingSubscribed.remove(f.table);
                    if (ack != null) ack.complete(null);
                    yield f;
                }
                case "unsubscribed"  -> {
                    var f = mapper.treeToValue(node, RealtimeFrame.Unsubscribed.class);
                    CompletableFuture<Void> ack = pendingUnsubscribed.remove(f.table);
                    if (ack != null) ack.complete(null);
                    yield f;
                }
                case "presence_state" -> mapper.treeToValue(node, RealtimeFrame.PresenceState.class);
                case "presence_diff"  -> mapper.treeToValue(node, RealtimeFrame.PresenceDiff.class);
                case "presenceerror"  -> mapper.treeToValue(node, RealtimeFrame.PresenceError.class);
                default -> null;
            };
        } catch (Exception e) {
            LOG.log(Level.FINE, "realtime: failed to parse frame: " + raw, e);
            return null;
        }
    }

    private void drainQueues() {
        for (java.util.concurrent.CopyOnWriteArrayList<BlockingQueue<Object>> qs : queues.values()) {
            for (BlockingQueue<Object> q : qs) {
                q.offer(SENTINEL);
            }
        }
    }

    // ------------------------------------------------------------------
    // Reconnect with backoff
    // ------------------------------------------------------------------

    private void scheduleReconnect() {
        if (closed.get()) return;
        Thread.ofVirtual().start(() -> {
            int attempt = 0;
            while (!closed.get()) {
                long delayMs = Math.min((long) (BACKOFF_BASE_MS * Math.pow(BACKOFF_FACTOR, attempt)),
                        BACKOFF_MAX_MS);
                LOG.fine("realtime: reconnect in " + delayMs + "ms (attempt " + (attempt + 1) + ")");
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                attempt++;
                synchronized (stateLock) {
                    if (ws != null) return; // already reconnected
                }
                try {
                    doConnect().join();
                    resubscribe();
                    return;
                } catch (Exception e) {
                    LOG.fine("realtime: reconnect attempt " + attempt + " failed: " + e.getMessage());
                }
            }
        });
    }

    private void resubscribe() {
        for (Map.Entry<String, SubscriptionOpts> entry : subscriptions.entrySet()) {
            try {
                CompletableFuture<Void> ack = new CompletableFuture<>();
                pendingSubscribed.put(entry.getKey(), ack);
                sendSubscribe(entry.getKey(), entry.getValue()).join();
                ack.orTimeout(10, TimeUnit.SECONDS).join();
            } catch (Exception e) {
                LOG.fine("realtime: resubscribe " + entry.getKey() + " failed: " + e.getMessage());
            }
        }
    }

    // ------------------------------------------------------------------
    // WebSocket listener
    // ------------------------------------------------------------------

    private final class Listener implements WebSocket.Listener {
        private final StringBuilder textAccum = new StringBuilder();

        @Override
        public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
            textAccum.append(data);
            if (last) {
                String raw = textAccum.toString();
                textAccum.setLength(0);
                Object frame = parseFrame(raw);
                if (frame != null) dispatch(frame);
            }
            socket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket socket, int statusCode, String reason) {
            LOG.fine("realtime: closed (code=" + statusCode + ", reason=" + reason + ")");
            synchronized (stateLock) {
                ws = null;
            }
            if (!closed.get()) scheduleReconnect();
            return null;
        }

        @Override
        public void onError(WebSocket socket, Throwable error) {
            LOG.log(Level.FINE, "realtime: error", error);
            synchronized (stateLock) {
                ws = null;
            }
            if (!closed.get()) scheduleReconnect();
        }

        @Override
        public void onOpen(WebSocket socket) {
            socket.request(1);
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    void removeQueue(String table, BlockingQueue<Object> queue) {
        java.util.concurrent.CopyOnWriteArrayList<BlockingQueue<Object>> qs = queues.get(table);
        if (qs != null) {
            qs.remove(queue);
            if (qs.isEmpty()) {
                doUnsubscribe(table);
            }
        }
    }

    // ------------------------------------------------------------------
    // Nested types
    // ------------------------------------------------------------------

    private record SubscriptionOpts(String filter, Long lastEventId) {}
}
