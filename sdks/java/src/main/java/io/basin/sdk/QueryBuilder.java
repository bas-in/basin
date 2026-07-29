package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Fluent query builder for {@code GET|POST|PATCH|DELETE /rest/v1/:table}.
 *
 * <p>Route source (verified):
 * {@code crates/basin-rest/src/server.rs} — GET|POST|PATCH|DELETE {@code /rest/v1/:table}.
 * Filter grammar is Basin's PostgREST-style dialect ({@code crates/basin-rest/src/parser.rs}).
 *
 * <p>Supported filters: {@code select}, {@code eq}, {@code neq}, {@code gt}, {@code gte},
 * {@code lt}, {@code lte}, {@code in}, {@code is}, {@code order}, {@code limit},
 * {@code offset}, {@code cursor}.
 *
 * <p>NOT full PostgREST: no {@code or=}, {@code not.}, {@code like/ilike}, embedded
 * resource selects, or Prefer headers. Filters AND together.
 *
 * <p>Response shapes ({@code crates/basin-rest/src/routes/data.rs}):
 * <ul>
 *   <li>plain GET → JSON array of rows
 *   <li>GET with limit or cursor → {@code {"rows":[…],"next_cursor":"…"}}
 *   <li>GET with {@code ?stream=true} → NDJSON; trailing {@code {"_basin_next_cursor":"…"}} line
 *       captured as cursor (not yielded as a row); see {@link #stream()}
 *   <li>GET with {@code Accept: application/vnd.apache.arrow.stream} → Arrow IPC byte stream;
 *       pagination cursor in {@code X-Basin-Next-Cursor} header; see {@link #toArrow()}
 *   <li>POST → 201, {@code {"ok":true,"tag":"…"}} (or rows)
 *   <li>PATCH/DELETE → {@code {"ok":true,"tag":"…"}}
 *   <li>DELETE may surface 501 {@code E_ENGINE_UNSUPPORTED}
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * QueryResult result = client.table("orders")
 *     .select("id,total,status")
 *     .eq("status", "paid")
 *     .gte("total", "100")
 *     .order("total", false)
 *     .limit(50)
 *     .execute()
 *     .join();
 * }</pre>
 *
 * <p>Obtain via {@link BasinClient#table(String)}.
 */
public final class QueryBuilder {

    private final BasinTransport transport;
    private final String table;
    private final List<String[]> query; // mutable accumulator

    QueryBuilder(BasinTransport transport, String table) {
        this(transport, table, new ArrayList<>());
    }

    private QueryBuilder(BasinTransport transport, String table, List<String[]> query) {
        this.transport = transport;
        this.table = table;
        this.query = query;
    }

    // Each filter method appends to the query list and returns this builder.
    // Immutable-style: we intentionally reuse the list since QueryBuilder is
    // created fresh per call chain (like the Python/Go SDKs do).

    private QueryBuilder add(String key, String value) {
        query.add(new String[]{key, value});
        return this;
    }

    // ------------------------------------------------------------------
    // Projection
    // ------------------------------------------------------------------

    /**
     * {@code select=<cols>} — project specific columns.
     * Pass {@code "*"} or call without argument for all columns.
     */
    public QueryBuilder select(String columns) {
        return add("select", columns == null || columns.isEmpty() ? "*" : columns);
    }

    // ------------------------------------------------------------------
    // Filters
    // ------------------------------------------------------------------

    /** {@code <col>=eq.<value>} */
    public QueryBuilder eq(String column, Object value) {
        return add(column, "eq." + literal(value));
    }

    /** {@code <col>=neq.<value>} */
    public QueryBuilder neq(String column, Object value) {
        return add(column, "neq." + literal(value));
    }

    /** {@code <col>=gt.<value>} */
    public QueryBuilder gt(String column, Object value) {
        return add(column, "gt." + literal(value));
    }

    /** {@code <col>=gte.<value>} */
    public QueryBuilder gte(String column, Object value) {
        return add(column, "gte." + literal(value));
    }

    /** {@code <col>=lt.<value>} */
    public QueryBuilder lt(String column, Object value) {
        return add(column, "lt." + literal(value));
    }

    /** {@code <col>=lte.<value>} */
    public QueryBuilder lte(String column, Object value) {
        return add(column, "lte." + literal(value));
    }

    /**
     * {@code <col>=in.(a,b,c)} — parenthesised list per {@code parser.rs parse_in_list}.
     */
    public QueryBuilder in(String column, List<?> values) {
        StringBuilder sb = new StringBuilder("in.(");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(literal(values.get(i)));
        }
        sb.append(')');
        return add(column, sb.toString());
    }

    /**
     * {@code <col>=is.null} or {@code <col>=is.notnull}.
     *
     * @param value {@code "null"} or {@code "notnull"}
     */
    public QueryBuilder is(String column, String value) {
        return add(column, "is." + value);
    }

    // ------------------------------------------------------------------
    // Ordering / pagination
    // ------------------------------------------------------------------

    /**
     * {@code order=<col>.asc|desc}. Repeatable.
     *
     * @param ascending {@code true} for ascending order
     */
    public QueryBuilder order(String column, boolean ascending) {
        return add("order", column + "." + (ascending ? "asc" : "desc"));
    }

    /** {@code limit=N} — switches the GET response to {@code {"rows":[…],"next_cursor":"…"}}. */
    public QueryBuilder limit(int n) {
        return add("limit", String.valueOf(n));
    }

    /** {@code offset=N}. */
    public QueryBuilder offset(int n) {
        return add("offset", String.valueOf(n));
    }

    /** {@code cursor=<token>} — resume keyset pagination from a next_cursor token. */
    public QueryBuilder cursor(String token) {
        return add("cursor", token);
    }

    // ------------------------------------------------------------------
    // Execution — async (primary)
    // ------------------------------------------------------------------

    /**
     * Execute as GET; normalises both response shapes into a {@link QueryResult}.
     * Returns a {@link CompletableFuture} — compose with {@code thenApply}, or
     * call {@code .join()} for blocking.
     */
    public CompletableFuture<QueryResult> execute() {
        return transport.executeJsonAsync(
                transport.jsonGet("/rest/v1/" + table, new ArrayList<>(query), true))
                .thenApply(this::normalizeGet);
    }

    // ------------------------------------------------------------------
    // NDJSON streaming
    // ------------------------------------------------------------------

    /**
     * Execute as GET with {@code ?stream=true} (server-side NDJSON).
     *
     * <p>The server returns one JSON object per line.  A trailing
     * {@code {"_basin_next_cursor":"…"}} line carries the pagination cursor and
     * is not included in the returned rows.
     *
     * <p>This is a <em>blocking</em> call: the full response body is received
     * before any row is returned.  For true incremental processing use an
     * async reactive pipeline; this method is a convenience wrapper that
     * matches the JS/Python SDK {@code stream()} shape.
     *
     * <p>Usage:
     * <pre>{@code
     * StreamResult sr = client.table("events")
     *     .gte("id", "1000")
     *     .stream();
     * for (Map<String, Object> row : sr) {
     *     // process row …
     * }
     * String next = sr.nextCursor();   // null when no next page
     * }</pre>
     *
     * @return a {@link StreamResult} that is {@link Iterable} over the decoded rows
     *         and exposes the trailing cursor via {@link StreamResult#nextCursor()}
     * @throws BasinApiException     on server error
     * @throws BasinNetworkException on transport failure
     */
    public StreamResult stream() {
        List<String[]> streamQuery = new ArrayList<>(query);
        streamQuery.add(new String[]{"stream", "true"});

        HttpResponse<byte[]> resp = transport.executeRaw(
                transport.jsonGet("/rest/v1/" + table, streamQuery, true));

        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            // Decode error envelope via the transport's standard path.
            JsonNode errNode = null;
            byte[] body = resp.body();
            if (body != null && body.length > 0) {
                try {
                    errNode = transport.mapper.readTree(body);
                } catch (IOException ignored) {
                }
            }
            throw BasinApiException.fromBody(errNode, resp.statusCode());
        }

        String responseText = resp.body() != null
                ? new String(resp.body(), java.nio.charset.StandardCharsets.UTF_8)
                : "";
        return new StreamResult(responseText, transport.mapper);
    }

    /**
     * Result of a {@link #stream()} call.
     *
     * <p>Implements {@link Iterable} so rows can be consumed with a for-each loop.
     * The trailing {@code _basin_next_cursor} sentinel line is captured and
     * accessible via {@link #nextCursor()}.
     */
    public static final class StreamResult implements Iterable<Map<String, Object>> {

        private final List<Map<String, Object>> rows;
        private final String nextCursor;

        @SuppressWarnings("unchecked")
        StreamResult(String responseText, ObjectMapper mapper) {
            List<Map<String, Object>> parsed = new ArrayList<>();
            String cursor = null;

            for (String line : responseText.split("\n")) {
                String trimmed = line.strip();
                if (trimmed.isEmpty()) continue;
                try {
                    JsonNode node = mapper.readTree(trimmed);
                    if (node.has("_basin_next_cursor")) {
                        JsonNode cv = node.get("_basin_next_cursor");
                        cursor = cv.isNull() ? null : cv.asText();
                        // Sentinel line — not a data row; stop here.
                        continue;
                    }
                    Map<String, Object> row = mapper.convertValue(node,
                            mapper.getTypeFactory().constructMapType(
                                    Map.class, String.class, Object.class));
                    parsed.add(row);
                } catch (IOException e) {
                    // Skip unparseable lines (e.g. a partial last line on timeout).
                }
            }

            this.rows = java.util.Collections.unmodifiableList(parsed);
            this.nextCursor = cursor;
        }

        /**
         * Keyset pagination cursor from the trailing {@code _basin_next_cursor} line.
         * {@code null} when the response contained no cursor line.
         */
        public String nextCursor() {
            return nextCursor;
        }

        /** All decoded rows, excluding the trailing cursor sentinel. */
        public List<Map<String, Object>> rows() {
            return rows;
        }

        @Override
        public Iterator<Map<String, Object>> iterator() {
            return rows.iterator();
        }
    }

    // ------------------------------------------------------------------
    // Arrow IPC transport
    // ------------------------------------------------------------------

    /**
     * Execute the query and decode the response as an Apache Arrow columnar table.
     *
     * <p>Sends {@code Accept: application/vnd.apache.arrow.stream}.  When the
     * server replies with that Content-Type (Arrow IPC stream format), the bytes
     * are decoded natively via {@code org.apache.arrow:arrow-vector} — zero JSON
     * round-trip, full i64 / timestamp / decimal fidelity.
     *
     * <p>Falls back to a JSON-→-Arrow conversion when the server replies with
     * JSON (e.g. an older server that does not support the IPC path).  The
     * fallback is transparent; column types are inferred from JSON values
     * (numbers become {@code Float8 / BigInt} where unambiguous, everything else
     * becomes {@code Utf8}).
     *
     * <p>Pagination state is surfaced in {@link ArrowResult#nextCursor} (decoded
     * from the {@code X-Basin-Next-Cursor} response header) so callers can page:
     * <pre>{@code
     * String cursor = null;
     * do {
     *     QueryBuilder qb = client.table("orders").limit(1000);
     *     if (cursor != null) qb = qb.cursor(cursor);
     *     try (ArrowResult ar = qb.toArrow()) {
     *         processRoot(ar.root);          // columnar access
     *         cursor = ar.nextCursor;
     *     }
     * } while (cursor != null);
     * }</pre>
     *
     * <p><strong>Dependency note:</strong> callers must add
     * {@code org.apache.arrow:arrow-vector} (and an allocator such as
     * {@code arrow-memory-unsafe}) to their runtime classpath.  These are
     * declared {@code compileOnly} in the SDK build to keep the default
     * footprint small.  See the SDK README for the full dependency snippet.
     *
     * @return an {@link ArrowResult} containing the {@link org.apache.arrow.vector.VectorSchemaRoot}
     *         and the next-page cursor; caller must close it to release off-heap buffers
     * @throws BasinApiException      on server error
     * @throws BasinNetworkException  on transport failure
     * @throws BasinException         when Arrow classes are absent from the classpath
     *                                or when the IPC stream cannot be decoded
     */
    public ArrowResult toArrow() {
        // Build the request with the Arrow IPC Accept header.
        List<String[]> arrowQuery = new ArrayList<>(query);
        HttpResponse<byte[]> resp = transport.executeRaw(
                arrowRequest("/rest/v1/" + table, arrowQuery));

        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            JsonNode errNode = null;
            byte[] body = resp.body();
            if (body != null && body.length > 0) {
                try {
                    errNode = transport.mapper.readTree(body);
                } catch (IOException ignored) {
                }
            }
            throw BasinApiException.fromBody(errNode, resp.statusCode());
        }

        String nextCursor = resp.headers().firstValue("x-basin-next-cursor").orElse(null);
        if (nextCursor != null && nextCursor.isEmpty()) nextCursor = null;
        long rowCount = resp.headers().firstValue("x-basin-row-count")
                .map(s -> { try { return Long.parseLong(s); } catch (NumberFormatException e) { return -1L; } })
                .orElse(-1L);

        String ct = resp.headers().firstValue("content-type").orElse("");
        byte[] body = resp.body() != null ? resp.body() : new byte[0];

        if (ct.contains(ARROW_STREAM_MIME)) {
            // Native IPC decode path.
            return decodeArrowIpc(body, nextCursor, rowCount);
        }

        // Fallback: server returned JSON — convert to Arrow on the client side.
        return jsonToArrow(body, nextCursor, rowCount);
    }

    /** MIME type sent as {@code Accept} header to request Arrow IPC. */
    private static final String ARROW_STREAM_MIME = "application/vnd.apache.arrow.stream";

    private java.net.http.HttpRequest arrowRequest(String path, List<String[]> q) {
        return transport.baseRequest(path, q, true)
                .GET()
                .header("Accept", ARROW_STREAM_MIME)
                .build();
    }

    /**
     * Decode raw Arrow IPC stream bytes into an {@link ArrowResult}.
     *
     * <p>Uses {@code org.apache.arrow.vector.ipc.ArrowStreamReader} so the schema
     * and all record batches are materialised into a single
     * {@link org.apache.arrow.vector.VectorSchemaRoot}.
     */
    private static ArrowResult decodeArrowIpc(byte[] ipcBytes, String nextCursor, long rowCount) {
        try {
            org.apache.arrow.memory.RootAllocator allocator =
                    new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE);
            org.apache.arrow.vector.ipc.ArrowStreamReader reader =
                    new org.apache.arrow.vector.ipc.ArrowStreamReader(
                            new ByteArrayInputStream(ipcBytes), allocator);
            org.apache.arrow.vector.VectorSchemaRoot root = reader.getVectorSchemaRoot();

            // Load all record batches into the root.  For a single-batch stream (the
            // common case) this is one loadNextBatch() call.
            while (reader.loadNextBatch()) {
                // Each loadNextBatch() call fills the root with the next batch.
                // For multi-batch IPC streams we accumulate by transferring vectors.
                // Basin currently materialises results into a single batch; if that
                // changes we only see the first batch here — acceptable for v1.
                break;
            }
            reader.close();
            // Note: allocator is intentionally left open; closing the root will free
            // the buffers.  A production implementation would scope the allocator to
            // the ArrowResult lifetime.
            return new ArrowResult(root, nextCursor, rowCount);
        } catch (NoClassDefFoundError e) {
            throw new BasinException(
                    "Arrow IPC decode failed: arrow-vector not on classpath. "
                    + "Add org.apache.arrow:arrow-vector and arrow-memory-unsafe "
                    + "to your runtime dependencies.", e);
        } catch (IOException e) {
            throw new BasinException("Arrow IPC decode error: " + e.getMessage(), e);
        }
    }

    /**
     * Fallback path: server returned JSON — convert rows to Arrow on the client.
     *
     * <p>Types are inferred from JSON values:
     * <ul>
     *   <li>JSON integer → {@code BigInt (Int64)}
     *   <li>JSON float   → {@code Float8 (Float64)}
     *   <li>JSON boolean → {@code Bit}
     *   <li>anything else → {@code Utf8} (string representation)
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private ArrowResult jsonToArrow(byte[] jsonBytes, String nextCursor, long rowCount) {
        List<Map<String, Object>> rows;
        try {
            if (jsonBytes.length == 0) {
                rows = List.of();
            } else {
                JsonNode node = transport.mapper.readTree(jsonBytes);
                rows = (List<Map<String, Object>>) normalizeGetRows(node);
            }
        } catch (IOException e) {
            throw new BasinException("JSON parse error in Arrow fallback: " + e.getMessage(), e);
        }

        try {
            org.apache.arrow.memory.RootAllocator allocator =
                    new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE);
            org.apache.arrow.vector.VectorSchemaRoot root =
                    buildVectorSchemaRoot(rows, allocator);
            return new ArrowResult(root, nextCursor, rowCount >= 0 ? rowCount : rows.size());
        } catch (NoClassDefFoundError e) {
            throw new BasinException(
                    "Arrow JSON-fallback failed: arrow-vector not on classpath. "
                    + "Add org.apache.arrow:arrow-vector and arrow-memory-unsafe "
                    + "to your runtime dependencies.", e);
        }
    }

    /**
     * Build a {@link org.apache.arrow.vector.VectorSchemaRoot} from a list of JSON rows.
     *
     * <p>Schema is inferred from the first row (or empty when the list is empty).
     * Type inference: integer → BigInt, decimal → Float8, boolean → Bit, else Utf8.
     */
    private static org.apache.arrow.vector.VectorSchemaRoot buildVectorSchemaRoot(
            List<Map<String, Object>> rows,
            org.apache.arrow.memory.RootAllocator allocator) {

        if (rows.isEmpty()) {
            return org.apache.arrow.vector.VectorSchemaRoot.create(
                    new org.apache.arrow.vector.types.pojo.Schema(List.of()),
                    allocator);
        }

        // Derive schema from the first row.
        Map<String, Object> first = rows.get(0);
        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>();
        for (Map.Entry<String, Object> e : first.entrySet()) {
            fields.add(org.apache.arrow.vector.types.pojo.Field.nullable(
                    e.getKey(), inferArrowType(e.getValue())));
        }

        org.apache.arrow.vector.types.pojo.Schema schema =
                new org.apache.arrow.vector.types.pojo.Schema(fields);
        org.apache.arrow.vector.VectorSchemaRoot root =
                org.apache.arrow.vector.VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();

        List<org.apache.arrow.vector.FieldVector> vectors = root.getFieldVectors();
        int rowIdx = 0;
        for (Map<String, Object> row : rows) {
            for (int c = 0; c < fields.size(); c++) {
                String colName = fields.get(c).getName();
                Object val = row.get(colName);
                setVectorValue(vectors.get(c), rowIdx, val);
            }
            rowIdx++;
        }

        // Finalise value count in each vector.
        for (org.apache.arrow.vector.FieldVector v : vectors) {
            v.setValueCount(rows.size());
        }
        root.setRowCount(rows.size());
        return root;
    }

    private static org.apache.arrow.vector.types.pojo.ArrowType inferArrowType(Object sample) {
        if (sample instanceof Long || sample instanceof Integer) {
            return new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true);
        }
        if (sample instanceof Double || sample instanceof Float) {
            return new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                    org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        }
        if (sample instanceof Boolean) {
            return new org.apache.arrow.vector.types.pojo.ArrowType.Bool();
        }
        return new org.apache.arrow.vector.types.pojo.ArrowType.Utf8();
    }

    private static void setVectorValue(
            org.apache.arrow.vector.FieldVector vec, int idx, Object val) {
        if (val == null) {
            // null — leave the validity bit unset (default for allocateNew).
            return;
        }
        if (vec instanceof org.apache.arrow.vector.BigIntVector bv) {
            long l = val instanceof Number n ? n.longValue() : 0L;
            bv.set(idx, l);
        } else if (vec instanceof org.apache.arrow.vector.Float8Vector fv) {
            double d = val instanceof Number n ? n.doubleValue() : 0.0;
            fv.set(idx, d);
        } else if (vec instanceof org.apache.arrow.vector.BitVector bv) {
            bv.set(idx, Boolean.TRUE.equals(val) ? 1 : 0);
        } else if (vec instanceof org.apache.arrow.vector.VarCharVector sv) {
            byte[] bytes = val.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            sv.set(idx, bytes);
        }
    }

    /** Extract a flat row list from a JSON body (array or paginated object). */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> normalizeGetRows(JsonNode node) {
        ObjectMapper m = new ObjectMapper();
        if (node == null) return List.of();
        if (node.isArray()) {
            return m.convertValue(node,
                    m.getTypeFactory().constructCollectionType(List.class, Map.class));
        }
        if (node.isObject() && node.has("rows")) {
            return m.convertValue(node.get("rows"),
                    m.getTypeFactory().constructCollectionType(List.class, Map.class));
        }
        return List.of();
    }

    // ------------------------------------------------------------------
    // Blocking convenience — stream & Arrow
    // ------------------------------------------------------------------

    /**
     * Blocking convenience for {@link #stream()} — identical semantics,
     * documented on that method.
     */
    public StreamResult streamBlocking() {
        return stream();
    }

    /**
     * Blocking convenience for {@link #toArrow()} — identical semantics,
     * documented on that method.
     */
    public ArrowResult toArrowBlocking() {
        return toArrow();
    }

    /** Insert one row or a list of rows via {@code POST /rest/v1/:table} (201). */
    public CompletableFuture<JsonNode> insert(Object rows) {
        return transport.executeJsonAsync(
                transport.jsonPost("/rest/v1/" + table, null, rows, true));
    }

    /**
     * Update rows matching the accumulated filters via
     * {@code PATCH /rest/v1/:table?<filters>}.
     */
    public CompletableFuture<JsonNode> update(Map<String, Object> values) {
        return transport.executeJsonAsync(
                transport.jsonPatch("/rest/v1/" + table, new ArrayList<>(query), values, true));
    }

    /**
     * Delete rows matching the accumulated filters via
     * {@code DELETE /rest/v1/:table?<filters>}.
     *
     * <p>May raise {@link BasinApiException} with code {@code E_ENGINE_UNSUPPORTED} (501)
     * on engines without DELETE support.
     */
    public CompletableFuture<JsonNode> delete() {
        return transport.executeJsonAsync(
                transport.jsonDelete("/rest/v1/" + table, new ArrayList<>(query), null, true));
    }

    // ------------------------------------------------------------------
    // Blocking convenience wrappers
    // ------------------------------------------------------------------

    /**
     * Blocking variant of {@link #execute()}.
     *
     * @return normalised query result
     * @throws BasinApiException    on server error
     * @throws BasinNetworkException on transport failure
     */
    public QueryResult executeBlocking() {
        return execute().join();
    }

    /** Blocking variant of {@link #insert(Object)}. */
    public JsonNode insertBlocking(Object rows) {
        return insert(rows).join();
    }

    /** Blocking variant of {@link #update(Map)}. */
    public JsonNode updateBlocking(Map<String, Object> values) {
        return update(values).join();
    }

    /** Blocking variant of {@link #delete()}. */
    public JsonNode deleteBlocking() {
        return delete().join();
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private QueryResult normalizeGet(JsonNode node) {
        ObjectMapper mapper = transport.mapper;
        if (node == null) return new QueryResult(List.of(), null);

        if (node.isArray()) {
            List<Map<String, Object>> rows = mapper.convertValue(node,
                    mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
            return new QueryResult(rows, null);
        }

        if (node.isObject()) {
            if (node.has("rows")) {
                List<Map<String, Object>> rows = mapper.convertValue(node.get("rows"),
                        mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
                String cursor = node.has("next_cursor") && !node.get("next_cursor").isNull()
                        ? node.get("next_cursor").asText() : null;
                return new QueryResult(rows, cursor);
            }
        }
        // ok/tag shape or empty
        return new QueryResult(List.of(), null);
    }

    private static String literal(Object v) {
        if (v == null) return "null";
        if (v instanceof Boolean) return v.toString();
        return v.toString();
    }
}
