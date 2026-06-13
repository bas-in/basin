package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
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
