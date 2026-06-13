package io.basin.sdk;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Normalised result of a {@code GET /rest/v1/:table} request.
 *
 * <p>When the server returns a plain JSON array, {@link #nextCursor} is {@code null}.
 * When the server returns {@code {"rows":[…],"next_cursor":"…"}}, {@link #nextCursor}
 * is populated.
 */
public final class QueryResult {

    /** Decoded rows. Each row is a {@code Map<String, Object>}. */
    public final List<Map<String, Object>> rows;

    /**
     * Keyset pagination cursor for the next page.
     * {@code null} when there are no more pages or when no limit was set.
     */
    public final String nextCursor;

    public QueryResult(List<Map<String, Object>> rows, String nextCursor) {
        this.rows = rows;
        this.nextCursor = nextCursor;
    }

    /**
     * Decode the rows into a typed list via a JSON round-trip.
     *
     * <pre>{@code
     * List<Order> orders = result.into(mapper, Order.class);
     * }</pre>
     *
     * @param mapper Jackson {@link ObjectMapper}
     * @param type   target element type
     * @param <T>    element type
     * @return typed list
     * @throws BasinException wrapping any JSON error
     */
    public <T> List<T> into(ObjectMapper mapper, Class<T> type) {
        try {
            byte[] bytes = mapper.writeValueAsBytes(rows);
            return mapper.readValue(bytes,
                    mapper.getTypeFactory().constructCollectionType(List.class, type));
        } catch (IOException e) {
            throw new BasinException("Failed to decode query result into " + type.getName(), e);
        }
    }
}
