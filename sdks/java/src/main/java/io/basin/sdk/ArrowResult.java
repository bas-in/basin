package io.basin.sdk;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Result of a {@link QueryBuilder#toArrow()} call.
 *
 * <p>Wraps an Arrow {@link VectorSchemaRoot} (the in-memory columnar representation of a
 * record batch) together with the keyset pagination cursor returned by the server.
 *
 * <p>The {@link VectorSchemaRoot} holds reference-counted Arrow buffers.
 * <strong>Callers are responsible for closing it</strong> when done to release off-heap
 * memory.  Use try-with-resources:
 *
 * <pre>{@code
 * try (ArrowResult ar = client.table("orders").limit(1000).toArrow()) {
 *     VectorSchemaRoot root = ar.root;
 *     System.out.println("rows: " + root.getRowCount());
 *     // read columns from root …
 *     String cursor = ar.nextCursor;   // null when no next page
 * }
 * }</pre>
 *
 * <p>Pagination:
 * <pre>{@code
 * String cursor = null;
 * do {
 *     QueryBuilder qb = client.table("events").limit(1000);
 *     if (cursor != null) qb = qb.cursor(cursor);
 *     try (ArrowResult ar = qb.toArrow()) {
 *         // process ar.root …
 *         cursor = ar.nextCursor;
 *     }
 * } while (cursor != null);
 * }</pre>
 *
 * @see QueryBuilder#toArrow()
 */
public final class ArrowResult implements AutoCloseable {

    /**
     * Columnar result data.
     * Must be closed by the caller — see class-level javadoc.
     */
    public final VectorSchemaRoot root;

    /**
     * Keyset pagination cursor for the next page.
     * Populated from the {@code X-Basin-Next-Cursor} response header.
     * {@code null} when there are no more pages.
     */
    public final String nextCursor;

    /**
     * Total row count as reported by the server in the
     * {@code X-Basin-Row-Count} response header.
     * {@code -1} when the header is absent.
     */
    public final long rowCount;

    ArrowResult(VectorSchemaRoot root, String nextCursor, long rowCount) {
        this.root = root;
        this.nextCursor = nextCursor;
        this.rowCount = rowCount;
    }

    /**
     * Closes the underlying {@link VectorSchemaRoot} and releases its Arrow buffers.
     */
    @Override
    public void close() {
        root.close();
    }
}
