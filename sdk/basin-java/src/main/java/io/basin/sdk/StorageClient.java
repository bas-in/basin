package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Storage client for {@code /storage/v1/*} routes.
 *
 * <p>Route sources (verified against {@code crates/basin-rest/src/server.rs:373-424};
 * handlers in {@code crates/basin-rest/src/routes/storage.rs} and
 * {@code storage_sign.rs}):
 * <ul>
 *   <li>{@code POST   /storage/v1/bucket}                                  create bucket
 *   <li>{@code GET    /storage/v1/bucket/:name}                            bucket metadata
 *   <li>{@code DELETE /storage/v1/bucket/:name}                            delete + purge
 *   <li>{@code POST   /storage/v1/object/:bucket/*path}                    upload (JWT)
 *   <li>{@code GET    /storage/v1/object/:bucket/*path}                    download (JWT)
 *   <li>{@code DELETE /storage/v1/object/:bucket/*path}                    delete (JWT)
 *   <li>{@code POST   /storage/v1/object/list/:bucket}                     list objects
 *   <li>{@code DELETE /storage/v1/object/:bucket}                          bulk delete
 *   <li>{@code GET    /storage/v1/object/public/:project_id/:bucket/*path} public download
 *   <li>{@code POST   /storage/v1/object/sign/upload/:bucket/*path}        mint signed URL
 * </ul>
 *
 * <p>Obtain via {@link BasinClient#storage()}.
 */
public final class StorageClient {

    private final BasinTransport transport;
    private final Supplier<String> projectIdSupplier;

    StorageClient(BasinTransport transport, Supplier<String> projectIdSupplier) {
        this.transport = transport;
        this.projectIdSupplier = projectIdSupplier;
    }

    // ------------------------------------------------------------------
    // Bucket operations
    // ------------------------------------------------------------------

    /**
     * {@code POST /storage/v1/bucket} (201) — create a bucket.
     *
     * @param name             bucket name
     * @param isPublic         whether objects are publicly accessible without a token
     * @param fileSizeLimit    maximum object size in bytes; {@code null} for no limit
     * @param allowedMimeTypes mime type allowlist; {@code null} or empty for all types
     */
    public CompletableFuture<StorageBucket> createBucket(String name, boolean isPublic,
                                                          Long fileSizeLimit,
                                                          List<String> allowedMimeTypes) {
        Map<String, Object> body = new HashMap<>();
        body.put("name", name);
        body.put("public", isPublic);
        if (fileSizeLimit != null) body.put("file_size_limit", fileSizeLimit);
        body.put("allowed_mime_types", allowedMimeTypes != null ? allowedMimeTypes : List.of());
        return transport.executeJsonAsync(transport.jsonPost("/storage/v1/bucket", null, body, true))
                .thenApply(n -> transport.readValue(n, StorageBucket.class));
    }

    /** Blocking variant of {@link #createBucket}. */
    public StorageBucket createBucketBlocking(String name, boolean isPublic,
                                               Long fileSizeLimit, List<String> allowedMimeTypes) {
        return createBucket(name, isPublic, fileSizeLimit, allowedMimeTypes).join();
    }

    /** {@code GET /storage/v1/bucket/:name} — bucket metadata. */
    public CompletableFuture<StorageBucket> getBucket(String name) {
        return transport.executeJsonAsync(
                transport.jsonGet("/storage/v1/bucket/" + encodePath(name), null, true))
                .thenApply(n -> transport.readValue(n, StorageBucket.class));
    }

    /** Blocking variant of {@link #getBucket}. */
    public StorageBucket getBucketBlocking(String name) {
        return getBucket(name).join();
    }

    /** {@code DELETE /storage/v1/bucket/:name} — delete bucket and purge all objects. */
    public CompletableFuture<JsonNode> deleteBucket(String name) {
        return transport.executeJsonAsync(
                transport.jsonDelete("/storage/v1/bucket/" + encodePath(name), null, null, true));
    }

    /** Blocking variant of {@link #deleteBucket}. */
    public JsonNode deleteBucketBlocking(String name) {
        return deleteBucket(name).join();
    }

    /**
     * Return an object-level client scoped to one bucket.
     *
     * <pre>{@code
     * StorageBucketClient bucket = client.storage().fromBucket("avatars");
     * bucket.upload("users/alice.png", bytes, "image/png").join();
     * }</pre>
     */
    public StorageBucketClient fromBucket(String bucket) {
        return new StorageBucketClient(transport, bucket, projectIdSupplier);
    }

    // ------------------------------------------------------------------
    // Path encoding
    // ------------------------------------------------------------------

    static String encodePath(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static String encodeObjectPath(String path) {
        String[] parts = path.split("/", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append('/');
            sb.append(encodePath(parts[i]));
        }
        return sb.toString();
    }
}
