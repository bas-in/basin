package io.basin.sdk;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Object-level storage operations scoped to a single bucket.
 *
 * <p>Obtain via {@link StorageClient#fromBucket(String)}.
 */
public final class StorageBucketClient {

    private final BasinTransport transport;
    private final String bucket;
    private final Supplier<String> projectIdSupplier;

    StorageBucketClient(BasinTransport transport, String bucket, Supplier<String> projectIdSupplier) {
        this.transport = transport;
        this.bucket = bucket;
        this.projectIdSupplier = projectIdSupplier;
    }

    private String objectPath(String path) {
        return "/storage/v1/object/"
                + StorageClient.encodePath(bucket) + "/"
                + StorageClient.encodeObjectPath(path);
    }

    // ------------------------------------------------------------------
    // Upload / Download / Delete
    // ------------------------------------------------------------------

    /**
     * {@code POST /storage/v1/object/:bucket/*path} — upload an object.
     *
     * @param path        storage path within the bucket, e.g. {@code "users/alice.png"}
     * @param data        raw bytes to upload
     * @param contentType MIME type; {@code null} to omit the header
     */
    public CompletableFuture<StorageObject> upload(String path, byte[] data, String contentType) {
        Map<String, String> headers = new HashMap<>();
        if (contentType != null) headers.put("Content-Type", contentType);
        HttpRequest req = transport.rawPost(objectPath(path), data, headers, true);
        return transport.executeJsonAsync(req)
                .thenApply(n -> transport.readValue(n, StorageObject.class));
    }

    /** Blocking variant of {@link #upload}. */
    public StorageObject uploadBlocking(String path, byte[] data, String contentType) {
        return upload(path, data, contentType).join();
    }

    /**
     * {@code GET /storage/v1/object/:bucket/*path} — authenticated download.
     */
    public CompletableFuture<DownloadResult> download(String path) {
        HttpRequest req = transport.jsonGet(objectPath(path), null, true);
        return transport.executeRawAsync(req).thenApply(resp -> {
            int status = resp.statusCode();
            if (status < 200 || status >= 300) {
                throw new BasinApiException("E_UNKNOWN", "HTTP " + status, status);
            }
            String ct = resp.headers().firstValue("content-type").orElse(null);
            String etag = resp.headers().firstValue("etag").orElse(null);
            return new DownloadResult(resp.body(), ct, etag);
        });
    }

    /** Blocking variant of {@link #download}. */
    public DownloadResult downloadBlocking(String path) {
        return download(path).join();
    }

    /**
     * {@code DELETE /storage/v1/object/:bucket/*path} — single object delete.
     */
    public CompletableFuture<JsonNode> remove(String path) {
        return transport.executeJsonAsync(
                transport.jsonDelete(objectPath(path), null, null, true));
    }

    /** Blocking variant of {@link #remove}. */
    public JsonNode removeBlocking(String path) {
        return remove(path).join();
    }

    // ------------------------------------------------------------------
    // List / Bulk delete
    // ------------------------------------------------------------------

    /**
     * {@code POST /storage/v1/object/list/:bucket} — list objects.
     *
     * @param prefix path prefix filter; {@code null} for all objects
     * @param limit  max results; {@code null} for server default
     * @param offset pagination offset; {@code null} for 0
     */
    public CompletableFuture<List<StorageObject>> list(String prefix, Integer limit, Integer offset) {
        Map<String, Object> body = new HashMap<>();
        if (prefix != null) body.put("prefix", prefix);
        if (limit != null) body.put("limit", limit);
        if (offset != null) body.put("offset", offset);
        return transport.executeJsonAsync(
                transport.jsonPost("/storage/v1/object/list/" + StorageClient.encodePath(bucket),
                        null, body, true))
                .thenApply(n -> transport.mapper.convertValue(n,
                        transport.mapper.getTypeFactory()
                                .constructCollectionType(List.class, StorageObject.class)));
    }

    /** Blocking variant of {@link #list}. */
    public List<StorageObject> listBlocking(String prefix, Integer limit, Integer offset) {
        return list(prefix, limit, offset).join();
    }

    /**
     * {@code DELETE /storage/v1/object/:bucket} — bulk delete by prefix list.
     *
     * <p>Deletes every object matching any prefix. RLS-denied objects are skipped
     * server-side.
     */
    public CompletableFuture<JsonNode> removeByPrefixes(List<String> prefixes) {
        return transport.executeJsonAsync(
                transport.jsonDelete("/storage/v1/object/" + StorageClient.encodePath(bucket),
                        null, Map.of("prefixes", prefixes), true));
    }

    /** Blocking variant of {@link #removeByPrefixes}. */
    public JsonNode removeByPrefixesBlocking(List<String> prefixes) {
        return removeByPrefixes(prefixes).join();
    }

    // ------------------------------------------------------------------
    // URLs
    // ------------------------------------------------------------------

    /**
     * Build the no-JWT public URL for an object.
     *
     * <p>Path: {@code GET /storage/v1/object/public/:project_id/:bucket/*path}.
     * Only works for buckets with {@code public=true}.
     *
     * @throws BasinApiException when no project ID can be resolved
     */
    public String getPublicUrl(String path) {
        String project = projectIdSupplier.get();
        if (project == null || project.isEmpty()) {
            throw new BasinApiException("E_INVALID_REQUEST",
                    "public URLs require a project id — pass projectId to BasinClient.Builder", 0);
        }
        return transport.baseUrl
                + "/storage/v1/object/public/" + project
                + "/" + StorageClient.encodePath(bucket)
                + "/" + StorageClient.encodeObjectPath(path);
    }

    /**
     * {@code POST /storage/v1/object/sign/upload/:bucket/*path} — mint a signed URL.
     *
     * <p>Despite the {@code upload} literal in the path this mints a download URL —
     * that segment only disambiguates the axum router (see {@code storage_sign.rs}).
     * TTL is capped at 7 days server-side.
     *
     * @param path      storage path within the bucket
     * @param expiresIn URL lifetime in seconds (default: 3600)
     */
    public CompletableFuture<SignedUrl> createSignedUrl(String path, int expiresIn) {
        String apiPath = "/storage/v1/object/sign/upload/"
                + StorageClient.encodePath(bucket) + "/"
                + StorageClient.encodeObjectPath(path);
        return transport.executeJsonAsync(
                transport.jsonPost(apiPath, null, Map.of("expires_in", expiresIn), true))
                .thenApply(n -> {
                    // JSON has camelCase keys: signedUrl, expiresAt
                    String rel = n.has("signedUrl") ? n.get("signedUrl").asText()
                            : n.has("signed_url") ? n.get("signed_url").asText() : "";
                    String exp = n.has("expiresAt") ? n.get("expiresAt").asText()
                            : n.has("expires_at") ? n.get("expires_at").asText() : "";
                    return new SignedUrl(rel, exp, transport.baseUrl + rel);
                });
    }

    /** Blocking variant of {@link #createSignedUrl}. */
    public SignedUrl createSignedUrlBlocking(String path, int expiresIn) {
        return createSignedUrl(path, expiresIn).join();
    }
}
