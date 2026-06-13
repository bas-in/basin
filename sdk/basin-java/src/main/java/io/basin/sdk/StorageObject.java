package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Metadata for an uploaded storage object. */
public final class StorageObject {

    @JsonProperty("id")
    public final String id;

    @JsonProperty("bucket_id")
    public final String bucketId;

    @JsonProperty("path")
    public final String path;

    @JsonProperty("size")
    public final long size;

    @JsonProperty("mime_type")
    public final String mimeType;

    /** Arbitrary metadata object. Decoded as a generic {@code Object}. */
    @JsonProperty("metadata")
    public final Object metadata;

    @JsonProperty("owner")
    public final String owner;

    @JsonProperty("etag")
    public final String etag;

    @JsonProperty("created_at")
    public final String createdAt;

    @JsonProperty("updated_at")
    public final String updatedAt;

    public StorageObject(
            @JsonProperty("id") String id,
            @JsonProperty("bucket_id") String bucketId,
            @JsonProperty("path") String path,
            @JsonProperty("size") long size,
            @JsonProperty("mime_type") String mimeType,
            @JsonProperty("metadata") Object metadata,
            @JsonProperty("owner") String owner,
            @JsonProperty("etag") String etag,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("updated_at") String updatedAt) {
        this.id = id;
        this.bucketId = bucketId;
        this.path = path;
        this.size = size;
        this.mimeType = mimeType;
        this.metadata = metadata;
        this.owner = owner;
        this.etag = etag;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }
}
