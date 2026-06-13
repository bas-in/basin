package io.basin.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Metadata for a storage bucket. Mirrors the Rust {@code Bucket} struct. */
public final class StorageBucket {

    @JsonProperty("id")
    public final String id;

    @JsonProperty("name")
    public final String name;

    @JsonProperty("public")
    public final boolean isPublic;

    /** Maximum file size in bytes. {@code null} when no limit is set. */
    @JsonProperty("file_size_limit")
    public final Long fileSizeLimit;

    @JsonProperty("allowed_mime_types")
    public final List<String> allowedMimeTypes;

    @JsonProperty("created_at")
    public final String createdAt;

    @JsonProperty("updated_at")
    public final String updatedAt;

    public StorageBucket(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("public") boolean isPublic,
            @JsonProperty("file_size_limit") Long fileSizeLimit,
            @JsonProperty("allowed_mime_types") List<String> allowedMimeTypes,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("updated_at") String updatedAt) {
        this.id = id;
        this.name = name;
        this.isPublic = isPublic;
        this.fileSizeLimit = fileSizeLimit;
        this.allowedMimeTypes = allowedMimeTypes;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }
}
