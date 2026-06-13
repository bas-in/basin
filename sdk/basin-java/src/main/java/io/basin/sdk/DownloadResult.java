package io.basin.sdk;

/** Raw bytes downloaded from storage, with optional content-type and ETag. */
public final class DownloadResult {

    /** Raw object bytes. */
    public final byte[] data;

    /** Value of the {@code Content-Type} response header, or {@code null}. */
    public final String contentType;

    /** Value of the {@code ETag} response header, or {@code null}. */
    public final String etag;

    public DownloadResult(byte[] data, String contentType, String etag) {
        this.data = data;
        this.contentType = contentType;
        this.etag = etag;
    }
}
