<?php

declare(strict_types=1);

namespace Basin\Types;

/** POST /storage/v1/bucket or GET /storage/v1/bucket/:name response. */
final class Bucket
{
    public function __construct(
        public readonly string $id,
        public readonly string $name,
        public readonly bool $public,
        public readonly ?int $fileSizeLimit,
        /** @var string[] */
        public readonly array $allowedMimeTypes,
        public readonly string $createdAt,
        public readonly string $updatedAt,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            id: (string) $data['id'],
            name: (string) $data['name'],
            public: (bool) $data['public'],
            fileSizeLimit: isset($data['file_size_limit']) ? (int) $data['file_size_limit'] : null,
            allowedMimeTypes: (array) ($data['allowed_mime_types'] ?? []),
            createdAt: (string) $data['created_at'],
            updatedAt: (string) $data['updated_at'],
        );
    }
}

/** POST /storage/v1/object/:bucket/*path or list response element. */
final class StorageObject
{
    public function __construct(
        public readonly string $id,
        public readonly string $bucketId,
        public readonly string $path,
        public readonly int $size,
        public readonly ?string $mimeType,
        public readonly mixed $metadata,
        public readonly ?string $owner,
        public readonly string $etag,
        public readonly string $createdAt,
        public readonly string $updatedAt,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            id: (string) $data['id'],
            bucketId: (string) $data['bucket_id'],
            path: (string) $data['path'],
            size: (int) $data['size'],
            mimeType: isset($data['mime_type']) ? (string) $data['mime_type'] : null,
            metadata: $data['metadata'] ?? null,
            owner: isset($data['owner']) ? (string) $data['owner'] : null,
            etag: (string) $data['etag'],
            createdAt: (string) $data['created_at'],
            updatedAt: (string) $data['updated_at'],
        );
    }
}

/**
 * POST /storage/v1/object/sign/upload/:bucket/*path response.
 *
 * JSON field names from the server (storage_sign.rs):
 *   signedUrl  — camelCase, server-relative path
 *   expiresAt  — camelCase, RFC 3339
 */
final class SignedUrl
{
    public function __construct(
        /** Server-relative signed URL path. */
        public readonly string $signedUrl,
        /** RFC 3339 expiry timestamp. */
        public readonly string $expiresAt,
        /** Full absolute URL (base URL + signedUrl). */
        public readonly string $absoluteUrl,
    ) {
    }

    /**
     * @param array<string,mixed> $data
     * @param string $baseUrl HTTP base URL of the Basin server
     */
    public static function fromArray(array $data, string $baseUrl): self
    {
        // Server serialises with camelCase: signedUrl / expiresAt
        // Fall back to snake_case for forward compat.
        $rel = (string) ($data['signedUrl'] ?? $data['signed_url'] ?? '');
        $expires = (string) ($data['expiresAt'] ?? $data['expires_at'] ?? '');

        return new self(
            signedUrl: $rel,
            expiresAt: $expires,
            absoluteUrl: rtrim($baseUrl, '/') . $rel,
        );
    }
}

/** Result of a download operation. */
final class DownloadResult
{
    public function __construct(
        public readonly string $data,
        public readonly ?string $contentType,
        public readonly ?string $etag,
    ) {
    }
}
