<?php

declare(strict_types=1);

namespace Basin\Storage;

use Basin\Exception\BasinApiException;
use Basin\Http\Transport;
use Basin\Types\Bucket;
use Basin\Types\DownloadResult;
use Basin\Types\SignedUrl;
use Basin\Types\StorageObject;

/**
 * Storage client — bindings for /storage/v1/*.
 *
 * Route sources (verified, crates/basin-rest/src/server.rs):
 *   POST   /storage/v1/bucket                              create bucket
 *   GET    /storage/v1/bucket/:name                        bucket metadata
 *   DELETE /storage/v1/bucket/:name                        delete + purge
 *   POST   /storage/v1/object/:bucket/*path                upload (JWT)
 *   GET    /storage/v1/object/:bucket/*path                download (JWT)
 *   DELETE /storage/v1/object/:bucket/*path                delete (JWT)
 *   POST   /storage/v1/object/list/:bucket                 list (body: prefix/limit/offset)
 *   DELETE /storage/v1/object/:bucket                      bulk delete (body: prefixes[])
 *   GET    /storage/v1/object/public/:project_id/:bucket/*path  public download (no JWT)
 *   POST   /storage/v1/object/sign/upload/:bucket/*path    mint signed URL (JWT)
 *   GET    /storage/v1/object/sign/:project/:bucket/*path  signed download (no JWT)
 *
 * Usage:
 *   $client->storage()->createBucket('avatars', public: true);
 *   $client->storage()->fromBucket('avatars')->upload('user.jpg', $bytes, 'image/jpeg');
 */
class StorageClient
{
    public function __construct(
        private readonly Transport $transport,
        private readonly \Closure $projectIdGetter,
    ) {
    }

    // ------------------------------------------------------------------
    // Bucket operations
    // ------------------------------------------------------------------

    /**
     * POST /storage/v1/bucket (201).
     *
     * @param string[] $allowedMimeTypes
     */
    public function createBucket(
        string $name,
        bool $public = false,
        ?int $fileSizeLimit = null,
        array $allowedMimeTypes = [],
    ): Bucket {
        $data = (array) $this->transport->requestJson('POST', '/storage/v1/bucket', body: [
            'name'               => $name,
            'public'             => $public,
            'file_size_limit'    => $fileSizeLimit,
            'allowed_mime_types' => $allowedMimeTypes,
        ]);
        return Bucket::fromArray($data);
    }

    /**
     * GET /storage/v1/bucket/:name.
     */
    public function getBucket(string $name): Bucket
    {
        $data = (array) $this->transport->requestJson('GET', '/storage/v1/bucket/' . rawurlencode($name));
        return Bucket::fromArray($data);
    }

    /**
     * DELETE /storage/v1/bucket/:name — also purges object bytes.
     * @return array<string,mixed>
     */
    public function deleteBucket(string $name): array
    {
        return (array) $this->transport->requestJson('DELETE', '/storage/v1/bucket/' . rawurlencode($name));
    }

    /**
     * Return an object-level client scoped to one bucket.
     */
    public function fromBucket(string $bucket): StorageBucketClient
    {
        return new StorageBucketClient($this->transport, $bucket, $this->projectIdGetter);
    }
}

/**
 * Object-level operations scoped to one bucket.
 */
class StorageBucketClient
{
    public function __construct(
        private readonly Transport $transport,
        private readonly string $bucket,
        private readonly \Closure $projectIdGetter,
    ) {
    }

    // ------------------------------------------------------------------
    // Object operations
    // ------------------------------------------------------------------

    /**
     * POST /storage/v1/object/:bucket/*path — upload a file.
     */
    public function upload(string $path, string $data, ?string $contentType = null): StorageObject
    {
        $headers = [];
        if ($contentType !== null) {
            $headers['Content-Type'] = $contentType;
        }

        $result = (array) $this->transport->requestJson(
            'POST',
            $this->objectPath($path),
            body: $data,
            headers: $headers,
        );
        return StorageObject::fromArray($result);
    }

    /**
     * GET /storage/v1/object/:bucket/*path — authenticated download.
     */
    public function download(string $path): DownloadResult
    {
        $response = $this->transport->raw('GET', $this->objectPath($path));
        return new DownloadResult(
            data: (string) $response->getBody(),
            contentType: $response->getHeaderLine('Content-Type') ?: null,
            etag: $response->getHeaderLine('ETag') ?: null,
        );
    }

    /**
     * DELETE /storage/v1/object/:bucket/*path — single delete.
     * @return array<string,mixed>
     */
    public function remove(string $path): array
    {
        return (array) $this->transport->requestJson('DELETE', $this->objectPath($path));
    }

    /**
     * POST /storage/v1/object/list/:bucket.
     * @return StorageObject[]
     */
    public function list(?string $prefix = null, ?int $limit = null, ?int $offset = null): array
    {
        $data = $this->transport->requestJson(
            'POST',
            '/storage/v1/object/list/' . rawurlencode($this->bucket),
            body: ['prefix' => $prefix, 'limit' => $limit, 'offset' => $offset],
        );
        return array_map(
            fn(array $d) => StorageObject::fromArray($d),
            (array) ($data ?? []),
        );
    }

    /**
     * DELETE /storage/v1/object/:bucket with {prefixes:[...]} — bulk delete.
     *
     * RLS-denied objects are skipped server-side.
     *
     * @param string[] $prefixes
     * @return array<string,mixed>
     */
    public function removeByPrefixes(array $prefixes): array
    {
        return (array) $this->transport->requestJson(
            'DELETE',
            '/storage/v1/object/' . rawurlencode($this->bucket),
            body: ['prefixes' => $prefixes],
        );
    }

    /**
     * Build the no-JWT public URL.
     *
     * GET /storage/v1/object/public/:project_id/:bucket/*path
     * Only works for buckets with public=true.
     */
    public function getPublicUrl(string $path): string
    {
        $project = ($this->projectIdGetter)();
        if ($project === null) {
            throw new BasinApiException(
                'E_INVALID_REQUEST',
                'public URLs require a project id (pass project_id to new Client())',
                0,
            );
        }
        $base = $this->transport->getBaseUrl();
        return "{$base}/storage/v1/object/public/{$project}/{$this->encodedBucket()}/{$this->encodePath($path)}";
    }

    /**
     * POST /storage/v1/object/sign/upload/:bucket/*path — mint signed download URL.
     *
     * TTL is capped at 7 days server-side; default 3600 s.
     * Despite the 'upload' literal in the path this mints a download URL —
     * that segment only disambiguates the axum router (storage_sign.rs).
     */
    public function createSignedUrl(string $path, int $expiresIn = 3600): SignedUrl
    {
        $data = (array) $this->transport->requestJson(
            'POST',
            "/storage/v1/object/sign/upload/{$this->encodedBucket()}/{$this->encodePath($path)}",
            body: ['expires_in' => $expiresIn],
        );
        return SignedUrl::fromArray($data, $this->transport->getBaseUrl());
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private function objectPath(string $path): string
    {
        return "/storage/v1/object/{$this->encodedBucket()}/{$this->encodePath($path)}";
    }

    private function encodedBucket(): string
    {
        return rawurlencode($this->bucket);
    }

    private function encodePath(string $path): string
    {
        return implode('/', array_map('rawurlencode', explode('/', $path)));
    }
}
