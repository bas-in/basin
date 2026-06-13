<?php

declare(strict_types=1);

namespace Basin\Types;

/** GET /auth/v1/api-keys array element. */
final class ApiKeyDescriptor
{
    public function __construct(
        public readonly int $id,
        public readonly string $name,
        public readonly string $createdAt,
        public readonly ?string $lastUsedAt,
        public readonly ?string $revokedAt,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            id: (int) $data['id'],
            name: (string) $data['name'],
            createdAt: (string) $data['created_at'],
            lastUsedAt: isset($data['last_used_at']) ? (string) $data['last_used_at'] : null,
            revokedAt: isset($data['revoked_at']) ? (string) $data['revoked_at'] : null,
        );
    }
}
