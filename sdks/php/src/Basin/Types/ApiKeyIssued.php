<?php

declare(strict_types=1);

namespace Basin\Types;

/** POST /auth/v1/api-keys response — the secret is shown exactly once. */
final class ApiKeyIssued
{
    public function __construct(
        public readonly int $id,
        public readonly string $name,
        public readonly string $secret,
        public readonly string $createdAt,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            id: (int) $data['id'],
            name: (string) $data['name'],
            secret: (string) $data['secret'],
            createdAt: (string) $data['created_at'],
        );
    }
}
