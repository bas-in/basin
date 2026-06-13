<?php

declare(strict_types=1);

namespace Basin\Types;

/**
 * Token body returned by /auth/v1/signin, /auth/v1/refresh,
 * /auth/v1/magic-link/consume (basin-rest routes/auth.rs token_body).
 */
final class Session
{
    public function __construct(
        public readonly string $accessToken,
        public readonly string $refreshToken,
        public readonly string $accessExpiresAt,   // RFC 3339
        public readonly string $refreshExpiresAt,  // RFC 3339
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            accessToken: (string) $data['access_token'],
            refreshToken: (string) $data['refresh_token'],
            accessExpiresAt: (string) $data['access_expires_at'],
            refreshExpiresAt: (string) $data['refresh_expires_at'],
        );
    }
}
