<?php

declare(strict_types=1);

namespace Basin\Types;

/**
 * GET /auth/v1/oauth/:provider/authorize response.
 *
 * Redirect the user's browser to $redirectUrl to begin the provider OAuth
 * flow. The server's callback (GET /auth/v1/oauth/:provider/callback) will
 * exchange the code and issue Basin JWT + refresh tokens.
 */
final class OAuthAuthorizeResult
{
    public function __construct(
        /** Full provider authorize URL — redirect the browser here. */
        public readonly string $redirectUrl,
        /** CSRF state value included in redirect_url; echoed back on callback. */
        public readonly string $state,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            redirectUrl: (string) $data['redirect_url'],
            state: (string) $data['state'],
        );
    }
}
