<?php

declare(strict_types=1);

namespace Basin\Types;

/** POST /auth/v1/signup response → {ok, user_id}. */
final class SignUpResult
{
    public function __construct(
        public readonly bool $ok,
        public readonly string $userId,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(
            ok: (bool) $data['ok'],
            userId: (string) $data['user_id'],
        );
    }
}
