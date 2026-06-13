<?php

declare(strict_types=1);

namespace Basin\Types;

/**
 * Normalised result of a GET query.
 *
 * The server returns two shapes:
 *   - plain array → rows only, no pagination
 *   - {rows, next_cursor} → paginated response (when limit or cursor is set)
 */
final class QueryResult
{
    public function __construct(
        /** @var array<int,array<string,mixed>> */
        public readonly array $rows,
        public readonly ?string $nextCursor,
    ) {
    }

    /**
     * Build from a decoded JSON response body (either shape).
     *
     * @param array<string,mixed>|list<array<string,mixed>> $body
     */
    public static function fromBody(mixed $body): self
    {
        if (is_array($body) && array_is_list($body)) {
            return new self(rows: $body, nextCursor: null);
        }

        if (is_array($body) && isset($body['rows'])) {
            return new self(
                rows: (array) $body['rows'],
                nextCursor: isset($body['next_cursor']) ? (string) $body['next_cursor'] : null,
            );
        }

        // {ok, tag} empty-result shape (ExecResult::Empty) — no rows.
        return new self(rows: [], nextCursor: null);
    }
}

/** Non-row write result: {ok, tag}. */
final class ExecTag
{
    public function __construct(
        public readonly bool $ok,
        public readonly string $tag,
    ) {
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        return new self(ok: (bool) $data['ok'], tag: (string) $data['tag']);
    }
}
