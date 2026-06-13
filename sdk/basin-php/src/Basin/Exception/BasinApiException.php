<?php

declare(strict_types=1);

namespace Basin\Exception;

/**
 * Raised for any non-2xx response from a Basin server.
 *
 * The Basin server returns a JSON error envelope:
 *   {"code": "E_...", "message": "..."}
 *
 * `$code` is the stable machine-readable contract; `$message` is
 * human-readable and not guaranteed to be stable across server versions.
 *
 * Known error codes (basin-rest/src/errors.rs ErrorCode::as_str):
 *   E_UNAUTHENTICATED, E_FORBIDDEN, E_NOT_FOUND, E_INVALID_REQUEST,
 *   E_RATE_LIMITED, E_ENGINE_UNSUPPORTED, E_INTERNAL, E_EMAIL_DISABLED,
 *   E_REVOKED_TOKEN
 *
 * Unknown codes from a newer server are surfaced as plain strings so an
 * older SDK does not lose information.
 */
class BasinApiException extends BasinException
{
    public function __construct(
        private readonly string $errorCode,
        string $message,
        private readonly int $httpStatus,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, $httpStatus, $previous);
    }

    /** Stable error code, e.g. "E_NOT_FOUND". Match on this, not getMessage(). */
    public function getErrorCode(): string
    {
        return $this->errorCode;
    }

    /** HTTP status code (0 when synthesised client-side without a round-trip). */
    public function getHttpStatus(): int
    {
        return $this->httpStatus;
    }

    /**
     * Build from a decoded JSON error envelope and HTTP status.
     *
     * @param array<string,mixed> $body
     */
    public static function fromResponseBody(array $body, int $status): self
    {
        $code = is_string($body['code'] ?? null) ? $body['code'] : 'E_UNKNOWN';
        $message = is_string($body['message'] ?? null) ? $body['message'] : "HTTP {$status}";

        return new self($code, $message, $status);
    }
}
