<?php

declare(strict_types=1);

namespace Basin\Tests;

use Basin\Exception\BasinApiException;
use PHPUnit\Framework\TestCase;

class ErrorDecodeTest extends TestCase
{
    public function testFromResponseBodyKnownCode(): void
    {
        $e = BasinApiException::fromResponseBody(
            ['code' => 'E_NOT_FOUND', 'message' => 'table not found'],
            404,
        );

        $this->assertSame('E_NOT_FOUND', $e->getErrorCode());
        $this->assertSame('table not found', $e->getMessage());
        $this->assertSame(404, $e->getHttpStatus());
    }

    public function testFromResponseBodyMissingCode(): void
    {
        $e = BasinApiException::fromResponseBody(['message' => 'Something broke'], 500);

        $this->assertSame('E_UNKNOWN', $e->getErrorCode());
        $this->assertSame(500, $e->getHttpStatus());
    }

    public function testFromResponseBodyEmptyEnvelope(): void
    {
        $e = BasinApiException::fromResponseBody([], 429);

        $this->assertSame('E_UNKNOWN', $e->getErrorCode());
        $this->assertSame('HTTP 429', $e->getMessage());
        $this->assertSame(429, $e->getHttpStatus());
    }

    public function testFromResponseBodyNonStringCode(): void
    {
        $e = BasinApiException::fromResponseBody(['code' => 123, 'message' => 'bad'], 400);

        $this->assertSame('E_UNKNOWN', $e->getErrorCode());
    }

    public function testFromResponseBodyFutureCode(): void
    {
        // A future error code from a newer server should pass through as-is.
        $e = BasinApiException::fromResponseBody(
            ['code' => 'E_FUTURE_ERROR', 'message' => 'future error'],
            422,
        );

        $this->assertSame('E_FUTURE_ERROR', $e->getErrorCode());
    }

    public function testExceptionInheritance(): void
    {
        $e = BasinApiException::fromResponseBody(
            ['code' => 'E_UNAUTHENTICATED', 'message' => 'unauthorized'],
            401,
        );

        $this->assertInstanceOf(\Basin\Exception\BasinException::class, $e);
        $this->assertInstanceOf(\RuntimeException::class, $e);
    }

    public function testErrorCodeMatchableBySwitchCase(): void
    {
        $e = BasinApiException::fromResponseBody(
            ['code' => 'E_RATE_LIMITED', 'message' => 'too many requests'],
            429,
        );

        $handled = match ($e->getErrorCode()) {
            'E_RATE_LIMITED' => 'rate-limited',
            'E_NOT_FOUND'    => 'not-found',
            default          => 'other',
        };

        $this->assertSame('rate-limited', $handled);
    }
}
