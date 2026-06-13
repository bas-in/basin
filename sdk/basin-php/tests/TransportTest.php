<?php

declare(strict_types=1);

namespace Basin\Tests;

use Basin\Exception\BasinApiException;
use Basin\Exception\BasinNetworkException;
use Basin\Http\Transport;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

class TransportTest extends TestCase
{
    private function makeTransport(MockHandler $mock, ?string $token = null): Transport
    {
        $stack = HandlerStack::create($mock);
        return new Transport(
            baseUrl: 'http://localhost:8080',
            token: $token,
            handler: $stack,
        );
    }

    public function testRequestJsonDecodes(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true,"tag":"INSERT 1"}'),
        ]);
        $transport = $this->makeTransport($mock);

        $result = $transport->requestJson('POST', '/rest/v1/orders', body: ['total' => 50]);

        $this->assertIsArray($result);
        $this->assertTrue($result['ok']);
        $this->assertSame('INSERT 1', $result['tag']);
    }

    public function testNon2xxThrowsBasinApiException(): void
    {
        $mock = new MockHandler([
            new Response(404, ['Content-Type' => 'application/json'],
                '{"code":"E_NOT_FOUND","message":"table not found"}'),
        ]);
        $transport = $this->makeTransport($mock);

        $this->expectException(BasinApiException::class);

        try {
            $transport->requestJson('GET', '/rest/v1/nonexistent');
        } catch (BasinApiException $e) {
            $this->assertSame('E_NOT_FOUND', $e->getErrorCode());
            $this->assertSame(404, $e->getHttpStatus());
            $this->assertStringContainsString('table not found', $e->getMessage());
            throw $e;
        }
    }

    public function testNon2xxWithNonJsonBodyUsesDefaults(): void
    {
        $mock = new MockHandler([
            new Response(500, [], 'Internal Server Error'),
        ]);
        $transport = $this->makeTransport($mock);

        $this->expectException(BasinApiException::class);

        try {
            $transport->requestJson('GET', '/rest/v1/table');
        } catch (BasinApiException $e) {
            $this->assertSame('E_UNKNOWN', $e->getErrorCode());
            $this->assertSame(500, $e->getHttpStatus());
            throw $e;
        }
    }

    public function testAuthHeaderAddedWhenTokenSet(): void
    {
        $container = [];
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '[]'),
        ]);
        $stack = HandlerStack::create($mock);
        $stack->push(\GuzzleHttp\Middleware::history($container));

        $transport = new Transport(
            baseUrl: 'http://localhost:8080',
            token: 'my-secret-key',
            handler: $stack,
        );

        $transport->requestJson('GET', '/rest/v1/orders');

        $this->assertCount(1, $container);
        $this->assertSame(
            'Bearer my-secret-key',
            $container[0]['request']->getHeaderLine('Authorization'),
        );
    }

    public function testAuthHeaderSkippedWhenAuthFalse(): void
    {
        $container = [];
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true}'),
        ]);
        $stack = HandlerStack::create($mock);
        $stack->push(\GuzzleHttp\Middleware::history($container));

        $transport = new Transport(
            baseUrl: 'http://localhost:8080',
            token: 'my-secret-key',
            handler: $stack,
        );

        $transport->requestJson('POST', '/auth/v1/signup', body: [], auth: false);

        $this->assertFalse($container[0]['request']->hasHeader('Authorization'));
    }

    public function testTokenGetterOverridesStaticToken(): void
    {
        $container = [];
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '[]'),
        ]);
        $stack = HandlerStack::create($mock);
        $stack->push(\GuzzleHttp\Middleware::history($container));

        $transport = new Transport(
            baseUrl: 'http://localhost:8080',
            token: 'static-token',
            handler: $stack,
        );
        $transport->setTokenGetter(fn() => 'live-session-token');

        $transport->requestJson('GET', '/rest/v1/orders');

        $this->assertSame(
            'Bearer live-session-token',
            $container[0]['request']->getHeaderLine('Authorization'),
        );
    }

    public function test204ReturnsNull(): void
    {
        $mock = new MockHandler([
            new Response(204, [], ''),
        ]);
        $transport = $this->makeTransport($mock);

        $result = $transport->requestJson('POST', '/auth/v1/magic-link', body: ['email' => 'a@b.com'], auth: false);
        $this->assertNull($result);
    }
}
