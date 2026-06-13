<?php

declare(strict_types=1);

namespace Basin\Tests;

use Basin\Exception\BasinApiException;
use Basin\Functions\FunctionsClient;
use Basin\Http\Transport;
use Basin\Types\FunctionInvokeResult;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

class FunctionsClientTest extends TestCase
{
    /** @var list<array<string,mixed>> */
    private array $container = [];

    private function makeFunctions(MockHandler $mock): FunctionsClient
    {
        $this->container = [];
        $stack = HandlerStack::create($mock);
        $stack->push(Middleware::history($this->container));
        $transport = new Transport('http://localhost:8080', token: 'test-token', handler: $stack);
        return new FunctionsClient($transport);
    }

    // ------------------------------------------------------------------
    // rpc()
    // ------------------------------------------------------------------

    public function testRpcScalarResult(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '42'),
        ]);
        $fn = $this->makeFunctions($mock);

        $result = $fn->rpc('add_numbers', ['a' => 20, 'b' => 22]);

        $this->assertSame(42, $result);
        $this->assertSame('POST', $this->container[0]['request']->getMethod());
        $this->assertStringContainsString('/rest/v1/rpc/add_numbers', (string) $this->container[0]['request']->getUri());
    }

    public function testRpcTableResult(): void
    {
        $rows = [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']];
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode($rows)),
        ]);
        $fn = $this->makeFunctions($mock);

        $result = $fn->rpc('get_users');

        $this->assertIsArray($result);
        $this->assertCount(2, $result);
    }

    // ------------------------------------------------------------------
    // invoke()
    // ------------------------------------------------------------------

    public function testInvokeReturnsResult(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"message":"hello"}'),
        ]);
        $fn = $this->makeFunctions($mock);

        $result = $fn->invoke('my-function');

        $this->assertInstanceOf(FunctionInvokeResult::class, $result);
        $this->assertSame(200, $result->status);
        $this->assertSame('hello', $result->data['message']);
    }

    public function testInvokeWithBody(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true}'),
        ]);
        $fn = $this->makeFunctions($mock);

        $fn->invoke('my-function', body: ['input' => 'value']);

        $body = json_decode((string) $this->container[0]['request']->getBody(), true);
        $this->assertSame('value', $body['input']);
    }

    public function testInvokeNon2xxWithBasinEnvelopeThrows(): void
    {
        $mock = new MockHandler([
            new Response(401, ['Content-Type' => 'application/json'],
                '{"code":"E_UNAUTHENTICATED","message":"JWT missing"}'),
        ]);
        $fn = $this->makeFunctions($mock);

        $this->expectException(BasinApiException::class);

        try {
            $fn->invoke('secured-function');
        } catch (BasinApiException $e) {
            $this->assertSame('E_UNAUTHENTICATED', $e->getErrorCode());
            throw $e;
        }
    }

    public function testInvokeNon2xxWithoutBasinEnvelopeDoesNotThrow(): void
    {
        // A function's own non-2xx response is NOT an SDK error.
        $mock = new MockHandler([
            new Response(404, ['Content-Type' => 'text/plain'], 'Not Found'),
        ]);
        $fn = $this->makeFunctions($mock);

        $result = $fn->invoke('my-function');

        $this->assertSame(404, $result->status);
        $this->assertSame('Not Found', $result->data);
    }

    public function testInvokeMethodOverride(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{}'),
        ]);
        $fn = $this->makeFunctions($mock);

        $fn->invoke('my-function', method: 'GET');

        $this->assertSame('GET', $this->container[0]['request']->getMethod());
    }

    public function testInvokeAddsAuthHeader(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{}'),
        ]);
        $fn = $this->makeFunctions($mock);

        $fn->invoke('my-function');

        $this->assertSame(
            'Bearer test-token',
            $this->container[0]['request']->getHeaderLine('Authorization'),
        );
    }
}
