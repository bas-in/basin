<?php

declare(strict_types=1);

namespace Basin\Tests;

use Basin\Auth\AuthClient;
use Basin\Client;
use Basin\Functions\FunctionsClient;
use Basin\Query\QueryBuilder;
use Basin\Realtime\RealtimeClient;
use Basin\Storage\StorageClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
    private function makeClient(MockHandler $mock): Client
    {
        $stack = HandlerStack::create($mock);
        return new Client([
            'url'        => 'http://localhost:8080',
            'token'      => 'test-token',
            'project_id' => 'proj_01J',
            'handler'    => $stack,
        ]);
    }

    public function testFromReturnsQueryBuilder(): void
    {
        $mock = new MockHandler([]);
        $client = $this->makeClient($mock);

        $this->assertInstanceOf(QueryBuilder::class, $client->from('orders'));
    }

    public function testTableAliasForFrom(): void
    {
        $mock = new MockHandler([]);
        $client = $this->makeClient($mock);

        $this->assertInstanceOf(QueryBuilder::class, $client->table('orders'));
    }

    public function testAuthIsAuthClient(): void
    {
        $mock = new MockHandler([]);
        $client = $this->makeClient($mock);

        $this->assertInstanceOf(AuthClient::class, $client->auth);
    }

    public function testFunctionsIsFunctionsClient(): void
    {
        $mock = new MockHandler([]);
        $client = $this->makeClient($mock);

        $this->assertInstanceOf(FunctionsClient::class, $client->functions);
    }

    public function testStorageReturnsStorageClient(): void
    {
        $mock = new MockHandler([]);
        $client = $this->makeClient($mock);

        $this->assertInstanceOf(StorageClient::class, $client->storage());
    }

    public function testStorageReturnsSameInstance(): void
    {
        $mock = new MockHandler([]);
        $client = $this->makeClient($mock);

        $this->assertSame($client->storage(), $client->storage());
    }

    public function testRealtimeReturnsRealtimeClient(): void
    {
        $mock = new MockHandler([]);
        $client = $this->makeClient($mock);

        $this->assertInstanceOf(RealtimeClient::class, $client->realtime());
    }

    public function testRealtimeReturnsSameInstance(): void
    {
        $mock = new MockHandler([]);
        $client = $this->makeClient($mock);

        $this->assertSame($client->realtime(), $client->realtime());
    }

    public function testHealth(): void
    {
        $mock = new MockHandler([
            new Response(200, [], 'ok'),
        ]);
        $client = $this->makeClient($mock);

        $this->assertSame('ok', $client->health());
    }

    public function testRpcConvenienceAlias(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '42'),
        ]);
        $client = $this->makeClient($mock);

        $result = $client->rpc('add', ['a' => 20, 'b' => 22]);
        $this->assertSame(42, $result);
    }

    public function testRequestEscapeHatch(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"result":"ok"}'),
        ]);
        $client = $this->makeClient($mock);

        $result = $client->request('GET', '/admin/v1/info');
        $this->assertSame('ok', $result['result']);
    }

    public function testMissingUrlThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Client([]);
    }

    public function testAutoRefreshWiredCorrectly(): void
    {
        // When a session is active, the transport should use the session token.
        $sessionBody = json_encode([
            'access_token'      => 'session-token',
            'refresh_token'     => 'refresh-token',
            'access_expires_at' => '2099-01-01T00:00:00Z',
            'refresh_expires_at' => '2099-12-31T00:00:00Z',
        ]);

        $container = [];
        $mock = new MockHandler([
            // signin
            new Response(200, ['Content-Type' => 'application/json'], $sessionBody),
            // query after signin
            new Response(200, ['Content-Type' => 'application/json'], '[]'),
        ]);
        $stack = HandlerStack::create($mock);
        $stack->push(\GuzzleHttp\Middleware::history($container));

        $client = new Client([
            'url'     => 'http://localhost:8080',
            'token'   => 'static-token',
            'handler' => $stack,
        ]);

        $client->auth->signIn('user@example.com', 'pass');
        $client->from('orders')->execute();

        // The second request (query) should use the session token, not the static one.
        $this->assertSame(
            'Bearer session-token',
            $container[1]['request']->getHeaderLine('Authorization'),
        );
    }
}
