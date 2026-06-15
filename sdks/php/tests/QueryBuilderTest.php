<?php

declare(strict_types=1);

namespace Basin\Tests;

use Basin\Http\Transport;
use Basin\Query\QueryBuilder;
use Basin\Types\QueryResult;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

class QueryBuilderTest extends TestCase
{
    /** @var list<array<string,mixed>> */
    private array $container = [];

    private function makeBuilder(MockHandler $mock, string $table = 'orders'): QueryBuilder
    {
        $this->container = [];
        $stack = HandlerStack::create($mock);
        $stack->push(Middleware::history($this->container));
        $transport = new Transport('http://localhost:8080', handler: $stack);
        return new QueryBuilder($transport, $table);
    }

    private function requestUri(): string
    {
        return (string) $this->container[0]['request']->getUri();
    }

    // ------------------------------------------------------------------
    // Basic query construction
    // ------------------------------------------------------------------

    public function testSelectAllByDefault(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->select()->execute();

        $this->assertStringContainsString('select=%2A', $this->requestUri()); // '*' encoded
    }

    public function testSelectColumns(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->select('id,total')->execute();

        $this->assertStringContainsString('select=id%2Ctotal', $this->requestUri());
    }

    public function testSelectArray(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->select(['id', 'total'])->execute();

        $this->assertStringContainsString('select=id%2Ctotal', $this->requestUri());
    }

    public function testEqFilter(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->eq('status', 'paid')->execute();

        $this->assertStringContainsString('status=eq.paid', $this->requestUri());
    }

    public function testNeqFilter(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->neq('status', 'cancelled')->execute();

        $this->assertStringContainsString('status=neq.cancelled', $this->requestUri());
    }

    public function testGteFilter(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->gte('total', 100)->execute();

        $this->assertStringContainsString('total=gte.100', $this->requestUri());
    }

    public function testLtFilter(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->lt('total', 500)->execute();

        $this->assertStringContainsString('total=lt.500', $this->requestUri());
    }

    public function testInFilter(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->in('status', ['paid', 'pending'])->execute();

        // in.(paid,pending)
        $this->assertStringContainsString('status=in.', $this->requestUri());
        $this->assertStringContainsString('paid%2Cpending', $this->requestUri());
    }

    public function testIsNull(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->is('deleted_at', 'null')->execute();

        $this->assertStringContainsString('deleted_at=is.null', $this->requestUri());
    }

    public function testOrderAsc(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->order('created_at')->execute();

        $this->assertStringContainsString('order=created_at.asc', $this->requestUri());
    }

    public function testOrderDesc(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->order('created_at', ascending: false)->execute();

        $this->assertStringContainsString('order=created_at.desc', $this->requestUri());
    }

    public function testLimitAndOffset(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '{"rows":[],"next_cursor":null}')]);
        $builder = $this->makeBuilder($mock);

        $builder->limit(25)->offset(50)->execute();

        $uri = $this->requestUri();
        $this->assertStringContainsString('limit=25', $uri);
        $this->assertStringContainsString('offset=50', $uri);
    }

    public function testCursorPagination(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '{"rows":[],"next_cursor":null}')]);
        $builder = $this->makeBuilder($mock);

        $builder->cursor('opaque-token-abc')->execute();

        $this->assertStringContainsString('cursor=opaque-token-abc', $this->requestUri());
    }

    // ------------------------------------------------------------------
    // Response normalisation
    // ------------------------------------------------------------------

    public function testPlainArrayResponse(): void
    {
        $rows = [['id' => 1, 'total' => 50], ['id' => 2, 'total' => 150]];
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode($rows)),
        ]);
        $builder = $this->makeBuilder($mock);

        $result = $builder->execute();

        $this->assertInstanceOf(QueryResult::class, $result);
        $this->assertCount(2, $result->rows);
        $this->assertNull($result->nextCursor);
    }

    public function testPaginatedResponse(): void
    {
        $body = ['rows' => [['id' => 1]], 'next_cursor' => 'cursor_abc'];
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode($body)),
        ]);
        $builder = $this->makeBuilder($mock);

        $result = $builder->limit(1)->execute();

        $this->assertCount(1, $result->rows);
        $this->assertSame('cursor_abc', $result->nextCursor);
    }

    // ------------------------------------------------------------------
    // Write operations
    // ------------------------------------------------------------------

    public function testInsertSendsPost(): void
    {
        $mock = new MockHandler([
            new Response(201, ['Content-Type' => 'application/json'], '{"ok":true,"tag":"INSERT 1"}'),
        ]);
        $builder = $this->makeBuilder($mock);

        $result = $builder->insert(['total' => 50, 'status' => 'new']);

        $this->assertSame('POST', $this->container[0]['request']->getMethod());
        $this->assertTrue($result['ok']);
    }

    public function testUpdateSendsPatch(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true,"tag":"UPDATE 1"}'),
        ]);
        $builder = $this->makeBuilder($mock);

        $result = $builder->eq('id', 1)->update(['status' => 'paid']);

        $this->assertSame('PATCH', $this->container[0]['request']->getMethod());
        $this->assertTrue($result['ok']);
    }

    public function testDeleteSendsDelete(): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"ok":true,"tag":"DELETE 1"}'),
        ]);
        $builder = $this->makeBuilder($mock);

        $result = $builder->eq('id', 1)->delete();

        $this->assertSame('DELETE', $this->container[0]['request']->getMethod());
        $this->assertTrue($result['ok']);
    }

    // ------------------------------------------------------------------
    // Chaining + URL path
    // ------------------------------------------------------------------

    public function testFullChainBuildsCorrectUrl(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock, 'users');

        $builder
            ->select('id,name')
            ->eq('active', true)
            ->order('name')
            ->limit(10)
            ->execute();

        $uri = $this->requestUri();
        $this->assertStringContainsString('/rest/v1/users', $uri);
        $this->assertStringContainsString('select=id%2Cname', $uri);
        $this->assertStringContainsString('active=eq.true', $uri);
        $this->assertStringContainsString('order=name.asc', $uri);
        $this->assertStringContainsString('limit=10', $uri);
    }

    public function testGetReturnsRowsOnly(): void
    {
        $rows = [['id' => 1]];
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], json_encode($rows)),
        ]);
        $builder = $this->makeBuilder($mock);

        $result = $builder->get();

        $this->assertIsArray($result);
        $this->assertSame(1, $result[0]['id']);
    }

    // ------------------------------------------------------------------
    // Null / bool literal encoding
    // ------------------------------------------------------------------

    public function testNullLiteral(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->eq('deleted_at', null)->execute();

        $this->assertStringContainsString('deleted_at=eq.null', $this->requestUri());
    }

    public function testBoolTrueLiteral(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->eq('active', true)->execute();

        $this->assertStringContainsString('active=eq.true', $this->requestUri());
    }

    public function testBoolFalseLiteral(): void
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'application/json'], '[]')]);
        $builder = $this->makeBuilder($mock);

        $builder->eq('active', false)->execute();

        $this->assertStringContainsString('active=eq.false', $this->requestUri());
    }
}
