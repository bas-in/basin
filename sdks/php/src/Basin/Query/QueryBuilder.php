<?php

declare(strict_types=1);

namespace Basin\Query;

use Basin\Http\Transport;
use Basin\Types\QueryResult;

/**
 * Fluent query builder for /rest/v1/:table.
 *
 * Route: GET|POST|PATCH|DELETE /rest/v1/:table
 * Source: crates/basin-rest/src/server.rs:286-300
 *
 * Filter grammar (Basin's PostgREST-style dialect, parser.rs):
 *   select=<cols,csv>
 *   <col>=eq.<value>   eq|neq|gt|gte|lt|lte|in|is
 *   in.(a,b,c)  — parenthesised list
 *   is.null / is.notnull
 *   order=<col>.asc|desc (repeatable)
 *   limit=N, offset=N
 *   cursor=<token>  keyset pagination
 *
 * NOT full PostgREST: no or=, not., like|ilike, embedded resource selects,
 * Prefer headers. All filters AND together.
 *
 * Response shapes:
 *   plain GET → JSON array of rows
 *   GET with limit or cursor → {rows, next_cursor}
 *   GET with ?stream=true → NDJSON, final line {"_basin_next_cursor":"..."}
 *   POST → 201, {ok, tag} or rows; PATCH/DELETE → {ok, tag}
 *
 * Usage:
 *   $result = $client->from('orders')
 *       ->select('id,total')
 *       ->gte('total', 100)
 *       ->limit(50)
 *       ->execute();
 *
 *   foreach ($result->rows as $row) { ... }
 *   $nextPage = $client->from('orders')->cursor($result->nextCursor)->execute();
 */
class QueryBuilder
{
    /** @var list<array{string,string}> */
    private array $query = [];

    public function __construct(
        private readonly Transport $transport,
        private readonly string $table,
    ) {
    }

    // ------------------------------------------------------------------
    // Projection / filter / ordering — all return $this for chaining
    // ------------------------------------------------------------------

    /**
     * select=<cols> projection; omit or pass '*' for all columns.
     *
     * @param string|string[]|null $columns Column CSV, array, or null for '*'
     */
    public function select(string|array|null $columns = null): static
    {
        if (is_array($columns)) {
            $cols = implode(',', $columns);
        } else {
            $cols = $columns ?? '*';
        }
        $this->query[] = ['select', $cols];
        return $this;
    }

    public function eq(string $column, int|float|string|bool|null $value): static
    {
        $this->query[] = [$column, 'eq.' . $this->literal($value)];
        return $this;
    }

    public function neq(string $column, int|float|string|bool|null $value): static
    {
        $this->query[] = [$column, 'neq.' . $this->literal($value)];
        return $this;
    }

    public function gt(string $column, int|float|string $value): static
    {
        $this->query[] = [$column, 'gt.' . $this->literal($value)];
        return $this;
    }

    public function gte(string $column, int|float|string $value): static
    {
        $this->query[] = [$column, 'gte.' . $this->literal($value)];
        return $this;
    }

    public function lt(string $column, int|float|string $value): static
    {
        $this->query[] = [$column, 'lt.' . $this->literal($value)];
        return $this;
    }

    public function lte(string $column, int|float|string $value): static
    {
        $this->query[] = [$column, 'lte.' . $this->literal($value)];
        return $this;
    }

    /**
     * <col>=in.(a,b,c) — parenthesised list per parser.rs parse_in_list.
     *
     * @param list<int|float|string|bool|null> $values
     */
    public function in(string $column, array $values): static
    {
        $parts = array_map(fn($v) => $this->literal($v), $values);
        $this->query[] = [$column, 'in.(' . implode(',', $parts) . ')'];
        return $this;
    }

    /**
     * <col>=is.null / <col>=is.notnull.
     */
    public function is(string $column, string $value): static
    {
        $this->query[] = [$column, "is.{$value}"];
        return $this;
    }

    /**
     * order=<col>.asc|desc (repeatable for multi-column sort).
     */
    public function order(string $column, bool $ascending = true): static
    {
        $dir = $ascending ? 'asc' : 'desc';
        $this->query[] = ['order', "{$column}.{$dir}"];
        return $this;
    }

    /**
     * limit=N. Switches the GET response to {rows, next_cursor}.
     */
    public function limit(int $n): static
    {
        $this->query[] = ['limit', (string) $n];
        return $this;
    }

    public function offset(int $n): static
    {
        $this->query[] = ['offset', (string) $n];
        return $this;
    }

    /**
     * Resume keyset pagination from a next_cursor token.
     */
    public function cursor(string $token): static
    {
        $this->query[] = ['cursor', $token];
        return $this;
    }

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------

    /**
     * Execute as GET and return a QueryResult (rows + optional next_cursor).
     */
    public function execute(): QueryResult
    {
        $body = $this->transport->requestJson('GET', "/rest/v1/{$this->table}", query: $this->query);
        return QueryResult::fromBody($body);
    }

    /**
     * Execute as GET and return rows only.
     * @return list<array<string,mixed>>
     */
    public function get(): array
    {
        return $this->execute()->rows;
    }

    /**
     * Execute as GET with ?stream=true (NDJSON), yielding rows as a generator.
     *
     * The final {"_basin_next_cursor":...} line is captured (not yielded) and
     * stored; retrieve it via the generator's return value after iteration.
     *
     * @return \Generator<int,array<string,mixed>,null,string|null>
     */
    public function stream(): \Generator
    {
        $query = array_merge($this->query, [['stream', 'true']]);
        $response = $this->transport->raw('GET', "/rest/v1/{$this->table}", query: $query);
        $body = (string) $response->getBody();

        $nextCursor = null;
        foreach (explode("\n", $body) as $line) {
            $line = trim($line);
            if ($line === '') {
                continue;
            }
            $parsed = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            if (isset($parsed['_basin_next_cursor'])) {
                $nextCursor = (string) $parsed['_basin_next_cursor'];
                continue;
            }
            yield $parsed;
        }

        return $nextCursor;
    }

    /**
     * POST /rest/v1/:table — insert one row or an array of rows (201).
     *
     * @param array<string,mixed>|list<array<string,mixed>> $values
     * @return mixed {ok, tag} or rows
     */
    public function insert(array $values): mixed
    {
        return $this->transport->requestJson('POST', "/rest/v1/{$this->table}", body: $values);
    }

    /**
     * PATCH /rest/v1/:table?<filters> — update rows matching the current filters.
     *
     * @param array<string,mixed> $values
     * @return mixed {ok, tag}
     */
    public function update(array $values): mixed
    {
        return $this->transport->requestJson(
            'PATCH',
            "/rest/v1/{$this->table}",
            query: $this->query,
            body: $values,
        );
    }

    /**
     * DELETE /rest/v1/:table?<filters>.
     *
     * May throw BasinApiException with errorCode E_ENGINE_UNSUPPORTED (501)
     * on engines without DELETE support.
     *
     * @return mixed {ok, tag}
     */
    public function delete(): mixed
    {
        return $this->transport->requestJson(
            'DELETE',
            "/rest/v1/{$this->table}",
            query: $this->query,
        );
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private function literal(int|float|string|bool|null $v): string
    {
        if ($v === null) {
            return 'null';
        }
        if (is_bool($v)) {
            return $v ? 'true' : 'false';
        }
        return (string) $v;
    }
}
