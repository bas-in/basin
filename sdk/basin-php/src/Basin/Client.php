<?php

declare(strict_types=1);

namespace Basin;

use Basin\Auth\AuthClient;
use Basin\Exception\BasinApiException;
use Basin\Functions\FunctionsClient;
use Basin\Http\Transport;
use Basin\Query\QueryBuilder;
use Basin\Realtime\RealtimeClient;
use Basin\Storage\StorageClient;
use GuzzleHttp\HandlerStack;

/**
 * Basin PHP client.
 *
 * The primary entry point for all Basin SDK operations.
 *
 *   $basin = new Basin\Client([
 *       'url'        => 'https://api.basin.run',
 *       'token'      => 'my-api-key',
 *       'project_id' => '01J...',
 *   ]);
 *
 *   // Query builder
 *   $result = $basin->from('orders')
 *       ->select('id,total')
 *       ->eq('status', 'paid')
 *       ->gte('total', 100)
 *       ->limit(50)
 *       ->execute();
 *
 *   // Auth
 *   $session = $basin->auth->signIn('user@example.com', 'password');
 *
 *   // Storage
 *   $basin->storage()->fromBucket('avatars')->upload('user.jpg', $bytes, 'image/jpeg');
 *
 *   // Functions
 *   $result = $basin->functions->rpc('my_function', ['arg1' => 'value']);
 *
 * Configuration keys:
 *   url         (string)  required  Base URL of the Basin server
 *   token       (string)  optional  JWT or raw API key (Authorization: Bearer)
 *   project_id  (string)  optional  Project ULID; required for public storage
 *                                   URLs, realtime, and auth operations
 *   timeout     (float)   optional  Request timeout in seconds (default: 30)
 *   handler     (HandlerStack) optional  Guzzle handler stack (for testing)
 */
class Client
{
    public readonly AuthClient $auth;
    public readonly FunctionsClient $functions;

    private readonly Transport $transport;
    private readonly ?string $explicitProjectId;
    private readonly ?string $staticToken;
    private ?RealtimeClient $realtimeClient = null;
    private ?StorageClient $storageClient = null;

    /**
     * @param array{
     *   url: string,
     *   token?: string|null,
     *   project_id?: string|null,
     *   timeout?: float,
     *   handler?: HandlerStack|null,
     * } $config
     */
    public function __construct(array $config)
    {
        $url = $config['url'] ?? throw new \InvalidArgumentException('Basin Client: "url" is required');
        $token = $config['token'] ?? null;
        $projectId = $config['project_id'] ?? null;
        $timeout = (float) ($config['timeout'] ?? 30.0);
        $handler = $config['handler'] ?? null;

        $this->staticToken = $token;
        $this->explicitProjectId = $projectId;

        $this->transport = new Transport(
            baseUrl: $url,
            token: $token,
            timeout: $timeout,
            handler: $handler,
        );

        $this->auth = new AuthClient($this->transport, $projectId);
        $this->functions = new FunctionsClient($this->transport);

        // Wire the transport's token resolver to the auth client so every
        // request uses the freshest session token (auto-refresh).
        $this->transport->setTokenGetter(fn() => $this->auth->accessToken());
    }

    // ------------------------------------------------------------------
    // Query builder
    // ------------------------------------------------------------------

    /**
     * Return a QueryBuilder for /rest/v1/:table.
     *
     * Chainable fluent interface:
     *   $basin->from('orders')->select('id,total')->gte('total', 100)->execute()
     */
    public function from(string $table): QueryBuilder
    {
        return new QueryBuilder($this->transport, $table);
    }

    /** Alias for from() — matches the JS SDK style. */
    public function table(string $table): QueryBuilder
    {
        return $this->from($table);
    }

    // ------------------------------------------------------------------
    // Storage
    // ------------------------------------------------------------------

    /**
     * Return the storage client.
     *
     *   $basin->storage()->createBucket('avatars');
     *   $basin->storage()->fromBucket('avatars')->upload('file.jpg', $bytes);
     */
    public function storage(): StorageClient
    {
        if ($this->storageClient === null) {
            $this->storageClient = new StorageClient(
                $this->transport,
                fn() => $this->resolveProjectId(),
            );
        }
        return $this->storageClient;
    }

    // ------------------------------------------------------------------
    // Functions
    // ------------------------------------------------------------------

    /**
     * POST /rest/v1/rpc/:fn — convenience alias for functions->rpc().
     *
     * @param array<string,mixed> $args
     */
    public function rpc(string $fnName, array $args = []): mixed
    {
        return $this->functions->rpc($fnName, $args);
    }

    // ------------------------------------------------------------------
    // Realtime
    // ------------------------------------------------------------------

    /**
     * Return the realtime WebSocket client (lazy).
     *
     * Requires textalk/websocket:
     *   composer require textalk/websocket
     *
     *   $rt = $basin->realtime();
     *   $rt->subscribe('orders', function (array $event) { ... });
     *   $rt->listen();
     */
    public function realtime(): RealtimeClient
    {
        if ($this->realtimeClient === null) {
            $this->realtimeClient = new RealtimeClient(
                baseUrl: $this->transport->getBaseUrl(),
                tokenGetter: fn() => $this->auth->accessToken() ?? $this->staticToken,
                projectIdGetter: fn() => $this->resolveProjectId(),
            );
        }
        return $this->realtimeClient;
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    /**
     * GET /health → 'ok'.
     */
    public function health(): string
    {
        $response = $this->transport->raw('GET', '/health', auth: false);
        return trim((string) $response->getBody());
    }

    /**
     * Escape hatch for routes not yet wrapped (e.g. /admin/v1/*).
     *
     * @param list<array{string,string}>|null $query
     * @return mixed
     */
    public function request(string $method, string $path, ?array $query = null, mixed $body = null): mixed
    {
        return $this->transport->requestJson($method, $path, query: $query, body: $body);
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    private function resolveProjectId(): ?string
    {
        if ($this->explicitProjectId !== null) {
            return $this->explicitProjectId;
        }
        $session = $this->auth->getSession();
        if ($session !== null) {
            $fromSession = $this->projectIdFromJwt($session->accessToken);
            if ($fromSession !== null) {
                return $fromSession;
            }
        }
        if ($this->staticToken !== null) {
            return $this->projectIdFromJwt($this->staticToken);
        }
        return null;
    }

    private function projectIdFromJwt(string $token): ?string
    {
        $parts = explode('.', $token);
        if (count($parts) !== 3) {
            return null;
        }
        try {
            $payload = base64_decode(strtr($parts[1], '-_', '+/'), true);
            if ($payload === false) {
                return null;
            }
            $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
            $val = $decoded['project_id'] ?? null;
            return is_string($val) ? $val : null;
        } catch (\Throwable) {
            return null;
        }
    }
}
