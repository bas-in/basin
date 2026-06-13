<?php

declare(strict_types=1);

namespace Basin\Http;

use Basin\Exception\BasinApiException;
use Basin\Exception\BasinNetworkException;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\ResponseInterface;

/**
 * HTTP transport layer backed by Guzzle.
 *
 * Rationale for Guzzle: it is the de-facto standard PHP HTTP client, ships
 * PSR-7 + PSR-18 support, has a well-tested MockHandler for PHPUnit, and is
 * already a transitive dependency in most Laravel / Symfony projects, so it
 * adds zero extra weight in practice. symfony/http-client was considered but
 * Guzzle's MockHandler ecosystem is more ergonomic for SDK unit testing.
 *
 * The transport is intentionally thin:
 *   - Builds Authorization: Bearer headers from a token getter callable.
 *   - Throws Basin\Exception\BasinApiException for non-2xx responses.
 *   - Throws Basin\Exception\BasinNetworkException for transport errors.
 *   - Returns decoded JSON for request() calls, raw ResponseInterface for
 *     raw() calls (file downloads, etc.).
 */
final class Transport
{
    private readonly GuzzleClient $client;

    /**
     * @param string $baseUrl     HTTP base URL, e.g. "https://api.basin.run"
     * @param string|null $token  Static bearer token (JWT or API key)
     * @param callable|null $tokenGetter  Callable returning the live token; overrides $token when set
     * @param float $timeout      Default request timeout in seconds
     * @param HandlerStack|null $handler  Injected handler stack (for testing with MockHandler)
     */
    public function __construct(
        private readonly string $baseUrl,
        private ?string $token = null,
        private mixed $tokenGetter = null,
        private readonly float $timeout = 30.0,
        ?HandlerStack $handler = null,
    ) {
        $config = [
            'base_uri' => rtrim($baseUrl, '/') . '/',
            'timeout'  => $timeout,
            'headers'  => [
                'Accept'       => 'application/json',
                'Content-Type' => 'application/json',
                'User-Agent'   => 'basin-php/1.0.0',
            ],
            'http_errors' => false, // we handle status codes ourselves
        ];

        if ($handler !== null) {
            $config['handler'] = $handler;
        }

        $this->client = new GuzzleClient($config);
    }

    public function getBaseUrl(): string
    {
        return rtrim($this->baseUrl, '/');
    }

    /** Inject a live-token getter (wired from AuthClient after construction). */
    public function setTokenGetter(callable $getter): void
    {
        $this->tokenGetter = $getter;
    }

    /** Set a static token (used when no session is active). */
    public function setToken(?string $token): void
    {
        $this->token = $token;
    }

    /** Return the current bearer token (live session takes precedence). */
    public function resolveToken(): ?string
    {
        if ($this->tokenGetter !== null) {
            $live = ($this->tokenGetter)();
            if ($live !== null) {
                return $live;
            }
        }

        return $this->token;
    }

    /**
     * Execute an HTTP request and return the decoded JSON body.
     *
     * @param string $method           HTTP method
     * @param string $path             Path relative to base URL (leading slash)
     * @param list<array{string,string}>|null $query  Query parameters as [key, value] pairs
     * @param mixed $body              JSON-serialisable body (null = no body)
     * @param array<string,string> $headers  Additional headers
     * @param bool $auth               Whether to add the Authorization header
     * @return mixed  Decoded JSON (array, scalar, or null)
     */
    public function requestJson(
        string $method,
        string $path,
        ?array $query = null,
        mixed $body = null,
        array $headers = [],
        bool $auth = true,
    ): mixed {
        $response = $this->rawRequest($method, $path, $query, $body, $headers, $auth);
        return $this->decodeJson($response);
    }

    /**
     * Execute an HTTP request and return the raw Guzzle response.
     * Used for binary downloads, streaming, etc.
     *
     * @param string $method
     * @param string $path
     * @param list<array{string,string}>|null $query
     * @param mixed $body
     * @param array<string,string> $headers
     * @param bool $auth
     */
    public function raw(
        string $method,
        string $path,
        ?array $query = null,
        mixed $body = null,
        array $headers = [],
        bool $auth = true,
    ): ResponseInterface {
        return $this->rawRequest($method, $path, $query, $body, $headers, $auth);
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private function rawRequest(
        string $method,
        string $path,
        ?array $query,
        mixed $body,
        array $headers,
        bool $auth,
    ): ResponseInterface {
        // Build URL
        $url = $this->buildUrl($path, $query);

        // Build headers
        $reqHeaders = $headers;
        if ($auth) {
            $token = $this->resolveToken();
            if ($token !== null) {
                $reqHeaders['Authorization'] = "Bearer {$token}";
            }
        }

        // Build request options
        $options = ['headers' => $reqHeaders];

        if ($body !== null) {
            if (is_string($body) || is_resource($body)) {
                // Raw binary body (e.g. file upload)
                $options['body'] = $body;
                // Remove default Content-Type so caller can set it explicitly
            } else {
                $options['json'] = $body;
            }
        }

        try {
            $response = $this->client->request($method, $url, $options);
        } catch (ConnectException $e) {
            throw new BasinNetworkException($e->getMessage(), 0, $e);
        } catch (RequestException $e) {
            if ($e->hasResponse()) {
                $response = $e->getResponse();
                // Fall through to status-code handling below.
            } else {
                throw new BasinNetworkException($e->getMessage(), 0, $e);
            }
        }

        $status = $response->getStatusCode();
        if ($status >= 200 && $status < 300) {
            return $response;
        }

        // Non-2xx — try to decode the error envelope.
        $bodyStr = (string) $response->getBody();
        $errorData = [];
        if ($bodyStr !== '') {
            try {
                $decoded = json_decode($bodyStr, true, 512, JSON_THROW_ON_ERROR);
                if (is_array($decoded)) {
                    $errorData = $decoded;
                }
            } catch (\JsonException) {
                // Not JSON — use an empty envelope; fromResponseBody will fill defaults.
            }
        }

        throw BasinApiException::fromResponseBody($errorData, $status);
    }

    /**
     * Decode the response body as JSON.
     *
     * Returns null for 204 No Content or empty bodies.
     */
    private function decodeJson(ResponseInterface $response): mixed
    {
        $body = (string) $response->getBody();
        if ($body === '' || $response->getStatusCode() === 204) {
            return null;
        }

        try {
            return json_decode($body, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new \RuntimeException("Basin: failed to decode JSON response: {$e->getMessage()}", 0, $e);
        }
    }

    /**
     * Build a full URL from a path and optional query pairs.
     *
     * @param list<array{string,string}>|null $query
     */
    private function buildUrl(string $path, ?array $query): string
    {
        $url = rtrim($this->baseUrl, '/') . '/' . ltrim($path, '/');

        if ($query !== null && count($query) > 0) {
            $parts = [];
            foreach ($query as [$k, $v]) {
                $parts[] = rawurlencode((string) $k) . '=' . rawurlencode((string) $v);
            }
            $url .= '?' . implode('&', $parts);
        }

        return $url;
    }
}
