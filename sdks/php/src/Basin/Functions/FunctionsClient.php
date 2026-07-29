<?php

declare(strict_types=1);

namespace Basin\Functions;

use Basin\Exception\BasinApiException;
use Basin\Http\Transport;
use Basin\Types\FunctionInvokeResult;

/**
 * Functions client — Basin's two function-invocation surfaces.
 *
 * Route sources (verified, crates/basin-rest/src/server.rs):
 *   ANY  /fn/v1/:name           server.rs:290-291 (any_fn_handler)
 *     HTTP-handler-shape Wasm functions. The guest's response (status,
 *     headers, body) is proxied back verbatim.
 *   POST /rest/v1/rpc/:fn_name  server.rs:288 (post_rpc)
 *     Catalog SQL / Wasm UDFs. Body is a JSON object of named args;
 *     scalar results unwrap to the bare value, table results return an
 *     array of row objects.
 *
 * Both are Bearer-authenticated.
 */
class FunctionsClient
{
    public function __construct(
        private readonly Transport $transport,
    ) {
    }

    /**
     * ANY /fn/v1/:name — invoke an HTTP-handler function.
     *
     * The function's response is proxied verbatim. Non-2xx statuses from the
     * function are NOT automatically errors — unless the body is Basin's own
     * error envelope (auth 401, unknown function 404, invocation failure 500),
     * which throws BasinApiException as usual.
     *
     * @param string $name      Function name
     * @param string $method    HTTP method (default POST)
     * @param mixed $body       Request body: string/bytes passed raw, array JSON-encoded
     * @param array<string,string> $headers  Additional headers
     */
    public function invoke(
        string $name,
        string $method = 'POST',
        mixed $body = null,
        array $headers = [],
    ): FunctionInvokeResult {
        $hdrs = $headers;

        // Add auth manually — raw() on transport won't JSON-encode for us here.
        $token = $this->transport->resolveToken();
        if ($token !== null) {
            $hdrs['Authorization'] = "Bearer {$token}";
        }

        // Encode body: arrays become JSON; strings/resources pass raw.
        $encodedBody = null;
        if ($body !== null) {
            if (is_array($body) || is_object($body)) {
                $hdrs['Content-Type'] = $hdrs['Content-Type'] ?? 'application/json';
                $encodedBody = json_encode($body, JSON_THROW_ON_ERROR);
            } else {
                $encodedBody = $body;
            }
        }

        $response = $this->transport->raw(
            $method,
            '/fn/v1/' . rawurlencode($name),
            body: $encodedBody,
            headers: $hdrs,
            auth: false, // we set auth above manually
        );

        $status = $response->getStatusCode();
        $ct = $response->getHeaderLine('Content-Type');
        $bodyStr = (string) $response->getBody();

        $data = $bodyStr;
        if (str_contains($ct, 'json') && $bodyStr !== '') {
            try {
                $data = json_decode($bodyStr, true, 512, JSON_THROW_ON_ERROR);
            } catch (\JsonException) {
                // Leave as string if not valid JSON.
            }
        }

        // Basin-layer failures (not the function's own responses) use the
        // standard error envelope; surface those as typed exceptions.
        if ($status < 200 || $status >= 300) {
            if (is_array($data)
                && is_string($data['code'] ?? null)
                && str_starts_with($data['code'], 'E_')
                && is_string($data['message'] ?? null)
            ) {
                throw BasinApiException::fromResponseBody($data, $status);
            }
        }

        return new FunctionInvokeResult(
            status: $status,
            headers: array_map(
                fn(array $v) => implode(', ', $v),
                $response->getHeaders(),
            ),
            data: $data,
        );
    }

    /**
     * POST /rest/v1/rpc/:fn_name — call a catalog SQL / Wasm UDF.
     *
     * Args are named; missing declared args become NULL. Returns the bare
     * scalar for single-value results, or an array of rows for RETURNS TABLE.
     *
     * @param array<string,mixed> $args
     */
    public function rpc(string $fnName, array $args = []): mixed
    {
        return $this->transport->requestJson(
            'POST',
            '/rest/v1/rpc/' . rawurlencode($fnName),
            body: $args,
        );
    }
}
