<?php

declare(strict_types=1);

namespace Basin\Types;

/** Proxied response from an HTTP-handler function (ANY /fn/v1/:name). */
final class FunctionInvokeResult
{
    public function __construct(
        public readonly int $status,
        /** @var array<string,string> */
        public readonly array $headers,
        public readonly mixed $data,
    ) {
    }
}
