<?php

declare(strict_types=1);

namespace Basin\Laravel;

use Basin\Client;
use Illuminate\Support\Facades\Facade;

/**
 * Laravel facade for the Basin client.
 *
 * @method static \Basin\Query\QueryBuilder from(string $table)
 * @method static \Basin\Query\QueryBuilder table(string $table)
 * @method static \Basin\Auth\AuthClient auth()
 * @method static \Basin\Storage\StorageClient storage()
 * @method static \Basin\Functions\FunctionsClient functions()
 * @method static \Basin\Realtime\RealtimeClient realtime()
 * @method static mixed rpc(string $fnName, array $args = [])
 * @method static string health()
 *
 * @see \Basin\Client
 */
class BasinFacade extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return Client::class;
    }
}
