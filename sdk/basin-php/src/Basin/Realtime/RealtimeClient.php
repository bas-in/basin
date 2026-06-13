<?php

declare(strict_types=1);

namespace Basin\Realtime;

use Basin\Exception\BasinApiException;
use Basin\Exception\BasinNetworkException;

/**
 * Realtime WebSocket client for Basin.
 *
 * Connects to GET /realtime/v1/ws/:project.
 *
 * Protocol source: crates/basin-realtime/src/ws.rs (ClientMsg / ServerMsg
 * + presence.rs serialize_presence_* helpers).
 *
 * Auth: Sec-WebSocket-Protocol: basin-v1, <token>  — the server also accepts
 * Authorization: Bearer but the subprotocol form is preferred for browser
 * compatibility and is what the JS SDK uses.
 *
 * Requires the optional textalk/websocket package:
 *   composer require textalk/websocket
 *
 * Alternatively ratchet/pawl can be used for async (ReactPHP) environments.
 *
 * Usage (synchronous, textalk/websocket):
 *
 *   $realtime = $client->realtime();
 *   $realtime->subscribe('orders', function (array $event): void {
 *       echo $event['op'] . PHP_EOL;
 *   });
 *
 * Wire frames (client → server, JSON text):
 *   {"type":"subscribe","table":"orders"}
 *   {"type":"subscribe","table":"users","filter":"NEW.status = 'paid'"}
 *   {"type":"unsubscribe","table":"orders"}
 *   {"type":"presence_track","channel":"room:1","client_id":"c1","metadata":{}}
 *   {"type":"presence_untrack","channel":"room:1","client_id":"c1"}
 *   {"type":"heartbeat","channel":"room:1","client_id":"c1"}
 *
 * Server → client event frames:
 *   {"type":"event","project":"...","table":"orders","op":"INSERT","after":{...},"seq":42}
 *   {"type":"error","code":"lag","table":"orders","missed":5}
 *   {"type":"subscribed","table":"orders"}
 *   {"type":"unsubscribed","table":"orders"}
 *   {"type":"presence_state","channel":"...","presences":[...]}
 *   {"type":"presence_diff","channel":"...","joins":[...],"leaves":[...]}
 *   {"type":"presenceerror","code":"...","channel":"...","message":"..."}
 *   (note: "presenceerror" has no underscore — rename_all="lowercase" in ws.rs)
 *
 * Server WebSocket close codes:
 *   4001 — unauthorized (JWT missing or invalid)
 *   4003 — forbidden (cross-project JWT mismatch)
 *   4008 — project_deleted
 *
 * Reconnect: exponential backoff (0.5 s → 1 s → 2 s … max 30 s).
 * All active subscriptions are reinstated after reconnect.
 */
class RealtimeClient
{
    private const BACKOFF_BASE = 0.5;
    private const BACKOFF_MAX  = 30.0;
    private const BACKOFF_FACTOR = 2.0;

    /** @var object|null textalk WebSocket client instance */
    private mixed $ws = null;

    /** @var array<string,array{filter:string|null,last_event_id:int|null}> Active subscriptions keyed by table */
    private array $subscriptions = [];

    /** @var callable[] Registered callbacks keyed by table */
    private array $callbacks = [];

    public function __construct(
        private readonly string $baseUrl,
        private readonly \Closure $tokenGetter,
        private readonly \Closure $projectIdGetter,
    ) {
    }

    // ------------------------------------------------------------------
    // Connection
    // ------------------------------------------------------------------

    /**
     * Open the WebSocket connection.
     *
     * Requires textalk/websocket. Throws \RuntimeException if the package is
     * not installed (add composer require textalk/websocket to your project).
     */
    public function connect(): void
    {
        if ($this->ws !== null) {
            return;
        }

        $this->ws = $this->buildConnection();
    }

    /**
     * Close the WebSocket connection.
     */
    public function disconnect(): void
    {
        if ($this->ws !== null) {
            try {
                $this->ws->close();
            } catch (\Throwable) {
            }
            $this->ws = null;
        }
    }

    public function isConnected(): bool
    {
        return $this->ws !== null;
    }

    // ------------------------------------------------------------------
    // Subscribe / unsubscribe
    // ------------------------------------------------------------------

    /**
     * Subscribe to a table with a callback.
     *
     * The callback receives the decoded server frame (array) on each event.
     * Call this before listen() to register interest; then call listen() in
     * your event loop.
     *
     * @param callable(array):void $callback
     */
    public function subscribe(
        string $table,
        callable $callback,
        ?string $filter = null,
        ?int $lastEventId = null,
    ): void {
        $this->connect();
        $this->subscriptions[$table] = [
            'filter'        => $filter,
            'last_event_id' => $lastEventId,
        ];
        $this->callbacks[$table] = $callback;
        $this->sendSubscribe($table, $filter, $lastEventId);
    }

    /**
     * Unsubscribe from a table.
     */
    public function unsubscribe(string $table): void
    {
        if ($this->ws === null) {
            return;
        }
        $this->send(['type' => 'unsubscribe', 'table' => $table]);
        unset($this->subscriptions[$table], $this->callbacks[$table]);
    }

    // ------------------------------------------------------------------
    // Event loop
    // ------------------------------------------------------------------

    /**
     * Listen for incoming frames and dispatch them to registered callbacks.
     *
     * Runs synchronously until $maxMessages frames are processed or $timeoutMs
     * elapses. Set $maxMessages=null and $timeoutMs=null to loop indefinitely
     * (typically in a worker process / CLI script).
     *
     * @param int|null $maxMessages Stop after N messages (null = unlimited)
     * @param int|null $timeoutMs   Stop after N ms (null = unlimited)
     */
    public function listen(?int $maxMessages = null, ?int $timeoutMs = null): void
    {
        if ($this->ws === null) {
            $this->connect();
        }

        $received = 0;
        $deadline = $timeoutMs !== null ? microtime(true) + ($timeoutMs / 1000) : null;

        while (true) {
            if ($maxMessages !== null && $received >= $maxMessages) {
                break;
            }
            if ($deadline !== null && microtime(true) >= $deadline) {
                break;
            }

            try {
                $rawMsg = $this->ws->receive();
            } catch (\Throwable $e) {
                // Connection dropped — attempt reconnect.
                $this->ws = null;
                $this->reconnectWithBackoff();
                continue;
            }

            if ($rawMsg === null) {
                continue;
            }

            $frame = $this->parseFrame((string) $rawMsg);
            if ($frame === null) {
                continue;
            }

            $received++;
            $this->dispatchFrame($frame);
        }
    }

    // ------------------------------------------------------------------
    // Presence API
    // ------------------------------------------------------------------

    /**
     * Send presence_track message.
     */
    public function presenceTrack(string $channel, string $clientId, mixed $metadata = null): void
    {
        $this->connect();
        $this->send([
            'type'      => 'presence_track',
            'channel'   => $channel,
            'client_id' => $clientId,
            'metadata'  => $metadata ?? (object) [],
        ]);
    }

    /**
     * Send presence_untrack message.
     */
    public function presenceUntrack(string $channel, string $clientId): void
    {
        $this->connect();
        $this->send([
            'type'      => 'presence_untrack',
            'channel'   => $channel,
            'client_id' => $clientId,
        ]);
    }

    /**
     * Send heartbeat to refresh the presence TTL.
     */
    public function presenceHeartbeat(string $channel, string $clientId): void
    {
        $this->connect();
        $this->send([
            'type'      => 'heartbeat',
            'channel'   => $channel,
            'client_id' => $clientId,
        ]);
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private function buildConnection(): mixed
    {
        $this->assertWebSocketPackageLoaded();

        $project = ($this->projectIdGetter)();
        if ($project === null) {
            throw new BasinApiException(
                'E_INVALID_REQUEST',
                'realtime requires a project id — pass project_id to new Client()',
                0,
            );
        }

        $token = ($this->tokenGetter)();
        if ($token === null) {
            throw new BasinApiException(
                'E_UNAUTHENTICATED',
                'realtime requires a JWT or API key',
                0,
            );
        }

        $wsBase = rtrim($this->baseUrl, '/');
        $wsBase = str_replace(['https://', 'http://'], ['wss://', 'ws://'], $wsBase);
        $url = "{$wsBase}/realtime/v1/ws/{$project}";

        // Auth via Sec-WebSocket-Protocol: basin-v1, <token>
        // This is the preferred form (browser-compatible, matches JS SDK).
        $options = [
            'headers' => [
                'Sec-WebSocket-Protocol' => "basin-v1, {$token}",
            ],
        ];

        return new \WebSocket\Client($url, $options);
    }

    private function sendSubscribe(string $table, ?string $filter, ?int $lastEventId): void
    {
        $msg = ['type' => 'subscribe', 'table' => $table];
        if ($filter !== null) {
            $msg['filter'] = $filter;
        }
        if ($lastEventId !== null) {
            $msg['last_event_id'] = $lastEventId;
        }
        $this->send($msg);
    }

    /** @param array<string,mixed> $msg */
    private function send(array $msg): void
    {
        if ($this->ws === null) {
            throw new BasinApiException('E_UNKNOWN', 'realtime socket is not open', 0);
        }
        $this->ws->send(json_encode($msg, JSON_THROW_ON_ERROR));
    }

    /** @return array<string,mixed>|null */
    private function parseFrame(string $raw): ?array
    {
        if ($raw === '') {
            return null;
        }
        try {
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            return is_array($decoded) ? $decoded : null;
        } catch (\JsonException) {
            return null;
        }
    }

    /** @param array<string,mixed> $frame */
    private function dispatchFrame(array $frame): void
    {
        $type = $frame['type'] ?? '';

        switch ($type) {
            case 'event':
            case 'error':
            case 'gap':
                $table = $frame['table'] ?? '';
                if (isset($this->callbacks[$table])) {
                    ($this->callbacks[$table])($frame);
                }
                break;

            case 'subscribed':
            case 'unsubscribed':
                // Ack frames — no user callback needed.
                break;

            case 'presence_state':
            case 'presence_diff':
            case 'presenceerror': // no underscore — rename_all="lowercase" in ws.rs
                $channel = $frame['channel'] ?? '';
                if (isset($this->callbacks[$channel])) {
                    ($this->callbacks[$channel])($frame);
                }
                break;

            default:
                // Unknown frame type — ignore (forward compat).
        }
    }

    private function reconnectWithBackoff(): void
    {
        $attempt = 0;
        while (true) {
            $delay = min(
                self::BACKOFF_BASE * (self::BACKOFF_FACTOR ** $attempt),
                self::BACKOFF_MAX,
            );
            usleep((int) ($delay * 1_000_000));
            $attempt++;

            try {
                $this->ws = $this->buildConnection();
                // Reinstate all active subscriptions.
                foreach ($this->subscriptions as $table => $opts) {
                    $this->sendSubscribe($table, $opts['filter'], $opts['last_event_id']);
                }
                return;
            } catch (\Throwable) {
                // Try again.
            }
        }
    }

    private function assertWebSocketPackageLoaded(): void
    {
        if (!class_exists(\WebSocket\Client::class)) {
            throw new \RuntimeException(
                "textalk/websocket is required for Basin realtime support.\n"
                . "Install it with: composer require textalk/websocket\n"
                . "Alternatively ratchet/pawl can be used for async (ReactPHP) environments.",
            );
        }
    }
}
