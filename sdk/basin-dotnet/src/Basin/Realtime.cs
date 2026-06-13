using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Basin;

/// <summary>
/// Options for subscribing to a realtime channel.
/// </summary>
public sealed class SubscribeOptions
{
    /// <summary>
    /// Optional server-side SQL predicate, e.g. "NEW.status = 'paid'".
    /// Invalid predicates arrive as <see cref="RealtimeErrorFrame"/> with code "invalid_filter".
    /// </summary>
    public string? Filter { get; init; }

    /// <summary>
    /// Reconnect cursor — the last seq the client successfully processed.
    /// When given, the server replays missed events from its replay ring,
    /// or emits a <see cref="RealtimeGapFrame"/> if the ring has been evicted.
    /// </summary>
    public long? LastEventId { get; init; }
}

/// <summary>
/// Async WebSocket client for GET /realtime/v1/ws/:project.
///
/// Protocol source: crates/basin-realtime/src/ws.rs (ClientMsg / ServerMsg).
///
/// Auth: Sec-WebSocket-Protocol: basin-v1,&lt;token&gt; subprotocol header.
///
/// Reconnect: exponential backoff (0.5 s → 30 s) with automatic resubscription
/// of all active channels after reconnect.
///
/// Obtain via <see cref="BasinClient.Realtime"/> — do not construct directly.
/// </summary>
public sealed class RealtimeClient : IAsyncDisposable
{
    private readonly string _baseUrl;
    private readonly Func<CancellationToken, Task<string?>> _getToken;
    private readonly Func<string?> _getProjectId;
    // Factory: (url, subprotocols) → already-connected ClientWebSocket.
    // Used in tests to inject a FakeWebSocket without opening a real network connection.
    private readonly Func<string, IEnumerable<string>, ClientWebSocket>? _wsFactory;

    private ClientWebSocket? _ws;
    private CancellationTokenSource? _recvCts;
    private Task? _recvTask;

    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private volatile bool _closed;

    // Active subscriptions: table → options
    private readonly Dictionary<string, SubscribeOptions> _subscriptions = new();

    // Per-table + per-channel queues
    private readonly Dictionary<string, List<System.Threading.Channels.Channel<ServerFrame?>>> _channels = new();

    // Pending subscribe acks: table → TaskCompletionSource
    private readonly Dictionary<string, TaskCompletionSource<bool>> _pendingSubscribed = new();
    private readonly Dictionary<string, TaskCompletionSource<bool>> _pendingUnsubscribed = new();

    // Reconnect backoff params
    private const double BackoffBase = 0.5;
    private const double BackoffMax = 30.0;
    private const double BackoffFactor = 2.0;

    internal RealtimeClient(
        string baseUrl,
        Func<CancellationToken, Task<string?>> getToken,
        Func<string?> getProjectId,
        Func<string, IEnumerable<string>, ClientWebSocket>? wsFactory = null)
    {
        _baseUrl = baseUrl;
        _getToken = getToken;
        _getProjectId = getProjectId;
        _wsFactory = wsFactory;
    }

    /// <summary>True when the WebSocket connection is currently open.</summary>
    public bool IsConnected => _ws?.State == WebSocketState.Open;

    // ------------------------------------------------------------------
    // Connection management
    // ------------------------------------------------------------------

    /// <summary>Open the WebSocket connection. No-op if already connected.</summary>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        await _connectLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_ws?.State == WebSocketState.Open)
                return;

            await DoConnectAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _connectLock.Release();
        }
    }

    private async Task DoConnectAsync(CancellationToken ct)
    {
        var project = _getProjectId()
            ?? throw new BasinApiException(
                "E_INVALID_REQUEST",
                "realtime requires a project id — pass ProjectId to BasinClient.Builder",
                0);

        var token = await _getToken(ct).ConfigureAwait(false)
            ?? throw new BasinApiException(
                "E_UNAUTHENTICATED",
                "realtime requires a JWT or API key",
                0);

        // Build ws:// URL from http:// base.
        var wsBase = _baseUrl.TrimEnd('/');
        wsBase = wsBase
            .Replace("https://", "wss://", StringComparison.OrdinalIgnoreCase)
            .Replace("http://", "ws://", StringComparison.OrdinalIgnoreCase);

        var url = $"{wsBase}/realtime/v1/ws/{project}";

        // Auth: Sec-WebSocket-Protocol: basin-v1,<token>
        // This is the same subprotocol form used by JS + Python SDKs.
        var subprotocols = new[] { "basin-v1", token };

        ClientWebSocket ws;
        if (_wsFactory is not null)
        {
            // Test injection: factory returns an already-connected socket.
            ws = _wsFactory(url, subprotocols);
        }
        else
        {
            ws = new ClientWebSocket();
            foreach (var proto in subprotocols)
                ws.Options.AddSubProtocol(proto);

            await ws.ConnectAsync(new Uri(url), ct).ConfigureAwait(false);
        }

        _ws = ws;
        _recvCts = new CancellationTokenSource();
        _recvTask = Task.Run(() => RecvLoopAsync(_recvCts.Token), _recvCts.Token);
    }

    /// <summary>Close the WebSocket connection and stop all active listen() generators.</summary>
    public async Task DisconnectAsync(CancellationToken ct = default)
    {
        _closed = true;

        if (_recvCts is not null)
        {
            await _recvCts.CancelAsync().ConfigureAwait(false);
            try { await (_recvTask ?? Task.CompletedTask).ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            _recvCts.Dispose();
            _recvCts = null;
            _recvTask = null;
        }

        await CloseWsAsync().ConfigureAwait(false);

        // Drain all channels with a null sentinel so IAsyncEnumerable consumers exit.
        DrainChannels();
    }

    private async Task CloseWsAsync()
    {
        if (_ws is null)
            return;

        try
        {
            if (_ws.State == WebSocketState.Open)
                await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "disconnect", CancellationToken.None)
                          .ConfigureAwait(false);
        }
        catch { /* ignore close errors */ }

        _ws.Dispose();
        _ws = null;
    }

    private void DrainChannels()
    {
        foreach (var chs in _channels.Values)
            foreach (var ch in chs)
                ch.Writer.TryComplete();
    }

    // ------------------------------------------------------------------
    // Send
    // ------------------------------------------------------------------

    private async Task SendAsync(object msg, CancellationToken ct)
    {
        if (_ws is null || _ws.State != WebSocketState.Open)
            throw new BasinApiException("E_UNKNOWN", "realtime socket is not open", 0);

        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(msg, BasinJsonOptions.Default));
        // Use the ArraySegment overload so FakeWebSocket can intercept in tests.
        await _ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, ct)
                 .ConfigureAwait(false);
    }

    // ------------------------------------------------------------------
    // Receive loop
    // ------------------------------------------------------------------

    private async Task RecvLoopAsync(CancellationToken ct)
    {
        var buffer = new ArraySegment<byte>(new byte[32 * 1024]);
        var sb = new StringBuilder();

        try
        {
            while (!ct.IsCancellationRequested && _ws?.State == WebSocketState.Open)
            {
                sb.Clear();
                WebSocketReceiveResult result;

                do
                {
                    // Use the ArraySegment overload so FakeWebSocket can override it in tests.
                    result = await _ws.ReceiveAsync(buffer, ct).ConfigureAwait(false);

                    if (result.MessageType == WebSocketMessageType.Close)
                        return;

                    if (result.MessageType == WebSocketMessageType.Text)
                        sb.Append(Encoding.UTF8.GetString(buffer.Array!, 0, result.Count));

                } while (!result.EndOfMessage);

                var raw = sb.ToString();
                var frame = ParseFrame(raw);
                if (frame is not null)
                    DispatchFrame(frame);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        catch { /* swallow unexpected recv errors */ }
        finally
        {
            if (_ws is not null)
            {
                _ws.Dispose();
                _ws = null;
            }
        }

        // Unexpected disconnect — reconnect unless closed by caller.
        if (!_closed)
            _ = Task.Run(ReconnectAsync, CancellationToken.None);
    }

    private static ServerFrame? ParseFrame(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;

            if (!root.TryGetProperty("type", out var typeProp) ||
                typeProp.ValueKind != JsonValueKind.String)
                return null;

            var type = typeProp.GetString()!;

            return type switch
            {
                "event"          => Wrap(type, ev: JsonSerializer.Deserialize<RealtimeEvent>(raw, BasinJsonOptions.Default)),
                "error"          => Wrap(type, err: JsonSerializer.Deserialize<RealtimeErrorFrame>(raw, BasinJsonOptions.Default)),
                "gap"            => Wrap(type, gap: JsonSerializer.Deserialize<RealtimeGapFrame>(raw, BasinJsonOptions.Default)),
                "subscribed"     => Wrap(type, sub: JsonSerializer.Deserialize<SubscribedFrame>(raw, BasinJsonOptions.Default)),
                "unsubscribed"   => Wrap(type, unsub: JsonSerializer.Deserialize<UnsubscribedFrame>(raw, BasinJsonOptions.Default)),
                "presence_state" => Wrap(type, ps: JsonSerializer.Deserialize<PresenceStateFrame>(raw, BasinJsonOptions.Default)),
                "presence_diff"  => Wrap(type, pd: JsonSerializer.Deserialize<PresenceDiffFrame>(raw, BasinJsonOptions.Default)),
                // "presenceerror" — no underscore, from Rust rename_all="lowercase" on PresenceError
                "presenceerror"  => Wrap(type, pe: JsonSerializer.Deserialize<PresenceErrorFrame>(raw, BasinJsonOptions.Default)),
                _                => null,
            };
        }
        catch
        {
            return null;
        }
    }

    private static ServerFrame Wrap(string type,
        RealtimeEvent? ev = null,
        RealtimeErrorFrame? err = null,
        RealtimeGapFrame? gap = null,
        SubscribedFrame? sub = null,
        UnsubscribedFrame? unsub = null,
        PresenceStateFrame? ps = null,
        PresenceDiffFrame? pd = null,
        PresenceErrorFrame? pe = null) =>
        new()
        {
            Type = type,
            Event = ev,
            Error = err,
            Gap = gap,
            Subscribed = sub,
            Unsubscribed = unsub,
            PresenceState = ps,
            PresenceDiff = pd,
            PresenceError = pe,
        };

    private void DispatchFrame(ServerFrame frame)
    {
        switch (frame.Type)
        {
            case "event":
            {
                var table = frame.Event!.Table;
                RouteToChannels(table, frame);
                break;
            }
            case "error":
            {
                RouteToChannels(frame.Error!.Table, frame);
                break;
            }
            case "gap":
            {
                RouteToChannels(frame.Gap!.Table, frame);
                break;
            }
            case "subscribed":
            {
                var table = frame.Subscribed!.Table;
                if (_pendingSubscribed.TryGetValue(table, out var tcs))
                {
                    _pendingSubscribed.Remove(table);
                    tcs.TrySetResult(true);
                }
                break;
            }
            case "unsubscribed":
            {
                var table = frame.Unsubscribed!.Table;
                if (_pendingUnsubscribed.TryGetValue(table, out var tcs))
                {
                    _pendingUnsubscribed.Remove(table);
                    tcs.TrySetResult(true);
                }
                break;
            }
            case "presence_state":
                RouteToChannels(frame.PresenceState!.Channel, frame);
                break;
            case "presence_diff":
                RouteToChannels(frame.PresenceDiff!.Channel, frame);
                break;
            case "presenceerror":
                RouteToChannels(frame.PresenceError!.Channel, frame);
                break;
        }
    }

    private void RouteToChannels(string key, ServerFrame frame)
    {
        if (!_channels.TryGetValue(key, out var chs))
            return;

        foreach (var ch in chs)
            ch.Writer.TryWrite(frame);
    }

    // ------------------------------------------------------------------
    // Reconnect
    // ------------------------------------------------------------------

    private async Task ReconnectAsync()
    {
        var attempt = 0;
        while (!_closed)
        {
            var delay = Math.Min(BackoffBase * Math.Pow(BackoffFactor, attempt), BackoffMax);
            await Task.Delay(TimeSpan.FromSeconds(delay)).ConfigureAwait(false);
            attempt++;

            try
            {
                await _connectLock.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (_ws?.State == WebSocketState.Open)
                        return; // another path already reconnected

                    await DoConnectAsync(CancellationToken.None).ConfigureAwait(false);
                }
                finally
                {
                    _connectLock.Release();
                }

                // Re-issue all active subscriptions.
                await ResubscribeAsync().ConfigureAwait(false);
                return;
            }
            catch { /* retry */ }
        }
    }

    private async Task ResubscribeAsync()
    {
        foreach (var (table, opts) in _subscriptions.ToList())
        {
            try
            {
                var tcs = new TaskCompletionSource<bool>();
                _pendingSubscribed[table] = tcs;
                await SendSubscribeAsync(table, opts, CancellationToken.None).ConfigureAwait(false);
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await tcs.Task.WaitAsync(cts.Token).ConfigureAwait(false);
            }
            catch { /* log in production */ }
        }
    }

    // ------------------------------------------------------------------
    // Subscribe / unsubscribe
    // ------------------------------------------------------------------

    private Task SendSubscribeAsync(string table, SubscribeOptions opts, CancellationToken ct)
    {
        var msg = new Dictionary<string, object?> { ["type"] = "subscribe", ["table"] = table };
        if (opts.Filter is not null)
            msg["filter"] = opts.Filter;
        if (opts.LastEventId is not null)
            msg["last_event_id"] = opts.LastEventId;

        return SendAsync(msg, ct);
    }

    private async Task SubscribeInternalAsync(string table, SubscribeOptions opts, CancellationToken ct)
    {
        await ConnectAsync(ct).ConfigureAwait(false);
        _subscriptions[table] = opts;

        if (!_channels.ContainsKey(table))
            _channels[table] = new List<System.Threading.Channels.Channel<ServerFrame?>>();

        var tcs = new TaskCompletionSource<bool>();
        _pendingSubscribed[table] = tcs;

        await SendSubscribeAsync(table, opts, ct).ConfigureAwait(false);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        await tcs.Task.WaitAsync(cts.Token).ConfigureAwait(false);
    }

    private async Task UnsubscribeInternalAsync(string table, CancellationToken ct)
    {
        if (_ws?.State != WebSocketState.Open)
            return;

        var tcs = new TaskCompletionSource<bool>();
        _pendingUnsubscribed[table] = tcs;

        await SendAsync(new { type = "unsubscribe", table }, ct).ConfigureAwait(false);

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));
            await tcs.Task.WaitAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { /* timeout — continue */ }

        _subscriptions.Remove(table);
    }

    // ------------------------------------------------------------------
    // Public API — IAsyncEnumerable
    // ------------------------------------------------------------------

    /// <summary>
    /// Subscribe to change events for <paramref name="table"/> and yield
    /// <see cref="ServerFrame"/> values via an async stream.
    ///
    /// The subscription is established on entry and torn down when the
    /// caller cancels <paramref name="ct"/> or disposes the enumerator.
    /// Multiple concurrent Listen() calls for the same table share one
    /// subscription.
    ///
    /// Yields event / error / gap / presence_state / presence_diff / presenceerror frames.
    /// </summary>
    public async IAsyncEnumerable<ServerFrame> ListenAsync(
        string table,
        SubscribeOptions? options = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var opts = options ?? new SubscribeOptions();
        var channel = System.Threading.Channels.Channel.CreateUnbounded<ServerFrame?>(
            new System.Threading.Channels.UnboundedChannelOptions { SingleReader = true });

        if (!_channels.ContainsKey(table))
            _channels[table] = new List<System.Threading.Channels.Channel<ServerFrame?>>();

        _channels[table].Add(channel);

        try
        {
            await SubscribeInternalAsync(table, opts, ct).ConfigureAwait(false);

            await foreach (var frame in channel.Reader.ReadAllAsync(ct))
            {
                if (frame is null)
                    yield break;

                yield return frame;
            }
        }
        finally
        {
            var list = _channels.GetValueOrDefault(table);
            list?.Remove(channel);
            channel.Writer.TryComplete();

            if (list?.Count == 0 && _subscriptions.ContainsKey(table))
                await UnsubscribeInternalAsync(table, CancellationToken.None).ConfigureAwait(false);
        }
    }

    // ------------------------------------------------------------------
    // Presence
    // ------------------------------------------------------------------

    /// <summary>
    /// Send {"type":"presence_track",...} to register in a presence channel.
    /// The server enforces that client_id matches the authenticated JWT identity.
    /// </summary>
    public async Task PresenceTrackAsync(
        string channel,
        string clientId,
        object? metadata = null,
        CancellationToken ct = default)
    {
        await ConnectAsync(ct).ConfigureAwait(false);
        await SendAsync(new
        {
            type = "presence_track",
            channel,
            client_id = clientId,
            metadata = metadata ?? new { },
        }, ct).ConfigureAwait(false);
    }

    /// <summary>Send {"type":"presence_untrack",...}.</summary>
    public async Task PresenceUntrackAsync(
        string channel,
        string clientId,
        CancellationToken ct = default)
    {
        await ConnectAsync(ct).ConfigureAwait(false);
        await SendAsync(new { type = "presence_untrack", channel, client_id = clientId }, ct)
            .ConfigureAwait(false);
    }

    /// <summary>Send {"type":"heartbeat",...} to refresh the presence TTL.</summary>
    public async Task PresenceHeartbeatAsync(
        string channel,
        string clientId,
        CancellationToken ct = default)
    {
        await ConnectAsync(ct).ConfigureAwait(false);
        await SendAsync(new { type = "heartbeat", channel, client_id = clientId }, ct)
            .ConfigureAwait(false);
    }

    // ------------------------------------------------------------------
    // IAsyncDisposable
    // ------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        _connectLock.Dispose();
    }
}
