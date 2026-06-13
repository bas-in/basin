using System.Text.Json;

namespace Basin;

/// <summary>
/// Top-level Basin SDK client.
///
/// Thread-safe; safe for concurrent use from multiple Tasks.
///
/// <code>
/// using var client = new BasinClient.Builder()
///     .WithUrl("https://your-project.basin.run")
///     .WithApiKey("your-api-key")
///     .WithProjectId("01JWXXX...")
///     .Build();
///
/// var result = await client.Table("orders")
///     .Select("id,total")
///     .Gte("total", "100")
///     .RunAsync();
///
/// foreach (var row in result.Rows)
///     Console.WriteLine(row["id"]);
/// </code>
/// </summary>
public sealed class BasinClient : IAsyncDisposable, IDisposable
{
    private readonly BasinTransport _transport;
    private readonly string? _explicitProjectId;
    private readonly string? _staticKey;

    // Lazy realtime client.
    private RealtimeClient? _realtime;
    private readonly object _realtimeLock = new();

    /// <summary>Auth client for /auth/v1/* routes.</summary>
    public AuthClient Auth { get; }

    /// <summary>Storage client for /storage/v1/* routes.</summary>
    public StorageClient Storage { get; }

    /// <summary>Functions client for /fn/v1/* and /rest/v1/rpc/* routes.</summary>
    public FunctionsClient Functions { get; }

    private BasinClient(BasinOptions opts)
    {
        _staticKey = opts.ApiKey;
        _explicitProjectId = opts.ProjectId;

        _transport = new BasinTransport(
            opts.Url ?? throw new ArgumentNullException(nameof(opts.Url), "Url must be set"),
            opts.ApiKey,
            opts.HttpClient,
            opts.Timeout);

        Auth = new AuthClient(_transport, opts.ProjectId);
        Functions = new FunctionsClient(_transport);
        Storage = new StorageClient(_transport, ResolveProjectId);

        // Wire the transport's token getter to the auth client so every
        // request uses the freshest session token.
        _transport.SetTokenGetter(Auth.AccessTokenAsync);
    }

    // ------------------------------------------------------------------
    // Project ID resolution
    // ------------------------------------------------------------------

    private string? ResolveProjectId()
    {
        if (!string.IsNullOrEmpty(_explicitProjectId))
            return _explicitProjectId;

        var session = Auth.GetSession();
        if (session is not null)
        {
            var fromSession = AuthClient.ProjectIdFromJwt(session.AccessToken);
            if (!string.IsNullOrEmpty(fromSession))
                return fromSession;
        }

        if (_staticKey is not null)
        {
            var fromKey = AuthClient.ProjectIdFromJwt(_staticKey);
            if (!string.IsNullOrEmpty(fromKey))
                return fromKey;
        }

        return null;
    }

    // ------------------------------------------------------------------
    // Query API
    // ------------------------------------------------------------------

    /// <summary>Return a <see cref="QueryBuilder"/> for /rest/v1/:table.</summary>
    public QueryBuilder Table(string name) => new(_transport, name);

    /// <summary>Alias for <see cref="Table(string)"/>, matching JS SDK style.</summary>
    public QueryBuilder From(string name) => Table(name);

    // ------------------------------------------------------------------
    // RPC convenience
    // ------------------------------------------------------------------

    /// <summary>
    /// POST /rest/v1/rpc/:fn_name — convenience alias for Functions.RpcAsync().
    /// </summary>
    public Task<JsonElement?> RpcAsync(
        string fnName,
        IDictionary<string, object?>? args = null,
        CancellationToken ct = default) =>
        Functions.RpcAsync(fnName, args, ct);

    // ------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------

    /// <summary>GET /health → "ok".</summary>
    public async Task<string> HealthAsync(CancellationToken ct = default)
    {
        var resp = await _transport.RequestAsync(
            HttpMethod.Get, "/health", auth: false, ct: ct).ConfigureAwait(false);

        return await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------
    // Realtime (lazy)
    // ------------------------------------------------------------------

    /// <summary>
    /// Lazy <see cref="RealtimeClient"/> for GET /realtime/v1/ws/:project.
    /// The WebSocket connection is not opened until the first ListenAsync() or
    /// ConnectAsync() call.
    /// </summary>
    public RealtimeClient Realtime
    {
        get
        {
            if (_realtime is null)
            {
                lock (_realtimeLock)
                {
                    _realtime ??= new RealtimeClient(
                        _transport.BaseUrl,
                        Auth.AccessTokenAsync,
                        ResolveProjectId);
                }
            }

            return _realtime;
        }
    }

    // ------------------------------------------------------------------
    // Escape hatch
    // ------------------------------------------------------------------

    /// <summary>
    /// Execute an arbitrary request against the Basin API.
    /// Use for routes not yet wrapped by the SDK (e.g. /admin/v1/*).
    /// </summary>
    public Task<JsonElement?> RequestAsync(
        string method,
        string path,
        object? body = null,
        CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            new HttpMethod(method.ToUpperInvariant()),
            path,
            body: body,
            ct: ct);

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        if (_realtime is not null)
            await _realtime.DisposeAsync().ConfigureAwait(false);

        _transport.Dispose();
    }

    public void Dispose()
    {
        if (_realtime is not null)
        {
            // Fire-and-forget dispose (sync Dispose on async type).
            try { _realtime.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
            catch { /* swallow on sync dispose */ }
        }

        _transport.Dispose();
    }

    // ------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------

    /// <summary>
    /// Fluent builder for <see cref="BasinClient"/>.
    ///
    /// <code>
    /// using var client = new BasinClient.Builder()
    ///     .WithUrl("https://your-project.basin.run")
    ///     .WithApiKey("your-api-key")
    ///     .Build();
    /// </code>
    /// </summary>
    public sealed class Builder
    {
        private readonly BasinOptions _opts = new();

        /// <summary>Basin server base URL, e.g. "https://your-project.basin.run".</summary>
        public Builder WithUrl(string url) { _opts.Url = url; return this; }

        /// <summary>
        /// JWT or raw API key — sent as Authorization: Bearer &lt;key&gt;.
        /// Pass null to authenticate later via Auth.SignInAsync().
        /// </summary>
        public Builder WithApiKey(string? key) { _opts.ApiKey = key; return this; }

        /// <summary>
        /// Default project ULID. Optional when the API key is a JWT that carries
        /// a project_id claim. Required for public storage URLs and realtime.
        /// </summary>
        public Builder WithProjectId(string? id) { _opts.ProjectId = id; return this; }

        /// <summary>Default per-request timeout. Default: 30 seconds.</summary>
        public Builder WithTimeout(TimeSpan timeout) { _opts.Timeout = timeout; return this; }

        /// <summary>Custom HttpClient for testing or advanced transport config.</summary>
        public Builder WithHttpClient(HttpClient client) { _opts.HttpClient = client; return this; }

        /// <summary>Build the <see cref="BasinClient"/>.</summary>
        public BasinClient Build()
        {
            if (string.IsNullOrEmpty(_opts.Url))
                throw new InvalidOperationException("BasinClient.Builder: WithUrl() must be called");

            return new BasinClient(_opts);
        }
    }

    private sealed class BasinOptions
    {
        public string? Url { get; set; }
        public string? ApiKey { get; set; }
        public string? ProjectId { get; set; }
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
        public HttpClient? HttpClient { get; set; }
    }
}
