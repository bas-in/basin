using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Basin;

/// <summary>
/// Internal HTTP transport — Bearer-token injection, error-envelope decoding,
/// and JSON (de)serialisation. Shared by all sub-clients.
/// </summary>
internal sealed class BasinTransport : IDisposable
{
    internal readonly string BaseUrl;
    private readonly HttpClient _http;
    private readonly bool _ownsClient;

    // The static API key or JWT set at construction.
    internal readonly string? StaticKey;

    // Wired after construction by BasinClient so every request uses the
    // freshest session token (auto-refreshed by AuthClient).
    private Func<CancellationToken, Task<string?>>? _tokenGetter;

    internal BasinTransport(string baseUrl, string? key, HttpClient? httpClient, TimeSpan timeout)
    {
        BaseUrl = baseUrl.TrimEnd('/');
        StaticKey = key;

        if (httpClient is not null)
        {
            _http = httpClient;
            _ownsClient = false;
        }
        else
        {
            _http = new HttpClient { Timeout = timeout };
            _ownsClient = true;
        }
    }

    internal void SetTokenGetter(Func<CancellationToken, Task<string?>> getter) =>
        _tokenGetter = getter;

    // ------------------------------------------------------------------
    // Bearer resolution
    // ------------------------------------------------------------------

    private async Task<string?> BearerAsync(CancellationToken ct)
    {
        if (_tokenGetter is not null)
        {
            var tok = await _tokenGetter(ct).ConfigureAwait(false);
            if (tok is not null)
                return tok;
        }
        return StaticKey;
    }

    // ------------------------------------------------------------------
    // Core request
    // ------------------------------------------------------------------

    /// <summary>
    /// Execute an HTTP request. Throws <see cref="BasinApiException"/> for
    /// non-2xx responses and <see cref="BasinNetworkException"/> on transport
    /// failures.
    /// </summary>
    internal async Task<HttpResponseMessage> RequestAsync(
        HttpMethod method,
        string path,
        IEnumerable<(string, string)>? query = null,
        byte[]? body = null,
        string? contentType = null,
        IDictionary<string, string>? extraHeaders = null,
        bool auth = true,
        CancellationToken ct = default)
    {
        var url = BuildUrl(path, query);
        var req = new HttpRequestMessage(method, url);

        if (auth)
        {
            var bearer = await BearerAsync(ct).ConfigureAwait(false);
            if (bearer is not null)
                req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearer);
        }

        if (body is not null)
        {
            req.Content = new ByteArrayContent(body);
            req.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType ?? "application/json");
        }

        if (extraHeaders is not null)
        {
            foreach (var (k, v) in extraHeaders)
                req.Headers.TryAddWithoutValidation(k, v);
        }

        HttpResponseMessage resp;
        try
        {
            resp = await _http.SendAsync(req, HttpCompletionOption.ResponseContentRead, ct)
                              .ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            throw new BasinNetworkException(ex.Message, ex);
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            throw new BasinNetworkException("Request timed out", ex);
        }

        if (!resp.IsSuccessStatusCode)
        {
            var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw BasinApiException.FromResponseBody(raw, (int)resp.StatusCode);
        }

        return resp;
    }

    /// <summary>Execute and deserialise the JSON response body.</summary>
    internal async Task<T?> RequestJsonAsync<T>(
        HttpMethod method,
        string path,
        IEnumerable<(string, string)>? query = null,
        object? body = null,
        IDictionary<string, string>? extraHeaders = null,
        bool auth = true,
        CancellationToken ct = default)
    {
        byte[]? bodyBytes = null;
        if (body is not null)
            bodyBytes = JsonSerializer.SerializeToUtf8Bytes(body, BasinJsonOptions.Default);

        var resp = await RequestAsync(method, path, query, bodyBytes, "application/json",
                                      extraHeaders, auth, ct).ConfigureAwait(false);

        if (resp.StatusCode == System.Net.HttpStatusCode.NoContent)
            return default;

        var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (string.IsNullOrEmpty(raw))
            return default;

        return JsonSerializer.Deserialize<T>(raw, BasinJsonOptions.Default);
    }

    /// <summary>Execute and return the raw response bytes (for storage downloads).</summary>
    internal async Task<(byte[] Data, string? ContentType, string? ETag)> RequestBytesAsync(
        HttpMethod method,
        string path,
        IDictionary<string, string>? extraHeaders = null,
        bool auth = true,
        CancellationToken ct = default)
    {
        var resp = await RequestAsync(method, path, null, null, null, extraHeaders, auth, ct)
                        .ConfigureAwait(false);

        var data = await resp.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
        resp.Content.Headers.TryGetValues("Content-Type", out var ctValues);
        resp.Headers.TryGetValues("ETag", out var etagValues);

        return (data,
                ctValues?.FirstOrDefault(),
                etagValues?.FirstOrDefault());
    }

    // ------------------------------------------------------------------
    // URL construction
    // ------------------------------------------------------------------

    internal string BuildUrl(string path, IEnumerable<(string, string)>? query)
    {
        var sb = new StringBuilder(BaseUrl);
        sb.Append(path);
        if (query is not null)
        {
            var first = true;
            foreach (var (k, v) in query)
            {
                sb.Append(first ? '?' : '&');
                sb.Append(Uri.EscapeDataString(k));
                sb.Append('=');
                sb.Append(Uri.EscapeDataString(v));
                first = false;
            }
        }
        return sb.ToString();
    }

    // ------------------------------------------------------------------
    // Helpers for FunctionsClient (raw access needed for /fn/v1/:name)
    // ------------------------------------------------------------------

    internal Task<string?> GetBearerTokenAsync(CancellationToken ct) => BearerAsync(ct);

    internal HttpClient GetRawHttpClient() => _http;

    // ------------------------------------------------------------------
    // IDisposable
    // ------------------------------------------------------------------

    public void Dispose()
    {
        if (_ownsClient)
            _http.Dispose();
    }
}
