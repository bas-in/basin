using System.Text;
using System.Text.Json;

namespace Basin;

/// <summary>
/// Functions client — two function-invocation surfaces.
///
/// Route sources (verified against crates/basin-rest/src/server.rs):
/// - ANY  /fn/v1/:name          HTTP-handler functions (any_fn_handler)
/// - POST /rest/v1/rpc/:fn_name catalog SQL / Wasm UDFs (post_rpc)
/// </summary>
public sealed class FunctionsClient
{
    private readonly BasinTransport _transport;

    internal FunctionsClient(BasinTransport transport) => _transport = transport;

    /// <summary>
    /// ANY /fn/v1/:name — invoke an HTTP-handler function.
    ///
    /// The function's response is proxied verbatim. Non-2xx statuses are NOT
    /// automatically errors unless the body is Basin's own error envelope
    /// (auth 401, unknown function 404, invocation failure 500).
    /// </summary>
    public async Task<FunctionInvokeResult> InvokeAsync(
        string name,
        object? body = null,
        string method = "POST",
        IDictionary<string, string>? headers = null,
        CancellationToken ct = default)
    {
        var hdrs = new Dictionary<string, string>(
            headers ?? new Dictionary<string, string>(),
            StringComparer.OrdinalIgnoreCase);

        // Inject bearer token manually (we bypass RequestAsync so we can
        // proxy non-2xx statuses from the function's own handler).
        var bearer = await _transport.GetBearerTokenAsync(ct).ConfigureAwait(false);
        if (bearer is not null)
            hdrs["Authorization"] = $"Bearer {bearer}";

        byte[]? bodyBytes = null;
        if (body is byte[] rawBytes)
        {
            bodyBytes = rawBytes;
            hdrs.TryAdd("Content-Type", "application/octet-stream");
        }
        else if (body is string str)
        {
            bodyBytes = Encoding.UTF8.GetBytes(str);
            hdrs.TryAdd("Content-Type", "text/plain");
        }
        else if (body is not null)
        {
            bodyBytes = JsonSerializer.SerializeToUtf8Bytes(body, BasinJsonOptions.Default);
            hdrs.TryAdd("Content-Type", "application/json");
        }

        var url = _transport.BuildUrl($"/fn/v1/{Uri.EscapeDataString(name)}", null);
        using var req = new HttpRequestMessage(new HttpMethod(method.ToUpperInvariant()), url);

        foreach (var (k, v) in hdrs)
        {
            if (k.Equals("Content-Type", StringComparison.OrdinalIgnoreCase))
                continue; // set via Content
            req.Headers.TryAddWithoutValidation(k, v);
        }

        if (bodyBytes is not null)
        {
            req.Content = new ByteArrayContent(bodyBytes);
            if (hdrs.TryGetValue("Content-Type", out var ct2))
                req.Content.Headers.ContentType =
                    System.Net.Http.Headers.MediaTypeHeaderValue.Parse(ct2);
        }

        HttpResponseMessage resp;
        try
        {
            resp = await _transport.GetRawHttpClient()
                                   .SendAsync(req, HttpCompletionOption.ResponseContentRead, ct)
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

        var respContentType = resp.Content.Headers.ContentType?.MediaType ?? "";
        var respText = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

        object? data;
        if (respContentType.Contains("json") && !string.IsNullOrEmpty(respText))
        {
            try
            {
                data = JsonDocument.Parse(respText).RootElement.Clone();
            }
            catch
            {
                data = respText;
            }
        }
        else
        {
            data = respText;
        }

        // Basin-layer failures (not the function's own responses) use the
        // standard error envelope; surface those as typed exceptions.
        if (!resp.IsSuccessStatusCode && data is JsonElement je && IsErrorEnvelope(je))
        {
            var code = je.TryGetProperty("code", out var c) ? c.GetString() ?? "E_UNKNOWN" : "E_UNKNOWN";
            var msg = je.TryGetProperty("message", out var m) ? m.GetString() ?? "" : $"HTTP {(int)resp.StatusCode}";
            throw new BasinApiException(code, msg, (int)resp.StatusCode);
        }

        var respHeaders = new Dictionary<string, string>();
        foreach (var h in resp.Headers)
            respHeaders[h.Key] = string.Join(", ", h.Value);
        foreach (var h in resp.Content.Headers)
            respHeaders[h.Key] = string.Join(", ", h.Value);

        return new FunctionInvokeResult
        {
            Status = (int)resp.StatusCode,
            Headers = respHeaders,
            Data = data,
        };
    }

    private static bool IsErrorEnvelope(JsonElement je)
    {
        if (je.ValueKind != JsonValueKind.Object)
            return false;

        if (!je.TryGetProperty("code", out var c) || c.ValueKind != JsonValueKind.String)
            return false;

        var code = c.GetString() ?? "";
        return code.StartsWith("E_", StringComparison.Ordinal) && je.TryGetProperty("message", out _);
    }

    /// <summary>
    /// POST /rest/v1/rpc/:fn_name — call a catalog SQL function or Wasm UDF.
    /// Args are named; missing declared args become NULL.
    /// Returns the bare scalar for single-value results, or an array of rows.
    /// </summary>
    public Task<JsonElement?> RpcAsync(
        string fnName,
        IDictionary<string, object?>? args = null,
        CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Post,
            $"/rest/v1/rpc/{Uri.EscapeDataString(fnName)}",
            body: (object?)args ?? new { },
            ct: ct);
}
