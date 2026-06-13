using System.Net;
using System.Text;
using System.Text.Json;

namespace Basin.Tests;

/// <summary>
/// Mock HttpMessageHandler for injecting canned HTTP responses in tests,
/// without any network dependency.
/// </summary>
internal sealed class MockHandler : HttpMessageHandler
{
    private readonly List<(Func<HttpRequestMessage, bool> Match, HttpResponseMessage Response)> _rules = new();
    private readonly List<HttpRequestMessage> _requests = new();

    public IReadOnlyList<HttpRequestMessage> Requests => _requests;

    public MockHandler Respond(
        Func<HttpRequestMessage, bool> match,
        HttpStatusCode status,
        string? body = null,
        string contentType = "application/json")
    {
        var resp = new HttpResponseMessage(status);
        if (body is not null)
            resp.Content = new StringContent(body, Encoding.UTF8, contentType);

        _rules.Add((match, resp));
        return this;
    }

    public MockHandler RespondJson(
        Func<HttpRequestMessage, bool> match,
        object payload,
        HttpStatusCode status = HttpStatusCode.OK)
    {
        var body = JsonSerializer.Serialize(payload, BasinJsonOptions.Default);
        return Respond(match, status, body);
    }

    public MockHandler RespondError(
        Func<HttpRequestMessage, bool> match,
        string code,
        string message,
        HttpStatusCode status)
    {
        var body = JsonSerializer.Serialize(new { code, message });
        return Respond(match, status, body);
    }

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        _requests.Add(request);

        foreach (var (match, resp) in _rules)
        {
            if (match(request))
            {
                // Clone so the response can be read multiple times in tests.
                var clone = new HttpResponseMessage(resp.StatusCode);
                if (resp.Content is not null)
                {
                    var bodyText = resp.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                    var ct = resp.Content.Headers.ContentType?.ToString() ?? "application/json";
                    clone.Content = new StringContent(bodyText, Encoding.UTF8,
                        ct.Split(';')[0].Trim());
                }
                return Task.FromResult(clone);
            }
        }

        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound)
        {
            Content = new StringContent(
                JsonSerializer.Serialize(new { code = "E_NOT_FOUND", message = "no mock rule matched" }),
                Encoding.UTF8, "application/json")
        });
    }
}

/// <summary>
/// Predicates used in MockHandler.Respond().
/// </summary>
internal static class Match
{
    public static Func<HttpRequestMessage, bool> Method(HttpMethod m, string pathContains) =>
        r => r.Method == m && (r.RequestUri?.PathAndQuery.Contains(pathContains) ?? false);

    public static Func<HttpRequestMessage, bool> Post(string path) => Method(HttpMethod.Post, path);
    public static Func<HttpRequestMessage, bool> Get(string path) => Method(HttpMethod.Get, path);
    public static Func<HttpRequestMessage, bool> Delete(string path) => Method(HttpMethod.Delete, path);
    public static Func<HttpRequestMessage, bool> Patch(string path) => Method(HttpMethod.Patch, path);

    public static Func<HttpRequestMessage, bool> Any(string pathContains) =>
        r => r.RequestUri?.PathAndQuery.Contains(pathContains) ?? false;
}
