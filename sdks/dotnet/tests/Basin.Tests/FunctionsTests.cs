using System.Net;
using System.Text.Json;
using Xunit;

namespace Basin.Tests;

/// <summary>
/// FunctionsClient tests: /fn/v1/:name (invoke) and /rest/v1/rpc/:fn_name.
/// </summary>
public class FunctionsTests
{
    private const string BaseUrl = "http://localhost";

    private static BasinClient BuildClient(MockHandler handler) =>
        new BasinClient.Builder()
            .WithUrl(BaseUrl)
            .WithApiKey("test-key")
            .WithHttpClient(new HttpClient(handler))
            .Build();

    // ------------------------------------------------------------------
    // RPC
    // ------------------------------------------------------------------

    [Fact]
    public async Task RpcAsync_PostsToRpcRoute()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/rest/v1/rpc/add_two"), 99);

        using var client = BuildClient(handler);
        var result = await client.Functions.RpcAsync("add_two",
            new Dictionary<string, object?> { ["a"] = 1, ["b"] = 2 });

        Assert.Equal(99, result!.Value.GetInt32());

        var req = handler.Requests.Single();
        Assert.Contains("/rest/v1/rpc/add_two", req.RequestUri!.AbsolutePath);
        Assert.Equal(HttpMethod.Post, req.Method);
    }

    [Fact]
    public async Task RpcAsync_HandlesNullArgs()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/rest/v1/rpc/hello"), "world");

        using var client = BuildClient(handler);
        var result = await client.Functions.RpcAsync("hello");
        Assert.NotNull(result);
    }

    // ------------------------------------------------------------------
    // Invoke (/fn/v1/:name)
    // ------------------------------------------------------------------

    [Fact]
    public async Task InvokeAsync_PostsToFnRoute()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Any("/fn/v1/my-handler"),
                new { result = "ok" });

        using var client = BuildClient(handler);
        var result = await client.Functions.InvokeAsync("my-handler",
            body: new { key = "value" });

        Assert.Equal(200, result.Status);
        Assert.NotNull(result.Data);
    }

    [Fact]
    public async Task InvokeAsync_ThrowsBasinApiException_OnBasinErrorEnvelope()
    {
        var handler = new MockHandler()
            .Respond(Match.Any("/fn/v1/broken"), HttpStatusCode.NotFound,
                """{"code":"E_NOT_FOUND","message":"function not found"}""");

        using var client = BuildClient(handler);
        var ex = await Assert.ThrowsAsync<BasinApiException>(() =>
            client.Functions.InvokeAsync("broken"));

        Assert.Equal("E_NOT_FOUND", ex.Code);
    }

    [Fact]
    public async Task InvokeAsync_DoesNotThrow_OnNon2xxNonBasinError()
    {
        // A non-2xx response whose body is NOT a Basin error envelope should be
        // proxied through without throwing.
        var handler = new MockHandler()
            .Respond(Match.Any("/fn/v1/proxy-handler"), HttpStatusCode.BadRequest,
                """{"custom_error":"bad input"}""");

        using var client = BuildClient(handler);
        var result = await client.Functions.InvokeAsync("proxy-handler");

        // Should not throw; status and data are returned verbatim.
        Assert.Equal(400, result.Status);
    }

    [Fact]
    public async Task InvokeAsync_AcceptsGetMethod()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Any("/fn/v1/metrics"), new { count = 42 });

        using var client = BuildClient(handler);
        var result = await client.Functions.InvokeAsync("metrics", method: "GET");

        Assert.Equal(200, result.Status);
        var req = handler.Requests.Single();
        Assert.Equal(HttpMethod.Get, req.Method);
    }
}
