using System.Net;
using Xunit;

namespace Basin.Tests;

/// <summary>
/// Verifies error-envelope decoding and the exception hierarchy.
/// </summary>
public class ErrorTests
{
    [Fact]
    public void FromResponseBody_ParsesCodeAndMessage()
    {
        var ex = BasinApiException.FromResponseBody(
            """{"code":"E_NOT_FOUND","message":"table not found"}""", 404);

        Assert.Equal("E_NOT_FOUND", ex.Code);
        Assert.Equal("table not found", ex.Message);
        Assert.Equal(404, ex.Status);
    }

    [Fact]
    public void FromResponseBody_HandlesEmptyBody()
    {
        var ex = BasinApiException.FromResponseBody("", 500);
        Assert.Equal("E_UNKNOWN", ex.Code);
        Assert.Equal(500, ex.Status);
    }

    [Fact]
    public void FromResponseBody_HandlesMalformedJson()
    {
        var ex = BasinApiException.FromResponseBody("not json", 400);
        Assert.Equal("E_UNKNOWN", ex.Code);
        // message falls back to the raw text
        Assert.Equal("not json", ex.Message);
    }

    [Fact]
    public void FromResponseBody_ParsesSqlState()
    {
        var ex = BasinApiException.FromResponseBody(
            """{"code":"E_INTERNAL","message":"unique violation","sqlstate":"23505"}""", 409);

        Assert.Equal("23505", ex.SqlState);
    }

    [Fact]
    public async Task Client_ThrowsBasinApiException_OnNon2xxResponse()
    {
        var handler = new MockHandler()
            .RespondError(Match.Get("/rest/v1/orders"), "E_NOT_FOUND", "table not found", HttpStatusCode.NotFound);

        using var client = new BasinClient.Builder()
            .WithUrl("http://localhost")
            .WithApiKey("test-key")
            .WithHttpClient(new HttpClient(handler))
            .Build();

        var ex = await Assert.ThrowsAsync<BasinApiException>(() =>
            client.Table("orders").RunAsync());

        Assert.Equal("E_NOT_FOUND", ex.Code);
        Assert.Equal(404, ex.Status);
    }

    [Fact]
    public void BasinException_Hierarchy()
    {
        BasinException e1 = new BasinApiException("E_INTERNAL", "oops", 500);
        BasinException e2 = new BasinNetworkException("connection refused");

        Assert.IsAssignableFrom<BasinException>(e1);
        Assert.IsAssignableFrom<BasinException>(e2);
        Assert.IsAssignableFrom<Exception>(e1);
    }
}
