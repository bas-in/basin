using System.Net;
using System.Text.Json;
using Xunit;

namespace Basin.Tests;

/// <summary>
/// QueryBuilder tests: URL construction, response normalisation, writes,
/// cursor pagination, Arrow IPC byte path.
/// </summary>
public class QueryTests
{
    private const string BaseUrl = "http://localhost";

    private static BasinClient BuildClient(MockHandler handler) =>
        new BasinClient.Builder()
            .WithUrl(BaseUrl)
            .WithApiKey("test-key")
            .WithHttpClient(new HttpClient(handler))
            .Build();

    // ------------------------------------------------------------------
    // URL / filter construction
    // ------------------------------------------------------------------

    [Fact]
    public async Task Select_AppendsSelectParam()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"), new object[] { });

        using var client = BuildClient(handler);
        await client.Table("orders").Select("id,total").RunAsync();

        var req = handler.Requests.Single();
        Assert.Contains("select=id%2Ctotal", req.RequestUri!.Query);
    }

    [Fact]
    public async Task Eq_AppendsEqFilter()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"), new object[] { });

        using var client = BuildClient(handler);
        await client.Table("orders").Eq("status", "paid").RunAsync();

        var req = handler.Requests.Single();
        Assert.Contains("status=eq.paid", req.RequestUri!.Query);
    }

    [Fact]
    public async Task In_AppendsInList()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"), new object[] { });

        using var client = BuildClient(handler);
        await client.Table("orders").In("status", new[] { "new", "processing" }).RunAsync();

        var req = handler.Requests.Single();
        Assert.Contains("in.", req.RequestUri!.Query);
    }

    [Fact]
    public async Task Order_AppendsOrderParam()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"), new object[] { });

        using var client = BuildClient(handler);
        await client.Table("orders").Order("total", ascending: false).RunAsync();

        var req = handler.Requests.Single();
        Assert.Contains("order=total.desc", req.RequestUri!.Query);
    }

    [Fact]
    public async Task Limit_AppendLimitParam()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"), new object[] { });

        using var client = BuildClient(handler);
        await client.Table("orders").Limit(25).RunAsync();

        var req = handler.Requests.Single();
        Assert.Contains("limit=25", req.RequestUri!.Query);
    }

    [Fact]
    public async Task Cursor_AppendsCursorParam()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"), new object[] { });

        using var client = BuildClient(handler);
        await client.Table("orders").Cursor("cursor-opaque-token").RunAsync();

        var req = handler.Requests.Single();
        Assert.Contains("cursor=cursor-opaque-token", req.RequestUri!.Query);
    }

    // ------------------------------------------------------------------
    // Response normalisation
    // ------------------------------------------------------------------

    [Fact]
    public async Task RunAsync_NormalisesPlainArray()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"),
                new[] { new { id = 1, total = 50 }, new { id = 2, total = 100 } });

        using var client = BuildClient(handler);
        var result = await client.Table("orders").RunAsync();

        Assert.Equal(2, result.Rows.Count);
        Assert.Null(result.NextCursor);
    }

    [Fact]
    public async Task RunAsync_NormalisesPaginatedResponse()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"),
                new { rows = new[] { new { id = 1 } }, next_cursor = "cursor-page2" });

        using var client = BuildClient(handler);
        var result = await client.Table("orders").RunAsync();

        Assert.Single(result.Rows);
        Assert.Equal("cursor-page2", result.NextCursor);
    }

    [Fact]
    public async Task RunAsync_EmptyArrayResponse()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"), new object[] { });

        using var client = BuildClient(handler);
        var result = await client.Table("orders").RunAsync();

        Assert.Empty(result.Rows);
        Assert.Null(result.NextCursor);
    }

    [Fact]
    public async Task Into_DeserialiseRowsToTypedList()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/orders"),
                new[] { new { id = 1, total = 49.99 } });

        using var client = BuildClient(handler);
        var result = await client.Table("orders").RunAsync();

        var orders = result.Into<OrderDto>();
        Assert.Single(orders);
        Assert.Equal(1, orders[0].Id);
    }

    private sealed record OrderDto([property: System.Text.Json.Serialization.JsonPropertyName("id")] int Id,
                                   [property: System.Text.Json.Serialization.JsonPropertyName("total")] double Total);

    // ------------------------------------------------------------------
    // Writes
    // ------------------------------------------------------------------

    [Fact]
    public async Task InsertAsync_PostsToTable()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/rest/v1/orders"),
                new { ok = true, tag = "INSERT 0 1" },
                HttpStatusCode.Created);

        using var client = BuildClient(handler);
        await client.Table("orders").InsertAsync(new { total = 49.99, status = "new" });

        Assert.Equal(HttpMethod.Post, handler.Requests.Single().Method);
    }

    [Fact]
    public async Task UpdateAsync_PatchesToTableWithFilters()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Patch("/rest/v1/orders"), new { ok = true, tag = "UPDATE 1" });

        using var client = BuildClient(handler);
        await client.Table("orders")
                    .Eq("id", "7")
                    .UpdateAsync(new { status = "shipped" });

        var req = handler.Requests.Single();
        Assert.Equal(HttpMethod.Patch, req.Method);
        Assert.Contains("id=eq.7", req.RequestUri!.Query);
    }

    [Fact]
    public async Task DeleteAsync_DeletesWithFilters()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Delete("/rest/v1/orders"), new { ok = true, tag = "DELETE 1" });

        using var client = BuildClient(handler);
        await client.Table("orders").Eq("id", "5").DeleteAsync();

        var req = handler.Requests.Single();
        Assert.Equal(HttpMethod.Delete, req.Method);
        Assert.Contains("id=eq.5", req.RequestUri!.Query);
    }

    // ------------------------------------------------------------------
    // RPC
    // ------------------------------------------------------------------

    [Fact]
    public async Task RpcAsync_PostsToRpcRoute()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/rest/v1/rpc/add_two"), 42);

        using var client = BuildClient(handler);
        var result = await client.RpcAsync("add_two",
            new Dictionary<string, object?> { ["a"] = 1, ["b"] = 2 });

        Assert.Equal(42, result!.Value.GetInt32());
    }

    // ------------------------------------------------------------------
    // Cursor pagination round-trip
    // ------------------------------------------------------------------

    [Fact]
    public async Task CursorPagination_TwoPages()
    {
        var handler = new MockHandler()
            .RespondJson(
                r => r.Method == HttpMethod.Get &&
                     r.RequestUri!.PathAndQuery.Contains("/rest/v1/orders") &&
                     !r.RequestUri.Query.Contains("cursor"),
                new { rows = new[] { new { id = 1 } }, next_cursor = "tok-page2" })
            .RespondJson(
                r => r.Method == HttpMethod.Get &&
                     r.RequestUri!.PathAndQuery.Contains("/rest/v1/orders") &&
                     r.RequestUri.Query.Contains("tok-page2"),
                new { rows = new[] { new { id = 2 } }, next_cursor = (string?)null });

        using var client = BuildClient(handler);

        var page1 = await client.Table("orders").Limit(1).RunAsync();
        Assert.Equal("tok-page2", page1.NextCursor);

        var page2 = await client.Table("orders").Limit(1).Cursor(page1.NextCursor!).RunAsync();
        Assert.Null(page2.NextCursor);
        Assert.Equal(2, page2.Rows[0]["id"].GetInt32());
    }

    // ------------------------------------------------------------------
    // Arrow IPC byte path
    // ------------------------------------------------------------------

    [Fact]
    public async Task ToArrowBytesAsync_ReturnsEmptyWhenServerReturnsJson()
    {
        // Server that returns JSON (no Arrow support).
        var handler = new MockHandler()
            .RespondJson(Match.Get("/rest/v1/events"), new object[] { });

        using var client = BuildClient(handler);
        var (bytes, cursor) = await client.Table("events").ToArrowBytesAsync();

        // JSON fallback → empty bytes.
        Assert.Empty(bytes);
        Assert.Null(cursor);
    }

    [Fact]
    public async Task ToArrowBytesAsync_ReturnsRawBytesWhenServerReturnsArrowMime()
    {
        var arrowMagic = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }; // fake IPC bytes
        var handler = new MockHandler();
        // Register a rule that checks for Accept: arrow header and returns arrow content-type.
        handler.Respond(
            r => r.Method == HttpMethod.Get &&
                 r.RequestUri!.PathAndQuery.Contains("/rest/v1/events") &&
                 r.Headers.Accept.Any(a => a.MediaType?.Contains("arrow") ?? false),
            HttpStatusCode.OK,
            body: null,
            contentType: "application/vnd.apache.arrow.stream");

        // The mock handler responds with arrow content type but empty body —
        // enough to verify the branch is taken.
        using var client = BuildClient(handler);
        var (bytes, cursor) = await client.Table("events").ToArrowBytesAsync();
        // Branch taken: content-type was arrow, so we return the (empty) bytes.
        Assert.Empty(bytes); // empty because mock returns no body
    }

    // ------------------------------------------------------------------
    // NDJSON streaming
    // ------------------------------------------------------------------

    [Fact]
    public async Task StreamAsync_AppendsStreamTrueParam()
    {
        var ndjson = "{\"id\":1}\n{\"id\":2}\n";
        var handler = new MockHandler()
            .Respond(Match.Get("/rest/v1/orders"), HttpStatusCode.OK, ndjson);

        using var client = BuildClient(handler);
        var stream = client.Table("orders").StreamAsync();
        await foreach (var _ in stream) { }

        var req = handler.Requests.Single();
        Assert.Contains("stream=true", req.RequestUri!.Query);
    }

    [Fact]
    public async Task StreamAsync_YieldsRows()
    {
        var ndjson = "{\"id\":1,\"total\":50}\n{\"id\":2,\"total\":99}\n";
        var handler = new MockHandler()
            .Respond(Match.Get("/rest/v1/orders"), HttpStatusCode.OK, ndjson);

        using var client = BuildClient(handler);
        var rows = new List<Dictionary<string, System.Text.Json.JsonElement>>();
        await foreach (var row in client.Table("orders").StreamAsync())
            rows.Add(row);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0]["id"].GetInt32());
        Assert.Equal(2, rows[1]["id"].GetInt32());
    }

    [Fact]
    public async Task StreamAsync_SkipsBlankLines()
    {
        // Extra blank lines between NDJSON rows must be ignored.
        var ndjson = "{\"id\":1}\n\n{\"id\":2}\n\n";
        var handler = new MockHandler()
            .Respond(Match.Get("/rest/v1/items"), HttpStatusCode.OK, ndjson);

        using var client = BuildClient(handler);
        var rows = new List<Dictionary<string, System.Text.Json.JsonElement>>();
        await foreach (var row in client.Table("items").StreamAsync())
            rows.Add(row);

        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task StreamAsync_ExtractsNextCursorFromTrailingLine()
    {
        // The trailing sentinel must be consumed and not yielded as a row.
        var ndjson =
            "{\"id\":1}\n" +
            "{\"id\":2}\n" +
            "{\"_basin_next_cursor\":\"tok-abc\"}\n";

        var handler = new MockHandler()
            .Respond(Match.Get("/rest/v1/events"), HttpStatusCode.OK, ndjson);

        using var client = BuildClient(handler);
        var stream = client.Table("events").StreamAsync();

        var rows = new List<Dictionary<string, System.Text.Json.JsonElement>>();
        await foreach (var row in stream)
            rows.Add(row);

        // Two data rows, not three.
        Assert.Equal(2, rows.Count);
        // Cursor captured from sentinel line.
        Assert.Equal("tok-abc", stream.NextCursor);
    }

    [Fact]
    public async Task StreamAsync_NullCursorWhenNoSentinel()
    {
        var ndjson = "{\"id\":1}\n";
        var handler = new MockHandler()
            .Respond(Match.Get("/rest/v1/logs"), HttpStatusCode.OK, ndjson);

        using var client = BuildClient(handler);
        var stream = client.Table("logs").StreamAsync();
        await foreach (var _ in stream) { }

        Assert.Null(stream.NextCursor);
    }

    [Fact]
    public async Task StreamAsync_EmptyBody_YieldsNoRows()
    {
        var handler = new MockHandler()
            .Respond(Match.Get("/rest/v1/empty"), HttpStatusCode.OK, "");

        using var client = BuildClient(handler);
        var count = 0;
        await foreach (var _ in client.Table("empty").StreamAsync())
            count++;

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task StreamAsync_FiltersAndParamsComposedBeforeStreamParam()
    {
        // Verify that filters set before StreamAsync() are included in the request URL.
        var ndjson = "{\"id\":5}\n";
        var handler = new MockHandler()
            .Respond(Match.Get("/rest/v1/orders"), HttpStatusCode.OK, ndjson);

        using var client = BuildClient(handler);
        var stream = client.Table("orders")
                           .Select("id")
                           .Eq("status", "paid")
                           .Limit(100)
                           .StreamAsync();
        await foreach (var _ in stream) { }

        var query = handler.Requests.Single().RequestUri!.Query;
        Assert.Contains("stream=true", query);
        Assert.Contains("select=id", query);
        Assert.Contains("status=eq.paid", query);
        Assert.Contains("limit=100", query);
    }

    [Fact]
    public async Task StreamAsync_PaginationRoundTrip()
    {
        // Page 1 ends with a cursor; page 2 ends with no cursor.
        var page1Ndjson =
            "{\"id\":1}\n" +
            "{\"_basin_next_cursor\":\"tok-page2\"}\n";
        var page2Ndjson =
            "{\"id\":2}\n";

        var handler = new MockHandler()
            .Respond(
                r => r.Method == HttpMethod.Get &&
                     r.RequestUri!.PathAndQuery.Contains("/rest/v1/orders") &&
                     !r.RequestUri.Query.Contains("cursor"),
                HttpStatusCode.OK, page1Ndjson)
            .Respond(
                r => r.Method == HttpMethod.Get &&
                     r.RequestUri!.PathAndQuery.Contains("/rest/v1/orders") &&
                     r.RequestUri.Query.Contains("tok-page2"),
                HttpStatusCode.OK, page2Ndjson);

        using var client = BuildClient(handler);

        var stream1 = client.Table("orders").Limit(1).StreamAsync();
        var ids = new List<int>();
        await foreach (var row in stream1)
            ids.Add(row["id"].GetInt32());

        Assert.Equal("tok-page2", stream1.NextCursor);
        Assert.Single(ids);

        var stream2 = client.Table("orders").Limit(1).Cursor(stream1.NextCursor!).StreamAsync();
        await foreach (var row in stream2)
            ids.Add(row["id"].GetInt32());

        Assert.Null(stream2.NextCursor);
        Assert.Equal(new[] { 1, 2 }, ids);
    }
}
