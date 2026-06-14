using System.Text.Json;

namespace Basin;

/// <summary>
/// Fluent query builder for /rest/v1/:table.
///
/// Route source (verified): crates/basin-rest/src/server.rs
/// GET|POST|PATCH|DELETE /rest/v1/:table.
///
/// Filter grammar (Basin's PostgREST-style dialect, parser.rs):
/// - select=cols,csv
/// - col=op.value with ops: eq|neq|gt|gte|lt|lte|in|is
///   (in.(a,b,c) parenthesised; is.null / is.notnull)
/// - order=col[.asc|.desc], limit=N, offset=N
/// - cursor=token (keyset pagination)
/// - stream=true → NDJSON, one row per line; trailing {"_basin_next_cursor":"..."} when paginating
///
/// Response shapes (crates/basin-rest/src/routes/data.rs):
/// - plain GET → JSON array of rows
/// - GET with limit or cursor → { rows, next_cursor }
/// - GET with stream=true → NDJSON (see StreamAsync)
/// - POST → 201, { ok, tag } or rows; PATCH/DELETE → { ok, tag }
/// - DELETE may surface 501 E_ENGINE_UNSUPPORTED
/// </summary>
public sealed class QueryBuilder
{
    private readonly BasinTransport _transport;
    private readonly string _table;
    private readonly List<(string Key, string Value)> _query = new();

    internal QueryBuilder(BasinTransport transport, string table)
    {
        _transport = transport;
        _table = table;
    }

    // ------------------------------------------------------------------
    // Projection / filter / ordering (all return this for chaining)
    // ------------------------------------------------------------------

    /// <summary>select=cols — comma-separated column list; null or "*" for all columns.</summary>
    public QueryBuilder Select(string? columns = null)
    {
        _query.Add(("select", string.IsNullOrEmpty(columns) ? "*" : columns));
        return this;
    }

    /// <summary>select=col1,col2,... from an array of column names.</summary>
    public QueryBuilder Select(params string[] columns)
    {
        _query.Add(("select", string.Join(",", columns)));
        return this;
    }

    public QueryBuilder Eq(string column, string value)   => Filter(column, "eq", value);
    public QueryBuilder Neq(string column, string value)  => Filter(column, "neq", value);
    public QueryBuilder Gt(string column, string value)   => Filter(column, "gt", value);
    public QueryBuilder Gte(string column, string value)  => Filter(column, "gte", value);
    public QueryBuilder Lt(string column, string value)   => Filter(column, "lt", value);
    public QueryBuilder Lte(string column, string value)  => Filter(column, "lte", value);

    /// <summary>col=in.(a,b,c) — parenthesised list per parser.rs parse_in_list.</summary>
    public QueryBuilder In(string column, IEnumerable<string> values)
    {
        _query.Add((column, $"in.({string.Join(",", values)})"));
        return this;
    }

    /// <summary>col=is.null / col=is.notnull</summary>
    public QueryBuilder Is(string column, string value)
    {
        _query.Add((column, $"is.{value}"));
        return this;
    }

    /// <summary>order=col.asc|desc (repeatable for multi-column sort).</summary>
    public QueryBuilder Order(string column, bool ascending = true)
    {
        _query.Add(("order", $"{column}.{(ascending ? "asc" : "desc")}"));
        return this;
    }

    /// <summary>limit=N. Switches the GET response to { rows, next_cursor }.</summary>
    public QueryBuilder Limit(int n)
    {
        _query.Add(("limit", n.ToString()));
        return this;
    }

    public QueryBuilder Offset(int n)
    {
        _query.Add(("offset", n.ToString()));
        return this;
    }

    /// <summary>Resume keyset pagination from a next_cursor token.</summary>
    public QueryBuilder Cursor(string token)
    {
        _query.Add(("cursor", token));
        return this;
    }

    private QueryBuilder Filter(string column, string op, string value)
    {
        _query.Add((column, $"{op}.{value}"));
        return this;
    }

    // ------------------------------------------------------------------
    // Execution — read
    // ------------------------------------------------------------------

    /// <summary>
    /// Execute as GET /rest/v1/:table and normalise both response shapes
    /// (plain array and { rows, next_cursor }).
    /// </summary>
    public async Task<QueryResult> RunAsync(CancellationToken ct = default)
    {
        var resp = await _transport.RequestAsync(
            HttpMethod.Get,
            $"/rest/v1/{Uri.EscapeDataString(_table)}",
            query: _query,
            ct: ct).ConfigureAwait(false);

        var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        return NormalizeGet(raw);
    }

    /// <summary>Execute as GET and return rows only.</summary>
    public async Task<IReadOnlyList<Dictionary<string, JsonElement>>> RowsAsync(
        CancellationToken ct = default)
    {
        var r = await RunAsync(ct).ConfigureAwait(false);
        return r.Rows;
    }

    /// <summary>
    /// Execute as GET with <c>?stream=true</c> (NDJSON) and stream rows as they
    /// arrive without buffering the full response body.
    ///
    /// The server sends one JSON object per line. A trailing line of the form
    /// <c>{"_basin_next_cursor":"…"}</c> signals keyset pagination; it is consumed
    /// internally and not yielded as a row. Retrieve it from
    /// <see cref="StreamResult.NextCursor"/> after the <c>await foreach</c> loop
    /// completes.
    ///
    /// <code>
    /// var stream = client.Table("events").Limit(5000).StreamAsync();
    /// await foreach (var row in stream)
    ///     Console.WriteLine(row["id"]);
    ///
    /// // Cursor is populated once the loop finishes (or is broken out of early).
    /// var cursor = stream.NextCursor;
    /// </code>
    ///
    /// Combine with active filters and ordering just like <see cref="RunAsync"/>:
    /// <code>
    /// await foreach (var row in client.Table("orders").Eq("status", "paid").StreamAsync())
    ///     Process(row);
    /// </code>
    /// </summary>
    public StreamResult StreamAsync(CancellationToken ct = default)
    {
        var query = new List<(string, string)>(_query) { ("stream", "true") };
        return new StreamResult(_transport, _table, query, ct);
    }

    /// <summary>
    /// Execute as GET with Accept: application/vnd.apache.arrow.stream.
    /// Returns raw Arrow IPC bytes when the server responds with Arrow IPC;
    /// callers who have Apache.Arrow installed can decode these directly.
    ///
    /// The response header x-basin-next-cursor is returned as the second
    /// element of the tuple when present.
    ///
    /// Falls back to an empty byte array when the server returns JSON — callers
    /// should check the Content-Type header or use RunAsync() in that case.
    /// </summary>
    public async Task<(byte[] ArrowIpcBytes, string? NextCursor)> ToArrowBytesAsync(
        CancellationToken ct = default)
    {
        const string arrowMime = "application/vnd.apache.arrow.stream";

        var resp = await _transport.RequestAsync(
            HttpMethod.Get,
            $"/rest/v1/{Uri.EscapeDataString(_table)}",
            query: _query,
            extraHeaders: new Dictionary<string, string> { ["Accept"] = arrowMime },
            ct: ct).ConfigureAwait(false);

        resp.Headers.TryGetValues("x-basin-next-cursor", out var cursorValues);
        var cursor = cursorValues?.FirstOrDefault();

        if (resp.Content.Headers.ContentType?.MediaType?.Contains("arrow") == true)
        {
            var bytes = await resp.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
            return (bytes, cursor);
        }

        // Server returned JSON — not Arrow IPC.
        return (Array.Empty<byte>(), cursor);
    }

    // ------------------------------------------------------------------
    // Execution — write
    // ------------------------------------------------------------------

    /// <summary>
    /// POST /rest/v1/:table — insert one row or a list of rows (201).
    /// Returns the server response as a raw JsonElement (typically { ok, tag }).
    /// </summary>
    public async Task<JsonElement?> InsertAsync(object values, CancellationToken ct = default)
    {
        return await _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Post,
            $"/rest/v1/{Uri.EscapeDataString(_table)}",
            body: values,
            ct: ct).ConfigureAwait(false);
    }

    /// <summary>PATCH /rest/v1/:table?filters — update rows matching the active filters.</summary>
    public async Task<JsonElement?> UpdateAsync(object values, CancellationToken ct = default)
    {
        return await _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Patch,
            $"/rest/v1/{Uri.EscapeDataString(_table)}",
            query: _query,
            body: values,
            ct: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// DELETE /rest/v1/:table?filters.
    /// May throw <see cref="BasinApiException"/> with code E_ENGINE_UNSUPPORTED (501)
    /// on engines without DELETE support.
    /// </summary>
    public async Task<JsonElement?> DeleteAsync(CancellationToken ct = default)
    {
        return await _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Delete,
            $"/rest/v1/{Uri.EscapeDataString(_table)}",
            query: _query,
            ct: ct).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static QueryResult NormalizeGet(string raw)
    {
        if (string.IsNullOrEmpty(raw))
            return new QueryResult();

        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        if (root.ValueKind == JsonValueKind.Array)
        {
            var rows = ParseRowArray(root);
            return new QueryResult { Rows = rows };
        }

        if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("rows", out var rowsEl))
        {
            var rows = rowsEl.ValueKind == JsonValueKind.Array
                ? ParseRowArray(rowsEl)
                : Array.Empty<Dictionary<string, JsonElement>>();

            string? nextCursor = null;
            if (root.TryGetProperty("next_cursor", out var nc) &&
                nc.ValueKind == JsonValueKind.String)
            {
                nextCursor = nc.GetString();
            }

            return new QueryResult { Rows = rows, NextCursor = nextCursor };
        }

        return new QueryResult();
    }

    private static IReadOnlyList<Dictionary<string, JsonElement>> ParseRowArray(JsonElement arrayEl)
    {
        var rows = new List<Dictionary<string, JsonElement>>(arrayEl.GetArrayLength());
        foreach (var el in arrayEl.EnumerateArray())
        {
            if (el.ValueKind != JsonValueKind.Object) continue;
            var row = new Dictionary<string, JsonElement>();
            foreach (var prop in el.EnumerateObject())
                row[prop.Name] = prop.Value.Clone();
            rows.Add(row);
        }
        return rows;
    }
}
