using System.Text.Json;

namespace Basin;

/// <summary>
/// Storage client for /storage/v1/* routes.
///
/// Route sources (verified against crates/basin-rest/src/server.rs and
/// crates/basin-rest/src/routes/storage.rs + storage_sign.rs):
/// - POST   /storage/v1/bucket                                  create bucket
/// - GET    /storage/v1/bucket/:name                            bucket metadata
/// - DELETE /storage/v1/bucket/:name                            delete + purge
/// - POST   /storage/v1/object/:bucket/*path                    upload (JWT)
/// - GET    /storage/v1/object/:bucket/*path                    download (JWT)
/// - DELETE /storage/v1/object/:bucket/*path                    delete (JWT)
/// - POST   /storage/v1/object/list/:bucket                     list (body: prefix/limit/offset)
/// - DELETE /storage/v1/object/:bucket                          bulk delete (body: prefixes[])
/// - GET    /storage/v1/object/public/:project_id/:bucket/*path public download (no JWT)
/// - POST   /storage/v1/object/sign/upload/:bucket/*path        mint signed URL (JWT)
/// - GET    /storage/v1/object/sign/:project/:bucket/*path      signed download (no JWT)
/// </summary>
public sealed class StorageClient
{
    private readonly BasinTransport _transport;
    private readonly Func<string?> _projectIdGetter;

    internal StorageClient(BasinTransport transport, Func<string?> projectIdGetter)
    {
        _transport = transport;
        _projectIdGetter = projectIdGetter;
    }

    /// <summary>POST /storage/v1/bucket (201).</summary>
    public async Task<Bucket> CreateBucketAsync(
        string name,
        bool @public = false,
        long? fileSizeLimit = null,
        IEnumerable<string>? allowedMimeTypes = null,
        CancellationToken ct = default)
    {
        var result = await _transport.RequestJsonAsync<Bucket>(
            HttpMethod.Post,
            "/storage/v1/bucket",
            body: new
            {
                name,
                @public,
                file_size_limit = fileSizeLimit,
                allowed_mime_types = (allowedMimeTypes ?? Enumerable.Empty<string>()).ToArray()
            },
            ct: ct).ConfigureAwait(false);

        return result!;
    }

    /// <summary>GET /storage/v1/bucket/:name.</summary>
    public async Task<Bucket> GetBucketAsync(string name, CancellationToken ct = default)
    {
        var result = await _transport.RequestJsonAsync<Bucket>(
            HttpMethod.Get,
            $"/storage/v1/bucket/{Uri.EscapeDataString(name)}",
            ct: ct).ConfigureAwait(false);

        return result!;
    }

    /// <summary>DELETE /storage/v1/bucket/:name — also purges object bytes.</summary>
    public Task<JsonElement?> DeleteBucketAsync(string name, CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Delete,
            $"/storage/v1/bucket/{Uri.EscapeDataString(name)}",
            ct: ct);

    /// <summary>Return an object-level client scoped to one bucket.</summary>
    public StorageBucketClient FromBucket(string bucket) =>
        new(_transport, bucket, _projectIdGetter);
}

/// <summary>Object operations scoped to one bucket.</summary>
public sealed class StorageBucketClient
{
    private readonly BasinTransport _transport;
    private readonly string _bucket;
    private readonly Func<string?> _projectIdGetter;

    internal StorageBucketClient(
        BasinTransport transport,
        string bucket,
        Func<string?> projectIdGetter)
    {
        _transport = transport;
        _bucket = bucket;
        _projectIdGetter = projectIdGetter;
    }

    private string ObjectPath(string path) =>
        $"/storage/v1/object/{Uri.EscapeDataString(_bucket)}/{EncodeObjectPath(path)}";

    private static string EncodeObjectPath(string path) =>
        string.Join("/", path.Split('/').Select(Uri.EscapeDataString));

    /// <summary>POST /storage/v1/object/:bucket/*path — upload bytes.</summary>
    public async Task<StorageObject> UploadAsync(
        string path,
        byte[] data,
        string? contentType = null,
        CancellationToken ct = default)
    {
        var extraHeaders = contentType is not null
            ? new Dictionary<string, string> { ["Content-Type"] = contentType }
            : null;

        // Storage upload uses raw bytes, not JSON encoding.
        var resp = await _transport.RequestAsync(
            HttpMethod.Post,
            ObjectPath(path),
            body: data,
            contentType: contentType ?? "application/octet-stream",
            extraHeaders: extraHeaders,
            ct: ct).ConfigureAwait(false);

        var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        return JsonSerializer.Deserialize<StorageObject>(raw, BasinJsonOptions.Default)!;
    }

    /// <summary>GET /storage/v1/object/:bucket/*path — authenticated download.</summary>
    public async Task<DownloadResult> DownloadAsync(string path, CancellationToken ct = default)
    {
        var (data, contentType, etag) = await _transport.RequestBytesAsync(
            HttpMethod.Get,
            ObjectPath(path),
            ct: ct).ConfigureAwait(false);

        return new DownloadResult { Data = data, ContentType = contentType, ETag = etag };
    }

    /// <summary>DELETE /storage/v1/object/:bucket/*path — single delete.</summary>
    public Task<JsonElement?> RemoveAsync(string path, CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Delete,
            ObjectPath(path),
            ct: ct);

    /// <summary>POST /storage/v1/object/list/:bucket.</summary>
    public async Task<IReadOnlyList<StorageObject>> ListAsync(
        string? prefix = null,
        int? limit = null,
        int? offset = null,
        CancellationToken ct = default)
    {
        var result = await _transport.RequestJsonAsync<List<StorageObject>>(
            HttpMethod.Post,
            $"/storage/v1/object/list/{Uri.EscapeDataString(_bucket)}",
            body: new { prefix, limit, offset },
            ct: ct).ConfigureAwait(false);

        return result ?? new List<StorageObject>();
    }

    /// <summary>
    /// DELETE /storage/v1/object/:bucket with { prefixes: [...] }.
    /// Bulk-delete every object matching any prefix.
    /// </summary>
    public Task<JsonElement?> RemoveByPrefixesAsync(
        IEnumerable<string> prefixes,
        CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Delete,
            $"/storage/v1/object/{Uri.EscapeDataString(_bucket)}",
            body: new { prefixes = prefixes.ToArray() },
            ct: ct);

    /// <summary>
    /// Build the no-JWT public URL.
    /// GET /storage/v1/object/public/:project_id/:bucket/*path
    /// Only works for buckets with Public=true.
    /// </summary>
    public string GetPublicUrl(string path)
    {
        var project = _projectIdGetter();
        if (project is null)
            throw new BasinApiException(
                "E_INVALID_REQUEST",
                "public URLs require a project id — pass ProjectId to BasinClient.Builder",
                0);

        return $"{_transport.BaseUrl}/storage/v1/object/public/{project}" +
               $"/{Uri.EscapeDataString(_bucket)}/{EncodeObjectPath(path)}";
    }

    /// <summary>
    /// POST /storage/v1/object/sign/upload/:bucket/*path — mint a signed URL.
    /// TTL is capped at 7 days server-side; default 3600 s.
    /// The signed URL is for download despite "upload" in the path (the segment
    /// only disambiguates the axum router — see storage_sign.rs).
    /// </summary>
    public async Task<SignedUrl> CreateSignedUrlAsync(
        string path,
        int expiresIn = 3600,
        CancellationToken ct = default)
    {
        var resp = await _transport.RequestAsync(
            HttpMethod.Post,
            $"/storage/v1/object/sign/upload/{Uri.EscapeDataString(_bucket)}/{EncodeObjectPath(path)}",
            body: JsonSerializer.SerializeToUtf8Bytes(new { expires_in = expiresIn }, BasinJsonOptions.Default),
            contentType: "application/json",
            ct: ct).ConfigureAwait(false);

        var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

        // Wire fields are camelCase: signedUrl / expiresAt (storage_sign.rs).
        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;
        var rel = root.TryGetProperty("signedUrl", out var su) ? su.GetString() ?? "" : "";
        var expiresAt = root.TryGetProperty("expiresAt", out var ea) ? ea.GetString() ?? "" : "";

        return new SignedUrl
        {
            SignedUrlPath = rel,
            ExpiresAt = expiresAt,
            AbsoluteUrl = _transport.BaseUrl + rel,
        };
    }
}
