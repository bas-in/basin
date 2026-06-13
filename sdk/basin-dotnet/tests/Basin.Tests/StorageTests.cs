using System.Net;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Basin.Tests;

/// <summary>
/// StorageClient and StorageBucketClient tests.
/// </summary>
public class StorageTests
{
    private const string BaseUrl = "http://localhost";

    private static BasinClient BuildClient(MockHandler handler, string? projectId = "proj-001") =>
        new BasinClient.Builder()
            .WithUrl(BaseUrl)
            .WithApiKey("test-key")
            .WithProjectId(projectId)
            .WithHttpClient(new HttpClient(handler))
            .Build();

    private static object BucketPayload(string name = "avatars") => new
    {
        id = "bucket-001",
        name,
        @public = false,
        file_size_limit = (long?)null,
        allowed_mime_types = new string[] { },
        created_at = "2024-01-01T00:00:00Z",
        updated_at = "2024-01-01T00:00:00Z",
    };

    private static object ObjectPayload(string path = "alice/photo.png") => new
    {
        id = "obj-001",
        bucket_id = "bucket-001",
        path,
        size = 1024L,
        mime_type = "image/png",
        metadata = new { },
        owner = "user-001",
        etag = "\"abc123\"",
        created_at = "2024-01-01T00:00:00Z",
        updated_at = "2024-01-01T00:00:00Z",
    };

    // ------------------------------------------------------------------
    // Bucket management
    // ------------------------------------------------------------------

    [Fact]
    public async Task CreateBucketAsync_ReturnsBucket()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/storage/v1/bucket"), BucketPayload(), HttpStatusCode.Created);

        using var client = BuildClient(handler);
        var bucket = await client.Storage.CreateBucketAsync("avatars", @public: true);

        Assert.Equal("avatars", bucket.Name);
        Assert.Equal("bucket-001", bucket.Id);
    }

    [Fact]
    public async Task GetBucketAsync_ReturnsBucket()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/storage/v1/bucket/avatars"), BucketPayload());

        using var client = BuildClient(handler);
        var bucket = await client.Storage.GetBucketAsync("avatars");
        Assert.Equal("avatars", bucket.Name);
    }

    [Fact]
    public async Task DeleteBucketAsync_CallsDeleteRoute()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Delete("/storage/v1/bucket/avatars"), new { ok = true });

        using var client = BuildClient(handler);
        await client.Storage.DeleteBucketAsync("avatars");

        Assert.Equal(HttpMethod.Delete, handler.Requests.Single().Method);
    }

    // ------------------------------------------------------------------
    // Object operations
    // ------------------------------------------------------------------

    [Fact]
    public async Task UploadAsync_PostsBytesToObjectRoute()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/storage/v1/object/avatars"), ObjectPayload());

        using var client = BuildClient(handler);
        var obj = await client.Storage.FromBucket("avatars")
                              .UploadAsync("alice/photo.png", new byte[] { 1, 2, 3 }, "image/png");

        Assert.Equal("alice/photo.png", obj.Path);
        Assert.Equal(HttpMethod.Post, handler.Requests.Single().Method);
    }

    [Fact]
    public async Task DownloadAsync_ReturnsBytesAndHeaders()
    {
        var handler = new MockHandler();
        handler.Respond(
            Match.Get("/storage/v1/object/avatars"),
            HttpStatusCode.OK,
            body: null,   // will be set below via content
            contentType: "image/png");

        // Rebuild mock with actual content
        var handler2 = new MockHandler();
        var content = new byte[] { 0x89, 0x50, 0x4E, 0x47 }; // PNG magic bytes
        handler2.Respond(
            Match.Get("/storage/v1/object/avatars"),
            HttpStatusCode.OK,
            body: Convert.ToBase64String(content), // just need non-empty
            contentType: "image/png");

        using var client = BuildClient(handler2);
        var result = await client.Storage.FromBucket("avatars").DownloadAsync("alice/photo.png");

        Assert.NotNull(result.Data);
        Assert.Equal("image/png", result.ContentType);
    }

    [Fact]
    public async Task RemoveAsync_DeletesSingleObject()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Delete("/storage/v1/object/avatars/alice/photo.png"), new { ok = true });

        using var client = BuildClient(handler);
        await client.Storage.FromBucket("avatars").RemoveAsync("alice/photo.png");

        Assert.Equal(HttpMethod.Delete, handler.Requests.Single().Method);
    }

    [Fact]
    public async Task ListAsync_PostsToListRoute()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/storage/v1/object/list/avatars"),
                new[] { ObjectPayload() });

        using var client = BuildClient(handler);
        var objects = await client.Storage.FromBucket("avatars").ListAsync(prefix: "alice/");

        Assert.Single(objects);
        Assert.Equal("alice/photo.png", objects[0].Path);
    }

    [Fact]
    public async Task RemoveByPrefixesAsync_DeletesWithBody()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Delete("/storage/v1/object/avatars"), new { ok = true });

        using var client = BuildClient(handler);
        await client.Storage.FromBucket("avatars")
                    .RemoveByPrefixesAsync(new[] { "alice/", "bob/" });

        Assert.Equal(HttpMethod.Delete, handler.Requests.Single().Method);
    }

    // ------------------------------------------------------------------
    // Public URL + signed URL
    // ------------------------------------------------------------------

    [Fact]
    public void GetPublicUrl_BuildsCorrectUrl()
    {
        var handler = new MockHandler();
        using var client = BuildClient(handler, "proj-001");

        var url = client.Storage.FromBucket("avatars").GetPublicUrl("alice/photo.png");
        Assert.Equal("http://localhost/storage/v1/object/public/proj-001/avatars/alice/photo.png", url);
    }

    [Fact]
    public void GetPublicUrl_ThrowsWhenNoProjectId()
    {
        var handler = new MockHandler();
        using var client = BuildClient(handler, projectId: null);

        var ex = Assert.Throws<BasinApiException>(() =>
            client.Storage.FromBucket("avatars").GetPublicUrl("file.txt"));

        Assert.Equal("E_INVALID_REQUEST", ex.Code);
    }

    [Fact]
    public async Task CreateSignedUrlAsync_ReturnsSignedUrl()
    {
        // Wire response uses camelCase: signedUrl / expiresAt (storage_sign.rs)
        var handler = new MockHandler()
            .RespondJson(
                Match.Post("/storage/v1/object/sign/upload/avatars"),
                new
                {
                    signedUrl = "/storage/v1/object/sign/proj-001/avatars/alice/photo.png?token=xyz",
                    expiresAt = "2099-01-01T00:00:00Z",
                });

        using var client = BuildClient(handler);
        var signed = await client.Storage.FromBucket("avatars")
                                 .CreateSignedUrlAsync("alice/photo.png", 3600);

        Assert.Contains("token=xyz", signed.SignedUrlPath);
        Assert.Equal("2099-01-01T00:00:00Z", signed.ExpiresAt);
        Assert.StartsWith("http://localhost", signed.AbsoluteUrl);
    }
}
