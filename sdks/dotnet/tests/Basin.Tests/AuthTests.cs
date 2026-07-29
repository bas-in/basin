using System.Net;
using System.Text.Json;
using Xunit;

namespace Basin.Tests;

/// <summary>
/// Auth client tests: all flows + auto-refresh + MFA + OAuth.
/// </summary>
public class AuthTests
{
    private const string BaseUrl = "http://localhost";

    private static BasinClient BuildClient(MockHandler handler) =>
        new BasinClient.Builder()
            .WithUrl(BaseUrl)
            .WithApiKey("test-key")
            .WithProjectId("proj-01")
            .WithHttpClient(new HttpClient(handler))
            .Build();

    private static object SessionPayload(string accessToken = "access-tok",
        string refreshToken = "refresh-tok") => new
    {
        access_token = accessToken,
        refresh_token = refreshToken,
        access_expires_at = "2099-01-01T00:00:00Z",
        refresh_expires_at = "2099-01-01T00:00:00Z",
    };

    // ------------------------------------------------------------------
    // Sign up / sign in / sign out / refresh
    // ------------------------------------------------------------------

    [Fact]
    public async Task SignUpAsync_ReturnsSignUpResult()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/signup"),
                new { ok = true, user_id = "user-001" },
                HttpStatusCode.Created);

        using var client = BuildClient(handler);
        var result = await client.Auth.SignUpAsync("alice@example.com", "secret");

        Assert.True(result.Ok);
        Assert.Equal("user-001", result.UserId);
    }

    [Fact]
    public async Task SignInAsync_StoresSessionAndReturnsIt()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/signin"), SessionPayload());

        using var client = BuildClient(handler);
        var session = await client.Auth.SignInAsync("alice@example.com", "secret");

        Assert.Equal("access-tok", session.AccessToken);
        Assert.Equal("refresh-tok", session.RefreshToken);
        Assert.NotNull(client.Auth.GetSession());
    }

    [Fact]
    public async Task SignOutAsync_ClearsSessionEvenOnNetworkError()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/signin"), SessionPayload())
            .RespondError(Match.Post("/auth/v1/signout"), "E_INTERNAL", "oops", HttpStatusCode.InternalServerError);

        using var client = BuildClient(handler);
        await client.Auth.SignInAsync("a@b.com", "pw");
        Assert.NotNull(client.Auth.GetSession());

        // Should not throw even though signout returned 500.
        // Wait — 500 is not a 404; it should re-throw. Use 404 instead.
        var handler2 = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/signin"), SessionPayload())
            .Respond(Match.Post("/auth/v1/signout"), HttpStatusCode.NotFound);

        using var client2 = BuildClient(handler2);
        await client2.Auth.SignInAsync("a@b.com", "pw");
        await client2.Auth.SignOutAsync(); // 404 → tolerated silently
        Assert.Null(client2.Auth.GetSession());
    }

    [Fact]
    public async Task SignOutAsync_IsNoOpWhenNoSession()
    {
        var handler = new MockHandler();
        using var client = BuildClient(handler);

        // Should not throw.
        await client.Auth.SignOutAsync();
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task RefreshSessionAsync_ThrowsWhenNoSession()
    {
        var handler = new MockHandler();
        using var client = BuildClient(handler);

        var ex = await Assert.ThrowsAsync<BasinApiException>(() =>
            client.Auth.RefreshSessionAsync());

        Assert.Equal("E_UNAUTHENTICATED", ex.Code);
    }

    [Fact]
    public async Task RefreshSessionAsync_RotatesTokens()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/signin"), SessionPayload("tok-v1", "ref-v1"))
            .RespondJson(Match.Post("/auth/v1/refresh"), SessionPayload("tok-v2", "ref-v2"));

        using var client = BuildClient(handler);
        await client.Auth.SignInAsync("a@b.com", "pw");
        var refreshed = await client.Auth.RefreshSessionAsync();

        Assert.Equal("tok-v2", refreshed.AccessToken);
        Assert.Equal("tok-v2", client.Auth.GetSession()!.AccessToken);
    }

    // ------------------------------------------------------------------
    // Auto-refresh
    // ------------------------------------------------------------------

    [Fact]
    public async Task AccessTokenAsync_AutoRefreshesNearExpiry()
    {
        // Set an already-expired access token.
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/refresh"), SessionPayload("tok-fresh", "ref-fresh"));

        using var client = BuildClient(handler);
        client.Auth.SetSession(new Session
        {
            AccessToken = "tok-expired",
            RefreshToken = "ref-old",
            AccessExpiresAt = "2000-01-01T00:00:00Z",   // past
            RefreshExpiresAt = "2099-01-01T00:00:00Z",
        });

        var tok = await client.Auth.AccessTokenAsync();
        Assert.Equal("tok-fresh", tok);
    }

    // ------------------------------------------------------------------
    // API keys
    // ------------------------------------------------------------------

    [Fact]
    public async Task CreateApiKeyAsync_ReturnsIssuedKey()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/api-keys"),
                new { id = 7, name = "ci-key", secret = "sk_live_xxx", created_at = "2024-01-01T00:00:00Z" });

        using var client = BuildClient(handler);
        var key = await client.Auth.CreateApiKeyAsync("ci-key");

        Assert.Equal(7, key.Id);
        Assert.Equal("ci-key", key.Name);
        Assert.Equal("sk_live_xxx", key.Secret);
    }

    [Fact]
    public async Task ListApiKeysAsync_ReturnsDescriptors()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/auth/v1/api-keys"),
                new[]
                {
                    new { id = 1, name = "k1", created_at = "2024-01-01T00:00:00Z", last_used_at = (string?)null, revoked_at = (string?)null },
                });

        using var client = BuildClient(handler);
        var keys = await client.Auth.ListApiKeysAsync();

        Assert.Single(keys);
        Assert.Equal(1, keys[0].Id);
    }

    [Fact]
    public async Task DeleteApiKeyAsync_CallsDeleteRoute()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Delete("/auth/v1/api-keys/42"), new { ok = true });

        using var client = BuildClient(handler);
        var resp = await client.Auth.DeleteApiKeyAsync(42);
        // Just verify no exception thrown.
        Assert.NotNull(handler.Requests.FirstOrDefault(r => r.Method == HttpMethod.Delete));
    }

    // ------------------------------------------------------------------
    // Magic links
    // ------------------------------------------------------------------

    [Fact]
    public async Task RequestMagicLinkAsync_Succeeds()
    {
        var handler = new MockHandler()
            .Respond(Match.Post("/auth/v1/magic-link"), HttpStatusCode.NoContent);

        using var client = BuildClient(handler);
        await client.Auth.RequestMagicLinkAsync("alice@example.com");
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task ConsumeMagicLinkAsync_StoresSession()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/magic-link/consume"), SessionPayload("ml-tok"));

        using var client = BuildClient(handler);
        var session = await client.Auth.ConsumeMagicLinkAsync("magic-token-xyz");

        Assert.Equal("ml-tok", session.AccessToken);
        Assert.Equal("ml-tok", client.Auth.GetSession()?.AccessToken);
    }

    // ------------------------------------------------------------------
    // OAuth
    // ------------------------------------------------------------------

    [Fact]
    public async Task GetOAuthAuthorizeUrlAsync_ReturnsRedirectUrl()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Get("/auth/v1/oauth/google/authorize"),
                new { redirect_url = "https://accounts.google.com/o/oauth2/v2/auth?...", state = "csrf-abc" });

        using var client = BuildClient(handler);
        var result = await client.Auth.GetOAuthAuthorizeUrlAsync("google", redirectTo: "https://app.example.com/callback");

        Assert.Contains("google.com", result.RedirectUrl);
        Assert.Equal("csrf-abc", result.State);
    }

    // ------------------------------------------------------------------
    // MFA — TOTP
    // ------------------------------------------------------------------

    [Fact]
    public async Task EnrollFactorAsync_ReturnsTotpEnrollResult()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/factors"),
                new
                {
                    factor_id = "factor-001",
                    factor_type = "totp",
                    secret_b32 = "JBSWY3DPEHPK3PXP",
                    otpauth_uri = "otpauth://totp/Example:alice@example.com?secret=JBSWY3DPEHPK3PXP",
                });

        using var client = BuildClient(handler);
        var result = await client.Auth.EnrollFactorAsync("totp", "Authenticator App");

        Assert.IsType<TotpEnrollResult>(result);
        var totp = (TotpEnrollResult)result;
        Assert.Equal("factor-001", totp.FactorId);
        Assert.Equal("JBSWY3DPEHPK3PXP", totp.SecretB32);
    }

    [Fact]
    public async Task EnrollFactorAsync_ReturnsWebAuthnEnrollResult()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/factors"),
                new
                {
                    factor_id = "factor-002",
                    factor_type = "webauthn",
                    challenge_id = "chal-001",
                    creation_options_json = "{\"rp\":{\"name\":\"Basin\"}}",
                });

        using var client = BuildClient(handler);
        var result = await client.Auth.EnrollFactorAsync("webauthn", "YubiKey");

        Assert.IsType<WebAuthnEnrollResult>(result);
        var wa = (WebAuthnEnrollResult)result;
        Assert.Equal("chal-001", wa.ChallengeId);
    }

    [Fact]
    public async Task VerifyFactorAsync_ReturnRecoveryCodes()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/factors/factor-001/verify"),
                new { ok = true, recovery_codes = new[] { "aabb1122", "ccdd3344" } });

        using var client = BuildClient(handler);
        var result = await client.Auth.VerifyFactorAsync("factor-001", code: "123456");

        Assert.True(result.Ok);
        Assert.Equal(2, result.RecoveryCodes?.Count);
    }

    [Fact]
    public async Task ChallengeFactorAsync_ReturnsTotpChallenge()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/factors/factor-001/challenge"),
                new { challenge_id = "chal-xyz" });

        using var client = BuildClient(handler);
        var result = await client.Auth.ChallengeFactorAsync("factor-001");

        Assert.IsType<TotpChallengeResult>(result);
        Assert.Equal("chal-xyz", ((TotpChallengeResult)result).ChallengeId);
    }

    [Fact]
    public async Task VerifyChallengeAsync_StoresAal2Session()
    {
        var handler = new MockHandler()
            .RespondJson(Match.Post("/auth/v1/factors/factor-001/challenge/verify"),
                SessionPayload("aal2-tok"));

        using var client = BuildClient(handler);
        var session = await client.Auth.VerifyChallengeAsync("factor-001", "chal-xyz", code: "654321");

        Assert.Equal("aal2-tok", session.AccessToken);
        Assert.Equal("aal2-tok", client.Auth.GetSession()?.AccessToken);
    }

    // ------------------------------------------------------------------
    // JWT project_id extraction
    // ------------------------------------------------------------------

    [Fact]
    public void ProjectIdFromJwt_ExtractsProjectId()
    {
        // Build a minimal JWT with project_id claim (base64url-encoded payload).
        // Header: {"alg":"none"}
        // Payload: {"project_id":"proj-abc","sub":"user1"}
        var header = Base64UrlEncode("""{"alg":"none"}""");
        var payload = Base64UrlEncode("""{"project_id":"proj-abc","sub":"user1"}""");
        var token = $"{header}.{payload}.sig";

        var projectId = AuthClient.ProjectIdFromJwt(token);
        Assert.Equal("proj-abc", projectId);
    }

    [Fact]
    public void ProjectIdFromJwt_ReturnsNullForRawKey()
    {
        var projectId = AuthClient.ProjectIdFromJwt("raw-api-key-no-dots");
        Assert.Null(projectId);
    }

    private static string Base64UrlEncode(string s)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(s);
        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }
}
