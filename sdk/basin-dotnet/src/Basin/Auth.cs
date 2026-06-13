using System.Text;
using System.Text.Json;

namespace Basin;

/// <summary>
/// Auth client for /auth/v1/* routes.
///
/// Route sources (verified against crates/basin-rest/src/server.rs):
/// - POST /auth/v1/signup
/// - POST /auth/v1/signin
/// - POST /auth/v1/refresh
/// - POST /auth/v1/signout
/// - POST /auth/v1/verify-email
/// - POST /auth/v1/reset-password
/// - POST /auth/v1/request-password-reset
/// - POST /auth/v1/magic-link
/// - POST /auth/v1/magic-link/consume
/// - POST|GET /auth/v1/api-keys
/// - DELETE /auth/v1/api-keys/:id
/// - GET /auth/v1/oauth/:provider/authorize
/// - POST|GET /auth/v1/factors
/// - POST /auth/v1/factors/:id/verify
/// - POST /auth/v1/factors/:id/challenge
/// - POST /auth/v1/factors/:id/challenge/verify
/// - DELETE /auth/v1/factors/:id
/// </summary>
public sealed class AuthClient
{
    private readonly BasinTransport _transport;
    private readonly string? _defaultProjectId;

    // SemaphoreSlim(1,1) guards both session reads during auto-refresh
    // and concurrent refresh calls (only one refresh executes at a time).
    private readonly SemaphoreSlim _sessionLock = new(1, 1);
    private Session? _session;

    // 10-second skew so we refresh slightly before the token actually expires.
    private const int ExpirySkewSeconds = 10;

    internal AuthClient(BasinTransport transport, string? defaultProjectId)
    {
        _transport = transport;
        _defaultProjectId = defaultProjectId;
    }

    // ------------------------------------------------------------------
    // Session management
    // ------------------------------------------------------------------

    /// <summary>Return the current session, or null when signed out.</summary>
    public Session? GetSession() => _session;

    /// <summary>
    /// Adopt an externally obtained token pair (e.g. restored from persistent storage).
    /// </summary>
    public void SetSession(Session? session) => _session = session;

    /// <summary>
    /// Return a valid access token, auto-refreshing if within <see cref="ExpirySkewSeconds"/>
    /// of expiry. Returns null when there is no active session (callers fall back to the
    /// static key).
    ///
    /// SemaphoreSlim(1,1)-guarded so concurrent calls share a single refresh.
    /// </summary>
    public async Task<string?> AccessTokenAsync(CancellationToken ct = default)
    {
        var s = _session;
        if (s is null)
            return null;

        if (IsNearExpiry(s.AccessExpiresAt))
        {
            await _sessionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                // Re-check after acquiring the lock — another thread may have refreshed.
                s = _session;
                if (s is not null && IsNearExpiry(s.AccessExpiresAt))
                    s = await RefreshSessionAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                _sessionLock.Release();
            }
        }

        return s?.AccessToken;
    }

    private static bool IsNearExpiry(string accessExpiresAt)
    {
        try
        {
            var expiry = DateTimeOffset.Parse(accessExpiresAt).ToUnixTimeSeconds();
            return expiry - ExpirySkewSeconds <= DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        }
        catch
        {
            return false;
        }
    }

    // ------------------------------------------------------------------
    // Project ID resolution
    // ------------------------------------------------------------------

    private string ResolveProjectId(string? projectId = null)
    {
        var pid = projectId
                  ?? _defaultProjectId
                  ?? (_transport.StaticKey is not null
                      ? ProjectIdFromJwt(_transport.StaticKey)
                      : null);

        if (pid is null)
            throw new BasinApiException(
                "E_INVALID_REQUEST",
                "project id required: pass ProjectId to BasinClient.Builder or supply a JWT",
                0);

        return pid;
    }

    /// <summary>
    /// Decode the project_id claim from a Basin JWT payload segment.
    /// Returns null for non-JWT tokens (e.g. raw API keys).
    /// </summary>
    internal static string? ProjectIdFromJwt(string token)
    {
        var parts = token.Split('.');
        if (parts.Length != 3)
            return null;

        try
        {
            var payload = parts[1]
                .Replace('-', '+')
                .Replace('_', '/');

            // Add padding
            payload += (payload.Length % 4) switch
            {
                2 => "==",
                3 => "=",
                _ => ""
            };

            var bytes = Convert.FromBase64String(payload);
            var json = Encoding.UTF8.GetString(bytes);
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("project_id", out var el) &&
                el.ValueKind == JsonValueKind.String)
            {
                return el.GetString();
            }
        }
        catch { /* malformed JWT — not a Basin JWT */ }

        return null;
    }

    // ------------------------------------------------------------------
    // Auth flows
    // ------------------------------------------------------------------

    /// <summary>POST /auth/v1/signup → { ok, user_id } (201).</summary>
    public async Task<SignUpResult> SignUpAsync(
        string email,
        string password,
        string? projectId = null,
        CancellationToken ct = default)
    {
        var result = await _transport.RequestJsonAsync<SignUpResult>(
            HttpMethod.Post,
            "/auth/v1/signup",
            body: new { project_id = ResolveProjectId(projectId), email, password },
            auth: false,
            ct: ct).ConfigureAwait(false);

        return result!;
    }

    /// <summary>POST /auth/v1/signin → token body; stored as the live session.</summary>
    public async Task<Session> SignInAsync(
        string email,
        string password,
        string? projectId = null,
        CancellationToken ct = default)
    {
        var session = await _transport.RequestJsonAsync<Session>(
            HttpMethod.Post,
            "/auth/v1/signin",
            body: new { project_id = ResolveProjectId(projectId), email, password },
            auth: false,
            ct: ct).ConfigureAwait(false);

        _session = session!;
        return session!;
    }

    /// <summary>
    /// Revoke the current refresh token server-side (POST /auth/v1/signout),
    /// then clear the local session.
    ///
    /// If there is no active session this is a no-op. 404 responses (older
    /// servers without the signout route) and network errors are tolerated
    /// silently. The local session is ALWAYS cleared.
    /// </summary>
    public async Task SignOutAsync(CancellationToken ct = default)
    {
        var current = _session;
        _session = null; // clear local state first

        if (current is null)
            return;

        try
        {
            await _transport.RequestJsonAsync<object>(
                HttpMethod.Post,
                "/auth/v1/signout",
                body: new { refresh_token = current.RefreshToken },
                auth: false,
                ct: ct).ConfigureAwait(false);
        }
        catch (BasinApiException ex) when (ex.Status == 404)
        {
            // Older server without the signout route — tolerate silently.
        }
        catch (BasinNetworkException)
        {
            // Network error — local clear already done; client is signed out.
        }
    }

    /// <summary>
    /// POST /auth/v1/refresh with the stored refresh token.
    /// Rotates both tokens. Throws E_REVOKED_TOKEN if the token was already rotated.
    /// </summary>
    public async Task<Session> RefreshSessionAsync(CancellationToken ct = default)
    {
        var current = _session
            ?? throw new BasinApiException("E_UNAUTHENTICATED", "no session to refresh", 0);

        var session = await _transport.RequestJsonAsync<Session>(
            HttpMethod.Post,
            "/auth/v1/refresh",
            body: new { refresh_token = current.RefreshToken },
            auth: false,
            ct: ct).ConfigureAwait(false);

        _session = session!;
        return session!;
    }

    /// <summary>POST /auth/v1/verify-email → { ok: true }.</summary>
    public Task<JsonElement?> VerifyEmailAsync(
        string token,
        string? projectId = null,
        CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Post,
            "/auth/v1/verify-email",
            body: new { project_id = ResolveProjectId(projectId), token },
            auth: false,
            ct: ct);

    /// <summary>POST /auth/v1/request-password-reset → { ok: true }.</summary>
    public Task<JsonElement?> RequestPasswordResetAsync(
        string email,
        string? projectId = null,
        CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Post,
            "/auth/v1/request-password-reset",
            body: new { project_id = ResolveProjectId(projectId), email },
            auth: false,
            ct: ct);

    /// <summary>POST /auth/v1/reset-password → { ok: true }.</summary>
    public Task<JsonElement?> ResetPasswordAsync(
        string token,
        string newPassword,
        string? projectId = null,
        CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Post,
            "/auth/v1/reset-password",
            body: new { project_id = ResolveProjectId(projectId), token, new_password = newPassword },
            auth: false,
            ct: ct);

    /// <summary>POST /auth/v1/magic-link — 204 always (never confirms address).</summary>
    public Task RequestMagicLinkAsync(string email, CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Post,
            "/auth/v1/magic-link",
            body: new { email },
            auth: false,
            ct: ct);

    /// <summary>POST /auth/v1/magic-link/consume → token body; stored as session.</summary>
    public async Task<Session> ConsumeMagicLinkAsync(string token, CancellationToken ct = default)
    {
        var session = await _transport.RequestJsonAsync<Session>(
            HttpMethod.Post,
            "/auth/v1/magic-link/consume",
            body: new { token },
            auth: false,
            ct: ct).ConfigureAwait(false);

        _session = session!;
        return session!;
    }

    // ------------------------------------------------------------------
    // API Keys
    // ------------------------------------------------------------------

    /// <summary>POST /auth/v1/api-keys (JWT-gated) → issued key incl. one-time secret.</summary>
    public Task<ApiKeyIssued> CreateApiKeyAsync(string name, CancellationToken ct = default) =>
        _transport.RequestJsonAsync<ApiKeyIssued>(
            HttpMethod.Post,
            "/auth/v1/api-keys",
            body: new { name },
            ct: ct)!;

    /// <summary>GET /auth/v1/api-keys (JWT-gated).</summary>
    public async Task<IReadOnlyList<ApiKeyDescriptor>> ListApiKeysAsync(CancellationToken ct = default)
    {
        var result = await _transport.RequestJsonAsync<List<ApiKeyDescriptor>>(
            HttpMethod.Get,
            "/auth/v1/api-keys",
            ct: ct).ConfigureAwait(false);

        return result ?? new List<ApiKeyDescriptor>();
    }

    /// <summary>DELETE /auth/v1/api-keys/:id (JWT-gated) → { ok: true }.</summary>
    public Task<JsonElement?> DeleteApiKeyAsync(long id, CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Delete,
            $"/auth/v1/api-keys/{id}",
            ct: ct);

    // ------------------------------------------------------------------
    // OAuth
    // ------------------------------------------------------------------

    /// <summary>
    /// GET /auth/v1/oauth/:provider/authorize → { redirect_url, state }.
    ///
    /// Returns the provider authorize URL. Redirect the user's browser to
    /// result.RedirectUrl to begin the OAuth flow.
    ///
    /// Supported providers: google, github, apple, bitbucket, discord, figma,
    /// gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify, twitch,
    /// twitter_x. Custom OIDC providers are registered server-side.
    /// </summary>
    public async Task<OAuthAuthorizeResult> GetOAuthAuthorizeUrlAsync(
        string provider,
        string redirectTo = "",
        string? projectId = null,
        CancellationToken ct = default)
    {
        var pid = ResolveProjectId(projectId);
        var result = await _transport.RequestJsonAsync<OAuthAuthorizeResult>(
            HttpMethod.Get,
            $"/auth/v1/oauth/{Uri.EscapeDataString(provider)}/authorize",
            query: new[] { ("project_id", pid), ("redirect_to", redirectTo) },
            auth: false,
            ct: ct).ConfigureAwait(false);

        return result!;
    }

    // ------------------------------------------------------------------
    // MFA
    // ------------------------------------------------------------------

    /// <summary>
    /// POST /auth/v1/factors — begin MFA factor enrollment (JWT-gated).
    ///
    /// Returns <see cref="TotpEnrollResult"/> for factor_type="totp",
    /// or <see cref="WebAuthnEnrollResult"/> for factor_type="webauthn".
    /// The factor is "unverified" until <see cref="VerifyFactorAsync"/> succeeds.
    /// </summary>
    public async Task<object> EnrollFactorAsync(
        string factorType,
        string friendlyName = "",
        CancellationToken ct = default)
    {
        using var resp = await _transport.RequestAsync(
            HttpMethod.Post,
            "/auth/v1/factors",
            body: System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
                new { factor_type = factorType, friendly_name = friendlyName },
                BasinJsonOptions.Default),
            contentType: "application/json",
            ct: ct).ConfigureAwait(false);

        var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(raw);

        if (doc.RootElement.TryGetProperty("factor_type", out var ft) &&
            ft.GetString() == "webauthn")
        {
            return JsonSerializer.Deserialize<WebAuthnEnrollResult>(raw, BasinJsonOptions.Default)!;
        }

        return JsonSerializer.Deserialize<TotpEnrollResult>(raw, BasinJsonOptions.Default)!;
    }

    /// <summary>GET /auth/v1/factors — list MFA factors for the current user (JWT-gated).</summary>
    public async Task<IReadOnlyList<FactorDescriptor>> ListFactorsAsync(CancellationToken ct = default)
    {
        var result = await _transport.RequestJsonAsync<List<FactorDescriptor>>(
            HttpMethod.Get,
            "/auth/v1/factors",
            ct: ct).ConfigureAwait(false);

        return result ?? new List<FactorDescriptor>();
    }

    /// <summary>
    /// POST /auth/v1/factors/:id/verify — confirm enrollment (JWT-gated).
    /// Promote the factor from "unverified" to "verified".
    /// RecoveryCodes in the result is present only on the first verified factor.
    /// </summary>
    public Task<VerifyFactorResult> VerifyFactorAsync(
        string factorId,
        string? code = null,
        string? attestation = null,
        string? challengeId = null,
        CancellationToken ct = default)
    {
        var body = new Dictionary<string, object?>();
        if (code is not null) body["code"] = code;
        if (attestation is not null) body["attestation"] = attestation;
        if (challengeId is not null) body["challenge_id"] = challengeId;

        return _transport.RequestJsonAsync<VerifyFactorResult>(
            HttpMethod.Post,
            $"/auth/v1/factors/{Uri.EscapeDataString(factorId)}/verify",
            body: body,
            ct: ct)!;
    }

    /// <summary>
    /// POST /auth/v1/factors/:id/challenge — begin a step-up challenge (JWT-gated).
    /// Returns <see cref="TotpChallengeResult"/> or <see cref="WebAuthnChallengeResult"/>.
    /// </summary>
    public async Task<object> ChallengeFactorAsync(string factorId, CancellationToken ct = default)
    {
        // Send an empty JSON object body — the Python SDK sends {} explicitly.
        using var resp = await _transport.RequestAsync(
            HttpMethod.Post,
            $"/auth/v1/factors/{Uri.EscapeDataString(factorId)}/challenge",
            body: System.Text.Encoding.UTF8.GetBytes("{}"),
            contentType: "application/json",
            ct: ct).ConfigureAwait(false);

        var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(raw);

        if (doc.RootElement.TryGetProperty("request_options_json", out var ro) &&
            ro.ValueKind != JsonValueKind.Null)
        {
            return JsonSerializer.Deserialize<WebAuthnChallengeResult>(raw, BasinJsonOptions.Default)!;
        }

        return JsonSerializer.Deserialize<TotpChallengeResult>(raw, BasinJsonOptions.Default)!;
    }

    /// <summary>
    /// POST /auth/v1/factors/:id/challenge/verify — complete step-up → aal2 JWT.
    /// On success stores the new session (access token carries "aal2" claim).
    /// </summary>
    public async Task<Session> VerifyChallengeAsync(
        string factorId,
        string challengeId,
        string? code = null,
        string? assertion = null,
        CancellationToken ct = default)
    {
        var body = new Dictionary<string, object?> { ["challenge_id"] = challengeId };
        if (code is not null) body["code"] = code;
        if (assertion is not null) body["assertion"] = assertion;

        var session = await _transport.RequestJsonAsync<Session>(
            HttpMethod.Post,
            $"/auth/v1/factors/{Uri.EscapeDataString(factorId)}/challenge/verify",
            body: body,
            ct: ct).ConfigureAwait(false);

        _session = session!;
        return session!;
    }

    /// <summary>
    /// DELETE /auth/v1/factors/:id — unenroll an MFA factor (requires aal2 JWT).
    /// The caller must hold an aal2 access token (via VerifyChallengeAsync).
    /// </summary>
    public Task<JsonElement?> UnenrollFactorAsync(string factorId, CancellationToken ct = default) =>
        _transport.RequestJsonAsync<JsonElement?>(
            HttpMethod.Delete,
            $"/auth/v1/factors/{Uri.EscapeDataString(factorId)}",
            ct: ct);
}
