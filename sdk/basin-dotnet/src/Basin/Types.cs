using System.Text.Json;
using System.Text.Json.Serialization;

namespace Basin;

// ---------------------------------------------------------------------------
// Auth types
// ---------------------------------------------------------------------------

/// <summary>
/// Token pair returned by /auth/v1/signin, /auth/v1/refresh,
/// /auth/v1/magic-link/consume (crates/basin-rest/src/routes/auth.rs token_body).
/// JSON field names match the Rust serde structs exactly.
/// </summary>
public sealed class Session
{
    [JsonPropertyName("access_token")]
    public string AccessToken { get; init; } = "";

    [JsonPropertyName("refresh_token")]
    public string RefreshToken { get; init; } = "";

    /// <summary>RFC 3339 expiry timestamp.</summary>
    [JsonPropertyName("access_expires_at")]
    public string AccessExpiresAt { get; init; } = "";

    /// <summary>RFC 3339 expiry timestamp.</summary>
    [JsonPropertyName("refresh_expires_at")]
    public string RefreshExpiresAt { get; init; } = "";
}

/// <summary>POST /auth/v1/signup response.</summary>
public sealed class SignUpResult
{
    [JsonPropertyName("ok")]
    public bool Ok { get; init; }

    [JsonPropertyName("user_id")]
    public string UserId { get; init; } = "";
}

/// <summary>
/// POST /auth/v1/api-keys response (the secret is shown exactly once).
/// </summary>
public sealed class ApiKeyIssued
{
    [JsonPropertyName("id")]
    public long Id { get; init; }

    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("secret")]
    public string Secret { get; init; } = "";

    [JsonPropertyName("created_at")]
    public string CreatedAt { get; init; } = "";
}

/// <summary>GET /auth/v1/api-keys element.</summary>
public sealed class ApiKeyDescriptor
{
    [JsonPropertyName("id")]
    public long Id { get; init; }

    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("created_at")]
    public string CreatedAt { get; init; } = "";

    [JsonPropertyName("last_used_at")]
    public string? LastUsedAt { get; init; }

    [JsonPropertyName("revoked_at")]
    public string? RevokedAt { get; init; }
}

/// <summary>GET /auth/v1/oauth/:provider/authorize response.</summary>
public sealed class OAuthAuthorizeResult
{
    /// <summary>Full provider authorize URL — redirect the browser here.</summary>
    [JsonPropertyName("redirect_url")]
    public string RedirectUrl { get; init; } = "";

    /// <summary>CSRF state value included in RedirectUrl; echoed back on callback.</summary>
    [JsonPropertyName("state")]
    public string State { get; init; } = "";
}

// ---------------------------------------------------------------------------
// MFA types
// ---------------------------------------------------------------------------

/// <summary>POST /auth/v1/factors response for factor_type="totp".</summary>
public sealed class TotpEnrollResult
{
    [JsonPropertyName("factor_id")]
    public string FactorId { get; init; } = "";

    [JsonPropertyName("factor_type")]
    public string FactorType { get; init; } = "";

    /// <summary>Base-32 TOTP secret — display in QR code or give to an auth app.</summary>
    [JsonPropertyName("secret_b32")]
    public string SecretB32 { get; init; } = "";

    /// <summary>otpauth://totp/... URI suitable for QR code display.</summary>
    [JsonPropertyName("otpauth_uri")]
    public string OtpauthUri { get; init; } = "";
}

/// <summary>
/// POST /auth/v1/factors response for factor_type="webauthn".
/// Pass CreationOptionsJson to navigator.credentials.create() and then call
/// VerifyFactor with the attestation response and the returned ChallengeId.
/// </summary>
public sealed class WebAuthnEnrollResult
{
    [JsonPropertyName("factor_id")]
    public string FactorId { get; init; } = "";

    [JsonPropertyName("factor_type")]
    public string FactorType { get; init; } = "";

    [JsonPropertyName("challenge_id")]
    public string ChallengeId { get; init; } = "";

    /// <summary>Serialised PublicKeyCredentialCreationOptions JSON.</summary>
    [JsonPropertyName("creation_options_json")]
    public string CreationOptionsJson { get; init; } = "";
}

/// <summary>Element of GET /auth/v1/factors array.</summary>
public sealed class FactorDescriptor
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = "";

    /// <summary>"totp" | "webauthn"</summary>
    [JsonPropertyName("factor_type")]
    public string FactorType { get; init; } = "";

    /// <summary>"unverified" | "verified"</summary>
    [JsonPropertyName("status")]
    public string Status { get; init; } = "";

    [JsonPropertyName("friendly_name")]
    public string FriendlyName { get; init; } = "";

    [JsonPropertyName("created_at")]
    public string CreatedAt { get; init; } = "";

    [JsonPropertyName("updated_at")]
    public string UpdatedAt { get; init; } = "";
}

/// <summary>
/// POST /auth/v1/factors/:id/verify response.
/// RecoveryCodes is only present on the first verified factor — store securely.
/// </summary>
public sealed class VerifyFactorResult
{
    [JsonPropertyName("ok")]
    public bool Ok { get; init; }

    [JsonPropertyName("recovery_codes")]
    public IReadOnlyList<string>? RecoveryCodes { get; init; }
}

/// <summary>POST /auth/v1/factors/:id/challenge response for a TOTP factor.</summary>
public sealed class TotpChallengeResult
{
    [JsonPropertyName("challenge_id")]
    public string ChallengeId { get; init; } = "";
}

/// <summary>
/// POST /auth/v1/factors/:id/challenge response for a WebAuthn factor.
/// Pass RequestOptionsJson to navigator.credentials.get() to obtain the assertion.
/// </summary>
public sealed class WebAuthnChallengeResult
{
    [JsonPropertyName("challenge_id")]
    public string ChallengeId { get; init; } = "";

    /// <summary>Serialised PublicKeyCredentialRequestOptions JSON; present for WebAuthn factors.</summary>
    [JsonPropertyName("request_options_json")]
    public string? RequestOptionsJson { get; init; }
}

// ---------------------------------------------------------------------------
// Query / data types
// ---------------------------------------------------------------------------

/// <summary>Non-row result shape for writes: { ok, tag }.</summary>
public sealed class ExecTag
{
    [JsonPropertyName("ok")]
    public bool Ok { get; init; }

    [JsonPropertyName("tag")]
    public string Tag { get; init; } = "";
}

/// <summary>
/// Normalised result of a GET request.
/// When the server returns a plain JSON array, NextCursor is null.
/// When the server returns { rows, next_cursor }, NextCursor may be populated.
/// </summary>
public sealed class QueryResult
{
    public IReadOnlyList<Dictionary<string, JsonElement>> Rows { get; init; }
        = Array.Empty<Dictionary<string, JsonElement>>();

    public string? NextCursor { get; init; }

    /// <summary>
    /// Deserialise rows into a typed list via System.Text.Json.
    /// </summary>
    public IReadOnlyList<T> Into<T>()
    {
        var json = JsonSerializer.Serialize(Rows);
        return JsonSerializer.Deserialize<List<T>>(json, BasinJsonOptions.Default)
               ?? new List<T>();
    }
}

/// <summary>
/// Lazily-streamed result of a <c>?stream=true</c> (NDJSON) GET request.
///
/// Implements <see cref="IAsyncEnumerable{T}"/> so it can be used directly in
/// an <c>await foreach</c> loop. After the loop completes (or after the
/// enumerator is disposed), <see cref="NextCursor"/> holds the keyset cursor
/// from the trailing <c>{"_basin_next_cursor":"…"}</c> NDJSON line, or null
/// when the server sent no cursor.
///
/// The result object can be enumerated exactly once; call
/// <see cref="QueryBuilder.StreamAsync"/> again for subsequent pages.
/// </summary>
public sealed class StreamResult : IAsyncEnumerable<Dictionary<string, JsonElement>>
{
    private readonly BasinTransport _transport;
    private readonly string _table;
    private readonly IReadOnlyList<(string, string)> _query;
    private readonly CancellationToken _ct;

    /// <summary>
    /// The keyset pagination cursor extracted from the trailing NDJSON line
    /// <c>{"_basin_next_cursor":"…"}</c>. Null until the enumeration completes
    /// or when the server sent no cursor.
    /// </summary>
    public string? NextCursor { get; private set; }

    internal StreamResult(
        BasinTransport transport,
        string table,
        IReadOnlyList<(string, string)> query,
        CancellationToken ct)
    {
        _transport = transport;
        _table     = table;
        _query     = query;
        _ct        = ct;
    }

    /// <inheritdoc />
    public async IAsyncEnumerator<Dictionary<string, JsonElement>> GetAsyncEnumerator(
        CancellationToken cancellationToken = default)
    {
        // Merge cancellation tokens: the one passed at construction time and the
        // one passed by the await-foreach infrastructure (e.g. via WithCancellation).
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_ct, cancellationToken);
        var ct = linked.Token;

        var resp = await _transport.RequestStreamAsync(
            HttpMethod.Get,
            $"/rest/v1/{Uri.EscapeDataString(_table)}",
            query: _query,
            ct: ct).ConfigureAwait(false);

        using (resp)
        await using (var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false))
        using (var reader = new System.IO.StreamReader(stream, System.Text.Encoding.UTF8,
                                                       detectEncodingFromByteOrderMarks: false,
                                                       bufferSize: 4096,
                                                       leaveOpen: true))
        {
            string? line;
            while ((line = await reader.ReadLineAsync(ct).ConfigureAwait(false)) is not null)
            {
                if (line.Length == 0) continue;

                JsonElement root;
                try
                {
                    using var doc = JsonDocument.Parse(line);
                    root = doc.RootElement.Clone();
                }
                catch (JsonException)
                {
                    // Skip malformed lines rather than aborting the stream.
                    continue;
                }

                if (root.ValueKind != JsonValueKind.Object) continue;

                // Detect the trailing cursor sentinel.
                if (root.TryGetProperty("_basin_next_cursor", out var nc) &&
                    nc.ValueKind == JsonValueKind.String)
                {
                    NextCursor = nc.GetString();
                    continue;
                }

                // Materialise the row.
                var row = new Dictionary<string, JsonElement>();
                foreach (var prop in root.EnumerateObject())
                    row[prop.Name] = prop.Value.Clone();

                yield return row;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Storage types
// ---------------------------------------------------------------------------

/// <summary>Metadata object for a storage bucket.</summary>
public sealed class Bucket
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = "";

    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    [JsonPropertyName("public")]
    public bool Public { get; init; }

    [JsonPropertyName("file_size_limit")]
    public long? FileSizeLimit { get; init; }

    [JsonPropertyName("allowed_mime_types")]
    public IReadOnlyList<string> AllowedMimeTypes { get; init; } = Array.Empty<string>();

    [JsonPropertyName("created_at")]
    public string CreatedAt { get; init; } = "";

    [JsonPropertyName("updated_at")]
    public string UpdatedAt { get; init; } = "";
}

/// <summary>Metadata for an uploaded storage object.</summary>
public sealed class StorageObject
{
    [JsonPropertyName("id")]
    public string Id { get; init; } = "";

    [JsonPropertyName("bucket_id")]
    public string BucketId { get; init; } = "";

    [JsonPropertyName("path")]
    public string Path { get; init; } = "";

    [JsonPropertyName("size")]
    public long Size { get; init; }

    [JsonPropertyName("mime_type")]
    public string? MimeType { get; init; }

    [JsonPropertyName("metadata")]
    public JsonElement? Metadata { get; init; }

    [JsonPropertyName("owner")]
    public string? Owner { get; init; }

    [JsonPropertyName("etag")]
    public string ETag { get; init; } = "";

    [JsonPropertyName("created_at")]
    public string CreatedAt { get; init; } = "";

    [JsonPropertyName("updated_at")]
    public string UpdatedAt { get; init; } = "";
}

/// <summary>
/// POST /storage/v1/object/sign/upload/:bucket/*path response.
/// Wire fields are camelCase (signedUrl / expiresAt) per the Rust handler.
/// </summary>
public sealed class SignedUrl
{
    /// <summary>Server-relative signed path (signedUrl wire field).</summary>
    [JsonPropertyName("signedUrl")]
    public string SignedUrlPath { get; init; } = "";

    /// <summary>RFC 3339 expiry (expiresAt wire field).</summary>
    [JsonPropertyName("expiresAt")]
    public string ExpiresAt { get; init; } = "";

    /// <summary>Full URL built from base + SignedUrlPath (SDK-computed, not in wire response).</summary>
    public string AbsoluteUrl { get; init; } = "";
}

/// <summary>Result of a storage object download.</summary>
public sealed class DownloadResult
{
    public byte[] Data { get; init; } = Array.Empty<byte>();
    public string? ContentType { get; init; }
    public string? ETag { get; init; }
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/// <summary>Proxied response from ANY /fn/v1/:name.</summary>
public sealed class FunctionInvokeResult
{
    public int Status { get; init; }
    public IReadOnlyDictionary<string, string> Headers { get; init; }
        = new Dictionary<string, string>();

    /// <summary>
    /// Parsed response body. JsonElement when Content-Type is JSON, string otherwise.
    /// </summary>
    public object? Data { get; init; }
}

// ---------------------------------------------------------------------------
// Realtime wire frames
// // Protocol source: crates/basin-realtime/src/ws.rs (ClientMsg / ServerMsg)
// ---------------------------------------------------------------------------

/// <summary>"INSERT" | "UPDATE" | "DELETE"</summary>
public enum ChangeOp { INSERT, UPDATE, DELETE }

/// <summary>{"type":"event",...} server frame for a table change event.</summary>
public sealed class RealtimeEvent
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("project")]
    public string Project { get; init; } = "";

    [JsonPropertyName("table")]
    public string Table { get; init; } = "";

    [JsonPropertyName("op")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public ChangeOp Op { get; init; }

    [JsonPropertyName("seq")]
    public long Seq { get; init; }

    [JsonPropertyName("before")]
    public Dictionary<string, JsonElement>? Before { get; init; }

    [JsonPropertyName("after")]
    public Dictionary<string, JsonElement>? After { get; init; }
}

/// <summary>{"type":"error",...} frame — invalid_filter or lag errors.</summary>
public sealed class RealtimeErrorFrame
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("code")]
    public string Code { get; init; } = "";

    [JsonPropertyName("table")]
    public string Table { get; init; } = "";

    [JsonPropertyName("missed")]
    public long? Missed { get; init; }
}

/// <summary>
/// {"type":"gap",...} frame — reconnect-resume gap; missed events were evicted,
/// cold re-sync needed.
/// </summary>
public sealed class RealtimeGapFrame
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("table")]
    public string Table { get; init; } = "";

    [JsonPropertyName("last_event_id")]
    public long LastEventId { get; init; }

    [JsonPropertyName("oldest_in_ring")]
    public long OldestInRing { get; init; }

    [JsonPropertyName("newest_in_ring")]
    public long NewestInRing { get; init; }
}

/// <summary>{"type":"subscribed","table":...} ack frame.</summary>
public sealed class SubscribedFrame
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("table")]
    public string Table { get; init; } = "";
}

/// <summary>{"type":"unsubscribed","table":...} ack frame.</summary>
public sealed class UnsubscribedFrame
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("table")]
    public string Table { get; init; } = "";
}

/// <summary>A single presence member.</summary>
public sealed class PresenceEntry
{
    [JsonPropertyName("client_id")]
    public string ClientId { get; init; } = "";

    [JsonPropertyName("metadata")]
    public JsonElement? Metadata { get; init; }

    [JsonPropertyName("joined_at")]
    public string? JoinedAt { get; init; }
}

/// <summary>{"type":"presence_state",...} frame — snapshot of current presence channel members.</summary>
public sealed class PresenceStateFrame
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("channel")]
    public string Channel { get; init; } = "";

    [JsonPropertyName("presences")]
    public IReadOnlyList<PresenceEntry> Presences { get; init; } = Array.Empty<PresenceEntry>();
}

/// <summary>{"type":"presence_diff",...} frame — delta of joins/leaves.</summary>
public sealed class PresenceDiffFrame
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("channel")]
    public string Channel { get; init; } = "";

    [JsonPropertyName("joins")]
    public IReadOnlyList<PresenceEntry> Joins { get; init; } = Array.Empty<PresenceEntry>();

    [JsonPropertyName("leaves")]
    public IReadOnlyList<PresenceEntry> Leaves { get; init; } = Array.Empty<PresenceEntry>();
}

/// <summary>
/// {"type":"presenceerror",...} frame.
/// Note: wire type is "presenceerror" (no underscore) per Rust rename_all="lowercase".
/// </summary>
public sealed class PresenceErrorFrame
{
    [JsonPropertyName("type")]
    public string Type { get; init; } = "";

    [JsonPropertyName("code")]
    public string Code { get; init; } = "";

    [JsonPropertyName("channel")]
    public string Channel { get; init; } = "";

    [JsonPropertyName("message")]
    public string Message { get; init; } = "";
}

/// <summary>
/// Discriminated union of all server-to-client realtime frame types.
/// Exactly one property is non-null depending on the frame type.
/// </summary>
public sealed class ServerFrame
{
    /// <summary>Raw "type" value from the wire frame.</summary>
    public string Type { get; init; } = "";

    public RealtimeEvent? Event { get; init; }
    public RealtimeErrorFrame? Error { get; init; }
    public RealtimeGapFrame? Gap { get; init; }
    public SubscribedFrame? Subscribed { get; init; }
    public UnsubscribedFrame? Unsubscribed { get; init; }
    public PresenceStateFrame? PresenceState { get; init; }
    public PresenceDiffFrame? PresenceDiff { get; init; }
    public PresenceErrorFrame? PresenceError { get; init; }
}

// ---------------------------------------------------------------------------
// Shared JSON options
// ---------------------------------------------------------------------------

internal static class BasinJsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };
}
