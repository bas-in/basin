using System.Text.Json;

namespace Basin;

/// <summary>
/// Base for all Basin SDK exceptions.
/// </summary>
public class BasinException : Exception
{
    protected BasinException(string message) : base(message) { }
    protected BasinException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Raised for any non-2xx response from a Basin server.
/// </summary>
/// <remarks>
/// <para><see cref="Code"/> is the stable contract — match on it, not <see cref="Message"/>.</para>
/// <para>
/// Stable codes: E_UNAUTHENTICATED, E_FORBIDDEN, E_NOT_FOUND, E_INVALID_REQUEST,
/// E_RATE_LIMITED, E_ENGINE_UNSUPPORTED, E_INTERNAL, E_EMAIL_DISABLED, E_REVOKED_TOKEN.
/// </para>
/// </remarks>
public class BasinApiException : BasinException
{
    /// <summary>Stable error code from the response body (E_*).</summary>
    public string Code { get; }

    /// <summary>HTTP status code of the response (0 when synthesised client-side).</summary>
    public int Status { get; }

    /// <summary>
    /// Optional SQLSTATE code from the engine (e.g. "23505" for unique violation).
    /// Null when the error did not originate from the SQL engine.
    /// </summary>
    public string? SqlState { get; }

    public BasinApiException(string code, string message, int status, string? sqlState = null)
        : base(message)
    {
        Code = code;
        Status = status;
        SqlState = sqlState;
    }

    public override string ToString() =>
        $"BasinApiException(code={Code}, status={Status}, message={Message}" +
        (SqlState is not null ? $", sqlstate={SqlState}" : "") + ")";

    internal static BasinApiException FromResponseBody(string body, int status)
    {
        var code = "E_UNKNOWN";
        var message = $"HTTP {status}";
        string? sqlState = null;

        if (!string.IsNullOrEmpty(body))
        {
            try
            {
                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;
                if (root.TryGetProperty("code", out var codeEl) && codeEl.ValueKind == JsonValueKind.String)
                    code = codeEl.GetString() ?? code;
                if (root.TryGetProperty("message", out var msgEl) && msgEl.ValueKind == JsonValueKind.String)
                    message = msgEl.GetString() ?? message;
                if (root.TryGetProperty("sqlstate", out var sqlEl) && sqlEl.ValueKind == JsonValueKind.String)
                    sqlState = sqlEl.GetString();
            }
            catch (JsonException)
            {
                message = body;
            }
        }

        return new BasinApiException(code, message, status, sqlState);
    }
}

/// <summary>
/// Raised when the transport layer fails (connection refused, timeout, etc.)
/// before a server response is received.
/// </summary>
public class BasinNetworkException : BasinException
{
    public BasinNetworkException(string message) : base(message) { }
    public BasinNetworkException(string message, Exception inner) : base(message, inner) { }
}
