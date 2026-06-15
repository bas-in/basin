/// Shared wire types mirrored from the Rust handlers they bind to.
///
/// Field names use exact server serde names (snake_case from Rust `serde`
/// with `rename_all = "snake_case"`, except where JSON uses camelCase
/// — e.g. signedUrl / expiresAt from storage_sign.rs, presenceerror from
/// ws.rs rename_all="lowercase").
library;

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Token body returned by /auth/v1/signin, /auth/v1/refresh,
/// /auth/v1/magic-link/consume (crates/basin-rest/src/routes/auth.rs token_body).
class Session {
  final String accessToken;
  final String refreshToken;

  /// RFC 3339 expiry of the access token.
  final String accessExpiresAt;

  /// RFC 3339 expiry of the refresh token.
  final String refreshExpiresAt;

  const Session({
    required this.accessToken,
    required this.refreshToken,
    required this.accessExpiresAt,
    required this.refreshExpiresAt,
  });

  factory Session.fromJson(Map<String, dynamic> j) => Session(
    accessToken: j['access_token'] as String,
    refreshToken: j['refresh_token'] as String,
    accessExpiresAt: j['access_expires_at'] as String,
    refreshExpiresAt: j['refresh_expires_at'] as String,
  );

  Map<String, dynamic> toJson() => {
    'access_token': accessToken,
    'refresh_token': refreshToken,
    'access_expires_at': accessExpiresAt,
    'refresh_expires_at': refreshExpiresAt,
  };
}

/// POST /auth/v1/signup response.
class SignUpResult {
  final bool ok;
  final String userId;

  const SignUpResult({required this.ok, required this.userId});

  factory SignUpResult.fromJson(Map<String, dynamic> j) =>
      SignUpResult(ok: j['ok'] as bool, userId: j['user_id'] as String);
}

/// POST /auth/v1/api-keys response (the secret is shown exactly once).
class ApiKeyIssued {
  final int id;
  final String name;
  final String secret;
  final String createdAt;

  const ApiKeyIssued({
    required this.id,
    required this.name,
    required this.secret,
    required this.createdAt,
  });

  factory ApiKeyIssued.fromJson(Map<String, dynamic> j) => ApiKeyIssued(
    id: (j['id'] as num).toInt(),
    name: j['name'] as String,
    secret: j['secret'] as String,
    createdAt: j['created_at'] as String,
  );
}

/// GET /auth/v1/api-keys element.
class ApiKeyDescriptor {
  final int id;
  final String name;
  final String createdAt;
  final String? lastUsedAt;
  final String? revokedAt;

  const ApiKeyDescriptor({
    required this.id,
    required this.name,
    required this.createdAt,
    this.lastUsedAt,
    this.revokedAt,
  });

  factory ApiKeyDescriptor.fromJson(Map<String, dynamic> j) => ApiKeyDescriptor(
    id: (j['id'] as num).toInt(),
    name: j['name'] as String,
    createdAt: j['created_at'] as String,
    lastUsedAt: j['last_used_at'] as String?,
    revokedAt: j['revoked_at'] as String?,
  );
}

// ---------------------------------------------------------------------------
// OAuth
// ---------------------------------------------------------------------------

/// GET /auth/v1/oauth/:provider/authorize response.
class OAuthAuthorizeResult {
  /// Full provider authorize URL — redirect the browser here.
  final String redirectUrl;

  /// CSRF state value included in redirectUrl; echoed back on callback.
  final String state;

  const OAuthAuthorizeResult({required this.redirectUrl, required this.state});

  factory OAuthAuthorizeResult.fromJson(Map<String, dynamic> j) =>
      OAuthAuthorizeResult(
        redirectUrl: j['redirect_url'] as String,
        state: j['state'] as String,
      );
}

// ---------------------------------------------------------------------------
// MFA
// ---------------------------------------------------------------------------

/// POST /auth/v1/factors response for factor_type="totp".
class TotpEnrollResult {
  final String factorId;
  final String factorType; // "totp"

  /// Base-32 TOTP secret — display in the QR code or give to an auth app.
  final String secretB32;

  /// otpauth://totp/... URI suitable for QR code display.
  final String otpauthUri;

  const TotpEnrollResult({
    required this.factorId,
    required this.factorType,
    required this.secretB32,
    required this.otpauthUri,
  });

  factory TotpEnrollResult.fromJson(Map<String, dynamic> j) => TotpEnrollResult(
    factorId: j['factor_id'] as String,
    factorType: j['factor_type'] as String,
    secretB32: j['secret_b32'] as String,
    otpauthUri: j['otpauth_uri'] as String,
  );
}

/// POST /auth/v1/factors response for factor_type="webauthn".
class WebAuthnEnrollResult {
  final String factorId;
  final String factorType; // "webauthn"
  final String challengeId;

  /// Serialised PublicKeyCredentialCreationOptions JSON.
  final String creationOptionsJson;

  const WebAuthnEnrollResult({
    required this.factorId,
    required this.factorType,
    required this.challengeId,
    required this.creationOptionsJson,
  });

  factory WebAuthnEnrollResult.fromJson(Map<String, dynamic> j) =>
      WebAuthnEnrollResult(
        factorId: j['factor_id'] as String,
        factorType: j['factor_type'] as String,
        challengeId: j['challenge_id'] as String,
        creationOptionsJson: j['creation_options_json'] as String,
      );
}

/// Discriminated union returned by AuthClient.enrollFactor.
sealed class MfaEnrollResult {
  static MfaEnrollResult fromJson(Map<String, dynamic> j) {
    final type = j['factor_type'] as String?;
    if (type == 'webauthn') return WebAuthnEnrollResult.fromJson(j);
    return TotpEnrollResult.fromJson(j);
  }
}

extension TotpEnrollResultMixin on TotpEnrollResult implements MfaEnrollResult {}
extension WebAuthnEnrollResultMixin
    on WebAuthnEnrollResult
    implements MfaEnrollResult {}

/// Element of GET /auth/v1/factors array.
class FactorDescriptor {
  final String id;

  /// "totp" or "webauthn"
  final String factorType;

  /// "unverified" or "verified"
  final String status;
  final String friendlyName;
  final String createdAt;
  final String updatedAt;

  const FactorDescriptor({
    required this.id,
    required this.factorType,
    required this.status,
    required this.friendlyName,
    required this.createdAt,
    required this.updatedAt,
  });

  factory FactorDescriptor.fromJson(Map<String, dynamic> j) => FactorDescriptor(
    id: j['id'] as String,
    factorType: j['factor_type'] as String,
    status: j['status'] as String,
    friendlyName: j['friendly_name'] as String,
    createdAt: j['created_at'] as String,
    updatedAt: j['updated_at'] as String,
  );
}

/// POST /auth/v1/factors/:id/verify response.
///
/// [recoveryCodes] is only present on the **first** verified factor for a
/// user — store them securely; shown exactly once.
class VerifyFactorResult {
  final bool ok;
  final List<String>? recoveryCodes;

  const VerifyFactorResult({required this.ok, this.recoveryCodes});

  factory VerifyFactorResult.fromJson(Map<String, dynamic> j) =>
      VerifyFactorResult(
        ok: (j['ok'] as bool?) ?? true,
        recoveryCodes:
            (j['recovery_codes'] as List?)?.map((e) => e as String).toList(),
      );
}

/// POST /auth/v1/factors/:id/challenge response for TOTP.
class TotpChallengeResult {
  final String challengeId;

  const TotpChallengeResult({required this.challengeId});

  factory TotpChallengeResult.fromJson(Map<String, dynamic> j) =>
      TotpChallengeResult(challengeId: j['challenge_id'] as String);
}

/// POST /auth/v1/factors/:id/challenge response for WebAuthn.
class WebAuthnChallengeResult {
  final String challengeId;

  /// Serialised PublicKeyCredentialRequestOptions JSON; pass to
  /// navigator.credentials.get().
  final String? requestOptionsJson;

  const WebAuthnChallengeResult({
    required this.challengeId,
    this.requestOptionsJson,
  });

  factory WebAuthnChallengeResult.fromJson(Map<String, dynamic> j) =>
      WebAuthnChallengeResult(
        challengeId: j['challenge_id'] as String,
        requestOptionsJson: j['request_options_json'] as String?,
      );
}

/// Discriminated union returned by AuthClient.challengeFactor.
sealed class MfaChallengeResult {
  static MfaChallengeResult fromJson(Map<String, dynamic> j) {
    if (j.containsKey('request_options_json')) {
      return WebAuthnChallengeResult.fromJson(j);
    }
    return TotpChallengeResult.fromJson(j);
  }
}

extension TotpChallengeResultMixin
    on TotpChallengeResult
    implements MfaChallengeResult {}
extension WebAuthnChallengeResultMixin
    on WebAuthnChallengeResult
    implements MfaChallengeResult {}

// ---------------------------------------------------------------------------
// Data / query
// ---------------------------------------------------------------------------

typedef Row = Map<String, dynamic>;

/// Non-row result for writes: { ok: true, tag: "..." }.
class ExecTag {
  final bool ok;
  final String tag;

  const ExecTag({required this.ok, required this.tag});

  factory ExecTag.fromJson(Map<String, dynamic> j) =>
      ExecTag(ok: j['ok'] as bool, tag: j['tag'] as String);
}

/// Wrapped paginated GET response when limit or cursor is supplied.
class Page {
  final List<Row> rows;
  final String? nextCursor;

  const Page({required this.rows, this.nextCursor});
}

// ---------------------------------------------------------------------------
// Realtime wire frames (crates/basin-realtime/src/ws.rs ClientMsg/ServerMsg)
// ---------------------------------------------------------------------------

typedef ChangeOp = String; // "INSERT" | "UPDATE" | "DELETE"

/// {"type":"event",...} server frame.
class RealtimeEvent {
  final String type;
  final String project;
  final String table;
  final ChangeOp op;
  final int seq;
  final Row? before;
  final Row? after;

  const RealtimeEvent({
    required this.type,
    required this.project,
    required this.table,
    required this.op,
    required this.seq,
    this.before,
    this.after,
  });

  factory RealtimeEvent.fromJson(Map<String, dynamic> j) => RealtimeEvent(
    type: j['type'] as String,
    project: j['project'] as String,
    table: j['table'] as String,
    op: j['op'] as String,
    seq: (j['seq'] as num).toInt(),
    before: j['before'] as Row?,
    after: j['after'] as Row?,
  );
}

class RealtimeErrorFrame {
  final String type;
  final String code;
  final String table;
  final int? missed;

  const RealtimeErrorFrame({
    required this.type,
    required this.code,
    required this.table,
    this.missed,
  });

  factory RealtimeErrorFrame.fromJson(Map<String, dynamic> j) =>
      RealtimeErrorFrame(
        type: j['type'] as String,
        code: j['code'] as String,
        table: j['table'] as String,
        missed: (j['missed'] as num?)?.toInt(),
      );
}

/// Reconnect-resume gap: missed events were evicted; cold re-sync needed.
class RealtimeGapFrame {
  final String type;
  final String table;
  final int lastEventId;
  final int oldestInRing;
  final int newestInRing;

  const RealtimeGapFrame({
    required this.type,
    required this.table,
    required this.lastEventId,
    required this.oldestInRing,
    required this.newestInRing,
  });

  factory RealtimeGapFrame.fromJson(Map<String, dynamic> j) => RealtimeGapFrame(
    type: j['type'] as String,
    table: j['table'] as String,
    lastEventId: (j['last_event_id'] as num).toInt(),
    oldestInRing: (j['oldest_in_ring'] as num).toInt(),
    newestInRing: (j['newest_in_ring'] as num).toInt(),
  );
}

class PresenceEntry {
  final String clientId;
  final dynamic metadata;
  final String? joinedAt;

  const PresenceEntry({required this.clientId, this.metadata, this.joinedAt});

  factory PresenceEntry.fromJson(Map<String, dynamic> j) => PresenceEntry(
    clientId: j['client_id'] as String? ?? '',
    metadata: j['metadata'],
    joinedAt: j['joined_at'] as String?,
  );
}

/// {"type":"presence_state","channel":…,"presences":[…]}
class PresenceStateFrame {
  final String type;
  final String channel;
  final List<PresenceEntry> presences;

  const PresenceStateFrame({
    required this.type,
    required this.channel,
    required this.presences,
  });

  factory PresenceStateFrame.fromJson(Map<String, dynamic> j) =>
      PresenceStateFrame(
        type: j['type'] as String,
        channel: j['channel'] as String,
        presences:
            (j['presences'] as List? ?? [])
                .map((e) => PresenceEntry.fromJson(e as Map<String, dynamic>))
                .toList(),
      );
}

/// {"type":"presence_diff","channel":…,"joins":[…],"leaves":[…]}
class PresenceDiffFrame {
  final String type;
  final String channel;
  final List<PresenceEntry> joins;
  final List<PresenceEntry> leaves;

  const PresenceDiffFrame({
    required this.type,
    required this.channel,
    required this.joins,
    required this.leaves,
  });

  factory PresenceDiffFrame.fromJson(Map<String, dynamic> j) =>
      PresenceDiffFrame(
        type: j['type'] as String,
        channel: j['channel'] as String,
        joins:
            (j['joins'] as List? ?? [])
                .map((e) => PresenceEntry.fromJson(e as Map<String, dynamic>))
                .toList(),
        leaves:
            (j['leaves'] as List? ?? [])
                .map((e) => PresenceEntry.fromJson(e as Map<String, dynamic>))
                .toList(),
      );
}

/// {"type":"presenceerror",...}
/// Note: wire type is "presenceerror" (no underscore) — server uses
/// rename_all="lowercase" on the PresenceError variant.
class PresenceErrorFrame {
  final String type;
  final String code;
  final String channel;
  final String message;

  const PresenceErrorFrame({
    required this.type,
    required this.code,
    required this.channel,
    required this.message,
  });

  factory PresenceErrorFrame.fromJson(Map<String, dynamic> j) =>
      PresenceErrorFrame(
        type: j['type'] as String,
        code: j['code'] as String,
        channel: j['channel'] as String,
        message: j['message'] as String? ?? '',
      );
}

/// Union of all server → client realtime frame types.
sealed class RealtimeServerFrame {}

extension RealtimeEventFrame on RealtimeEvent implements RealtimeServerFrame {}
extension RealtimeErrorFrameExt
    on RealtimeErrorFrame
    implements RealtimeServerFrame {}
extension RealtimeGapFrameExt
    on RealtimeGapFrame
    implements RealtimeServerFrame {}
extension PresenceStateFrameExt
    on PresenceStateFrame
    implements RealtimeServerFrame {}
extension PresenceDiffFrameExt
    on PresenceDiffFrame
    implements RealtimeServerFrame {}
extension PresenceErrorFrameExt
    on PresenceErrorFrame
    implements RealtimeServerFrame {}

// ---------------------------------------------------------------------------
// Storage (crates/basin-rest/src/routes/storage.rs, storage_sign.rs)
// ---------------------------------------------------------------------------

class Bucket {
  final String id;
  final String name;
  final bool public;
  final int? fileSizeLimit;
  final List<String> allowedMimeTypes;
  final String createdAt;
  final String updatedAt;

  const Bucket({
    required this.id,
    required this.name,
    required this.public,
    this.fileSizeLimit,
    required this.allowedMimeTypes,
    required this.createdAt,
    required this.updatedAt,
  });

  factory Bucket.fromJson(Map<String, dynamic> j) => Bucket(
    id: j['id'] as String,
    name: j['name'] as String,
    public: j['public'] as bool,
    fileSizeLimit: (j['file_size_limit'] as num?)?.toInt(),
    allowedMimeTypes:
        (j['allowed_mime_types'] as List? ?? []).map((e) => e as String).toList(),
    createdAt: j['created_at'] as String,
    updatedAt: j['updated_at'] as String,
  );
}

class StorageObject {
  final String id;
  final String bucketId;
  final String path;
  final int size;
  final String? mimeType;
  final dynamic metadata;
  final String? owner;
  final String etag;
  final String createdAt;
  final String updatedAt;

  const StorageObject({
    required this.id,
    required this.bucketId,
    required this.path,
    required this.size,
    this.mimeType,
    this.metadata,
    this.owner,
    required this.etag,
    required this.createdAt,
    required this.updatedAt,
  });

  factory StorageObject.fromJson(Map<String, dynamic> j) => StorageObject(
    id: j['id'] as String,
    bucketId: j['bucket_id'] as String,
    path: j['path'] as String,
    size: (j['size'] as num).toInt(),
    mimeType: j['mime_type'] as String?,
    metadata: j['metadata'],
    owner: j['owner'] as String?,
    etag: j['etag'] as String,
    createdAt: j['created_at'] as String,
    updatedAt: j['updated_at'] as String,
  );
}

/// POST /storage/v1/object/sign/upload/:bucket/*path response.
///
/// JSON field names are camelCase: `signedUrl`, `expiresAt`
/// (from storage_sign.rs serde).
class SignedUrl {
  /// Server-relative URL.
  final String signedUrl;

  /// RFC 3339 expiry.
  final String expiresAt;

  /// Full absolute URL (base + signedUrl) — convenience for immediate use.
  final String absoluteUrl;

  const SignedUrl({
    required this.signedUrl,
    required this.expiresAt,
    required this.absoluteUrl,
  });

  factory SignedUrl.fromJson(Map<String, dynamic> j, String baseUrl) {
    // Server sends camelCase: signedUrl / expiresAt (storage_sign.rs)
    final rel =
        (j['signedUrl'] as String?) ?? (j['signed_url'] as String?) ?? '';
    final exp =
        (j['expiresAt'] as String?) ?? (j['expires_at'] as String?) ?? '';
    return SignedUrl(
      signedUrl: rel,
      expiresAt: exp,
      absoluteUrl: '$baseUrl$rel',
    );
  }
}

class DownloadResult {
  final List<int> data;
  final String? contentType;
  final String? etag;

  const DownloadResult({required this.data, this.contentType, this.etag});
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

class FunctionInvokeResult {
  final int status;
  final Map<String, String> headers;
  final dynamic data;

  const FunctionInvokeResult({
    required this.status,
    required this.headers,
    required this.data,
  });
}
