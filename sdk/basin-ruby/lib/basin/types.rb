# frozen_string_literal: true

module Basin
  # ---------------------------------------------------------------------------
  # Auth
  # ---------------------------------------------------------------------------

  # Token body returned by /auth/v1/signin, /auth/v1/refresh,
  # /auth/v1/magic-link/consume  (crates/basin-rest/src/routes/auth.rs token_body).
  Session = Struct.new(:access_token, :refresh_token, :access_expires_at, :refresh_expires_at, keyword_init: true) do
    def self.from_hash(h)
      new(
        access_token:       h.fetch("access_token"),
        refresh_token:      h.fetch("refresh_token"),
        access_expires_at:  h.fetch("access_expires_at"),
        refresh_expires_at: h.fetch("refresh_expires_at")
      )
    end
  end

  # POST /auth/v1/signup response.
  SignUpResult = Struct.new(:ok, :user_id, keyword_init: true) do
    def self.from_hash(h)
      new(ok: h.fetch("ok"), user_id: h.fetch("user_id"))
    end
  end

  # POST /auth/v1/api-keys response (secret shown exactly once).
  ApiKeyIssued = Struct.new(:id, :name, :secret, :created_at, keyword_init: true) do
    def self.from_hash(h)
      new(id: h["id"], name: h["name"], secret: h["secret"], created_at: h["created_at"])
    end
  end

  # GET /auth/v1/api-keys element.
  ApiKeyDescriptor = Struct.new(:id, :name, :created_at, :last_used_at, :revoked_at, keyword_init: true) do
    def self.from_hash(h)
      new(
        id:           h["id"],
        name:         h["name"],
        created_at:   h["created_at"],
        last_used_at: h["last_used_at"],
        revoked_at:   h["revoked_at"]
      )
    end
  end

  # GET /auth/v1/oauth/:provider/authorize response.
  OAuthAuthorizeResult = Struct.new(:redirect_url, :state, keyword_init: true) do
    def self.from_hash(h)
      new(redirect_url: h.fetch("redirect_url"), state: h.fetch("state"))
    end
  end

  # POST /auth/v1/factors response for factor_type="totp".
  TotpEnrollResult = Struct.new(:factor_id, :factor_type, :secret_b32, :otpauth_uri, keyword_init: true) do
    def self.from_hash(h)
      new(
        factor_id:   h.fetch("factor_id"),
        factor_type: h.fetch("factor_type"),
        secret_b32:  h.fetch("secret_b32"),
        otpauth_uri: h.fetch("otpauth_uri")
      )
    end
  end

  # POST /auth/v1/factors response for factor_type="webauthn".
  WebAuthnEnrollResult = Struct.new(:factor_id, :factor_type, :challenge_id, :creation_options_json, keyword_init: true) do
    def self.from_hash(h)
      new(
        factor_id:             h.fetch("factor_id"),
        factor_type:           h.fetch("factor_type"),
        challenge_id:          h.fetch("challenge_id"),
        creation_options_json: h.fetch("creation_options_json")
      )
    end
  end

  # GET /auth/v1/factors element.
  FactorDescriptor = Struct.new(:id, :factor_type, :status, :friendly_name, :created_at, :updated_at, keyword_init: true) do
    def self.from_hash(h)
      new(
        id:            h.fetch("id"),
        factor_type:   h.fetch("factor_type"),
        status:        h.fetch("status"),
        friendly_name: h.fetch("friendly_name"),
        created_at:    h.fetch("created_at"),
        updated_at:    h.fetch("updated_at")
      )
    end
  end

  # POST /auth/v1/factors/:id/verify response.
  # +recovery_codes+ is present only on the first verified factor — store securely.
  VerifyFactorResult = Struct.new(:ok, :recovery_codes, keyword_init: true) do
    def self.from_hash(h)
      new(ok: h.fetch("ok", true), recovery_codes: h["recovery_codes"])
    end
  end

  # POST /auth/v1/factors/:id/challenge response for a TOTP factor.
  TotpChallengeResult = Struct.new(:challenge_id, keyword_init: true) do
    def self.from_hash(h)
      new(challenge_id: h.fetch("challenge_id"))
    end
  end

  # POST /auth/v1/factors/:id/challenge response for a WebAuthn factor.
  WebAuthnChallengeResult = Struct.new(:challenge_id, :request_options_json, keyword_init: true) do
    def self.from_hash(h)
      new(
        challenge_id:         h.fetch("challenge_id"),
        request_options_json: h["request_options_json"]
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Data / Query
  # ---------------------------------------------------------------------------

  # Wrapped paginated GET response when limit or cursor is supplied.
  Page = Struct.new(:rows, :next_cursor, keyword_init: true)

  # ---------------------------------------------------------------------------
  # Realtime wire frames (crates/basin-realtime/src/ws.rs)
  # ---------------------------------------------------------------------------

  RealtimeEvent = Struct.new(:type, :project, :table, :op, :seq, :before, :after, keyword_init: true) do
    def self.from_hash(h)
      new(
        type:    h.fetch("type"),
        project: h.fetch("project"),
        table:   h.fetch("table"),
        op:      h.fetch("op"),
        seq:     h.fetch("seq"),
        before:  h["before"],
        after:   h["after"]
      )
    end
  end

  RealtimeErrorFrame = Struct.new(:type, :code, :table, :missed, keyword_init: true) do
    def self.from_hash(h)
      new(type: h["type"], code: h["code"], table: h["table"], missed: h["missed"])
    end
  end

  RealtimeGapFrame = Struct.new(:type, :table, :last_event_id, :oldest_in_ring, :newest_in_ring, keyword_init: true) do
    def self.from_hash(h)
      new(
        type:            h["type"],
        table:           h["table"],
        last_event_id:   h["last_event_id"],
        oldest_in_ring:  h["oldest_in_ring"],
        newest_in_ring:  h["newest_in_ring"]
      )
    end
  end

  SubscribedFrame = Struct.new(:type, :table, keyword_init: true) do
    def self.from_hash(h)
      new(type: h["type"], table: h["table"])
    end
  end

  UnsubscribedFrame = Struct.new(:type, :table, keyword_init: true) do
    def self.from_hash(h)
      new(type: h["type"], table: h["table"])
    end
  end

  # Presence frames (crates/basin-realtime/src/presence.rs)

  PresenceEntry = Struct.new(:client_id, :metadata, :joined_at, keyword_init: true) do
    def self.from_hash(h)
      new(client_id: h.fetch("client_id", ""), metadata: h["metadata"], joined_at: h["joined_at"])
    end
  end

  PresenceStateFrame = Struct.new(:type, :channel, :presences, keyword_init: true) do
    def self.from_hash(h)
      new(
        type:      h["type"],
        channel:   h["channel"],
        presences: (h["presences"] || []).map { |p| PresenceEntry.from_hash(p) }
      )
    end
  end

  PresenceDiffFrame = Struct.new(:type, :channel, :joins, :leaves, keyword_init: true) do
    def self.from_hash(h)
      new(
        type:    h["type"],
        channel: h["channel"],
        joins:   (h["joins"]  || []).map { |p| PresenceEntry.from_hash(p) },
        leaves:  (h["leaves"] || []).map { |p| PresenceEntry.from_hash(p) }
      )
    end
  end

  # Note: server serialises PresenceError with rename_all="lowercase" →
  # wire type is "presenceerror" (no underscore).
  PresenceErrorFrame = Struct.new(:type, :code, :channel, :message, keyword_init: true) do
    def self.from_hash(h)
      new(type: h["type"], code: h["code"], channel: h["channel"], message: h.fetch("message", ""))
    end
  end

  # ---------------------------------------------------------------------------
  # Storage (crates/basin-rest/src/routes/storage.rs, storage_sign.rs)
  # ---------------------------------------------------------------------------

  Bucket = Struct.new(:id, :name, :public, :file_size_limit, :allowed_mime_types, :created_at, :updated_at, keyword_init: true) do
    def self.from_hash(h)
      new(
        id:                 h.fetch("id"),
        name:               h.fetch("name"),
        public:             h.fetch("public"),
        file_size_limit:    h["file_size_limit"],
        allowed_mime_types: h.fetch("allowed_mime_types", []),
        created_at:         h.fetch("created_at"),
        updated_at:         h.fetch("updated_at")
      )
    end
  end

  StorageObject = Struct.new(
    :id, :bucket_id, :path, :size, :mime_type, :metadata, :owner, :etag, :created_at, :updated_at,
    keyword_init: true
  ) do
    def self.from_hash(h)
      new(
        id:         h.fetch("id"),
        bucket_id:  h.fetch("bucket_id"),
        path:       h.fetch("path"),
        size:       h.fetch("size"),
        mime_type:  h["mime_type"],
        metadata:   h["metadata"],
        owner:      h["owner"],
        etag:       h.fetch("etag"),
        created_at: h.fetch("created_at"),
        updated_at: h.fetch("updated_at")
      )
    end
  end

  DownloadResult = Struct.new(:data, :content_type, :etag, keyword_init: true)

  # POST /storage/v1/object/sign/upload/:bucket/*path response.
  # signedUrl (camelCase) and expiresAt (camelCase) are the wire field names
  # from storage_sign.rs — matched exactly here.
  SignedUrl = Struct.new(:signed_url, :expires_at, :absolute_url, keyword_init: true) do
    def self.from_hash(h, base_url)
      rel = h["signedUrl"] || h["signed_url"] || ""
      new(
        signed_url:   rel,
        expires_at:   h["expiresAt"] || h["expires_at"] || "",
        absolute_url: base_url.chomp("/") + rel
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Functions (crates/basin-rest/src/routes/fn_handler.rs)
  # ---------------------------------------------------------------------------

  FunctionInvokeResult = Struct.new(:status, :headers, :data, keyword_init: true)
end
