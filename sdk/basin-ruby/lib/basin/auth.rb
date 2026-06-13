# frozen_string_literal: true

require "base64"
require "json"
require "time"
require "mutex_m"
require_relative "errors"
require_relative "types"

module Basin
  # Auth client — bindings for /auth/v1/*.
  #
  # Route sources (verified against crates/basin-rest/src/server.rs):
  #
  #   POST /auth/v1/signup                         server.rs:250
  #   POST /auth/v1/signin                         server.rs:251
  #   POST /auth/v1/refresh                        server.rs:252
  #   POST /auth/v1/signout                        server.rs:253
  #   POST /auth/v1/verify-email                   server.rs:254
  #   POST /auth/v1/reset-password                 server.rs:255
  #   POST /auth/v1/request-password-reset         server.rs:256
  #   POST /auth/v1/magic-link                     server.rs:262
  #   POST /auth/v1/magic-link/consume             server.rs:263
  #   POST|GET /auth/v1/api-keys                   server.rs:267-270
  #   DELETE /auth/v1/api-keys/:id                 server.rs:271
  #   GET /auth/v1/oauth/:provider/authorize       server.rs:277
  #   POST /auth/v1/factors                        server.rs:286
  #   GET  /auth/v1/factors                        server.rs:287
  #   POST /auth/v1/factors/:id/verify             server.rs:290
  #   POST /auth/v1/factors/:id/challenge          server.rs:294
  #   POST /auth/v1/factors/:id/challenge/verify   server.rs:298
  #   DELETE /auth/v1/factors/:id                  server.rs:302
  #
  # sign_out calls POST /auth/v1/signout with the refresh token (server-side
  # revocation row), then clears the local session regardless of server result.
  # 404 from older servers and network errors are tolerated silently.
  #
  # Auto-refresh: access_token() checks expiry and calls refresh_session()
  # automatically when within EXPIRY_SKEW_S seconds of the stated expiry.
  # A Mutex guards concurrent callers so only one refresh is in-flight at once.
  class AuthClient
    # Refresh this many seconds before stated expiry to avoid clock drift.
    EXPIRY_SKEW_S = 10

    # @param http [Basin::HTTP]
    # @param default_project_id [String, nil]
    def initialize(http, default_project_id = nil)
      @http               = http
      @default_project_id = default_project_id
      @session            = nil
      @mutex              = Mutex.new
    end

    # Return the live session, or nil when signed out.
    # @return [Basin::Session, nil]
    def get_session
      @session
    end

    # Adopt an externally obtained token pair (e.g. restored from storage or
    # received via an OAuth redirect).
    # @param session [Basin::Session, nil]
    def set_session(session)
      @mutex.synchronize { @session = session }
    end

    # Return a valid access token, auto-refreshing if within EXPIRY_SKEW_S
    # of expiry.  Returns nil when there is no session (callers fall back to
    # the static key).
    #
    # Thread-safe via a Mutex: if two threads race on an expiring token only
    # one refresh call is made.
    #
    # @return [String, nil]
    def access_token
      @mutex.synchronize do
        s = @session
        return nil if s.nil?

        begin
          expiry = Time.parse(s.access_expires_at).to_f
          if expiry - EXPIRY_SKEW_S <= Time.now.to_f
            s = _refresh_session_locked
          end
        rescue ArgumentError, TypeError
          # Unparseable timestamp — return the token as-is.
        end
        s.access_token
      end
    end

    # --------------------------------------------------------------------------
    # Auth flows
    # --------------------------------------------------------------------------

    # POST /auth/v1/signup → { ok, user_id }
    # @return [Basin::SignUpResult]
    def sign_up(email:, password:, project_id: nil)
      data = @http.request_json("POST", "/auth/v1/signup",
        body: { "project_id" => resolve_project_id(project_id), "email" => email, "password" => password },
        auth: false)
      SignUpResult.from_hash(data)
    end

    # POST /auth/v1/signin → token body; stored as the live session.
    # @return [Basin::Session]
    def sign_in(email:, password:, project_id: nil)
      data = @http.request_json("POST", "/auth/v1/signin",
        body: { "project_id" => resolve_project_id(project_id), "email" => email, "password" => password },
        auth: false)
      session = Session.from_hash(data)
      @mutex.synchronize { @session = session }
      session
    end

    # POST /auth/v1/signout with the stored refresh token.
    #
    # Calls the server to write a revocation row, then clears the local
    # session.  Always safe to call — tolerates no-session, 404, and network
    # errors (local state is cleared in all cases).
    def sign_out
      current = @mutex.synchronize do
        s = @session
        @session = nil
        s
      end
      return if current.nil?

      begin
        @http.request_json("POST", "/auth/v1/signout",
          body: { "refresh_token" => current.refresh_token },
          auth: false)
      rescue Basin::ApiError => e
        raise unless e.status == 404
        # Older server without signout route — tolerate silently.
      rescue Basin::NetworkError
        # Network failure — local clear already done.
      end
    end

    # POST /auth/v1/refresh with the stored refresh token.
    # Rotates both tokens.  Raises E_REVOKED_TOKEN if already rotated.
    # @return [Basin::Session]
    def refresh_session
      @mutex.synchronize { _refresh_session_locked }
    end

    # POST /auth/v1/verify-email → { ok: true }
    def verify_email(token:, project_id: nil)
      @http.request_json("POST", "/auth/v1/verify-email",
        body: { "project_id" => resolve_project_id(project_id), "token" => token },
        auth: false)
    end

    # POST /auth/v1/request-password-reset → { ok: true }
    def request_password_reset(email:, project_id: nil)
      @http.request_json("POST", "/auth/v1/request-password-reset",
        body: { "project_id" => resolve_project_id(project_id), "email" => email },
        auth: false)
    end

    # POST /auth/v1/reset-password → { ok: true }
    def reset_password(token:, new_password:, project_id: nil)
      @http.request_json("POST", "/auth/v1/reset-password",
        body: { "project_id" => resolve_project_id(project_id), "token" => token, "new_password" => new_password },
        auth: false)
    end

    # POST /auth/v1/magic-link — 204 always (never reveals whether address exists).
    def request_magic_link(email)
      @http.request_json("POST", "/auth/v1/magic-link",
        body: { "email" => email },
        auth: false)
    end

    # POST /auth/v1/magic-link/consume → token body; stored as session.
    # @return [Basin::Session]
    def consume_magic_link(token)
      data = @http.request_json("POST", "/auth/v1/magic-link/consume",
        body: { "token" => token },
        auth: false)
      session = Session.from_hash(data)
      @mutex.synchronize { @session = session }
      session
    end

    # --------------------------------------------------------------------------
    # API keys
    # --------------------------------------------------------------------------

    # POST /auth/v1/api-keys (JWT-gated) → issued key incl. one-time secret.
    # @return [Basin::ApiKeyIssued]
    def create_api_key(name)
      data = @http.request_json("POST", "/auth/v1/api-keys", body: { "name" => name })
      ApiKeyIssued.from_hash(data)
    end

    # GET /auth/v1/api-keys (JWT-gated).
    # @return [Array<Basin::ApiKeyDescriptor>]
    def list_api_keys
      data = @http.request_json("GET", "/auth/v1/api-keys")
      Array(data).map { |d| ApiKeyDescriptor.from_hash(d) }
    end

    # DELETE /auth/v1/api-keys/:id (JWT-gated) → { ok: true }.
    def delete_api_key(key_id)
      @http.request_json("DELETE", "/auth/v1/api-keys/#{key_id}")
    end

    # --------------------------------------------------------------------------
    # OAuth
    # --------------------------------------------------------------------------

    # GET /auth/v1/oauth/:provider/authorize → { redirect_url, state }.
    #
    # Returns the provider authorize URL built server-side (with PKCE + signed
    # CSRF state).  Redirect the user's browser to +result.redirect_url+.
    #
    # Supported preset providers: google, github, apple, bitbucket, discord,
    # figma, gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify,
    # twitch, twitter_x.
    #
    # @param provider [String] e.g. "google", "github"
    # @param redirect_to [String] post-flow redirect URL (validated server-side)
    # @return [Basin::OAuthAuthorizeResult]
    def get_oauth_authorize_url(provider, redirect_to: "", project_id: nil)
      pid  = resolve_project_id(project_id)
      data = @http.request_json("GET", "/auth/v1/oauth/#{provider}/authorize",
        query: [["project_id", pid], ["redirect_to", redirect_to]],
        auth: false)
      OAuthAuthorizeResult.from_hash(data)
    end

    # --------------------------------------------------------------------------
    # MFA — TOTP + WebAuthn
    # --------------------------------------------------------------------------

    # POST /auth/v1/factors — begin MFA enrollment (JWT-gated).
    #
    # @param factor_type [String] "totp" or "webauthn"
    # @param friendly_name [String] e.g. "My YubiKey"
    # @return [Basin::TotpEnrollResult, Basin::WebAuthnEnrollResult]
    def enroll_factor(factor_type, friendly_name: "")
      data = @http.request_json("POST", "/auth/v1/factors",
        body: { "factor_type" => factor_type, "friendly_name" => friendly_name })
      if data["factor_type"] == "webauthn"
        WebAuthnEnrollResult.from_hash(data)
      else
        TotpEnrollResult.from_hash(data)
      end
    end

    # GET /auth/v1/factors — list MFA factors for the current user (JWT-gated).
    # @return [Array<Basin::FactorDescriptor>]
    def list_factors
      data = @http.request_json("GET", "/auth/v1/factors")
      Array(data).map { |d| FactorDescriptor.from_hash(d) }
    end

    # POST /auth/v1/factors/:id/verify — confirm enrollment (JWT-gated).
    #
    # Call after enroll_factor to promote the factor from unverified → verified.
    #
    # @param factor_id [String] UUID returned by enroll_factor
    # @param code [String, nil]        TOTP: 6-digit OTP code
    # @param attestation [String, nil] WebAuthn: attestation JSON
    # @param challenge_id [String, nil] WebAuthn: challenge_id from enroll_factor
    # @return [Basin::VerifyFactorResult]
    def verify_factor(factor_id, code: nil, attestation: nil, challenge_id: nil)
      body = {}
      body["code"]         = code         if code
      body["attestation"]  = attestation  if attestation
      body["challenge_id"] = challenge_id if challenge_id
      data = @http.request_json("POST", "/auth/v1/factors/#{factor_id}/verify", body: body)
      VerifyFactorResult.from_hash(data)
    end

    # POST /auth/v1/factors/:id/challenge — begin step-up challenge (JWT-gated).
    #
    # @param factor_id [String] UUID of a verified factor
    # @return [Basin::TotpChallengeResult, Basin::WebAuthnChallengeResult]
    def challenge_factor(factor_id)
      data = @http.request_json("POST", "/auth/v1/factors/#{factor_id}/challenge", body: {})
      if data["request_options_json"]
        WebAuthnChallengeResult.from_hash(data)
      else
        TotpChallengeResult.from_hash(data)
      end
    end

    # POST /auth/v1/factors/:id/challenge/verify → aal2 Session (JWT-gated).
    #
    # On success the new session (carrying aal2 in JWT claims) is stored as
    # the live session.
    #
    # @param factor_id    [String] UUID of the factor being challenged
    # @param challenge_id [String] from challenge_factor
    # @param code      [String, nil] TOTP: 6-digit OTP
    # @param assertion [String, nil] WebAuthn: assertion JSON
    # @return [Basin::Session]
    def verify_challenge(factor_id, challenge_id, code: nil, assertion: nil)
      body = { "challenge_id" => challenge_id }
      body["code"]      = code      if code
      body["assertion"] = assertion if assertion
      data = @http.request_json("POST", "/auth/v1/factors/#{factor_id}/challenge/verify", body: body)
      session = Session.from_hash(data)
      @mutex.synchronize { @session = session }
      session
    end

    # DELETE /auth/v1/factors/:id — unenroll (requires aal2 JWT).
    #
    # The caller must hold an aal2 access token.  E_FORBIDDEN is raised if
    # the session is aal1 only.
    def unenroll_factor(factor_id)
      @http.request_json("DELETE", "/auth/v1/factors/#{factor_id}")
    end

    private

    # Resolve project_id from argument → constructor default → JWT claim.
    def resolve_project_id(project_id = nil)
      pid = project_id || @default_project_id || project_id_from_key
      unless pid
        raise Basin::ApiError.new(
          "E_INVALID_REQUEST",
          "project_id required: pass project_id: or set it in Basin::Client.new",
          0
        )
      end
      pid
    end

    def project_id_from_key
      self.class.project_id_from_jwt(@http.bearer)
    end

    # Must be called with @mutex already held.
    def _refresh_session_locked
      current = @session
      raise Basin::ApiError.new("E_UNAUTHENTICATED", "no session to refresh", 0) if current.nil?

      data = @http.request_json("POST", "/auth/v1/refresh",
        body: { "refresh_token" => current.refresh_token },
        auth: false)
      session = Session.from_hash(data)
      @session = session
      session
    end

    # Decode the project_id claim from a Basin JWT payload segment.
    # Returns nil for non-JWT tokens (e.g. raw API keys).
    # @param token [String, nil]
    # @return [String, nil]
    def self.project_id_from_jwt(token)
      return nil unless token.is_a?(String)
      parts = token.split(".")
      return nil unless parts.length == 3

      b64 = parts[1].tr("-_", "+/")
      b64 += "=" * ((4 - b64.length % 4) % 4)
      payload = JSON.parse(Base64.decode64(b64))
      val = payload["project_id"]
      val.is_a?(String) ? val : nil
    rescue
      nil
    end
  end
end
