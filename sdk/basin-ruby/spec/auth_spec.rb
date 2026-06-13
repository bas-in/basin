# frozen_string_literal: true

require "spec_helper"

RSpec.describe Basin::AuthClient do
  let(:client)     { new_client }
  let(:auth)       { client.auth }
  let(:session_h)  { fake_session }

  # ---------------------------------------------------------------------------
  # sign_up
  # ---------------------------------------------------------------------------
  describe "#sign_up" do
    it "posts to /auth/v1/signup and returns SignUpResult" do
      stub_post("/auth/v1/signup", { "ok" => true, "user_id" => "usr-1" }, status: 201)
      result = auth.sign_up(email: "alice@example.com", password: "secret")
      expect(result).to be_a(Basin::SignUpResult)
      expect(result.ok).to be(true)
      expect(result.user_id).to eq("usr-1")
    end
  end

  # ---------------------------------------------------------------------------
  # sign_in
  # ---------------------------------------------------------------------------
  describe "#sign_in" do
    it "posts to /auth/v1/signin, returns Session, stores it" do
      stub_post("/auth/v1/signin", session_h)
      session = auth.sign_in(email: "alice@example.com", password: "secret")
      expect(session).to be_a(Basin::Session)
      expect(auth.get_session).to eq(session)
    end
  end

  # ---------------------------------------------------------------------------
  # sign_out
  # ---------------------------------------------------------------------------
  describe "#sign_out" do
    it "posts to /auth/v1/signout and clears local session" do
      stub_post("/auth/v1/signin", session_h)
      auth.sign_in(email: "a@b.com", password: "pw")

      stub_post("/auth/v1/signout", { "ok" => true })
      auth.sign_out
      expect(auth.get_session).to be_nil
    end

    it "is a no-op when not signed in" do
      expect { auth.sign_out }.not_to raise_error
    end

    it "tolerates 404 from older servers" do
      stub_post("/auth/v1/signin", session_h)
      auth.sign_in(email: "a@b.com", password: "pw")

      stub_request(:post, "#{BASE_URL}/auth/v1/signout")
        .to_return(status: 404, body: '{"code":"E_NOT_FOUND","message":"no route"}',
                   headers: { "Content-Type" => "application/json" })
      expect { auth.sign_out }.not_to raise_error
      expect(auth.get_session).to be_nil
    end
  end

  # ---------------------------------------------------------------------------
  # refresh_session (auto-refresh)
  # ---------------------------------------------------------------------------
  describe "#access_token auto-refresh" do
    it "refreshes when token is near expiry" do
      near_expiry = (Time.now + 5).utc.iso8601  # 5s < EXPIRY_SKEW_S=10
      expired_session = {
        "access_token"       => "old-at",
        "refresh_token"      => "rt-1",
        "access_expires_at"  => near_expiry,
        "refresh_expires_at" => (Time.now + 86_400).utc.iso8601
      }
      fresh = fake_session(at: "new-at", expires_in: 3600)

      stub_post("/auth/v1/signin", expired_session)
      stub_post("/auth/v1/refresh", fresh)

      auth.sign_in(email: "a@b.com", password: "pw")
      tok = auth.access_token
      expect(tok).to eq("new-at")
    end

    it "returns nil when there is no session" do
      expect(auth.access_token).to be_nil
    end
  end

  # ---------------------------------------------------------------------------
  # API keys
  # ---------------------------------------------------------------------------
  describe "#create_api_key / #list_api_keys / #delete_api_key" do
    it "creates an API key" do
      issued = { "id" => 1, "name" => "ci", "secret" => "sk_xxx", "created_at" => "2026-01-01T00:00:00Z" }
      stub_post("/auth/v1/api-keys", issued)
      result = auth.create_api_key("ci")
      expect(result).to be_a(Basin::ApiKeyIssued)
      expect(result.secret).to eq("sk_xxx")
    end

    it "lists API keys" do
      keys = [{ "id" => 1, "name" => "ci", "created_at" => "2026-01-01T00:00:00Z",
                "last_used_at" => nil, "revoked_at" => nil }]
      stub_get("/auth/v1/api-keys", keys)
      result = auth.list_api_keys
      expect(result).to be_an(Array)
      expect(result.first).to be_a(Basin::ApiKeyDescriptor)
    end

    it "deletes an API key" do
      stub_delete("/auth/v1/api-keys/1", { "ok" => true })
      expect { auth.delete_api_key(1) }.not_to raise_error
    end
  end

  # ---------------------------------------------------------------------------
  # Magic links
  # ---------------------------------------------------------------------------
  describe "#request_magic_link / #consume_magic_link" do
    it "requests a magic link (204 always)" do
      stub_request(:post, "#{BASE_URL}/auth/v1/magic-link")
        .to_return(status: 204, body: "", headers: {})
      expect { auth.request_magic_link("alice@example.com") }.not_to raise_error
    end

    it "consumes a magic link and stores session" do
      stub_post("/auth/v1/magic-link/consume", session_h)
      session = auth.consume_magic_link("tok-abc")
      expect(session).to be_a(Basin::Session)
      expect(auth.get_session).to eq(session)
    end
  end

  # ---------------------------------------------------------------------------
  # OAuth
  # ---------------------------------------------------------------------------
  describe "#get_oauth_authorize_url" do
    it "returns OAuthAuthorizeResult" do
      payload = { "redirect_url" => "https://accounts.google.com/o/oauth2/v2/auth?...", "state" => "csrf-xyz" }
      stub_request(:get, "#{BASE_URL}/auth/v1/oauth/google/authorize")
        .with(query: { "project_id" => FAKE_PROJECT, "redirect_to" => "https://myapp.example.com/cb" })
        .to_return(status: 200, body: JSON.generate(payload), headers: { "Content-Type" => "application/json" })

      result = auth.get_oauth_authorize_url("google", redirect_to: "https://myapp.example.com/cb")
      expect(result).to be_a(Basin::OAuthAuthorizeResult)
      expect(result.redirect_url).to include("accounts.google.com")
      expect(result.state).to eq("csrf-xyz")
    end
  end

  # ---------------------------------------------------------------------------
  # MFA — TOTP
  # ---------------------------------------------------------------------------
  describe "MFA TOTP flow" do
    let(:factor_id) { "factor-uuid-1" }

    it "enrolls a TOTP factor" do
      payload = {
        "factor_id" => factor_id, "factor_type" => "totp",
        "secret_b32" => "JBSWY3DPEHPK3PXP",
        "otpauth_uri" => "otpauth://totp/basin%3Aalice?secret=JBSWY3DPEHPK3PXP"
      }
      stub_post("/auth/v1/factors", payload, status: 201)
      result = auth.enroll_factor("totp", friendly_name: "My App")
      expect(result).to be_a(Basin::TotpEnrollResult)
      expect(result.secret_b32).to eq("JBSWY3DPEHPK3PXP")
    end

    it "verifies a TOTP factor" do
      stub_post("/auth/v1/factors/#{factor_id}/verify",
                { "ok" => true, "recovery_codes" => %w[aaa bbb] })
      result = auth.verify_factor(factor_id, code: "123456")
      expect(result.ok).to be(true)
      expect(result.recovery_codes).to eq(%w[aaa bbb])
    end

    it "lists factors" do
      factors = [{
        "id" => factor_id, "factor_type" => "totp", "status" => "verified",
        "friendly_name" => "My App",
        "created_at" => "2026-01-01T00:00:00Z", "updated_at" => "2026-01-01T00:00:00Z"
      }]
      stub_get("/auth/v1/factors", factors)
      result = auth.list_factors
      expect(result.first).to be_a(Basin::FactorDescriptor)
    end

    it "challenges a TOTP factor" do
      stub_post("/auth/v1/factors/#{factor_id}/challenge", { "challenge_id" => "ch-1" })
      result = auth.challenge_factor(factor_id)
      expect(result).to be_a(Basin::TotpChallengeResult)
      expect(result.challenge_id).to eq("ch-1")
    end

    it "verifies a TOTP challenge → aal2 session" do
      stub_post("/auth/v1/factors/#{factor_id}/challenge/verify", session_h)
      session = auth.verify_challenge(factor_id, "ch-1", code: "654321")
      expect(session).to be_a(Basin::Session)
    end

    it "unenrolls a factor" do
      stub_delete("/auth/v1/factors/#{factor_id}", { "ok" => true })
      expect { auth.unenroll_factor(factor_id) }.not_to raise_error
    end
  end

  # ---------------------------------------------------------------------------
  # MFA — WebAuthn
  # ---------------------------------------------------------------------------
  describe "MFA WebAuthn flow" do
    let(:factor_id) { "wafactor-uuid-2" }

    it "enrolls a WebAuthn factor" do
      payload = {
        "factor_id" => factor_id, "factor_type" => "webauthn",
        "challenge_id" => "ch-wa-1",
        "creation_options_json" => '{"rp":{"name":"basin"}}'
      }
      stub_post("/auth/v1/factors", payload, status: 201)
      result = auth.enroll_factor("webauthn")
      expect(result).to be_a(Basin::WebAuthnEnrollResult)
      expect(result.challenge_id).to eq("ch-wa-1")
    end

    it "challenges a WebAuthn factor" do
      payload = { "challenge_id" => "ch-wa-2", "request_options_json" => '{"allowCredentials":[]}' }
      stub_post("/auth/v1/factors/#{factor_id}/challenge", payload)
      result = auth.challenge_factor(factor_id)
      expect(result).to be_a(Basin::WebAuthnChallengeResult)
      expect(result.request_options_json).not_to be_nil
    end
  end

  # ---------------------------------------------------------------------------
  # JWT project_id extraction
  # ---------------------------------------------------------------------------
  describe ".project_id_from_jwt" do
    it "decodes project_id from a Basin JWT" do
      # Payload: {"project_id":"proj-abc"}
      payload_b64 = Base64.strict_encode64('{"project_id":"proj-abc"}').tr("=", "")
      token = "header.#{payload_b64}.sig"
      expect(Basin::AuthClient.project_id_from_jwt(token)).to eq("proj-abc")
    end

    it "returns nil for a non-JWT string" do
      expect(Basin::AuthClient.project_id_from_jwt("raw-api-key")).to be_nil
    end

    it "returns nil for nil" do
      expect(Basin::AuthClient.project_id_from_jwt(nil)).to be_nil
    end
  end
end
