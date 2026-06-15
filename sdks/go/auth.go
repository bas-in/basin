package basin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// expirySkew is the number of seconds before a token's stated expiry at which
// the SDK will proactively refresh the session.
const expirySkew = 10 * time.Second

// AuthClient provides bindings for /auth/v1/* routes.
//
// Route sources (verified against crates/basin-rest/src/server.rs):
//
//	POST /auth/v1/signup                          server.rs:250
//	POST /auth/v1/signin                          server.rs:251
//	POST /auth/v1/refresh                         server.rs:252
//	POST /auth/v1/verify-email                    server.rs:253
//	POST /auth/v1/reset-password                  server.rs:254
//	POST /auth/v1/request-password-reset          server.rs:255
//	POST /auth/v1/magic-link                      server.rs:262
//	POST /auth/v1/magic-link/consume              server.rs:263
//	POST|GET /auth/v1/api-keys                    server.rs:267
//	DELETE /auth/v1/api-keys/:id                  server.rs:271
//	GET /auth/v1/oauth/:provider/authorize        server.rs:277
//	POST|GET /auth/v1/factors                     server.rs:286-287
//	POST /auth/v1/factors/:id/verify              server.rs:290
//	POST /auth/v1/factors/:id/challenge           server.rs:294
//	POST /auth/v1/factors/:id/challenge/verify    server.rs:298
//	DELETE /auth/v1/factors/:id                   server.rs:302
//
// NOTE: There is NO /auth/v1/signout route server-side (verified against
// crates/basin-rest/src/server.rs). SignOut clears the local session only.
// Token revocation happens via refresh-token rotation on the server.
//
// Auth is per-project: SignUp/SignIn take a ProjectID in the request body.
type AuthClient struct {
	t                *transport
	defaultProjectID string

	mu      sync.Mutex
	session *Session
}

func newAuthClient(t *transport, projectID string) *AuthClient {
	return &AuthClient{t: t, defaultProjectID: projectID}
}

// GetSession returns the current session, or nil when signed out.
func (a *AuthClient) GetSession() *Session {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.session
}

// SetSession adopts an externally obtained token pair (e.g. restored from
// persistent storage across process restarts).
func (a *AuthClient) SetSession(s *Session) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.session = s
}

// AccessToken returns a valid access token, auto-refreshing if the session is
// near expiry (within expirySkew). Returns "" when there is no session.
// This method is called by the transport on every request.
func (a *AuthClient) AccessToken(ctx context.Context) (string, error) {
	a.mu.Lock()
	s := a.session
	a.mu.Unlock()

	if s == nil {
		return "", nil
	}

	expiry, err := time.Parse(time.RFC3339, s.AccessExpiresAt)
	if err == nil && time.Now().Add(expirySkew).After(expiry) {
		refreshed, rerr := a.refreshSessionLocked(ctx, s.RefreshToken)
		if rerr != nil {
			return "", rerr
		}
		return refreshed.AccessToken, nil
	}
	return s.AccessToken, nil
}

func (a *AuthClient) resolveProjectID(projectID string) (string, error) {
	if projectID != "" {
		return projectID, nil
	}
	if a.defaultProjectID != "" {
		return a.defaultProjectID, nil
	}
	// Try to extract from the transport's static key (JWT bearer).
	if pid := projectIDFromJWT(a.t.staticKey); pid != "" {
		return pid, nil
	}
	return "", newBasinError("E_INVALID_REQUEST",
		"project id required: pass WithProjectID or use a JWT that carries a project_id claim", 0)
}

// SignUp registers a new user account.
// POST /auth/v1/signup → { ok, user_id } (201).
func (a *AuthClient) SignUp(ctx context.Context, email, password, projectID string) (*SignUpResult, error) {
	pid, err := a.resolveProjectID(projectID)
	if err != nil {
		return nil, err
	}
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/signup", nil, map[string]any{
		"project_id": pid,
		"email":      email,
		"password":   password,
	}, false)
	if err != nil {
		return nil, err
	}
	var result SignUpResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("basin: decode signup: %w", err)
	}
	return &result, nil
}

// SignIn authenticates with email and password, stores the session, and
// returns it. POST /auth/v1/signin → token body.
func (a *AuthClient) SignIn(ctx context.Context, email, password, projectID string) (*Session, error) {
	pid, err := a.resolveProjectID(projectID)
	if err != nil {
		return nil, err
	}
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/signin", nil, map[string]any{
		"project_id": pid,
		"email":      email,
		"password":   password,
	}, false)
	if err != nil {
		return nil, err
	}
	var session Session
	if err := json.Unmarshal(raw, &session); err != nil {
		return nil, fmt.Errorf("basin: decode session: %w", err)
	}
	a.mu.Lock()
	a.session = &session
	a.mu.Unlock()
	return &session, nil
}

// SignOut clears the local session.
//
// The server has no /auth/v1/signout route (verified against
// crates/basin-rest/src/server.rs). This method is local-only by design.
// Token revocation happens server-side via refresh-token rotation.
func (a *AuthClient) SignOut(_ context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.session = nil
}

// RefreshSession rotates both tokens using the stored refresh token.
// POST /auth/v1/refresh → token body.
func (a *AuthClient) RefreshSession(ctx context.Context) (*Session, error) {
	a.mu.Lock()
	s := a.session
	a.mu.Unlock()

	if s == nil {
		return nil, newBasinError("E_UNAUTHENTICATED", "no session to refresh", 0)
	}
	return a.refreshSessionLocked(ctx, s.RefreshToken)
}

// refreshSessionLocked performs the actual refresh call and stores the result.
// The mutex is NOT held during the HTTP call; it is acquired only to swap the
// session pointer, preventing a deadlock if AccessToken triggers a refresh
// during an in-flight request.
func (a *AuthClient) refreshSessionLocked(ctx context.Context, refreshToken string) (*Session, error) {
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/refresh", nil, map[string]any{
		"refresh_token": refreshToken,
	}, false)
	if err != nil {
		return nil, err
	}
	var session Session
	if err := json.Unmarshal(raw, &session); err != nil {
		return nil, fmt.Errorf("basin: decode session: %w", err)
	}
	a.mu.Lock()
	a.session = &session
	a.mu.Unlock()
	return &session, nil
}

// VerifyEmail confirms an email verification token.
// POST /auth/v1/verify-email → { ok: true }.
func (a *AuthClient) VerifyEmail(ctx context.Context, token, projectID string) error {
	pid, err := a.resolveProjectID(projectID)
	if err != nil {
		return err
	}
	_, _, err = a.t.doJSON(ctx, "POST", "/auth/v1/verify-email", nil, map[string]any{
		"project_id": pid,
		"token":      token,
	}, false)
	return err
}

// RequestPasswordReset sends a password-reset email.
// POST /auth/v1/request-password-reset → { ok: true }.
func (a *AuthClient) RequestPasswordReset(ctx context.Context, email, projectID string) error {
	pid, err := a.resolveProjectID(projectID)
	if err != nil {
		return err
	}
	_, _, err = a.t.doJSON(ctx, "POST", "/auth/v1/request-password-reset", nil, map[string]any{
		"project_id": pid,
		"email":      email,
	}, false)
	return err
}

// ResetPassword completes a password-reset flow.
// POST /auth/v1/reset-password → { ok: true }.
func (a *AuthClient) ResetPassword(ctx context.Context, token, newPassword, projectID string) error {
	pid, err := a.resolveProjectID(projectID)
	if err != nil {
		return err
	}
	_, _, err = a.t.doJSON(ctx, "POST", "/auth/v1/reset-password", nil, map[string]any{
		"project_id":   pid,
		"token":        token,
		"new_password": newPassword,
	}, false)
	return err
}

// RequestMagicLink sends a magic-link login email.
// POST /auth/v1/magic-link → 204 (never confirms address).
func (a *AuthClient) RequestMagicLink(ctx context.Context, email string) error {
	_, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/magic-link", nil, map[string]any{
		"email": email,
	}, false)
	return err
}

// ConsumeMagicLink exchanges a magic-link token for a session.
// POST /auth/v1/magic-link/consume → token body; stored as session.
func (a *AuthClient) ConsumeMagicLink(ctx context.Context, token string) (*Session, error) {
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/magic-link/consume", nil, map[string]any{
		"token": token,
	}, false)
	if err != nil {
		return nil, err
	}
	var session Session
	if err := json.Unmarshal(raw, &session); err != nil {
		return nil, fmt.Errorf("basin: decode session: %w", err)
	}
	a.mu.Lock()
	a.session = &session
	a.mu.Unlock()
	return &session, nil
}

// CreateAPIKey creates a new API key (JWT-gated).
// POST /auth/v1/api-keys → issued key including one-time secret.
func (a *AuthClient) CreateAPIKey(ctx context.Context, name string) (*ApiKeyIssued, error) {
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/api-keys", nil, map[string]any{
		"name": name,
	}, true)
	if err != nil {
		return nil, err
	}
	var result ApiKeyIssued
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("basin: decode api key: %w", err)
	}
	return &result, nil
}

// ListAPIKeys returns all API keys for the current user (JWT-gated).
// GET /auth/v1/api-keys.
func (a *AuthClient) ListAPIKeys(ctx context.Context) ([]ApiKeyDescriptor, error) {
	raw, _, err := a.t.doJSON(ctx, "GET", "/auth/v1/api-keys", nil, nil, true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return []ApiKeyDescriptor{}, nil
	}
	var keys []ApiKeyDescriptor
	if err := json.Unmarshal(raw, &keys); err != nil {
		return nil, fmt.Errorf("basin: decode api keys: %w", err)
	}
	return keys, nil
}

// DeleteAPIKey revokes an API key (JWT-gated).
// DELETE /auth/v1/api-keys/:id → { ok: true }.
func (a *AuthClient) DeleteAPIKey(ctx context.Context, keyID int) error {
	_, _, err := a.t.doJSON(ctx, "DELETE", fmt.Sprintf("/auth/v1/api-keys/%d", keyID), nil, nil, true)
	return err
}

// ---------------------------------------------------------------------------
// OAuth
// ---------------------------------------------------------------------------

// GetOAuthAuthorizeURL builds the provider authorize URL server-side and
// returns it. The SDK cannot perform the browser redirect itself — redirect
// the user's browser to result.RedirectURL.
//
// GET /auth/v1/oauth/:provider/authorize → { redirect_url, state }.
//
// After the OAuth flow completes, the server's callback handler
// (GET /auth/v1/oauth/:provider/callback) issues Basin JWT + refresh tokens.
// Supported preset providers: google, github, apple, bitbucket, discord,
// figma, gitlab, linkedin, microsoft, notion, slack, spotify, twitch,
// twitter_x. Custom OIDC providers are registered server-side.
func (a *AuthClient) GetOAuthAuthorizeURL(ctx context.Context, provider, redirectTo, projectID string) (*OAuthAuthorizeResult, error) {
	pid, err := a.resolveProjectID(projectID)
	if err != nil {
		return nil, err
	}
	raw, _, err := a.t.doJSON(ctx, "GET", "/auth/v1/oauth/"+provider+"/authorize",
		[]queryParam{{"project_id", pid}, {"redirect_to", redirectTo}}, nil, false)
	if err != nil {
		return nil, err
	}
	var result OAuthAuthorizeResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("basin: decode oauth authorize: %w", err)
	}
	return &result, nil
}

// ---------------------------------------------------------------------------
// MFA — factor lifecycle
// ---------------------------------------------------------------------------

// EnrollFactor begins MFA factor enrollment (JWT-gated).
// POST /auth/v1/factors.
//
// factorType is "totp" or "webauthn". friendlyName is optional.
//
// Returns *TotpEnrollResult for TOTP or *WebAuthnEnrollResult for WebAuthn.
// The factor is "unverified" until VerifyFactor succeeds.
func (a *AuthClient) EnrollFactor(ctx context.Context, factorType, friendlyName string) (any, error) {
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/factors", nil, map[string]any{
		"factor_type":   factorType,
		"friendly_name": friendlyName,
	}, true)
	if err != nil {
		return nil, err
	}

	// Peek at factor_type to decide which struct to decode into.
	var probe struct {
		FactorType string `json:"factor_type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("basin: decode factor type: %w", err)
	}

	if probe.FactorType == "webauthn" {
		var result WebAuthnEnrollResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, fmt.Errorf("basin: decode webauthn enroll: %w", err)
		}
		return &result, nil
	}
	var result TotpEnrollResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("basin: decode totp enroll: %w", err)
	}
	return &result, nil
}

// ListFactors returns the MFA factors for the current user (JWT-gated).
// GET /auth/v1/factors.
func (a *AuthClient) ListFactors(ctx context.Context) ([]FactorDescriptor, error) {
	raw, _, err := a.t.doJSON(ctx, "GET", "/auth/v1/factors", nil, nil, true)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return []FactorDescriptor{}, nil
	}
	var factors []FactorDescriptor
	if err := json.Unmarshal(raw, &factors); err != nil {
		return nil, fmt.Errorf("basin: decode factors: %w", err)
	}
	return factors, nil
}

// VerifyFactor confirms factor enrollment (JWT-gated).
// POST /auth/v1/factors/:id/verify.
//
// For TOTP: pass code (6-digit OTP). For WebAuthn: pass attestation and
// challengeID. RecoveryCodes in the result is only populated on the first
// verified factor — store them securely, shown exactly once.
func (a *AuthClient) VerifyFactor(ctx context.Context, factorID, code, attestation, challengeID string) (*VerifyFactorResult, error) {
	body := map[string]any{}
	if code != "" {
		body["code"] = code
	}
	if attestation != "" {
		body["attestation"] = attestation
	}
	if challengeID != "" {
		body["challenge_id"] = challengeID
	}
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/factors/"+factorID+"/verify", nil, body, true)
	if err != nil {
		return nil, err
	}
	var result VerifyFactorResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("basin: decode verify factor: %w", err)
	}
	return &result, nil
}

// ChallengeFactor begins a step-up challenge (JWT-gated).
// POST /auth/v1/factors/:id/challenge.
//
// Returns *TotpChallengeResult for TOTP or *WebAuthnChallengeResult for
// WebAuthn. The server auto-detects factor type.
func (a *AuthClient) ChallengeFactor(ctx context.Context, factorID string) (any, error) {
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/factors/"+factorID+"/challenge", nil, map[string]any{}, true)
	if err != nil {
		return nil, err
	}

	// Peek at request_options_json to discriminate WebAuthn from TOTP.
	var probe struct {
		RequestOptionsJSON json.RawMessage `json:"request_options_json"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("basin: decode challenge type: %w", err)
	}

	if probe.RequestOptionsJSON != nil {
		var result WebAuthnChallengeResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, fmt.Errorf("basin: decode webauthn challenge: %w", err)
		}
		return &result, nil
	}
	var result TotpChallengeResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("basin: decode totp challenge: %w", err)
	}
	return &result, nil
}

// VerifyChallenge completes a step-up challenge and returns an aal2 Session.
// POST /auth/v1/factors/:id/challenge/verify.
//
// The returned session carries aal2 in its JWT claims.
func (a *AuthClient) VerifyChallenge(ctx context.Context, factorID, challengeID, code, assertion string) (*Session, error) {
	body := map[string]any{"challenge_id": challengeID}
	if code != "" {
		body["code"] = code
	}
	if assertion != "" {
		body["assertion"] = assertion
	}
	raw, _, err := a.t.doJSON(ctx, "POST", "/auth/v1/factors/"+factorID+"/challenge/verify", nil, body, true)
	if err != nil {
		return nil, err
	}
	var session Session
	if err := json.Unmarshal(raw, &session); err != nil {
		return nil, fmt.Errorf("basin: decode session: %w", err)
	}
	a.mu.Lock()
	a.session = &session
	a.mu.Unlock()
	return &session, nil
}

// UnenrollFactor removes an MFA factor (JWT-gated; requires aal2 access token).
// DELETE /auth/v1/factors/:id.
func (a *AuthClient) UnenrollFactor(ctx context.Context, factorID string) error {
	_, _, err := a.t.doJSON(ctx, "DELETE", "/auth/v1/factors/"+factorID, nil, nil, true)
	return err
}

// ---------------------------------------------------------------------------
// JWT helper
// ---------------------------------------------------------------------------

// projectIDFromJWT decodes the project_id claim from a Basin JWT payload.
// Returns "" for non-JWT tokens (e.g. raw API keys).
func projectIDFromJWT(token string) string {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		return ""
	}
	b64 := parts[1]
	// Convert URL-safe base64 to standard; add padding.
	b64 = strings.ReplaceAll(b64, "-", "+")
	b64 = strings.ReplaceAll(b64, "_", "/")
	switch len(b64) % 4 {
	case 2:
		b64 += "=="
	case 3:
		b64 += "="
	}
	payload, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return ""
	}
	var claims struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return claims.ProjectID
}
