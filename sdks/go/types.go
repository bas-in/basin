package basin

import "encoding/json"

// ---------------------------------------------------------------------------
// Auth types
// ---------------------------------------------------------------------------

// Session holds the token pair returned by /auth/v1/signin, /auth/v1/refresh,
// and /auth/v1/magic-link/consume (crates/basin-rest/src/routes/auth.rs
// token_body). JSON field names match the Rust serde structs exactly.
type Session struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	AccessExpiresAt  string `json:"access_expires_at"`  // RFC 3339
	RefreshExpiresAt string `json:"refresh_expires_at"` // RFC 3339
}

// SignUpResult is the response from POST /auth/v1/signup.
type SignUpResult struct {
	OK     bool   `json:"ok"`
	UserID string `json:"user_id"`
}

// ApiKeyIssued is the response from POST /auth/v1/api-keys.
// The secret is shown exactly once; store it immediately.
type ApiKeyIssued struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Secret    string `json:"secret"`
	CreatedAt string `json:"created_at"`
}

// ApiKeyDescriptor is one element of the GET /auth/v1/api-keys array.
type ApiKeyDescriptor struct {
	ID         int     `json:"id"`
	Name       string  `json:"name"`
	CreatedAt  string  `json:"created_at"`
	LastUsedAt *string `json:"last_used_at"`
	RevokedAt  *string `json:"revoked_at"`
}

// OAuthAuthorizeResult is the response from GET /auth/v1/oauth/:provider/authorize.
type OAuthAuthorizeResult struct {
	// RedirectURL is the full provider authorize URL — redirect the browser here.
	RedirectURL string `json:"redirect_url"`
	// State is the CSRF state value included in RedirectURL.
	State string `json:"state"`
}

// ---------------------------------------------------------------------------
// MFA types (crates/basin-rest/src/routes/auth.rs enroll_factor / factors*)
// ---------------------------------------------------------------------------

// TotpEnrollResult is returned by POST /auth/v1/factors for factor_type="totp".
type TotpEnrollResult struct {
	FactorID   string `json:"factor_id"`
	FactorType string `json:"factor_type"` // "totp"
	// SecretB32 is the base-32 TOTP secret for QR code display.
	SecretB32  string `json:"secret_b32"`
	// OtpauthURI is the otpauth://totp/... URI for QR display.
	OtpauthURI string `json:"otpauth_uri"`
}

// WebAuthnEnrollResult is returned by POST /auth/v1/factors for factor_type="webauthn".
type WebAuthnEnrollResult struct {
	FactorID   string `json:"factor_id"`
	FactorType string `json:"factor_type"` // "webauthn"
	ChallengeID string `json:"challenge_id"`
	// CreationOptionsJSON is the serialised PublicKeyCredentialCreationOptions
	// JSON to pass to navigator.credentials.create().
	CreationOptionsJSON json.RawMessage `json:"creation_options_json"`
}

// FactorDescriptor is one element of the GET /auth/v1/factors array.
// Mirrors FactorDescriptor in crates/basin-auth/src/mfa.rs.
type FactorDescriptor struct {
	ID           string `json:"id"`
	FactorType   string `json:"factor_type"` // "totp" | "webauthn"
	Status       string `json:"status"`       // "unverified" | "verified"
	FriendlyName string `json:"friendly_name"`
	CreatedAt    string `json:"created_at"` // RFC 3339
	UpdatedAt    string `json:"updated_at"` // RFC 3339
}

// VerifyFactorResult is the response from POST /auth/v1/factors/:id/verify.
// RecoveryCodes is only present on the first verified factor — store securely.
type VerifyFactorResult struct {
	OK            bool     `json:"ok"`
	RecoveryCodes []string `json:"recovery_codes"` // nil when absent
}

// TotpChallengeResult is returned by POST /auth/v1/factors/:id/challenge for
// TOTP factors.
type TotpChallengeResult struct {
	ChallengeID string `json:"challenge_id"`
}

// WebAuthnChallengeResult is returned by POST /auth/v1/factors/:id/challenge
// for WebAuthn factors.
type WebAuthnChallengeResult struct {
	ChallengeID string `json:"challenge_id"`
	// RequestOptionsJSON is the serialised PublicKeyCredentialRequestOptions
	// JSON to pass to navigator.credentials.get(). Nil for TOTP factors.
	RequestOptionsJSON json.RawMessage `json:"request_options_json"`
}

// ---------------------------------------------------------------------------
// Query / data types
// ---------------------------------------------------------------------------

// Row is a single result row as a generic map.
type Row = map[string]any

// ExecResult is the non-row result shape for writes: { ok, tag }.
type ExecResult struct {
	OK  bool   `json:"ok"`
	Tag string `json:"tag"`
}

// QueryResult holds the normalised output of a GET request.
// When the server returns a plain JSON array, NextCursor is empty.
// When the server returns { rows, next_cursor }, NextCursor may be populated.
type QueryResult struct {
	Rows       []Row
	NextCursor string
}

// Into decodes the rows into dest via a JSON round-trip.
// dest must be a pointer to a slice of a struct type.
//
//	var orders []Order
//	err := result.Into(&orders)
func (r *QueryResult) Into(dest any) error {
	b, err := json.Marshal(r.Rows)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dest)
}

// ---------------------------------------------------------------------------
// Storage types (crates/basin-rest/src/routes/storage.rs)
// ---------------------------------------------------------------------------

// Bucket is the metadata object for a storage bucket.
type Bucket struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Public           bool     `json:"public"`
	FileSizeLimit    *int64   `json:"file_size_limit"`
	AllowedMimeTypes []string `json:"allowed_mime_types"`
	CreatedAt        string   `json:"created_at"`
	UpdatedAt        string   `json:"updated_at"`
}

// StorageObject is the metadata for an uploaded object.
type StorageObject struct {
	ID        string `json:"id"`
	BucketID  string `json:"bucket_id"`
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	MimeType  string `json:"mime_type"`
	Metadata  any    `json:"metadata"`
	Owner     string `json:"owner"`
	ETag      string `json:"etag"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// SignedURL is the response from POST /storage/v1/object/sign/upload/:bucket/*path.
// AbsoluteURL is the full URL (base + SignedURL).
type SignedURL struct {
	// SignedURL is the server-relative signed path.
	SignedURL string `json:"signedUrl"`
	// ExpiresAt is the RFC 3339 expiry time.
	ExpiresAt string `json:"expiresAt"`
	// AbsoluteURL is the full URL, built by the SDK from base + SignedURL.
	AbsoluteURL string `json:"-"`
}

// DownloadResult holds raw downloaded bytes with optional content-type/etag.
type DownloadResult struct {
	Data        []byte
	ContentType string
	ETag        string
}

// FunctionInvokeResult is the proxied response from ANY /fn/v1/:name.
// The function's status, headers, and body are returned verbatim.
type FunctionInvokeResult struct {
	Status  int
	Headers map[string]string
	Data    any
}
