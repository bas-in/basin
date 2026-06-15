// Thin HTTP client wrapping the basin-cloud /v1/* REST API.
//
// Mirrors `bas-in/basin-cli`'s `client.go` shape (typed APIError envelope,
// Bearer-token injection, JSON marshalling) so the provider's HTTP
// layer reads the same as the CLI's. Stdlib-only: net/http does the
// transport, encoding/json shapes the bodies.
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// APIError is the cloud's typed error envelope.
type APIError struct {
	HTTPStatus int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("HTTP %d (%s)", e.HTTPStatus, e.Code)
	}
	if e.Code == "" {
		return fmt.Sprintf("HTTP %d: %s", e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("HTTP %d (%s): %s", e.HTTPStatus, e.Code, e.Message)
}

// AsAPIError unwraps a generic error into *APIError when possible.
func AsAPIError(err error) (*APIError, bool) {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae, true
	}
	return nil, false
}

// Client is a single base-URL + bearer-token combo.
type Client struct {
	BaseURL   string
	Token     string
	HTTP      *http.Client
	UserAgent string
}

// NewClient binds the base URL and bearer token.
func NewClient(base, token string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(base, "/"),
		Token:   token,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
		},
		UserAgent: "terraform-provider-basin/" + UserAgentVersion,
	}
}

// Do executes an HTTP request, decoding a JSON body into out.
func (c *Client) Do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		if out == nil {
			_, _ = io.Copy(io.Discard, res.Body)
			return nil
		}
		return json.NewDecoder(res.Body).Decode(out)
	}
	b, _ := io.ReadAll(res.Body)
	ae := &APIError{HTTPStatus: res.StatusCode}
	if len(b) > 0 {
		var nested struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(b, &nested); err == nil && (nested.Error.Code != "" || nested.Error.Message != "") {
			ae.Code = nested.Error.Code
			ae.Message = nested.Error.Message
			return ae
		}
		var flat struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(b, &flat); err == nil && (flat.Code != "" || flat.Message != "") {
			ae.Code = flat.Code
			ae.Message = flat.Message
			return ae
		}
		ae.Message = strings.TrimSpace(string(b))
	}
	return ae
}

// IsNotFound returns true for 404 responses.
func IsNotFound(err error) bool {
	if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == http.StatusNotFound {
		return true
	}
	return false
}

// ── Typed wire shapes (mirroring the cloud's /v1/* responses) ─────

// User mirrors models.PublicUser as far as the provider cares.
type User struct {
	ID            string `json:"id,omitempty"`
	Email         string `json:"email,omitempty"`
	Name          string `json:"name,omitempty"`
	EmailVerified bool   `json:"email_verified,omitempty"`
}

// Org is the read shape for /v1/orgs/{slug}.
type Org struct {
	ID           string `json:"id,omitempty"`
	Slug         string `json:"slug,omitempty"`
	Name         string `json:"name,omitempty"`
	Plan         string `json:"plan,omitempty"`
	BillingEmail string `json:"billing_email,omitempty"`
}

// Project is the read shape for /v1/projects/{ref}.
type Project struct {
	ID             string `json:"id,omitempty"`
	OrgID          string `json:"org_id,omitempty"`
	Slug           string `json:"slug,omitempty"`
	Ref            string `json:"ref,omitempty"`
	Name           string `json:"name,omitempty"`
	Region         string `json:"region,omitempty"`
	Status         string `json:"status,omitempty"`
	EngineTenantID string `json:"engine_tenant_id,omitempty"`
	CreatedAt      string `json:"created_at,omitempty"`
}

// Pgwire is the once-only reveal envelope from POST /v1/orgs/:slug/projects.
type Pgwire struct {
	PgwireUser    string `json:"pgwire_user,omitempty"`
	Password      string `json:"password,omitempty"`
	DBName        string `json:"dbname,omitempty"`
	ConnectionURL string `json:"connection_url,omitempty"`
	EngineTenant  string `json:"engine_tenant,omitempty"`
}

// CreateProjectResponse is the create envelope.
type CreateProjectResponse struct {
	Project *Project       `json:"project,omitempty"`
	Keys    map[string]any `json:"keys,omitempty"`
	Pgwire  *Pgwire        `json:"pgwire,omitempty"`
}

// CreateProjectRequest is the body for POST /v1/orgs/:slug/projects.
type CreateProjectRequest struct {
	Name   string `json:"name"`
	Region string `json:"region"`
}

// UpdateProjectRequest is the body for PATCH /v1/projects/:ref.
type UpdateProjectRequest struct {
	Name *string `json:"name,omitempty"`
}

// APIToken mirrors models.APIToken minus the secret material.
type APIToken struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Prefix      string   `json:"prefix,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	ProjectIDs  []string `json:"project_ids,omitempty"`
	IPAllowlist []string `json:"ip_allowlist,omitempty"`
	CreatedAt   string   `json:"created_at,omitempty"`
	ExpiresAt   *string  `json:"expires_at,omitempty"`
}

// CreateTokenRequest is the body for POST /v1/orgs/:slug/api-tokens.
type CreateTokenRequest struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	ProjectIDs  []string `json:"project_ids,omitempty"`
	IPAllowlist []string `json:"ip_allowlist,omitempty"`
	ExpiresAt   *string  `json:"expires_at,omitempty"`
}

// UpdateTokenRequest is the body for PATCH /v1/orgs/:slug/api-tokens/:id.
type UpdateTokenRequest struct {
	Name        *string   `json:"name,omitempty"`
	Description *string   `json:"description,omitempty"`
	Scope       *string   `json:"scope,omitempty"`
	ProjectIDs  *[]string `json:"project_ids,omitempty"`
	IPAllowlist *[]string `json:"ip_allowlist,omitempty"`
	ExpiresAt   *string   `json:"expires_at,omitempty"`
}

// CreateTokenResponse is the once-only reveal envelope.
type CreateTokenResponse struct {
	Token     *APIToken `json:"token,omitempty"`
	FullToken string    `json:"full_token,omitempty"`
}

// Webhook mirrors handlers.webhookResponse (read shape).
type Webhook struct {
	ID        string   `json:"id,omitempty"`
	ProjectID string   `json:"project_id,omitempty"`
	URL       string   `json:"url,omitempty"`
	Events    []string `json:"events,omitempty"`
	Active    bool     `json:"active,omitempty"`
	SecretSet bool     `json:"secret_set,omitempty"`
	CreatedAt string   `json:"created_at,omitempty"`
	UpdatedAt string   `json:"updated_at,omitempty"`
}

// CreateWebhookRequest is the body for POST /v1/projects/:ref/webhooks.
type CreateWebhookRequest struct {
	URL         string   `json:"url"`
	Events      []string `json:"events"`
	SecretPlain string   `json:"secret_plain,omitempty"`
}

// PatchWebhookRequest is the body for PATCH /v1/projects/:ref/webhooks/:id.
type PatchWebhookRequest struct {
	URL    *string   `json:"url,omitempty"`
	Events *[]string `json:"events,omitempty"`
	Active *bool     `json:"active,omitempty"`
}

// CreateWebhookResponse — the create-only envelope returns the
// generated secret in `secret_plain` exactly once.
type CreateWebhookResponse struct {
	Webhook     *Webhook `json:"webhook,omitempty"`
	SecretPlain string   `json:"secret_plain,omitempty"`
}

// CustomDomain mirrors handlers.domainResponse.
type CustomDomain struct {
	ID                string `json:"id,omitempty"`
	ProjectID         string `json:"project_id,omitempty"`
	Domain            string `json:"domain,omitempty"`
	Status            string `json:"status,omitempty"`
	VerificationToken string `json:"verification_token,omitempty"`
	VerificationKind  string `json:"verification_kind,omitempty"`
	TXTRecordName     string `json:"txt_record_name,omitempty"`
	TXTRecordValue    string `json:"txt_record_value,omitempty"`
	CertStatus        string `json:"cert_status,omitempty"`
	CreatedAt         string `json:"created_at,omitempty"`
	UpdatedAt         string `json:"updated_at,omitempty"`
	VerifiedAt        string `json:"verified_at,omitempty"`
}

// CreateCustomDomainRequest is the body for POST .../custom-domains.
type CreateCustomDomainRequest struct {
	Domain string `json:"domain"`
}

// EngineVersion mirrors models.EngineVersion catalog rows.
type EngineVersion struct {
	Version         string  `json:"version,omitempty"`
	Channel         string  `json:"channel,omitempty"`
	ReleasedAt      string  `json:"released_at,omitempty"`
	EOLAt           *string `json:"eol_at,omitempty"`
	ReleaseNotesURL string  `json:"release_notes_url,omitempty"`
	IsRecommended   bool    `json:"is_recommended,omitempty"`
	Notes           string  `json:"notes,omitempty"`
}

// listEnvelope is the common `{"items":[…]}` shape used by index
// endpoints (orgs/projects, projects/webhooks, projects/custom-domains).
type listEnvelope[T any] struct {
	Items []T `json:"items,omitempty"`
}
