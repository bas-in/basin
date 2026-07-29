// Shared mock httptest.Server used by every resource_*_test.go.
// Lives in its own package so the resource_* test files can import
// it. The mock implements just enough of the v1 surface to drive a
// resource lifecycle end-to-end.
package mockcloud

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"
)

type Org = apiclient.Org
type Project = apiclient.Project
type APIToken = apiclient.APIToken
type Webhook = apiclient.Webhook
type CustomDomain = apiclient.CustomDomain
type EngineVersion = apiclient.EngineVersion
type User = apiclient.User
type CreateProjectRequest = apiclient.CreateProjectRequest
type UpdateProjectRequest = apiclient.UpdateProjectRequest
type CreateProjectResponse = apiclient.CreateProjectResponse
type Pgwire = apiclient.Pgwire
type CreateTokenRequest = apiclient.CreateTokenRequest
type UpdateTokenRequest = apiclient.UpdateTokenRequest
type CreateTokenResponse = apiclient.CreateTokenResponse
type CreateWebhookRequest = apiclient.CreateWebhookRequest
type PatchWebhookRequest = apiclient.PatchWebhookRequest
type CreateWebhookResponse = apiclient.CreateWebhookResponse
type CreateCustomDomainRequest = apiclient.CreateCustomDomainRequest

// MockServer is a single-process in-memory cloud impersonator. The
// resource_*_test.go files mount their own handlers onto its mux;
// MockServer is just a tiny convenience wrapper around httptest +
// chi-style param parsing.
type MockServer struct {
	*httptest.Server
	mu sync.Mutex

	Orgs          map[string]*Org
	Projects      map[string]*Project // keyed by ref
	Tokens        map[string]*APIToken
	Webhooks      map[string]*Webhook
	Domains       map[string]*CustomDomain
	EngineVersion []EngineVersion

	// Handler is the test's custom routing function. The default
	// MockServer factory wires a router that covers the v1 surface
	// touched by the resources; tests can override individual routes
	// by replacing Handler.
	Handler http.HandlerFunc
}

// NewMockServer wires the default router covering /v1/me + the four
// resources' CRUD plus the two read-only data sources. Tests can
// pre-populate Orgs / Projects etc. before issuing requests.
func NewMockServer() *MockServer {
	m := &MockServer{
		Orgs:     map[string]*Org{},
		Projects: map[string]*Project{},
		Tokens:   map[string]*APIToken{},
		Webhooks: map[string]*Webhook{},
		Domains:  map[string]*CustomDomain{},
		EngineVersion: []EngineVersion{
			{Version: "0.1.0", Channel: "stable", ReleasedAt: "2026-04-01T00:00:00Z", IsRecommended: true},
		},
	}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.Handler != nil {
			m.Handler(w, r)
			return
		}
		m.defaultRouter(w, r)
	}))
	return m
}

func (m *MockServer) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (m *MockServer) writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": code, "message": msg},
	})
}

// defaultRouter is intentionally tiny — we only handle the routes the
// provider hits. Path matching is by prefix-and-split so we don't need
// chi.
func (m *MockServer) defaultRouter(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if r.Header.Get("Authorization") == "" {
		m.writeErr(w, 401, "unauthorized", "missing bearer")
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	// /v1/me
	if r.Method == "GET" && len(parts) == 2 && parts[0] == "v1" && parts[1] == "me" {
		m.writeJSON(w, 200, &User{ID: "01H_USER", Email: "tf@example.com"})
		return
	}

	// /v1/orgs/{slug}
	if r.Method == "GET" && len(parts) == 3 && parts[0] == "v1" && parts[1] == "orgs" {
		o, ok := m.Orgs[parts[2]]
		if !ok {
			m.writeErr(w, 404, "not_found", "org not found")
			return
		}
		m.writeJSON(w, 200, o)
		return
	}

	// /v1/orgs/{slug}/projects (POST)
	if r.Method == "POST" && len(parts) == 4 && parts[0] == "v1" && parts[1] == "orgs" && parts[3] == "projects" {
		var body CreateProjectRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		ref := body.Name + "-1a2b3c4d"
		o := m.Orgs[parts[2]]
		if o == nil {
			m.writeErr(w, 404, "not_found", "org not found")
			return
		}
		p := &Project{
			ID: "01H_PROJ_" + body.Name, OrgID: o.ID, Name: body.Name, Region: body.Region,
			Ref: ref, Status: "active", EngineTenantID: "tenant_" + ref, CreatedAt: "2026-05-10T00:00:00Z",
		}
		m.Projects[ref] = p
		m.writeJSON(w, 201, &CreateProjectResponse{
			Project: p,
			Pgwire:  &Pgwire{ConnectionURL: "postgresql://u:pw@host/" + ref, EngineTenant: ref},
		})
		return
	}

	// /v1/projects/{ref}
	if len(parts) == 3 && parts[0] == "v1" && parts[1] == "projects" {
		ref := parts[2]
		switch r.Method {
		case "GET":
			p, ok := m.Projects[ref]
			if !ok {
				m.writeErr(w, 404, "not_found", "project not found")
				return
			}
			m.writeJSON(w, 200, p)
			return
		case "PATCH":
			p, ok := m.Projects[ref]
			if !ok {
				m.writeErr(w, 404, "not_found", "project not found")
				return
			}
			var body UpdateProjectRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Name != nil {
				p.Name = *body.Name
			}
			m.writeJSON(w, 200, p)
			return
		case "DELETE":
			delete(m.Projects, ref)
			w.WriteHeader(204)
			return
		}
	}

	// /v1/orgs/{slug}/api-tokens (POST)
	if r.Method == "POST" && len(parts) == 4 && parts[0] == "v1" && parts[1] == "orgs" && parts[3] == "api-tokens" {
		var body CreateTokenRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		t := &APIToken{
			ID: "tok_" + body.Name, Name: body.Name, Description: body.Description,
			Prefix: "bso_org_abc", Scope: body.Scope,
			ProjectIDs: body.ProjectIDs, IPAllowlist: body.IPAllowlist,
			ExpiresAt: body.ExpiresAt, CreatedAt: "2026-05-10T00:00:00Z",
		}
		m.Tokens[t.ID] = t
		m.writeJSON(w, 201, &CreateTokenResponse{Token: t, FullToken: "bso_org_abc_full_" + t.ID})
		return
	}

	// /v1/orgs/{slug}/api-tokens/{id}
	if len(parts) == 5 && parts[0] == "v1" && parts[1] == "orgs" && parts[3] == "api-tokens" {
		id := parts[4]
		switch r.Method {
		case "GET":
			t, ok := m.Tokens[id]
			if !ok {
				m.writeErr(w, 404, "not_found", "token not found")
				return
			}
			m.writeJSON(w, 200, t)
			return
		case "PATCH":
			t, ok := m.Tokens[id]
			if !ok {
				m.writeErr(w, 404, "not_found", "token not found")
				return
			}
			var body UpdateTokenRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Name != nil {
				t.Name = *body.Name
			}
			if body.Description != nil {
				t.Description = *body.Description
			}
			if body.Scope != nil {
				t.Scope = *body.Scope
			}
			if body.ProjectIDs != nil {
				t.ProjectIDs = *body.ProjectIDs
			}
			if body.IPAllowlist != nil {
				t.IPAllowlist = *body.IPAllowlist
			}
			if body.ExpiresAt != nil {
				v := *body.ExpiresAt
				t.ExpiresAt = &v
			}
			m.writeJSON(w, 200, t)
			return
		case "DELETE":
			delete(m.Tokens, id)
			w.WriteHeader(204)
			return
		}
	}

	// /v1/projects/{ref}/webhooks (POST)
	if r.Method == "POST" && len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "webhooks" {
		var body CreateWebhookRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		w2 := &Webhook{
			ID: "wh_" + sanitize(body.URL), ProjectID: "01H_PROJ", URL: body.URL, Events: body.Events,
			Active: true, SecretSet: true, CreatedAt: "2026-05-10T00:00:00Z", UpdatedAt: "2026-05-10T00:00:00Z",
		}
		m.Webhooks[w2.ID] = w2
		m.writeJSON(w, 201, &CreateWebhookResponse{Webhook: w2, SecretPlain: "whsec_" + w2.ID})
		return
	}

	// /v1/projects/{ref}/webhooks/{id}
	if len(parts) == 5 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "webhooks" {
		id := parts[4]
		switch r.Method {
		case "GET":
			w2, ok := m.Webhooks[id]
			if !ok {
				m.writeErr(w, 404, "not_found", "webhook not found")
				return
			}
			m.writeJSON(w, 200, w2)
			return
		case "PATCH":
			w2, ok := m.Webhooks[id]
			if !ok {
				m.writeErr(w, 404, "not_found", "webhook not found")
				return
			}
			var body PatchWebhookRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.URL != nil {
				w2.URL = *body.URL
			}
			if body.Events != nil {
				w2.Events = *body.Events
			}
			if body.Active != nil {
				w2.Active = *body.Active
			}
			m.writeJSON(w, 200, w2)
			return
		case "DELETE":
			delete(m.Webhooks, id)
			w.WriteHeader(204)
			return
		}
	}

	// /v1/projects/{ref}/custom-domains (POST)
	if r.Method == "POST" && len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "custom-domains" {
		var body CreateCustomDomainRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		d := &CustomDomain{
			ID: "dom_" + sanitize(body.Domain), ProjectID: "01H_PROJ", Domain: body.Domain,
			Status: "pending", VerificationToken: "verify_" + body.Domain,
			VerificationKind: "txt", TXTRecordName: "_basin-verify." + body.Domain,
			TXTRecordValue: "basin-verify=verify_" + body.Domain,
			CertStatus:     "none", CreatedAt: "2026-05-10T00:00:00Z", UpdatedAt: "2026-05-10T00:00:00Z",
		}
		m.Domains[d.ID] = d
		m.writeJSON(w, 201, d)
		return
	}

	// /v1/projects/{ref}/custom-domains/{id}
	if len(parts) == 5 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "custom-domains" {
		id := parts[4]
		switch r.Method {
		case "GET":
			d, ok := m.Domains[id]
			if !ok {
				m.writeErr(w, 404, "not_found", "domain not found")
				return
			}
			m.writeJSON(w, 200, d)
			return
		case "DELETE":
			delete(m.Domains, id)
			w.WriteHeader(204)
			return
		}
	}

	// /v1/admin/engine-versions
	if r.Method == "GET" && r.URL.Path == "/v1/admin/engine-versions" {
		m.writeJSON(w, 200, map[string]any{"items": m.EngineVersion})
		return
	}

	m.writeErr(w, 404, "not_found", "no route: "+r.Method+" "+r.URL.Path)
}

// sanitize strips characters that would break a path-segment ID.
func sanitize(s string) string {
	r := strings.NewReplacer("/", "_", ":", "_", "?", "_", "&", "_", "=", "_", " ", "_", ".", "-")
	return r.Replace(s)
}
