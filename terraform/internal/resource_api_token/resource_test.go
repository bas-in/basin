// Lifecycle + narrowing-rule tests for basin_api_token.
package resource_api_token

import (
	"context"
	"testing"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"
	"github.com/bas-in/terraform-provider-basin/internal/mockcloud"
)

func TestTokenLifecycle(t *testing.T) {
	srv := mockcloud.NewMockServer()
	defer srv.Close()
	srv.Orgs["acme"] = &mockcloud.Org{ID: "01H_ORG_ACME", Slug: "acme"}

	c := apiclient.NewClient(srv.URL, "bso_org_test")
	ctx := context.Background()

	// Create
	var created apiclient.CreateTokenResponse
	if err := c.Do(ctx, "POST", "/v1/orgs/acme/api-tokens", apiclient.CreateTokenRequest{
		Name:        "ci-bot",
		Scope:       "write",
		ProjectIDs:  []string{"01H_PROJ_API"},
		IPAllowlist: []string{"203.0.113.0/24"},
	}, &created); err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.FullToken == "" {
		t.Fatalf("create did not return full_token")
	}
	id := created.Token.ID

	// Read
	var t1 apiclient.APIToken
	if err := c.Do(ctx, "GET", "/v1/orgs/acme/api-tokens/"+id, nil, &t1); err != nil {
		t.Fatalf("read: %v", err)
	}
	if t1.Scope != "write" {
		t.Fatalf("read scope=%s want write", t1.Scope)
	}

	// Narrowing update: write → read
	scope := "read"
	var t2 apiclient.APIToken
	if err := c.Do(ctx, "PATCH", "/v1/orgs/acme/api-tokens/"+id, apiclient.UpdateTokenRequest{
		Scope: &scope,
	}, &t2); err != nil {
		t.Fatalf("update: %v", err)
	}
	if t2.Scope != "read" {
		t.Fatalf("scope did not narrow: %s", t2.Scope)
	}

	// Delete
	if err := c.Do(ctx, "DELETE", "/v1/orgs/acme/api-tokens/"+id, nil, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	err := c.Do(ctx, "GET", "/v1/orgs/acme/api-tokens/"+id, nil, &t1)
	if !apiclient.IsNotFound(err) {
		t.Fatalf("expected 404 after delete, got %v", err)
	}
}

func TestScopeRank(t *testing.T) {
	if scopeRank("read") >= scopeRank("write") {
		t.Errorf("read should outrank write")
	}
	if scopeRank("write") >= scopeRank("admin") {
		t.Errorf("write should be ranked below admin")
	}
	if scopeRank("garbage") != 0 {
		t.Errorf("unknown scope should rank 0")
	}
}
