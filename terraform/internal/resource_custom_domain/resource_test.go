// Lifecycle test for basin_custom_domain.
package resource_custom_domain

import (
	"context"
	"testing"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"
	"github.com/bas-in/terraform-provider-basin/internal/mockcloud"
)

func TestCustomDomainLifecycle(t *testing.T) {
	srv := mockcloud.NewMockServer()
	defer srv.Close()
	c := apiclient.NewClient(srv.URL, "bso_org_test")
	ctx := context.Background()

	var d apiclient.CustomDomain
	if err := c.Do(ctx, "POST", "/v1/projects/api-1a2b3c4d/custom-domains", apiclient.CreateCustomDomainRequest{
		Domain: "api.example.com",
	}, &d); err != nil {
		t.Fatalf("create: %v", err)
	}
	if d.VerificationToken == "" || d.TXTRecordName == "" {
		t.Fatalf("create did not return verification metadata: %#v", d)
	}
	id := d.ID

	var got apiclient.CustomDomain
	if err := c.Do(ctx, "GET", "/v1/projects/api-1a2b3c4d/custom-domains/"+id, nil, &got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Domain != "api.example.com" || got.Status != "pending" {
		t.Fatalf("read mismatch: %#v", got)
	}

	if err := c.Do(ctx, "DELETE", "/v1/projects/api-1a2b3c4d/custom-domains/"+id, nil, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	err := c.Do(ctx, "GET", "/v1/projects/api-1a2b3c4d/custom-domains/"+id, nil, &got)
	if !apiclient.IsNotFound(err) {
		t.Fatalf("expected 404 after delete, got %v", err)
	}
}
