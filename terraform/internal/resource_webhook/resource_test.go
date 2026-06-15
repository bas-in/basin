// Lifecycle test for basin_webhook.
package resource_webhook

import (
	"context"
	"testing"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"
	"github.com/bas-in/terraform-provider-basin/internal/mockcloud"
)

func TestWebhookLifecycle(t *testing.T) {
	srv := mockcloud.NewMockServer()
	defer srv.Close()
	c := apiclient.NewClient(srv.URL, "bso_org_test")
	ctx := context.Background()

	var created apiclient.CreateWebhookResponse
	if err := c.Do(ctx, "POST", "/v1/projects/api-1a2b3c4d/webhooks", apiclient.CreateWebhookRequest{
		URL: "https://hooks.example.com/in", Events: []string{"project.snapshot.completed"},
	}, &created); err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.SecretPlain == "" {
		t.Fatalf("create did not return secret_plain")
	}
	id := created.Webhook.ID

	var got apiclient.Webhook
	if err := c.Do(ctx, "GET", "/v1/projects/api-1a2b3c4d/webhooks/"+id, nil, &got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.URL != "https://hooks.example.com/in" {
		t.Fatalf("read URL mismatch: %s", got.URL)
	}

	newURL := "https://hooks.example.com/v2"
	active := false
	var patched apiclient.Webhook
	if err := c.Do(ctx, "PATCH", "/v1/projects/api-1a2b3c4d/webhooks/"+id, apiclient.PatchWebhookRequest{
		URL: &newURL, Active: &active,
	}, &patched); err != nil {
		t.Fatalf("update: %v", err)
	}
	if patched.URL != newURL || patched.Active {
		t.Fatalf("update did not persist: %#v", patched)
	}

	if err := c.Do(ctx, "DELETE", "/v1/projects/api-1a2b3c4d/webhooks/"+id, nil, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	err := c.Do(ctx, "GET", "/v1/projects/api-1a2b3c4d/webhooks/"+id, nil, &got)
	if !apiclient.IsNotFound(err) {
		t.Fatalf("expected 404 after delete, got %v", err)
	}
}
