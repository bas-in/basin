// Provider-level smoke tests — the heavy framework-acceptance path
// (resource.Test) is gated on TF_ACC; here we just exercise the
// Client + me-probe wiring against the mock cloud.
package provider

import (
	"context"
	"testing"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"
	"github.com/bas-in/terraform-provider-basin/internal/mockcloud"
)

func TestMeProbeSucceeds(t *testing.T) {
	srv := mockcloud.NewMockServer()
	defer srv.Close()
	c := apiclient.NewClient(srv.URL, "bso_org_anything")
	var u apiclient.User
	if err := c.Do(context.Background(), "GET", "/v1/me", nil, &u); err != nil {
		t.Fatalf("me probe: %v", err)
	}
	if u.Email == "" {
		t.Fatalf("me returned empty user")
	}
}

func TestMeProbeUnauthorized(t *testing.T) {
	srv := mockcloud.NewMockServer()
	defer srv.Close()
	c := apiclient.NewClient(srv.URL, "") // no token → mock 401s
	var u apiclient.User
	err := c.Do(context.Background(), "GET", "/v1/me", nil, &u)
	if err == nil {
		t.Fatalf("expected 401, got nil")
	}
	ae, ok := apiclient.AsAPIError(err)
	if !ok || ae.HTTPStatus != 401 {
		t.Fatalf("expected APIError 401, got %v", err)
	}
}
