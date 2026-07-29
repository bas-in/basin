// Lifecycle test for basin_project — drives the wire layer through
// the same mockcloud server the framework-level acceptance tests
// will use once TF_ACC is set. This test does NOT require TF_ACC; it
// exercises the resource's HTTP contract end-to-end via apiclient.
package resource_project

import (
	"context"
	"strings"
	"testing"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"
	"github.com/bas-in/terraform-provider-basin/internal/mockcloud"
)

func TestProjectLifecycle(t *testing.T) {
	srv := mockcloud.NewMockServer()
	defer srv.Close()
	srv.Orgs["acme"] = &mockcloud.Org{ID: "01H_ORG_ACME", Slug: "acme", Name: "Acme", Plan: "team"}

	c := apiclient.NewClient(srv.URL, "bso_org_test")
	ctx := context.Background()

	// Create
	var createOut apiclient.CreateProjectResponse
	if err := c.Do(ctx, "POST", "/v1/orgs/acme/projects", apiclient.CreateProjectRequest{
		Name: "api", Region: "jnb",
	}, &createOut); err != nil {
		t.Fatalf("create: %v", err)
	}
	if createOut.Project == nil || createOut.Project.Ref == "" {
		t.Fatalf("create returned empty project: %#v", createOut)
	}
	if createOut.Pgwire == nil || createOut.Pgwire.ConnectionURL == "" {
		t.Fatalf("create did not return pgwire connection_url")
	}
	ref := createOut.Project.Ref

	// Read
	var got apiclient.Project
	if err := c.Do(ctx, "GET", "/v1/projects/"+ref, nil, &got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Name != "api" || got.Region != "jnb" {
		t.Fatalf("read mismatch: %#v", got)
	}

	// Update (rename — only mutable field)
	newName := "api-v2"
	var patched apiclient.Project
	if err := c.Do(ctx, "PATCH", "/v1/projects/"+ref, apiclient.UpdateProjectRequest{Name: &newName}, &patched); err != nil {
		t.Fatalf("update: %v", err)
	}
	if patched.Name != "api-v2" {
		t.Fatalf("update did not persist new name: %s", patched.Name)
	}

	// Delete
	if err := c.Do(ctx, "DELETE", "/v1/projects/"+ref, nil, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Confirm 404 after delete
	err := c.Do(ctx, "GET", "/v1/projects/"+ref, nil, &got)
	if !apiclient.IsNotFound(err) {
		t.Fatalf("expected 404 after delete, got %v", err)
	}
}

func TestProjectImportParsing(t *testing.T) {
	cases := []struct {
		in    string
		ok    bool
		slug  string
		ref   string
	}{
		{"acme/api-1a2b3c4d", true, "acme", "api-1a2b3c4d"},
		{"justref", false, "", ""},
		{"/missing-slug", false, "", ""},
		{"acme/", false, "", ""},
	}
	for _, tc := range cases {
		parts := strings.SplitN(tc.in, "/", 2)
		ok := len(parts) == 2 && parts[0] != "" && parts[1] != ""
		if ok != tc.ok {
			t.Errorf("import %q: ok=%v want %v", tc.in, ok, tc.ok)
		}
		if ok {
			if parts[0] != tc.slug || parts[1] != tc.ref {
				t.Errorf("import %q parsed slug=%s ref=%s; want %s/%s",
					tc.in, parts[0], parts[1], tc.slug, tc.ref)
			}
		}
	}
}
