// cmd_oauth_apps_test — coverage for `basin oauth-apps` subcommands.
//
// Secret-bearing responses (create, rotate) are verified to print the
// client_secret with a "store now" warning. Subsequent get/list calls
// must NOT include the client_secret — only client_id + metadata.
//
// Missing --org produces a typed error before any HTTP call.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────

func oauthAppsSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func gOAuthApps(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleOAuthApp(id string) map[string]any {
	return map[string]any{
		"id":           id,
		"name":         "My App",
		"client_id":    "client_" + id,
		"redirect_uri": "https://app.example.com/callback",
		"scopes":       []string{"read", "write"},
		"enabled":      true,
		"created_at":   "2026-05-01T00:00:00Z",
	}
}

// ── oauth-apps list ───────────────────────────────────────────────────

func TestOAuthAppsListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"apps": []any{sampleOAuthApp("app1"), sampleOAuthApp("app2")},
		})
	})
	srv := oauthAppsSrv(t, mux)

	read := captureStdout(t)
	if err := cmdOAuthApps(gOAuthApps(srv), []string{"list", "--org=acme"}); err != nil {
		t.Fatalf("oauth-apps list: %v", err)
	}
	out := read()
	if !strings.Contains(out, "app1") || !strings.Contains(out, "app2") {
		t.Errorf("expected both app IDs in output: %q", out)
	}
	// client_secret must NOT appear in list output.
	if strings.Contains(strings.ToLower(out), "secret") {
		t.Errorf("client_secret must not appear in list output: %q", out)
	}
}

func TestOAuthAppsListJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"apps": []any{sampleOAuthApp("app1")},
		})
	})
	srv := oauthAppsSrv(t, mux)

	g := gOAuthApps(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdOAuthApps(g, []string{"list", "--org=acme"}); err != nil {
		t.Fatalf("oauth-apps list --json: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	apps, ok := result["apps"]
	if !ok {
		t.Fatalf("JSON missing 'apps' key: %s", out)
	}
	appList, _ := apps.([]any)
	if len(appList) == 0 {
		t.Fatalf("expected at least one app in list")
	}
}

func TestOAuthAppsListMissingOrg(t *testing.T) {
	err := cmdOAuthApps(&globalFlags{token: "tok"}, []string{"list"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
	if !strings.Contains(err.Error(), "--org") {
		t.Errorf("expected --org mention in error: %v", err)
	}
}

// ── oauth-apps get ────────────────────────────────────────────────────

func TestOAuthAppsGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/app1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"app": sampleOAuthApp("app1")})
	})
	srv := oauthAppsSrv(t, mux)

	read := captureStdout(t)
	if err := cmdOAuthApps(gOAuthApps(srv), []string{"get", "--org=acme", "app1"}); err != nil {
		t.Fatalf("oauth-apps get: %v", err)
	}
	out := read()
	if !strings.Contains(out, "client_app1") {
		t.Errorf("expected client_id in output: %q", out)
	}
}

func TestOAuthAppsGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/notfound", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		stubJSON(w, map[string]any{"error": map[string]any{"code": "not_found", "message": "app not found"}})
	})
	srv := oauthAppsSrv(t, mux)

	err := cmdOAuthApps(gOAuthApps(srv), []string{"get", "--org=acme", "notfound"})
	if err == nil {
		t.Fatal("expected error for 404")
	}
	ae, ok := AsAPIError(err)
	if !ok {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if ae.HTTPStatus != 404 {
		t.Errorf("expected HTTP 404, got %d", ae.HTTPStatus)
	}
}

// ── oauth-apps create ─────────────────────────────────────────────────

func TestOAuthAppsCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			// GET requests for list also hit this path.
			stubJSON(w, map[string]any{"apps": []any{}})
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		stubJSON(w, map[string]any{
			"app":           sampleOAuthApp("app1"),
			"client_secret": "cs_supersecret456",
		})
	})
	srv := oauthAppsSrv(t, mux)

	read := captureStdout(t)
	if err := cmdOAuthApps(gOAuthApps(srv), []string{"create",
		"--org=acme", "--name=My App", "--redirect-uri=https://app.example.com/callback"}); err != nil {
		t.Fatalf("oauth-apps create: %v", err)
	}
	out := read()
	// Secret MUST be shown on creation.
	if !strings.Contains(out, "cs_supersecret456") {
		t.Errorf("expected client_secret in create output: %q", out)
	}
	// Store-now warning must be present.
	if !strings.Contains(strings.ToLower(out), "store") {
		t.Errorf("expected 'store now' warning in output: %q", out)
	}
	if gotBody["name"] != "My App" {
		t.Errorf("expected name=My App in body: %v", gotBody)
	}
}

func TestOAuthAppsCreateSecretNotInGet(t *testing.T) {
	// After creating, a GET call must not include the client_secret.
	secret := "cs_supersecret456"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			stubJSON(w, map[string]any{
				"app":           sampleOAuthApp("app1"),
				"client_secret": secret,
			})
			return
		}
		stubJSON(w, map[string]any{"apps": []any{sampleOAuthApp("app1")}})
	})
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/app1", func(w http.ResponseWriter, r *http.Request) {
		// GET /oauth-apps/{id} — must NOT return the secret.
		stubJSON(w, map[string]any{"app": sampleOAuthApp("app1")})
	})
	srv := oauthAppsSrv(t, mux)

	g := gOAuthApps(srv)
	g.json = true

	// Create step.
	read := captureStdout(t)
	if err := cmdOAuthApps(g, []string{"create",
		"--org=acme", "--name=My App", "--redirect-uri=https://app.example.com/callback"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	createOut := read()
	if !strings.Contains(createOut, secret) {
		t.Fatalf("secret must appear in create output: %q", createOut)
	}

	// Get step — secret must NOT appear.
	read = captureStdout(t)
	if err := cmdOAuthApps(g, []string{"get", "--org=acme", "app1"}); err != nil {
		t.Fatalf("get: %v", err)
	}
	getOut := read()
	if strings.Contains(getOut, secret) {
		t.Errorf("secret must NOT appear in get output: %q", getOut)
	}
}

func TestOAuthAppsCreateMissingOrg(t *testing.T) {
	err := cmdOAuthApps(&globalFlags{token: "tok"}, []string{"create",
		"--name=x", "--redirect-uri=https://x.com/cb"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

func TestOAuthAppsCreateMissingName(t *testing.T) {
	err := cmdOAuthApps(&globalFlags{token: "tok"}, []string{"create",
		"--org=acme", "--redirect-uri=https://x.com/cb"})
	if err == nil {
		t.Fatal("expected error for missing --name")
	}
}

// ── oauth-apps rotate ─────────────────────────────────────────────────

func TestOAuthAppsRotateHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/app1/rotate-secret", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"app":           sampleOAuthApp("app1"),
			"client_secret": "cs_newrotated789",
		})
	})
	srv := oauthAppsSrv(t, mux)

	read := captureStdout(t)
	if err := cmdOAuthApps(gOAuthApps(srv), []string{"rotate", "--org=acme", "--yes", "app1"}); err != nil {
		t.Fatalf("oauth-apps rotate: %v", err)
	}
	out := read()
	// Rotated secret MUST be shown.
	if !strings.Contains(out, "cs_newrotated789") {
		t.Errorf("expected new client_secret in rotate output: %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "store") {
		t.Errorf("expected 'store now' warning in rotate output: %q", out)
	}
}

func TestOAuthAppsRotateMissingOrg(t *testing.T) {
	err := cmdOAuthApps(&globalFlags{token: "tok"}, []string{"rotate", "app1", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

// ── oauth-apps disable / enable ───────────────────────────────────────

func TestOAuthAppsDisableHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/app1/disable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"enabled": false})
	})
	srv := oauthAppsSrv(t, mux)

	g := gOAuthApps(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdOAuthApps(g, []string{"disable", "--org=acme", "app1"}); err != nil {
		t.Fatalf("oauth-apps disable: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if enabled, ok := result["enabled"].(bool); !ok || enabled {
		t.Errorf("expected enabled=false: %v", result)
	}
}

func TestOAuthAppsEnableHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/app1/enable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"enabled": true})
	})
	srv := oauthAppsSrv(t, mux)

	g := gOAuthApps(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdOAuthApps(g, []string{"enable", "--org=acme", "app1"}); err != nil {
		t.Fatalf("oauth-apps enable: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if result["enabled"] != true {
		t.Errorf("expected enabled=true: %v", result)
	}
}

func TestOAuthAppsDisableMissingOrg(t *testing.T) {
	err := cmdOAuthApps(&globalFlags{token: "tok"}, []string{"disable", "app1"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

// ── oauth-apps delete ─────────────────────────────────────────────────

func TestOAuthAppsDeleteHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/app1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"deleted": true, "id": "app1"})
	})
	srv := oauthAppsSrv(t, mux)

	g := gOAuthApps(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdOAuthApps(g, []string{"delete", "--org=acme", "--yes", "app1"}); err != nil {
		t.Fatalf("oauth-apps delete: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if result["deleted"] != true {
		t.Errorf("expected deleted=true: %v", result)
	}
}

func TestOAuthAppsDeleteMissingOrg(t *testing.T) {
	err := cmdOAuthApps(&globalFlags{token: "tok"}, []string{"delete", "app1", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

func TestOAuthAppsDelete403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/app1", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		stubJSON(w, map[string]any{"error": map[string]any{"code": "forbidden", "message": "insufficient role"}})
	})
	srv := oauthAppsSrv(t, mux)

	err := cmdOAuthApps(gOAuthApps(srv), []string{"delete", "--org=acme", "--yes", "app1"})
	if err == nil {
		t.Fatal("expected error for 403")
	}
	ae, ok := AsAPIError(err)
	if !ok {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if ae.HTTPStatus != 403 {
		t.Errorf("expected HTTP 403, got %d", ae.HTTPStatus)
	}
}

// ── oauth-apps patch ──────────────────────────────────────────────────

func TestOAuthAppsPatchHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/oauth-apps/app1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Errorf("decode body: %v", err)
			}
			updated := sampleOAuthApp("app1")
			updated["name"] = "Updated App"
			stubJSON(w, map[string]any{"app": updated})
			return
		}
		stubJSON(w, map[string]any{"app": sampleOAuthApp("app1")})
	})
	srv := oauthAppsSrv(t, mux)

	read := captureStdout(t)
	if err := cmdOAuthApps(gOAuthApps(srv), []string{"patch", "--org=acme", "--name=Updated App", "app1"}); err != nil {
		t.Fatalf("oauth-apps patch: %v", err)
	}
	read()
	if gotBody["name"] != "Updated App" {
		t.Errorf("expected name=Updated App in PATCH body: %v", gotBody)
	}
}

func TestOAuthAppsPatchMissingOrg(t *testing.T) {
	err := cmdOAuthApps(&globalFlags{token: "tok"}, []string{"patch", "app1", "--name=x"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}
