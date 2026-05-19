// cmd_engine_pin_test — coverage for the engine-pin subcommand.
//
// Tests drive an httptest.Server stub and assert on method + path +
// query string + body. Validation errors are tested before any HTTP call.
// Shared helpers (stubJSON, stubError, captureStdout, withTempConfigDir)
// come from cmd_branches_test.go and integration_test.go.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────

func enginePinSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setEnginePinEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gEnginePin(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func samplePin(version string) map[string]any {
	return map[string]any{
		"version":   version,
		"pinned_at": "2026-05-01T00:00:00Z",
		"pinned_by": "user-1",
	}
}

// ── validation: engine-pin put ────────────────────────────────────────

func TestEnginePinPutMissingVersion(t *testing.T) {
	mux := http.NewServeMux()
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	err := cmdEnginePin(gEnginePin(srv), []string{"put", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error when --version is missing")
	}
	if !strings.Contains(err.Error(), "--version") {
		t.Errorf("error should mention --version: %v", err)
	}
}

// ── engine-pin get ────────────────────────────────────────────────────

func TestEnginePinGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"pin": samplePin("1.2.3")})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	err := cmdEnginePin(gEnginePin(srv), []string{"get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin get: %v", err)
	}
	if !strings.Contains(out, "1.2.3") {
		t.Errorf("expected version in output: %q", out)
	}
}

func TestEnginePinGetNullPin(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"pin": nil})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	err := cmdEnginePin(gEnginePin(srv), []string{"get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin get null: %v", err)
	}
	if !strings.Contains(out, "(no engine version pin") {
		t.Errorf("expected 'no engine version pin' in output: %q", out)
	}
}

func TestEnginePinGetJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"pin": samplePin("2.0.0")})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdEnginePin(g, []string{"get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin get --json: %v", err)
	}
	var got struct {
		Pin *EnginePin `json:"pin"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode pin JSON: %v\nbody=%s", err, out)
	}
	if got.Pin == nil || got.Pin.Version != "2.0.0" {
		t.Errorf("pin.version: got %+v", got.Pin)
	}
}

func TestEnginePinGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "project not found")
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	err := cmdEnginePin(gEnginePin(srv), []string{"get", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── engine-pin put ────────────────────────────────────────────────────

func TestEnginePinPutWithYes(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{"pin": samplePin("1.5.0")})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	err := cmdEnginePin(gEnginePin(srv), []string{"put", "--project=proj1", "--version=1.5.0", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin put: %v", err)
	}
	if gotBody["version"] != "1.5.0" {
		t.Errorf("version in body: got %v", gotBody["version"])
	}
	if !strings.Contains(out, "1.5.0") {
		t.Errorf("expected version in output: %q", out)
	}
}

func TestEnginePinPutJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"pin": samplePin("3.0.0")})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdEnginePin(g, []string{"put", "--project=proj1", "--version=3.0.0", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin put --json: %v", err)
	}
	var got struct {
		Pin *EnginePin `json:"pin"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode put JSON: %v\nbody=%s", err, out)
	}
	if got.Pin == nil || got.Pin.Version != "3.0.0" {
		t.Errorf("pin.version: got %+v", got.Pin)
	}
}

// ── engine-pin delete ─────────────────────────────────────────────────

func TestEnginePinDeleteWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"unpinned": true})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	err := cmdEnginePin(gEnginePin(srv), []string{"delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "removed") {
		t.Errorf("expected 'removed' in output: %q", out)
	}
}

func TestEnginePinDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdEnginePin(gEnginePin(srv), []string{"delete", "--project=proj1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("engine-pin delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestEnginePinDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"unpinned": true})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdEnginePin(g, []string{"delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin delete --json: %v", err)
	}
	var got struct {
		Unpinned bool `json:"unpinned"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Unpinned {
		t.Errorf("unpinned: expected true, got false")
	}
}

// ── engine-pin events ─────────────────────────────────────────────────

func TestEnginePinEventsHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"events": []any{
				map[string]any{"id": "ev1", "action": "pin", "version": "1.5.0", "at": "2026-05-01T00:00:00Z", "actor_id": "user-1"},
				map[string]any{"id": "ev2", "action": "unpin", "version": "", "at": "2026-05-02T00:00:00Z", "actor_id": "user-2"},
			},
		})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	err := cmdEnginePin(gEnginePin(srv), []string{"events", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin events: %v", err)
	}
	if !strings.Contains(out, "ev1") || !strings.Contains(out, "ev2") {
		t.Errorf("event ids missing from output: %q", out)
	}
}

func TestEnginePinEventsQueryLimit(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/events", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	_ = captureStdout(t)
	err := cmdEnginePin(gEnginePin(srv), []string{"events", "--project=proj1", "--limit=10"})
	if err != nil {
		t.Fatalf("engine-pin events query limit: %v", err)
	}
	if !strings.Contains(gotQuery, "limit=10") {
		t.Errorf("limit not passed as query param: got %q", gotQuery)
	}
}

func TestEnginePinEventsJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/events", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"events": []any{
				map[string]any{"id": "ev1", "action": "pin", "version": "2.0.0", "at": "2026-05-01T00:00:00Z", "actor_id": "user-1"},
			},
		})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdEnginePin(g, []string{"events", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin events --json: %v", err)
	}
	var got struct {
		Events []*EnginePinEvent `json:"events"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode events JSON: %v\nbody=%s", err, out)
	}
	if len(got.Events) != 1 || got.Events[0].ID != "ev1" {
		t.Errorf("unexpected events in JSON: %+v", got.Events)
	}
}

func TestEnginePinEventsEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/events", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	read := captureStdout(t)
	err := cmdEnginePin(gEnginePin(srv), []string{"events", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-pin events empty: %v", err)
	}
	if !strings.Contains(out, "(no engine pin events)") {
		t.Errorf("expected '(no engine pin events)', got: %q", out)
	}
}

func TestEnginePinGet403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "insufficient permissions")
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	err := cmdEnginePin(gEnginePin(srv), []string{"get", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

func TestEnginePinPut422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine/pin", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "invalid version format")
	})
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	err := cmdEnginePin(gEnginePin(srv), []string{"put", "--project=proj1", "--version=bad-ver", "--yes"})
	if err == nil {
		t.Fatal("expected error on 422, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusUnprocessableEntity {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

func TestEnginePinNoProject(t *testing.T) {
	mux := http.NewServeMux()
	srv := enginePinSrv(t, mux)
	setEnginePinEnv(t, srv)

	err := cmdEnginePin(gEnginePin(srv), []string{"get"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}
