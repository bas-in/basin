// cmd_pgwire_test — coverage for the pgwire subcommand tree.
//
// All tests use an httptest.Server stub. Stubs assert on request
// method and path. Confirmation prompts are bypassed via --yes or
// piped stdin strings. The test helpers stubJSON / stubError /
// captureStdout / withTempConfigDir are defined in other test files
// in package main.
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

// ── test server helpers ───────────────────────────────────────────────

func pgwireSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setPgwireEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gPgwire(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleCreds() map[string]any {
	return map[string]any{
		"host":     "db.basin.run",
		"port":     5432,
		"user":     "proj_user",
		"dbname":   "proj_db",
		"password": "••••abcd",
		"sslmode":  "require",
	}
}

func sampleReveal() map[string]any {
	return map[string]any{
		"host":     "db.basin.run",
		"port":     5432,
		"user":     "proj_user",
		"dbname":   "proj_db",
		"password": "supersecretpassword1234",
		"sslmode":  "require",
	}
}

func sampleEngineKey(id, name, role string) map[string]any {
	return map[string]any{
		"id":         id,
		"name":       name,
		"role":       role,
		"prefix":     "ek_",
		"masked_key": "ek_••••••••",
		"created_at": "2026-01-01T00:00:00Z",
	}
}

// ── pgwire show ───────────────────────────────────────────────────────

func TestPgwireShow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, sampleCreds())
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"show", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("pgwire show: %v", err)
	}
	if !strings.Contains(out, "db.basin.run") {
		t.Errorf("host missing from output: %q", out)
	}
	if !strings.Contains(out, "proj_user") {
		t.Errorf("user missing from output: %q", out)
	}
	// Password must be masked, not plaintext.
	if strings.Contains(out, "supersecretpassword") {
		t.Errorf("plaintext password must not appear in show output: %q", out)
	}
	if !strings.Contains(out, "••••") {
		t.Errorf("masked bullets missing from output: %q", out)
	}
}

func TestPgwireShowJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, sampleCreds())
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdPgwire(g, []string{"show", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("pgwire show --json: %v", err)
	}
	var got PgwireCredentials
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode pgwire show JSON: %v\nbody=%s", err, out)
	}
	if got.Host != "db.basin.run" {
		t.Errorf("host: got %q, want 'db.basin.run'", got.Host)
	}
	if got.User != "proj_user" {
		t.Errorf("user: got %q, want 'proj_user'", got.User)
	}
}

func TestPgwireShow404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "pgwire credentials not configured")
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	err := cmdPgwire(gPgwire(srv), []string{"show", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != 404 {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── pgwire reveal ─────────────────────────────────────────────────────

func TestPgwireRevealHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/reveal", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, sampleReveal())
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"reveal", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("pgwire reveal: %v", err)
	}
	// Output must be a postgres:// DSN.
	if !strings.HasPrefix(strings.TrimSpace(out), "postgres://") {
		t.Errorf("expected DSN starting with postgres://, got: %q", out)
	}
	// DSN must contain the user and host.
	if !strings.Contains(out, "proj_user") {
		t.Errorf("user missing from DSN: %q", out)
	}
	if !strings.Contains(out, "db.basin.run") {
		t.Errorf("host missing from DSN: %q", out)
	}
	// Password must be included in the DSN (this is the reveal path).
	if !strings.Contains(out, "supersecretpassword1234") {
		t.Errorf("password missing from reveal DSN: %q", out)
	}
}

func TestPgwireRevealJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/reveal", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, sampleReveal())
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdPgwire(g, []string{"reveal", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("pgwire reveal --json: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode pgwire reveal JSON: %v\nbody=%s", err, out)
	}
	dsn, ok := got["dsn"]
	if !ok {
		t.Fatalf("expected 'dsn' key in JSON output, got: %v", got)
	}
	if !strings.HasPrefix(dsn, "postgres://") {
		t.Errorf("dsn must start with postgres://, got: %q", dsn)
	}
}

func TestPgwireRevealServerPrebuiltDSN(t *testing.T) {
	// If the server returns a dsn field, use it verbatim.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/reveal", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"dsn": "postgres://user:pass@host:5432/db?sslmode=require",
		})
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"reveal", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("pgwire reveal prebuilt DSN: %v", err)
	}
	if !strings.Contains(out, "postgres://user:pass@host:5432/db?sslmode=require") {
		t.Errorf("prebuilt DSN not used verbatim: %q", out)
	}
}

func TestPgwireRevealSpecialCharsInPassword(t *testing.T) {
	// Passwords with @, %, : must be percent-encoded in the DSN so that
	// libpq's URI parser can correctly identify the authority boundary.
	// url.UserPassword encodes: @ → %40, % → %25, / → %2F, : → %3A.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/reveal", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"host":     "db.basin.run",
			"port":     5432,
			"user":     "proj_user",
			"dbname":   "proj_db",
			"password": "p@ss%word:/test",
			"sslmode":  "require",
		})
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"reveal", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("pgwire reveal special chars: %v", err)
	}
	// The raw @ must be percent-encoded (otherwise the URI parser mistakes it
	// for the user@host boundary).
	if strings.Contains(out, "p@ss") {
		t.Errorf("unencoded @ in DSN: %q — must be percent-encoded", out)
	}
	// The encoded @ must appear in the password section (%40 = encoded @).
	if !strings.Contains(out, "p%40ss") {
		t.Errorf("expected percent-encoded @ in DSN: %q", out)
	}
}

// ── pgwire rotate ─────────────────────────────────────────────────────

func TestPgwireRotateWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/rotate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, sampleReveal())
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"rotate", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("pgwire rotate: %v", err)
	}
	if !called {
		t.Error("rotate endpoint was not called")
	}
	if !strings.HasPrefix(strings.TrimSpace(out), "postgres://") {
		t.Errorf("expected DSN in rotate output, got: %q", out)
	}
}

func TestPgwireRotateDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/rotate", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, sampleReveal())
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"rotate", "--project=proj1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("pgwire rotate declined: unexpected error: %v", err)
	}
	if called {
		t.Error("rotate endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestPgwireRotateJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdPgwire(g, []string{"rotate", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required': %v", err)
	}
}

func TestPgwireRotateJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/rotate", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, sampleReveal())
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdPgwire(g, []string{"rotate", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("pgwire rotate --json: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode pgwire rotate JSON: %v\nbody=%s", err, out)
	}
	dsn, ok := got["dsn"]
	if !ok {
		t.Fatalf("expected 'dsn' in JSON output, got: %v", got)
	}
	if !strings.HasPrefix(dsn, "postgres://") {
		t.Errorf("dsn must start with postgres://, got: %q", dsn)
	}
}

// ── pgwire engine-keys list ───────────────────────────────────────────

func TestPgwireEngineKeysListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine-keys", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"keys": []any{}})
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"engine-keys", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-keys list empty: %v", err)
	}
	if !strings.Contains(out, "(no engine keys)") {
		t.Errorf("expected '(no engine keys)', got: %q", out)
	}
}

func TestPgwireEngineKeysListPopulated(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine-keys", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"keys": []any{
				sampleEngineKey("ek1", "anon", "anon"),
				sampleEngineKey("ek2", "service_role", "service_role"),
			},
		})
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"engine-keys", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-keys list: %v", err)
	}
	if !strings.Contains(out, "anon") || !strings.Contains(out, "service_role") {
		t.Errorf("key roles missing from output: %q", out)
	}
	if !strings.Contains(out, "••••••••") {
		t.Errorf("masked bullets missing from output: %q", out)
	}
}

func TestPgwireEngineKeysListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine-keys", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"keys": []any{sampleEngineKey("ek1", "anon", "anon")},
		})
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdPgwire(g, []string{"engine-keys", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("engine-keys list --json: %v", err)
	}
	var got EngineKeysResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode engine-keys list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Keys) != 1 || got.Keys[0].ID != "ek1" {
		t.Errorf("engine keys: %+v", got.Keys)
	}
}

// ── pgwire engine-keys rotate ─────────────────────────────────────────

func TestPgwireEngineKeysRotateWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine-keys/rotate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{
			"keys":        []any{sampleEngineKey("ek1", "anon", "anon")},
			"anon_key":    "eyJnewAnonKey",
			"service_key": "eyJnewServiceKey",
		})
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"engine-keys", "rotate", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("engine-keys rotate: %v", err)
	}
	if !called {
		t.Error("rotate endpoint was not called")
	}
	if !strings.Contains(out, "rotated") {
		t.Errorf("expected 'rotated' in output: %q", out)
	}
	if !strings.Contains(out, "eyJnewAnonKey") {
		t.Errorf("new anon_key missing from output: %q", out)
	}
}

func TestPgwireEngineKeysRotateDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine-keys/rotate", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, map[string]any{"keys": []any{}})
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdPgwire(gPgwire(srv), []string{"engine-keys", "rotate", "--project=proj1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("engine-keys rotate declined: unexpected error: %v", err)
	}
	if called {
		t.Error("rotate endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestPgwireEngineKeysRotateJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdPgwire(g, []string{"engine-keys", "rotate", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required': %v", err)
	}
}

func TestPgwireEngineKeysRotateJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/engine-keys/rotate", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"keys":        []any{sampleEngineKey("ek1", "anon", "anon")},
			"anon_key":    "eyJanonNew",
			"service_key": "eyJserviceNew",
		})
	})
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdPgwire(g, []string{"engine-keys", "rotate", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("engine-keys rotate --json: %v", err)
	}
	var got EngineKeysRotateResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode engine-keys rotate JSON: %v\nbody=%s", err, out)
	}
	if got.AnonKey != "eyJanonNew" {
		t.Errorf("anon_key: got %q, want 'eyJanonNew'", got.AnonKey)
	}
	if got.ServiceKey != "eyJserviceNew" {
		t.Errorf("service_key: got %q, want 'eyJserviceNew'", got.ServiceKey)
	}
}

// ── arg-parse / dispatcher ────────────────────────────────────────────

func TestPgwireUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	err := cmdPgwire(gPgwire(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestPgwireHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdPgwire(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("pgwire help: %v", err)
	}
}

func TestPgwireNoProjectFlag(t *testing.T) {
	mux := http.NewServeMux()
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	err := cmdPgwire(gPgwire(srv), []string{"show"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}

func TestPgwireMaskPassword(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", "••••••••"},
		{"abcd", "••••••••"},           // ≤8 chars: no suffix (would leak too much)
		{"abcde", "••••••••"},          // 5 chars: no suffix
		{"abcdefgh", "••••••••"},       // exactly 8 chars: no suffix
		{"supersecret1234", "••••••••1234"}, // >8 chars: last 4 shown
		{"abc@xyz!pass_end", "••••••••_end"}, // >8 chars: last 4 shown
	}
	for _, tc := range cases {
		got := maskPassword(tc.input)
		if got != tc.want {
			t.Errorf("maskPassword(%q): got %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestBuildDSN(t *testing.T) {
	r := &PgwireRevealResponse{
		Host:     "db.basin.run",
		Port:     5432,
		User:     "proj_user",
		DBName:   "proj_db",
		Password: "simplepassword",
		SSLMode:  "require",
	}
	dsn := buildDSN(r)
	if !strings.HasPrefix(dsn, "postgres://proj_user:simplepassword@db.basin.run:5432/proj_db") {
		t.Errorf("DSN: got %q", dsn)
	}
	if !strings.Contains(dsn, "sslmode=require") {
		t.Errorf("sslmode missing from DSN: %q", dsn)
	}
}

func TestBuildDSNSpecialChars(t *testing.T) {
	// url.UserPassword encodes @ → %40, % → %25, / → %2F, : → %3A.
	// The result for "p@ss%/word" is "p%40ss%25%2Fword".
	r := &PgwireRevealResponse{
		Host:     "db.basin.run",
		Port:     5432,
		User:     "proj_user",
		DBName:   "proj_db",
		Password: "p@ss%/word",
		SSLMode:  "require",
	}
	dsn := buildDSN(r)
	// Raw @ must not appear unencoded (it would break the authority boundary).
	if strings.Contains(dsn, "p@ss") {
		t.Errorf("unencoded @ in DSN password: %q", dsn)
	}
	// The encoded @ must be present (percent-encoding of @).
	if !strings.Contains(dsn, "p%40ss") {
		t.Errorf("expected percent-encoded @ in DSN: %q", dsn)
	}
	// The encoded % must also be present (percent-encoding of percent sign).
	if !strings.Contains(dsn, "%25") {
		t.Errorf("expected percent-encoded percent sign in DSN: %q", dsn)
	}
}

func TestBuildDSNPrebuilt(t *testing.T) {
	prebuilt := "postgres://user:pw@host:5432/db?sslmode=require"
	r := &PgwireRevealResponse{DSN: prebuilt}
	if got := buildDSN(r); got != prebuilt {
		t.Errorf("prebuilt DSN not returned verbatim: got %q", got)
	}
}

func TestBuildDSNDefaults(t *testing.T) {
	// When host/port/sslmode are zero-values, sensible defaults apply.
	r := &PgwireRevealResponse{
		User:     "u",
		DBName:   "mydb",
		Password: "pw",
	}
	dsn := buildDSN(r)
	if !strings.Contains(dsn, "localhost") {
		t.Errorf("expected localhost default: %q", dsn)
	}
	if !strings.Contains(dsn, ":5432") {
		t.Errorf("expected port 5432 default: %q", dsn)
	}
	if !strings.Contains(dsn, "sslmode=require") {
		t.Errorf("expected sslmode=require default: %q", dsn)
	}
}

func TestPgwireEngineKeysUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := pgwireSrv(t, mux)
	setPgwireEnv(t, srv)

	err := cmdPgwire(gPgwire(srv), []string{"engine-keys", "nope"})
	if err == nil {
		t.Fatal("expected error for unknown engine-keys subcommand")
	}
}
