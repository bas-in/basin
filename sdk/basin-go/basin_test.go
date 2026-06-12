package basin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestClient(t *testing.T, mux *http.ServeMux) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := New(srv.URL, "test-api-key", WithProjectID("proj-01"))
	return client, srv
}

func respondJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func respondError(w http.ResponseWriter, status int, code, message string) {
	respondJSON(w, status, map[string]any{"code": code, "message": message})
}

// ---------------------------------------------------------------------------
// Error envelope decoding
// ---------------------------------------------------------------------------

func TestBasinErrorDecode(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/signup", func(w http.ResponseWriter, r *http.Request) {
		respondError(w, 409, "E_INVALID_REQUEST", "email already registered")
	})
	client, _ := newTestClient(t, mux)

	_, err := client.Auth.SignUp(context.Background(), "a@b.com", "pw", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var be *BasinError
	if !errors.As(err, &be) {
		t.Fatalf("expected *BasinError, got %T: %v", err, err)
	}
	if be.Code != "E_INVALID_REQUEST" {
		t.Errorf("Code: got %q want %q", be.Code, "E_INVALID_REQUEST")
	}
	if be.Status != 409 {
		t.Errorf("Status: got %d want %d", be.Status, 409)
	}
}

func TestBasinErrorWithSQLState(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 409, map[string]any{
			"code":     "E_INVALID_REQUEST",
			"message":  "duplicate key",
			"sqlstate": "23505",
		})
	})
	client, _ := newTestClient(t, mux)

	_, err := client.Table("orders").Insert(context.Background(), map[string]any{"id": 1})
	var be *BasinError
	if !errors.As(err, &be) {
		t.Fatalf("expected *BasinError, got %T", err)
	}
	if be.SQLState != "23505" {
		t.Errorf("SQLState: got %q want %q", be.SQLState, "23505")
	}
}

// ---------------------------------------------------------------------------
// Auth flows
// ---------------------------------------------------------------------------

func TestSignUp(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/signup", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method: got %q want POST", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["project_id"] != "proj-01" {
			t.Errorf("project_id: got %v want proj-01", body["project_id"])
		}
		if body["email"] != "alice@example.com" {
			t.Errorf("email: got %v", body["email"])
		}
		respondJSON(w, 201, map[string]any{"ok": true, "user_id": "usr-01"})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Auth.SignUp(context.Background(), "alice@example.com", "pw", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.UserID != "usr-01" {
		t.Errorf("UserID: got %q want usr-01", result.UserID)
	}
}

func TestSignIn(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/signin", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{
			"access_token":       "at-tok",
			"refresh_token":      "rt-tok",
			"access_expires_at":  "2099-01-01T00:00:00Z",
			"refresh_expires_at": "2099-01-01T00:00:00Z",
		})
	})
	client, _ := newTestClient(t, mux)

	sess, err := client.Auth.SignIn(context.Background(), "alice@example.com", "pw", "")
	if err != nil {
		t.Fatal(err)
	}
	if sess.AccessToken != "at-tok" {
		t.Errorf("AccessToken: got %q want at-tok", sess.AccessToken)
	}
	// Session must be stored.
	stored := client.Auth.GetSession()
	if stored == nil || stored.AccessToken != "at-tok" {
		t.Error("session not stored after SignIn")
	}
}

func TestSignOut(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/signin", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{
			"access_token": "at", "refresh_token": "rt",
			"access_expires_at": "2099-01-01T00:00:00Z", "refresh_expires_at": "2099-01-01T00:00:00Z",
		})
	})
	client, _ := newTestClient(t, mux)
	_, _ = client.Auth.SignIn(context.Background(), "a@b.com", "pw", "")
	client.Auth.SignOut(context.Background())
	if client.Auth.GetSession() != nil {
		t.Error("session should be nil after SignOut")
	}
}

func TestRefreshSession(t *testing.T) {
	calls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/refresh", func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["refresh_token"] != "rt-tok" {
			t.Errorf("refresh_token: got %v", body["refresh_token"])
		}
		respondJSON(w, 200, map[string]any{
			"access_token": "at-new", "refresh_token": "rt-new",
			"access_expires_at": "2099-01-01T00:00:00Z", "refresh_expires_at": "2099-01-01T00:00:00Z",
		})
	})
	client, _ := newTestClient(t, mux)
	client.Auth.SetSession(&Session{
		AccessToken:      "at-tok",
		RefreshToken:     "rt-tok",
		AccessExpiresAt:  "2099-01-01T00:00:00Z",
		RefreshExpiresAt: "2099-01-01T00:00:00Z",
	})

	sess, err := client.Auth.RefreshSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sess.AccessToken != "at-new" {
		t.Errorf("AccessToken: got %q", sess.AccessToken)
	}
	if calls != 1 {
		t.Errorf("expected 1 refresh call, got %d", calls)
	}
}

func TestRefreshSessionNoSession(t *testing.T) {
	client := New("http://localhost", "key")
	_, err := client.Auth.RefreshSession(context.Background())
	var be *BasinError
	if !errors.As(err, &be) {
		t.Fatalf("expected *BasinError, got %T", err)
	}
	if be.Code != "E_UNAUTHENTICATED" {
		t.Errorf("Code: got %q", be.Code)
	}
}

func TestRefreshRace(t *testing.T) {
	// Multiple goroutines calling AccessToken concurrently when the session is
	// near expiry should each receive a valid token and not race.
	calls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/refresh", func(w http.ResponseWriter, r *http.Request) {
		calls++
		respondJSON(w, 200, map[string]any{
			"access_token": fmt.Sprintf("at-%d", calls), "refresh_token": "rt-new",
			"access_expires_at": "2099-01-01T00:00:00Z", "refresh_expires_at": "2099-01-01T00:00:00Z",
		})
	})
	client, _ := newTestClient(t, mux)
	// Set a session that is already expired.
	client.Auth.SetSession(&Session{
		AccessToken:      "at-old",
		RefreshToken:     "rt-tok",
		AccessExpiresAt:  "2000-01-01T00:00:00Z", // already expired
		RefreshExpiresAt: "2099-01-01T00:00:00Z",
	})

	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			tok, err := client.Auth.AccessToken(context.Background())
			if err != nil {
				done <- err
				return
			}
			if tok == "" {
				done <- fmt.Errorf("empty token")
				return
			}
			done <- nil
		}()
	}
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

func TestAutoRefreshOnExpiry(t *testing.T) {
	refreshed := false
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/refresh", func(w http.ResponseWriter, r *http.Request) {
		refreshed = true
		respondJSON(w, 200, map[string]any{
			"access_token": "at-fresh", "refresh_token": "rt-2",
			"access_expires_at": "2099-01-01T00:00:00Z", "refresh_expires_at": "2099-01-01T00:00:00Z",
		})
	})
	mux.HandleFunc("/rest/v1/t", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer at-fresh" {
			t.Errorf("expected refreshed token in Authorization, got %q", r.Header.Get("Authorization"))
		}
		respondJSON(w, 200, []map[string]any{})
	})
	client, _ := newTestClient(t, mux)
	client.Auth.SetSession(&Session{
		AccessToken:      "at-expired",
		RefreshToken:     "rt-1",
		AccessExpiresAt:  "2000-01-01T00:00:00Z", // expired
		RefreshExpiresAt: "2099-01-01T00:00:00Z",
	})

	_, err := client.Table("t").Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed {
		t.Error("expected auto-refresh to fire")
	}
}

func TestRequestMagicLink(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/magic-link", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["email"] != "alice@example.com" {
			t.Errorf("email: got %v", body["email"])
		}
		w.WriteHeader(204)
	})
	client, _ := newTestClient(t, mux)
	if err := client.Auth.RequestMagicLink(context.Background(), "alice@example.com"); err != nil {
		t.Fatal(err)
	}
}

func TestAPIKeyCRUD(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/api-keys", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			respondJSON(w, 201, map[string]any{
				"id": 7, "name": body["name"], "secret": "sk-secret", "created_at": "2024-01-01T00:00:00Z",
			})
		case "GET":
			respondJSON(w, 200, []map[string]any{
				{"id": 7, "name": "my-key", "created_at": "2024-01-01T00:00:00Z"},
			})
		}
	})
	mux.HandleFunc("/auth/v1/api-keys/7", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		respondJSON(w, 200, map[string]any{"ok": true})
	})
	client, _ := newTestClient(t, mux)
	ctx := context.Background()

	issued, err := client.Auth.CreateAPIKey(ctx, "my-key")
	if err != nil {
		t.Fatal(err)
	}
	if issued.Secret != "sk-secret" {
		t.Errorf("Secret: got %q", issued.Secret)
	}

	keys, err := client.Auth.ListAPIKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].ID != 7 {
		t.Errorf("ListAPIKeys: got %v", keys)
	}

	if err := client.Auth.DeleteAPIKey(ctx, 7); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// MFA shapes
// ---------------------------------------------------------------------------

func TestEnrollTOTP(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/factors", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method: got %s", r.Method)
		}
		respondJSON(w, 200, map[string]any{
			"factor_id":   "fac-01",
			"factor_type": "totp",
			"secret_b32":  "JBSWY3DPEHPK3PXP",
			"otpauth_uri": "otpauth://totp/Basin:alice?secret=JBSWY3DPEHPK3PXP",
		})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Auth.EnrollFactor(context.Background(), "totp", "My App")
	if err != nil {
		t.Fatal(err)
	}
	totp, ok := result.(*TotpEnrollResult)
	if !ok {
		t.Fatalf("expected *TotpEnrollResult, got %T", result)
	}
	if totp.SecretB32 != "JBSWY3DPEHPK3PXP" {
		t.Errorf("SecretB32: got %q", totp.SecretB32)
	}
}

func TestEnrollWebAuthn(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/factors", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{
			"factor_id":             "fac-02",
			"factor_type":           "webauthn",
			"challenge_id":          "chal-01",
			"creation_options_json": json.RawMessage(`{"rp":{"name":"Basin"}}`),
		})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Auth.EnrollFactor(context.Background(), "webauthn", "YubiKey")
	if err != nil {
		t.Fatal(err)
	}
	wa, ok := result.(*WebAuthnEnrollResult)
	if !ok {
		t.Fatalf("expected *WebAuthnEnrollResult, got %T", result)
	}
	if wa.ChallengeID != "chal-01" {
		t.Errorf("ChallengeID: got %q", wa.ChallengeID)
	}
}

func TestListFactors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/factors", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("method: got %s", r.Method)
		}
		respondJSON(w, 200, []map[string]any{
			{
				"id": "fac-01", "factor_type": "totp", "status": "verified",
				"friendly_name": "My App", "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z",
			},
		})
	})
	client, _ := newTestClient(t, mux)

	factors, err := client.Auth.ListFactors(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(factors) != 1 || factors[0].FactorType != "totp" {
		t.Errorf("ListFactors: got %v", factors)
	}
}

func TestVerifyFactor(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/factors/fac-01/verify", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["code"] != "123456" {
			t.Errorf("code: got %v", body["code"])
		}
		respondJSON(w, 200, map[string]any{
			"ok":             true,
			"recovery_codes": []string{"aabb1122", "ccdd3344"},
		})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Auth.VerifyFactor(context.Background(), "fac-01", "123456", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RecoveryCodes) != 2 {
		t.Errorf("RecoveryCodes: got %v", result.RecoveryCodes)
	}
}

func TestChallengeTOTP(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/factors/fac-01/challenge", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{"challenge_id": "chal-99"})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Auth.ChallengeFactor(context.Background(), "fac-01")
	if err != nil {
		t.Fatal(err)
	}
	totp, ok := result.(*TotpChallengeResult)
	if !ok {
		t.Fatalf("expected *TotpChallengeResult, got %T", result)
	}
	if totp.ChallengeID != "chal-99" {
		t.Errorf("ChallengeID: got %q", totp.ChallengeID)
	}
}

func TestChallengeWebAuthn(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/factors/fac-02/challenge", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{
			"challenge_id":         "chal-01",
			"request_options_json": json.RawMessage(`{"allowCredentials":[]}`),
		})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Auth.ChallengeFactor(context.Background(), "fac-02")
	if err != nil {
		t.Fatal(err)
	}
	wa, ok := result.(*WebAuthnChallengeResult)
	if !ok {
		t.Fatalf("expected *WebAuthnChallengeResult, got %T", result)
	}
	if wa.RequestOptionsJSON == nil {
		t.Error("RequestOptionsJSON should not be nil")
	}
}

func TestVerifyChallenge(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/factors/fac-01/challenge/verify", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["challenge_id"] != "chal-01" {
			t.Errorf("challenge_id: got %v", body["challenge_id"])
		}
		respondJSON(w, 200, map[string]any{
			"access_token": "at-aal2", "refresh_token": "rt-2",
			"access_expires_at": "2099-01-01T00:00:00Z", "refresh_expires_at": "2099-01-01T00:00:00Z",
		})
	})
	client, _ := newTestClient(t, mux)

	sess, err := client.Auth.VerifyChallenge(context.Background(), "fac-01", "chal-01", "999999", "")
	if err != nil {
		t.Fatal(err)
	}
	if sess.AccessToken != "at-aal2" {
		t.Errorf("AccessToken: got %q", sess.AccessToken)
	}
}

func TestUnenrollFactor(t *testing.T) {
	called := false
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/factors/fac-01", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("method: got %s", r.Method)
		}
		called = true
		respondJSON(w, 200, map[string]any{"ok": true})
	})
	client, _ := newTestClient(t, mux)
	if err := client.Auth.UnenrollFactor(context.Background(), "fac-01"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("DELETE not called")
	}
}

// ---------------------------------------------------------------------------
// OAuth
// ---------------------------------------------------------------------------

func TestOAuthAuthorizeURL(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/oauth/google/authorize", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("method: got %s", r.Method)
		}
		if r.URL.Query().Get("project_id") != "proj-01" {
			t.Errorf("project_id: got %q", r.URL.Query().Get("project_id"))
		}
		respondJSON(w, 200, map[string]any{
			"redirect_url": "https://accounts.google.com/o/oauth2/auth?...",
			"state":        "csrf-state",
		})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Auth.GetOAuthAuthorizeURL(context.Background(), "google", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(result.RedirectURL, "https://accounts.google.com") {
		t.Errorf("RedirectURL: got %q", result.RedirectURL)
	}
	if result.State != "csrf-state" {
		t.Errorf("State: got %q", result.State)
	}
}

// ---------------------------------------------------------------------------
// Query builder — URL construction
// ---------------------------------------------------------------------------

func TestQueryBuilderURLConstruction(t *testing.T) {
	tests := []struct {
		name    string
		build   func(*QueryBuilder) *QueryBuilder
		wantURL string
	}{
		{
			name:    "select star by default",
			build:   func(q *QueryBuilder) *QueryBuilder { return q },
			wantURL: "/rest/v1/orders",
		},
		{
			name:    "select columns",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.Select("id", "total") },
			wantURL: "/rest/v1/orders?select=id%2Ctotal",
		},
		{
			name:    "eq filter",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.Eq("status", "open") },
			wantURL: "/rest/v1/orders?status=eq.open",
		},
		{
			name:    "gt filter",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.Gt("total", "100") },
			wantURL: "/rest/v1/orders?total=gt.100",
		},
		{
			name:    "in filter",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.In("status", []string{"open", "closed"}) },
			wantURL: "/rest/v1/orders?status=in.%28open%2Cclosed%29",
		},
		{
			name:    "is null",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.Is("deleted_at", "null") },
			wantURL: "/rest/v1/orders?deleted_at=is.null",
		},
		{
			name:    "order asc",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.Order("total", true) },
			wantURL: "/rest/v1/orders?order=total.asc",
		},
		{
			name:    "order desc",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.Order("total", false) },
			wantURL: "/rest/v1/orders?order=total.desc",
		},
		{
			name:    "limit and offset",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.Limit(10).Offset(20) },
			wantURL: "/rest/v1/orders?limit=10&offset=20",
		},
		{
			name:    "cursor",
			build:   func(q *QueryBuilder) *QueryBuilder { return q.Cursor("tok123") },
			wantURL: "/rest/v1/orders?cursor=tok123",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			called := false
			mux := http.NewServeMux()
			mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
				called = true
				got := r.URL.Path + "?" + r.URL.RawQuery
				if r.URL.RawQuery == "" {
					got = r.URL.Path
				}
				if got != tc.wantURL {
					t.Errorf("URL: got %q want %q", got, tc.wantURL)
				}
				respondJSON(w, 200, []map[string]any{})
			})
			client, _ := newTestClient(t, mux)
			q := tc.build(client.Table("orders"))
			_, _ = q.Run(context.Background())
			if !called {
				t.Error("handler not called")
			}
		})
	}
}

func TestQueryBuilderImmutability(t *testing.T) {
	// Chaining should not mutate the original builder.
	base := New("http://example.com", "key").Table("orders")
	q1 := base.Eq("a", "1")
	q2 := base.Eq("b", "2")
	if len(q1.query) == len(q2.query) && len(q1.query) > 0 && q1.query[0] == q2.query[0] {
		// They could be equal by coincidence if both add one param — just check lengths.
	}
	// Verify that q1 does not contain q2's filter.
	for _, p := range q1.query {
		if p.key == "b" {
			t.Error("q1 should not contain q2's filter")
		}
	}
}

// ---------------------------------------------------------------------------
// Query builder — response normalisation
// ---------------------------------------------------------------------------

func TestQueryBuilderPlainArrayResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, []map[string]any{{"id": 1}, {"id": 2}})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Table("orders").Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Rows: got %d want 2", len(result.Rows))
	}
	if result.NextCursor != "" {
		t.Errorf("NextCursor should be empty for plain array")
	}
}

func TestQueryBuilderPaginatedResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{
			"rows":        []map[string]any{{"id": 1}},
			"next_cursor": "cursor-abc",
		})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Table("orders").Limit(1).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.NextCursor != "cursor-abc" {
		t.Errorf("NextCursor: got %q want cursor-abc", result.NextCursor)
	}
}

func TestQueryResultInto(t *testing.T) {
	result := &QueryResult{
		Rows: []Row{{"id": float64(1), "name": "Alice"}, {"id": float64(2), "name": "Bob"}},
	}
	type User struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	var users []User
	if err := result.Into(&users); err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 || users[0].Name != "Alice" {
		t.Errorf("Into: got %+v", users)
	}
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

func TestInsert(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method: got %s", r.Method)
		}
		respondJSON(w, 201, map[string]any{"ok": true, "tag": "INSERT 1"})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Table("orders").Insert(context.Background(), map[string]any{"total": 99})
	if err != nil {
		t.Fatal(err)
	}
	if result.Tag != "INSERT 1" {
		t.Errorf("Tag: got %q", result.Tag)
	}
}

func TestUpdate(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" {
			t.Errorf("method: got %s", r.Method)
		}
		if !strings.Contains(r.URL.RawQuery, "status=eq.open") {
			t.Errorf("filter missing: got %q", r.URL.RawQuery)
		}
		respondJSON(w, 200, map[string]any{"ok": true, "tag": "UPDATE 3"})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Table("orders").
		Eq("status", "open").
		Update(context.Background(), map[string]any{"status": "closed"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Tag != "UPDATE 3" {
		t.Errorf("Tag: got %q", result.Tag)
	}
}

func TestDelete(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("method: got %s", r.Method)
		}
		respondJSON(w, 200, map[string]any{"ok": true, "tag": "DELETE 1"})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Table("orders").Eq("id", "5").Delete(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Error("ok should be true")
	}
}

// ---------------------------------------------------------------------------
// RPC / Functions
// ---------------------------------------------------------------------------

func TestRPC(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/rest/v1/rpc/my_fn", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method: got %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["x"] != float64(5) {
			t.Errorf("arg x: got %v", body["x"])
		}
		respondJSON(w, 200, 25) // scalar result
	})
	client, _ := newTestClient(t, mux)

	result, err := client.RPC(context.Background(), "my_fn", map[string]any{"x": 5})
	if err != nil {
		t.Fatal(err)
	}
	if result != float64(25) {
		t.Errorf("result: got %v want 25", result)
	}
}

func TestFunctionInvoke(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fn/v1/my-handler", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-api-key" {
			t.Errorf("Authorization: got %q", r.Header.Get("Authorization"))
		}
		respondJSON(w, 200, map[string]any{"hello": "world"})
	})
	client, _ := newTestClient(t, mux)

	result, err := client.Functions.Invoke(context.Background(), "my-handler", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != 200 {
		t.Errorf("Status: got %d", result.Status)
	}
}

func TestFunctionInvokeErrorEnvelope(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fn/v1/bad-fn", func(w http.ResponseWriter, r *http.Request) {
		respondError(w, 404, "E_NOT_FOUND", "function not found")
	})
	client, _ := newTestClient(t, mux)

	_, err := client.Functions.Invoke(context.Background(), "bad-fn", nil)
	var be *BasinError
	if !errors.As(err, &be) {
		t.Fatalf("expected *BasinError, got %T", err)
	}
	if be.Code != "E_NOT_FOUND" {
		t.Errorf("Code: got %q", be.Code)
	}
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

func TestCreateBucket(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/storage/v1/bucket", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method: got %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["name"] != "avatars" {
			t.Errorf("name: got %v", body["name"])
		}
		respondJSON(w, 201, map[string]any{
			"id": "bkt-01", "name": "avatars", "public": false,
			"allowed_mime_types": []string{}, "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z",
		})
	})
	client, _ := newTestClient(t, mux)

	bucket, err := client.Storage.CreateBucket(context.Background(), "avatars")
	if err != nil {
		t.Fatal(err)
	}
	if bucket.Name != "avatars" {
		t.Errorf("Name: got %q", bucket.Name)
	}
}

func TestGetBucket(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/storage/v1/bucket/avatars", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{
			"id": "bkt-01", "name": "avatars", "public": true,
			"allowed_mime_types": []string{}, "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z",
		})
	})
	client, _ := newTestClient(t, mux)

	bucket, err := client.Storage.GetBucket(context.Background(), "avatars")
	if err != nil {
		t.Fatal(err)
	}
	if !bucket.Public {
		t.Error("Public: expected true")
	}
}

func TestDeleteBucket(t *testing.T) {
	called := false
	mux := http.NewServeMux()
	mux.HandleFunc("/storage/v1/bucket/avatars", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("method: got %s", r.Method)
		}
		called = true
		respondJSON(w, 200, map[string]any{"ok": true})
	})
	client, _ := newTestClient(t, mux)
	if err := client.Storage.DeleteBucket(context.Background(), "avatars"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("DELETE not called")
	}
}

func TestObjectUploadDownloadRemove(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/storage/v1/object/avatars/alice/profile.png", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			if r.Header.Get("Content-Type") != "image/png" {
				t.Errorf("Content-Type: got %q", r.Header.Get("Content-Type"))
			}
			respondJSON(w, 200, map[string]any{
				"id": "obj-01", "bucket_id": "bkt-01", "path": "alice/profile.png",
				"size": 42, "etag": "abc123", "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z",
			})
		case "GET":
			w.Header().Set("Content-Type", "image/png")
			w.Header().Set("ETag", "abc123")
			w.WriteHeader(200)
			_, _ = w.Write([]byte{0x89, 0x50, 0x4E, 0x47})
		case "DELETE":
			respondJSON(w, 200, map[string]any{"ok": true})
		}
	})
	client, _ := newTestClient(t, mux)
	bkt := client.Storage.FromBucket("avatars")
	ctx := context.Background()

	obj, err := bkt.Upload(ctx, "alice/profile.png", []byte{1, 2, 3}, WithContentType("image/png"))
	if err != nil {
		t.Fatal(err)
	}
	if obj.ETag != "abc123" {
		t.Errorf("ETag: got %q", obj.ETag)
	}

	dl, err := bkt.Download(ctx, "alice/profile.png")
	if err != nil {
		t.Fatal(err)
	}
	if dl.ContentType != "image/png" {
		t.Errorf("ContentType: got %q", dl.ContentType)
	}
	if len(dl.Data) != 4 {
		t.Errorf("Data len: got %d", len(dl.Data))
	}

	if err := bkt.Remove(ctx, "alice/profile.png"); err != nil {
		t.Fatal(err)
	}
}

func TestObjectList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/storage/v1/object/list/avatars", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method: got %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["prefix"] != "alice/" {
			t.Errorf("prefix: got %v", body["prefix"])
		}
		respondJSON(w, 200, []map[string]any{
			{"id": "obj-01", "bucket_id": "bkt-01", "path": "alice/pic.png",
				"size": 10, "etag": "e1", "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z"},
		})
	})
	client, _ := newTestClient(t, mux)

	objects, err := client.Storage.FromBucket("avatars").List(context.Background(), "alice/", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Errorf("objects: got %d", len(objects))
	}
}

func TestRemoveByPrefixes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/storage/v1/object/avatars", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("method: got %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		prefixes, _ := body["prefixes"].([]any)
		if len(prefixes) != 2 {
			t.Errorf("prefixes: got %v", prefixes)
		}
		respondJSON(w, 200, map[string]any{"ok": true})
	})
	client, _ := newTestClient(t, mux)

	err := client.Storage.FromBucket("avatars").
		RemoveByPrefixes(context.Background(), []string{"alice/", "bob/"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetPublicURL(t *testing.T) {
	client := New("http://localhost:8080", "key", WithProjectID("proj-42"))
	bkt := client.Storage.FromBucket("avatars")

	u, err := bkt.GetPublicURL("alice/profile.png")
	if err != nil {
		t.Fatal(err)
	}
	want := "http://localhost:8080/storage/v1/object/public/proj-42/avatars/alice/profile.png"
	if u != want {
		t.Errorf("URL: got %q want %q", u, want)
	}
}

func TestGetPublicURLNoProjectID(t *testing.T) {
	client := New("http://localhost:8080", "key") // no project ID
	_, err := client.Storage.FromBucket("avatars").GetPublicURL("x.png")
	var be *BasinError
	if !errors.As(err, &be) {
		t.Fatalf("expected *BasinError, got %T", err)
	}
	if be.Code != "E_INVALID_REQUEST" {
		t.Errorf("Code: got %q", be.Code)
	}
}

func TestCreateSignedURL(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/storage/v1/object/sign/upload/avatars/alice/pic.png", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method: got %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["expires_in"] != float64(3600) {
			t.Errorf("expires_in: got %v", body["expires_in"])
		}
		respondJSON(w, 200, map[string]any{
			"signedUrl": "/storage/v1/object/sign/proj-01/avatars/alice/pic.png?token=tok",
			"expiresAt": "2099-01-01T00:00:00Z",
		})
	})
	client, _ := newTestClient(t, mux)

	signed, err := client.Storage.FromBucket("avatars").
		CreateSignedURL(context.Background(), "alice/pic.png", 3600)
	if err != nil {
		t.Fatal(err)
	}
	if signed.SignedURL == "" {
		t.Error("SignedURL should not be empty")
	}
	if !strings.Contains(signed.AbsoluteURL, "token=tok") {
		t.Errorf("AbsoluteURL: got %q", signed.AbsoluteURL)
	}
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func TestHealth(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, "ok")
	})
	client, _ := newTestClient(t, mux)

	resp, err := client.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if resp != "ok" {
		t.Errorf("Health: got %q want ok", resp)
	}
}

// ---------------------------------------------------------------------------
// JWT helper
// ---------------------------------------------------------------------------

func TestProjectIDFromJWT(t *testing.T) {
	// Craft a minimal JWT with project_id claim.
	// header.payload.sig — base64url encoded.
	payload := `{"project_id":"proj-abc","sub":"usr-1"}`
	encoded := strings.TrimRight(
		strings.NewReplacer("+", "-", "/", "_").Replace(
			encodeB64([]byte(payload)),
		), "=")
	token := "header." + encoded + ".sig"
	if got := projectIDFromJWT(token); got != "proj-abc" {
		t.Errorf("projectIDFromJWT: got %q want proj-abc", got)
	}
}

func TestProjectIDFromJWTNonJWT(t *testing.T) {
	if got := projectIDFromJWT("sk_live_12345"); got != "" {
		t.Errorf("expected empty for non-JWT, got %q", got)
	}
}

func encodeB64(b []byte) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	out := make([]byte, 0, (len(b)+2)/3*4)
	for i := 0; i < len(b); i += 3 {
		var val uint32
		n := 3
		if i+1 >= len(b) {
			n = 1
		} else if i+2 >= len(b) {
			n = 2
		}
		val = uint32(b[i]) << 16
		if n > 1 {
			val |= uint32(b[i+1]) << 8
		}
		if n > 2 {
			val |= uint32(b[i+2])
		}
		out = append(out, chars[(val>>18)&63], chars[(val>>12)&63])
		if n > 1 {
			out = append(out, chars[(val>>6)&63])
		} else {
			out = append(out, '=')
		}
		if n > 2 {
			out = append(out, chars[val&63])
		} else {
			out = append(out, '=')
		}
	}
	return string(out)
}
