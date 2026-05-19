// cmd_pgwire — pgwire credentials + engine-keys management.
//
//	basin pgwire show          [--project=<ref>]
//	basin pgwire reveal        [--project=<ref>]
//	basin pgwire rotate        [--project=<ref>] [--yes]
//	basin pgwire engine-keys list   [--project=<ref>]
//	basin pgwire engine-keys rotate [--project=<ref>] [--yes]
//
// Routes:
//   GET    /v1/projects/{ref}/pgwire              → show (masked password)
//   POST   /v1/projects/{ref}/pgwire/reveal       → full DSN once
//   POST   /v1/projects/{ref}/pgwire/rotate       → new password (or DSN)
//   GET    /v1/projects/{ref}/engine-keys         → engine JWT keys (masked)
//   POST   /v1/projects/{ref}/engine-keys/rotate  → new engine JWT keys
//
// Password masking convention: "••••••••<last4>" where last4 is the
// final 4 characters of the password when known. When the password is
// fully opaque (server returns only a masked form), we display
// "••••••••" with no suffix.
//
// DSN construction for `reveal` uses url.PathEscape on the password so
// special characters (%, @, /) are percent-encoded and the DSN is safe
// to paste into psql / libpq without shell quoting issues.
//
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
// Error with hint at `basin link` if neither is available.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// PgwireCredentials is the read shape from GET /v1/projects/{ref}/pgwire.
// Password is always masked on this endpoint; use reveal to get the full DSN.
type PgwireCredentials struct {
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	DBName   string `json:"dbname,omitempty"`
	Password string `json:"password,omitempty"` // masked form from the server
	SSLMode  string `json:"sslmode,omitempty"`
}

// PgwireRevealResponse is the once-only response from
// POST /v1/projects/{ref}/pgwire/reveal. Password contains the full
// plaintext; DSN may be pre-built by the server or assembled client-side.
type PgwireRevealResponse struct {
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	DBName   string `json:"dbname,omitempty"`
	Password string `json:"password,omitempty"`
	SSLMode  string `json:"sslmode,omitempty"`
	DSN      string `json:"dsn,omitempty"` // server may pre-build this
}

// PgwireRotateResponse is the response from POST /v1/projects/{ref}/pgwire/rotate.
type PgwireRotateResponse struct {
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	DBName   string `json:"dbname,omitempty"`
	Password string `json:"password,omitempty"` // new plaintext password
	SSLMode  string `json:"sslmode,omitempty"`
	DSN      string `json:"dsn,omitempty"` // server may pre-build this
}

// EngineKey is one JWT key returned by GET /v1/projects/{ref}/engine-keys.
type EngineKey struct {
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Role      string `json:"role,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
	MaskedKey string `json:"masked_key,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

// EngineKeysResponse is the list response from GET /v1/projects/{ref}/engine-keys.
type EngineKeysResponse struct {
	Keys []*EngineKey `json:"keys,omitempty"`
}

// EngineKeysRotateResponse is the response from POST /v1/projects/{ref}/engine-keys/rotate.
type EngineKeysRotateResponse struct {
	Keys    []*EngineKey `json:"keys,omitempty"`
	AnonKey string       `json:"anon_key,omitempty"`
	ServiceKey string    `json:"service_key,omitempty"`
}

// ── Helpers ───────────────────────────────────────────────────────────

// maskPassword returns a masked representation of a password for display.
// Shows "••••••••" plus the last 4 characters only when the password
// is longer than 8 characters — this matches the convention used by
// tools like psql, 1Password, and the Basin dashboard. For short or
// empty passwords the suffix is omitted entirely to avoid leaking a
// meaningful fraction of a short secret.
func maskPassword(pw string) string {
	if pw == "" {
		return "••••••••"
	}
	runes := []rune(pw)
	if len(runes) > 8 {
		return "••••••••" + string(runes[len(runes)-4:])
	}
	return "••••••••"
}

// maskEngineKey masks an engine key for display.
func maskEngineKey(k *EngineKey) string {
	if k == nil {
		return ""
	}
	if k.MaskedKey != "" {
		return k.MaskedKey
	}
	if k.Prefix != "" {
		return k.Prefix + "••••••••"
	}
	return "(masked)"
}

// buildDSN assembles a postgres:// DSN from the reveal response fields.
// Uses url.URL + url.UserPassword so that special characters in the
// password (%, @, :, /) are correctly percent-encoded per RFC 3986 §3.2.1.
// url.UserPassword encodes @ as %40, % as %25, / as %2F, : as %3A —
// all characters that would otherwise confuse libpq's URI parser.
// If the server already returned a DSN, that is used verbatim.
func buildDSN(r *PgwireRevealResponse) string {
	if r.DSN != "" {
		return r.DSN
	}
	host := r.Host
	if host == "" {
		host = "localhost"
	}
	port := r.Port
	if port == 0 {
		port = 5432
	}
	sslmode := r.SSLMode
	if sslmode == "" {
		sslmode = "require"
	}
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(r.User, r.Password),
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     "/" + r.DBName,
		RawQuery: "sslmode=" + sslmode,
	}
	return u.String()
}

// buildRotateDSN assembles a postgres:// DSN from the rotate response.
// Same encoding logic as buildDSN but operates on PgwireRotateResponse.
func buildRotateDSN(r *PgwireRotateResponse) string {
	if r.DSN != "" {
		return r.DSN
	}
	host := r.Host
	if host == "" {
		host = "localhost"
	}
	port := r.Port
	if port == 0 {
		port = 5432
	}
	sslmode := r.SSLMode
	if sslmode == "" {
		sslmode = "require"
	}
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(r.User, r.Password),
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     "/" + r.DBName,
		RawQuery: "sslmode=" + sslmode,
	}
	return u.String()
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdPgwire(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdPgwireShow(g, nil)
	}
	switch args[0] {
	case "show":
		return cmdPgwireShow(g, args[1:])
	case "reveal":
		return cmdPgwireReveal(g, args[1:])
	case "rotate":
		return cmdPgwireRotate(g, args[1:])
	case "engine-keys":
		return cmdPgwireEngineKeys(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("pgwire", "Manage pgwire credentials and engine JWT keys.", []string{
			"show         [--project=<ref>]            Show pgwire metadata (password masked).",
			"reveal       [--project=<ref>]            Print the full connection DSN (shown ONCE).",
			"rotate       [--project=<ref>] [--yes]    Rotate the pgwire password (new DSN shown ONCE).",
			"engine-keys list   [--project=<ref>]      List engine JWT keys (masked).",
			"engine-keys rotate [--project=<ref>] [--yes]  Rotate engine JWT keys.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for pgwire", args[0])
		return errSilent
	}
}

// ── pgwire show ───────────────────────────────────────────────────────

func cmdPgwireShow(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("pgwire show", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var creds PgwireCredentials
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/pgwire", nil, &creds); err != nil {
		return err
	}
	if g.json {
		// JSON shape: PgwireCredentials { host, port, user, dbname, password (masked), sslmode }
		// Password is always masked on this path — use `reveal` for the full DSN.
		return printJSON(os.Stdout, creds)
	}
	port := creds.Port
	if port == 0 {
		port = 5432
	}
	sslmode := creds.SSLMode
	if sslmode == "" {
		sslmode = "require"
	}
	fmt.Fprintf(os.Stdout, "host:     %s\n", creds.Host)
	fmt.Fprintf(os.Stdout, "port:     %d\n", port)
	fmt.Fprintf(os.Stdout, "user:     %s\n", creds.User)
	fmt.Fprintf(os.Stdout, "dbname:   %s\n", creds.DBName)
	fmt.Fprintf(os.Stdout, "password: %s\n", maskPassword(creds.Password))
	fmt.Fprintf(os.Stdout, "sslmode:  %s\n", sslmode)
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Run `basin pgwire reveal` to get the full DSN.")
	return nil
}

// ── pgwire reveal ─────────────────────────────────────────────────────

func cmdPgwireReveal(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("pgwire reveal", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp PgwireRevealResponse
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/pgwire/reveal", nil, &resp); err != nil {
		return err
	}
	dsn := buildDSN(&resp)
	if g.json {
		// JSON shape: { dsn: string } — full DSN, shown once.
		return printJSON(os.Stdout, map[string]string{"dsn": dsn})
	}
	fmt.Fprintln(os.Stdout, dsn)
	return nil
}

// ── pgwire rotate ─────────────────────────────────────────────────────

func cmdPgwireRotate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("pgwire rotate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to rotate pgwire credentials non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Rotate pgwire password for project %q? All existing connections will be dropped. [y/N] ", ref)
		line, err := readLine()
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			fmt.Fprintln(os.Stdout, "Aborted.")
			return nil
		}
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp PgwireRotateResponse
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/pgwire/rotate", nil, &resp); err != nil {
		return err
	}
	dsn := buildRotateDSN(&resp)
	if g.json {
		// JSON shape: { dsn: string } — new DSN after rotation, shown once.
		return printJSON(os.Stdout, map[string]string{"dsn": dsn})
	}
	if !g.quiet {
		fmt.Fprintln(os.Stderr, "⚠  Store this DSN now — the password will NOT be shown again.")
	}
	fmt.Fprintln(os.Stdout, dsn)
	return nil
}

// ── pgwire engine-keys dispatcher ────────────────────────────────────

func cmdPgwireEngineKeys(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin pgwire engine-keys (list | rotate) ...")
	}
	switch args[0] {
	case "list":
		return cmdPgwireEngineKeysList(g, args[1:])
	case "rotate":
		return cmdPgwireEngineKeysRotate(g, args[1:])
	default:
		printErr(g, "unknown subcommand %q for pgwire engine-keys", args[0])
		return errSilent
	}
}

// ── pgwire engine-keys list ───────────────────────────────────────────

func cmdPgwireEngineKeysList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("pgwire engine-keys list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp EngineKeysResponse
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/engine-keys", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: EngineKeysResponse { keys: [ EngineKey ] }
		// masked_key or prefix shown; full key never present in list.
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Keys) == 0 {
		fmt.Fprintln(os.Stdout, "(no engine keys)")
		return nil
	}
	t := newTable(g, "ID", "NAME", "ROLE", "KEY (MASKED)", "CREATED")
	for _, k := range resp.Keys {
		t.row(k.ID, k.Name, k.Role, maskEngineKey(k), k.CreatedAt)
	}
	return t.flush()
}

// ── pgwire engine-keys rotate ─────────────────────────────────────────

func cmdPgwireEngineKeysRotate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("pgwire engine-keys rotate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to rotate engine keys non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Rotate engine JWT keys for project %q? SDK clients using the old keys must be updated. [y/N] ", ref)
		line, err := readLine()
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			fmt.Fprintln(os.Stdout, "Aborted.")
			return nil
		}
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp EngineKeysRotateResponse
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/engine-keys/rotate", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: EngineKeysRotateResponse { keys: [ EngineKey ], anon_key?: string, service_key?: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "Engine keys rotated.")
	if resp.AnonKey != "" {
		if !g.quiet {
			fmt.Fprintln(os.Stderr, "⚠  Store these keys now — they will NOT be shown again.")
		}
		fmt.Fprintf(os.Stdout, "anon_key:    %s\n", resp.AnonKey)
	}
	if resp.ServiceKey != "" {
		fmt.Fprintf(os.Stdout, "service_key: %s\n", resp.ServiceKey)
	}
	return nil
}
