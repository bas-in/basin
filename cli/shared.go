// shared — global flag parsing, output formatters, and small JSON
// helpers used across every cmd_*.go.
//
// The top-level CLI carries a small set of flags that apply uniformly
// to every subcommand (--api-url, --token, --json, -q/--quiet,
// --no-color). We strip them off os.Args before dispatch so each
// subcommand only sees its own flag.FlagSet.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
)

// errSilent is returned by handlers that have already written a
// tailored error message to stderr; main() exits non-zero without
// re-printing anything.
var errSilent = errors.New("silent")

// globalFlags carries the small set of flags every subcommand
// understands. We accept --foo=bar, --foo bar, and -q (the only
// short flag) — flag.FlagSet would happily handle this for us, but
// since we want the shape "basin <cmd> <subcmd> --flags" with each
// subcommand parsing its own subflags, we hand-roll the global pass.
type globalFlags struct {
	apiURL  string
	token   string
	orgSlug string
	json    bool
	quiet   bool
	noColor bool
}

// parseGlobalFlags strips the top-level flags off argv and returns
// the residual subcommand args. Anything we don't recognise is
// passed through so the subcommand can parse it.
func parseGlobalFlags(argv []string) (*globalFlags, []string, error) {
	g := &globalFlags{
		apiURL:  envOr("BASIN_API", defaultAPIURL()),
		token:   os.Getenv("BASIN_TOKEN"),
		noColor: os.Getenv("NO_COLOR") != "",
	}
	rest := make([]string, 0, len(argv))
	i := 0
	for i < len(argv) {
		a := argv[i]
		switch {
		case a == "--":
			rest = append(rest, argv[i+1:]...)
			i = len(argv)
		case a == "--json":
			g.json = true
		case a == "--no-color":
			g.noColor = true
		case a == "-q" || a == "--quiet":
			g.quiet = true
		case a == "-h" || a == "--help":
			// Only treat as the top-level help command when we haven't
			// yet seen a subcommand. Otherwise pass through so the
			// subcommand's own --help branch fires.
			if len(rest) == 0 {
				rest = append(rest, "help")
			} else {
				rest = append(rest, a)
			}
		case strings.HasPrefix(a, "--api-url="):
			g.apiURL = strings.TrimPrefix(a, "--api-url=")
		case a == "--api-url":
			if i+1 >= len(argv) {
				return nil, nil, errors.New("--api-url requires a value")
			}
			g.apiURL = argv[i+1]
			i++
		case strings.HasPrefix(a, "--token="):
			g.token = strings.TrimPrefix(a, "--token=")
		case a == "--token":
			if i+1 >= len(argv) {
				return nil, nil, errors.New("--token requires a value")
			}
			g.token = argv[i+1]
			i++
		case strings.HasPrefix(a, "--org="):
			g.orgSlug = strings.TrimPrefix(a, "--org=")
		default:
			rest = append(rest, a)
		}
		i++
	}
	return g, rest, nil
}

// envOr returns os.Getenv(k) or dflt if empty.
func envOr(k, dflt string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return dflt
}

// defaultAPIURL builds the CLI's fallback API URL from BASIN_DOMAIN
// (or "basin.run" when unset). The operator only configures one
// env variable to retarget the binary at a different cloud.
func defaultAPIURL() string {
	d := strings.TrimSpace(os.Getenv("BASIN_DOMAIN"))
	if d == "" {
		d = "basin.run"
	}
	return "https://api." + d
}

// resolveToken applies the lookup ladder spelled out in main.go.
// loaded is the parsed config (may be nil if no config exists).
// orgSlug filters the per-org map; empty falls through to default.
func resolveToken(g *globalFlags, loaded *configFile) string {
	if g.token != "" {
		return g.token
	}
	if loaded == nil {
		return ""
	}
	if g.orgSlug != "" {
		if t, ok := loaded.Tokens[g.orgSlug]; ok && t != "" {
			return t
		}
	}
	return loaded.DefaultToken
}

// requireClient builds an authenticated *Client, returning a typed
// error when the caller has no credentials. Subcommands call this
// at the top of their handler so the error message is uniform.
func requireClient(g *globalFlags) (*Client, error) {
	cfg, _ := readConfigFile() // best-effort; missing is fine
	tok := resolveToken(g, cfg)
	if tok == "" {
		fmt.Fprintln(os.Stderr, "basin: not signed in. Run `basin login` first, set $BASIN_TOKEN, or pass --token=<pat>.")
		return nil, errSilent
	}
	apiURL := g.apiURL
	if cfg != nil && cfg.APIURL != "" && os.Getenv("BASIN_API") == "" && apiURL == defaultAPIURL() {
		// Config file overrides the static default but loses to --api-url
		// and $BASIN_API. We detect "user didn't override" by checking
		// the in-process default + env emptiness above.
		apiURL = cfg.APIURL
	}
	return NewClient(apiURL, tok), nil
}

// printJSON writes v as a single-line / pretty-printed JSON object
// to w. We default to pretty (2-space) when we can detect a TTY on
// stdout; piped output stays compact-ish via json.Marshal.
func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// table is a tiny wrapper around text/tabwriter for the default
// non-JSON output path. The header row is written separately so we
// can emit a separator and (when colour is enabled) dim the columns.
type table struct {
	w       *tabwriter.Writer
	headers []string
	dim     bool
}

// newTable returns a tabwriter-backed table. If g indicates colour
// is OK and stdout is a tty (best-effort via NO_COLOR/quiet) we
// allow ANSI dim on separator rows.
func newTable(g *globalFlags, headers ...string) *table {
	t := &table{
		w:       tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0),
		headers: headers,
		dim:     !g.noColor && os.Getenv("NO_COLOR") == "" && isTerminal(os.Stdout),
	}
	t.writeHeader()
	return t
}

func (t *table) writeHeader() {
	if len(t.headers) == 0 {
		return
	}
	fmt.Fprintln(t.w, strings.Join(t.headers, "\t"))
	sep := make([]string, len(t.headers))
	for i, h := range t.headers {
		// Use the same width as the header label so columns align before
		// tabwriter pads.
		sep[i] = strings.Repeat("─", visibleLen(h))
	}
	line := strings.Join(sep, "\t")
	if t.dim {
		line = ansiDim + line + ansiReset
	}
	fmt.Fprintln(t.w, line)
}

// row writes a single data row. Cells are stringified via fmt.Sprint.
func (t *table) row(cells ...any) {
	parts := make([]string, len(cells))
	for i, c := range cells {
		parts[i] = fmt.Sprint(c)
	}
	fmt.Fprintln(t.w, strings.Join(parts, "\t"))
}

// flush forwards to tabwriter.
func (t *table) flush() error { return t.w.Flush() }

// visibleLen returns rune count for table-separator sizing. Good
// enough for ASCII headers; we don't render anything that needs
// proper grapheme width here.
func visibleLen(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

// ANSI codes — used for separator dimming + nothing else. We never
// colour data cells: it conflicts with --json semantics for callers
// that pipe output into jq | grep.
const (
	ansiDim   = "\x1b[2m"
	ansiReset = "\x1b[0m"
)

// isTerminal is a stdlib-only TTY check: check if stdout is a
// character device. Avoids golang.org/x/term to keep zero deps.
func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return (st.Mode() & os.ModeCharDevice) != 0
}

// formatExpiresAt collapses an optional RFC3339 timestamp into a
// short table cell. nil / empty → "never".
func formatExpiresAt(s *string) string {
	if s == nil || *s == "" {
		return "never"
	}
	return *s
}

// printErr is a thin wrapper that respects --quiet (errors still
// print: --quiet only suppresses prose, not failures). Always to
// stderr; never colours.
func printErr(g *globalFlags, format string, args ...any) {
	_ = g // reserved for future quiet-aware levels
	fmt.Fprintf(os.Stderr, "basin: "+format+"\n", args...)
}

// printInfo respects --quiet so a script driver can ignore noise
// without piping to /dev/null. Always to stderr (stdout is reserved
// for parseable output).
func printInfo(g *globalFlags, format string, args ...any) {
	if g.quiet {
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

// readAllStdin slurps stdin until EOF. Returns "" if no data is
// piped (best-effort: we don't try to detect tty-ness because some
// CI runners pipe an empty stream).
func readAllStdin() (string, error) {
	st, err := os.Stdin.Stat()
	if err != nil {
		return "", err
	}
	if (st.Mode() & os.ModeCharDevice) != 0 {
		// Interactive stdin: nothing piped.
		return "", nil
	}
	b, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// readMaybeFile loads a file into a string when path is non-empty.
// Returns ("", nil) when path is empty.
func readMaybeFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
