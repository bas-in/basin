// cmd_erd — Entity-Relationship Diagram export for a project.
//
//	basin erd export [--project=<ref>] [--format=svg|dot|json] [--output=<path>]
//
// Fetches GET /v1/projects/{ref}/erd?format=<format> and streams the response
// body to --output=<path>, or stdout when --output is absent.
//
// For --format=json the cloud returns:
//
//	{ "tables": [...], "relationships": [...] }
//
// For --format=svg and --format=dot the response body is raw text/SVG or
// Graphviz DOT source. No --json mode for those: we print the source as-is.
//
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// ERDTable is one table entry in the JSON ERD response.
type ERDTable struct {
	Name    string      `json:"name,omitempty"`
	Schema  string      `json:"schema,omitempty"`
	Columns []ERDColumn `json:"columns,omitempty"`
}

// ERDColumn is a column entry inside an ERDTable.
type ERDColumn struct {
	Name     string `json:"name,omitempty"`
	Type     string `json:"type,omitempty"`
	Nullable bool   `json:"nullable,omitempty"`
	PK       bool   `json:"pk,omitempty"`
}

// ERDRelationship describes one inferred or explicit FK relationship.
type ERDRelationship struct {
	FromTable  string `json:"from_table,omitempty"`
	FromColumn string `json:"from_column,omitempty"`
	ToTable    string `json:"to_table,omitempty"`
	ToColumn   string `json:"to_column,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Via        string `json:"via,omitempty"`
}

// ERDResponse is the JSON shape from GET /v1/projects/{ref}/erd?format=json.
type ERDResponse struct {
	Tables        []ERDTable        `json:"tables,omitempty"`
	Relationships []ERDRelationship `json:"relationships,omitempty"`
}

// cmdERD is the top-level handler for `basin erd`.
func cmdERD(g *globalFlags, args []string) error {
	if len(args) == 0 {
		printErr(g, "usage: basin erd export [--project=<ref>] [--format=svg|dot|json] [--output=<path>]")
		return errSilent
	}
	switch args[0] {
	case "export":
		return cmdERDExport(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("erd", "Export the project Entity-Relationship Diagram.", []string{
			"export [--project=<ref>] [--format=svg|dot|json] [--output=<path>]",
			"  --format  Output format: svg (default), dot, or json.",
			"  --output  Write to this path (defaults to stdout).",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for erd", args[0])
		return errSilent
	}
}

// cmdERDExport implements `basin erd export`.
func cmdERDExport(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("erd export", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	format := fs.String("format", "svg", "Output format: svg, dot, or json.")
	output := fs.String("output", "", "Write output to this file (defaults to stdout).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}

	// Validate format.
	switch *format {
	case "svg", "dot", "json":
		// ok
	default:
		return fmt.Errorf("--format must be svg, dot, or json (got %q)", *format)
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}

	// Build URL with format query param.
	qs := "?" + url.Values{"format": {*format}}.Encode()
	path := "/v1/projects/" + ref + "/erd" + qs

	// We use a raw HTTP request here (not c.do) so we can stream the
	// body without JSON-decoding it for svg/dot, and so we can inspect
	// the content-type for the json format.
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	// Accept both JSON and raw text/image types.
	req.Header.Set("Accept", "application/json, image/svg+xml, text/vnd.graphviz, text/plain, */*")

	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		b, _ := io.ReadAll(res.Body)
		ae := &APIError{HTTPStatus: res.StatusCode}
		if len(b) > 0 {
			var nested struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if jerr := json.Unmarshal(b, &nested); jerr == nil && (nested.Error.Code != "" || nested.Error.Message != "") {
				ae.Code = nested.Error.Code
				ae.Message = nested.Error.Message
				return ae
			}
			ae.Message = strings.TrimSpace(string(b))
		}
		return ae
	}

	// Read body.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("erd export: read response: %w", err)
	}

	// For JSON format: if g.json is set, pretty-print; otherwise pass through.
	if *format == "json" {
		if g.json {
			// Decode into typed struct and re-encode for clean JSON shape.
			// JSON shape: { "tables": [...], "relationships": [...] }
			var erd ERDResponse
			// The cloud wraps in {"data":{...}} — unwrap same as c.do does.
			var wrapped struct {
				Data  json.RawMessage `json:"data"`
				Error json.RawMessage `json:"error"`
			}
			if jerr := json.Unmarshal(body, &wrapped); jerr == nil &&
				len(wrapped.Data) > 0 && string(wrapped.Data) != "null" {
				if jerr2 := json.Unmarshal(wrapped.Data, &erd); jerr2 != nil {
					return fmt.Errorf("erd export: decode JSON: %w", jerr2)
				}
			} else {
				if jerr2 := json.Unmarshal(body, &erd); jerr2 != nil {
					return fmt.Errorf("erd export: decode JSON: %w", jerr2)
				}
			}
			return printJSON(os.Stdout, erd)
		}
		// Not --json: pass through the raw JSON body.
	}

	// Determine output destination.
	var dst io.Writer = os.Stdout
	if *output != "" {
		f, err := os.Create(*output)
		if err != nil {
			return fmt.Errorf("erd export: create output file: %w", err)
		}
		defer f.Close()
		dst = f
	}

	_, err = io.Copy(dst, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("erd export: write output: %w", err)
	}
	// Add trailing newline for svg/dot if output is stdout and doesn't end with one.
	if *output == "" && *format != "json" && len(body) > 0 && body[len(body)-1] != '\n' {
		_, _ = fmt.Fprintln(dst)
	}
	return nil
}
