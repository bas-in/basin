// cmd_metrics — project and org-level metrics surface.
//
//	basin metrics [--project=<ref>] [--org=<slug>] [--range=24h|7d|30d] [--metric=<name>]
//
// Exactly one of --project (or a working project) and --org must resolve.
// Mutually exclusive: if both resolve, the command errors.
//
// Project-level: GET /v1/projects/{ref}/metrics?range=...&metric=...
// Org-level:     GET /v1/orgs/{slug}/metrics?range=...&metric=...
//
// JSON shape (project and org paths share the same envelope):
//
//	{ "metrics": [{ "name": string, "value": number, "unit": string, "timestamp": string }] }
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// Metric is one data point returned by the /metrics endpoints.
type Metric struct {
	Name      string  `json:"name,omitempty"`
	Value     float64 `json:"value,omitempty"`
	Unit      string  `json:"unit,omitempty"`
	Timestamp string  `json:"timestamp,omitempty"`
}

// cmdMetrics is the top-level handler for `basin metrics`.
func cmdMetrics(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("metrics", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	orgSlug := fs.String("org", "", "Org slug (mutually exclusive with --project).")
	rng := fs.String("range", "", "Time range: 24h, 7d, or 30d.")
	metric := fs.String("metric", "", "Filter to a single named metric.")

	if len(args) > 0 && (args[0] == "--help" || args[0] == "-h" || args[0] == "help") {
		helpForCommand("metrics", "Fetch project or org-level metrics.", []string{
			"[--project=<ref>] [--org=<slug>] [--range=24h|7d|30d] [--metric=<name>]",
			"Exactly one of --project (or working project) and --org must be set.",
		})
		return nil
	}

	if err := fs.Parse(args); err != nil {
		return errSilent
	}

	// Resolve org flag — prefer explicit --org flag, then fall back to
	// g.orgSlug set by the global --org= flag.
	org := *orgSlug
	if org == "" {
		org = g.orgSlug
	}

	// Resolve project ref (flag or working project).
	var ref string
	if *project != "" {
		ref = *project
	} else if org == "" {
		// No --org and no --project: try the working project.
		cwd, cwdErr := os.Getwd()
		if cwdErr == nil {
			wp, _ := loadWorkingProject(cwd)
			if wp != nil && wp.ProjectRef != "" {
				ref = wp.ProjectRef
			}
		}
	}

	// Mutual-exclusivity: both can't be set.
	if ref != "" && org != "" {
		return fmt.Errorf("--project and --org are mutually exclusive: use one or the other")
	}
	// Neither resolved.
	if ref == "" && org == "" {
		return fmt.Errorf("one of --project or --org is required (or run `basin link` to bind this directory)")
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	// Build query string.
	qs := queryString(map[string]string{
		"range":  *rng,
		"metric": *metric,
	})

	var path string
	if ref != "" {
		path = "/v1/projects/" + ref + "/metrics" + qs
	} else {
		path = "/v1/orgs/" + org + "/metrics" + qs
	}

	var resp struct {
		Metrics []Metric `json:"metrics"`
	}
	if err := c.do(ctx, "GET", path, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 403 {
			return fmt.Errorf("access denied: check that your token has read access to this resource")
		}
		return err
	}

	if g.json {
		// JSON shape: { "metrics": [{ "name": string, "value": number, "unit": string, "timestamp": string }] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Metrics) == 0 {
		fmt.Fprintln(os.Stdout, "(no metrics)")
		return nil
	}
	t := newTable(g, "NAME", "VALUE", "UNIT", "TIMESTAMP")
	for _, m := range resp.Metrics {
		t.row(m.Name, fmt.Sprintf("%g", m.Value), m.Unit, m.Timestamp)
	}
	return t.flush()
}
