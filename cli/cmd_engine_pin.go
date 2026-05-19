// cmd_engine_pin — engine version pin lifecycle for a project.
//
//	basin engine-pin get    [--project=<ref>]
//	basin engine-pin put    --version=<v> [--project=<ref>] [--yes]
//	basin engine-pin delete [--project=<ref>] [--yes]
//	basin engine-pin events [--project=<ref>] [--limit=N]
//
// Backed by /v1/projects/{ref}/engine/pin (GET/PUT/DELETE)
// and /v1/projects/{ref}/engine/events (GET).
// The cloud router mounts these under the /engine sub-route of the
// per-project group (server.go ~line 812).
//
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
// Error with hint at `basin link` if neither is set.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// EnginePin is the "pin" object returned by GET/PUT /engine/pin.
// JSON shape: { version, pinned_at, pinned_by }
type EnginePin struct {
	Version  string `json:"version,omitempty"`
	PinnedAt string `json:"pinned_at,omitempty"`
	PinnedBy string `json:"pinned_by,omitempty"`
}

// EnginePinEvent is one entry in GET /engine/events.
// JSON shape: { id, action, version, at, actor_id }
type EnginePinEvent struct {
	ID      string `json:"id,omitempty"`
	Action  string `json:"action,omitempty"`
	Version string `json:"version,omitempty"`
	At      string `json:"at,omitempty"`
	ActorID string `json:"actor_id,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdEnginePin(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("engine-pin", "Get / set / delete the engine version pin for a project.", []string{
			"get    [--project=<ref>]                      Show the current engine version pin.",
			"put    --version=<v> [--project=<ref>] [--yes] Pin the engine to a specific version.",
			"delete [--project=<ref>] [--yes]               Unpin the engine version.",
			"events [--project=<ref>] [--limit=N]           List pin audit events.",
		})
		return nil
	}
	switch args[0] {
	case "get":
		return cmdEnginePinGet(g, args[1:])
	case "put":
		return cmdEnginePinPut(g, args[1:])
	case "delete":
		return cmdEnginePinDelete(g, args[1:])
	case "events":
		return cmdEnginePinEvents(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("engine-pin", "Get / set / delete the engine version pin for a project.", []string{
			"get    [--project=<ref>]                      Show the current engine version pin.",
			"put    --version=<v> [--project=<ref>] [--yes] Pin the engine to a specific version.",
			"delete [--project=<ref>] [--yes]               Unpin the engine version.",
			"events [--project=<ref>] [--limit=N]           List pin audit events.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for engine-pin", args[0])
		return errSilent
	}
}

// ── engine-pin get ───────────────────────────────────────────────────

func cmdEnginePinGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("engine-pin get", flag.ContinueOnError)
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

	var resp struct {
		Pin *EnginePin `json:"pin"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/engine/pin", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { pin: EnginePin | null }
		return printJSON(os.Stdout, resp)
	}
	if resp.Pin == nil {
		fmt.Fprintln(os.Stdout, "(no engine version pin set)")
		return nil
	}
	fmt.Fprintf(os.Stdout, "version:   %s\n", resp.Pin.Version)
	fmt.Fprintf(os.Stdout, "pinned_at: %s\n", resp.Pin.PinnedAt)
	fmt.Fprintf(os.Stdout, "pinned_by: %s\n", resp.Pin.PinnedBy)
	return nil
}

// ── engine-pin put ───────────────────────────────────────────────────

func cmdEnginePinPut(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("engine-pin put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	ver := fs.String("version", "", "Engine version to pin (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *ver == "" {
		return fmt.Errorf("engine-pin put requires --version=<version>")
	}

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to pin engine version %q non-interactively under --json", *ver)
		}
		fmt.Fprintf(os.Stderr, "Pin engine to version %q on project %q? [y/N] ", *ver, ref)
		line, err := readLine()
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			fmt.Fprintln(os.Stdout, "Aborted.")
			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	body := map[string]any{"version": *ver}
	var resp struct {
		Pin *EnginePin `json:"pin"`
	}
	if err := c.do(ctx, "PUT", "/v1/projects/"+ref+"/engine/pin", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { pin: EnginePin }
		return printJSON(os.Stdout, resp)
	}
	if resp.Pin != nil {
		fmt.Fprintf(os.Stdout, "Pinned engine to version %s.\n", resp.Pin.Version)
	} else {
		fmt.Fprintf(os.Stdout, "Pinned engine to version %s.\n", *ver)
	}
	return nil
}

// ── engine-pin delete ────────────────────────────────────────────────

func cmdEnginePinDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("engine-pin delete", flag.ContinueOnError)
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
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to unpin engine version non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Remove the engine version pin from project %q? [y/N] ", ref)
		line, err := readLine()
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			fmt.Fprintln(os.Stdout, "Aborted.")
			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Unpinned bool `json:"unpinned"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/engine/pin", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { unpinned: true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "Engine version pin removed.")
	return nil
}

// ── engine-pin events ────────────────────────────────────────────────

func cmdEnginePinEvents(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("engine-pin events", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	limit := fs.Int("limit", 50, "Maximum number of events to return.")
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

	q := queryString(map[string]string{"limit": fmt.Sprintf("%d", *limit)})
	var resp struct {
		Events []*EnginePinEvent `json:"events"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/engine/events"+q, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { events: [ EnginePinEvent ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Events) == 0 {
		fmt.Fprintln(os.Stdout, "(no engine pin events)")
		return nil
	}
	t := newTable(g, "ID", "ACTION", "VERSION", "AT", "ACTOR_ID")
	for _, ev := range resp.Events {
		t.row(ev.ID, ev.Action, ev.Version, ev.At, ev.ActorID)
	}
	return t.flush()
}
