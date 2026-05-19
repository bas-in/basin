// cmd_transfers — project + org-ownership transfer flows.
//
//	basin projects transfers list                              [--project <ref>]
//	basin projects transfers create --to-org=<slug>            [--project <ref>] [--yes]
//	basin projects transfers cancel <id>                       [--project <ref>] [--yes]
//
//	basin orgs ownership-transfers list <slug>
//	basin orgs ownership-transfers create --to-user-id=<id> <slug> [--yes]
//	basin orgs ownership-transfers cancel <slug> <id>          [--yes]
//	basin orgs ownership-transfers accept <slug> <id>
//	basin orgs ownership-transfers decline <slug> <id>
//
//	basin orgs incoming-project-transfers list <slug>
//	basin orgs incoming-project-transfers accept <slug> <id>
//	basin orgs incoming-project-transfers decline <slug> <id>
//
// The flat-package layout lets cmdProjects + cmdOrgs route into the
// sub-dispatchers here without an extra file-level seam.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ProjectTransfer mirrors a single row from
// GET /v1/projects/{ref}/transfers.
type ProjectTransfer struct {
	ID          string `json:"id,omitempty"`
	FromOrgSlug string `json:"from_org_slug,omitempty"`
	ToOrgSlug   string `json:"to_org_slug,omitempty"`
	Status      string `json:"status,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
}

// OrgOwnershipTransfer mirrors a single row from
// GET /v1/orgs/{slug}/ownership-transfers.
type OrgOwnershipTransfer struct {
	ID        string `json:"id,omitempty"`
	ToUserID  string `json:"to_user_id,omitempty"`
	Status    string `json:"status,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

// IncomingProjectTransfer mirrors a single row from
// GET /v1/orgs/{slug}/incoming-project-transfers. The receiving-org
// side sees the from-org + the project that's headed their way.
type IncomingProjectTransfer struct {
	ID          string `json:"id,omitempty"`
	FromOrgSlug string `json:"from_org_slug,omitempty"`
	ToOrgSlug   string `json:"to_org_slug,omitempty"`
	ProjectRef  string `json:"project_ref,omitempty"`
	Status      string `json:"status,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
}

// cmdProjectsTransfers routes `basin projects transfers <sub> …` into
// list / create / cancel. Called from cmdProjects.
func cmdProjectsTransfers(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdProjectsTransfersList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdProjectsTransfersList(g, args[1:])
	case "create":
		return cmdProjectsTransfersCreate(g, args[1:])
	case "cancel":
		return cmdProjectsTransfersCancel(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("projects transfers", "Project transfer requests between orgs.", []string{
			"list                              List pending + historical transfers for a project.",
			"create --to-org <slug> [--yes]    Initiate a transfer to another org.",
			"cancel <id> [--yes]               Cancel a pending transfer you initiated.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for projects transfers", args[0])
		return errSilent
	}
}

func cmdProjectsTransfersList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("projects transfers list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (else cwd binding).")
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
		Transfers []*ProjectTransfer `json:"transfers"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/transfers", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { transfers: [ProjectTransfer] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Transfers) == 0 {
		fmt.Fprintln(os.Stdout, "(no transfers)")
		return nil
	}
	for _, t := range resp.Transfers {
		fmt.Fprintf(os.Stdout, "%s\t%s\t%s → %s\t%s\n", t.ID, t.Status, t.FromOrgSlug, t.ToOrgSlug, t.CreatedAt)
	}
	return nil
}

func cmdProjectsTransfersCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("projects transfers create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (else cwd binding).")
	toOrg := fs.String("to-org", "", "Destination org slug (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if strings.TrimSpace(*toOrg) == "" {
		return fmt.Errorf("projects transfers create requires --to-org=<slug>")
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
			return fmt.Errorf("confirmation required: pass --yes to initiate a transfer non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Initiate transfer of project %q to org %q? [y/N] ", ref, *toOrg)
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

	body := map[string]any{"to_org_slug": *toOrg}
	var resp struct {
		Transfer *ProjectTransfer `json:"transfer"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/transfers", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { transfer: ProjectTransfer }
		return printJSON(os.Stdout, resp)
	}
	if resp.Transfer != nil {
		fmt.Fprintf(os.Stdout, "Transfer initiated: %s → %s (id=%s, status=%s)\n", ref, *toOrg, resp.Transfer.ID, resp.Transfer.Status)
	} else {
		fmt.Fprintf(os.Stdout, "Transfer initiated: %s → %s\n", ref, *toOrg)
	}
	return nil
}

func cmdProjectsTransfersCancel(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("projects transfers cancel", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (else cwd binding).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin projects transfers cancel <id> [--project=<ref>] [--yes]")
	}
	id := rest[0]
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
			return fmt.Errorf("confirmation required: pass --yes to cancel transfer %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Cancel transfer %q for project %q? [y/N] ", id, ref)
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
		Cancelled bool   `json:"cancelled"`
		ID        string `json:"id"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/transfers/"+id+"/cancel", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { cancelled: bool, id: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Cancelled transfer %q.\n", id)
	return nil
}

// cmdOrgsOwnershipTransfers routes `basin orgs ownership-transfers <sub>`.
// Called from cmdOrgs.
func cmdOrgsOwnershipTransfers(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin orgs ownership-transfers <list|create|cancel|accept|decline> <slug> [args]")
	}
	switch args[0] {
	case "list":
		return cmdOrgsOwnershipTransfersList(g, args[1:])
	case "create":
		return cmdOrgsOwnershipTransfersCreate(g, args[1:])
	case "cancel":
		return cmdOrgsOwnershipTransfersCancel(g, args[1:])
	case "accept":
		return cmdOrgsOwnershipTransfersAccept(g, args[1:])
	case "decline":
		return cmdOrgsOwnershipTransfersDecline(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("orgs ownership-transfers", "Org-owner handover requests.", []string{
			"list <slug>                                   List ownership transfers.",
			"create --to-user-id <id> <slug> [--yes]       Initiate handover (owner only).",
			"cancel <slug> <id> [--yes]                    Cancel a pending handover you initiated.",
			"accept <slug> <id>                            Accept an incoming handover.",
			"decline <slug> <id>                           Decline an incoming handover.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for orgs ownership-transfers", args[0])
		return errSilent
	}
}

func cmdOrgsOwnershipTransfersList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs ownership-transfers list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin orgs ownership-transfers list <slug>")
	}
	slug := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Transfers []*OrgOwnershipTransfer `json:"transfers"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+slug+"/ownership-transfers", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { transfers: [OrgOwnershipTransfer] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Transfers) == 0 {
		fmt.Fprintln(os.Stdout, "No ownership transfers.")
		return nil
	}
	for _, t := range resp.Transfers {
		fmt.Fprintf(os.Stdout, "%s\t%s\t→ user %s\t%s\n", t.ID, t.Status, t.ToUserID, t.CreatedAt)
	}
	return nil
}

func cmdOrgsOwnershipTransfersCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs ownership-transfers create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	toUser := fs.String("to-user-id", "", "Recipient user id (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin orgs ownership-transfers create --to-user-id=<id> <slug> [--yes]")
	}
	slug := rest[0]
	if strings.TrimSpace(*toUser) == "" {
		return fmt.Errorf("orgs ownership-transfers create requires --to-user-id=<id>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to initiate ownership transfer non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Initiate ownership transfer of org %q to user %q? [y/N] ", slug, *toUser)
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

	body := map[string]any{"to_user_id": *toUser}
	var resp struct {
		Transfer *OrgOwnershipTransfer `json:"transfer"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+slug+"/ownership-transfers", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { transfer: OrgOwnershipTransfer }
		return printJSON(os.Stdout, resp)
	}
	if resp.Transfer != nil {
		fmt.Fprintf(os.Stdout, "Ownership transfer initiated: org %q → user %q (id=%s, status=%s)\n", slug, *toUser, resp.Transfer.ID, resp.Transfer.Status)
	} else {
		fmt.Fprintf(os.Stdout, "Ownership transfer initiated: org %q → user %q\n", slug, *toUser)
	}
	return nil
}

func cmdOrgsOwnershipTransfersCancel(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs ownership-transfers cancel", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) < 2 {
		return fmt.Errorf("usage: basin orgs ownership-transfers cancel <slug> <id> [--yes]")
	}
	slug, id := rest[0], rest[1]
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to cancel ownership transfer %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Cancel ownership transfer %q for org %q? [y/N] ", id, slug)
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
		Cancelled bool   `json:"cancelled"`
		ID        string `json:"id"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+slug+"/ownership-transfers/"+id+"/cancel", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { cancelled: bool, id: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Cancelled ownership transfer %q.\n", id)
	return nil
}

func cmdOrgsOwnershipTransfersAccept(g *globalFlags, args []string) error {
	return ownershipTransferAcceptDecline(g, args, "accept")
}

func cmdOrgsOwnershipTransfersDecline(g *globalFlags, args []string) error {
	return ownershipTransferAcceptDecline(g, args, "decline")
}

// ownershipTransferAcceptDecline factors the accept/decline pair —
// identical except for the trailing path segment and the prose verb.
func ownershipTransferAcceptDecline(g *globalFlags, args []string, action string) error {
	fs := flag.NewFlagSet("orgs ownership-transfers "+action, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) < 2 {
		return fmt.Errorf("usage: basin orgs ownership-transfers %s <slug> <id>", action)
	}
	slug, id := rest[0], rest[1]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	// Response envelope key matches the action verb ("accepted" /
	// "declined") to match the cloud's existing handler shape.
	var resp struct {
		Accepted bool   `json:"accepted,omitempty"`
		Declined bool   `json:"declined,omitempty"`
		ID       string `json:"id"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+slug+"/ownership-transfers/"+id+"/"+action, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { accepted: bool, id: string } | { declined: bool, id: string }
		return printJSON(os.Stdout, resp)
	}
	if action == "accept" {
		fmt.Fprintf(os.Stdout, "Accepted ownership transfer %q.\n", id)
	} else {
		fmt.Fprintf(os.Stdout, "Declined ownership transfer %q.\n", id)
	}
	return nil
}

// cmdOrgsIncomingProjectTransfers routes
// `basin orgs incoming-project-transfers <sub>`.
func cmdOrgsIncomingProjectTransfers(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin orgs incoming-project-transfers <list|accept|decline> <slug> [id]")
	}
	switch args[0] {
	case "list":
		return cmdOrgsIncomingProjectTransfersList(g, args[1:])
	case "accept":
		return incomingProjectTransferAcceptDecline(g, args[1:], "accept")
	case "decline":
		return incomingProjectTransferAcceptDecline(g, args[1:], "decline")
	case "--help", "-h", "help":
		helpForCommand("orgs incoming-project-transfers", "Inbound project handovers from other orgs.", []string{
			"list <slug>                  List incoming project transfers.",
			"accept <slug> <id>           Accept a handover into this org.",
			"decline <slug> <id>          Decline a handover.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for orgs incoming-project-transfers", args[0])
		return errSilent
	}
}

func cmdOrgsIncomingProjectTransfersList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs incoming-project-transfers list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin orgs incoming-project-transfers list <slug>")
	}
	slug := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Transfers []*IncomingProjectTransfer `json:"transfers"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+slug+"/incoming-project-transfers", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { transfers: [IncomingProjectTransfer] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Transfers) == 0 {
		fmt.Fprintln(os.Stdout, "No incoming project transfers.")
		return nil
	}
	for _, t := range resp.Transfers {
		fmt.Fprintf(os.Stdout, "%s\t%s\t%s → %s\tproject=%s\t%s\n", t.ID, t.Status, t.FromOrgSlug, t.ToOrgSlug, t.ProjectRef, t.CreatedAt)
	}
	return nil
}

func incomingProjectTransferAcceptDecline(g *globalFlags, args []string, action string) error {
	fs := flag.NewFlagSet("orgs incoming-project-transfers "+action, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) < 2 {
		return fmt.Errorf("usage: basin orgs incoming-project-transfers %s <slug> <id>", action)
	}
	slug, id := rest[0], rest[1]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Accepted bool   `json:"accepted,omitempty"`
		Declined bool   `json:"declined,omitempty"`
		ID       string `json:"id"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+slug+"/incoming-project-transfers/"+id+"/"+action, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { accepted: bool, id: string } | { declined: bool, id: string }
		return printJSON(os.Stdout, resp)
	}
	if action == "accept" {
		fmt.Fprintf(os.Stdout, "Accepted incoming project transfer %q.\n", id)
	} else {
		fmt.Fprintf(os.Stdout, "Declined incoming project transfer %q.\n", id)
	}
	return nil
}
