// cmd_domains — custom-domain lifecycle for a project.
//
//	basin domains list   [--project=<ref>]
//	basin domains add    <hostname> [--project=<ref>]
//	basin domains verify <id> [--project=<ref>]
//	basin domains cert   <id> [--project=<ref>]
//	basin domains remove <id> [--project=<ref>] [--yes]
//
// Backed by /v1/projects/{ref}/custom-domains/* (migration 0009).
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

// CustomDomain is the read shape from GET /v1/projects/{ref}/custom-domains.
// JSON shape: { id, hostname, status, cert_status, created_at }
type CustomDomain struct {
	ID         string `json:"id,omitempty"`
	ProjectID  string `json:"project_id,omitempty"`
	Hostname   string `json:"hostname,omitempty"`
	Status     string `json:"status,omitempty"`
	CertStatus string `json:"cert_status,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
}

// CustomDomainDNSRecord is one DNS record the user must configure at
// their registrar. Returned by POST /v1/projects/{ref}/custom-domains.
type CustomDomainDNSRecord struct {
	Type  string `json:"type,omitempty"`
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
	TTL   int    `json:"ttl,omitempty"`
}

// AddDomainResponse is the envelope from POST /v1/projects/{ref}/custom-domains.
// JSON shape: { domain: CustomDomain, dns_records: [ CustomDomainDNSRecord ] }
type AddDomainResponse struct {
	Domain     *CustomDomain           `json:"domain,omitempty"`
	DNSRecords []CustomDomainDNSRecord `json:"dns_records,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdDomains(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdDomainsList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdDomainsList(g, args[1:])
	case "add":
		return cmdDomainsAdd(g, args[1:])
	case "verify":
		return cmdDomainsVerify(g, args[1:])
	case "cert":
		return cmdDomainsCert(g, args[1:])
	case "remove":
		return cmdDomainsRemove(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("domains", "Manage custom domains for a project.", []string{
			"list             [--project=<ref>]           List custom domains.",
			"add   <hostname> [--project=<ref>]           Add a custom domain (prints DNS records).",
			"verify <id>      [--project=<ref>]           Trigger DNS verification.",
			"cert   <id>      [--project=<ref>]           Request TLS certificate issuance.",
			"remove <id>      [--project=<ref>] [--yes]   Remove a custom domain.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for domains", args[0])
		return errSilent
	}
}

// ── domains list ─────────────────────────────────────────────────────

func cmdDomainsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("domains list", flag.ContinueOnError)
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
		Domains []*CustomDomain `json:"domains"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/custom-domains", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { domains: [ CustomDomain ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Domains) == 0 {
		fmt.Fprintln(os.Stdout, "(no custom domains)")
		return nil
	}
	t := newTable(g, "ID", "HOSTNAME", "STATUS", "CERT", "CREATED")
	for _, d := range resp.Domains {
		t.row(d.ID, d.Hostname, d.Status, d.CertStatus, d.CreatedAt)
	}
	return t.flush()
}

// ── domains add ──────────────────────────────────────────────────────

func cmdDomainsAdd(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("domains add", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin domains add <hostname> [--project=<ref>]")
	}
	hostname := rest[0]

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

	body := map[string]any{"hostname": hostname}
	var resp AddDomainResponse
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/custom-domains", body, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 409 {
			return fmt.Errorf("hostname %q is already registered on this project", hostname)
		}
		return err
	}
	if g.json {
		// JSON shape: { domain: CustomDomain, dns_records: [ CustomDomainDNSRecord ] }
		return printJSON(os.Stdout, resp)
	}
	if resp.Domain != nil {
		fmt.Fprintf(os.Stdout, "Added domain %q (id=%s, status=%s)\n",
			resp.Domain.Hostname, resp.Domain.ID, resp.Domain.Status)
	}
	if len(resp.DNSRecords) > 0 {
		fmt.Fprintln(os.Stdout, "\nAdd these DNS records at your registrar:")
		t := newTable(g, "TYPE", "NAME", "VALUE", "TTL")
		for _, r := range resp.DNSRecords {
			ttl := ""
			if r.TTL > 0 {
				ttl = fmt.Sprintf("%d", r.TTL)
			}
			t.row(r.Type, r.Name, r.Value, ttl)
		}
		return t.flush()
	}
	return nil
}

// ── domains verify ───────────────────────────────────────────────────

func cmdDomainsVerify(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("domains verify", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin domains verify <id> [--project=<ref>]")
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
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Verified    bool   `json:"verified"`
		NextCheckAt string `json:"next_check_at,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/custom-domains/"+id+"/verify", nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("domain %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { verified: bool, next_check_at: string }
		return printJSON(os.Stdout, resp)
	}
	if resp.Verified {
		fmt.Fprintf(os.Stdout, "Domain %s verified.\n", id)
	} else {
		fmt.Fprintf(os.Stdout, "Domain %s not yet verified.", id)
		if resp.NextCheckAt != "" {
			fmt.Fprintf(os.Stdout, " Next check at: %s", resp.NextCheckAt)
		}
		fmt.Fprintln(os.Stdout)
	}
	return nil
}

// ── domains cert ─────────────────────────────────────────────────────

func cmdDomainsCert(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("domains cert", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin domains cert <id> [--project=<ref>]")
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
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		CertStatus string `json:"cert_status,omitempty"`
		Issued     bool   `json:"issued"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/custom-domains/"+id+"/cert", nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("domain %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { cert_status: string, issued: bool }
		return printJSON(os.Stdout, resp)
	}
	if resp.Issued {
		fmt.Fprintf(os.Stdout, "Certificate issued for domain %s (status=%s).\n", id, resp.CertStatus)
	} else {
		fmt.Fprintf(os.Stdout, "Certificate issuance requested for domain %s (status=%s).\n", id, resp.CertStatus)
	}
	return nil
}

// ── domains remove ───────────────────────────────────────────────────

func cmdDomainsRemove(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("domains remove", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin domains remove <id> [--project=<ref>] [--yes]")
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
			return fmt.Errorf("confirmation required: pass --yes to remove domain %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Remove custom domain %q from project %q? This cannot be undone. [y/N] ", id, ref)
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
		Removed bool   `json:"removed"`
		ID      string `json:"id,omitempty"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/custom-domains/"+id, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("domain %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { removed: bool, id: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Removed domain %s.\n", id)
	return nil
}
