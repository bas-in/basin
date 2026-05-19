// cmd_db — daily-driver database workflow.
//
//	basin db push  [--project=<ref>] [--quiet]
//	basin db pull  [--project=<ref>] [--force]
//	basin db diff  [--project=<ref>]
//	basin db reset [--project=<ref>] [--yes]
//	basin db url   [--project=<ref>] [--reveal] [--rotate] [--yes]
//	basin db dump  [--project=<ref>] [--schema-only|--data-only]
//	basin db lint  [--project=<ref>]
//
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
// All migration calls use /admin/v1/projects/{ref}/migrations per decisions.md.
// Snapshot safety-net uses POST /v1/projects/{ref}/backups/snapshots.
// SQL execution uses POST /v1/projects/{ref}/sql/query.
// pgwire credentials: GET /v1/projects/{ref}/pgwire, POST .../reveal, POST .../rotate.
//
// TODO(cloud-route-move): migration endpoints use /admin/v1/ — switch to /v1/ when
// the cloud router mirrors them under /v1/projects/{ref}/migrations.
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ── dispatcher ────────────────────────────────────────────────────────

func cmdDB(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("db", "Database workflow: push / pull / diff / reset / url / dump / lint.", []string{
			"push   [--project=<ref>]                     Apply local migrations to the remote project.",
			"pull   [--project=<ref>] [--force]           Write remote migrations to ./basin/migrations/.",
			"diff   [--project=<ref>]                     Generate a new migration from schema drift.",
			"reset  [--project=<ref>] [--yes]             Snapshot → rollback all → re-apply + seed.",
			"url    [--project=<ref>] [--reveal] [--rotate] [--yes]  Print the pgwire DSN.",
			"dump   [--project=<ref>] [--schema-only|--data-only]    pg_dump-shaped output to stdout.",
			"lint   [--project=<ref>]                     Dry-run each ./basin/migrations/*.sql file.",
		})
		return nil
	}
	switch args[0] {
	case "push":
		return cmdDBPush(g, args[1:])
	case "pull":
		return cmdDBPull(g, args[1:])
	case "diff":
		return cmdDBDiff(g, args[1:])
	case "reset":
		return cmdDBReset(g, args[1:])
	case "url":
		return cmdDBURL(g, args[1:])
	case "dump":
		return cmdDBDump(g, args[1:])
	case "lint":
		return cmdDBLint(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("db", "Database workflow: push / pull / diff / reset / url / dump / lint.", []string{
			"push   [--project=<ref>]                     Apply local migrations to the remote project.",
			"pull   [--project=<ref>] [--force]           Write remote migrations to ./basin/migrations/.",
			"diff   [--project=<ref>]                     Generate a new migration from schema drift.",
			"reset  [--project=<ref>] [--yes]             Snapshot → rollback all → re-apply + seed.",
			"url    [--project=<ref>] [--reveal] [--rotate] [--yes]  Print the pgwire DSN.",
			"dump   [--project=<ref>] [--schema-only|--data-only]    pg_dump-shaped output to stdout.",
			"lint   [--project=<ref>]                     Dry-run each ./basin/migrations/*.sql file.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for db", args[0])
		return errSilent
	}
}

// ── toMigrationFilename ───────────────────────────────────────────────
//
// Copied from cmd_migrations.go's toSlug + timestamp naming pattern.
// Duplication is intentional: decisions.md says copy is fine for
// helpers shared across independent files in the same package.
func toMigrationFilename(name string) string {
	slug := toSlug(name)
	now := time.Now().UTC()
	ts := now.Unix()
	return fmt.Sprintf("%d_%s.sql", ts, slug)
}

// ── db push ───────────────────────────────────────────────────────────

func cmdDBPush(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("db push", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
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

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("db push: cwd: %w", err)
	}
	localFiles, err := collectLocalMigrations(cwd)
	if err != nil {
		return fmt.Errorf("db push: list local migrations: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Fetch already-applied migrations.
	var listResp struct {
		Migrations []MigrationRow `json:"migrations"`
	}
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations", nil, &listResp); err != nil {
		return fmt.Errorf("db push: list remote migrations: %w", err)
	}

	// Build set of already-applied filenames.
	applied := make(map[string]struct{}, len(listResp.Migrations))
	for _, m := range listResp.Migrations {
		key := migrationKey(m)
		applied[key] = struct{}{}
	}

	type appliedEntry struct {
		Filename string `json:"filename"`
		ID       string `json:"id"`
	}
	type skippedEntry struct {
		Filename string `json:"filename"`
		Reason   string `json:"reason"`
	}
	var appliedList []appliedEntry
	var skippedList []skippedEntry

	for _, path := range localFiles {
		base := filepath.Base(path)
		if _, ok := applied[base]; ok {
			skippedList = append(skippedList, skippedEntry{Filename: base, Reason: "already applied"})
			continue
		}
		sql, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("db push: read %s: %w", base, err)
		}
		body := map[string]any{
			"source": base,
			"sql":    string(sql),
		}
		var createResp struct {
			ID     string `json:"id"`
			Source string `json:"source"`
		}
		if err := c.do(ctx, "POST", "/admin/v1/projects/"+ref+"/migrations", body, &createResp); err != nil {
			return fmt.Errorf("db push: apply %s: %w", base, err)
		}
		appliedList = append(appliedList, appliedEntry{Filename: base, ID: createResp.ID})
		if !g.quiet {
			printInfo(g, "applied %s", base)
		}
	}

	if g.json {
		// JSON shape: { "applied": [{ "filename": "...", "id": "..." }], "skipped": [{ "filename": "...", "reason": "already applied" }] }
		return printJSON(os.Stdout, map[string]any{
			"applied": appliedList,
			"skipped": skippedList,
		})
	}
	fmt.Fprintf(os.Stdout, "applied: %d  skipped: %d\n", len(appliedList), len(skippedList))
	return nil
}

// ── db pull ───────────────────────────────────────────────────────────

func cmdDBPull(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("db pull", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	force := fs.Bool("force", false, "Overwrite local files even when they differ from the remote.")
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

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var listResp struct {
		Migrations []MigrationRow `json:"migrations"`
	}
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations", nil, &listResp); err != nil {
		return fmt.Errorf("db pull: list remote migrations: %w", err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("db pull: cwd: %w", err)
	}
	migrationsDir := filepath.Join(cwd, "basin", "migrations")
	if err := os.MkdirAll(migrationsDir, 0o755); err != nil {
		return fmt.Errorf("db pull: create migrations dir: %w", err)
	}

	type pulledEntry struct {
		Filename string `json:"filename"`
	}
	type skippedEntry struct {
		Filename string `json:"filename"`
		Reason   string `json:"reason"`
	}
	var pulledList []pulledEntry
	var skippedList []skippedEntry

	for _, m := range listResp.Migrations {
		// Fetch full migration SQL.
		var detail MigrationDetail
		if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations/"+m.ID, nil, &detail); err != nil {
			return fmt.Errorf("db pull: fetch migration %s: %w", m.ID, err)
		}
		filename := detail.Source
		if filename == "" {
			filename = detail.ID + ".sql"
		}
		// Use only the basename.
		filename = filepath.Base(filename)
		if !strings.HasSuffix(filename, ".sql") {
			filename += ".sql"
		}
		destPath := filepath.Join(migrationsDir, filename)
		remote := []byte(detail.SQL)

		// Check if local file exists and differs.
		if existing, err := os.ReadFile(destPath); err == nil {
			localHash := sha256.Sum256(existing)
			remoteHash := sha256.Sum256(remote)
			if localHash == remoteHash {
				skippedList = append(skippedList, skippedEntry{Filename: filename, Reason: "identical"})
				continue
			}
			if !*force {
				skippedList = append(skippedList, skippedEntry{Filename: filename, Reason: "local differs (use --force to overwrite)"})
				continue
			}
		}

		if err := os.WriteFile(destPath, remote, 0o644); err != nil {
			return fmt.Errorf("db pull: write %s: %w", filename, err)
		}
		pulledList = append(pulledList, pulledEntry{Filename: filename})
		if !g.quiet {
			printInfo(g, "pulled %s", filename)
		}
	}

	if g.json {
		// JSON shape: { "pulled": [{ "filename": "..." }], "skipped": [{ "filename": "...", "reason": "..." }] }
		return printJSON(os.Stdout, map[string]any{
			"pulled":  pulledList,
			"skipped": skippedList,
		})
	}
	fmt.Fprintf(os.Stdout, "pulled: %d  skipped: %d\n", len(pulledList), len(skippedList))
	return nil
}

// ── db diff ───────────────────────────────────────────────────────────

func cmdDBDiff(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("db diff", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var diffResp map[string]any
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations/diff", nil, &diffResp); err != nil {
		return fmt.Errorf("db diff: %w", err)
	}

	// Extract diff SQL from the response.
	diffSQL := ""
	for _, key := range []string{"diff_sql", "diff", "sql", "body"} {
		if v, ok := diffResp[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				diffSQL = s
				break
			}
		}
	}

	if diffSQL == "" {
		if g.json {
			// JSON shape: { "wrote": null, "reason": "no-diff" }
			return printJSON(os.Stdout, map[string]any{
				"wrote":  nil,
				"reason": "no-diff",
			})
		}
		fmt.Fprintln(os.Stdout, "no schema drift")
		return nil
	}

	// Write as a new timestamped migration file.
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("db diff: cwd: %w", err)
	}
	dir := filepath.Join(cwd, "basin", "migrations")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("db diff: create migrations dir: %w", err)
	}

	// Use same collision-avoidance as migrations new.
	slug := "schema_drift"
	now := time.Now().UTC()
	ts := now.Unix()
	var destPath string
	for {
		filename := fmt.Sprintf("%d_%s.sql", ts, slug)
		destPath = filepath.Join(dir, filename)
		if _, err := os.Stat(destPath); os.IsNotExist(err) {
			break
		}
		ts++
	}

	if err := os.WriteFile(destPath, []byte(diffSQL), 0o644); err != nil {
		return fmt.Errorf("db diff: write migration: %w", err)
	}

	if g.json {
		// JSON shape: { "wrote": "<path>" }
		return printJSON(os.Stdout, map[string]any{
			"wrote": destPath,
		})
	}
	fmt.Fprintf(os.Stdout, "wrote %s\n", destPath)
	return nil
}

// ── db reset ──────────────────────────────────────────────────────────

func cmdDBReset(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("db reset", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
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
			return fmt.Errorf("confirmation required: pass --yes to reset non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Reset project %q? This will snapshot, rollback all migrations, re-apply local ones, and run seed.sql. [y/N] ", ref)
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
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// (a) Safety snapshot.
	label := fmt.Sprintf("pre-reset-%d", time.Now().Unix())
	var snapResp struct {
		ID string `json:"id"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/backups/snapshots", map[string]any{"label": label}, &snapResp); err != nil {
		return fmt.Errorf("db reset: create snapshot: %w", err)
	}
	snapshotID := snapResp.ID

	// (b) List existing migrations (newest first = reverse lex).
	var listResp struct {
		Migrations []MigrationRow `json:"migrations"`
	}
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations", nil, &listResp); err != nil {
		return fmt.Errorf("db reset: list migrations: %w", err)
	}
	// Roll back newest-first (reverse the list).
	migrations := listResp.Migrations
	for i, j := 0, len(migrations)-1; i < j; i, j = i+1, j-1 {
		migrations[i], migrations[j] = migrations[j], migrations[i]
	}
	rolledBack := 0
	for _, m := range migrations {
		if err := c.do(ctx, "POST", "/admin/v1/projects/"+ref+"/migrations/"+m.ID+"/rollback", nil, nil); err != nil {
			return fmt.Errorf("db reset: rollback migration %s: %w", m.ID, err)
		}
		rolledBack++
	}

	// (c) Re-apply local migrations.
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("db reset: cwd: %w", err)
	}
	localFiles, err := collectLocalMigrations(cwd)
	if err != nil {
		return fmt.Errorf("db reset: list local migrations: %w", err)
	}
	sort.Strings(localFiles)
	appliedCount := 0
	for _, path := range localFiles {
		base := filepath.Base(path)
		sql, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("db reset: read %s: %w", base, err)
		}
		body := map[string]any{"source": base, "sql": string(sql)}
		if err := c.do(ctx, "POST", "/admin/v1/projects/"+ref+"/migrations", body, nil); err != nil {
			return fmt.Errorf("db reset: apply %s: %w", base, err)
		}
		appliedCount++
	}

	// (d) Run seed.sql if non-empty.
	seeded := false
	seedPath := filepath.Join(cwd, "basin", "seed.sql")
	if seedBytes, err := os.ReadFile(seedPath); err == nil {
		seedSQL := strings.TrimSpace(string(seedBytes))
		if seedSQL != "" && seedSQL != "-- seed data" {
			body := map[string]any{"sql": seedSQL, "writes_enabled": true}
			if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/sql/query", body, nil); err != nil {
				return fmt.Errorf("db reset: run seed.sql: %w", err)
			}
			seeded = true
		}
	}

	if g.json {
		// JSON shape: { "snapshot_id": "...", "rolled_back": N, "applied": N, "seeded": bool }
		return printJSON(os.Stdout, map[string]any{
			"snapshot_id": snapshotID,
			"rolled_back": rolledBack,
			"applied":     appliedCount,
			"seeded":      seeded,
		})
	}
	fmt.Fprintf(os.Stdout, "snapshot: %s\nrolled_back: %d\napplied: %d\nseeded: %v\n",
		snapshotID, rolledBack, appliedCount, seeded)
	return nil
}

// ── db url ────────────────────────────────────────────────────────────

func cmdDBURL(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("db url", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	reveal := fs.Bool("reveal", false, "Show the full unmasked DSN (one-time reveal).")
	rotate := fs.Bool("rotate", false, "Rotate the pgwire password and print the new DSN.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt for --rotate.")
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if *rotate {
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
		var resp PgwireRotateResponse
		if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/pgwire/rotate", nil, &resp); err != nil {
			return fmt.Errorf("db url rotate: %w", err)
		}
		dsn := buildRotateDSN(&resp)
		if g.json {
			// JSON shape: { "dsn": "..." } — new DSN after rotation.
			return printJSON(os.Stdout, map[string]string{"dsn": dsn})
		}
		fmt.Fprintln(os.Stdout, dsn)
		return nil
	}

	if *reveal {
		var resp PgwireRevealResponse
		if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/pgwire/reveal", nil, &resp); err != nil {
			return fmt.Errorf("db url reveal: %w", err)
		}
		dsn := buildDSN(&resp)
		if g.json {
			// JSON shape: { "dsn": "..." } — full unmasked DSN.
			return printJSON(os.Stdout, map[string]string{"dsn": dsn})
		}
		fmt.Fprintln(os.Stdout, dsn)
		return nil
	}

	// Default: show masked DSN.
	var creds PgwireCredentials
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/pgwire", nil, &creds); err != nil {
		return fmt.Errorf("db url show: %w", err)
	}
	host := creds.Host
	if host == "" {
		host = "localhost"
	}
	port := creds.Port
	if port == 0 {
		port = 5432
	}
	sslmode := creds.SSLMode
	if sslmode == "" {
		sslmode = "require"
	}
	maskedPW := maskPassword(creds.Password)
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(creds.User, maskedPW),
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     "/" + creds.DBName,
		RawQuery: "sslmode=" + sslmode,
	}
	dsn := u.String()
	if g.json {
		// JSON shape: { "dsn": "..." } — masked password.
		return printJSON(os.Stdout, map[string]string{"dsn": dsn})
	}
	fmt.Fprintln(os.Stdout, dsn)
	return nil
}

// ── db dump ───────────────────────────────────────────────────────────

func cmdDBDump(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("db dump", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	schemaOnly := fs.Bool("schema-only", false, "Emit only CREATE TABLE statements.")
	dataOnly := fs.Bool("data-only", false, "Emit only INSERT statements.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *schemaOnly && *dataOnly {
		return fmt.Errorf("db dump: --schema-only and --data-only are mutually exclusive")
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	emitSchema := !*dataOnly
	emitData := !*schemaOnly

	// ── schema ───────────────────────────────────────────────────────
	// Query information_schema.columns for all public tables.
	schemaSQL := `SELECT table_name, column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position`

	type colInfo struct {
		Table      string
		Column     string
		DataType   string
		IsNullable string
		OrdinalPos int
	}

	// We always fetch the schema to know which tables exist.
	var schemaResult QueryResult
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/sql/query",
		map[string]any{"sql": schemaSQL, "writes_enabled": false}, &schemaResult); err != nil {
		return fmt.Errorf("db dump: query schema: %w", err)
	}

	// Parse column info.
	tables := make(map[string][]colInfo)
	tableOrder := []string{}
	for _, row := range schemaResult.Rows {
		if len(row) < 5 {
			continue
		}
		tableName := fmt.Sprint(row[0])
		colName := fmt.Sprint(row[1])
		dataType := fmt.Sprint(row[2])
		isNullable := fmt.Sprint(row[3])
		ordPos := 0
		if v, ok := row[4].(float64); ok {
			ordPos = int(v)
		}
		if _, seen := tables[tableName]; !seen {
			tableOrder = append(tableOrder, tableName)
			tables[tableName] = nil
		}
		tables[tableName] = append(tables[tableName], colInfo{
			Table:      tableName,
			Column:     colName,
			DataType:   dataType,
			IsNullable: isNullable,
			OrdinalPos: ordPos,
		})
	}

	w := io.Writer(os.Stdout)

	if emitSchema {
		fmt.Fprintln(w, "-- basin db dump — schema")
		fmt.Fprintln(w)
		for _, tbl := range tableOrder {
			cols := tables[tbl]
			fmt.Fprintf(w, "CREATE TABLE IF NOT EXISTS %s (\n", quoteIdent(tbl))
			for i, col := range cols {
				nullable := ""
				if strings.ToUpper(col.IsNullable) == "NO" {
					nullable = " NOT NULL"
				}
				comma := ","
				if i == len(cols)-1 {
					comma = ""
				}
				fmt.Fprintf(w, "  %s %s%s%s\n", quoteIdent(col.Column), col.DataType, nullable, comma)
			}
			fmt.Fprintln(w, ");")
			fmt.Fprintln(w)
		}
	}

	if emitData {
		if emitSchema {
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w, "-- basin db dump — data")
		fmt.Fprintln(w)
		for _, tbl := range tableOrder {
			// Fetch all rows for this table.
			selectSQL := fmt.Sprintf("SELECT * FROM %s", quoteIdent(tbl))
			var dataResult QueryResult
			if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/sql/query",
				map[string]any{"sql": selectSQL, "writes_enabled": false}, &dataResult); err != nil {
				// Non-fatal: skip the table with a comment.
				fmt.Fprintf(w, "-- skipped %s: %v\n", tbl, err)
				continue
			}
			if len(dataResult.Rows) == 0 {
				continue
			}
			cols := tables[tbl]
			colNames := make([]string, len(cols))
			for i, c := range cols {
				colNames[i] = quoteIdent(c.Column)
			}
			colList := strings.Join(colNames, ", ")
			for _, row := range dataResult.Rows {
				vals := make([]string, len(row))
				for i, v := range row {
					vals[i] = sqlLiteral(v)
				}
				fmt.Fprintf(w, "INSERT INTO %s (%s) VALUES (%s);\n",
					quoteIdent(tbl), colList, strings.Join(vals, ", "))
			}
		}
	}
	return nil
}

// quoteIdent wraps an identifier in double-quotes and escapes any
// embedded double-quotes per SQL standard (double them up).
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// sqlLiteral converts a JSON-decoded Go value into a SQL literal safe
// for use inside INSERT VALUES. Rules:
//   - nil → NULL
//   - bool → TRUE / FALSE
//   - float64 → numeric literal (integer when exact)
//   - string → single-quoted, with embedded ' doubled
//   - anything else → single-quoted string form
func sqlLiteral(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if t {
			return "TRUE"
		}
		return "FALSE"
	case float64:
		if t == float64(int64(t)) {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%g", t)
	case string:
		return "'" + strings.ReplaceAll(t, "'", "''") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", t), "'", "''") + "'"
	}
}

// ── db lint ───────────────────────────────────────────────────────────

// dbLintError holds one parse/validation error from dry-running a migration.
type dbLintError struct {
	File    string `json:"file"`
	Line    int    `json:"line"`
	Message string `json:"message"`
}

func cmdDBLint(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("db lint", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	// lint requires a project ref to POST to the dry-run endpoint.
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("db lint: cwd: %w", err)
	}
	localFiles, err := collectLocalMigrations(cwd)
	if err != nil {
		return fmt.Errorf("db lint: list local migrations: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var lintErrors []dbLintError
	checked := 0

	for _, path := range localFiles {
		base := filepath.Base(path)
		sql, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("db lint: read %s: %w", base, err)
		}
		checked++
		body := map[string]any{
			"sql":            string(sql),
			"writes_enabled": false,
			"dry_run":        true,
		}
		var result map[string]any
		if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/sql/query", body, &result); err != nil {
			// Map API errors to lint errors.
			msg := err.Error()
			line := extractLineNumber(msg)
			lintErrors = append(lintErrors, dbLintError{
				File:    base,
				Line:    line,
				Message: msg,
			})
			if !g.quiet {
				fmt.Fprintf(os.Stderr, "%s:%d: %s\n", base, line, msg)
			}
		}
	}

	if g.json {
		// JSON shape: { "files_checked": N, "errors": [{ "file": "...", "line": N, "message": "..." }] }
		return printJSON(os.Stdout, map[string]any{
			"files_checked": checked,
			"errors":        lintErrors,
		})
	}
	if len(lintErrors) > 0 {
		fmt.Fprintf(os.Stdout, "%d file(s) checked, %d error(s)\n", checked, len(lintErrors))
		return fmt.Errorf("lint failed: %d error(s)", len(lintErrors))
	}
	fmt.Fprintf(os.Stdout, "%d file(s) checked, no errors\n", checked)
	return nil
}

// extractLineNumber tries to parse a line number from an error message.
// Returns 1 as a fallback when no line number is found.
func extractLineNumber(msg string) int {
	// Look for patterns like "line 5", "LINE 5", or ":5:" in error messages.
	lower := strings.ToLower(msg)
	if idx := strings.Index(lower, "line "); idx >= 0 {
		var n int
		_, err := fmt.Sscanf(msg[idx+5:], "%d", &n)
		if err == nil && n > 0 {
			return n
		}
	}
	return 1
}
