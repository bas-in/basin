// cmd_logs — live log stream + history poll fallback.
//
//	basin logs --project <ref> [--tail]
//
// We try the streaming endpoint first (NDJSON or text/event-stream
// over /v1/projects/:ref/logs/stream). On any non-2xx (404 / 501)
// we fall back to polling /v1/projects/:ref/logs/history every few
// seconds. The streaming endpoint may not exist yet on every cluster
// — the fallback keeps the CLI useful regardless.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"
)

func cmdLogs(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (required).")
	tail := fs.Bool("tail", false, "Follow live logs (defaults to a one-shot recent dump).")
	limit := fs.Int("limit", 100, "Lines to fetch on a one-shot read.")
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("logs", "Stream live project logs (or poll history).", []string{
			"--project=<ref>     Project ref (required).",
			"--tail              Follow the live log stream.",
			"--limit=N           Lines on a one-shot read (default 100).",
		})
	}
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if *tail {
		return streamLogs(c, *project, g)
	}
	return historyOnce(c, *project, *limit, g)
}

func streamLogs(c *Client, ref string, g *globalFlags) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	res, err := c.stream(ctx, "/v1/projects/"+ref+"/logs/stream")
	if err != nil {
		// Streaming endpoint not present — fall back to polling.
		var ae *APIError
		if errors.As(err, &ae) && (ae.HTTPStatus == 404 || ae.HTTPStatus == 501) {
			printInfo(g, "log stream not reachable — polling /history every 3s. Ctrl-C to stop.")
			return pollHistory(ctx, c, ref, g)
		}
		return err
	}
	defer res.Body.Close()
	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		// Trim "data: " prefix if the server happens to emit SSE.
		line = strings.TrimPrefix(line, "data: ")
		if line == "" {
			continue
		}
		fmt.Fprintln(os.Stdout, line)
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func historyOnce(c *Client, ref string, limit int, g *globalFlags) error {
	q := queryString(map[string]string{"limit": fmt.Sprintf("%d", limit)})
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Lines []map[string]any `json:"lines"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/logs/history"+q, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { lines: [ { at: string, level: string, message: string, ... } ] }
		return printJSON(os.Stdout, resp)
	}
	for _, ln := range resp.Lines {
		renderLogLine(ln)
	}
	return nil
}

// pollHistory loops until ctx is done. It tracks the last printed
// timestamp so we don't re-emit a line already shown. Best-effort:
// out-of-order or missing timestamps fall back to a 3s window.
func pollHistory(ctx context.Context, c *Client, ref string, g *globalFlags) error {
	var since string
	tk := time.NewTicker(3 * time.Second)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tk.C:
		}
		q := queryString(map[string]string{"since": since, "limit": "200"})
		var resp struct {
			Lines []map[string]any `json:"lines"`
		}
		if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/logs/history"+q, nil, &resp); err != nil {
			// Don't kill the loop on transient errors — surface and keep
			// polling so a flapping endpoint doesn't break the CLI.
			printErr(g, "history poll failed: %v", err)
			continue
		}
		for _, ln := range resp.Lines {
			renderLogLine(ln)
			if ts, ok := ln["at"].(string); ok && ts > since {
				since = ts
			}
		}
	}
}

// renderLogLine prints one log entry in human-friendly form. Falls
// back to a JSON dump when the shape isn't recognised.
func renderLogLine(ln map[string]any) {
	at, _ := ln["at"].(string)
	level, _ := ln["level"].(string)
	msg, _ := ln["message"].(string)
	if at == "" && level == "" && msg == "" {
		fmt.Fprintln(os.Stdout, ln)
		return
	}
	fmt.Fprintf(os.Stdout, "%s  %-5s  %s\n", at, level, msg)
}
