// telemetry — opt-in anonymous usage pings.
//
// Default OFF. The operator flips it via:
//
//	basin config set telemetry on
//	basin config set telemetry off
//
// When ON, every command POSTs a minimal JSON body to
// `{api_url}/v1/cli/telemetry` after the handler returns:
//
//	{
//	  "cmd":         "projects list",
//	  "duration_ms": 423,
//	  "version":     "0.1.0",
//	  "os":          "darwin/arm64",
//	  "ok":          true
//	}
//
// No PII, no command args, no env. The endpoint may not exist yet on
// the cloud — we soft-fail on any HTTP / transport error so a missing
// route never pollutes the user's command output. Same 2s timeout as
// /v1/version so a sick endpoint can't slow the CLI down.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"time"
)

// telemetryEndpoint is split out for the test stub.
const telemetryEndpoint = "/v1/cli/telemetry"

// telemetryPoster is the HTTP do-er. Stubbed in tests. Production
// wires to a one-shot client with a tight timeout.
type telemetryPoster func(ctx context.Context, url string, body []byte) error

// telemetryPost is the live HTTP poster. Replaced in tests via
// telemetryPostOverride.
var telemetryPost telemetryPoster = func(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "basin-cli/"+version)
	client := &http.Client{Timeout: 2 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	// We don't care about the response body; success and 404 are both
	// "fine — keep going". Anything 5xx is silently dropped too.
	return nil
}

// emitTelemetry is the side-effect entry point called from main.run().
// All paths return silently — never bubble errors through the CLI exit.
func emitTelemetry(g *globalFlags, cmd string, dur time.Duration, runErr error) {
	if g == nil {
		return
	}
	cf, err := readConfigFile()
	if err != nil || cf == nil || !cf.Telemetry {
		// Default OFF: missing file, parse error, or flag-not-set all
		// short-circuit silently.
		return
	}
	body, err := json.Marshal(map[string]any{
		"cmd":         cmd,
		"duration_ms": dur.Milliseconds(),
		"version":     version,
		"os":          runtime.GOOS + "/" + runtime.GOARCH,
		"ok":          runErr == nil,
	})
	if err != nil {
		return
	}
	url := g.apiURL + telemetryEndpoint
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = telemetryPost(ctx, url, body) // soft-fail
}
