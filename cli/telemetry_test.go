// telemetry_test — toggle persistence + emit-body shape via fetch stub.
//
// We don't drive the live HTTP poster: tests swap telemetryPost for a
// closure that records (url, body) into local state. That lets us
// assert:
//
//   - Off (default) → poster is never called.
//   - On → poster is called exactly once per command, with the right
//     URL, JSON shape, and content-type-agnostic body decode.
//   - The on-disk Telemetry bool round-trips through writeConfigFile
//     and readConfigFile.
package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// stubTelemetry swaps telemetryPost for a recorder and restores it at
// test-end. Returns a slice the test can inspect after the call.
func stubTelemetry(t *testing.T) *[]map[string]any {
	t.Helper()
	prev := telemetryPost
	var got []map[string]any
	telemetryPost = func(ctx context.Context, url string, body []byte) error {
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			t.Errorf("telemetry body not JSON: %v\nraw: %s", err, body)
			return nil
		}
		m["__url"] = url
		got = append(got, m)
		return nil
	}
	t.Cleanup(func() { telemetryPost = prev })
	return &got
}

func TestTelemetryDefaultOff(t *testing.T) {
	withTempConfigDir(t)
	got := stubTelemetry(t)
	g := &globalFlags{apiURL: "https://example.invalid"}
	emitTelemetry(g, "whoami", 12*time.Millisecond, nil)
	if len(*got) != 0 {
		t.Errorf("telemetry called despite default-off; got=%+v", *got)
	}
}

func TestTelemetryOnRoundTrip(t *testing.T) {
	withTempConfigDir(t)
	got := stubTelemetry(t)

	// Persist telemetry = on.
	cf := &configFile{Telemetry: true}
	if err := writeConfigFile(cf); err != nil {
		t.Fatalf("writeConfigFile: %v", err)
	}
	// Re-read confirms the bool is on disk.
	roundTrip, err := readConfigFile()
	if err != nil {
		t.Fatalf("readConfigFile: %v", err)
	}
	if roundTrip == nil || !roundTrip.Telemetry {
		t.Fatalf("telemetry didn't round-trip: %+v", roundTrip)
	}

	g := &globalFlags{apiURL: "https://example.invalid"}
	emitTelemetry(g, "projects list", 42*time.Millisecond, nil)

	if len(*got) != 1 {
		t.Fatalf("telemetry not posted; got=%+v", *got)
	}
	body := (*got)[0]
	if body["cmd"] != "projects list" {
		t.Errorf("body.cmd = %v, want 'projects list'", body["cmd"])
	}
	if body["duration_ms"].(float64) != 42 {
		t.Errorf("body.duration_ms = %v, want 42", body["duration_ms"])
	}
	if body["ok"] != true {
		t.Errorf("body.ok = %v, want true", body["ok"])
	}
	if body["version"] == "" {
		t.Errorf("body.version is empty")
	}
	if body["os"] == "" {
		t.Errorf("body.os is empty")
	}
	if body["__url"] != "https://example.invalid/v1/cli/telemetry" {
		t.Errorf("posted url = %v, want %s%s", body["__url"], g.apiURL, telemetryEndpoint)
	}
}

func TestConfigSetTelemetryPersists(t *testing.T) {
	withTempConfigDir(t)

	g := &globalFlags{}
	if err := cmdConfigSet(g, []string{"telemetry", "on"}); err != nil {
		t.Fatalf("config set telemetry on: %v", err)
	}
	cf, err := readConfigFile()
	if err != nil || cf == nil || !cf.Telemetry {
		t.Fatalf("telemetry not persisted: cf=%+v err=%v", cf, err)
	}

	if err := cmdConfigSet(g, []string{"telemetry", "off"}); err != nil {
		t.Fatalf("config set telemetry off: %v", err)
	}
	cf, err = readConfigFile()
	if err != nil {
		t.Fatalf("readConfigFile: %v", err)
	}
	if cf == nil || cf.Telemetry {
		t.Fatalf("telemetry should be off: %+v", cf)
	}
}

func TestParseOnOffAcceptsCommonSpellings(t *testing.T) {
	cases := []struct {
		in    string
		want  bool
		valid bool
	}{
		{"on", true, true},
		{"ON", true, true},
		{"true", true, true},
		{"yes", true, true},
		{"1", true, true},
		{"off", false, true},
		{"false", false, true},
		{"no", false, true},
		{"0", false, true},
		{"maybe", false, false},
	}
	for _, c := range cases {
		got, ok := parseOnOff(c.in)
		if ok != c.valid || got != c.want {
			t.Errorf("parseOnOff(%q) = (%v,%v); want (%v,%v)", c.in, got, ok, c.want, c.valid)
		}
	}
}
