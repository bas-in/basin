// cmd_config — read/write CLI config flags.
//
//	basin config get telemetry
//	basin config set telemetry on|off
//	basin config show
//
// Read-only by default. The `set` form persists via writeConfigFile so
// the toggle survives across invocations. Today only `telemetry` is a
// boolean; the dispatch is open so future flags (default region, auto-
// update, etc.) drop in alongside.
package main

import (
	"fmt"
	"os"
	"strings"
)

func cmdConfig(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdConfigShow(g, nil)
	}
	switch args[0] {
	case "get":
		return cmdConfigGet(g, args[1:])
	case "set":
		return cmdConfigSet(g, args[1:])
	case "show":
		return cmdConfigShow(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("config", "Read or write CLI config values.", []string{
			"show                          Print the on-disk config.",
			"get <key>                     Print one key.",
			"set <key> <value>             Persist a key.",
			"",
			"Keys:",
			"  telemetry                   on|off  (anonymous usage pings; default off)",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for config", args[0])
		return errSilent
	}
}

func cmdConfigShow(g *globalFlags, args []string) error {
	_ = args
	cf, err := readConfigFile()
	if err != nil {
		return err
	}
	if cf == nil {
		cf = &configFile{}
	}
	if g.json {
		// JSON shape: { "telemetry": bool, "default_org": string, "api_url": string }
		return printJSON(os.Stdout, map[string]any{
			"telemetry":   cf.Telemetry,
			"default_org": cf.DefaultOrg,
			"api_url":     cf.APIURL,
		})
	}
	fmt.Fprintf(os.Stdout, "telemetry:   %s\n", onOff(cf.Telemetry))
	fmt.Fprintf(os.Stdout, "default_org: %s\n", cf.DefaultOrg)
	fmt.Fprintf(os.Stdout, "api_url:     %s\n", cf.APIURL)
	return nil
}

func cmdConfigGet(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin config get <key>")
	}
	key := args[0]
	cf, err := readConfigFile()
	if err != nil {
		return err
	}
	if cf == nil {
		cf = &configFile{}
	}
	switch key {
	case "telemetry":
		if g.json {
			// JSON shape: { "telemetry": bool }
			return printJSON(os.Stdout, map[string]any{"telemetry": cf.Telemetry})
		}
		fmt.Fprintln(os.Stdout, onOff(cf.Telemetry))
		return nil
	case "default_org":
		fmt.Fprintln(os.Stdout, cf.DefaultOrg)
		return nil
	case "api_url":
		fmt.Fprintln(os.Stdout, cf.APIURL)
		return nil
	default:
		return fmt.Errorf("unknown key %q (try: telemetry, default_org, api_url)", key)
	}
}

func cmdConfigSet(g *globalFlags, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: basin config set <key> <value>")
	}
	key, val := args[0], args[1]
	cf, err := readConfigFile()
	if err != nil {
		return err
	}
	if cf == nil {
		cf = &configFile{}
	}
	switch key {
	case "telemetry":
		b, ok := parseOnOff(val)
		if !ok {
			return fmt.Errorf("telemetry must be on|off (got %q)", val)
		}
		cf.Telemetry = b
	case "default_org":
		cf.DefaultOrg = val
	case "api_url":
		cf.APIURL = val
	default:
		return fmt.Errorf("unknown key %q (try: telemetry, default_org, api_url)", key)
	}
	if err := writeConfigFile(cf); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if g.json {
		// JSON shape: { "set": true, "key": string, "value": string }
		return printJSON(os.Stdout, map[string]any{
			"set":   true,
			"key":   key,
			"value": val,
		})
	}
	fmt.Fprintf(os.Stdout, "Set %s = %s.\n", key, val)
	return nil
}

// onOff renders a bool as "on" / "off" for human readers.
func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// parseOnOff accepts on/off, true/false, yes/no, 1/0 (case-insensitive)
// so the operator can use whichever spelling fits their muscle memory.
func parseOnOff(s string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "on", "true", "yes", "1":
		return true, true
	case "off", "false", "no", "0":
		return false, true
	default:
		return false, false
	}
}
