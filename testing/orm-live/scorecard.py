#!/usr/bin/env python3
"""Scorecard for the live-ORM compatibility harness.

Parses TAP-ish output from each ORM suite (``results/<suite>.tap``), compares
it against ``baseline.json``, prints a scorecard table, and exits nonzero on
regressions.

TAP-ish line grammar (one per test, anything else ignored):

    ok - <suite>.<test>
    not ok - <suite>.<test> # <reason>

Verdicts:
    PASS        observed ok,      baseline pass
    XFAIL       observed not ok,  baseline xfail   (known gap — named)
    XPASS       observed ok,      baseline xfail   (progress! re-baseline)
    REGRESSION  observed not ok,  baseline pass    -> exit 1
    NEW-PASS    observed ok,      not in baseline  (add to baseline)
    NEW-FAIL    observed not ok,  not in baseline  -> exit 1
    MISSING     in baseline, suite ran, no result  -> exit 1
    SKIP        suite toolchain unavailable (whole suite, never silent)
"""

import argparse
import json
import re
import sys
from pathlib import Path

TAP_RE = re.compile(r"^(ok|not ok)(?:\s+\d+)?\s+-\s+([A-Za-z0-9_.-]+)(?:\s+#\s*(.*))?\s*$")

FAILING = {"REGRESSION", "NEW-FAIL", "MISSING"}


def parse_tap(path: Path, suite: str):
    """-> {test_name: (passed: bool, reason: str)} preserving file order.

    TAP lines carry a ``<suite>.`` prefix (``ok - prisma.connect``); baseline
    keys are unprefixed (``connect``) — strip the prefix here.
    """
    results = {}
    prefix = f"{suite}."
    for line in path.read_text(errors="replace").splitlines():
        m = TAP_RE.match(line.strip())
        if not m:
            continue
        passed = m.group(1) == "ok"
        name = m.group(2)
        if name.startswith(prefix):
            name = name[len(prefix):]
        results[name] = (passed, (m.group(3) or "").strip())
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True, type=Path)
    ap.add_argument("--baseline", required=True, type=Path)
    ap.add_argument("--update-baseline", action="store_true")
    args = ap.parse_args()

    baseline = json.loads(args.baseline.read_text()) if args.baseline.exists() else {
        "version": 1, "suites": {}}
    suites_baseline = baseline.get("suites", {})

    rows = []          # (suite, test, observed, expected, verdict, note)
    observed_all = {}  # suite -> {test: (passed, reason)}
    skipped = []

    suite_names = sorted(set(suites_baseline)
                         | {p.stem for p in args.results.glob("*.tap")}
                         | {p.stem for p in args.results.glob("*.skipped")})

    for suite in suite_names:
        tap = args.results / f"{suite}.tap"
        marker = args.results / f"{suite}.skipped"
        if marker.exists() and not tap.exists():
            skipped.append(suite)
            why = marker.read_text().strip() or "toolchain unavailable"
            for test in suites_baseline.get(suite, {}):
                rows.append((suite, test, "-", "-", "SKIP", why))
            continue
        if not tap.exists():
            skipped.append(suite)
            for test in suites_baseline.get(suite, {}):
                rows.append((suite, test, "-", "-", "SKIP", "no results file"))
            continue

        observed = parse_tap(tap, suite)
        observed_all[suite] = observed
        expected = suites_baseline.get(suite, {})

        for test, (passed, reason) in observed.items():
            exp = expected.get(test)
            if exp is None:
                verdict = "NEW-PASS" if passed else "NEW-FAIL"
                note = reason if not passed else "add baseline entry"
            elif exp.get("expected") == "pass":
                verdict = "PASS" if passed else "REGRESSION"
                note = "" if passed else reason
            else:  # xfail
                gap = exp.get("gap", "unnamed gap")
                verdict = "XPASS" if passed else "XFAIL"
                note = f"gap closed? re-baseline — was: {gap}" if passed else gap
            rows.append((suite, test, "ok" if passed else "FAIL",
                         exp.get("expected") if exp else "-", verdict, note))

        for test, exp in expected.items():
            if test not in observed:
                rows.append((suite, test, "-", exp.get("expected"),
                             "MISSING", "baseline test produced no TAP line"))

    # ── print table ──────────────────────────────────────────────────────────
    headers = ("suite", "test", "got", "want", "verdict", "note")
    widths = [len(h) for h in headers]
    str_rows = [tuple(str(c or "") for c in r) for r in rows]
    for r in str_rows:
        for i, c in enumerate(r):
            widths[i] = min(max(widths[i], len(c)), 72)

    def fmt(r):
        return "  ".join(c[:72].ljust(widths[i]) for i, c in enumerate(r)).rstrip()

    print("=" * (sum(widths) + 10))
    print("LIVE-ORM COMPATIBILITY SCORECARD")
    print("=" * (sum(widths) + 10))
    print(fmt(headers))
    print(fmt(tuple("-" * w for w in widths)))
    for r in str_rows:
        print(fmt(r))

    counts = {}
    for r in rows:
        counts[r[4]] = counts.get(r[4], 0) + 1
    print("-" * (sum(widths) + 10))
    print("totals: " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    if skipped:
        print(f"skipped suites (toolchain unavailable): {', '.join(skipped)}")

    # ── update baseline ──────────────────────────────────────────────────────
    if args.update_baseline:
        for suite, observed in observed_all.items():
            sb = suites_baseline.setdefault(suite, {})
            for test, (passed, reason) in observed.items():
                old = sb.get(test, {})
                if passed:
                    sb[test] = {"expected": "pass"}
                else:
                    gap = old.get("gap") if old.get("expected") == "xfail" else None
                    sb[test] = {"expected": "xfail",
                                "gap": gap or (reason or "UNNAMED — fill in the gap description")}
        baseline["suites"] = suites_baseline
        args.baseline.write_text(json.dumps(baseline, indent=2) + "\n")
        print(f"baseline updated -> {args.baseline}")

    bad = [r for r in rows if r[4] in FAILING]
    if bad and not args.update_baseline:
        print(f"\nFAIL: {len(bad)} regression(s) vs baseline", file=sys.stderr)
        sys.exit(1)
    xpass = [r for r in rows if r[4] == "XPASS"]
    if xpass:
        print(f"\nNOTE: {len(xpass)} known gap(s) now pass — run with "
              "--update-baseline and commit the diff.")
    print("\nOK: no regressions vs baseline")


if __name__ == "__main__":
    main()
