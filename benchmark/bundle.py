#!/usr/bin/env python3
"""Bundle benchmark JSON sidecars into a dashboard-loadable `results.js`
plus a human-readable Markdown report — and render the matching
index_<slug>.html from `benchmark/template.html`.

The dashboard registry lives in `benchmark/dashboards.toml`. Each row is
keyed on (slug, storage_backend, compute_backend, environment, data_dir,
title, h1, subtitle, footer) and produces:

  benchmark/<data_dir>/results.js
  benchmark/RESULTS_<slug>.md
  benchmark/index_<slug>.html

By default this bundles every configured row. To bundle a single
dashboard, pass `--dir <data_dir>`.

To add a new dashboard: append a [[dashboard]] block to
`benchmark/dashboards.toml`, create the matching `benchmark/<data_dir>/`
folder, run the tests, and re-run this script. No edits to the bundler
or template are needed.

Browsers block fetch() of local files when index.html is opened via
file://. The script-tag-loadable `results.js` (window.__BASIN_RESULTS = ...)
sidesteps that so a plain double-click works.

Run after `cargo test -p basin-integration-tests --tests` for the synthetic
dashboard, or after `cargo test -p basin-integration-tests --test
s3_credentials_smoke -- --ignored --nocapture` (and the eventual s3_*
suite) for the real-cloud dashboard.
"""

from __future__ import annotations
import argparse
import json
import sys
import time
import tomllib
from pathlib import Path
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DASHBOARDS_TOML = HERE / "dashboards.toml"

# Storage backends that physically run on the local box vs. cross the
# network. Used as a sanity check against the per-row `environment` flag.
LOCAL_STORAGE_BACKENDS = {"localfs", "seaweedfs"}
CLOUD_STORAGE_BACKENDS = {"r2", "s3", "b2", "r2/s3/b2"}


# ---------- helpers ------------------------------------------------------------


def parse_generated_at(s: str | None) -> datetime | None:
    """The Rust helper writes timestamps as `@<unix-secs>`."""
    if not s or not isinstance(s, str):
        return None
    if s.startswith("@"):
        try:
            return datetime.fromtimestamp(int(s[1:]), tz=timezone.utc)
        except (ValueError, OSError):
            return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def fmt_value(v: float, unit: str) -> str:
    if unit in ("ms", "s"):
        return f"{v:.2f} {unit}"
    if unit == "x":
        return f"{v:.2f}×"
    if unit in ("KiB", "MiB", "B"):
        return f"{v:.2f} {unit}"
    if unit == "leaks":
        return f"{int(v)} leaks"
    return f"{v:g} {unit}".strip()


def bar_text(bar: dict | None) -> str:
    if not bar:
        return "—"
    op = bar.get("op", "")
    val = bar.get("value", 0.0)
    # f64::INFINITY (and NaN) round-trip from JSON as None — treat that as
    # "no upper/lower bound" rather than crashing the renderer.
    if val is None:
        if op == "greater_than_or_equal":
            return "≥ -∞"
        if op == "less_than":
            return "< ∞"
        if op == "equal":
            return "= ∞"
        return str(bar)
    if op == "greater_than_or_equal":
        return f"≥ {val:g}"
    if op == "less_than":
        return f"< {val:g}"
    if op == "equal":
        return f"= {val:g}"
    return str(bar)


def status_chip(passed: bool) -> str:
    return "**PASS**" if passed else "**FAIL**"


def _bar_admits(bar: dict | None, value) -> bool | None:
    """Does `value` satisfy `bar`? Returns None when undecidable (no bar, no
    value, or an unbounded ±∞ threshold that round-tripped as JSON null)."""
    if not bar or value is None:
        return None
    op = bar.get("op", "")
    thr = bar.get("value", None)
    if thr is None:  # ±∞ bound → always admits
        return True
    if op == "greater_than_or_equal":
        return value >= thr
    if op == "less_than":
        return value < thr
    if op == "equal":
        return abs(value - thr) < 1e-12
    return None


def bar_consistency_warning(report: dict) -> str | None:
    """Guard against the perf_stack-style mislabel: a report that reports
    `passed` while its OWN displayed `primary.bar` disagrees with
    `primary.value`. Returns a human-readable warning string when the published
    PASS/FAIL chip would contradict the bar shown next to it, else None.

    This is a renderer-side invariant only — the authoritative pass/fail is
    computed in the Rust harness. It exists so a future regime-bar mismatch
    (passed computed against one bar, a different bar displayed) is caught at
    publish time instead of shipping a card that reads "PASS … bar ≥3.0×"
    against a 1.226× value.
    """
    if "passed" not in report:
        return None
    primary = report.get("primary")
    if not isinstance(primary, dict):
        return None
    admits = _bar_admits(primary.get("bar"), primary.get("value"))
    if admits is None:
        return None
    passed = bool(report.get("passed"))
    if passed != admits:
        return (
            f"{report.get('id', '?')}: passed={passed} but the displayed bar "
            f"{bar_text(primary.get('bar'))} {'admits' if admits else 'rejects'} "
            f"value {primary.get('value')!r} — published chip contradicts its bar"
        )
    return None


# ---------- markdown sections --------------------------------------------------


def render_header(reports: dict[str, dict]) -> str:
    times = [
        parse_generated_at(r.get("generated_at"))
        for r in reports.values()
        if isinstance(r, dict)
    ]
    times = [t for t in times if t is not None]
    latest = max(times) if times else None
    latest_str = latest.strftime("%Y-%m-%d %H:%M:%S UTC") if latest else "n/a"

    viability = [r for r in reports.values() if r.get("kind") == "viability"]
    scaling = [r for r in reports.values() if r.get("kind") == "scaling"]
    compare = [r for r in reports.values() if r.get("kind") == "compare"]

    v_pass = sum(1 for r in viability if r.get("passed"))
    s_pass = sum(1 for r in scaling if r.get("passed"))

    return (
        f"# Basin — benchmark results\n\n"
        f"_Auto-generated by `benchmark/bundle.py`. Do not edit by hand._\n\n"
        f"- **Latest run:** {latest_str}\n"
        f"- **Viability:** {v_pass} / {len(viability)} passing\n"
        f"- **Scaling:** {s_pass} / {len(scaling)} passing\n"
        f"- **Postgres head-to-head:** {len(compare)} report"
        + ("s" if len(compare) != 1 else "")
        + "\n\n"
        f"For the live dashboard, open `benchmark/index_localfs.html` directly "
        f"(no server needed). To regenerate after running tests:\n\n"
        f"```sh\ncargo test -p basin-integration-tests --tests -- --nocapture\n"
        f"python3 benchmark/bundle.py\n```\n"
    )


def render_viability(reports: dict[str, dict]) -> str:
    items = [
        (name, r)
        for name, r in reports.items()
        if r.get("kind") == "viability"
    ]
    items.sort(key=lambda x: x[0])
    if not items:
        return ""

    lines = ["## Viability", ""]
    lines.append("| Test | Status | Measured | Bar |")
    lines.append("|---|---|---|---|")
    for _, r in items:
        primary = r.get("primary") or {}
        measured = fmt_value(
            primary.get("value", 0.0), primary.get("unit", "")
        )
        lines.append(
            f"| **{r.get('name', '?')}** "
            f"| {status_chip(bool(r.get('passed')))} "
            f"| `{measured}` "
            f"| `{bar_text(primary.get('bar'))}` |"
        )
    lines.append("")
    for _, r in items:
        primary = r.get("primary") or {}
        lines.append(
            f"- **{r.get('name', '?')}** — {r.get('claim', '').rstrip('.')}. "
            f"Measured `{primary.get('label', '')}` = "
            f"`{fmt_value(primary.get('value', 0.0), primary.get('unit', ''))}`."
        )
    lines.append("")
    return "\n".join(lines)


def render_scaling(reports: dict[str, dict]) -> str:
    items = [
        (name, r)
        for name, r in reports.items()
        if r.get("kind") == "scaling"
    ]
    items.sort(key=lambda x: x[0])
    if not items:
        return ""

    lines = ["## Scaling", ""]
    for _, r in items:
        primary = r.get("primary") or {}
        passed = bool(r.get("passed"))
        lines.append(
            f"### {r.get('name', '?')} — {status_chip(passed)}"
        )
        lines.append("")
        lines.append(f"_{r.get('claim', '')}_")
        if primary:
            lines.append("")
            lines.append(
                f"**{primary.get('label', '')}:** "
                f"`{fmt_value(primary.get('value', 0.0), primary.get('unit', ''))}` "
                f"(bar `{bar_text(primary.get('bar'))}`)"
            )
        lines.append("")

        x_axis = r.get("x_axis") or {}
        series = r.get("series") or []
        rows = r.get("rows") or []
        if x_axis and series and rows:
            x_key = x_axis.get("key", "x")
            header_keys = [x_key] + [s.get("key", "") for s in series]
            header_labels = [x_axis.get("label", x_key)] + [
                f"{s.get('label', '')}"
                + (f" ({s['unit']})" if s.get("unit") else "")
                for s in series
            ]
            lines.append("| " + " | ".join(header_labels) + " |")
            lines.append("| " + " | ".join(["---"] * len(header_labels)) + " |")
            for row in rows:
                cells = []
                for key in header_keys:
                    val = row.get(key)
                    if val is None:
                        cells.append("—")
                    elif isinstance(val, float):
                        cells.append(f"{val:.2f}")
                    elif isinstance(val, int):
                        cells.append(f"{val:,}")
                    else:
                        cells.append(str(val))
                lines.append("| " + " | ".join(cells) + " |")
            lines.append("")
    return "\n".join(lines)


def _is_three_way(r: dict) -> bool:
    """Return True if this compare report uses the 3-column neon/supabase/basin shape."""
    for m in r.get("metrics", []):
        if "neon" in m or "supabase" in m:
            return True
    return False


def render_compare(reports: dict[str, dict]) -> str:
    items = [
        (name, r)
        for name, r in reports.items()
        if r.get("kind") == "compare"
    ]
    if not items:
        return ""

    lines = ["## Postgres head-to-head", ""]
    for _, r in items:
        if not r.get("available", True):
            lines.append(
                f"### {r.get('name', '?')} — _unavailable_"
            )
            note = r.get("note")
            if note:
                lines.append(f"\n> {note}")
            lines.append("")
            continue

        lines.append(f"### {r.get('name', '?')}")
        lines.append("")
        lines.append(f"_{r.get('claim', '')}_")
        lines.append("")

        if _is_three_way(r):
            # 3-column compare: Neon | Supabase | Basin
            lines.append("| Metric | Neon | Supabase | Basin | Winner |")
            lines.append("|---|---|---|---|---|")
            for m in r.get("metrics", []):
                label = m.get("label", "?")
                neon = m.get("neon")
                supabase = m.get("supabase")
                basin = m.get("basin")
                unit = m.get("unit", "")
                better = m.get("better") or "—"
                winner_label = {
                    "basin": "**Basin**",
                    "neon": "**Neon**",
                    "supabase": "**Supabase**",
                    "tie": "tie",
                }.get(better, better)

                def _fmt(v: float | None, u: str) -> str:
                    if v is None:
                        return "—"
                    if u == "bytes":
                        return f"{v / 1_048_576:.2f} MiB"
                    return f"{v:.2f} {u}".strip()

                lines.append(
                    f"| {label} "
                    f"| `{_fmt(neon, unit)}` "
                    f"| `{_fmt(supabase, unit)}` "
                    f"| `{_fmt(basin, unit)}` "
                    f"| {winner_label} |"
                )
        else:
            # Legacy 2-column compare: Basin | Postgres
            lines.append("| Metric | Basin | Postgres | Winner | Ratio |")
            lines.append("|---|---|---|---|---|")
            for m in r.get("metrics", []):
                label = m.get("label", "?")
                basin = m.get("basin", 0.0)
                pg = m.get("postgres", 0.0)
                unit = m.get("unit", "")
                better = m.get("better", "tie")
                winner_label = {
                    "basin": "Basin",
                    "postgres": "Postgres",
                    "tie": "tie",
                }.get(better, better)
                ratio = m.get("ratio_text") or "—"
                if unit == "bytes":
                    basin_s = f"{basin / 1_048_576:.2f} MiB"
                    pg_s = f"{pg / 1_048_576:.2f} MiB"
                else:
                    basin_s = f"{basin:.2f} {unit}".strip()
                    pg_s = f"{pg:.2f} {unit}".strip()
                lines.append(
                    f"| {label} | `{basin_s}` | `{pg_s}` | **{winner_label}** | `{ratio}` |"
                )

        note = r.get("note")
        if note:
            lines.append("")
            lines.append(f"> {note}")
        lines.append("")
    return "\n".join(lines)


def render_footer() -> str:
    return (
        "## What to read next\n\n"
        "- [`README.md`](../README.md) — what Basin is, who it's for, how to try it.\n"
        "- [`WEDGE.md`](../WEDGE.md) — six-month wedge-deepening roadmap.\n"
        "- [`CAPABILITIES.md`](../CAPABILITIES.md) — public capability matrix.\n"
        "- [`docs/architecture.md`](../docs/architecture.md) — the four-layer stack.\n"
        "- [`docs/decisions/`](../docs/decisions/) — every \"no\" we've recorded, with the trigger that would change our mind.\n"
    )


# ---------- main ---------------------------------------------------------------


def parse_args(valid_dirs: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        default=None,
        choices=valid_dirs,
        help="bundle a single dashboard (default: bundle all configured)",
    )
    return parser.parse_args()


def bundle_one(data_dir: Path, slug: str) -> int:
    """Bundle a single data directory. Returns 0 on success, non-zero on error."""
    if not data_dir.is_dir():
        print(f"no data dir: {data_dir}", file=sys.stderr)
        return 1

    manifest_path = data_dir / "manifest.json"
    if not manifest_path.exists():
        print(f"missing manifest: {manifest_path}", file=sys.stderr)
        return 1
    manifest = json.loads(manifest_path.read_text())

    reports: dict[str, dict] = {}
    for p in sorted(data_dir.glob("*.json")):
        if p.name == "manifest.json":
            continue
        try:
            reports[p.stem] = json.loads(p.read_text())
        except json.JSONDecodeError as e:
            print(f"skipping malformed {p.name}: {e}", file=sys.stderr)

    # Publish-time guard: warn if any card's PASS/FAIL chip contradicts the
    # bar it displays (the perf_stack regime-bar mislabel). Renderer-side only;
    # the Rust harness owns the authoritative pass/fail.
    for r in reports.values():
        warn = bar_consistency_warning(r)
        if warn:
            print(f"WARNING: bar/passed mismatch — {warn}", file=sys.stderr)

    # Write the JS bundle for the dashboard.
    bundle = {"manifest": manifest, "reports": reports}
    out_js = data_dir / "results.js"
    out_js.write_text(
        "// auto-generated by benchmark/bundle.py — do not edit by hand\n"
        f"window.__BASIN_RESULTS = {json.dumps(bundle, indent=2)};\n"
    )

    # Markdown report file name follows the dashboard slug so each row
    # in dashboards.toml gets its own RESULTS_<slug>.md.
    md_path = HERE / f"RESULTS_{slug}.md"

    md_parts = [
        render_header(reports),
        render_viability(reports),
        render_scaling(reports),
        render_compare(reports),
        render_footer(),
    ]
    md = "\n".join(part for part in md_parts if part)
    md_path.write_text(md)

    print(
        f"wrote {out_js.relative_to(ROOT)} ({len(reports)} reports, "
        f"{out_js.stat().st_size} bytes)"
    )
    print(f"wrote {md_path.relative_to(ROOT)} ({md_path.stat().st_size} bytes)")
    return 0


# ---------- dashboard config -------------------------------------------------
#
# The set of dashboards lives in benchmark/dashboards.toml. Each row is
# (slug, storage_backend, compute_backend, environment, data_dir, title, h1,
#  subtitle, footer) and renders to one index_<slug>.html. To extend Basin
# along a new axis (e.g. multi-shard compute), append a row — no edits to
# the template or the bundler are required.


REQUIRED_FIELDS = (
    "slug",
    "storage_backend",
    "compute_backend",
    "environment",
    "data_dir",
    "title",
    "h1",
    "subtitle",
    "footer",
)


def load_dashboards() -> list[dict]:
    """Parse benchmark/dashboards.toml and validate each row."""
    if not DASHBOARDS_TOML.exists():
        print(f"missing config: {DASHBOARDS_TOML}", file=sys.stderr)
        return []
    with DASHBOARDS_TOML.open("rb") as fh:
        data = tomllib.load(fh)
    rows = data.get("dashboard") or []
    if not isinstance(rows, list):
        print(f"bad config: [[dashboard]] must be an array", file=sys.stderr)
        return []
    seen_slugs: set[str] = set()
    seen_dirs: set[str] = set()
    for row in rows:
        for field in REQUIRED_FIELDS:
            if field not in row:
                raise SystemExit(
                    f"dashboards.toml row missing field {field!r}: {row}"
                )
        if row["environment"] not in ("local", "cloud"):
            raise SystemExit(
                f"dashboards.toml: environment must be 'local' or 'cloud', "
                f"got {row['environment']!r} (slug={row['slug']!r})"
            )
        # Sanity: keep environment honest. localfs/seaweedfs must never be
        # tagged cloud, and r2/s3/b2 must never be tagged local.
        sb = row["storage_backend"]
        if sb in LOCAL_STORAGE_BACKENDS and row["environment"] != "local":
            raise SystemExit(
                f"dashboards.toml: storage_backend={sb!r} is local-only "
                f"but environment={row['environment']!r} (slug={row['slug']!r})"
            )
        if sb in CLOUD_STORAGE_BACKENDS and row["environment"] != "cloud":
            raise SystemExit(
                f"dashboards.toml: storage_backend={sb!r} is cloud-only "
                f"but environment={row['environment']!r} (slug={row['slug']!r})"
            )
        if row["slug"] in seen_slugs:
            raise SystemExit(f"dashboards.toml: duplicate slug {row['slug']!r}")
        if row["data_dir"] in seen_dirs:
            raise SystemExit(
                f"dashboards.toml: duplicate data_dir {row['data_dir']!r}"
            )
        seen_slugs.add(row["slug"])
        seen_dirs.add(row["data_dir"])
    return rows


# ---------- HTML template rendering ----------------------------------------
#
# Each row in dashboards.toml renders to one index_<slug>.html via
# benchmark/template.html. Editing index_*.html directly is a no-op: the
# next bundle.py run will overwrite it.


def _badge_html(row: dict) -> tuple[str, str, str]:
    env = row["environment"]
    storage = row["storage_backend"]
    compute = row["compute_backend"]
    # Display the env tag with a friendlier label so a reader doesn't
    # confuse "local" (the environment) with "localfs" (one of two local
    # storage backends). The CSS class still keys on the raw value so
    # the colour scheme is unchanged.
    env_label = {"local": "self-hosted", "cloud": "cloud"}.get(env, env)
    env_badge = f'<span class="env-tag {env}">{env_label}</span>'
    storage_badge = f'<span class="storage-tag">{storage}</span>'
    compute_badge = f'<span class="compute-tag">{compute}</span>'
    return env_badge, storage_badge, compute_badge


def _section_suffix(row: dict) -> str:
    """LocalFS keeps the bare section title (legacy); others append the H1 tail."""
    if row["slug"] == "localfs":
        return ""
    tail = row["h1"].split("—", 1)[-1].strip() if "—" in row["h1"] else row["h1"]
    return f" — {tail}"


def _html_name(row: dict) -> str:
    return f"index_{row['slug']}.html"


def render_html_dashboards(rows: list[dict]) -> None:
    template_path = HERE / "template.html"
    if not template_path.exists():
        # Graceful in a fresh checkout where the template might not be present.
        print("skip html: no template.html")
        return
    template = template_path.read_text()
    for row in rows:
        env_badge, storage_badge, compute_badge = _badge_html(row)
        nav_links = " · ".join(
            f'<a href="{_html_name(other)}">'
            f"{other['h1'].replace('Basin — ', '')}</a>"
            for other in rows
            if other["slug"] != row["slug"]
        )
        nav_html = f"Other dashboards: {nav_links}" if nav_links else ""
        html = (
            template
            .replace("{{TITLE}}", row["title"])
            .replace("{{H1}}", row["h1"])
            .replace("{{SUBTITLE}}", row["subtitle"])
            .replace("{{SECTION_SUFFIX}}", _section_suffix(row))
            .replace("{{FOOTER}}", row["footer"])
            .replace("{{DATA_DIR}}", row["data_dir"])
            .replace("{{NAV_LINKS}}", nav_html)
            .replace("{{ENVIRONMENT_BADGE}}", env_badge)
            .replace("{{STORAGE_BADGE}}", storage_badge)
            .replace("{{COMPUTE_BADGE}}", compute_badge)
        )
        out_path = HERE / _html_name(row)
        out_path.write_text(html)
        print(f"wrote {out_path.relative_to(ROOT)} ({len(html)} bytes)")


def main() -> int:
    rows = load_dashboards()
    if not rows:
        return 1
    by_dir = {row["data_dir"]: row for row in rows}
    args = parse_args(sorted(by_dir.keys()))
    # Default: bundle every configured dashboard so the rendered files stay
    # in sync. Missing/empty manifests are skipped, not fatal, so this works
    # even when only one set of tests has been run.
    targets = [args.dir] if args.dir else list(by_dir.keys())
    rc = 0
    for name in targets:
        row = by_dir[name]
        d = HERE / name
        if not (d / "manifest.json").exists():
            # Allow the missing-dashboard case to skip silently when bundling
            # all — common for a fresh checkout where only one set of tests
            # has been run yet.
            if args.dir is None:
                print(f"skip {name}/: no manifest yet")
                continue
        rc |= bundle_one(d, row["slug"])
    # Always re-render the HTML dashboards from the template, regardless of
    # which data dirs we just bundled. The HTML doesn't depend on data so
    # this is cheap.
    render_html_dashboards(rows)
    return rc


if __name__ == "__main__":
    _ = time.time
    sys.exit(main())
