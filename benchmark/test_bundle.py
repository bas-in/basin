#!/usr/bin/env python3
"""Unit tests for benchmark/bundle.py renderer guards.

Run: `python3 benchmark/test_bundle.py` (no third-party deps).

Focused on `bar_consistency_warning`, the publish-time guard that catches the
perf_stack regime-bar mislabel: a card whose published PASS/FAIL chip
contradicts the bar shown next to its primary value.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bundle import _bar_admits, bar_consistency_warning  # noqa: E402


def _ge(v):
    return {"op": "greater_than_or_equal", "value": v}


def _lt(v):
    return {"op": "less_than", "value": v}


def test_bar_admits_basic():
    assert _bar_admits(_ge(3.0), 3.0) is True
    assert _bar_admits(_ge(3.0), 1.226) is False
    assert _bar_admits(_lt(9000.0), 12.0) is True
    assert _bar_admits(_lt(9000.0), 9001.0) is False
    # Undecidable cases → None (no crash, no false alarm).
    assert _bar_admits(None, 1.0) is None
    assert _bar_admits(_ge(3.0), None) is None
    assert _bar_admits({"op": "greater_than_or_equal", "value": None}, 1.0) is True


def test_perf_stack_mislabel_is_flagged():
    # The exact regression: passed:true with primary 1.226 < displayed bar 3.0.
    bad = {
        "id": "perf_stack",
        "kind": "scaling",
        "passed": True,
        "primary": {"label": "speedup p50 (a→d)", "value": 1.226, "bar": _ge(3.0)},
    }
    warn = bar_consistency_warning(bad)
    assert warn is not None, "must flag passed:true against a bar it fails"
    assert "perf_stack" in warn


def test_consistent_loopback_card_is_clean():
    # The FIXED card: the displayed bar is the regime bar (0.8×) actually
    # applied, so passed:true and value 1.226 agree — no warning.
    good = {
        "id": "perf_stack",
        "passed": True,
        "primary": {"value": 1.226, "bar": _ge(0.8)},
    }
    assert bar_consistency_warning(good) is None


def test_consistent_fail_is_clean():
    fail = {
        "id": "x",
        "passed": False,
        "primary": {"value": 1.0, "bar": _ge(3.0)},
    }
    assert bar_consistency_warning(fail) is None


def test_no_primary_or_no_passed_is_clean():
    assert bar_consistency_warning({"id": "x", "passed": True}) is None
    assert bar_consistency_warning({"id": "x", "primary": {"value": 1.0}}) is None
    assert bar_consistency_warning({}) is None


def _run():
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except AssertionError as e:
                failures += 1
                print(f"FAIL {name}: {e}", file=sys.stderr)
    if failures:
        print(f"\n{failures} test(s) failed", file=sys.stderr)
        return 1
    print("\nall tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(_run())
