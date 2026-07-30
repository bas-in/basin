#!/usr/bin/env python3
"""make-og-card.py — render site/assets/og-card.png (1200x630).

The landing/docs pages' shared og:image / twitter:image. Generated rather than
hand-drawn so the mark cannot drift from brand/logo.svg: the tile below is the
approved SVG's own markup, parsed out and nested unmodified into the card at a
larger size — never redrawn or re-declared as a second literal. Colours are
lifted from site/index.html's :root tokens so the card and the page read as
one system.

Requires rsvg-convert (renders the composed SVG to PNG). Run from repo root:
    python3 scripts/make-og-card.py
"""
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOGO_SVG = REPO_ROOT / "brand" / "logo.svg"
OUT_PNG = REPO_ROOT / "site" / "assets" / "og-card.png"

W, H = 1200, 630

# Basin's survey palette, lifted verbatim from site/index.html's :root block.
INK = "#0F1115"
INK_2 = "#16191F"
PAPER = "#FAF7F0"
BASIN_TEAL = "#3B6F73"
BASIN_TEAL_DEEP = "#2A5256"
SEDIMENT_OCHRE = "#C4732B"
GRAPHITE_3 = "#828282"

FONT_DISPLAY = "Helvetica Neue, Helvetica, Arial, sans-serif"
FONT_MONO = "Menlo, Consolas, monospace"


def extract_mark_markup(svg_text: str) -> str:
    """Pull every drawable element out of brand/logo.svg's <svg>...</svg>,
    verbatim, so the card nests the exact approved mark rather than a
    redrawn approximation. Keeps the rect/ellipse/path elements as-is."""
    body = re.search(r"<svg\b[^>]*>(.*)</svg>", svg_text, re.S)
    if not body:
        raise SystemExit("make-og-card: could not find <svg>...</svg> in brand/logo.svg")
    inner = body.group(1)
    # Drop the <title> element (not meaningful nested inside a larger card;
    # the card gets its own accessible text) but keep every drawn shape.
    inner = re.sub(r"<title>.*?</title>", "", inner, flags=re.S)
    return inner.strip()


def build_svg(mark_markup: str) -> str:
    tile_size = 168
    tile_x, tile_y = 84, 84

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="{INK_2}"/>
      <stop offset="100%" stop-color="{INK}"/>
    </linearGradient>
    <radialGradient id="glow" cx="88%" cy="6%" r="60%">
      <stop offset="0%" stop-color="{BASIN_TEAL}" stop-opacity="0.28"/>
      <stop offset="100%" stop-color="{BASIN_TEAL}" stop-opacity="0"/>
    </radialGradient>
  </defs>

  <rect width="{W}" height="{H}" fill="url(#bg)"/>
  <rect width="{W}" height="{H}" fill="url(#glow)"/>

  <!-- survey contour isolines, faint, behind everything -->
  <g fill="none" stroke="{BASIN_TEAL_DEEP}" stroke-width="1.5" opacity="0.35">
    <ellipse cx="980" cy="470" rx="520" ry="210"/>
    <ellipse cx="980" cy="470" rx="420" ry="170"/>
    <ellipse cx="980" cy="470" rx="320" ry="130"/>
  </g>

  <!-- corner ticks, framing the sheet like the landing page's panels -->
  <g stroke="{GRAPHITE_3}" stroke-width="2" opacity="0.6">
    <path d="M40 40 L72 40 M40 40 L40 72"/>
    <path d="M{W - 40} 40 L{W - 72} 40 M{W - 40} 40 L{W - 40} 72"/>
    <path d="M40 {H - 40} L72 {H - 40} M40 {H - 40} L40 {H - 72}"/>
    <path d="M{W - 40} {H - 40} L{W - 72} {H - 40} M{W - 40} {H - 40} L{W - 40} {H - 72}"/>
  </g>

  <!-- brand tile: brand/logo.svg's own markup, nested unmodified and scaled -->
  <svg x="{tile_x}" y="{tile_y}" width="{tile_size}" height="{tile_size}" viewBox="0 0 128 128">
    {mark_markup}
  </svg>

  <text x="{tile_x + tile_size + 32}" y="{tile_y + 108}" font-family="{FONT_DISPLAY}"
        font-size="72" font-weight="700" fill="{PAPER}" letter-spacing="-2">Basin</text>

  <text x="84" y="330" font-family="{FONT_DISPLAY}" font-size="46" font-weight="700"
        fill="{PAPER}" letter-spacing="-1">A Postgres-compatible database</text>
  <text x="84" y="388" font-family="{FONT_DISPLAY}" font-size="46" font-weight="700"
        fill="{PAPER}" letter-spacing="-1">that lives on <tspan fill="{BASIN_TEAL}">object storage</tspan>.</text>

  <rect x="86" y="424" width="120" height="4" fill="{SEDIMENT_OCHRE}"/>

  <text x="84" y="474" font-family="{FONT_DISPLAY}" font-size="26" fill="{GRAPHITE_3}">
    Projects are S3 prefixes, not databases. One binary, pgwire on the
  </text>
  <text x="84" y="508" font-family="{FONT_DISPLAY}" font-size="26" fill="{GRAPHITE_3}">
    front, Vortex-compressed columnar files on the back.
  </text>

  <text x="84" y="{H - 56}" font-family="{FONT_MONO}" font-size="22" fill="{GRAPHITE_3}"
        letter-spacing="0.5">vulos.org/projects/basin</text>
</svg>
"""


def main() -> None:
    svg_text = LOGO_SVG.read_text(encoding="utf-8")
    mark_markup = extract_mark_markup(svg_text)
    card_svg = build_svg(mark_markup)

    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        ["rsvg-convert", "-w", str(W), "-h", str(H), "-o", str(OUT_PNG)],
        input=card_svg.encode("utf-8"),
    )
    if proc.returncode != 0:
        sys.exit(f"make-og-card: rsvg-convert failed with code {proc.returncode}")
    print(f"og-card: wrote {OUT_PNG} ({W}x{H})")


if __name__ == "__main__":
    main()
