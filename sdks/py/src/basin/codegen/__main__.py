"""
CLI entry point — ``python -m basin.codegen`` / ``basin-gen-types`` (T-015).

Fetches the engine's OpenAPI document and writes typed Python row models.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass

from ..errors import BasinError
from ..openapi.fetch import fetch_openapi
from .emit import find_table_names, openapi_to_types


@dataclass
class _Args:
    url: str
    key: str
    out: str
    pydantic: bool


def _parse_args(argv: list[str]) -> _Args:
    parser = argparse.ArgumentParser(
        prog="basin-gen-types",
        description="Generate typed Python row models from a basin engine's OpenAPI doc.",
    )
    parser.add_argument("--url", required=True, help="Basin engine root URL")
    parser.add_argument("--key", required=True, help="Anon API key")
    parser.add_argument(
        "--out",
        default="database.py",
        help="Output file path (default: database.py)",
    )
    parser.add_argument(
        "--pydantic",
        action="store_true",
        help="Emit pydantic v2 BaseModel classes instead of TypedDicts",
    )
    ns = parser.parse_args(argv)
    return _Args(url=ns.url, key=ns.key, out=ns.out, pydantic=ns.pydantic)


async def _run(args: _Args) -> tuple[str, int]:
    doc = await fetch_openapi(args.url, args.key)
    tables = find_table_names(doc)
    return openapi_to_types(doc, pydantic=args.pydantic), len(tables)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        source, table_count = asyncio.run(_run(args))
    except BasinError as exc:
        raise SystemExit(f"error: failed to generate types — {exc}") from exc

    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(source)
    sys.stdout.write(f"wrote {args.out} ({table_count} tables)\n")


if __name__ == "__main__":
    main()
