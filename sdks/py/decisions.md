# basin-py — Decisions

Running log of non-obvious choices. Newest first.

---

## 2026-05-22 — Repo seeded (ROADMAP + TASKS)

basin-py created as the Python sibling of basin-js. Key decisions baked into
the roadmap:

- **Async-first on `httpx`, with a delegating `SyncClient` facade.** The async
  client is the single implementation; the sync facade wraps it via a private
  event loop. No parallel sync codebase.
- **One runtime dep (`httpx`).** `pydantic` (typed rows), `websockets`
  (realtime WS), and `pyiceberg` (catalog) are optional extras, never required.
  SSE realtime rides on `httpx` with no extra.
- **Mirror basin-js exactly** on method names + route shapes. Any drift is a
  docs/support tax; parity is enforced by a fixture matrix (T-060).
- **Python 3.9+**, `from __future__ import annotations`, ship `py.typed`,
  `mypy --strict`.
- **Tooling:** hatchling build backend, `uv` for dev/CI, `ruff`, `pytest` +
  `pytest-asyncio` + `respx`.
- **Iceberg = interop, not a hand-rolled client.** Python already has
  `pyiceberg`; basin-py just hands back a configured `Catalog` pointed at the
  engine's REST catalog. This is the one place Python leads JS.

### Open decision (resolve in T-001)

- **PyPI distribution name.** Prefer `basin` (import package `basin`); if the
  name is taken on PyPI, fall back to distribution `basin-sdk` keeping the
  import package `basin`. Record the outcome here.
