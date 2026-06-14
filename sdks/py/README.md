# basin-py

The Python SDK for [basin](../basin) — a basin-native, async-first client on
`httpx` that mirrors [`basin-js`](../basin-js) method-for-method while
following Python idiom (an optional sync facade, `async for` iterators,
`pyiceberg` interop).

> **Status: v0.1 in development — core surface implemented, not yet
> published.** The query builder (filters, NDJSON streaming, cursor
> pagination), auth (password / magic-link / OAuth / MFA), admin, storage,
> functions, realtime (SSE + WebSocket + presence), retry with backoff, the
> sync facade, and OpenAPI-driven type codegen are all in place with a green
> `pytest` / `mypy --strict` / `ruff` suite. The plan lives in
> [`ROADMAP.md`](./ROADMAP.md); per-task status is tracked in
> [`TASKS.md`](./TASKS.md). The public surface is settled in `basin-js`;
> basin-py ports it without drifting on method names or route shapes.

## Planned shape

```python
from basin import create_client

client = create_client("https://<project>.basin.run", "<anon-key>")

# query builder — awaitable
res = await client.from_("users").select("*").eq("active", True).limit(50)

# basin-distinctive: cursor pagination + NDJSON streaming
async for row in client.from_("events").select().paginate():
    ...

# auth, storage, functions, realtime — Supabase-shaped, basin-routed
await client.auth.sign_in_with_password(email=..., password=...)
await client.functions.invoke("monthly_rollup", {"month": "2026-05"})
```

A `SyncClient` facade covers the same surface for scripts, notebooks, and
sync frameworks.

Generate typed row models from a running engine's OpenAPI document:

```sh
python -m basin.codegen --url https://<project>.basin.run --key <anon-key> --out database.py
# or: basin-gen-types --url … --key … --out database.py [--pydantic]
```

## Install (once published)

```sh
pip install basin                 # core (httpx only)
pip install 'basin[pydantic]'     # typed row models
pip install 'basin[realtime]'     # WebSocket realtime
pip install 'basin[iceberg]'      # pyiceberg catalog interop
```

## Develop

```sh
pip install -e '.[dev]'
ruff check . && mypy --strict src && pytest
```

MIT licensed (client SDKs are MIT; the basin server is Apache-2.0).
