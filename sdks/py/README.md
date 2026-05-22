# basin-py

The Python SDK for [basin](../basin) — a basin-native, async-first client on
`httpx` that mirrors [`basin-js`](../basin-js) method-for-method while
following Python idiom (an optional sync facade, `async for` iterators,
`pyiceberg` interop).

> **Status: pre-v0.1 — scaffolding.** No code is published yet. The plan lives
> in [`ROADMAP.md`](./ROADMAP.md); the agent-sized work items in
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
