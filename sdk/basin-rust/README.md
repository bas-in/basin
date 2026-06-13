# basin-sdk (Rust)

Async Rust client for [Basin](../../README.md)'s HTTP surfaces: the REST data
API, auth, functions, realtime, and object storage. Built on `tokio` +
`reqwest` + `serde` — the natural async Rust stack.

- **Async-first** — every network method is `async fn` returning
  `Result<T, BasinError>`. There is no blocking variant; drive it from a
  `tokio` runtime (see [Sync over async](#sync-over-async)).
- **Derived from the server, not invented** — every binding cites the basin-rest
  route it talks to (see the per-module docs); routes are verified against
  `crates/basin-rest/src/server.rs`.
- **Typed errors with SQLSTATE** — failures decode into [`BasinError`] carrying
  the stable `E_*` code, HTTP status, and the Postgres `SQLSTATE` when present.
- **Feature-gated extras** — `realtime` (WebSocket change/presence streams) and
  `arrow` (native Arrow IPC transport) are off by default so the core crate
  stays lean.

## Install

```toml
[dependencies]
basin-sdk = "0.1"

# with optional features:
basin-sdk = { version = "0.1", features = ["realtime", "arrow"] }
```

The crate's library name is `basin`, so you `use basin::...`.

## Quickstart

```rust,no_run
use basin::Client;

#[tokio::main]
async fn main() -> Result<(), basin::BasinError> {
    let client = Client::builder("http://localhost:8080")
        .token("my-api-key-or-jwt")   // JWT or raw API key — both Bearer-sent
        .project_id("01J...")          // optional when the token is a JWT
        .build()?;

    // Health check.
    assert_eq!(client.health().await?, "ok");

    // Query builder over /rest/v1/:table.
    let result = client
        .table("orders")
        .select("id,total,status")
        .eq("status", "paid")
        .gte("total", 100i64)
        .order("total", false)   // descending
        .limit(50)
        .run()
        .await?;
    for row in &result.rows {
        println!("{row}");
    }

    // Or deserialise straight into your own type:
    #[derive(serde::Deserialize)]
    struct Order { id: i64, total: f64 }
    let orders: Vec<Order> = client.table("orders").select("id,total").rows().await?;
    let _ = orders;

    // Writes.
    client.table("orders").insert(&serde_json::json!({"total": 50, "status": "new"})).await?;
    client.table("orders").eq("id", 1i64).update(&serde_json::json!({"status": "paid"})).await?;
    client.table("orders").eq("id", 1i64).delete().await?;

    Ok(())
}
```

### Sync over async

There is no blocking client. To call from synchronous code, drive a runtime:

```rust,no_run
# use basin::Client;
let rt = tokio::runtime::Runtime::new().unwrap();
let client = Client::builder("http://localhost:8080").token("k").build().unwrap();
let rows = rt.block_on(async { client.table("t").select("*").run().await }).unwrap();
let _ = rows;
```

## Cursor pagination

A query with `limit` or `cursor` returns the `{ rows, next_cursor }` shape; the
SDK normalises both into [`QueryResult`]. Page by feeding `next_cursor` back:

```rust,no_run
# use basin::Client;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
let mut cursor: Option<String> = None;
loop {
    let mut q = client.table("events").select("*").limit(1000);
    if let Some(c) = &cursor {
        q = q.cursor(c);
    }
    let page = q.run().await?;
    // … process page.rows …
    match page.next_cursor {
        Some(c) => cursor = Some(c),
        None => break,
    }
}
# Ok(())
# }
```

## Auth

The `auth()` client wraps every `/auth/v1/*` flow. Sessions are held behind an
async lock and the access token is **auto-refreshed** when within 10 s of
expiry — concurrent callers share a single refresh.

```rust,no_run
# use basin::Client;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
let session = client.auth().sign_in("a@b.com", "pw", None).await?;
// Subsequent requests use the session token automatically.

// API keys, magic links, password reset, OAuth, MFA all live here:
let issued = client.auth().create_api_key("ci-key").await?;
println!("store this once: {}", issued.secret);

let oauth = client.auth().get_oauth_authorize_url("github", "/done", None).await?;
println!("redirect the browser to {}", oauth.redirect_url);

client.auth().sign_out().await?; // revokes the refresh token server-side
let _ = session;
# Ok(())
# }
```

MFA covers the full TOTP + WebAuthn factor lifecycle: `enroll_factor`,
`list_factors`, `verify_factor`, `challenge_factor`, `verify_challenge` (returns
an `aal2` session), and `unenroll_factor` (requires `aal2`).

## Storage

```rust,no_run
# use basin::Client;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
let storage = client.storage();
storage.create_bucket("media", true, None, &[]).await?;
let media = storage.from_bucket("media");

media.upload("avatars/1.png", std::fs::read("1.png").unwrap(), Some("image/png")).await?;
let dl = media.download("avatars/1.png").await?;
println!("{} bytes, {:?}", dl.data.len(), dl.content_type);

// Public URL (bucket must be public) and time-limited signed URL.
let public = media.get_public_url("avatars/1.png")?;
let signed = media.create_signed_url("avatars/1.png", 3600).await?;
let _ = (public, signed);
# Ok(())
# }
```

## Functions

```rust,no_run
# use basin::Client;
# use reqwest::Method;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
// Catalog UDF — POST /rest/v1/rpc/:fn.
let total = client.rpc("sum_orders", Some(&serde_json::json!({"since": "2024-01-01"}))).await?;
let _ = total;

// HTTP-handler Wasm function — ANY /fn/v1/:name (response proxied verbatim).
let res = client
    .functions()
    .invoke("thumbnailer", Method::POST,
            Some(basin::InvokeBody::Json(serde_json::json!({"w": 128}))), &[])
    .await?;
println!("function returned {} -> {}", res.status, res.data);
# Ok(())
# }
```

## Realtime (feature `realtime`)

`listen()` returns a `futures::Stream` of change frames for a table and
transparently reconnects with exponential backoff (0.5 s → 30 s), re-issuing
the subscription each time. Auth uses the `Sec-WebSocket-Protocol: basin-v1,
<token>` subprotocol form, matching the JS SDK and the server's `ws.rs`.

```rust,no_run
# #[cfg(feature = "realtime")]
# async fn ex(client: basin::Client) -> Result<(), basin::BasinError> {
use futures_util::StreamExt;
use basin::realtime::{ServerFrame, SubscribeOptions};

let mut stream = client.realtime().listen("orders", SubscribeOptions {
    filter: Some("NEW.status = 'paid'".into()),
    last_event_id: None,
});
while let Some(frame) = stream.next().await {
    match frame? {
        ServerFrame::Event { op, after, .. } => println!("{op}: {after:?}"),
        ServerFrame::Gap { .. } => { /* cold re-sync from the analytical store */ }
        _ => {}
    }
}
# Ok(())
# }
```

## Arrow IPC (feature `arrow`)

`to_arrow()` sends `Accept: application/vnd.apache.arrow.stream`. When the
server replies with a native Arrow IPC stream the bytes decode straight into
`arrow_array::RecordBatch` (full i64 / timestamp fidelity, no JSON round-trip);
pagination state comes back in the `x-basin-next-cursor` / `x-basin-row-count`
response headers. Older servers that return JSON surface as
[`arrow::ArrowResult::JsonFallback`] with the raw rows.

```rust,no_run
# #[cfg(feature = "arrow")]
# async fn ex(client: basin::Client) -> Result<(), basin::BasinError> {
use basin::arrow::{ArrowQuery, ArrowResult};

match client.table("events").select("*").limit(10_000).to_arrow().await? {
    ArrowResult::Ipc { batches, next_cursor, .. } => {
        for b in &batches { println!("{} rows", b.num_rows()); }
        let _ = next_cursor;
    }
    ArrowResult::JsonFallback { rows, .. } => {
        println!("server returned JSON: {} rows", rows.len());
    }
}
# Ok(())
# }
```

## Error handling

Every fallible call returns `Result<T, BasinError>`. Match on the **stable
code**, never the message:

```rust,no_run
# use basin::{Client, BasinError};
# async fn ex(client: Client) -> Result<(), BasinError> {
match client.table("orders").delete().await {
    Ok(_) => {}
    Err(BasinError::Api(e)) if e.code == "E_ENGINE_UNSUPPORTED" => {
        // engine without DELETE support (501)
    }
    Err(BasinError::Api(e)) if e.sqlstate.as_deref() == Some("23505") => {
        // unique-constraint violation surfaced with its SQLSTATE
    }
    Err(e) => return Err(e),
}
# Ok(())
# }
```

The documented codes are in [`ERROR_CODES`]; a newer server may emit an
undocumented code, which passes through verbatim so an older SDK keeps working.

## Feature flags

| Feature    | Default | Adds                                                              |
|------------|:-------:|------------------------------------------------------------------|
| `realtime` |   no    | WebSocket change/presence streams (`tokio-tungstenite`, `futures`) |
| `arrow`    |   no    | Native Arrow IPC transport (`arrow-array`, `arrow-ipc`, `arrow-schema`) |

## Capability notes (honest)

- **Query grammar is PostgREST-*style*, not full PostgREST.** Supported:
  `select`, `eq/neq/gt/gte/lt/lte/in/is`, `order`, `limit`, `offset`, `cursor`.
  **Not** supported: `or=`, `not.`, `like/ilike`, embedded resource selects,
  `Prefer` headers. Filters AND together.
- **DELETE may be unsupported** on some engines — surfaces as
  `E_ENGINE_UNSUPPORTED` (501).
- **OAuth is authorize-URL only.** The SDK mints the provider authorize URL;
  the browser redirect and the server-side callback exchange happen outside the
  SDK.
- **WebAuthn MFA needs a browser bridge.** The SDK passes the
  `creation_options_json` / `request_options_json` and `attestation` /
  `assertion` blobs through; the actual `navigator.credentials.*` call is the
  app's job.
- **Realtime replay is best-effort.** If `last_event_id` predates the server's
  replay ring you receive a `Gap` frame and must cold re-sync.

This crate is **standalone** — it is intentionally not a member of the engine
Cargo workspace, so it has its own `target/` and dependency graph.
