# basin-sdk

Async Rust client for [Basin](https://basin.run) — the open-source Postgres-
compatible HTAP database. Covers the REST data API, auth, object storage,
server-side functions, and realtime change streams. Built on `tokio` + `reqwest`
+ `serde`; no blocking variant.

Works against any deployed `basin-engine` — the managed regional deployments at
`https://<region>.basin.run`, or a self-hosted engine (`cargo run -p
basin-server` / the published container). The SDK speaks directly to the engine;
Basin Cloud is the control plane (dashboard, billing, key minting) but is
**off the data path** once you have a URL + key.

The library name is `basin`, so you `use basin::…` after adding the crate.

## Install

```toml
[dependencies]
basin-sdk = "0.1"
```

```sh
# or with cargo-add:
cargo add basin-sdk
```

### Feature flags

| Feature    | Default | Adds |
|------------|:-------:|------|
| `realtime` | no | WebSocket change/presence streams (`tokio-tungstenite`, `futures-util`) |
| `arrow`    | no | Native Arrow IPC transport (`arrow-array`, `arrow-ipc`, `arrow-schema`) |

Enable features you need:

```toml
basin-sdk = { version = "0.1", features = ["realtime", "arrow"] }
```

## Quickstart

```rust,no_run
use basin::Client;

#[tokio::main]
async fn main() -> Result<(), basin::BasinError> {
    // BASIN_URL  — engine base URL, e.g. https://basin-engine.fly.dev
    // BASIN_KEY  — JWT or raw API key (both sent as Authorization: Bearer)
    // Mint a key at https://basin.run/app/project/<ref>/api-keys
    let client = Client::builder(std::env::var("BASIN_URL").unwrap())
        .token(std::env::var("BASIN_KEY").unwrap())
        // .project_id("01J...") — optional when the token is a JWT
        .build()?;

    // Health check
    assert_eq!(client.health().await?, "ok");

    // Filtered query — hits /rest/v1/orders
    let result = client
        .table("orders")
        .select("id,total,status")
        .eq("status", "paid")
        .gte("total", 100i64)
        .order("total", false) // descending
        .limit(50)
        .run()
        .await?;

    for row in &result.rows {
        println!("{row}");
    }

    // Deserialise straight into a typed Vec
    #[derive(serde::Deserialize)]
    struct Order { id: i64, total: f64, status: String }
    let orders: Vec<Order> = client
        .table("orders")
        .select("id,total,status")
        .rows()
        .await?;
    let _ = orders;

    // Writes
    client
        .table("orders")
        .insert(&serde_json::json!({"total": 50, "status": "new"}))
        .await?;
    client
        .table("orders")
        .eq("id", 1i64)
        .update(&serde_json::json!({"status": "paid"}))
        .await?;
    client.table("orders").eq("id", 1i64).delete().await?;

    Ok(())
}
```

`Client` is cheap to `Clone` — it shares one connection pool and one session
lock — so clone freely across tasks rather than building several.

### Sync over async

There is no blocking client. Drive a runtime from synchronous code:

```rust,no_run
# use basin::Client;
let rt = tokio::runtime::Runtime::new().unwrap();
let client = Client::builder("http://localhost:8080").token("k").build().unwrap();
let rows = rt.block_on(async { client.table("t").select("*").run().await }).unwrap();
let _ = rows;
```

## Auth

The `auth()` sub-client wraps every `/auth/v1/*` flow. Sessions are stored
behind an async `RwLock`; the access token **auto-refreshes** when within 10 s
of expiry and concurrent callers share a single refresh round-trip.

```rust,no_run
# use basin::Client;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
// Sign in — session stored automatically, subsequent requests use it
let session = client.auth().sign_in("you@example.com", "hunter2", None).await?;
let _ = session;

// API keys (one-time secret shown on creation)
let key = client.auth().create_api_key("ci-deploy").await?;
println!("store this once: {}", key.secret);

// List and revoke
let keys = client.auth().list_api_keys().await?;
if let Some(k) = keys.first() {
    client.auth().delete_api_key(k.id).await?;
}

// Magic link
client.auth().request_magic_link("you@example.com").await?;
// ... user clicks the emailed link, your app receives the token:
// client.auth().consume_magic_link(token).await?;

// OAuth — mint the provider URL server-side, redirect the browser to it
let oauth = client.auth()
    .get_oauth_authorize_url("github", "https://myapp.com/auth/callback", None)
    .await?;
println!("redirect to: {}", oauth.redirect_url);

// Password flows
client.auth().request_password_reset("you@example.com", None).await?;
// client.auth().reset_password(token, "new-password", None).await?;

// Verify email after sign-up
// client.auth().verify_email(token, None).await?;

// Sign out — revokes the refresh token server-side, clears local session
client.auth().sign_out().await?;
# Ok(())
# }
```

### Multi-factor authentication

TOTP and WebAuthn factors share the same lifecycle:

```rust,no_run
# use basin::Client;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
use basin::types::MfaEnrollResult;

// Enroll a TOTP factor
let enrollment = client.auth().enroll_factor("totp", "Authenticator App").await?;
if let MfaEnrollResult::Totp(totp) = enrollment {
    println!("scan this URI: {}", totp.otpauth_uri);
    // Confirm enrollment with a code from the authenticator app
    let result = client.auth()
        .verify_factor(&totp.factor_id, Some("123456"), None, None)
        .await?;
    if let Some(codes) = result.recovery_codes {
        println!("save recovery codes: {codes:?}");
    }
}

// Step-up challenge (produces an aal2 session JWT)
let factors = client.auth().list_factors().await?;
if let Some(f) = factors.first() {
    let challenge = client.auth().challenge_factor(&f.id).await?;
    // Use the challenge_id + TOTP code to verify:
    use basin::types::MfaChallengeResult;
    if let MfaChallengeResult::Totp(c) = challenge {
        let _aal2_session = client.auth()
            .verify_challenge(&f.id, &c.challenge_id, Some("654321"), None)
            .await?;
    }
}
# Ok(())
# }
```

### API-key auth and pgwire

**API key format:** `basin_{tenant_id}_{base64}`. Pass the full key to
`.token()`; the SDK sends it as `Authorization: Bearer` and the server tries
JWT verify first, then API-key lookup.

For direct psql / DBeaver / migration-tool access use the engine's pgwire
listener (default port 5433):

```
# JWT auth — access token as the username:
psql "postgres://<access_token>@<engine-host>:5433/basin"

# API-key auth:
psql "postgres://{tenant_id}_{hex}:<api_key>@<engine-host>:5433/basin"
```

`auth.uid()`, `auth.role()`, and `auth.jwt()` work identically over pgwire —
the same RLS policies apply.

## Query builder

The builder wraps `/rest/v1/:table` (GET / POST / PATCH / DELETE). Filter ops
map directly to Basin's PostgREST-style dialect.

```rust,no_run
# use basin::{Client, Scalar};
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
// Equality, comparison, null-check, IN-list
let rows = client
    .table("products")
    .select("id,name,price")
    .eq("active", true)
    .gte("price", 10i64)
    .lt("price", 500i64)
    .is("archived_at", "null")
    .r#in("category", [Scalar::Str("A".into()), Scalar::Str("B".into())])
    .order("price", true)  // ascending
    .offset(20)
    .limit(10)
    .run()
    .await?;
println!("{} rows, next_cursor: {:?}", rows.rows.len(), rows.next_cursor);
# Ok(())
# }
```

Supported filter operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `is`.
Not supported: `or`, `not`, `like`/`ilike`, embedded resource selects. Filters
AND together.

### Cursor pagination

A query with `limit` or `cursor` returns `{ rows, next_cursor }`; the SDK
normalises both response shapes into `QueryResult`. Feed `next_cursor` back for
keyset O(1) pagination:

```rust,no_run
# use basin::Client;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
let mut cursor: Option<String> = None;
loop {
    let mut q = client.table("events").select("id,ts").order("id", true).limit(1000);
    if let Some(c) = &cursor {
        q = q.cursor(c);
    }
    let page = q.run().await?;
    for row in &page.rows {
        // process row
        let _ = row;
    }
    match page.next_cursor {
        Some(c) => cursor = Some(c),
        None => break,
    }
}
# Ok(())
# }
```

## Storage

```rust,no_run
# use basin::Client;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
let storage = client.storage();

// Bucket management
storage.create_bucket("avatars", /*public=*/true, /*size_limit=*/None, &[]).await?;
let meta = storage.get_bucket("avatars").await?;
let _ = meta;

let bucket = storage.from_bucket("avatars");

// Upload / download
bucket.upload("users/1.png", std::fs::read("1.png").unwrap(), Some("image/png")).await?;
let dl = bucket.download("users/1.png").await?;
println!("{} bytes, type={:?}", dl.data.len(), dl.content_type);

// List objects with optional prefix + pagination
let objects = bucket.list(Some("users/"), Some(50), None).await?;
println!("{} objects", objects.len());

// Delete one or many by prefix
bucket.remove("users/1.png").await?;
bucket.remove_by_prefixes(&["users/old/".to_string()]).await?;

// Public URL (bucket must have public=true, project_id must be set on the client)
let public_url = bucket.get_public_url("logo.png")?;
println!("{public_url}");

// Time-limited signed download URL (TTL capped at 7 days server-side)
let signed = bucket.create_signed_url("private/report.pdf", 3600).await?;
println!("{}", signed.absolute_url);
# Ok(())
# }
```

## Functions

Two invocation surfaces:

```rust,no_run
# use basin::Client;
# async fn ex(client: Client) -> Result<(), basin::BasinError> {
// Catalog UDF — POST /rest/v1/rpc/:fn
// Scalar result unwraps to the bare value; RETURNS TABLE gives an array.
let total = client
    .rpc("sum_orders", Some(&serde_json::json!({"since": "2024-01-01"})))
    .await?;
println!("total: {total}");

// HTTP-handler Wasm function — ANY /fn/v1/:name (response proxied verbatim)
use reqwest::Method;
let res = client
    .functions()
    .invoke(
        "thumbnailer",
        Method::POST,
        Some(basin::InvokeBody::Json(serde_json::json!({"w": 128, "h": 128}))),
        &[],
    )
    .await?;
println!("status={} data={}", res.status, res.data);
# Ok(())
# }
```

## Realtime (feature `realtime`)

Add `features = ["realtime"]` to your dependency. `listen()` returns a
`futures::Stream` of `ServerFrame` values for one table or presence channel.
The client transparently reconnects with exponential backoff (0.5 s → 1 s →
2 s … capped at 30 s) and re-issues the subscription on each reconnect.

```rust,no_run
# #[cfg(feature = "realtime")]
# async fn ex(client: basin::Client) -> Result<(), basin::BasinError> {
use futures_util::StreamExt;
use basin::realtime::{ServerFrame, SubscribeOptions};

// Subscribe with a server-side predicate filter
let mut stream = client.realtime().listen(
    "orders",
    SubscribeOptions {
        filter: Some("NEW.status = 'paid'".into()),
        last_event_id: None, // set to resume after a disconnect
    },
);

while let Some(frame) = stream.next().await {
    match frame? {
        ServerFrame::Event { op, table, after, seq, .. } => {
            println!("[{op}] {table} seq={seq}: {after:?}");
        }
        ServerFrame::Gap { last_event_id, oldest_in_ring, .. } => {
            // Requested last_event_id predates the replay ring — cold re-sync needed
            println!("gap: requested {last_event_id}, ring starts at {oldest_in_ring}");
        }
        ServerFrame::Error { code, table, missed, .. } => {
            println!("lag on {table}: code={code}, missed={missed:?}");
        }
        _ => {}
    }
}
# Ok(())
# }
```

Presence frames (`PresenceState`, `PresenceDiff`, `PresenceError`) are fully
parsed and delivered via the stream. To receive them, subscribe to a presence
channel name. Note: the current SDK surfaces presence as a read-only `Stream`;
`presence_track` / `presence_untrack` send methods are not yet implemented
(see ROADMAP).

## Arrow IPC (feature `arrow`)

Add `features = ["arrow"]` to your dependency. `to_arrow()` sends
`Accept: application/vnd.apache.arrow.stream`. When the server replies with a
native Arrow IPC stream the bytes decode into `arrow_array::RecordBatch` —
full i64/timestamp fidelity, no JSON round-trip. Older servers that return JSON
surface as `ArrowResult::JsonFallback`. Pagination cursors come back in the
`x-basin-next-cursor` / `x-basin-row-count` response headers.

```rust,no_run
# #[cfg(feature = "arrow")]
# async fn ex(client: basin::Client) -> Result<(), basin::BasinError> {
use basin::arrow::{ArrowQuery, ArrowResult};

match client
    .table("events")
    .select("id,ts,value")
    .limit(50_000)
    .to_arrow()
    .await?
{
    ArrowResult::Ipc { batches, next_cursor, row_count } => {
        println!("{} batches, {} total rows", batches.len(), row_count.unwrap_or(0));
        for batch in &batches {
            println!("  batch: {} rows x {} cols", batch.num_rows(), batch.num_columns());
        }
        let _ = next_cursor; // pass to .cursor() for the next page
    }
    ArrowResult::JsonFallback { rows, next_cursor } => {
        println!("JSON fallback: {} rows", rows.len());
        let _ = next_cursor;
    }
}
# Ok(())
# }
```

## Error handling

Every fallible call returns `Result<T, BasinError>`. Match on the stable `E_*`
code, not the message (the message is human-readable and not a stability
contract):

```rust,no_run
# use basin::{Client, BasinError};
# async fn ex(client: Client) -> Result<(), BasinError> {
match client.table("orders").eq("id", 1i64).delete().await {
    Ok(_) => println!("deleted"),
    Err(BasinError::Api(e)) if e.code == "E_ENGINE_UNSUPPORTED" => {
        // 501 — engine without DELETE support
        eprintln!("DELETE not supported on this engine");
    }
    Err(BasinError::Api(e)) if e.sqlstate.as_deref() == Some("23505") => {
        // Postgres unique-constraint violation, surfaced with SQLSTATE
        eprintln!("unique violation: {}", e.message);
    }
    Err(BasinError::Api(e)) if e.code == "E_UNAUTHENTICATED" => {
        eprintln!("sign in first");
    }
    Err(BasinError::Network(msg)) => {
        eprintln!("network: {msg}");
    }
    Err(e) => return Err(e),
}
# Ok(())
# }
```

`BasinError` variants: `Api(ApiError)`, `Network(String)`, `Decode(String)`,
`InvalidRequest(String)`, `Realtime(String)` (feature `realtime`).

`ApiError` fields: `code: String`, `message: String`, `status: u16`,
`sqlstate: Option<String>`.

The documented stable codes are in `basin::ERROR_CODES`. An unknown code from a
newer server passes through verbatim — older clients keep working.

## SDK family

basin-sdk is part of the Basin SDK family. All SDKs speak directly to
`basin-engine` over the same REST + auth + realtime + storage surface; Basin
Cloud is the control plane, not the data path.

| SDK | Repo | Transport |
|-----|------|-----------|
| Rust | `basin-rust` (this crate) | `reqwest` async, pgwire |
| JavaScript / TypeScript | `basin-js` | `fetch`, browser / Node / Bun |
| Python | `basin-py` | `httpx` async + sync |

Engine connection options: pgwire (port 5433, for psql / ORMs / migrations) and
the REST + WebSocket HTTP surface (this SDK). Both honour the same RLS policies
and session functions (`auth.uid()`, `auth.role()`, `auth.jwt()`).

## License

MIT — see [`LICENSE`](./LICENSE).
