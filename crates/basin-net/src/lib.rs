//! `basin-net` — a Postgres `http` extension + Supabase `pg_net` SQL surface
//! for Basin.
//!
//! Apps using either of those Postgres extensions drop in here. The crate
//! provides:
//!
//! 1. A synchronous [`HttpClient`] that performs `GET` / `POST` requests
//!    inline. Mirrors the [`http`](https://github.com/pramsey/pgsql-http)
//!    extension's `http_get(url)` / `http_post(url, body, ct)` calls.
//! 2. An asynchronous [`RequestQueue`] that enqueues a request and writes the
//!    response into a per-project `_net_http_response` table. Mirrors
//!    Supabase's [`pg_net`](https://github.com/supabase/pg_net)
//!    `net.http_post` / `net._http_response` shape.
//! 3. A [`ResponseStore`] that exposes the persisted response rows.
//!
//! ## SQL surface (target shape)
//!
//! ```sql
//! -- Sync (Postgres `http` extension):
//! SELECT status, content FROM http_get('https://api.example.com/foo');
//! SELECT status FROM http_post(
//!   'https://hooks.slack.com/services/...',
//!   '{"text":"deploy ok"}',
//!   'application/json'
//! );
//!
//! -- Async (pg_net):
//! SELECT net.http_post(
//!   url := 'https://api.example.com/charge',
//!   body := '{"amount":1000}',
//!   headers := '{"Authorization":"Bearer sk_test_..."}'
//! ) AS request_id;
//!
//! -- Read response later:
//! SELECT * FROM _net_http_response WHERE id = $request_id;
//! ```
//!
//! In v0.1 the SQL surface is exposed as a Rust API on [`HttpClient`],
//! [`RequestQueue`], and [`ResponseStore`] only. The matching SQL function
//! interceptor lives in `basin-engine`'s `net_glue` module today as a
//! deliberate no-op stub — when the executor's pre-parse hook learns to
//! recognise the `http_get` / `http_post` table-function shape and the
//! `net.http_post` schema-qualified call form, the registration will land
//! there and `Engine::new` will pick it up automatically. The same staging
//! pattern is used by `basin-cron` for `cron.schedule(...)`.
//!
//! ## Per-project guardrails (non-negotiable)
//!
//! - **URL allowlist**: per-project catalog config. Default DENY-ALL. Hosts
//!   are opted in via [`HttpClient::allow_host`] (or `INSERT INTO
//!   _net_allowed_hosts ...` once the SQL surface lands). URLs whose host
//!   isn't in the allowlist surface `BasinError::Internal("host not on
//!   allowlist: ...")`. This is the SSRF-prevention boundary; it is the
//!   single most-load-bearing safety check in the crate.
//! - **Body cap**: 10 MiB default per request. Configurable via the
//!   `BASIN_NET_MAX_BODY_BYTES` env var, read once at [`HttpClient::new`]
//!   time. Applies to request bodies and response bodies.
//! - **Timeout**: 30s default per request. Configurable via
//!   `BASIN_NET_TIMEOUT_SECS`.
//! - **Rate limit**: 10 req/s sustained, burst 30, per project. Token bucket
//!   via the `governor` crate. A project that bursts over its quota gets a
//!   `BasinError::Internal("rate limited (basin_net)")` instead of an
//!   outbound request.
//! - **TLS verify on by default**. There is no way to disable from
//!   user-facing API. `reqwest` is built with `rustls-tls` so we never call
//!   into a system OpenSSL.
//!
//! ## Isolation
//!
//! All per-project state — allowlist, rate-limit bucket, response table — is
//! keyed on [`ProjectId`]. Project A's allowlist edit cannot affect B's
//! requests; A's response rows are only visible inside A's session (the same
//! catalog-level namespace boundary the rest of the engine relies on).

#![forbid(unsafe_code)]

mod client;
mod guards;
#[cfg(feature = "engine-store")]
mod queue;
#[cfg(feature = "engine-store")]
mod store;
mod types;

pub use client::HttpClient;
pub use guards::{
    check_url_safety, is_denied_ip, AllowList, GuardConfig, RateLimit,
    RebindingGuardedResolver, IMDS_V4, IMDS_V6,
};
#[cfg(feature = "engine-store")]
pub use queue::{RequestQueue, RequestQueueHandle};
#[cfg(feature = "engine-store")]
pub use store::ResponseStore;
pub use types::{HttpError, HttpRequest, HttpResponse, RequestId, ResponseRow};
