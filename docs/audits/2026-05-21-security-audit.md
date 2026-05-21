# Basin OSS — comprehensive security audit

**Date:** 2026-05-21
**Scope:** all of `crates/` + `services/basin-server/` at `main` (HEAD `a435ba8`).
**Mode:** read-only audit; no source/test edits.
**Cross-refs:**
- Wasm functions: `docs/audits/2026-05-21-wasm-functions-perf-security.md`
- Noisy-neighbor: `docs/audits/2026-05-21-noisy-neighbor-fairness.md`

This audit covers the new surfaces shipped this session (OAuth, MFA, blob
storage + RLS + signed URLs, realtime SSE/WS/presence, RPC mount, inbound
webhooks, reserved schemas, Wasm functions) **and** the pre-existing perimeter
(pgwire, REST JWT, cross-project isolation, secrets, rate limiting,
dependencies, TLS).

---

## 1. Executive summary

### Top 3 P0 (must-fix before beta)

| # | Finding | One-line exploit |
|---|---|---|
| **P0-1** | **TOTP replay protection is a no-op.** Both `verify_totp_factor` and `verify_totp_challenge` pass an empty `HashSet` as `used_steps` (`crates/basin-auth/src/mfa.rs:852, 931`), so an attacker who observes one 6-digit code can replay it for up to 90 seconds (current step ±1). | Replay any captured TOTP within ~90 s to bypass MFA → aal2 JWT issued (`mfa.rs:933`). |
| **P0-2** | **WebAuthn "verification" performs zero cryptography.** `verify_webauthn_factor` (`mfa.rs:1043`) and `verify_webauthn_challenge` (`mfa.rs:1182`) only check that the assertion JSON echoes back the issued challenge nonce — no signature verification against the registered credential, no counter check, no RP-ID binding, no origin check. The doc comment at `mfa.rs:1066-1069` admits "minimal structural validation". | Forge a WebAuthn assertion locally by constructing JSON with `type=public-key` and the (predictable, server-issued) challenge value → aal2 JWT issued without possessing any authenticator. |
| **P0-3** | **Inbound webhook endpoint has no HMAC verification — and the documented `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS` gate does not exist.** `POST /in/:project_id/:name` (`crates/basin-rest/src/routes/inbound.rs:52`) runs the registered SQL body with the caller's raw JSON body substituted in, gated only by webhook-name existence. Comment at `inbound.rs:21-26` acknowledges "v0.1 ships without HMAC verification". ADR 0019 §"TLS downgrade" promises a `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS=true` debug-only env gate; `grep -r BASIN_NET_ALLOW_PLAINTEXT` returns zero hits outside the ADR itself (`docs/decisions/0019-declarative-baas-surface.md:153`). | Any unauthenticated caller who can guess `project_id` + webhook `name` triggers the registered SQL body with arbitrary JSON payload — direct write into project tables / cron schedules / outbound HTTP. |

### Top 5 P1

| # | Finding | Surface |
|---|---|---|
| **P1-1** | **WebSocket subscribe `filter` parameter is silently ignored.** `ws.rs:678` destructures `ClientMsg::Subscribe { table, filter: _ }`. Predicate-filter pushdown (5.11.R5) compiles in `crates/basin-realtime/src/filter.rs` but is never wired into either the WS or SSE handler. Subscribers receive *all* RLS-permitted events for their `user_id` regardless of declared filter. | Realtime |
| **P1-2** | **Presence allows arbitrary `client_id` from the wire — no binding to JWT `user_id`.** `ws.rs:762-796` reads `client_id` from `ClientMsg::PresenceTrack` without any check that it matches `claims.user_id`. The connection can also track an arbitrary number of channels with arbitrary `metadata` — no per-connection cap, no per-channel cap, no metadata size cap. | Realtime |
| **P1-3** | **OAuth `redirect_to` allowlist is prefix-match only — vulnerable to host confusion.** `validate_redirect_to` (`oauth.rs:225-228`) does `redirect_to.starts_with(prefix)`. An allowlist of `https://example.com` matches `https://example.com.evil.com/cb` and `https://example.com@evil.com/cb`. | OAuth |
| **P1-4** | **Recovery codes use bcrypt cost 4 despite the doc claiming argon2.** `mfa.rs:25-34` doc-comment says "argon2 hashing of recovery codes". Actual code at `mfa.rs:729-730` uses `bcrypt::hash(&plain, 4)`. Comment at 726-728 admits "argon2 not in workspace yet". The recovery-code entropy (96 bits) makes this acceptable in practice, but a documentation/audit-trail mismatch will mislead reviewers. | MFA |
| **P1-5** | **`/admin/v1/projects/:id/{rotate,credentials}` does not check that the target project belongs to the caller's project-membership.** `admin.rs:86-141` checks only `claims.is_admin == true`; the path `:id` is then passed straight to `rotate_pgwire_password` / `list_project_credentials`. Any holder of *an* admin-grade JWT can rotate/list credentials for *any* project in the deployment. | REST admin |

### P2 list (counts only)

7 P2 findings: signed-URL secret reuses JWT secret (no rotation story — P2-1); OAuth identity linking trusts `email_verified=true` from any preset and unconditionally trusts GitHub even when the provider reported `false` (P2-2); OAuth state TTL is wall-clock-bound in DB but in-memory `OAuthStateCache` never expires (P2-3); MIME validation accepts the client-supplied `Content-Type` header without sniffing the body (P2-4); reserved-schema writes (`CREATE TABLE auth.users`) are not blocked at the engine layer (P2-5); pgwire username `{ulid}_{hex}` is treated as authoritative for project routing pre-bcrypt only as a hint, but `parse_project_from_pgwire_user` accepts any 27+ char string with `_` at position 26 (P2-6); JWT `iat` is not validated against a server-side issuance floor — a refresh-token compromise window equals refresh TTL (30 days default, `config.rs:376`) (P2-7).

---

## 2. Methodology

What I covered:
- Read every `.rs` in `crates/basin-auth/`, `crates/basin-blob/`,
  `crates/basin-realtime/`, `crates/basin-rest/src/routes/`,
  `crates/basin-net/src/{lib,guards,client}.rs`,
  `crates/basin-catalog/src/reserved_schema.rs`,
  `crates/basin-engine/src/schema_ddl.rs`.
- Grepped for: `secret`, `password`, `tls`, `plaintext`, `bcrypt`,
  `is_admin`, `is_reserved`, `verify_state`, `ct_eq`, `redirect`,
  `authorize`, `validate_ident`, `BASIN_NET_ALLOW_PLAINTEXT`.
- Cross-checked the existing `tests/integration/tests/security.rs` (578
  lines, 14 test cases) — covers cross-project ULID forgery, pgwire SQL
  injection, RLS bypass via UNION/CTE, partition-key traversal.
- Ran `cargo audit` (4 warnings, 0 vulnerabilities — see §7).

What I deferred:
- **Wasm function host imports** — the parallel audit at
  `2026-05-21-wasm-functions-perf-security.md` covers CPU/memory caps,
  the spawn_blocking thread leak, the per-project semaphore-map flood,
  the `query` host-call full-result materialisation, the secret-store
  project-scoping contract gap, and the HTTP-impl wiring drift. I do not
  re-cover those here.
- **Cron**, **iceberg-rest**, **vector**, **geo**, **trgm**,
  **placement**, **sketch** — outside this audit's scope; these crates
  do not expose auth/RLS boundaries directly.
- **pgwire extended-bind binary-format edge cases** — surface area is
  large; the existing `security.rs::pgwire_sql_injection_via_extended_bind`
  test gives partial coverage. Marked as "needs runtime test" below.

---

## 3. Findings table

| ID | Sev | Surface | File:line | Description |
|---|---|---|---|---|
| P0-1 | P0 | MFA | `crates/basin-auth/src/mfa.rs:852, 931` | TOTP replay window is never enforced — `verify_totp(secret, code, &HashSet::new())` |
| P0-2 | P0 | MFA | `crates/basin-auth/src/mfa.rs:1043-1126, 1182-1240` | WebAuthn verifies only the JSON shape + echoed challenge nonce; no signature, counter, AAGUID, RP-ID, or origin check |
| P0-3 | P0 | REST | `crates/basin-rest/src/routes/inbound.rs:52, 21-26` | Inbound webhook has no HMAC + no http-scheme guard despite ADR-promised `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS` env |
| P1-1 | P1 | Realtime | `crates/basin-realtime/src/ws.rs:678` + `crates/basin-realtime/src/sse.rs` (no `filter` plumbing) | WS/SSE `subscribe.filter` parsed off the wire but never compiled or applied |
| P1-2 | P1 | Realtime | `crates/basin-realtime/src/ws.rs:762-810` | Presence `client_id` impersonation + unbounded `metadata` size + no per-conn cap |
| P1-3 | P1 | OAuth | `crates/basin-auth/src/oauth.rs:225-228` | `redirect_to` allowlist uses naive prefix match → `prefix.evil.com` and `prefix@evil.com` pass |
| P1-4 | P1 | MFA | `crates/basin-auth/src/mfa.rs:729-730` | Recovery codes use bcrypt cost 4, not argon2id as documented |
| P1-5 | P1 | REST admin | `crates/basin-rest/src/routes/admin.rs:86-141` | Admin token from one project can rotate / list credentials for any project ULID |
| P2-1 | P2 | Signed URLs | `crates/basin-rest/src/routes/storage_sign.rs:140, 188` | Signing secret = JWT secret; no rotation story; key compromise = global blast radius |
| P2-2 | P2 | OAuth | `crates/basin-auth/src/oauth.rs:1215` | GitHub identity is always treated as verified regardless of provider response; `is_email_verified` accepts loose "true"-string |
| P2-3 | P2 | OAuth | `crates/basin-auth/src/oauth.rs:710-871` | In-memory `OAuthStateCache` never expires entries — long-running test/dev servers accumulate state |
| P2-4 | P2 | Storage | `crates/basin-rest/src/routes/storage.rs:286-291` | Server takes `Content-Type` from client header without any byte-level sniffing; comment at `storage.rs:33-37` calls this "server-side enforcement" but it isn't |
| P2-5 | P2 | Reserved schema | `crates/basin-engine/src/schema_ddl.rs:144-163` | Engine binds qualified `auth.users` directly to the reserved schema for any caller — DML/DDL on reserved schemas is not gated by `is_admin` |
| P2-6 | P2 | Pgwire | `crates/basin-auth/src/project_credentials.rs:102-109` | `parse_project_from_pgwire_user` returns the first 26 chars as a "project ULID" without validating the ULID alphabet; used as a routing hint pre-bcrypt |
| P2-7 | P2 | JWT | `crates/basin-auth/src/jwt.rs:115-130` + `config.rs:376` | No issuance-floor (kid + rotated-at) check; refresh-token compromise window = 30-day refresh TTL |

---

## 4. P0/P1 expanded

### P0-1: TOTP replay protection is a no-op

**Code:**
```rust
// mfa.rs:850-852  (verify_totp_factor)
let secret = enc.decrypt(&factor.secret_enc)?;
let _step = verify_totp(&secret, code, &HashSet::new())?;
//                                       ^^^^^^^^^^^^^ always empty

// mfa.rs:929-931  (verify_totp_challenge)
let secret = enc.decrypt(&factor.secret_enc)?;
let _step = verify_totp(&secret, code, &HashSet::new())?;
//                                       ^^^^^^^^^^^^^ always empty
```

The crate doc-comment at `mfa.rs:35-36` advertises "Replay cache: in-memory `DashMap<(user_id, step)>` with short TTL via the existing `governor` infrastructure". That cache is not built; the call sites pass a fresh empty `HashSet`. `verify_totp` itself (`mfa.rs:650-685`) is correct — it skips steps in `used_steps` — so adding the cache is the fix; the cryptography is fine.

**Repro (words):** Capture a victim's TOTP code (shoulder-surf, proxy leak, phishing). Within ~90s, replay the same 6-digit code against `POST /auth/v1/factors/:id/challenge/verify` for the victim's challenge → response carries an `aal2` access token (`mfa.rs:933 issue_aal2_tokens`). RLS policies that gate on `auth.aal() = 'aal2'` now permit access. The attacker does not need to compromise the TOTP secret.

**Mitigation:** Materialise the documented DashMap. Key by `(factor_id, step)`; insert on success in `verify_totp`; size-bound the map with `dashmap::DashMap::retain` on a background task; TTL = the size of the verify window (3 steps × 30s = 90s, plus skew).

---

### P0-2: WebAuthn verification is JSON-shape-only — no signature

**Code:**
```rust
// mfa.rs:1066-1106  (verify_webauthn_factor)
// In a real production implementation this would call `webauthn_rs` to
// verify the attestation. For the OSS implementation we do the minimal
// structural validation to prove the wiring compiles and tests pass,
// mirroring the pattern `webauthn-rs` would follow.
let attest: serde_json::Value = serde_json::from_str(attestation_json)?;
if attest.get("type").and_then(|v| v.as_str()) != Some("public-key") { … }
// … extract clientDataJSON, base64-decode, parse JSON …
let returned_challenge = client_data.get("challenge")…;
let challenge_ok: bool = returned_challenge.as_bytes()
    .ct_eq(challenge.challenge_data.as_bytes()).into();
// store the raw attestation as the "credential" (encrypted)
let secret_enc = enc.encrypt(attestation_json)?;
```

Same pattern at `mfa.rs:1182-1240` for `verify_webauthn_challenge`. The
factor is marked `verified` and an `aal2` JWT is issued.

What's missing (every one of these is a load-bearing FIDO2 check):
- No COSE public-key parsing.
- No signature verification of `authenticatorData || sha256(clientDataJSON)`.
- No counter / signature-counter rollback check (counter clone detection).
- No AAGUID allow/deny list.
- No RP-ID hash comparison against the registered RP.
- No origin comparison (clientDataJSON.origin).
- No `type` check on clientDataJSON itself (must be `webauthn.create` for
  attestation, `webauthn.get` for assertion).
- No attestation chain validation.

**Repro (words):** Hit `POST /auth/v1/factors` with `factor_type=webauthn`
→ server returns `challenge_id` + `challenge_b64`. Construct attestation
JSON locally:
```json
{
  "type": "public-key",
  "response": {
    "clientDataJSON": "<base64url of {\"challenge\":\"<echoed challenge>\"}>"
  }
}
```
POST to `/auth/v1/factors/:id/verify` → factor flips to `verified`. Then
challenge → verify with the same shape → aal2 JWT. No authenticator is
ever involved.

**Mitigation:** Wire `webauthn-rs` end-to-end (the dependency is named in
the crate doc but never pulled in). Until then, *do not advertise
WebAuthn support* and reject the `factor_type=webauthn` path with a
501.

---

### P0-3: Inbound webhook accepts unauthenticated payloads

**Code:** `crates/basin-rest/src/routes/inbound.rs:21-26`
```rust
//! Inbound webhooks are HMAC-authenticated by the caller (e.g. Stripe). v0.1
//! ships **without** HMAC verification — the ADR marks the full signature
//! scheme as v0.2. The endpoint is accessible without a bearer token; …
//! Operators requiring access control before v0.2 can gate this behind a
//! reverse-proxy rule.
```
Handler at `inbound.rs:52-107`: extracts `project_id`, validates the
webhook `name` is a SQL identifier, looks up the catalog row, parses the
body as JSON, substitutes `payload` in the registered SQL body, executes.

**Repro (words):** Enumerate project ULIDs (26-char crockford base32 — feasible if any leak via error messages, public listings, or inbound webhook URLs printed in customer setup docs). For each, try common webhook names (`stripe`, `github`, `app`). Any 200 response means the registered SQL body just ran with attacker-controlled `payload`. If the body is e.g. `INSERT INTO orders (data) VALUES (payload)`, the attacker writes arbitrary rows.

Compounding: ADR 0019 `docs/decisions/0019-declarative-baas-surface.md:153` promises:
> TLS required by default; HTTP-only inbound webhooks rejected unless `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS=true` (debug-only env, never set in prod).

`grep -r BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS /Users/pc/code/exo/basin/crates /Users/pc/code/exo/basin/services` returns 0 hits. The flag is not implemented; nothing about the inbound handler examines the request scheme. Plaintext `http://` inbound is accepted with no extra log.

**Mitigation:** Either remove the inbound surface from the v0.1 ship, or add a per-webhook `secret` column + `X-Basin-Signature` HMAC verification with `subtle::ConstantTimeEq` before the SQL body runs. Wire the env-gate that ADR 0019 promises.

---

### P1-1: WS/SSE subscribe `filter` is parsed and ignored

**Code:** `crates/basin-realtime/src/ws.rs:677-678`
```rust
match msg {
    ClientMsg::Subscribe { table, filter: _ } => {
```
`crates/basin-realtime/src/filter.rs` has a complete, tested compile-once
filter implementation, but no call site in `ws.rs` or `sse.rs` ever
constructs a `Filter`. The on-the-wire shape (`{"type":"subscribe","filter":"NEW.status='paid'"}`)
documented at `ws.rs:13-15, 209-213` is accepted by serde and discarded.

**Impact:** Subscribers receive every event their RLS (causation_user
match) lets through, not the subset they declared. For a chatty `events`
table this is a bandwidth and metadata-leak amplifier — a subscriber who
only wanted `status='paid'` events also sees every `status='draft'`
row their session is permitted to read. Not a privilege escalation in
itself (RLS still applies), but a confidentiality contract violation
because the client believes the filter is enforced server-side.

**Mitigation:** Construct `Filter::new(filter_str)` at subscribe time;
hold the `Arc<Filter>` per-subscription; call `filter.matches(&event)`
inside the `forwarder_task` before forwarding (it already returns
`Result<bool, String>` with fail-closed semantics).

---

### P1-2: Presence allows arbitrary client_id + unbounded metadata

**Code:** `crates/basin-realtime/src/ws.rs:762-796`
```rust
ClientMsg::PresenceTrack { channel, client_id, metadata } => {
    …
    presence.track(*project_id, &channel, &client_id, metadata);
```
No check that `client_id` equals `claims.user_id` or any identifier the
JWT proves the caller owns. No size cap on `metadata` (typed as
`serde_json::Value`). No per-connection cap on number of channels.

**Impact:**
- **Impersonation.** Alice connects with her JWT, sends
  `presence_track` with `client_id="bob"` → all other subscribers see
  Bob marked online; Alice has effectively "spoofed" Bob's presence.
- **Metadata flood.** A megabyte of JSON in `metadata` is broadcast to
  every subscriber on the channel. With 100 subscribers, each `track`
  costs 100 MB of fanout serialisation.
- **Channel flood.** Subscribe to 10 K channels per connection, each
  publishing a `presence_state` snapshot on join. Per-channel
  forwarder task pile-up.

**Mitigation:** Bind `client_id` to `claims.user_id.to_string()` (or
require it to be a prefix). Cap `metadata` JSON size at e.g. 4 KB.
Cap the number of presence channels per connection (e.g. 32).

---

### P1-3: `redirect_to` allowlist uses prefix match

**Code:** `crates/basin-auth/src/oauth.rs:225-228`
```rust
for prefix in allowed_prefixes {
    if redirect_to.starts_with(prefix.as_str()) {
        return Ok(());
    }
}
```
Allowlist entries come from `OAuthProviderRow::redirect_uri`
(comma-split, `oauth.rs:1019-1024`).

**Repro (words):** Project owner configures
`redirect_uri = https://example.com`. Attacker sends victim to
`/auth/v1/authorize?provider=google&redirect_to=https://example.com.evil.com/cb`
or `https://example.com@evil.com/cb` (URL userinfo). Both pass
`starts_with`. The OAuth dance completes, basin issues real tokens, and
the browser is 302-redirected to the attacker's domain with the tokens
in the URL fragment / cookie — an account-takeover primitive.

**Mitigation:** Parse the URL (`url::Url::parse`), compare `origin()`
strictly (`scheme + host + port`), then optionally match the path
prefix.

---

### P1-4: Recovery codes use bcrypt(cost=4) — doc claims argon2

**Code:** `mfa.rs:716-733`
```rust
pub fn generate_recovery_codes() -> (Vec<String>, Vec<String>) {
    …
    for _ in 0..RECOVERY_CODE_COUNT {
        let mut bytes = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut bytes);
        let plain = hex::encode(bytes);
        let hash = bcrypt::hash(&plain, 4)
            .unwrap_or_else(|_| format!("HASH_FAILED:{plain}"));
        …
```
Doc at `mfa.rs:25-34`:
> Eight recovery codes are issued at first factor enrollment. Each is a
> 12-byte random value hex-encoded. They are stored argon2id-hashed and
> consumed single-use.

The cost-4 bcrypt + 96-bit input means brute force is not feasible (cost
4 is ~3 ms × 2^96), so the *security* outcome is OK. The audit-trail
defect — a doc-vs-code mismatch on a key cryptographic primitive — is
the P1.

**Secondary issue:** the `unwrap_or_else` fallback writes the *plaintext
code* into the hash column with prefix `HASH_FAILED:`. If bcrypt fails
on any platform (e.g. blowfish init OOM), the plaintext lands in the DB
column intended for hashes. Recovery-code disclosure → MFA bypass.

**Mitigation:** Either implement argon2id, or update the doc to say
"bcrypt cost 4". Replace the `unwrap_or_else` with a propagated `Err`.

---

### P1-5: Admin endpoints lack project binding

**Code:** `crates/basin-rest/src/routes/admin.rs:86-99`
```rust
pub(crate) async fn rotate_project(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(pgwire_user): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let info = state.cfg.auth
        .rotate_pgwire_password(&pgwire_user)
        .await
        .map_err(ApiError::from)?;
```
Same shape at `admin.rs:108-141` for `list_project_credentials` —
no comparison between `claims.project_id` and the path `project_id`.

The doc at `admin.rs:1-6` admits "the wedge customer's control-plane
mints one such token at deploy time" — i.e. the deployment model is
single-project control plane. In a multi-project deployment (basin-cloud),
that single admin token shouldn't reach across projects.

**Mitigation:** Require either (a) `is_admin = true && claims.project_id == path_project_id`, or (b) a separate `is_super_admin` claim for the cross-project control-plane case, with the basic `is_admin` capped to own-project.

---

## 5. Existing controls (positive findings)

- **`security.rs` integration suite** — 14 tests covering cross-project
  fork structural impossibility (`security.rs:486`), RLS bypass via
  `UNION`/CTE (`security.rs:399, 449`), pgwire SQL injection via simple
  *and* extended bind (`security.rs:180, 240`), path traversal in table
  and project IDs (`security.rs:278, 310`), partition-key traversal
  (`security.rs:341`), pgwire rate-limit throttle (`security.rs:518`),
  ULID strict round-trip (`security.rs:538`).
- **JWT algorithm allow-listing.** `JwtKeys::new` constructs
  `Validation::new(Algorithm::HS256)` (`jwt.rs:117-122`). `jsonwebtoken`
  rejects tokens whose header `alg` doesn't match this list — `alg:none`
  is closed.
- **Refresh-token rotation + reuse detection.**
  `crates/basin-auth/src/flows/refresh.rs:57-131` rotates jti on every
  refresh and writes a blanket sentinel on reuse, invalidating all
  outstanding refreshes for the user. Aud-claim binding
  (`REFRESH_AUDIENCE = "basin-refresh"`) prevents access-token-as-refresh
  cross-replay (verified by `jwt.rs::access_token_rejected_by_refresh_verify`).
- **OAuth state HMAC + DB-bound nonce.** `create_state` /
  `verify_state_hmac` (`oauth.rs:159-200`) use `subtle::ConstantTimeEq`,
  bind `(project_id, provider, nonce)`, and the
  `consume_oauth_state` (`oauth.rs:566-588`) `DELETE … WHERE
  expires_at > now() RETURNING …` is a single-use TTL gate done in one
  SQL round-trip.
- **PKCE S256.** `pkce_pair` (`oauth.rs:137-144`) generates 32-byte
  verifier + `base64url(sha256(verifier))` challenge; included in the
  authorize URL with `code_challenge_method=S256`
  (`oauth.rs:1084`).
- **Signed-URL HMAC.** `compute_mac` (`storage_sign.rs:91-102`) is
  canonical-form bound over `(project, bucket, path, expires)` with
  newline separators; verify is `ct_eq` (`storage_sign.rs:193`);
  expiry checked first to avoid timing-leak via MAC compute on
  expired tokens.
- **Path traversal guard for blobs.** `paths.rs:38-57` rejects empty
  paths, leading/trailing `/`, and any component `==".."`. Tested at
  `paths.rs:136-159`.
- **Reserved-schema alias for user schemas.** `resolve_schema`
  (`reserved_schema.rs:131-144`) aliases any unknown schema to `public`,
  preventing collisions between user-defined `auth` schemas and the
  reserved namespace. (DOES NOT prevent writes to *reserved* schemas —
  see P2-5.)
- **SSRF guard on outbound HTTP.** `AllowList::check`
  (`basin-net/src/guards.rs:121-138`) is per-project deny-by-default
  on host basis; allowlist comes *before* rate-limit (intentional —
  comment at `guards.rs:14-17`) so a blocked host cannot exhaust the
  rate budget.
- **Per-project body-cap + timeout on outbound HTTP.** `GuardConfig`
  (`guards.rs:34-65`) — 10 MiB body, 30 s timeout, env-overridable but
  read once at construction (no mid-flight mutation).
- **bcrypt cost 12 default for password hashing.**
  `crates/basin-auth/src/config.rs:377`. Matches OWASP 2025 guidance for
  bcrypt.
- **API-key fall-back path scopes roles to empty + email to
  `<api-key>`.** `server.rs:271-282` — engine code that reads
  `claims.roles` cannot mistake an API-key call for an `admin` JWT.
- **Cross-project isolation on WS upgrade.** `ws.rs:413-420` rejects the
  upgrade with 403 when `claims.project_id != path project_id` — before
  the WebSocket is even established.
- **Pgwire self-routing username format.** `{ulid}_{hex}` embeds the
  project in the username (`project_credentials.rs:65-74`); the catalog
  is authoritative — the username is only a routing hint, the bcrypt
  hash is the gate.

---

## 6. Regression-test gaps

For each finding, the focused test the parallel test-add agent should
land in `tests/integration/tests/security.rs`:

| ID | Proposed test | Shape |
|---|---|---|
| P0-1 | `totp_replay_within_step_window_rejected` | Enroll TOTP, consume code at step N → 200; immediately replay same code → must 401. |
| P0-2 | `webauthn_forged_assertion_rejected` | Begin enrollment → POST `verify` with hand-crafted JSON `{"type":"public-key","response":{"clientDataJSON":"<base64 of echoed challenge>"}}`. **Currently passes** (this is the bug); test asserts it's rejected. |
| P0-3 | `inbound_webhook_without_signature_rejected` | POST `/in/:project/:name` with no `X-Basin-Signature` header → 401. |
| P0-3 | `inbound_webhook_http_scheme_rejected_in_prod` | Same as above, but specifically with the env var unset → 400. |
| P1-1 | `ws_subscribe_filter_actually_filters` | Subscribe with `filter: NEW.status='paid'`, publish a `draft` event → must NOT be delivered. |
| P1-2 | `ws_presence_track_rejects_other_user_client_id` | Alice's JWT cannot track `client_id="bob"` → 4003 close or error frame. |
| P1-2 | `ws_presence_metadata_size_capped` | track with 1 MB metadata → rejected. |
| P1-3 | `oauth_redirect_to_rejects_subdomain_confusion` | Allowlist `https://example.com`; attempt `https://example.com.evil.com/cb` → reject. Same for `@evil.com`. |
| P1-4 | `recovery_codes_use_argon2_or_doc_updated` | Inspect a stored hash's prefix — `$argon2id$` or doc test asserts current bcrypt. |
| P1-5 | `admin_rotate_other_project_rejected` | Mint admin JWT for project A, attempt `POST /admin/v1/projects/<B's pgwire_user>/rotate` → 403. |
| P2-1 | `signed_url_rotates_when_jwt_secret_rotates` | After rotating `BASIN_AUTH_JWT_SECRET`, an outstanding signed URL with the old secret fails → 403. Needs a rotation mechanism first. |
| P2-2 | `github_email_verified_respected` | Stub provider returns `email_verified=false`; identity creation must NOT auto-link to an existing verified email. |
| P2-3 | `oauth_state_cache_expires` | In-memory cache: insert state, advance clock past TTL, consume must fail. |
| P2-4 | `mime_sniff_overrides_client_header` | Upload a PNG body with `Content-Type: text/plain` → stored MIME is `image/png`. (needs sniffing impl first) |
| P2-5 | `create_table_in_reserved_schema_rejected` | `CREATE TABLE auth.evil (…)` on a regular user JWT → reject. |
| P2-6 | `pgwire_user_with_invalid_ulid_prefix_rejected` | Username `notavalidulid_00000000` (correct length, invalid char) → bcrypt path still runs (current) but routing hint should error. |
| P2-7 | `refresh_token_compromise_window_bounded` | Mint refresh at T=0, secret rotates at T=5min, presented refresh at T=10min must fail. Needs key-rotation infra. |

**Test-gap dispatch count:** 17 tests.

Additional "needs runtime test" items (not auditable from source alone):
- Behaviour of pgwire extended-bind with binary-format edge cases for
  `numeric`, `interval`, and JSON types.
- TLS posture for `pgwire-tls`: when client connects with `sslmode=allow`,
  is plaintext rejected or downgraded? Need a black-box test against the
  running server.
- WebAuthn implementation, once landed, against a `webauthn-rs`
  test-vector.

---

## 7. Dependency advisories

`cargo audit` against current `Cargo.lock` (run 2026-05-21):

| Crate | Version | RUSTSEC | Class |
|---|---|---|---|
| `bincode` | 1.3.3 | 2025-0141 | unmaintained |
| `paste` | 1.0.15 | 2024-0436 | unmaintained |
| `rustls-pemfile` | 2.2.0 | 2025-0134 | unmaintained |
| `lru` | 0.12.5 | 2026-0002 | unsound (`IterMut` Stacked Borrows violation) |

**Triage:**
- `bincode 1.x` is a transitive dep of `basin-wal` and `basin-vector`.
  Upgrade path is `bincode 2.x` (different API). No active CVE, just
  unmaintained.
- `lru` 0.12 is in `basin-storage` + `basin-engine`. The unsoundness is
  a `Miri`-only violation (Stacked Borrows model); will not trigger
  miscompilation on stable rustc today, but is the kind of thing that
  bites a future LLVM rev. Upgrade to `lru 0.13+`.
- `paste` and `rustls-pemfile` — both unmaintained-only.

No **vulnerabilities** (only warnings). The CI `cargo audit --deny
warnings` line in `SECURITY.md` does not match the actual workflow file
`.github/workflows/ci.yml:144` ("Default `cargo audit` exits non-zero
on vulnerabilities only"). Beta-gate: either align CI to `--deny warnings`
as the doc claims, or update `SECURITY.md`.

---

## 8. Sizing — effort to fix per P0/P1, beta gates

| ID | Effort | Beta-gate? |
|---|---|---|
| P0-1 (TOTP replay cache) | 2-4 h | **Yes.** Single-finding fix; the cache shape is already specified in the doc-comment. |
| P0-2 (WebAuthn real verification) | 1-2 days | **Yes.** Pull in `webauthn-rs`, wire registration + assertion paths, drop the JSON-only stub. Alternative: remove the WebAuthn factor type until the impl lands. |
| P0-3 (inbound webhook HMAC + scheme guard) | 1 day | **Yes.** Add `secret` column to inbound webhook def, add `X-Basin-Signature` header verify with `subtle::ConstantTimeEq`, wire the env-gated scheme check. |
| P1-1 (WS/SSE filter wiring) | 4-6 h | **Yes** if the wire shape is GA; otherwise drop `filter` from the docs. |
| P1-2 (presence binding + caps) | 4-8 h | **Yes.** Trust-boundary fix; one-line per check. |
| P1-3 (redirect_to strict origin) | 2 h | **Yes.** Trivial fix; one of the highest-leverage account-takeover primitives. |
| P1-4 (recovery codes: argon2 or doc fix) | 2 h doc fix, 1 day for argon2 impl | Beta: doc fix is sufficient (codes have 96-bit entropy). |
| P1-5 (admin project binding) | 1 h | **Yes if multi-project.** Single-project deployments unaffected. |

**Total beta-blocker effort:** ~4-5 engineer-days.

**P2 fixes are not beta gates** but should be tracked in the next
hardening sprint (~3-5 days total).

---

*End of audit.*
