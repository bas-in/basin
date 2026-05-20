---
title: "ADR 0020 — Auth v2: OAuth providers + MFA"
nav_section: decisions
sidebar_position: 20
summary: "Lifts ADR 0005's OAuth + MFA deferral. OSS basin-auth ships OAuth2/OIDC (provider presets + generic OIDC config) and MFA (TOTP + WebAuthn/passkeys together), with an AAL claim in the JWT. Cloud builds provider-registration UI only — no new primitives."
tags: [auth, security, oauth, mfa, oss]
---

# 0020 — Auth v2: OAuth providers + MFA

- **Status:** Accepted, 2026-05-20.
- **Tags:** auth, security, oauth, mfa
- **Supersedes:** the "deferred to v2" lines in
  [ADR 0005](./0005-auth-system.md) for OAuth + MFA.
- **Cross-references:**
  [ADR 0005 (auth system)](./0005-auth-system.md),
  [ADR 0013 (auth per-project schema)](./0013-auth-per-project-schema.md),
  [ADR 0006 (REST API layer)](./0006-rest-api-layer.md).

## Context

ADR 0005 shipped password + magic-link + JWT + API keys and explicitly
deferred **OAuth / social providers** and **MFA (TOTP, WebAuthn)** to
v2. basin-js stubbed `signInWithOAuth()` and `auth.mfa.*` with
`not_implemented` errors pending engine routes. This ADR lifts both
deferrals.

Decisions locked 2026-05-20:

1. **OAuth: provider presets + generic OIDC.** Ship Google / GitHub /
   Apple presets AND a generic OIDC config path for the long tail.
2. **MFA: TOTP + WebAuthn/passkeys together** in one spec (not TOTP-only
   first).
3. Both flows live in **OSS `basin-auth`**; cloud builds only the
   provider-registration / factor-management UI.

## Decision

### OAuth (authorization-code flow, OSS `basin-auth`)

- **Provider config** persisted in an `auth.oauth_providers` table
  (per-project): `provider` (preset name or `oidc`), `client_id`,
  `client_secret` (encrypted via the `EncryptionProvider` trait),
  `scopes`, `redirect_uri`, and for `oidc` the discovery URL or
  explicit `authorize`/`token`/`userinfo` endpoints.
- **Presets** ship endpoint/scope defaults for Google, GitHub, Apple.
  A preset row only needs `client_id` + `client_secret`.
- **Generic OIDC** uses RFC 8414 discovery (`.well-known/openid-
  configuration`) when given an issuer URL; falls back to explicit
  endpoints.
- **Endpoints:**
  - `GET /auth/v1/authorize?provider=<name>&redirect_to=<app_url>` —
    builds the provider authorize URL with a signed `state` (CSRF
    protection) + PKCE `code_challenge`, 302-redirects.
  - `GET /auth/v1/callback` — validates `state`, exchanges `code` for
    tokens (PKCE `code_verifier`), fetches userinfo, links/creates the
    `auth.users` row, issues Basin's own JWT + refresh token.
- **Identity linking:** match on verified email → link to existing
  user; otherwise create. An `auth.identities` table records
  `(user_id, provider, provider_user_id)` so one user can have
  multiple linked providers.

### MFA (OSS `basin-auth`)

- **Factor types:** `totp` (RFC 6238) and `webauthn` (passkeys / FIDO2).
- **AAL claim:** the JWT carries `aal` (`aal1` = single factor, `aal2`
  = MFA-verified) plus `amr` (methods array). RLS policies and basin-js
  consult `auth.aal()` — a new SQL session function alongside the
  existing `auth.uid()` / `auth.role()` / `auth.jwt()`.
- **Enrollment / verification endpoints:**
  - `POST /auth/v1/factors` — begin enrollment (TOTP: returns secret +
    otpauth URI for QR; WebAuthn: returns creation challenge).
  - `POST /auth/v1/factors/:id/verify` — confirm enrollment.
  - `POST /auth/v1/factors/:id/challenge` — begin a step-up challenge.
  - `POST /auth/v1/factors/:id/challenge/verify` — complete step-up;
    re-issues a JWT with `aal2`.
  - `DELETE /auth/v1/factors/:id` — unenroll (requires aal2).
- **Recovery codes:** one-time codes issued at first factor enrollment,
  stored hashed (argon2), single-use.
- **Tables:** `auth.mfa_factors` (id, user_id, type, status, secret/
  credential — encrypted), `auth.mfa_challenges` (short-TTL), `auth.
  mfa_recovery_codes` (hashed).

### Crypto provenance

No hand-rolled crypto. RustCrypto + established crates:
- `totp-rs` or `oath` for TOTP (RFC 6238).
- `webauthn-rs` for FIDO2 / passkeys (handles attestation + assertion).
- `oauth2` crate for the authz-code + PKCE flow.
- `subtle` for constant-time comparisons.
- Secrets at rest via the shipped `EncryptionProvider` trait.

## Security model

| Threat | Mitigation |
|---|---|
| OAuth CSRF / login-CSRF | Signed `state` parameter (HMAC, short TTL) validated on callback. |
| Authorization-code interception | PKCE (`code_challenge`/`code_verifier`) on every flow, including confidential clients. |
| Open-redirect via `redirect_to` | `redirect_to` validated against a per-project allowlist; reject otherwise. |
| Account takeover via unverified email | Only link on a provider-asserted **verified** email; otherwise create a distinct identity and require explicit linking. |
| TOTP replay | One-step-window tolerance; used-step cache rejects immediate reuse. |
| Recovery-code brute force | argon2-hashed, single-use, rate-limited via the existing `governor` setup. |
| Secret exposure | Provider secrets + TOTP seeds encrypted at rest; masked in `information_schema` / `pg_*` output; never logged. |
| Phishable second factor | WebAuthn/passkeys are origin-bound (unphishable); TOTP documented as the weaker option. |
| Downgrade (skip MFA) | Once a user has an enrolled factor, JWT issuance caps at `aal1` until a factor challenge succeeds; `aal2`-requiring RLS policies fail closed. |

## OSS / cloud split

| Concern | OSS (`basin-auth`) | basin-cloud |
|---|---|---|
| OAuth flow (authorize/callback/PKCE/linking) | ✅ | — |
| Provider config storage (encrypted) | ✅ | — |
| Provider app-registration UI (paste client_id/secret) | — | ✅ UI over OSS routes |
| Managed redirect URLs | — | ✅ |
| TOTP + WebAuthn enroll/verify/challenge | ✅ | — |
| AAL claim + `auth.aal()` SQL fn | ✅ | — |
| Factor-list dashboard, MFA-enforcement policy toggles | — | ✅ UI |

No new primitives land cloud-side — every cloud item is UI over the OSS
routes.

## What this does NOT include

- SAML / enterprise SSO — still deferred (ADR 0005); enterprise-only.
- SMS / phone-number auth — rejected (cost, SIM-swap risk).
- Per-provider account-merge UX beyond verified-email matching — basin-js
  surfaces a manual "link account" flow; engine just stores identities.

## References

- [ADR 0005 — Auth system](./0005-auth-system.md) — the v1 that
  deferred these; this ADR is the v2 lift.
- [ADR 0013 — Auth per-project schema](./0013-auth-per-project-schema.md)
  — where `auth.*` tables live and how `auth.uid()` etc. resolve.
- 2026-05-20 conversation log — the OAuth/MFA/storage planning session
  and the three locked decisions.
