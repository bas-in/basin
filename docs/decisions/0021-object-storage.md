---
title: "ADR 0021 — Object storage (catalog-backed blobs)"
nav_section: decisions
sidebar_position: 21
summary: "Supabase-style blob storage. Objects are rows in a storage.objects system table, access control reuses the RLS engine, bytes live in the same object_store the engine uses, signed URLs are HMAC over (path, expiry). New basin-blob crate. Cloud builds quota/billing/CDN/image-transforms."
tags: [storage, security, baas, oss]
---

# 0021 — Object storage (catalog-backed blobs)

- **Status:** Accepted, 2026-05-20.
- **Tags:** storage, security, baas
- **Supersedes:** none
- **Cross-references:**
  [ADR 0006 (REST API layer)](./0006-rest-api-layer.md),
  [ADR 0013 (auth per-project schema)](./0013-auth-per-project-schema.md),
  Phase 5.6 (row-level security).

## Context

Supabase-style "upload a file, get a URL, control access with policies"
storage does not exist in Basin. `basin-storage` is the
Parquet-on-object-store *engine* layer — not user-facing blob storage.
basin-js stubbed `storage.from(bucket).upload(...)` with
`not_implemented`.

Decision locked 2026-05-20: **catalog-backed metadata** — objects are
rows in a `storage.objects` system table; access control reuses the
existing RLS engine (Phase 5.6); bytes live in the same `object_store`
the engine already uses. This is Supabase Storage's model (a Postgres
table + S3) and means **zero new auth/policy machinery**.

## Decision

### New crate: `basin-blob`

`basin-storage` is taken (engine layer), so the user-facing blob crate
is `basin-blob`. Gated behind a `storage` Cargo feature
(`services/basin-server`, per ADR 0018).

### Data model (catalog-backed)

- **`storage.buckets`** — `(id, name, public bool, file_size_limit,
  allowed_mime_types[])`. Per-project.
- **`storage.objects`** — `(id, bucket_id, path, size, mime_type,
  metadata jsonb, owner uuid, created_at, updated_at, etag)`. One row
  per stored object. Per-project, project-scoped like every catalog
  object.
- **Bytes** live in `object_store` under
  `<project_prefix>/storage/<bucket>/<path>` — the same store the
  engine writes Parquet to. No second storage backend.

### Access control = RLS on `storage.objects`

No new policy engine. Access is governed by RLS policies (Phase 5.6)
on the `storage.objects` table, consulting `auth.uid()` / `auth.role()`
/ `auth.aal()` (the last from ADR 0020). Example:

```sql
CREATE POLICY "users read own avatars"
  ON storage.objects FOR SELECT
  USING (bucket_id = 'avatars' AND owner = auth.uid());
```

Public buckets short-circuit RLS for reads (the `public` flag); private
buckets always evaluate policies.

### HTTP API (in `basin-rest`)

PostgREST-adjacent, Supabase-shaped so basin-js maps cleanly:

- `POST   /storage/v1/object/:bucket/:path*` — upload (multipart or
  raw body). Body cap from `storage.buckets.file_size_limit`.
- `GET    /storage/v1/object/:bucket/:path*` — download. RLS-gated.
- `GET    /storage/v1/object/public/:bucket/:path*` — public-bucket
  fast path (no JWT).
- `DELETE /storage/v1/object/:bucket/:path*` — delete (RLS-gated).
- `POST   /storage/v1/object/list/:bucket` — list with prefix + paging.
- `POST   /storage/v1/object/sign/:bucket/:path*` — mint a signed URL
  (time-limited).
- Bucket CRUD under `/storage/v1/bucket`.

### Signed URLs

HMAC over `(project, bucket, path, expiry)` using the same crypto path
as basin-net / inbound webhooks (`hmac` + `sha2`, constant-time verify
via `subtle`). A signed URL grants time-boxed access without a JWT —
the signature is the capability. No DB round-trip on the hot read path
beyond signature verification + object_store fetch.

### Resumable uploads (TUS) — v1.1

Large-file resumable uploads via the TUS protocol are a fast-follow
(v1.1), not v1. v1 ships single-request upload with the body cap.

## Security model

| Threat | Mitigation |
|---|---|
| Unauthorized read/write | RLS policies on `storage.objects`; private buckets fail closed. |
| Signed-URL forgery | HMAC over `(project, bucket, path, expiry)`; constant-time verify; secret never leaves the server. |
| Signed-URL replay after expiry | Expiry baked into the signed payload; rejected past TTL. |
| Path traversal (`../`) | Path normalised + validated; rejected if it escapes the bucket prefix. |
| MIME spoofing / dangerous uploads | `allowed_mime_types[]` per bucket; content-type sniffed server-side, not trusted from the client. |
| Cross-project access | Object paths are project-prefixed in `object_store`; `storage.objects` rows are project-scoped; RLS enforces per-project. |
| Storage exhaustion (DoS) | `file_size_limit` per bucket; per-project quota enforced cloud-side (OSS exposes the counter). |
| Public-bucket info leak | `public` flag is explicit per bucket; defaults to private; listing a private bucket is RLS-gated. |

## OSS / cloud split

| Concern | OSS (`basin-blob`) | basin-cloud |
|---|---|---|
| Buckets + objects model + RLS | ✅ | — |
| Upload / download / list / delete HTTP routes | ✅ | — |
| Signed URLs (HMAC) | ✅ | — |
| object_store backend integration | ✅ | — |
| Per-project bytes-stored counter | ✅ (exposed) | — |
| Quota enforcement + billing on bytes + egress | — | ✅ |
| CDN in front of public reads | — | ✅ |
| Image transforms (resize / optimize) | — | ✅ (post-v1; could be a WASM UDF or external service) |
| Resumable uploads (TUS) | ✅ v1.1 | — |

## What this does NOT include (v1)

- Image transformations — deferred; cloud-side or a later WASM UDF.
- Resumable / multipart TUS uploads — v1.1 fast-follow.
- Cross-project object sharing — every object is project-scoped.
- Versioning / object history — deferred; overwrite replaces.

## References

- [ADR 0013 — Auth per-project schema](./0013-auth-per-project-schema.md)
  — where `auth.uid()` etc. (used by storage RLS) resolve.
- [ADR 0006 — REST API layer](./0006-rest-api-layer.md) — the HTTP
  surface the storage routes mount alongside.
- [ADR 0020 — Auth v2](./0020-auth-v2-oauth-mfa.md) — `auth.aal()`,
  usable in storage RLS for MFA-gated buckets.
- 2026-05-20 conversation log — the storage planning session; the
  catalog-backed-metadata decision.
