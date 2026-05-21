# Basin SaaS Starter

A small but complete multi-tenant SaaS reference app (todo + org management)
demonstrating the full Basin BaaS surface:

| Basin feature | Where it is used |
|---|---|
| **Postgres wire (pgwire)** | Drizzle ORM + postgres.js connect directly to Basin on port 5432 |
| **Drizzle ORM** | Schema (`src/lib/schema.ts`), migrations (`drizzle/`), type-safe queries |
| **basin-auth** | Email sign-up / sign-in / sign-out; JWT stored in localStorage |
| **Row-Level Security** | Per-org `todos` visibility; policies in `drizzle/0002_rls.sql` |
| **basin-rest** | Auto-generated REST surface; frontend queries via `@basin/basin-js` |
| **basin-blob (storage)** | Org avatar upload/download (`POST /storage/v1/object/avatars/…`) |

---

## What this demonstrates

**Multi-tenancy via RLS.**  Each organisation is a tenant.  The `todos`
table has a `CREATE POLICY` that restricts visibility to rows whose `org_id`
is in the user's `memberships` set:

```sql
CREATE POLICY todos_org_select ON todos
  FOR SELECT
  USING (
    org_id IN (
      SELECT org_id FROM memberships WHERE user_id = auth.uid()
    )
  );
```

`auth.uid()` resolves to the `sub` claim of the JWT supplied by
`@basin/basin-js` via the `Authorization: Bearer …` header.  The engine
enforces this at the logical-plan layer — there is no application-layer
`WHERE` clause to forget.

**The test in `tests/rls-isolation.test.ts` proves the isolation**:
Alice signs in → queries todos → Basin returns only Acme's todos;
Bob signs in → queries todos → Basin returns only Globex's todos.

---

## Prerequisites

| Tool | Purpose |
|---|---|
| Docker | Run Basin locally |
| Node.js 20+ | Run the app and scripts |
| `psql` | Optional — manual SQL inspection |

---

## Run Basin locally

The quickest path is the single-container dev image.

**Option A — pre-built image (after first Basin release):**

```sh
docker run --rm \
  -p 5432:5432 \
  -v basin-data:/var/basin \
  -e BASIN_AUTH_ENABLED=1 \
  -e BASIN_AUTH_JWT_SECRET=0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20 \
  --name basin \
  ghcr.io/bas-in/basin-server:latest
```

**Option B — build from source (today):**

```sh
# From the repo root:
docker build -t basin-server .

docker run --rm \
  -p 5432:5432 \
  -v basin-data:/var/basin \
  -e BASIN_AUTH_ENABLED=1 \
  -e BASIN_AUTH_JWT_SECRET=0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20 \
  --name basin \
  basin-server
```

> **Email verification:** `basin-auth` sends a verification link on sign-up.
> For local dev, add a Mailpit container:
> `docker run -d -p 1025:1025 -p 8025:8025 axllent/mailpit`
> then add to the `docker run` command:
> `-e BASIN_AUTH_SMTP_HOST=host.docker.internal -e BASIN_AUTH_SMTP_PORT=1025 -e BASIN_AUTH_SMTP_TLS=none`
> and visit `http://localhost:8025` to click the link.
>
> To skip email verification for quickstart, disable it:
> `-e BASIN_AUTH_EMAIL_VERIFY=0`

---

## Install and run

```sh
cd examples/saas-starter

# Install dependencies
npm install

# Copy and review env
cp .env.example .env.local

# Apply schema migrations (requires Basin running)
npm run db:migrate

# Apply RLS policies + create avatars storage bucket
npm run db:setup

# (Optional) seed demo data (two orgs, two users, sample todos)
npm run db:seed

# Start the Vite dev server
npm run dev
# Open http://localhost:5173
```

---

## npm scripts

| Script | What it does |
|---|---|
| `npm run dev` | Vite HMR dev server on `localhost:5173` |
| `npm run build` | TypeScript check + Vite production build |
| `npm test` | RLS isolation integration test (requires live Basin) |
| `npm run db:migrate` | Apply `drizzle/0001_initial.sql` via postgres.js |
| `npm run db:setup` | Apply `drizzle/0002_rls.sql` + create avatars bucket |
| `npm run db:seed` | Insert demo orgs/users/todos for manual testing |
| `npm run setup` | `db:migrate` + `db:setup` in sequence |

Set `BASIN_SKIP_LIVE_TESTS=1` to skip the integration test when no server
is available.

---

## File structure

```
examples/saas-starter/
├── drizzle/
│   ├── 0001_initial.sql    — table DDL (orgs, users, memberships, todos)
│   └── 0002_rls.sql        — RLS policies + storage.objects policies
├── scripts/
│   ├── migrate.ts          — apply 0001_initial.sql
│   ├── setup-rls.ts        — apply 0002_rls.sql + create avatars bucket
│   └── seed.ts             — demo data
├── src/
│   ├── lib/
│   │   ├── schema.ts       — Drizzle table definitions + TypeScript types
│   │   ├── basin.ts        — @basin/basin-js createClient singleton
│   │   └── basin-compat.ts — fetch-based shim (used in tests)
│   ├── api/
│   │   ├── auth.ts         — signUp / signIn / signOut wrappers
│   │   ├── todos.ts        — CRUD via basin-rest
│   │   └── storage.ts      — avatar upload/download via basin-blob
│   ├── pages/
│   │   ├── AuthPage.tsx    — sign-up / sign-in form
│   │   └── DashboardPage.tsx — org switcher + todo list + avatar upload
│   ├── App.tsx
│   ├── main.tsx
│   └── index.css
├── tests/
│   └── rls-isolation.test.ts — per-tenant isolation proof
├── drizzle.config.ts
├── vite.config.ts
├── tsconfig.json
└── package.json
```

---

## Client API surface used

All calls use the `@basin/basin-js` API shape from `docs/basin-js-design.md`.

**Auth** (basin-auth HTTP endpoints `POST /auth/v1/signup` + `/signin` + `/signout`):
```ts
const { data, error } = await basin.auth.signUp({ email, password })
const { data, error } = await basin.auth.signInWithPassword({ email, password })
await basin.auth.signOut()
const session = basin.auth.session()
```

**REST queries** (basin-rest `GET /rest/v1/<table>?...`):
```ts
const { data } = await basin.from('todos').select('id, title').eq('org_id', orgId)
await basin.from('todos').insert({ org_id, title, created_by })
await basin.from('todos').update({ done: true }).eq('id', id)
await basin.from('todos').delete().eq('id', id)
```

**Storage** (basin-blob `POST /storage/v1/object/avatars/<path>`):
```ts
// Called directly via fetch (SDK storage stub not yet published)
POST /storage/v1/object/avatars/<path>
POST /storage/v1/object/sign/avatars/<path>   // → signed URL
```

---

## Known limitations and pending items

| Feature | Status |
|---|---|
| `@basin/basin-js` npm package | **Not yet published** (design spec only, `docs/basin-js-design.md`). The app imports it; `src/lib/basin-compat.ts` is a fetch-based shim used in tests. Build will succeed once the package is published. |
| OAuth (GitHub / Google) | **Not yet available** in basin-auth v1. ADR 0020 plans it for v2. The UI button is rendered but disabled with a tooltip. |
| basin-blob storage | Ships in Phase 5.17 (accepted 2026-05-20). The avatar upload calls the HTTP API directly; it degrades gracefully with a warning if the endpoint is absent. |
| Email verification skip | `basin-auth` requires SMTP by default. Set `BASIN_AUTH_EMAIL_VERIFY=0` (or run Mailpit) for local dev. |
| `basin-js` type generation | `basin-cli generate-types` deferred to v0.2. Types are hand-written in `src/lib/schema.ts`. |
| Realtime subscriptions | Basin WebSocket CDC (docs/websocket-subscription-design.md) is Tier 2; the app does not use it yet. |

---

## How RLS is verified

`tests/rls-isolation.test.ts` does four checks:

1. Alice signs in → REST query returns only Acme todos (not Globex).
2. Bob signs in → REST query returns only Globex todos (not Acme).
3. Unauthenticated request → 401 or empty array (no data leakage).
4. Alice with explicit `org_id=globex` filter → still returns zero rows
   (RLS fires after the client filter, not instead of it).

These run against a live Basin instance.  Set `BASIN_SKIP_LIVE_TESTS=1` to
skip them in CI where no server is available.

---

## See also

- [`docs/tutorial.md`](../../docs/tutorial.md) — end-to-end Basin walkthrough
- [`docs/basin-js-design.md`](../../docs/basin-js-design.md) — `@basin/basin-js` API spec
- [`docs/decisions/0020-auth-v2-oauth-mfa.md`](../../docs/decisions/0020-auth-v2-oauth-mfa.md) — OAuth + MFA plan
- [`docs/decisions/0021-object-storage.md`](../../docs/decisions/0021-object-storage.md) — basin-blob design
- [`docs/multi-project.md`](../../docs/multi-project.md) — multi-tenant isolation model
