---
title: "Getting started with Basin — 15-minute walkthrough"
nav_section: overview
sidebar_position: 1
summary: "Spin up Basin with Docker, create tables, add RLS policies, call the auth and REST APIs, and wire a React/Vite frontend. Every step is copy-pasteable and verified against the real system."
tags: [getting-started, tutorial, auth, rls, rest, react]
---

# Getting started with Basin

This tutorial takes you from a fresh machine to a working Basin project in
about 15 minutes. You will:

1. Start Basin with a single `docker run`.
2. Connect with `psql` and verify the round-trip.
3. Create a schema with two tables and an RLS policy.
4. Sign up a user and sign in to get a JWT (basin-auth).
5. Run CRUD over psql and the REST API (basin-rest).
6. Wire a tiny React/Vite component to query Basin from the browser.
7. Learn the first-deployment path to a managed Basin.

Follow the steps in order — later sections use what earlier ones set up.

---

## Prerequisites

| Tool | Notes |
|---|---|
| Docker (any recent version) | No Rust toolchain required for the Docker path |
| `psql` | Ships with most OS Postgres packages; any version works |
| `curl` | For auth and REST calls |
| Node 20+ | For the React snippet in step 6 |

---

## 1. Start Basin

The GHCR image will be published on the first tagged release. Until then,
build the image from the repo root (takes about five minutes on the first
run; subsequent builds are fast because Docker caches the Cargo layer):

```sh
git clone https://github.com/vul-os/basin.git
cd basin

# Build the image.
docker build -t basin-server .

# Run it.
docker run --rm \
  -p 5432:5432 \
  -v basin-data:/var/basin \
  --name basin \
  basin-server
```

Once the published image ships, replace `basin-server` with
`ghcr.io/vul-os/basin-server:latest` — the flags stay the same.

Basin is ready when you see:

```
INFO basin_server: pgwire listener is accept-ready bind=0.0.0.0:5432
```

Key environment variables (all have defaults; override with `-e`):

| Variable | Default | What it controls |
|---|---|---|
| `BASIN_BIND` | `0.0.0.0:5432` | pgwire listen address inside the container |
| `BASIN_DATA_DIR` | `/var/basin` | Data root — mount a volume here for persistence |
| `BASIN_STORAGE_BACKEND` | `local` | `local` (filesystem), `s3`, or `tigris` |
| `BASIN_PROJECTS` | `basin=*` | Comma-separated `user=project_id` pairs; `*` auto-generates a ULID |

The `-v basin-data:/var/basin` flag persists data across restarts.
Omit it and all data is lost when the container exits.

**Port conflict?** If 5432 is already in use, map to a different host port:

```sh
docker run --rm -p 5433:5432 -v basin-data:/var/basin --name basin basin-server
# Then connect on 5433 everywhere below instead of 5432.
```

---

## 2. Connect with psql

Open a new terminal (leave the container running):

```sh
psql -h 127.0.0.1 -p 5432 -U basin
```

| Parameter | Value | Source |
|---|---|---|
| Host | `127.0.0.1` | localhost via the `-p 5432:5432` mapping |
| Port | `5432` | `BASIN_BIND=0.0.0.0:5432` inside the container |
| User | `basin` | `BASIN_PROJECTS=basin=*` auto-provisions this user |
| Password | _(none)_ | No auth required in the default dev configuration |

Run a quick sanity check — this is the same round-trip the smoke harness
(`tests/integration/scripts/docker-smoke.sh`) executes:

```sql
CREATE TABLE smoke (id int, name text);
INSERT INTO smoke VALUES (1, 'hello basin');
SELECT id, name FROM smoke WHERE id = 1;
```

Expected output:

```
 id |    name
----+-------------
  1 | hello basin
(1 row)
```

Data lands in Vortex-compressed columnar files under the volume:

```sh
docker exec basin find /var/basin -name '*.vortex'
```

Drop the smoke table when you are done:

```sql
DROP TABLE smoke;
```

---

## 3. Create a schema and an RLS policy

Still in the same psql session. Create two tables — a `users` profile table
and a `notes` table — then lock `notes` behind a row-level security policy
so each user can only see their own rows.

```sql
-- Users table. The id column matches the JWT `sub` claim issued by
-- basin-auth (a UUID string). Populated by the app on first sign-in.
CREATE TABLE users (
  id           TEXT        NOT NULL PRIMARY KEY,
  email        TEXT        NOT NULL UNIQUE,
  display_name TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Notes table. Each note belongs to one user.
CREATE TABLE notes (
  id         UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  user_id    TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title      TEXT        NOT NULL,
  body       TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Enable RLS on `notes` and add two policies:

```sql
-- Enable row-level security. After this, SELECT returns zero rows for
-- any role that has no matching policy — not an error, just an empty set.
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;

-- auth.uid() returns the `sub` claim from the JWT of the current request.
-- basin-rest injects this automatically from the Authorization header.

-- SELECT: users can only read their own notes.
CREATE POLICY notes_owner_select ON notes
  FOR SELECT
  USING (user_id = auth.uid());

-- INSERT: users can only create notes for themselves.
CREATE POLICY notes_owner_insert ON notes
  FOR INSERT
  WITH CHECK (user_id = auth.uid());

-- UPDATE: users can only edit their own notes.
CREATE POLICY notes_owner_update ON notes
  FOR UPDATE
  USING (user_id = auth.uid());

-- DELETE: users can only delete their own notes.
CREATE POLICY notes_owner_delete ON notes
  FOR DELETE
  USING (user_id = auth.uid());
```

Insert a couple of rows to have data ready for the REST step:

Seed some rows. Do this **before** enabling RLS — see the note below.

```sql
INSERT INTO users (id, email, display_name) VALUES
  ('user_alice', 'alice@example.com', 'Alice'),
  ('user_bob',   'bob@example.com',   'Bob');

INSERT INTO notes (user_id, title, body) VALUES
  ('user_alice', 'First note',  'Hello from Alice'),
  ('user_alice', 'Second note', 'Still Alice'),
  ('user_bob',   'Bob note',    'Only Bob sees this');
```

```sql
SELECT id, user_id, title FROM notes ORDER BY created_at;
```

> **There is no privileged bypass. Read this before you enable RLS.**
>
> A plain `psql` session carries **no JWT**, so `auth.uid()` is `NULL`, so
> `USING (user_id = auth.uid())` matches nothing and the session sees **zero
> rows** — not all of them. Enabling RLS locks *you* out too, which is the
> fail-closed behaviour you want from a security feature but is surprising the
> first time.
>
> So: seed and inspect data first, enable RLS second. To read the table again
> afterwards you need a session whose `auth.uid()` matches — see below — or you
> can `ALTER TABLE notes DISABLE ROW LEVEL SECURITY` while iterating.
>
> This is asserted by `rls_with_auth_uid_filters_per_user` in
> `tests/integration/tests/auth_rls_uid.rs`, and by the tutorial's own CI
> harness (`tests/integration/scripts/tutorial-smoke.sh`).

---

## 4. Auth: sign up and sign in

`basin-auth` and `basin-rest` ship in the OSS bundle but are **off by
default** — `BASIN_AUTH_ENABLED` and `BASIN_REST_ENABLED` both default to
`0`, and `BASIN_REST_ENABLED=1` requires `BASIN_AUTH_ENABLED=1` (ADR 0006:
a REST stack without auth is the largest data-leak class we know how to
create). The quickstart container in section 1 does **not** set either, so
restart it with them on before working through this section:

```sh
docker run --rm \
  -p 5432:5432 \
  -p 5434:5434 \
  -e BASIN_AUTH_ENABLED=1 \
  -e BASIN_REST_ENABLED=1 \
  -e BASIN_REST_BIND=0.0.0.0:5434 \
  -v basin-data:/var/basin \
  --name basin \
  basin-server
```

(`BASIN_REST_BIND=0.0.0.0:5434` is what makes the listener reachable from
outside the container; the default `127.0.0.1:5434` is container-local.)

REST and auth share one HTTP listener on its **own** port — `BASIN_REST_BIND`,
default `127.0.0.1:5434` — not the pgwire port. The path prefixes are
`/auth/v1/` and `/rest/v1/`.

> **SMTP note:** basin-auth requires SMTP configuration to send email
> verification and password-reset links. In the default Docker dev setup,
> email flows are disabled (`BASIN_AUTH_SMTP_TLS=none`) so sign-up succeeds
> immediately without an inbox check. For production, pass the full SMTP env
> block described in [ADR 0005](./decisions/0005-auth-system.md).

### Sign up

```sh
curl -s -X POST http://127.0.0.1:5434/auth/v1/signup \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"hunter2hunter2"}' \
  | jq .
```

Response:

```json
{
  "access_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 3600,
  "refresh_token": "rt_...",
  "user": {
    "id": "01JW...",
    "email": "alice@example.com"
  }
}
```

The `access_token` is a short-lived JWT (default TTL: 1 hour). The
`refresh_token` is an opaque token (default TTL: 30 days) used to rotate
the access token via `POST /auth/v1/refresh`.

### Sign in

```sh
JWT=$(curl -s -X POST http://127.0.0.1:5434/auth/v1/signin \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"hunter2hunter2"}' \
  | jq -r .access_token)

echo "JWT: ${JWT:0:40}..."
```

Keep `$JWT` in your shell. Every REST and direct-fetch call below passes it
in the `Authorization: Bearer $JWT` header. basin-rest verifies the token
and extracts `auth.uid()` from the `sub` claim, which the RLS policies use.

---

## 5. CRUD over psql and the REST API

### psql path — pass the JWT as the username

Basin has **no `SET request.jwt.claims`**. PostgREST and Supabase expose that
GUC as a debugging escape hatch; Basin does not implement it, and setting it
does nothing. Earlier revisions of this tutorial documented it. They were wrong.

The pgwire session's identity comes from the **JWT in the username field**, which
`basin-router` verifies at connect time
(`auth_context_from_username` in `crates/basin-router/src/protocol.rs`). So to
query as Alice, connect as Alice's token — using `$JWT` from step 4:

```sh
psql -h 127.0.0.1 -p 5432 -U "$JWT"
```

```sql
-- auth.uid() now resolves to Alice's `sub`, so the policy admits her rows only.
SELECT id, title FROM notes;
```

Expected output (only Alice's two rows):

```
 id                                   | title
--------------------------------------+-------------
 <uuid>                               | First note
 <uuid>                               | Second note
(2 rows)
```

Reconnect with Bob's token and the same query returns Bob's single row instead.

> **This needs basin-auth running** (`BASIN_AUTH_ENABLED=1` plus a JWT secret).
> With no auth service configured — which is the default of the quickstart
> container in step 1 — every session is anonymous, `auth.uid()` is `NULL`, and
> an RLS-enabled table returns zero rows to everyone. There is no way to read an
> RLS-protected table from a plain psql session on an auth-less server, by
> design.
>
> `auth.uid()` and `auth_uid()` are the same function; both forms work
> (`auth_uid_schema_form_equals_flat_form` in
> `tests/integration/tests/auth_rls_uid.rs`).

### REST API path (basin-rest)

`basin-rest` exposes every table at `/rest/v1/<table>` using
PostgREST-compatible URL conventions. The `$JWT` from Step 4 is the
authentication credential.

**Read notes (RLS-filtered to Alice's rows):**

```sh
curl -s "http://127.0.0.1:5434/rest/v1/notes" \
  -H "Authorization: Bearer $JWT" \
  | jq .
```

Because the RLS policy is active, only Alice's notes come back — there is
no `WHERE user_id = ?` in the application code.

**Filter and project:**

```sh
# Only the title column, ordered newest first.
curl -s "http://127.0.0.1:5434/rest/v1/notes?select=id,title&order=created_at.desc" \
  -H "Authorization: Bearer $JWT" \
  | jq .
```

**Insert a new note:**

```sh
curl -s -X POST "http://127.0.0.1:5434/rest/v1/notes" \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -H "Prefer: return=representation" \
  -d '{"user_id":"user_alice","title":"From REST","body":"Inserted via HTTP"}' \
  | jq .
```

The `Prefer: return=representation` header returns the inserted row,
including the server-generated `id` and `created_at`.

**Update a note** (replace `<note-id>` with an `id` from above):

```sh
curl -s -X PATCH "http://127.0.0.1:5434/rest/v1/notes?id=eq.<note-id>" \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -H "Prefer: return=representation" \
  -d '{"title":"Updated title"}' \
  | jq .
```

**Delete a note:**

```sh
curl -s -X DELETE "http://127.0.0.1:5434/rest/v1/notes?id=eq.<note-id>" \
  -H "Authorization: Bearer $JWT" \
  | jq .
```

### Supported query parameters (REST v1)

| Parameter | Example | Effect |
|---|---|---|
| `select` | `select=id,title` | Column projection |
| `<col>=eq.<val>` | `user_id=eq.user_alice` | Equality filter |
| `<col>=gt.<val>` | `created_at=gt.2026-01-01` | Greater-than |
| `<col>=lt.<val>` | `created_at=lt.2027-01-01` | Less-than |
| `<col>=in.(a,b)` | `user_id=in.(user_alice,user_bob)` | Membership |
| `<col>=is.null` | `body=is.null` | Null check |
| `order` | `order=created_at.desc` | Ordering |
| `limit` | `limit=10` | Result cap |
| `offset` | `offset=20` | Pagination offset |

---

## 6. A React/Vite frontend

The `examples/saas-starter/` directory in the repo is a working Vite + React
application that demonstrates CRUD, auth, and RLS end-to-end.

Below is the minimal pattern from that app — a note-list component that
authenticates, fetches notes (RLS-filtered server-side), and inserts new
ones.

> **npm package status:** `@basin/basin-js` is not yet published to npm
> as of May 2026. The saas-starter uses a fetch-based shim
> (`src/lib/basin-js-stub.ts`) that implements the same interface. The
> snippet below follows the same pattern — swap in the real SDK once it
> publishes, with no API changes.

### Install

```sh
npm create vite@latest my-basin-app -- --template react-ts
cd my-basin-app
npm install
```

Copy `examples/saas-starter/src/lib/basin-js-stub.ts` into your project as
`src/lib/basin.ts` and adjust the import path. Or point directly at the
real SDK once it ships:

```sh
# Once published:
npm install @basin/basin-js
```

### Configure

Create `.env.local`:

```sh
VITE_BASIN_URL=http://localhost:5432
VITE_BASIN_ANON_KEY=     # leave empty in dev (no RLS on anon key needed)
```

### basin client singleton (`src/lib/basin.ts`)

```ts
// Until @basin/basin-js is published, alias basin-js-stub.ts here.
// The API surface is identical — swap the import once the package ships.
import { createClient } from './basin-js-stub'

const BASIN_URL  = import.meta.env.VITE_BASIN_URL  ?? 'http://localhost:5432'
const BASIN_ANON = import.meta.env.VITE_BASIN_ANON_KEY ?? ''

export const basin = createClient(BASIN_URL, BASIN_ANON)
```

### Auth (sign in, sign up)

```ts
// Sign up — POST /auth/v1/signup
const { data, error } = await basin.auth.signUp({ email, password })
// data.user.id  = JWT `sub` claim (matches users.id in your schema)
// data.session.accessToken = JWT stored in localStorage automatically

// Sign in — POST /auth/v1/signin
const { data, error } = await basin.auth.signInWithPassword({ email, password })

// Sign out — POST /auth/v1/signout
await basin.auth.signOut()
```

The JWT is stored in `localStorage` under the key `basin-session` and
attached automatically to every subsequent REST call.

### Notes component — fetch + insert + delete

```tsx
import { useEffect, useState } from 'react'
import { basin } from './lib/basin'

interface Note {
  id: string
  user_id: string
  title: string
  body: string | null
  created_at: string
}

export function NoteList({ userId }: { userId: string }) {
  const [notes, setNotes]     = useState<Note[]>([])
  const [newTitle, setNewTitle] = useState('')

  // Fetch — RLS enforced server-side; no WHERE clause needed here.
  useEffect(() => {
    basin
      .from<Note>('notes')
      .select('id, user_id, title, body, created_at')
      .then(({ data, error }) => {
        if (!error && data) setNotes(data)
      })
  }, [])

  // Insert
  async function addNote() {
    if (!newTitle.trim()) return
    const { data, error } = await basin
      .from<Note>('notes')
      .insert({ user_id: userId, title: newTitle.trim() })
    if (!error && data?.[0]) {
      setNotes(n => [...n, data[0]])
      setNewTitle('')
    }
  }

  // Delete
  async function deleteNote(id: string) {
    const { error } = await basin.from<Note>('notes').delete().eq('id', id)
    if (!error) setNotes(n => n.filter(note => note.id !== id))
  }

  return (
    <div>
      <ul>
        {notes.map(note => (
          <li key={note.id}>
            <strong>{note.title}</strong>
            <button onClick={() => deleteNote(note.id)}>Delete</button>
          </li>
        ))}
      </ul>
      <input
        value={newTitle}
        onChange={e => setNewTitle(e.target.value)}
        placeholder="New note title"
      />
      <button onClick={addNote}>Add</button>
    </div>
  )
}
```

Under the hood each call translates to:

| Operation | HTTP call |
|---|---|
| `.from('notes').select(...)` | `GET /rest/v1/notes?select=id,user_id,...` |
| `.insert({...})` | `POST /rest/v1/notes` with `Prefer: return=representation` |
| `.delete().eq('id', id)` | `DELETE /rest/v1/notes?id=eq.<id>` |

The `Authorization: Bearer <jwt>` header is attached automatically from
the session stored in `localStorage`. The RLS policies fire on the server;
the component never writes a `WHERE user_id = ?` clause.

For a full working example with org switcher, avatar upload, and Drizzle
migrations, see [`examples/saas-starter/`](../examples/saas-starter/).

---

## 7. First deployment to a managed Basin

> **Forward-spec note:** the managed service and basin-cli are not yet
> publicly available. This section describes the intended path; the
> self-hosted Docker setup above is the working path today.

When the managed service launches, the deployment flow will be:

```sh
# Install basin-cli (Rust binary, Sigstore-signed release artefacts).
# Built from this repo's cli/ directory; releases publish pre-built binaries.
basin login                            # OAuth/JWT flow, stores credential in OS keychain
basin projects create my-app           # provisions a Basin engine on Fly Machines
basin projects connect my-app          # prints the postgres:// URL for psql / .env
```

Point your app at the cloud engine URL — everything else (psql, REST,
basin-js) works identically because the wire protocol is the same.

For production self-hosting today (before the managed service launches), see
[`docs/deployment.md`](./deployment.md) for the full env-var reference,
S3/Tigris object storage configuration, and durable catalog setup with
`BASIN_CATALOG=postgres://...`.

---

## Stop and clean up

```sh
# Stop the container (the named volume basin-data is preserved).
docker stop basin

# Remove the volume too — all data is discarded.
docker volume rm basin-data
```

---

## Next steps

| Document | What it covers |
|---|---|
| [5-Minute Docker Quickstart](./quickstart-docker.md) | Single-command start, env-var reference, troubleshooting |
| [SQL compatibility](./sql-compatibility.md) | Which Postgres SQL Basin accepts and which it defers |
| [Multi-project SaaS](./multi-project.md) | Per-project isolation, RLS with `auth.uid()`, cost math at 10k projects |
| [HTAP guide](./htap-guide.md) | Hot-tier vs cold-tier performance, `basin.sort_by`, memtable caps |
| [Deployment](./deployment.md) | Production storage backends, topology, configuration |
| [CAPABILITIES.md](../CAPABILITIES.md) | Full capability matrix: shipped, in-progress, planned, off-roadmap |
| [`examples/saas-starter/`](../examples/saas-starter/) | Full React/Vite app with auth, RLS, Drizzle migrations, and avatar upload |
