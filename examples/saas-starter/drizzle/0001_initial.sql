-- Basin SaaS Starter — initial schema migration
-- Run via: npm run db:migrate
-- Or manually: psql "$DATABASE_URL" -f drizzle/0001_initial.sql
--
-- Basin speaks pgwire v3; standard Postgres DDL works as-is.
-- Note: Basin defers a few DDL forms (no TRIGGER, no PL/pgSQL functions,
-- no CREATE EXTENSION) — see docs/sql-compatibility.md.

CREATE TABLE IF NOT EXISTS orgs (
  id          UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  name        TEXT        NOT NULL,
  slug        TEXT        NOT NULL UNIQUE,
  avatar_path TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- users.id = basin-auth JWT `sub` claim (UUID string).
-- Populated by the app on first sign-in (upsert pattern in src/api/auth.ts).
CREATE TABLE IF NOT EXISTS users (
  id           TEXT        NOT NULL PRIMARY KEY,
  email        TEXT        NOT NULL UNIQUE,
  display_name TEXT,
  avatar_path  TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS memberships (
  id         UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  org_id     UUID        NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
  user_id    TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role       TEXT        NOT NULL DEFAULT 'member',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS todos (
  id          UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  org_id      UUID        NOT NULL REFERENCES orgs(id) ON DELETE CASCADE,
  title       TEXT        NOT NULL,
  description TEXT,
  done        BOOLEAN     NOT NULL DEFAULT FALSE,
  created_by  TEXT        REFERENCES users(id),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
