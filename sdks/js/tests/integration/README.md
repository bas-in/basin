# Integration tests

These tests exercise `@basin/basin-js` against a **live basin-engine** (not
a mock fetch). They are gated behind three environment variables and skip
silently when any are missing — that keeps PRs from forks green and stops
CI noise for contributors who don't have a test project.

## Required env vars

| Var | Example | Notes |
|---|---|---|
| `BASIN_TEST_PROJECT_REF` | `projref_01H...` | Identifies which basin-cloud project the engine belongs to. Used as the master gate (presence = "run these"). |
| `BASIN_TEST_ENGINE_URL` | `https://basin-engine-dev.fly.dev` | The engine's public REST URL. Local: `http://localhost:5434`. |
| `BASIN_TEST_ANON_KEY` | `eyJhbGciOi...` | Project anon key (signed by basin-cloud, trusted by the engine). Mint via `/app/project/<ref>/api-keys` in the dashboard. |

Optional, only consumed by tests that need them:

| Var | Used by |
|---|---|
| `BASIN_TEST_USER_EMAIL` + `BASIN_TEST_USER_PASSWORD` | `auth.test.ts` — pre-provisioned account for signIn tests (avoids littering the test project with signup users). |
| `BASIN_TEST_SERVICE_KEY` | Reserved for future tests that need RLS bypass. |

## Required project state

The integration tests assume the test project has the following objects
pre-provisioned (one-time setup). Idempotent SQL:

```sql
-- A user the auth tests can sign into. Replace the password with the
-- value you set in BASIN_TEST_USER_PASSWORD.
-- (Or run `basin auth users create` once the CLI lands.)

-- A throwaway KV table the postgrest tests read/write.
create table if not exists basin_js_integration (
  id text primary key,
  value text not null
);
```

## Running

```bash
export BASIN_TEST_PROJECT_REF=projref_...
export BASIN_TEST_ENGINE_URL=https://basin-engine-dev.fly.dev
export BASIN_TEST_ANON_KEY=eyJ...
export BASIN_TEST_USER_EMAIL=integration@example.test
export BASIN_TEST_USER_PASSWORD=...

npm test -- tests/integration
```

Without the env vars: `vitest` discovers the files, every `describe.skipIf`
fires, the report shows them as skipped. No network traffic.

## CI

`.github/workflows/test.yml` runs `npm test` on every push, which
*includes* `tests/integration/**` in discovery. The skip-gate keeps them
silent until the secrets are added:

1. `gh secret set BASIN_TEST_PROJECT_REF`
2. `gh secret set BASIN_TEST_ENGINE_URL`
3. `gh secret set BASIN_TEST_ANON_KEY`
4. (optional) `gh secret set BASIN_TEST_USER_EMAIL`, `gh secret set BASIN_TEST_USER_PASSWORD`
5. Edit `.github/workflows/test.yml` to thread them into the `env:` block of the test step.

Fork PRs see no secrets, so the gate stays closed for them — security boundary holds.
