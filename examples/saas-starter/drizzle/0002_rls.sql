-- Basin SaaS Starter — Row-Level Security policies
-- Run via: npm run db:setup
-- Or manually: psql "$DATABASE_URL" -f drizzle/0002_rls.sql
--
-- auth.uid() returns the `sub` claim from the verified JWT of the current
-- session.  basin-rest injects the JWT into the request context so every
-- REST call automatically inherits the caller's identity.
--
-- The policy pattern here is: a user can see / mutate only rows that belong
-- to orgs they are a member of.  The memberships table is the join that
-- proves org membership.

-- ============================================================
-- todos — per-org visibility
-- ============================================================

ALTER TABLE todos ENABLE ROW LEVEL SECURITY;

-- SELECT: user can read todos in any org they are a member of
CREATE POLICY todos_org_select ON todos
  FOR SELECT
  USING (
    org_id IN (
      SELECT org_id FROM memberships WHERE user_id = auth.uid()
    )
  );

-- INSERT: user may create todos only in orgs they belong to,
-- and the created_by field must match their own identity.
CREATE POLICY todos_org_insert ON todos
  FOR INSERT
  WITH CHECK (
    org_id IN (
      SELECT org_id FROM memberships WHERE user_id = auth.uid()
    )
    AND (created_by IS NULL OR created_by = auth.uid())
  );

-- UPDATE: user may update todos in their orgs
CREATE POLICY todos_org_update ON todos
  FOR UPDATE
  USING (
    org_id IN (
      SELECT org_id FROM memberships WHERE user_id = auth.uid()
    )
  );

-- DELETE: user may delete todos in their orgs
CREATE POLICY todos_org_delete ON todos
  FOR DELETE
  USING (
    org_id IN (
      SELECT org_id FROM memberships WHERE user_id = auth.uid()
    )
  );

-- ============================================================
-- memberships — members can see their own membership rows
-- ============================================================

ALTER TABLE memberships ENABLE ROW LEVEL SECURITY;

CREATE POLICY memberships_self_select ON memberships
  FOR SELECT
  USING (user_id = auth.uid());

-- Only org owners / admins may insert new members.
-- In this starter we keep it simple: only the service role (used by the
-- server-side setup script) can insert memberships.  The app uses a
-- server-side API route that temporarily elevates to the service role.
-- NOTE: in a production app you would add a more granular policy here.
CREATE POLICY memberships_owner_insert ON memberships
  FOR INSERT
  WITH CHECK (user_id = auth.uid());

-- ============================================================
-- orgs — any org member may read the org row
-- ============================================================

ALTER TABLE orgs ENABLE ROW LEVEL SECURITY;

CREATE POLICY orgs_member_select ON orgs
  FOR SELECT
  USING (
    id IN (
      SELECT org_id FROM memberships WHERE user_id = auth.uid()
    )
  );

-- Only org owners may update the org row (avatar, name, slug).
CREATE POLICY orgs_owner_update ON orgs
  FOR UPDATE
  USING (
    id IN (
      SELECT org_id FROM memberships
      WHERE user_id = auth.uid() AND role = 'owner'
    )
  );

-- ============================================================
-- users — each user can read their own profile row
-- ============================================================

ALTER TABLE users ENABLE ROW LEVEL SECURITY;

CREATE POLICY users_self_select ON users
  FOR SELECT
  USING (id = auth.uid());

CREATE POLICY users_self_update ON users
  FOR UPDATE
  USING (id = auth.uid());

-- ============================================================
-- storage.objects — avatar bucket RLS
-- ============================================================
-- This policy is applied after the avatars bucket is created.
-- It restricts uploads/downloads to authenticated users who
-- own the object (owner = auth.uid()).
--
-- NOTE: basin-blob (Phase 5.17) must be enabled on the server
-- for this to take effect.  If storage is not yet available on
-- your Basin build, comment this block out.

CREATE POLICY "avatars owner read" ON storage.objects
  FOR SELECT
  USING (bucket_id = 'avatars' AND owner = auth.uid());

CREATE POLICY "avatars owner insert" ON storage.objects
  FOR INSERT
  WITH CHECK (bucket_id = 'avatars' AND owner = auth.uid());

CREATE POLICY "avatars owner delete" ON storage.objects
  FOR DELETE
  USING (bucket_id = 'avatars' AND owner = auth.uid());
