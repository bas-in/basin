/**
 * Drizzle ORM schema for the SaaS Starter.
 *
 * Four tables:
 *   orgs        — organisations (tenants)
 *   users       — application users (distinct from auth.users)
 *   memberships — which users belong to which org (many-to-many)
 *   todos       — the per-org todos that demonstrate RLS isolation
 *
 * RLS policies (enforced by Basin, not by application code) are defined in
 * scripts/setup-rls.ts and applied via a setup SQL script.  The policies use
 * auth.uid() — the sub claim from the caller's JWT — to decide visibility.
 *
 * The org_id foreign key on `todos` is what RLS pivots on:
 *
 *   CREATE POLICY todos_org_select ON todos FOR SELECT
 *     USING (org_id IN (
 *       SELECT org_id FROM memberships WHERE user_id = auth.uid()
 *     ));
 *
 * With this policy active a user can only see todos belonging to orgs they
 * are a member of.  The check is enforced inside Basin's logical-plan layer —
 * there is no application-layer WHERE clause to forget.
 */

import {
  pgTable,
  text,
  timestamp,
  boolean,
  uuid,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";

// ---------------------------------------------------------------------------
// orgs
// ---------------------------------------------------------------------------

export const orgs = pgTable("orgs", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  slug: text("slug").notNull().unique(),
  avatarPath: text("avatar_path"), // storage path — e.g. "org-avatars/<org_id>/avatar.png"
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
});

export type Org = typeof orgs.$inferSelect;
export type NewOrg = typeof orgs.$inferInsert;

// ---------------------------------------------------------------------------
// users (application-level profile row, linked to basin-auth sub claim)
// ---------------------------------------------------------------------------

export const users = pgTable("users", {
  /**
   * id = the `sub` claim from the basin-auth JWT.
   * basin-auth generates UUIDs for sub; we store the same value here so
   * auth.uid() == users.id in RLS policies.
   */
  id: text("id").primaryKey(), // basin-auth sub (uuid string)
  email: text("email").notNull().unique(),
  displayName: text("display_name"),
  avatarPath: text("avatar_path"), // storage path for user avatar
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
});

export type User = typeof users.$inferSelect;
export type NewUser = typeof users.$inferInsert;

// ---------------------------------------------------------------------------
// memberships
// ---------------------------------------------------------------------------

export const memberships = pgTable("memberships", {
  id: uuid("id").defaultRandom().primaryKey(),
  orgId: uuid("org_id")
    .notNull()
    .references(() => orgs.id, { onDelete: "cascade" }),
  userId: text("user_id")
    .notNull()
    .references(() => users.id, { onDelete: "cascade" }),
  role: text("role").notNull().default("member"), // "owner" | "admin" | "member"
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
});

export type Membership = typeof memberships.$inferSelect;
export type NewMembership = typeof memberships.$inferInsert;

// ---------------------------------------------------------------------------
// todos
// ---------------------------------------------------------------------------

export const todos = pgTable("todos", {
  id: uuid("id").defaultRandom().primaryKey(),
  orgId: uuid("org_id")
    .notNull()
    .references(() => orgs.id, { onDelete: "cascade" }),
  title: text("title").notNull(),
  description: text("description"),
  done: boolean("done").notNull().default(false),
  createdBy: text("created_by").references(() => users.id),
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
});

export type Todo = typeof todos.$inferSelect;
export type NewTodo = typeof todos.$inferInsert;

// ---------------------------------------------------------------------------
// Relations (for Drizzle query builder — no effect on the DB schema)
// ---------------------------------------------------------------------------

export const orgsRelations = relations(orgs, ({ many }) => ({
  memberships: many(memberships),
  todos: many(todos),
}));

export const usersRelations = relations(users, ({ many }) => ({
  memberships: many(memberships),
}));

export const membershipsRelations = relations(memberships, ({ one }) => ({
  org: one(orgs, { fields: [memberships.orgId], references: [orgs.id] }),
  user: one(users, {
    fields: [memberships.userId],
    references: [users.id],
  }),
}));

export const todosRelations = relations(todos, ({ one }) => ({
  org: one(orgs, { fields: [todos.orgId], references: [orgs.id] }),
  createdByUser: one(users, {
    fields: [todos.createdBy],
    references: [users.id],
  }),
}));
