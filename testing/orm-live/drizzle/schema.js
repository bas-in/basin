// Drizzle schema — plain ESM JS (consumed by both drizzle-kit and suite.mjs).
// Tables are d_-prefixed so they never collide with the other suites on a
// shared server.
import {
  pgTable, serial, text, integer, boolean, timestamp, jsonb,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";

export const users = pgTable("d_users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  name: text("name"),
  meta: jsonb("meta"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

export const posts = pgTable("d_posts", {
  id: serial("id").primaryKey(),
  title: text("title").notNull(),
  published: boolean("published").notNull().default(false),
  views: integer("views").notNull().default(0),
  authorId: integer("author_id").notNull().references(() => users.id),
});

export const usersRelations = relations(users, ({ many }) => ({
  posts: many(posts),
}));

export const postsRelations = relations(posts, ({ one }) => ({
  author: one(users, { fields: [posts.authorId], references: [users.id] }),
}));
