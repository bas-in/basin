// TypeORM entities via EntitySchema (the decorator-free API — keeps the suite
// plain ESM JS, no TS build step). Tables are t_-prefixed to avoid collisions
// with the other suites on a shared server.
import { EntitySchema } from "typeorm";

export const User = new EntitySchema({
  name: "User",
  tableName: "t_users",
  columns: {
    id: { type: Number, primary: true, generated: true },
    email: { type: String, unique: true },
    name: { type: String, nullable: true },
    createdAt: { name: "created_at", type: "timestamptz", createDate: true },
  },
  relations: {
    posts: { type: "one-to-many", target: "Post", inverseSide: "author" },
  },
});

export const Post = new EntitySchema({
  name: "Post",
  tableName: "t_posts",
  columns: {
    id: { type: Number, primary: true, generated: true },
    title: { type: String },
    views: { type: Number, default: 0 },
  },
  relations: {
    author: {
      type: "many-to-one",
      target: "User",
      inverseSide: "posts",
      joinColumn: { name: "author_id" },
      nullable: false,
      // Eager: every find() on Post pulls the author via TypeORM's own join
      // machinery — the "eager relations" coverage the harness wants.
      eager: true,
    },
  },
});
