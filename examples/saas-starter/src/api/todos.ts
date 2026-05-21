/**
 * Todo CRUD via basin-rest (the auto-generated REST surface).
 *
 * basin-rest exposes every table at /rest/v1/<table> using PostgREST-shaped
 * URL params (docs/tutorial.md § 5).  RLS policies are enforced server-side
 * using the JWT from the active session — the client does NOT add WHERE clauses.
 *
 * Filter params (per basin-js-design.md § 2 "Query"):
 *   .eq(col, val)  → col=eq.val
 *   .neq / .gt / .gte / .lt / .lte / .in / .is
 *
 * The RLS policy installed by drizzle/0002_rls.sql ensures a user only sees
 * todos belonging to orgs they are a member of.
 */

import { basin } from "../lib/basin";
import type { Todo, NewTodo } from "../lib/schema";

// ---------------------------------------------------------------------------
// Fetch todos for an org
// ---------------------------------------------------------------------------

export async function getTodos(
  orgId: string
): Promise<{ todos: Todo[]; error: string | null }> {
  const { data, error } = await basin
    .from<Todo>("todos")
    .select("id, org_id, title, description, done, created_by, created_at, updated_at")
    .eq("org_id", orgId);

  if (error) return { todos: [], error: error.message };
  return { todos: data ?? [], error: null };
}

// ---------------------------------------------------------------------------
// Create a todo
// ---------------------------------------------------------------------------

export async function createTodo(
  todo: Pick<NewTodo, "orgId" | "title" | "description" | "createdBy">
): Promise<{ todo: Todo | null; error: string | null }> {
  const { data, error } = await basin.from<Todo>("todos").insert({
    org_id: todo.orgId,
    title: todo.title,
    description: todo.description ?? null,
    created_by: todo.createdBy ?? null,
  });

  if (error) return { todo: null, error: error.message };
  return { todo: data?.[0] ?? null, error: null };
}

// ---------------------------------------------------------------------------
// Toggle a todo's done state
// ---------------------------------------------------------------------------

export async function toggleTodo(
  id: string,
  done: boolean
): Promise<{ error: string | null }> {
  const { error } = await basin
    .from<Todo>("todos")
    .update({ done })
    .eq("id", id);

  return { error: error?.message ?? null };
}

// ---------------------------------------------------------------------------
// Delete a todo
// ---------------------------------------------------------------------------

export async function deleteTodo(id: string): Promise<{ error: string | null }> {
  const { error } = await basin.from<Todo>("todos").delete().eq("id", id);
  return { error: error?.message ?? null };
}
