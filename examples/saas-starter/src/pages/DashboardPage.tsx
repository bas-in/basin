/**
 * Dashboard — org switcher + per-org todo CRUD + avatar upload.
 *
 * RLS is enforced entirely by Basin server-side.  The UI simply queries
 * basin-rest; the engine automatically filters to rows the caller's JWT
 * is allowed to see.  There is no WHERE clause in application code.
 */
import React, { useState, useEffect, useRef } from "react";
import { basin } from "../lib/basin";
import { getTodos, createTodo, toggleTodo, deleteTodo } from "../api/todos";
import { uploadAvatar, getAvatarUrl } from "../api/storage";
import type { Todo, Org } from "../lib/schema";

interface Props {
  user: { id: string; email: string };
  onSignOut: () => void;
}

export default function DashboardPage({ user, onSignOut }: Props) {
  const [orgs, setOrgs] = useState<Org[]>([]);
  const [selectedOrgId, setSelectedOrgId] = useState<string | null>(null);
  const [todos, setTodos] = useState<Todo[]>([]);
  const [newTitle, setNewTitle] = useState("");
  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);
  const [loadingOrgs, setLoadingOrgs] = useState(true);
  const [loadingTodos, setLoadingTodos] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [storageNote, setStorageNote] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Load orgs this user is a member of.
  // RLS on `orgs` ensures we only see orgs where user_id = auth.uid().
  useEffect(() => {
    async function loadOrgs() {
      setLoadingOrgs(true);
      const { data, error } = await basin
        .from<Org>("orgs")
        .select("id, name, slug, avatar_path, created_at, updated_at");
      if (!error && data) {
        setOrgs(data);
        if (data.length > 0) setSelectedOrgId(data[0].id);
      }
      setLoadingOrgs(false);
    }
    loadOrgs();
  }, []);

  // Load todos whenever the selected org changes.
  // Again RLS enforces the org boundary — no client-side filter needed.
  useEffect(() => {
    if (!selectedOrgId) return;
    setLoadingTodos(true);
    getTodos(selectedOrgId).then(({ todos, error }) => {
      if (!error) setTodos(todos);
      setLoadingTodos(false);
    });
  }, [selectedOrgId]);

  // Load avatar for selected org
  useEffect(() => {
    if (!selectedOrgId) return;
    const org = orgs.find((o) => o.id === selectedOrgId);
    if (org?.avatarPath) {
      getAvatarUrl(org.avatarPath).then(setAvatarUrl);
    } else {
      setAvatarUrl(null);
    }
  }, [selectedOrgId, orgs]);

  async function handleAddTodo(e: React.FormEvent) {
    e.preventDefault();
    if (!selectedOrgId || !newTitle.trim()) return;
    setError(null);
    const { todo, error } = await createTodo({
      orgId: selectedOrgId,
      title: newTitle.trim(),
      createdBy: user.id,
    });
    if (error) {
      setError(error);
    } else if (todo) {
      setTodos((t) => [...t, todo]);
      setNewTitle("");
    }
  }

  async function handleToggle(id: string, done: boolean) {
    const { error } = await toggleTodo(id, !done);
    if (!error) {
      setTodos((t) => t.map((todo) => (todo.id === id ? { ...todo, done: !done } : todo)));
    }
  }

  async function handleDelete(id: string) {
    const { error } = await deleteTodo(id);
    if (!error) {
      setTodos((t) => t.filter((todo) => todo.id !== id));
    }
  }

  async function handleAvatarUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file || !selectedOrgId) return;
    setStorageNote(null);
    const path = `org-avatars/${selectedOrgId}/avatar.${file.name.split(".").pop()}`;
    const { storagePath, error } = await uploadAvatar(path, file);
    if (error) {
      setStorageNote(`Avatar upload: ${error}`);
    } else if (storagePath) {
      // Update the org row's avatar_path via basin-rest
      await basin.from("orgs").update({ avatar_path: storagePath }).eq("id", selectedOrgId);
      setOrgs((os) =>
        os.map((o) => (o.id === selectedOrgId ? { ...o, avatarPath: storagePath } : o))
      );
      const url = await getAvatarUrl(storagePath);
      setAvatarUrl(url);
    }
  }

  const selectedOrg = orgs.find((o) => o.id === selectedOrgId);

  return (
    <div className="container" style={{ paddingTop: "2rem", paddingBottom: "3rem" }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "2rem" }}>
        <div>
          <h1 style={{ fontSize: "1.3rem" }}>Basin SaaS Starter</h1>
          <p className="muted" style={{ fontSize: "0.8rem" }}>
            Signed in as <strong>{user.email}</strong>
          </p>
        </div>
        <button className="btn" onClick={onSignOut}>Sign out</button>
      </div>

      {loadingOrgs ? (
        <p className="muted">Loading organisations…</p>
      ) : orgs.length === 0 ? (
        <div className="card">
          <p className="muted">
            You are not a member of any organisation yet.{" "}
            Ask an admin to add you, or run <code>npm run db:seed</code> to
            create demo data.
          </p>
        </div>
      ) : (
        <div style={{ display: "grid", gridTemplateColumns: "220px 1fr", gap: "1.5rem" }}>
          {/* Org sidebar */}
          <div>
            <h2 style={{ fontSize: "0.8rem", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "0.75rem" }} className="muted">
              Organisations
            </h2>
            {orgs.map((org) => (
              <button
                key={org.id}
                onClick={() => setSelectedOrgId(org.id)}
                className="btn"
                style={{
                  width: "100%",
                  justifyContent: "flex-start",
                  marginBottom: "0.4rem",
                  background: org.id === selectedOrgId ? "var(--border)" : undefined,
                  borderColor: org.id === selectedOrgId ? "var(--primary)" : undefined,
                }}
              >
                {org.name}
              </button>
            ))}
          </div>

          {/* Main content */}
          <div>
            {selectedOrg && (
              <>
                {/* Org header */}
                <div className="card" style={{ marginBottom: "1rem", display: "flex", alignItems: "center", gap: "1rem" }}>
                  {/* Avatar */}
                  <div
                    style={{
                      width: 56,
                      height: 56,
                      borderRadius: "50%",
                      background: "var(--border)",
                      overflow: "hidden",
                      flexShrink: 0,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      cursor: "pointer",
                    }}
                    onClick={() => fileInputRef.current?.click()}
                    title="Click to upload avatar"
                  >
                    {avatarUrl ? (
                      <img src={avatarUrl} alt="org avatar" style={{ width: "100%", height: "100%", objectFit: "cover" }} />
                    ) : (
                      <span style={{ fontSize: "1.4rem" }}>
                        {selectedOrg.name[0]?.toUpperCase()}
                      </span>
                    )}
                  </div>
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept="image/*"
                    style={{ display: "none" }}
                    onChange={handleAvatarUpload}
                  />

                  <div>
                    <h2 style={{ fontSize: "1rem" }}>{selectedOrg.name}</h2>
                    <p className="muted" style={{ fontSize: "0.8rem" }}>
                      {selectedOrg.slug}
                    </p>
                    {storageNote && (
                      <p style={{ fontSize: "0.75rem", color: "var(--danger)", marginTop: "0.25rem" }}>
                        {storageNote}
                      </p>
                    )}
                  </div>
                </div>

                {/* Add todo */}
                <form onSubmit={handleAddTodo} style={{ display: "flex", gap: "0.5rem", marginBottom: "1rem" }}>
                  <input
                    value={newTitle}
                    onChange={(e) => setNewTitle(e.target.value)}
                    placeholder="New todo…"
                    style={{ flex: 1 }}
                  />
                  <button type="submit" className="btn primary" disabled={!newTitle.trim()}>
                    Add
                  </button>
                </form>

                {error && <p className="error" style={{ marginBottom: "0.75rem" }}>{error}</p>}

                {/* Todo list */}
                {loadingTodos ? (
                  <p className="muted">Loading todos…</p>
                ) : todos.length === 0 ? (
                  <div className="card">
                    <p className="muted">No todos yet. Add one above.</p>
                  </div>
                ) : (
                  <div>
                    {todos.map((todo) => (
                      <div
                        key={todo.id}
                        className="card"
                        style={{
                          marginBottom: "0.5rem",
                          display: "flex",
                          alignItems: "center",
                          gap: "0.75rem",
                          padding: "0.75rem 1rem",
                        }}
                      >
                        <input
                          type="checkbox"
                          checked={todo.done}
                          onChange={() => handleToggle(todo.id, todo.done)}
                          style={{ width: 16, height: 16, cursor: "pointer" }}
                        />
                        <span
                          style={{
                            flex: 1,
                            textDecoration: todo.done ? "line-through" : undefined,
                            color: todo.done ? "var(--text-muted)" : undefined,
                          }}
                        >
                          {todo.title}
                        </span>
                        {todo.done && <span className="badge success">Done</span>}
                        <button
                          className="btn danger"
                          style={{ padding: "0.25rem 0.6rem", fontSize: "0.75rem" }}
                          onClick={() => handleDelete(todo.id)}
                        >
                          Delete
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
