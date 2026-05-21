/**
 * App shell — thin router wrapping auth state.
 *
 * Two page states:
 *   1. Not signed in → AuthPage (email/password sign-up + sign-in)
 *   2. Signed in     → DashboardPage (org selector + todo CRUD)
 */
import React, { useState, useEffect } from "react";
import AuthPage from "./pages/AuthPage";
import DashboardPage from "./pages/DashboardPage";
import { basin } from "./lib/basin";

interface AppUser {
  id: string;
  email: string;
}

export default function App() {
  const [user, setUser] = useState<AppUser | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Restore session from localStorage (basin-js does this automatically)
    const session = basin.auth.session();
    if (session) {
      // Decode user from session — basin-js exposes session.user in v0.1
      const s = session as unknown as {
        user?: { id: string; email: string };
        access_token?: string;
      };
      if (s.user?.id) {
        setUser({ id: s.user.id, email: s.user.email ?? "" });
      }
    }
    setLoading(false);
  }, []);

  if (loading) {
    return (
      <div style={{ display: "flex", justifyContent: "center", padding: "4rem" }}>
        <span className="muted">Loading…</span>
      </div>
    );
  }

  if (!user) {
    return (
      <AuthPage
        onAuth={(u) => setUser(u)}
      />
    );
  }

  return (
    <DashboardPage
      user={user}
      onSignOut={() => {
        basin.auth.signOut().then(() => setUser(null));
      }}
    />
  );
}
