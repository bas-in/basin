/**
 * Sign-up / sign-in page.
 *
 * Uses basin.auth.signUp() and basin.auth.signInWithPassword() from
 * @basin/basin-js (docs/basin-js-design.md § 2 "Auth").
 *
 * basin-auth endpoints under the hood:
 *   POST /auth/v1/signup  — { email, password }
 *   POST /auth/v1/signin  — { email, password }
 *
 * OAuth sign-in (GitHub / Google) is deferred to basin-auth v2 (ADR 0020).
 * The button is rendered but disabled with an explanatory tooltip.
 */
import React, { useState } from "react";
import { signUp, signIn } from "../api/auth";

interface Props {
  onAuth: (user: { id: string; email: string }) => void;
}

type Mode = "signin" | "signup";

export default function AuthPage({ onAuth }: Props) {
  const [mode, setMode] = useState<Mode>("signin");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [signupNote, setSignupNote] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setSignupNote(null);
    setLoading(true);

    try {
      if (mode === "signup") {
        const { user, error } = await signUp(email, password);
        if (error) {
          setError(error);
        } else if (user) {
          // basin-auth may require email verification before sign-in.
          // If the server has BASIN_AUTH_ENABLED=1 and SMTP configured,
          // the user must click the verification link before signing in.
          setSignupNote(
            "Account created! If email verification is enabled, " +
              "check your inbox and click the link before signing in."
          );
          setMode("signin");
        }
      } else {
        const { user, error } = await signIn(email, password);
        if (error) {
          setError(error);
        } else if (user) {
          onAuth(user);
        }
      }
    } finally {
      setLoading(false);
    }
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "2rem",
      }}
    >
      <div className="card" style={{ width: "100%", maxWidth: 420 }}>
        {/* Wordmark */}
        <div style={{ marginBottom: "1.5rem" }}>
          <h1 style={{ fontSize: "1.4rem" }}>
            Basin{" "}
            <span className="badge" style={{ verticalAlign: "middle", fontSize: "0.7rem" }}>
              SaaS Starter
            </span>
          </h1>
          <p className="muted" style={{ fontSize: "0.85rem", marginTop: "0.3rem" }}>
            Multi-tenant todo app — Basin auth + RLS demo
          </p>
        </div>

        {signupNote && (
          <div
            style={{
              background: "rgba(81,207,102,0.08)",
              border: "1px solid rgba(81,207,102,0.3)",
              borderRadius: "var(--radius)",
              padding: "0.75rem",
              marginBottom: "1rem",
              fontSize: "0.85rem",
            }}
          >
            {signupNote}
          </div>
        )}

        <form onSubmit={handleSubmit}>
          <div className="form-group">
            <label htmlFor="email">Email</label>
            <input
              id="email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="you@example.com"
              required
              autoComplete="email"
            />
          </div>

          <div className="form-group">
            <label htmlFor="password">Password</label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              required
              minLength={8}
              autoComplete={mode === "signup" ? "new-password" : "current-password"}
            />
          </div>

          {error && <p className="error">{error}</p>}

          <button
            type="submit"
            className="btn primary"
            style={{ width: "100%", marginTop: "0.5rem", justifyContent: "center" }}
            disabled={loading}
          >
            {loading
              ? "…"
              : mode === "signin"
              ? "Sign in"
              : "Create account"}
          </button>
        </form>

        {/* OAuth placeholder — not yet implemented */}
        <div style={{ marginTop: "1rem", textAlign: "center" }}>
          <p className="muted" style={{ fontSize: "0.8rem", marginBottom: "0.5rem" }}>
            — or —
          </p>
          <button
            className="btn"
            style={{ width: "100%", justifyContent: "center", opacity: 0.45 }}
            disabled
            title="OAuth sign-in is planned for basin-auth v2 (ADR 0020). Not yet available."
          >
            Sign in with GitHub (coming in basin-auth v2)
          </button>
        </div>

        <p
          style={{ marginTop: "1.25rem", textAlign: "center", fontSize: "0.85rem" }}
          className="muted"
        >
          {mode === "signin" ? (
            <>
              No account?{" "}
              <button
                className="btn"
                style={{ padding: 0, border: "none", background: "none", color: "var(--primary)", cursor: "pointer" }}
                onClick={() => { setMode("signup"); setError(null); }}
              >
                Create one
              </button>
            </>
          ) : (
            <>
              Already have an account?{" "}
              <button
                className="btn"
                style={{ padding: 0, border: "none", background: "none", color: "var(--primary)", cursor: "pointer" }}
                onClick={() => { setMode("signin"); setError(null); }}
              >
                Sign in
              </button>
            </>
          )}
        </p>
      </div>
    </div>
  );
}
