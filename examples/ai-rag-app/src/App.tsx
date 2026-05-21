/**
 * Minimal RAG UI.
 *
 * Query loop:
 *   1. User enters a question.
 *   2. The UI calls POST /fn/v1/rag-answer with { question, context }.
 *      The context is first retrieved from Basin via the REST API raw-SQL
 *      escape hatch (rpc/exec_sql), which mirrors what the CLI does.
 *
 * In a production setup you would call basin-js's .rpc() method or the
 * direct pgwire path.  Here we use fetch() with the REST tunnel shape so
 * the browser demo works without a pgwire client.
 *
 * The retrieval step calls POST /rest/v1/rpc/retrieve_chunks (a stored
 * procedure) — or falls back to embedding client-side if the browser
 * cannot reach pgwire.  For simplicity this demo uses the basin-fn Wasm
 * function for the full retrieve+answer round-trip: it receives the raw
 * question, runs the similarity SQL internally, and returns the answer.
 * This avoids shipping the embedding model to the browser.
 */

import React, { useState, useRef } from "react";

const BASIN_FN_URL =
  import.meta.env.VITE_BASIN_FN_URL ?? "/fn/v1/rag-answer";
const BASIN_JWT = import.meta.env.VITE_BASIN_JWT ?? "";

interface Message {
  role: "user" | "assistant" | "system";
  text: string;
}

export function App() {
  const [messages, setMessages] = useState<Message[]>([
    {
      role: "system",
      text:
        "Basin RAG demo — powered by native vector search (vector(1536) column, " +
        "ORDER BY embedding <=> '...' LIMIT 4) and a basin-fn Wasm function " +
        "that calls the inference API via the http host import.",
    },
  ]);
  const [question, setQuestion] = useState("");
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  async function handleAsk(e: React.FormEvent) {
    e.preventDefault();
    const q = question.trim();
    if (!q || loading) return;

    setQuestion("");
    setMessages((prev) => [...prev, { role: "user", text: q }]);
    setLoading(true);

    try {
      const resp = await fetch(BASIN_FN_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(BASIN_JWT ? { Authorization: `Bearer ${BASIN_JWT}` } : {}),
        },
        // The fn receives question + empty context; it runs the similarity
        // search itself (via query.exec) and calls the inference API.
        body: JSON.stringify({ question: q, context: "" }),
      });

      if (!resp.ok) {
        const text = await resp.text();
        setMessages((prev) => [
          ...prev,
          {
            role: "assistant",
            text: `Error ${resp.status}: ${text}`,
          },
        ]);
        return;
      }

      const body = (await resp.json()) as { answer: string };
      setMessages((prev) => [
        ...prev,
        { role: "assistant", text: body.answer },
      ]);
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text:
            `Failed to reach Basin function at ${BASIN_FN_URL}.\n\n` +
            `Make sure Basin is running and the function is deployed:\n` +
            `  basin functions deploy ./fn/rag-answer.ts --name rag-answer\n\n` +
            `Or use the CLI loop instead:\n  npm run query`,
        },
      ]);
    } finally {
      setLoading(false);
      setTimeout(() => inputRef.current?.focus(), 50);
    }
  }

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1 style={styles.title}>Basin RAG Demo</h1>
        <p style={styles.subtitle}>
          Vector search over embedded documents &middot; basin-fn inference
        </p>
      </header>

      <div style={styles.messages}>
        {messages.map((m, i) => (
          <MessageBubble key={i} message={m} />
        ))}
        {loading && (
          <div style={{ ...styles.bubble, ...styles.assistantBubble }}>
            <span style={styles.thinking}>Thinking…</span>
          </div>
        )}
      </div>

      <form onSubmit={handleAsk} style={styles.form}>
        <input
          ref={inputRef}
          style={styles.input}
          type="text"
          placeholder="Ask anything about Basin…"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          disabled={loading}
          autoFocus
        />
        <button style={styles.button} type="submit" disabled={loading || !question.trim()}>
          Ask
        </button>
      </form>

      <footer style={styles.footer}>
        <code>vector(1536) &lt;=&gt; query_embedding ORDER BY ... LIMIT 4</code>
        {" · "}
        <code>/fn/v1/rag-answer</code>
      </footer>
    </div>
  );
}

function MessageBubble({ message }: { message: Message }) {
  const bubbleStyle =
    message.role === "user"
      ? { ...styles.bubble, ...styles.userBubble }
      : message.role === "system"
      ? { ...styles.bubble, ...styles.systemBubble }
      : { ...styles.bubble, ...styles.assistantBubble };

  return (
    <div style={bubbleStyle}>
      <pre style={styles.pre}>{message.text}</pre>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    display: "flex",
    flexDirection: "column",
    gap: "1.25rem",
  },
  header: {
    borderBottom: "1px solid #30363d",
    paddingBottom: "1rem",
  },
  title: {
    fontSize: "1.5rem",
    fontWeight: 700,
    color: "#58a6ff",
  },
  subtitle: {
    fontSize: "0.85rem",
    color: "#8b949e",
    marginTop: "0.25rem",
  },
  messages: {
    display: "flex",
    flexDirection: "column",
    gap: "0.75rem",
    minHeight: "300px",
  },
  bubble: {
    padding: "0.75rem 1rem",
    borderRadius: "8px",
    maxWidth: "100%",
  },
  userBubble: {
    background: "#1f6feb",
    alignSelf: "flex-end",
    borderRadius: "8px 8px 2px 8px",
  },
  assistantBubble: {
    background: "#161b22",
    border: "1px solid #30363d",
    alignSelf: "flex-start",
  },
  systemBubble: {
    background: "#0d1117",
    border: "1px solid #21262d",
    fontSize: "0.8rem",
    color: "#8b949e",
  },
  pre: {
    whiteSpace: "pre-wrap",
    wordBreak: "break-word",
    fontFamily: "inherit",
    fontSize: "0.9rem",
    margin: 0,
  },
  thinking: {
    color: "#8b949e",
    fontStyle: "italic",
  },
  form: {
    display: "flex",
    gap: "0.5rem",
    borderTop: "1px solid #30363d",
    paddingTop: "1rem",
  },
  input: {
    flex: 1,
    padding: "0.6rem 0.875rem",
    background: "#0d1117",
    border: "1px solid #30363d",
    borderRadius: "6px",
    color: "#e1e4e8",
    fontSize: "1rem",
    outline: "none",
  },
  button: {
    padding: "0.6rem 1.25rem",
    background: "#1f6feb",
    border: "none",
    borderRadius: "6px",
    color: "#fff",
    fontWeight: 600,
    fontSize: "0.9rem",
    cursor: "pointer",
  },
  footer: {
    fontSize: "0.75rem",
    color: "#484f58",
    textAlign: "center",
  },
};
