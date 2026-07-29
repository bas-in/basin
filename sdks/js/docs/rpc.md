# RPC / functions — basin-js guide

Call server-side SQL or Wasm functions with `basin.functions.invoke()`. The
SDK POSTs to `/rest/v1/rpc/:fn_name` with a JSON body of named arguments and
returns the function result.

---

## Quick start

```ts
import { createClient } from "@bas-in/basin-js";

const basin = createClient(process.env.BASIN_URL!, process.env.BASIN_ANON_KEY!);

// Call a scalar function
const { data, error } = await basin.functions.invoke("add", {
  body: { x: 3, y: 4 },
});

if (error) throw error;
console.log(data); // 7
```

---

## `basin.functions.invoke()`

```ts
basin.functions.invoke<T = unknown>(
  fnName: string,
  options?: {
    body?: Record<string, unknown>;  // named arguments → JSON request body
    headers?: Record<string, string>; // per-call headers merged on top of defaults
  },
): Promise<{ data: T | null; error: BasinError | null }>
```

- **`fnName`** — the SQL function name as it appears in the engine RPC route
  (`POST /rest/v1/rpc/:fn_name`).
- **`body`** — named arguments sent as the JSON request body. Pass `{}` or
  omit for functions that take no arguments.
- **`headers`** — optional per-call headers merged on top of the client
  defaults (auth, content-type, etc.).

---

## Scalar functions (`RETURNS <type>`)

A function that returns a single scalar value yields that value directly in
`data`.

```sql
-- Server-side SQL
CREATE FUNCTION add(x int, y int) RETURNS int
LANGUAGE sql AS $$ SELECT x + y $$;
```

```ts
const { data, error } = await basin.functions.invoke<number>("add", {
  body: { x: 10, y: 32 },
});
// data === 42
```

---

## Table-returning functions (`RETURNS TABLE`)

A function that returns a result set yields an array of row objects in `data`.

```sql
-- Server-side SQL
CREATE FUNCTION active_users(min_logins int)
RETURNS TABLE(id uuid, email text, login_count int)
LANGUAGE sql AS $$
  SELECT id, email, login_count
  FROM users
  WHERE login_count >= min_logins;
$$;
```

```ts
type ActiveUser = { id: string; email: string; login_count: number };

const { data, error } = await basin.functions.invoke<ActiveUser[]>(
  "active_users",
  { body: { min_logins: 5 } },
);

if (data) {
  for (const user of data) {
    console.log(user.email, user.login_count);
  }
}
```

---

## Wasm functions

`LANGUAGE sql` and `LANGUAGE wasm` functions dispatch through the same RPC
route — the call syntax is identical:

```ts
const { data, error } = await basin.functions.invoke("process_image", {
  body: { path: "uploads/photo.jpg", quality: 80 },
});
```

---

## Auth

The JWT from the active session is forwarded automatically as
`Authorization: Bearer <token>`. Functions that require a signed-in user will
return a `BasinError("unauthorized")` if the session is missing or expired.

```ts
// Sign in first
await basin.auth.signInWithPassword({ email, password });

// Now invoke — bearer token is attached automatically
const { data, error } = await basin.functions.invoke("my_protected_fn", {
  body: { arg: "value" },
});

if (error?.code === "unauthorized") {
  // session expired — redirect to sign-in
}
```

Pass per-call auth by overriding the `Authorization` header:

```ts
const { data, error } = await basin.functions.invoke("fn", {
  body: { arg: 1 },
  headers: { Authorization: `Bearer ${customToken}` },
});
```

---

## Error handling

`invoke()` always resolves (never rejects). Inspect the `error` field.

```ts
const { data, error } = await basin.functions.invoke("risky_fn", {
  body: { input: "…" },
});

if (error) {
  switch (error.code) {
    case "unauthorized":
      // 401 or 403 from the engine
      break;
    case "network":
      // connection failed before reaching the engine
      break;
    case "internal":
      // engine returned a non-2xx HTTP status
      console.error(error.status, error.details);
      break;
    case "invalid_response":
      // engine response was not JSON
      break;
    default:
      console.error(error);
  }
}
```

`BasinError` fields:

| Field | Type | Description |
|---|---|---|
| `code` | `string` | Machine-readable error kind |
| `message` | `string` | Human-readable description |
| `status` | `number \| undefined` | HTTP status code when applicable |
| `details` | `unknown` | Raw response body for non-2xx replies |

---

## No-argument functions

Omit `body` or pass an empty object — the SDK sends `{}` as the request body
either way:

```ts
const { data } = await basin.functions.invoke("server_time");
// or equivalently:
const { data: data2 } = await basin.functions.invoke("server_time", { body: {} });
```

---

## Sub-path import

If you only need `FunctionsClient` without the full SDK:

```ts
import { FunctionsClient } from "@bas-in/basin-js/functions";
```

---

## API reference summary

```ts
// Invoke a server-side function
basin.functions.invoke<T>(fnName: string, options?: InvokeOptions): Promise<InvokeResult<T>>

interface InvokeOptions {
  body?: Record<string, unknown>;
  headers?: Record<string, string>;
}

interface InvokeResult<T = unknown> {
  data: T | null;
  error: BasinError | null;
}
```

Engine endpoint: `POST /rest/v1/rpc/:fn_name`
