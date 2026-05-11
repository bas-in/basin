# @basin/basin-js

Isomorphic JavaScript / TypeScript client for [Basin](https://basin.run).
Speaks **directly** to a deployed [`basin-engine`](https://github.com/bas-in/basin)
(the open-source Rust core, Apache-2.0) — auth, PostgREST-shaped table
queries today; storage, realtime, and edge functions in engine v0.2.

Works in Node 18+, browsers, Bun, Deno, Cloudflare Workers — anywhere
with a global `fetch`.

> **Status — `0.0.1` skeleton.** Public API surface is staked out so
> app code can be written against it. Method bodies land tier-by-tier;
> see [`TASKS.md`](./TASKS.md).

## Install

```sh
npm install @basin/basin-js
# or: pnpm add @basin/basin-js
# or: bun add @basin/basin-js
```

Deno: `import { createClient } from "jsr:@basin/basin-js";` (ships from the
same source via [JSR](https://jsr.io/@basin/basin-js); `jsr.json` is the
companion manifest in this repo). Browser via CDN:
`<script type="module" src="https://esm.sh/@basin/basin-js@0.1"></script>`.

## Quickstart

```ts
import { createClient } from "@basin/basin-js";

// BASIN_URL points at a deployed basin-engine — NOT basin-cloud.
// Mint BASIN_ANON_KEY at https://basin.run/app/project/<ref>/api-keys
const basin = createClient(
  process.env.BASIN_URL!,        // e.g. https://basin-engine.fly.dev
  process.env.BASIN_ANON_KEY!,
);

// Auth — hits engine /auth/v1/signin
await basin.auth.signInWithPassword({
  email: "you@example.com",
  password: "…",
});

// Query — hits engine /rest/v1/products
const { data, error } = await basin
  .from<{ id: number; name: string; price: number }>("products")
  .select("id, name, price")
  .eq("active", true)
  .order("price", { ascending: true })
  .limit(10);
```

## Architecture

[Basin Cloud](https://basin.run) is the **control plane** — dashboard,
billing, project management, and the place you mint the anon-key JWT
that the SDK ships to the engine. Once you have a URL + key,
basin-cloud is **off the data path**: every `basin.auth.*` and
`basin.from(...)` call lands on `basin-engine` directly. The engine
is open source and deployable to Fly with `./deploy.sh -t engine`
from the `basin-cloud` repo root, or runnable locally via
`cargo run -p basin-server`.

Engine v0.1 routes: `/auth/v1/{signup,signin,refresh,verify-email,
reset-password,request-password-reset,magic-link,magic-link/consume,
api-keys}`, `/rest/v1/:table`, `/health`. OAuth, MFA, storage,
realtime, and functions return `BasinError('not_implemented')` until
engine v0.2.

Full reference: <https://basin.run/docs/js-sdk>.

## API surface

| Namespace        | Methods                                                                                                       |
|------------------|---------------------------------------------------------------------------------------------------------------|
| `basin.auth`     | `signUp`, `signInWithPassword`, `signInWithMagicLink`, `consumeMagicLink`, `refreshSession`, `signOut`, `getSession`, `getUser`, `onAuthStateChange`, `requestPasswordReset`, `resetPassword`, `verifyEmail` |
| `basin.from(t)`  | `.select`, `.insert`, `.update`, `.upsert`, `.delete`, `.eq`, `.neq`, `.gt`, `.gte`, `.lt`, `.lte`, `.like`, `.ilike`, `.is`, `.in`, `.order`, `.limit`, `.range`, `.single`, `.maybeSingle` |
| `basin.storage`  | `from(bucket).upload`, `.download`, `.list`, `.remove`, `.createSignedUrl`, `.getPublicUrl` — **v0.2** |
| `basin.realtime` | `.channel(name).on(...).subscribe()` — **v0.2** |
| `basin.functions`| `.invoke(slug, { body })` — **v0.2** |

## Tree-shaking

Sub-path imports for consumers who only want one namespace:

```ts
import { AuthClient } from "@basin/basin-js/auth";
import { PostgrestQueryBuilder } from "@basin/basin-js/postgrest";
import { StorageClient } from "@basin/basin-js/storage";
```

## License

MIT — see [`LICENSE`](./LICENSE).
