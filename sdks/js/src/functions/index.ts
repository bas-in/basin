/**
 * Functions namespace exports. Tier 5 placeholder — see TASKS.md.
 *
 * `basin.functions.invoke('slug', { body })` POSTs to the cloud's
 * `/v1/projects/:ref/functions/:slug/invoke` route. Today returns
 * `not_implemented`; the body is wired against the v0.2 contract.
 */

export { FunctionsClient } from "./client.js";
export type {
  FunctionsClientDeps,
  InvokeOptions,
  InvokeResult,
} from "./client.js";
