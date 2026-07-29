/**
 * Vitest workspace — Node (default) + optional browser cells.
 *
 * Default state (CI matrix today): one workspace project running `vitest`
 * over `src/**\/*.test.ts` and `tests/integration/**\/*.test.ts` in Node.
 * Behaviour is identical to the zero-config mode `npm test` ran with
 * before this file existed, so adding the workspace is invisible to CI.
 *
 * Browser cells (chromium + webkit) only attach when both:
 *   1. `BASIN_TEST_BROWSER=1` is set in the environment, AND
 *   2. `@vitest/browser` + `playwright` are installed.
 *
 * (1) keeps the browser entries OFF in normal CI cells so they don't
 * import anything that isn't on disk. (2) is the user-gated piece: the
 * roadmap defers `@vitest/browser` + `playwright` devDep installs until
 * the user approves the new deps. Until that lands, even with
 * `BASIN_TEST_BROWSER=1` the browser projects below will fail with a
 * "package not found" — that's the intended signal that the deps need
 * to be installed.
 *
 * To activate (after the deps are installed):
 *   BASIN_TEST_BROWSER=1 npm test
 *
 * Roadmap: TASKS.md Tier 7 — `vitest --browser` smoke tests.
 */

const browserMode = process.env.BASIN_TEST_BROWSER === "1";

/**
 * Default Node project — `src/**\/*.test.ts` + integration tests.
 * Matches Vitest's zero-config discovery; explicit here so adding the
 * workspace file doesn't change what runs in node-only CI.
 */
const nodeProject = {
  test: {
    name: "node",
    include: ["src/**/*.test.ts", "tests/integration/**/*.test.ts"],
    environment: "node" as const,
    globals: false,
  },
};

/**
 * Browser projects — chromium + webkit. Each runs the same
 * `src/**\/*.test.ts` glob in a real browser so we catch
 * platform-specific bugs (e.g. localStorage shape, fetch quirks,
 * crypto.subtle gaps) before npm-publish day.
 *
 * `provider: "playwright"` is the modern default for `@vitest/browser`;
 * the legacy `webdriverio` provider is also supported but adds an
 * extra dep tree we don't need.
 *
 * Headless on CI; flip `headless: false` locally to watch the runs.
 */
const browserProjects = browserMode
  ? (["chromium", "webkit"] as const).map((engine) => ({
      test: {
        name: `browser-${engine}`,
        include: ["src/**/*.test.ts"],
        browser: {
          enabled: true,
          provider: "playwright" as const,
          headless: true,
          instances: [{ browser: engine }],
        },
      },
    }))
  : [];

export default [nodeProject, ...browserProjects];
