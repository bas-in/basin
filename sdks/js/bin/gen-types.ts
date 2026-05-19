#!/usr/bin/env node
import { writeFileSync } from "node:fs";
import { fetchOpenAPI } from "../src/openapi/fetch.js";
import { openAPIDocToTypes } from "../src/codegen/openapi-to-types.js";

function printUsage(): void {
  process.stdout.write(
    `Usage: basin-js-gen-types --url <basin-url> --key <anon-key> [--out <file>]\n` +
      `\n` +
      `Options:\n` +
      `  --url <url>   Basin engine root URL (required)\n` +
      `  --key <key>   Anon key (required)\n` +
      `  --out <file>  Output file path (default: database.types.ts)\n` +
      `  --help, -h    Show this help message\n`,
  );
}

function parseArgs(argv: string[]): {
  url: string | undefined;
  key: string | undefined;
  out: string;
  help: boolean;
} {
  const args = argv.slice(2);
  let url: string | undefined;
  let key: string | undefined;
  let out = "database.types.ts";
  let help = false;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--help" || arg === "-h") {
      help = true;
    } else if (arg === "--url") {
      url = args[++i];
    } else if (arg === "--key") {
      key = args[++i];
    } else if (arg === "--out") {
      out = args[++i] ?? out;
    }
  }

  return { url, key, out, help };
}

async function main(): Promise<void> {
  const { url, key, out, help } = parseArgs(process.argv);

  if (help) {
    printUsage();
    process.exit(0);
  }

  if (!url) {
    process.stderr.write("error: --url is required\n");
    printUsage();
    process.exit(1);
  }

  if (!key) {
    process.stderr.write("error: --key is required\n");
    printUsage();
    process.exit(1);
  }

  let doc;
  try {
    doc = await fetchOpenAPI(url, key);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    process.stderr.write(`error: failed to fetch OpenAPI doc — ${msg}\n`);
    process.exit(1);
  }

  const source = openAPIDocToTypes(doc);

  const tableCount = (source.match(/\bRow:/g) ?? []).length;

  try {
    writeFileSync(out, source, "utf-8");
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    process.stderr.write(`error: failed to write file — ${msg}\n`);
    process.exit(1);
  }

  process.stdout.write(`wrote ${out} (${tableCount} tables)\n`);
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`error: ${msg}\n`);
  process.exit(1);
});
