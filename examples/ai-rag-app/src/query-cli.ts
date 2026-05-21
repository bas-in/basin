/**
 * CLI query loop: ask → retrieve → answer (via basin-fn or local fallback).
 *
 * Run:
 *   OPENAI_API_KEY=sk-... npm run query
 *
 * The loop calls the basin-fn Wasm function at /fn/v1/rag-answer when
 * BASIN_URL is set and BASIN_JWT is a valid JWT.  Otherwise it falls back
 * to a direct OpenAI chat completion call for local development without
 * a live basin-fn deployment.
 */

import * as readline from "readline/promises";
import { createBasinPool } from "./basin-client.js";
import { retrieveChunks, buildContext } from "./retrieve.js";
import { answerViaFn } from "./answer.js";

async function main() {
  const pool = createBasinPool();
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  console.log("Basin RAG demo — type your question, or 'quit' to exit.\n");

  try {
    while (true) {
      const question = await rl.question("? ");
      if (!question || question.trim().toLowerCase() === "quit") break;

      // 1. Retrieve
      process.stdout.write("Retrieving relevant chunks…\n");
      const chunks = await retrieveChunks(question, pool, 4, "cosine");
      if (chunks.length === 0) {
        console.log(
          "No chunks found — run `npm run ingest` first to populate the corpus.\n"
        );
        continue;
      }
      const context = buildContext(chunks);
      console.log(`Retrieved ${chunks.length} chunks.\n`);

      // 2. Answer
      process.stdout.write("Generating answer…\n");
      const answer = await answerViaFn(question, context);
      console.log("\nAnswer:\n" + answer + "\n");
    }
  } finally {
    rl.close();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
