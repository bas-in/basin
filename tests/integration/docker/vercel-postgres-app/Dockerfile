# Phase 5.27.C — Node.js app container for Basin vercel-postgres e2e test.
#
# Builds a minimal node:20-alpine image that runs the Vercel-serverless-
# function-shaped load harness (app/app.js).
#
# Build context: tests/integration/docker/vercel-postgres-app/
# Usage (via docker-compose.yml):
#   docker compose -f docker-compose.yml up --build --abort-on-container-exit

FROM node:20-alpine AS deps

WORKDIR /app

# Copy only the package manifest first so Docker can cache the npm install
# layer independently of app source changes.
COPY app/package.json ./

# Install production deps only (no devDeps in this app).
RUN npm install --omit=dev

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM node:20-alpine AS runtime

WORKDIR /app

# Copy installed node_modules from the deps stage.
COPY --from=deps /app/node_modules ./node_modules

# Copy application source.
COPY app/package.json ./
COPY app/app.js      ./

# Syntax-check before shipping.
RUN node --check app.js

# Unprivileged user (node uid=1000 is pre-created in node:alpine images).
USER node

EXPOSE 3000

HEALTHCHECK --interval=5s --timeout=3s --start-period=15s --retries=6 \
    CMD wget -q -O- http://127.0.0.1:3000/health || exit 1

CMD ["node", "app.js"]
