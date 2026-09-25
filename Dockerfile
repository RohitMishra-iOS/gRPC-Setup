# ── Stage 1: deps ─────────────────────────────────────────────────────────────
FROM node:20-slim AS deps

WORKDIR /app

# Copy only package files first for better layer caching
COPY package.json package-lock.json ./

# Install production dependencies only
RUN npm ci --omit=dev

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM node:20-slim AS runner

WORKDIR /app

# Copy production node_modules from deps stage
COPY --from=deps /app/node_modules ./node_modules

# Copy application source
COPY server.js         ./server.js
COPY db.js             ./db.js
COPY grpc-web-proxy.js ./grpc-web-proxy.js
COPY status.proto      ./status.proto
COPY package.json      ./package.json

# ── Environment defaults ───────────────────────────────────────────────────────
# PORT is injected by Render automatically — this is the public-facing port
# for the gRPC-Web proxy. Do NOT set it here; let Render control it.
ENV NODE_ENV=production \
    GRPC_PORT=50051 \
    GRPC_HOST=0.0.0.0 \
    ALLOWED_ORIGINS=*

# Expose the gRPC-Web proxy port (Render overrides this with its own PORT)
EXPOSE 8080

# node:20-slim ships with dumb-init equivalent; use node directly.
# Node 20 handles SIGTERM correctly for graceful shutdown.
CMD ["node", "server.js"]
