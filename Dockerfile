# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS build

# protoc + google/protobuf/*.proto includes, needed by build.rs (tonic-build)
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Build dependencies first for better layer caching
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
 && echo 'fn main() {}' > build.rs \
 && cargo build --release \
 && rm -rf src build.rs

# Build the real application
COPY build.rs status.proto ./
COPY src ./src
RUN touch src/main.rs build.rs && cargo build --release

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runner

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /app/target/release/grpc-status-server ./grpc-status-server

# ── Environment defaults ───────────────────────────────────────────────────────
# PORT is injected by Render automatically — this is the public-facing port
# for the gRPC-Web server. Do NOT set it here; let Render control it.
ENV GRPC_PORT=50051 \
    GRPC_HOST=0.0.0.0 \
    ALLOWED_ORIGINS=*

# Expose the gRPC-Web port (Render overrides this with its own PORT)
EXPOSE 8080

# The binary handles SIGTERM itself for graceful shutdown.
CMD ["./grpc-status-server"]
