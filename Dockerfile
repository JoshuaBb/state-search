# ── Stage 1: Build SvelteKit frontend ────────────────────────────────────────
FROM node:22-alpine AS web-builder

WORKDIR /app/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# ── Stage 2: Compute cargo-chef recipe ───────────────────────────────────────
FROM rust:1-slim-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY data/ data/
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml core/
COPY api/Cargo.toml api/
COPY ingest/Cargo.toml ingest/
# Stub source files so cargo-chef can compute the dependency recipe
RUN mkdir -p core/src api/src ingest/src \
 && touch core/src/lib.rs \
 && printf 'fn main() {}' > api/src/main.rs \
 && printf 'fn main() {}' > ingest/src/main.rs
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Build Rust binaries ─────────────────────────────────────────────
FROM chef AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies (this layer is reused unless Cargo.lock changes)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build the real source
COPY Cargo.toml Cargo.lock ./
COPY core/ core/
COPY api/ api/
COPY ingest/ ingest/
COPY migrations/ migrations/
COPY config/ config/

RUN cargo build --release --bin api --bin ingest

# ── Stage 4a: API runtime ─────────────────────────────────────────────────────
FROM debian:bookworm-slim AS api

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=rust-builder /app/target/release/api /app/api
COPY --from=web-builder  /app/web/dist           /app/web/dist
COPY config/                                      /app/config/

ENV APP__SERVER__HOST=0.0.0.0
ENV APP__SERVER__PORT=3000

EXPOSE 3000

CMD ["/app/api"]

# ── Stage 4b: Ingest runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim AS ingest

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=rust-builder /app/target/release/ingest /app/ingest
COPY config/                                         /app/config/

ENTRYPOINT ["/app/ingest"]
