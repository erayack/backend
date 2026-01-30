# syntax=docker/dockerfile:1

FROM rust:slim-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY build.rs ./
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json
# Build dependencies (cached unless Cargo.toml/Cargo.lock change)
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY build.rs ./
COPY migrations ./migrations
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/data

COPY --from=builder /app/target/release/receiver /usr/local/bin/backend
COPY --from=builder /app/migrations /app/migrations

ENV DATABASE_URL=sqlite:/app/data/receiver.db?mode=rwc
ENV RECEIVER_INTERNAL_BIND_ADDR=0.0.0.0:3001

EXPOSE 3001

ENTRYPOINT ["backend"]
