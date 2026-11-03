FROM rust:1.94.0-bookworm AS builder

WORKDIR /workspace

RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential cmake pkg-config zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations
COPY web ./web

RUN cargo build --locked --release --workspace --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin motogear

WORKDIR /app

COPY --from=builder /workspace/target/release/motogear-api /usr/local/bin/motogear-api
COPY --from=builder /workspace/target/release/migrate /usr/local/bin/migrate
COPY --from=builder /workspace/target/release/seed /usr/local/bin/seed
COPY --from=builder /workspace/target/release/publisher /usr/local/bin/publisher
COPY --from=builder /workspace/target/release/inventory /usr/local/bin/inventory
COPY --from=builder /workspace/target/release/fulfillment /usr/local/bin/fulfillment
COPY --from=builder /workspace/target/release/notifications /usr/local/bin/notifications
COPY --chown=motogear:motogear web ./web

USER motogear

EXPOSE 3000
