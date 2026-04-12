FROM rust:1.94-alpine3.23 AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static protobuf-dev

WORKDIR /app

# Cache workspace + member manifests, build script, proto, and domain source.
# market2nats-domain is small (pure types) — include its real source in the
# cache layer so the dep-cache build compiles it correctly.
COPY Cargo.toml Cargo.lock ./
COPY crates/market2nats-domain/ crates/market2nats-domain/
COPY crates/market2nats/Cargo.toml crates/market2nats/Cargo.toml
COPY crates/market2nats/build.rs   crates/market2nats/build.rs
COPY crates/market2nats/proto/     crates/market2nats/proto/

# Dummy src for market2nats to cache external dependency compilation.
RUN mkdir -p crates/market2nats/src && \
    printf 'fn main() {}' > crates/market2nats/src/main.rs && \
    printf ''              > crates/market2nats/src/lib.rs  && \
    cargo build --release -p market2nats 2>/dev/null || true && \
    rm -rf crates/market2nats/src

# Copy real market2nats source and build.
COPY crates/market2nats/src/ crates/market2nats/src/
RUN cargo build --release -p market2nats --bin market2nats

# ── Runtime ──────────────────────────────────────────────────────────────

FROM alpine:3.23

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/market2nats /usr/local/bin/market2nats
COPY config/relay.toml /etc/market2nats/relay.toml

EXPOSE 8080

ENTRYPOINT ["market2nats"]
CMD ["/etc/market2nats/relay.toml"]
