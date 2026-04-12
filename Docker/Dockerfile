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
COPY crates/oracle/Cargo.toml crates/oracle/Cargo.toml

# Dummy src for market2nats and oracle to cache external dependency compilation.
RUN mkdir -p crates/market2nats/src && \
    printf 'fn main() {}' > crates/market2nats/src/main.rs && \
    printf ''              > crates/market2nats/src/lib.rs  && \
    mkdir -p crates/oracle/src && \
    printf 'fn main() {}' > crates/oracle/src/main.rs && \
    printf ''              > crates/oracle/src/lib.rs  && \
    cargo build --release -p market2nats 2>/dev/null || true && \
    cargo build --release -p oracle 2>/dev/null || true && \
    rm -rf crates/market2nats/src crates/oracle/src

# Copy real market2nats source and build.
COPY crates/market2nats/src/ crates/market2nats/src/
RUN cargo build --release -p market2nats --bin market2nats

# Copy real oracle source and build.
COPY crates/oracle/src/ crates/oracle/src/
RUN cargo build --release -p oracle --bin oracle

# ── Runtime ──────────────────────────────────────────────────────────────

FROM alpine:3.23

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/market2nats /usr/local/bin/market2nats
COPY --from=builder /app/target/release/oracle /usr/local/bin/oracle
COPY config/relay.toml /etc/market2nats/relay.toml
COPY config/oracle.toml /etc/market2nats/oracle.toml

EXPOSE 8080
EXPOSE 9091

ENTRYPOINT ["market2nats"]
CMD ["/etc/market2nats/relay.toml"]
