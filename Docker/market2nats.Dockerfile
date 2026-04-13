FROM rust:1.94-alpine3.23 AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static protobuf-dev

WORKDIR /app

# Cache workspace + member manifests, build script, proto, and domain source.
COPY Cargo.toml Cargo.lock ./
COPY crates/market2nats-domain/ crates/market2nats-domain/
COPY crates/market2nats/Cargo.toml crates/market2nats/Cargo.toml
COPY crates/market2nats/build.rs   crates/market2nats/build.rs
COPY crates/market2nats/proto/     crates/market2nats/proto/
COPY crates/oracle/Cargo.toml crates/oracle/Cargo.toml

# Dummy src to cache external dependency compilation.
# oracle dummy is kept for workspace resolution during the real market2nats build.
RUN mkdir -p crates/market2nats/src && \
    printf 'fn main() {}' > crates/market2nats/src/main.rs && \
    printf ''              > crates/market2nats/src/lib.rs  && \
    mkdir -p crates/oracle/src && \
    printf 'fn main() {}' > crates/oracle/src/main.rs && \
    printf ''              > crates/oracle/src/lib.rs  && \
    cargo build --release -p market2nats 2>/dev/null || true && \
    rm -rf crates/market2nats/src

# Copy real source and build.
COPY crates/market2nats/src/ crates/market2nats/src/
RUN rm -f target/release/market2nats target/release/deps/market2nats-* \
         target/release/deps/libmarket2nats-* && \
    cargo build --release -p market2nats --bin market2nats

# ── Runtime ──────────────────────────────────────────────────────────────

FROM alpine:3.23

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/market2nats /usr/local/bin/market2nats
COPY config/docker-relay.toml /etc/market2nats/relay.toml

EXPOSE 8080

ENTRYPOINT ["market2nats"]
CMD ["/etc/market2nats/relay.toml"]
