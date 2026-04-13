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
# market2nats dummy is kept for workspace resolution during the real oracle build.
RUN mkdir -p crates/market2nats/src && \
    printf 'fn main() {}' > crates/market2nats/src/main.rs && \
    printf ''              > crates/market2nats/src/lib.rs  && \
    mkdir -p crates/oracle/src && \
    printf 'fn main() {}' > crates/oracle/src/main.rs && \
    printf ''              > crates/oracle/src/lib.rs  && \
    cargo build --release -p oracle 2>/dev/null || true && \
    rm -rf crates/oracle/src

# Copy real oracle source and build.
COPY crates/oracle/src/ crates/oracle/src/
RUN cargo build --release -p oracle --bin oracle

# ── Runtime ──────────────────────────────────────────────────────────────

FROM alpine:3.23

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/oracle /usr/local/bin/oracle
COPY config/docker-oracle.toml /etc/oracle/oracle.toml

EXPOSE 9091

ENTRYPOINT ["oracle"]
CMD ["/etc/oracle/oracle.toml"]
