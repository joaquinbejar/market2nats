FROM rust:1.94-alpine3.23 AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static protobuf-dev

WORKDIR /app

# Cache dependencies by copying manifests first.
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto/ proto/

# Create a dummy src to build deps.
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src

# Copy real source and build.
COPY src/ src/
RUN cargo build --release --bin market2nats

# ── Runtime ──────────────────────────────────────────────────────────────

FROM alpine:3.23

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/market2nats /usr/local/bin/market2nats
COPY config/relay.toml /etc/market2nats/relay.toml

EXPOSE 8080

ENTRYPOINT ["market2nats"]
CMD ["/etc/market2nats/relay.toml"]
