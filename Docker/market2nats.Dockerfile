FROM rust:1.94-alpine3.23 AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static protobuf-dev

WORKDIR /app

COPY . .
RUN cargo build --release -p market2nats --bin market2nats

# ── Runtime ──────────────────────────────────────────────────────────────

FROM alpine:3.23

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/market2nats /usr/local/bin/market2nats
COPY config/* /etc/market2nats/

EXPOSE 8080

ENTRYPOINT ["market2nats"]
CMD ["/etc/market2nats/relay.all-spot-trades.toml"]
