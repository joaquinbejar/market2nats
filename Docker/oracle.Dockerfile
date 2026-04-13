FROM rust:1.94-alpine3.23 AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static protobuf-dev

WORKDIR /app

COPY . .
RUN cargo build --release -p oracle --bin oracle

# ── Runtime ──────────────────────────────────────────────────────────────

FROM alpine:3.23

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/oracle /usr/local/bin/oracle
COPY config/* /etc/oracle/

EXPOSE 9091

ENTRYPOINT ["oracle"]
CMD ["/etc/oracle/oracle.toml"]
