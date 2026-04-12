# Makefile for common tasks in a Rust project
# Detect current branch
CURRENT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)


# Default target
.PHONY: all
all: test fmt lint build

# Build the project
.PHONY: build
build:
	cargo build

.PHONY: release
release:
	cargo build --release

# Run unit tests (no external services needed)
.PHONY: test
test:
	LOGLEVEL=WARN cargo test

# Run integration tests (requires NATS: docker compose -f Docker/docker-compose.yml up -d nats)
.PHONY: integration-test
integration-test:
	LOGLEVEL=WARN cargo test -- --ignored

# Run all tests (unit + integration)
.PHONY: test-all
test-all:
	LOGLEVEL=WARN cargo test -- --include-ignored

# Format the code
.PHONY: fmt
fmt:
	cargo +stable fmt --all

# Check formatting
.PHONY: fmt-check
fmt-check:
	cargo +stable fmt --check

# Run Clippy for linting
.PHONY: lint
lint:
	cargo clippy --all-targets --all-features --workspace -- -D warnings

.PHONY: lint-fix
lint-fix: 
	cargo clippy --fix --all-targets --all-features --allow-dirty --allow-staged --workspace -- -D warnings

# Clean the project
.PHONY: clean
clean:
	cargo clean

# Pre-push checks
.PHONY: check
check: test fmt-check lint

# Run the project
.PHONY: run
run:
	cargo run

# Run the project with the Binance spot trades (BTC/ETH) profile.
# Requires NATS_USER and NATS_PASSWORD in the environment (the TOML resolves ${NATS_USER}/${NATS_PASSWORD}).
.PHONY: run-binance-spot-trades
run-binance-spot-trades:
	cargo run --release -- config/relay.binance-spot-trades.toml

# Run the project against Bybit (spot + linear USDT perps) for BTC and ETH.
# Exercises trade / ticker / l2_orderbook on spot, plus liquidation on linear.
.PHONY: run-bybit
run-bybit:
	cargo run --release -- config/relay.bybit.toml

# Run the project against BitMEX realtime (XBT/ETH perps).
.PHONY: run-bitmex
run-bitmex:
	cargo run --release -- config/relay.bitmex.toml

# Run the project against Bitstamp v2 public WS (spot BTC/ETH).
.PHONY: run-bitstamp
run-bitstamp:
	cargo run --release -- config/relay.bitstamp.toml

# Run the project against Hyperliquid (on-chain perpetuals).
.PHONY: run-hyperliquid
run-hyperliquid:
	cargo run --release -- config/relay.hyperliquid.toml

# Run the project against Crypto.com Exchange v1 (spot + perpetuals).
.PHONY: run-crypto-com
run-crypto-com:
	cargo run --release -- config/relay.crypto-com.toml


# Run the project against Kraken v2 spot.
# NOTE: requires the per-channel params object subscribe mode in generic_ws.
.PHONY: run-kraken
run-kraken:
	cargo run --release -- config/relay.kraken.toml


# Run the project against Kraken Futures (USD-margined perpetuals).
# NOTE: requires the per-channel frame tweak shared with Kraken spot / Gate.io.
.PHONY: run-kraken-futures
run-kraken-futures:
	cargo run --release -- config/relay.kraken-futures.toml


# Run the project against Gate.io v4 (spot + USDT perps).
# Uses subscribe_template (per-frame subscriptions). Native per-channel batching is a follow-up.
.PHONY: run-gate
run-gate:
	cargo run --release -- config/relay.gate.toml


.PHONY: fix
fix:
	cargo fix --allow-staged --allow-dirty

.PHONY: pre-push
pre-push: fix fmt lint-fix test doc

.PHONY: doc
doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

.PHONY: doc-open
doc-open:
	cargo doc --open

.PHONY: coverage
coverage:
	export LOGLEVEL=WARN
	cargo install cargo-tarpaulin
	mkdir -p coverage
	cargo tarpaulin --verbose --all-features --workspace --timeout 0 --out Xml --output-dir coverage

.PHONY: coverage-html
coverage-html:
	export LOGLEVEL=WARN
	cargo install cargo-tarpaulin
	mkdir -p coverage
	cargo tarpaulin --color Always --tests --all-targets --all-features --workspace --timeout 0 --out Html --output-dir coverage

.PHONY: open-coverage
open-coverage:
	open coverage/tarpaulin-report.html

# Rule to show git log
git-log:
	@if [ "$(CURRENT_BRANCH)" = "HEAD" ]; then \
		echo "You are in a detached HEAD state. Please check out a branch."; \
		exit 1; \
	fi; \
	echo "Showing git log for branch $(CURRENT_BRANCH) against main:"; \
	git log main..$(CURRENT_BRANCH) --pretty=full

.PHONY: create-doc
create-doc:
	cargo doc --no-deps --document-private-items

.PHONY: check-spanish
check-spanish:
	@rg -n --pcre2 -e '^\s*(//|///|//!|#|/\*|\*).*?[áéíóúÁÉÍÓÚñÑ¿¡]' \
    	    --glob '!target/*' \
    	    --glob '!**/*.png' \
    	    . || (echo "❌  Spanish comments found"; exit 1)

.PHONY: zip
zip:
	@echo "Creating $(ZIP_NAME) without any 'target' directories, 'Cargo.lock', and hidden files..."
	@find . -type f \
		! -path "*/target/*" \
		! -path "./.*" \
		! -name "Cargo.lock" \
		! -name ".*" \
		| zip -@ $(ZIP_NAME)
	@echo "$(ZIP_NAME) created successfully."


.PHONY: check-cargo-criterion
check-cargo-criterion:
	@command -v cargo-criterion > /dev/null || (echo "Installing cargo-criterion..."; cargo install cargo-criterion)

.PHONY: bench
bench: check-cargo-criterion
	cargo criterion --output-format=quiet

.PHONY: bench-show
bench-show:
	open target/criterion/report/index.html

.PHONY: bench-save
bench-save: check-cargo-criterion
	cargo criterion --output-format quiet --history-id v0.3.2 --history-description "Version 0.3.2 baseline"

.PHONY: bench-compare
bench-compare: check-cargo-criterion
	cargo criterion --output-format verbose

.PHONY: bench-json
bench-json: check-cargo-criterion
	cargo criterion --message-format json

.PHONY: bench-clean
bench-clean:
	rm -rf target/criterion


.PHONY: killall
killall:
	@echo "Killing market2nats processes (port 8080)..."
	@pid=$$(lsof -ti :8080 2>/dev/null); \
	if [ -n "$$pid" ]; then \
		echo "  Port 8080: killing PID $$pid"; \
		kill -9 $$pid 2>/dev/null || true; \
	else \
		echo "  Port 8080: free"; \
	fi
	@echo "Done."

.PHONY: workflow-coverage
workflow-coverage:
	DOCKER_HOST="$${DOCKER_HOST}" act push --job code_coverage_report \
       -P ubuntu-latest=catthehacker/ubuntu:latest \
       --privileged

.PHONY: workflow-build
workflow-build:
	DOCKER_HOST="$${DOCKER_HOST}" act push --job build \
       -P ubuntu-latest=catthehacker/ubuntu:latest

.PHONY: workflow-lint
workflow-lint:
	DOCKER_HOST="$${DOCKER_HOST}" act push --job lint

.PHONY: workflow-test
workflow-test:
	DOCKER_HOST="$${DOCKER_HOST}" act push --job run_tests

.PHONY: workflow
workflow: workflow-build workflow-lint workflow-test workflow-coverage

