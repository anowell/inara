set dotenv-load

# List available recipes
default:
    @just --list

# --- Build & Check ---

# Build the project
build:
    cargo build

# Build in release mode
build-release:
    cargo build --release

# Run all checks (format, lint, test)
check: fmt-check lint test

# Run clippy lints
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Check formatting
fmt-check:
    cargo fmt -- --check

# Format code
fmt:
    cargo fmt

# --- Testing ---

# Run all unit tests
test:
    cargo test

# Run unit tests with output
test-verbose:
    cargo test -- --nocapture

# Run integration tests (requires DATABASE_URL)
test-integration:
    cargo test --test '*' -- --ignored --test-threads=1

# Run all tests including integration
test-all: test test-integration

# --- Database ---

# Create a local database and write DATABASE_URL to .env (idempotent)
bootstrap:
    #!/usr/bin/env bash
    set -euo pipefail
    # Resolve connection settings from the standard PG* env vars, falling
    # back to local defaults. Prepopulating PG* lets each workspace bootstrap
    # its own database.
    host="${PGHOST:-localhost}"
    port="${PGPORT:-5432}"
    user="${PGUSER:-postgres}"
    password="${PGPASSWORD:-pass}"
    db="${PGDATABASE:-inara}"
    echo "Bootstrapping database '${db}' on ${host}:${port} as '${user}'…"
    if PGPASSWORD="$password" psql -h "$host" -p "$port" -U "$user" -d postgres \
        -tAc "SELECT 1 FROM pg_database WHERE datname = '${db}'" 2>/dev/null | grep -q 1; then
        echo "Database '${db}' already exists."
    else
        PGPASSWORD="$password" createdb -h "$host" -p "$port" -U "$user" "$db"
        echo "Created database '${db}'."
    fi
    # Persist DATABASE_URL only when it is not already set. `set dotenv-load`
    # loads .env on the next run, so a written value makes this a no-op.
    if [ -z "${DATABASE_URL:-}" ]; then
        echo "DATABASE_URL=postgres://${user}:${password}@${host}:${port}/${db}" >> .env
        echo "Wrote DATABASE_URL to .env"
    else
        echo "DATABASE_URL already set — leaving .env untouched."
    fi

# Check database connectivity
db-check:
    @psql "$DATABASE_URL" -c "SELECT 1 AS connected;" --no-align --tuples-only | grep -q 1 && echo "Database connection OK" || echo "Database connection FAILED"

# Show database schema summary
db-schema:
    @psql "$DATABASE_URL" -c "\dt+" 2>/dev/null || echo "No tables found or connection failed"

# Create a test schema for development
db-test-setup:
    psql "$DATABASE_URL" -f tests/fixtures/setup.sql

# Tear down test schema
db-test-teardown:
    psql "$DATABASE_URL" -f tests/fixtures/teardown.sql

# --- Run ---

# Run inara (connects to DATABASE_URL)
run *ARGS:
    cargo run -- {{ARGS}}

# Run with debug logging
run-debug *ARGS:
    RUST_LOG=debug cargo run -- {{ARGS}}

# --- Maintenance ---

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
deps-update:
    cargo update

# Check for outdated dependencies
deps-outdated:
    cargo outdated -R 2>/dev/null || echo "Install cargo-outdated: cargo install cargo-outdated"

# Generate documentation
doc:
    cargo doc --no-deps --open
