# Inara — Agent Instructions

Inara is a terminal-native schema explorer and migration generator for sqlx + Postgres, written in Rust.

## Before Starting Work

Run `bd onboard` to understand the current project state and available issues.

## Essential Reading

- **[CONTRIBUTING.md](CONTRIBUTING.md)** — Development standards, testing requirements, architecture patterns, and workflow. Read this first.
- **[docs/architecture.md](docs/architecture.md)** — High-level architecture overview.
- **[docs/navigation.md](docs/navigation.md)** — Helix-inspired navigation model reference.
- **[docs/schema-model.md](docs/schema-model.md)** — Schema model design decisions.

## Key Commands

```sh
just check            # Format + lint + test (MUST pass before completing any bead)
just test             # Unit tests
just test-integration # Integration tests (requires DATABASE_URL)
just db-check         # Verify database connectivity
```

## Rules

- Every bead must be proven with tests. "It compiles" is not done.
- `just check` must pass before marking any bead complete.
- Use `jj` for version control, never `git` directly.
- Use `bd` to track work status and add completion summaries.
- Follow helix editor patterns for all navigation and UX decisions.
- Use sqlx for all database access. Query pg_catalog for introspection.
- Schema model uses BTreeMap for deterministic ordering.
- Immutable state transitions for app state management.
- No `.unwrap()` in library code.
