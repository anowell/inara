# Contributing to Inara

This project is developed primarily by AI agents working from beads (work items). This guide ensures all contributors — human or agent — produce consistent, high-quality work.

## Core Principles

1. **Prove it works.** Every bead requires tests that demonstrate correctness. "It compiles" is not proof. Show that behavior matches expectations with assertions.
2. **Helix is the north star.** Navigation, keybinding philosophy, and UX patterns should feel like helix. When in doubt, do what helix does.
3. **Companion to sqlx, not a replacement.** Use sqlx for database access. Don't reimplement what sqlx already does well.
4. **Declarative, not procedural.** Schema is declared, not mutated. Migrations are generated from structural diffs, not hand-written.
5. **Explicit over implicit.** Especially for destructive operations like renames. No heuristic guessing.

## Development Setup

### Prerequisites

- Rust stable toolchain
- PostgreSQL (with a database you can connect to)
- [just](https://github.com/casey/just) command runner
- Database connection configured in `.env` (see `.env.example`)

### Quick Start

```sh
just build          # Build the project
just check          # Format + lint + test (run before every commit)
just test           # Unit tests only
just test-integration  # Integration tests (needs DATABASE_URL)
```

### Environment

The justfile uses `set dotenv-load` to automatically load `.env`. Required variables:

- `DATABASE_URL` — Postgres connection string (used by sqlx and integration tests)

Alternative individual variables are also supported: `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`.

## Code Standards

### Rust Style

- **Format:** Always run `cargo fmt`. CI will reject unformatted code.
- **Lint:** `cargo clippy -- -D warnings`. Zero warnings policy.
- **Errors:** Use `thiserror` for library errors, `anyhow` or `color-eyre` for application-level errors. No `.unwrap()` in library code.
- **Types:** Prefer strong types over stringly-typed APIs. Use newtypes where semantics differ (e.g., `TableName(String)` vs raw `String`).

### Architecture Patterns

- **State management:** Immutable state transitions. The app state is a value that gets replaced, not mutated in place.
- **Rendering:** Diff-driven via ratatui. Components render from state; they don't own state.
- **Schema model:** The `Schema` struct is the single source of truth. All features operate on it.
- **Database access:** All Postgres queries go through sqlx. Query `pg_catalog` and `information_schema` for introspection.
- **Ordering:** Use `BTreeMap` for deterministic ordering of schema elements.

### Testing Requirements

Every bead has an acceptance criteria section. Tests must cover those criteria. Here's the testing hierarchy:

1. **Unit tests** (`#[cfg(test)]` modules) — Test pure logic: schema model operations, diff algorithms, type mappings, parsing.
2. **Integration tests** (`tests/` directory, `#[ignore]`) — Test database-dependent features against real Postgres. Mark with `#[ignore]` so `cargo test` skips them; `just test-integration` runs them.
3. **Snapshot tests** — Use `insta` for rendering output. Schema text rendering, migration SQL output, and TUI frame captures should use snapshot assertions.
4. **Property tests** — Use `proptest` where applicable (e.g., parser round-trips: render → parse → render = identity).

**Proving it works means:**
- `just check` passes (format + lint + all unit tests)
- `just test-integration` passes (if bead touches database)
- Tests assert *behavior*, not just "doesn't crash"
- Edge cases are covered (empty schema, tables with no columns, circular FK references, etc.)

### File Organization

```
src/
  main.rs          — Entry point, CLI argument parsing
  lib.rs           — Library root, re-exports
  schema/
    mod.rs         — Schema model types (Schema, Table, Column, etc.)
    introspect.rs  — Postgres introspection queries
    diff.rs        — Structural diff engine
    render.rs      — Declarative text rendering
    parse.rs       — Declarative text parser
    types.rs       — PG→Rust type mapping
  tui/
    mod.rs         — TUI app entry, event loop
    app.rs         — Application state machine
    view.rs        — Main schema document view
    input.rs       — Key event handling, mode dispatch
    fuzzy.rs       — Fuzzy search overlay
    hud.rs         — Query HUD overlay
    widgets/       — Reusable ratatui widgets
  migration/
    mod.rs         — Migration generation
    loader.rs      — Migration file loading + indexing
    overlay.rs     — Pending migration overlay
    warnings.rs    — Fallible change warnings
tests/
  fixtures/        — SQL setup/teardown scripts, golden files
  integration/     — Integration tests (marked #[ignore])
docs/
  architecture.md  — High-level architecture overview
  navigation.md    — Navigation model reference
  schema-model.md  — Schema model design decisions
```

### Crate Selection

Use well-maintained, popular crates. Preferred choices:

| Purpose | Crate |
|---------|-------|
| TUI framework | `ratatui` |
| Terminal backend | `crossterm` |
| Database | `sqlx` (postgres feature) |
| Error handling | `thiserror`, `color-eyre` |
| Serialization | `serde`, `serde_json` |
| CLI parsing | `clap` |
| Fuzzy matching | `nucleo` or `fuzzy-matcher` |
| Parsing | `winnow` or `nom` |
| Snapshot testing | `insta` |
| Property testing | `proptest` |
| Logging | `tracing` |
| Async runtime | `tokio` |

## Workflow

### Before Starting a Bead

1. Read the bead description and acceptance criteria (`bd show <id>`)
2. Check dependencies are complete (`bd dep list <id>`)
3. Read relevant `docs/` files for architectural context
4. Update bead status: `bd update <id> --status in_progress`

### While Working

- Make small, logical commits with `jj commit -m "description"`
- Run `just check` frequently
- If blocked, update the bead: `bd update <id> --status blocked` and `bd comment add "reason"`

### Completing a Bead

1. All acceptance criteria met (verified by tests)
2. `just check` passes
3. `just test-integration` passes (if applicable)
4. No new clippy warnings
5. Add summary comment: `bd comment add "Summary: ..."`
6. Update bead: `bd update <id> --assignee anthony`
7. Commit: `jj commit -m "<bead-id>: <brief description>"`

### What NOT to Do

- Don't write docs for internal implementation details in `docs/`. Docs are for cross-cutting alignment only. Put implementation docs in code comments.
- Don't add features beyond the bead's acceptance criteria.
- Don't refactor adjacent code "while you're in there."
- Don't add dependencies without justification.
- Don't skip tests to "finish faster."
- Don't use `unwrap()` in non-test code.
