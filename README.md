# Inara

**A companion to sqlx. Elegant, fast, schema-first.**

Inara is a terminal-native schema explorer and migration generator for [sqlx](https://github.com/launchbadge/sqlx) + Postgres. It treats your database like a navigable codebase — not a GUI admin panel, not an ORM.

## What Inara Does

- **Navigate** tables, columns, indexes, and relations like symbols in an editor
- **Edit** schema structure in declarative SQL-like blocks
- **Generate** precise `.up.sql` migrations via structural diff
- **Browse** migration history linked to schema elements
- **Preview** pending migrations as a virtual overlay

## What Inara Is Not

- Not a query client or SQL editor
- Not an ORM (use sqlx directly)
- Not a GUI admin tool (it's a TUI)
- Not a full Postgres schema manager (covers 90% cleanly)

## Usage

### Prerequisites

- Rust toolchain (stable)
- PostgreSQL database
- [just](https://github.com/casey/just) command runner

### Setup

```sh
# Clone and build
just build

# Set your database connection
export DATABASE_URL="postgres://user:pass@localhost/mydb"
# Or add to .env file (auto-loaded by justfile)

# Verify database connectivity
just db-check
```

### Running

```sh
# Launch Inara connected to your database
just run

# With debug logging
just run-debug
```

### Key Bindings (Helix-inspired)

| Key | Action |
|-----|--------|
| `j`/`k` | Move down/up |
| `gg`/`G` | Jump to top/bottom |
| `Enter` | Expand/collapse table |
| `Space` | Open command menu |
| `Space t` | Fuzzy search tables |
| `Space c` | Fuzzy search columns |
| `Space m` | Fuzzy search migrations |
| `g r` | Goto incoming references |
| `g o` | Goto outgoing references |
| `g i` | Goto indexes |
| `g m` | Goto migrations affecting element |
| `q` | Query HUD (safe data glance) |
| `e` | Enter edit mode |
| `r` | Rename (explicit) |
| `:w` | Write migration from edits |
| `:q` | Quit |

### Development

```sh
# Run all checks (format, lint, test)
just check

# Run unit tests
just test

# Run integration tests (requires DATABASE_URL)
just test-integration

# Format code
just fmt
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## License

TBD
