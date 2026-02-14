# Architecture Overview

Inara is structured as three layers that compose through a shared schema model.

## Layers

```
┌─────────────────────────────────────┐
│           TUI Layer                 │  ratatui + crossterm
│  (input, rendering, navigation)     │  Event loop, views, overlays
├─────────────────────────────────────┤
│         Schema Layer                │  Core domain model
│  (model, diff, render, parse)       │  Pure Rust, no IO
├─────────────────────────────────────┤
│        Data Layer                   │  sqlx + tokio
│  (introspection, queries, migrations)│ Postgres access
└─────────────────────────────────────┘
```

### Schema Layer (pure, testable core)

The `Schema` struct is the single source of truth. It's a normalized structural model — not a SQL AST. All features operate on it:

- **Rendering** converts Schema → declarative SQL-like text blocks
- **Parsing** converts declarative text → Schema (round-trips with rendering)
- **Diffing** compares two Schema values → list of `Change` operations
- **Type mapping** annotates columns with Rust type equivalents

This layer has zero IO dependencies. It's pure data transformation, fully unit-testable.

### Data Layer (async, database-dependent)

- **Introspection** queries `pg_catalog` + `information_schema` via sqlx → builds Schema
- **Query HUD** runs safe, bounded queries for data glances
- **Migration loader** reads `.sql` files from the migrations directory
- **Fallible warnings** check live data before writing migrations

### TUI Layer (presentation, input handling)

- **App state machine** manages modes (Normal, Edit, Search, HUD)
- **Event loop** dispatches crossterm events to the current mode handler
- **Views** render state to ratatui frames — they don't own state
- **Overlays** (fuzzy search, HUD, space menu) compose over the main view

## State Management

App state is immutable. Actions produce new state:

```
Event → Action → State transition → Render
```

No mutable references to shared state. Components receive `&AppState` for rendering and return `Action` values from input handlers.

## Data Flow

```
Postgres ──introspect──→ Schema ──render──→ TUI Document
                            │
                            ├──edit──→ Edited Schema
                            │              │
                            └──diff──→ Changes ──generate──→ Migration SQL
```

## Key Design Decisions

1. **BTreeMap over HashMap** — Deterministic ordering for schema elements. Tables, columns, and constraints always appear in the same order.

2. **Structural diff, not text diff** — Comparing two Schema values produces typed Change operations. Never diff rendered text.

3. **Explicit renames** — The diff engine does not guess renames. Users must explicitly invoke rename, which records metadata consumed by the diff.

4. **No full SQL parser** — The declarative schema format uses a small, purpose-built parser. Migration replay uses `pg_query` crate for robust SQL parsing if needed.

5. **sqlx for everything DB** — No raw `tokio-postgres`. sqlx provides compile-time query checking and type safety.
