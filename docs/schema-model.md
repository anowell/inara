# Schema Model

The schema model is Inara's core data structure. Every feature operates on it. This document captures key design decisions for alignment across beads.

## Structure

```rust
Schema {
    tables: BTreeMap<String, Table>,
    enums: BTreeMap<String, EnumType>,
    types: BTreeMap<String, CustomType>,
}

Table {
    name: String,
    columns: Vec<Column>,       // Ordered as in pg_catalog
    constraints: Vec<Constraint>,
    indexes: Vec<Index>,
}

Column {
    name: String,
    pg_type: PgType,
    nullable: bool,
    default: Option<Expression>,
}
```

## Key Decisions

### BTreeMap for Top-Level Collections

Tables, enums, and custom types are stored in `BTreeMap<String, T>`. This gives:
- Deterministic iteration order (alphabetical by name)
- O(log n) lookup by name
- Consistent rendering regardless of introspection order

### Vec for Table Members

Columns use `Vec` (not BTreeMap) because column order matters — it reflects the physical table definition. Constraints and indexes also use `Vec` since they may share names or be anonymous.

### PgType Enum

`PgType` is a Rust enum covering common Postgres types, with a `Custom(String)` variant for anything else. This avoids stringly-typed comparisons while remaining extensible.

```rust
enum PgType {
    Boolean,
    SmallInt, Integer, BigInt,
    Real, DoublePrecision, Numeric(Option<(u32, u32)>),
    Text, Varchar(Option<u32>), Char(Option<u32>),
    Bytea,
    Uuid,
    Timestamp, Timestamptz,
    Date, Time, Timetz,
    Interval,
    Json, Jsonb,
    Array(Box<PgType>),
    Custom(String),  // enums, domains, composite types
}
```

### Expression for Defaults

Column defaults are stored as `Expression`, not raw strings. This enables structural comparison during diffing.

```rust
enum Expression {
    Literal(String),        // '42', 'hello'
    FunctionCall(String),   // now(), gen_random_uuid()
    Raw(String),            // Anything we can't parse structurally
}
```

The `Raw` variant is the escape hatch — if we can't parse a default expression, we preserve it as-is. Diffing two `Raw` values falls back to string comparison.

### Constraint Modeling

Constraints are an enum rather than separate collections:

```rust
enum Constraint {
    PrimaryKey { name: Option<String>, columns: Vec<String> },
    Unique { name: Option<String>, columns: Vec<String> },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        references: ForeignKeyRef,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Check { name: Option<String>, expression: String },
}
```

All constraint variants include `name: Option<String>` because Postgres assigns names to constraints (e.g., `users_pkey`), and we need them for `DropConstraint` and migration generation. The name is `Option` because constraints can also be anonymous (inline column constraints without explicit naming).

This keeps all constraints in a single list on the table, matching how Postgres reports them.

### Precomputed Relation Maps

For O(1) navigation, the app precomputes:

```rust
struct RelationMap {
    // table_name -> Vec<(source_table, fk_constraint)>
    incoming_fks: BTreeMap<String, Vec<ForeignKeyInfo>>,
    // table_name -> Vec<(target_table, fk_constraint)>
    outgoing_fks: BTreeMap<String, Vec<ForeignKeyInfo>>,
    // column identifier -> Vec<Index>
    column_indexes: BTreeMap<(String, String), Vec<String>>,
}
```

This is built once from a `Schema` and rebuilt when schema changes. Navigation uses the relation map, never scans the full schema.

## Diff Operations

The diff engine compares `old: &Schema` vs `new: &Schema` and produces `Vec<Change>`:

```rust
enum Change {
    AddTable(Table),
    DropTable(String),
    AddColumn { table: String, column: Column },
    DropColumn { table: String, column: String },
    AlterColumn { table: String, column: String, changes: ColumnChanges },
    AddConstraint { table: String, constraint: Constraint },
    DropConstraint { table: String, name: String },
    AddIndex { table: String, index: Index },
    DropIndex(String),
}
```

### Bundled Column Changes

Column-level modifications are bundled into a single `AlterColumn` variant via `ColumnChanges`. This groups related edits (rename + type change + nullability) into one change, enabling generation of a single `ALTER TABLE` statement per column.

```rust
struct ColumnChanges {
    rename: Option<String>,              // new name
    data_type: Option<(PgType, PgType)>, // (from, to)
    nullable: Option<bool>,              // true = nullable, false = not null
    default: Option<DefaultChange>,
}

enum DefaultChange {
    Set(Expression),
    Drop,
}
```

Rename is only produced when explicit rename metadata is provided. The diff engine never guesses renames from add+drop pairs.

## Migration Generation

Migration generation is disabled when there are pending (unapplied) migrations. Inara introspects the live database schema and generates migrations from the structural diff between the current schema and the user's edits. If pending migrations exist, the live schema does not reflect the intended state, so generating additional migrations could produce conflicts.

### Write Commands

- `:w` — Generate migration with safety checks. If the diff contains potentially destructive changes (e.g., adding NOT NULL to a column with NULL rows), a dialog presents options: cancel (to go back and add a default), accept (write as-is), or use AI to generate a data migration (if configured).
- `:w!` — Generate migration without confirmation. Skips the safety dialog and writes immediately.
- `:w <desc>` / `:w! <desc>` — Same as above, with a description for the migration filename.
