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
    Real, DoublePrecision, Numeric,
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
    AlterColumnType { table: String, column: String, from: PgType, to: PgType },
    SetNotNull { table: String, column: String },
    DropNotNull { table: String, column: String },
    SetDefault { table: String, column: String, default: Expression },
    DropDefault { table: String, column: String },
    AddConstraint { table: String, constraint: Constraint },
    DropConstraint { table: String, name: String },
    AddIndex(Index),
    DropIndex(String),
    RenameColumn { table: String, from: String, to: String },
}
```

Rename is only produced when explicit rename metadata is provided. The diff engine never guesses renames from add+drop pairs.
