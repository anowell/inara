use std::collections::{BTreeMap, BTreeSet};

use super::types::{Expression, PgType};
use super::{Column, Constraint, Index, Schema, Table};

/// Explicit rename metadata. The diff engine only produces rename changes
/// when this metadata is provided — it never guesses renames from add+drop pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename {
    pub table: String,
    pub from: String,
    pub to: String,
}

/// Compare two schemas and produce a list of structural changes.
///
/// The `renames` parameter provides explicit column rename metadata.
/// Without it, a renamed column appears as a drop + add pair.
///
/// Changes are ordered: drops before adds, table-level before column-level.
pub fn diff(old: &Schema, new: &Schema, renames: &[Rename]) -> Vec<Change> {
    // Index renames by (table, old_name) -> new_name
    let rename_map: BTreeMap<(&str, &str), &str> = renames
        .iter()
        .map(|r| ((r.table.as_str(), r.from.as_str()), r.to.as_str()))
        .collect();

    let mut changes = Vec::new();

    // Collect all table names from both schemas
    let old_tables: BTreeSet<&str> = old.tables.keys().map(|s| s.as_str()).collect();
    let new_tables: BTreeSet<&str> = new.tables.keys().map(|s| s.as_str()).collect();

    // Phase 1: Drop tables that no longer exist
    for &name in &old_tables {
        if !new_tables.contains(name) {
            changes.push(Change::DropTable(name.to_string()));
        }
    }

    // Phase 2: Add new tables
    for &name in &new_tables {
        if !old_tables.contains(name) {
            changes.push(Change::AddTable(new.tables[name].clone()));
        }
    }

    // Phase 3: Diff tables that exist in both schemas
    for &name in old_tables.intersection(&new_tables) {
        let old_table = &old.tables[name];
        let new_table = &new.tables[name];
        diff_table(old_table, new_table, &rename_map, &mut changes);
    }

    changes
}

/// Diff a single table: columns, constraints, then indexes.
fn diff_table(
    old: &Table,
    new: &Table,
    rename_map: &BTreeMap<(&str, &str), &str>,
    changes: &mut Vec<Change>,
) {
    diff_columns(old, new, rename_map, changes);
    diff_constraints(old, new, changes);
    diff_indexes(old, new, changes);
}

/// Diff columns within a table. Handles renames via explicit metadata.
fn diff_columns(
    old: &Table,
    new: &Table,
    rename_map: &BTreeMap<(&str, &str), &str>,
    changes: &mut Vec<Change>,
) {
    let table_name = &old.name;

    // Build column maps by name for O(1) lookup
    let old_cols: BTreeMap<&str, &Column> =
        old.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let new_cols: BTreeMap<&str, &Column> =
        new.columns.iter().map(|c| (c.name.as_str(), c)).collect();

    // Build set of new-column names that are targets of a rename in this table.
    // These should not be treated as "added" columns.
    let rename_targets: BTreeSet<&str> = rename_map
        .iter()
        .filter(|((tbl, _), _)| *tbl == table_name.as_str())
        .map(|(_, &to)| to)
        .collect();

    // Build set of old-column names that are sources of a rename in this table.
    // These should not be treated as "dropped" columns.
    let rename_sources: BTreeSet<&str> = rename_map
        .iter()
        .filter(|((tbl, _), _)| *tbl == table_name.as_str())
        .map(|((_, from), _)| *from)
        .collect();

    // Drops: old columns not in new schema (excluding rename sources)
    for &col_name in old_cols.keys() {
        if !new_cols.contains_key(col_name) && !rename_sources.contains(col_name) {
            changes.push(Change::DropColumn {
                table: table_name.clone(),
                column: col_name.to_string(),
            });
        }
    }

    // Adds: new columns not in old schema (excluding rename targets)
    for &col_name in new_cols.keys() {
        if !old_cols.contains_key(col_name) && !rename_targets.contains(col_name) {
            changes.push(Change::AddColumn {
                table: table_name.clone(),
                column: new_cols[col_name].clone(),
            });
        }
    }

    // Alters: columns present in both (by name), check for type/nullable/default changes
    for (&col_name, &old_col) in &old_cols {
        if let Some(&new_col) = new_cols.get(col_name) {
            let col_changes = diff_column(old_col, new_col, None);
            if !col_changes.is_empty() {
                changes.push(Change::AlterColumn {
                    table: table_name.clone(),
                    column: col_name.to_string(),
                    changes: col_changes,
                });
            }
        }
    }

    // Renamed columns: compare old_col with new_col at the renamed name
    for (&(tbl, from), &to) in rename_map {
        if tbl != table_name.as_str() {
            continue;
        }
        if let (Some(&old_col), Some(&new_col)) = (old_cols.get(from), new_cols.get(to)) {
            let col_changes = diff_column(old_col, new_col, Some(to.to_string()));
            // Always emit AlterColumn for renames even if nothing else changed
            changes.push(Change::AlterColumn {
                table: table_name.clone(),
                column: from.to_string(),
                changes: col_changes,
            });
        }
    }
}

/// Compare two columns and return the set of differences.
fn diff_column(old: &Column, new: &Column, rename: Option<String>) -> ColumnChanges {
    let data_type = if old.pg_type != new.pg_type {
        Some((old.pg_type.clone(), new.pg_type.clone()))
    } else {
        None
    };

    let nullable = if old.nullable != new.nullable {
        Some(new.nullable)
    } else {
        None
    };

    let default = match (&old.default, &new.default) {
        (None, None) => None,
        (Some(_), None) => Some(DefaultChange::Drop),
        (None, Some(expr)) => Some(DefaultChange::Set(expr.clone())),
        (Some(old_expr), Some(new_expr)) => {
            if old_expr != new_expr {
                Some(DefaultChange::Set(new_expr.clone()))
            } else {
                None
            }
        }
    };

    ColumnChanges {
        rename,
        data_type,
        nullable,
        default,
    }
}

/// Helper to get constraint name. Returns None for unnamed constraints.
fn constraint_name(c: &Constraint) -> Option<&str> {
    match c {
        Constraint::PrimaryKey { name, .. } => name.as_deref(),
        Constraint::Unique { name, .. } => name.as_deref(),
        Constraint::ForeignKey { name, .. } => name.as_deref(),
        Constraint::Check { name, .. } => name.as_deref(),
    }
}

/// Diff constraints within a table.
fn diff_constraints(old: &Table, new: &Table, changes: &mut Vec<Change>) {
    let table_name = &old.name;

    // Build maps of named constraints
    let old_named: BTreeMap<&str, &Constraint> = old
        .constraints
        .iter()
        .filter_map(|c| constraint_name(c).map(|n| (n, c)))
        .collect();
    let new_named: BTreeMap<&str, &Constraint> = new
        .constraints
        .iter()
        .filter_map(|c| constraint_name(c).map(|n| (n, c)))
        .collect();

    // Drop constraints that no longer exist
    for &name in old_named.keys() {
        if !new_named.contains_key(name) {
            changes.push(Change::DropConstraint {
                table: table_name.clone(),
                name: name.to_string(),
            });
        }
    }

    // Add new constraints
    for (&name, &constraint) in &new_named {
        if !old_named.contains_key(name) {
            changes.push(Change::AddConstraint {
                table: table_name.clone(),
                constraint: constraint.clone(),
            });
        }
    }

    // If a named constraint exists in both but differs, drop + re-add
    for (&name, &old_c) in &old_named {
        if let Some(&new_c) = new_named.get(name) {
            if old_c != new_c {
                changes.push(Change::DropConstraint {
                    table: table_name.clone(),
                    name: name.to_string(),
                });
                changes.push(Change::AddConstraint {
                    table: table_name.clone(),
                    constraint: new_c.clone(),
                });
            }
        }
    }
}

/// Diff indexes within a table.
fn diff_indexes(old: &Table, new: &Table, changes: &mut Vec<Change>) {
    let table_name = &old.name;

    let old_idx: BTreeMap<&str, &Index> =
        old.indexes.iter().map(|i| (i.name.as_str(), i)).collect();
    let new_idx: BTreeMap<&str, &Index> =
        new.indexes.iter().map(|i| (i.name.as_str(), i)).collect();

    // Drop indexes that no longer exist
    for &name in old_idx.keys() {
        if !new_idx.contains_key(name) {
            changes.push(Change::DropIndex(name.to_string()));
        }
    }

    // Add new indexes
    for (&name, &index) in &new_idx {
        if !old_idx.contains_key(name) {
            changes.push(Change::AddIndex {
                table: table_name.clone(),
                index: index.clone(),
            });
        }
    }

    // If an index exists in both but differs, drop + re-add
    for (&name, &old_i) in &old_idx {
        if let Some(&new_i) = new_idx.get(name) {
            if old_i != new_i {
                changes.push(Change::DropIndex(name.to_string()));
                changes.push(Change::AddIndex {
                    table: table_name.clone(),
                    index: new_i.clone(),
                });
            }
        }
    }
}

/// A structural change between two schema versions.
///
/// Produced by the diff engine when comparing `old: &Schema` vs `new: &Schema`.
/// Column-level modifications (rename, type change, nullability, default) are
/// bundled into a single `AlterColumn` variant. Renames are only produced when
/// explicit rename metadata is provided; the diff engine never guesses renames
/// from add+drop pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    AddTable(Table),
    DropTable(String),
    AddColumn {
        table: String,
        column: Column,
    },
    DropColumn {
        table: String,
        column: String,
    },
    AlterColumn {
        table: String,
        column: String,
        changes: ColumnChanges,
    },
    AddConstraint {
        table: String,
        constraint: Constraint,
    },
    DropConstraint {
        table: String,
        name: String,
    },
    AddIndex {
        table: String,
        index: Index,
    },
    DropIndex(String),
}

/// Bundled column modifications. Only set fields represent actual changes.
/// Enables generating a single `ALTER TABLE` statement per column.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnChanges {
    /// Rename the column. Contains the new name.
    pub rename: Option<String>,
    /// Change the column type. Contains (from, to) for migration context.
    pub data_type: Option<(PgType, PgType)>,
    /// Change nullability. `true` = make nullable, `false` = make not null.
    pub nullable: Option<bool>,
    /// Change or drop the default expression.
    pub default: Option<DefaultChange>,
}

impl ColumnChanges {
    /// Returns true if no changes are set.
    pub fn is_empty(&self) -> bool {
        self.rename.is_none()
            && self.data_type.is_none()
            && self.nullable.is_none()
            && self.default.is_none()
    }
}

/// A change to a column's default expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultChange {
    Set(Expression),
    Drop,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{ForeignKeyRef, ReferentialAction};

    // ── Helpers ──────────────────────────────────────────

    fn users_table() -> Table {
        let mut t = Table::new("users");
        t.add_column(Column::new("id", PgType::Uuid));
        t.add_column(Column::new("email", PgType::Text));
        t.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
        t.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        t
    }

    fn posts_table() -> Table {
        let mut t = Table::new("posts");
        t.add_column(Column::new("id", PgType::Uuid));
        t.add_column(Column::new("author_id", PgType::Uuid));
        t.add_column(Column::new("title", PgType::Text));
        t.add_constraint(Constraint::PrimaryKey {
            name: Some("posts_pkey".into()),
            columns: vec!["id".into()],
        });
        t.add_constraint(Constraint::ForeignKey {
            name: Some("posts_author_fk".into()),
            columns: vec!["author_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        t.add_index(Index {
            name: "posts_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: false,
            partial: None,
        });
        t
    }

    // ── Type tests (unchanged from original) ────────────

    #[test]
    fn change_add_table() {
        let table = Table::new("users");
        let change = Change::AddTable(table.clone());
        match &change {
            Change::AddTable(t) => assert_eq!(t.name, "users"),
            _ => panic!("expected AddTable"),
        }
    }

    #[test]
    fn change_drop_table() {
        let change = Change::DropTable("old_table".into());
        match &change {
            Change::DropTable(name) => assert_eq!(name, "old_table"),
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn change_add_column() {
        let col = Column::new("email", PgType::Text);
        let change = Change::AddColumn {
            table: "users".into(),
            column: col,
        };
        match &change {
            Change::AddColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column.name, "email");
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn change_drop_column() {
        let change = Change::DropColumn {
            table: "users".into(),
            column: "age".into(),
        };
        match &change {
            Change::DropColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "age");
            }
            _ => panic!("expected DropColumn"),
        }
    }

    #[test]
    fn alter_column_rename_only() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "name".into(),
            changes: ColumnChanges {
                rename: Some("full_name".into()),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn {
                table,
                column,
                changes,
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "name");
                assert_eq!(changes.rename.as_deref(), Some("full_name"));
                assert!(changes.data_type.is_none());
                assert!(changes.nullable.is_none());
                assert!(changes.default.is_none());
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_type_change() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "age".into(),
            changes: ColumnChanges {
                data_type: Some((PgType::Integer, PgType::BigInt)),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                let (from, to) = changes.data_type.as_ref().expect("should have type change");
                assert_eq!(*from, PgType::Integer);
                assert_eq!(*to, PgType::BigInt);
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_set_not_null() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(false),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(changes.nullable, Some(false));
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_drop_not_null() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(true),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(changes.nullable, Some(true));
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_set_default() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "created_at".into(),
            changes: ColumnChanges {
                default: Some(DefaultChange::Set(Expression::FunctionCall("now()".into()))),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(
                    changes.default,
                    Some(DefaultChange::Set(Expression::FunctionCall("now()".into())))
                );
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_drop_default() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "created_at".into(),
            changes: ColumnChanges {
                default: Some(DefaultChange::Drop),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(changes.default, Some(DefaultChange::Drop));
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_bundled_changes() {
        let changes = ColumnChanges {
            rename: Some("full_name".into()),
            data_type: Some((PgType::Varchar(Some(100)), PgType::Text)),
            nullable: Some(false),
            default: None,
        };
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "name".into(),
            changes,
        };
        match &change {
            Change::AlterColumn {
                column, changes, ..
            } => {
                assert_eq!(column, "name");
                assert_eq!(changes.rename.as_deref(), Some("full_name"));
                assert!(changes.data_type.is_some());
                assert_eq!(changes.nullable, Some(false));
                assert!(changes.default.is_none());
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn column_changes_is_empty() {
        let empty = ColumnChanges::default();
        assert!(empty.is_empty());

        let not_empty = ColumnChanges {
            nullable: Some(true),
            ..Default::default()
        };
        assert!(!not_empty.is_empty());
    }

    #[test]
    fn change_equality() {
        let a = Change::DropColumn {
            table: "users".into(),
            column: "age".into(),
        };
        let b = Change::DropColumn {
            table: "users".into(),
            column: "age".into(),
        };
        assert_eq!(a, b);
    }

    #[test]
    fn change_inequality_different_variants() {
        let a = Change::AddTable(Table::new("users"));
        let b = Change::DropTable("users".into());
        assert_ne!(a, b);
    }

    #[test]
    fn alter_column_inequality_different_changes() {
        let a = Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(false),
                ..Default::default()
            },
        };
        let b = Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(true),
                ..Default::default()
            },
        };
        assert_ne!(a, b);
    }

    // ── Diff engine tests ───────────────────────────────

    #[test]
    fn diff_identical_schemas_produces_empty() {
        let mut old = Schema::new();
        old.add_table(users_table());
        old.add_table(posts_table());
        let new = old.clone();

        let changes = diff(&old, &new, &[]);
        assert!(changes.is_empty());
    }

    #[test]
    fn diff_empty_schemas() {
        let changes = diff(&Schema::new(), &Schema::new(), &[]);
        assert!(changes.is_empty());
    }

    #[test]
    fn diff_add_table() {
        let old = Schema::new();
        let mut new = Schema::new();
        new.add_table(users_table());

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AddTable(t) => assert_eq!(t.name, "users"),
            other => panic!("expected AddTable, got {:?}", other),
        }
    }

    #[test]
    fn diff_drop_table() {
        let mut old = Schema::new();
        old.add_table(users_table());
        let new = Schema::new();

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::DropTable(name) => assert_eq!(name, "users"),
            other => panic!("expected DropTable, got {:?}", other),
        }
    }

    #[test]
    fn diff_add_column_to_existing_table() {
        let mut old = Schema::new();
        old.add_table(users_table());

        let mut new = Schema::new();
        let mut new_users = users_table();
        new_users.add_column(Column::new("bio", PgType::Text).nullable());
        new.add_table(new_users);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AddColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column.name, "bio");
                assert_eq!(column.pg_type, PgType::Text);
                assert!(column.nullable);
            }
            other => panic!("expected AddColumn, got {:?}", other),
        }
    }

    #[test]
    fn diff_drop_column() {
        let mut old = Schema::new();
        let mut old_users = users_table();
        old_users.add_column(Column::new("bio", PgType::Text).nullable());
        old.add_table(old_users);

        let mut new = Schema::new();
        new.add_table(users_table());

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::DropColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "bio");
            }
            other => panic!("expected DropColumn, got {:?}", other),
        }
    }

    #[test]
    fn diff_change_column_type() {
        let mut old = Schema::new();
        let mut old_users = Table::new("users");
        old_users.add_column(Column::new("age", PgType::Integer));
        old.add_table(old_users);

        let mut new = Schema::new();
        let mut new_users = Table::new("users");
        new_users.add_column(Column::new("age", PgType::BigInt));
        new.add_table(new_users);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AlterColumn {
                table,
                column,
                changes,
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "age");
                let (from, to) = changes.data_type.as_ref().expect("should have type change");
                assert_eq!(*from, PgType::Integer);
                assert_eq!(*to, PgType::BigInt);
                assert!(changes.nullable.is_none());
                assert!(changes.default.is_none());
                assert!(changes.rename.is_none());
            }
            other => panic!("expected AlterColumn, got {:?}", other),
        }
    }

    #[test]
    fn diff_toggle_nullability() {
        let mut old = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("email", PgType::Text)); // NOT NULL
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("email", PgType::Text).nullable()); // NULL
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AlterColumn {
                column, changes, ..
            } => {
                assert_eq!(column, "email");
                assert_eq!(changes.nullable, Some(true));
                assert!(changes.data_type.is_none());
                assert!(changes.default.is_none());
            }
            other => panic!("expected AlterColumn, got {:?}", other),
        }
    }

    #[test]
    fn diff_change_default_expression() {
        let mut old = Schema::new();
        let mut t = Table::new("users");
        t.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("users");
        t.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::Raw("CURRENT_TIMESTAMP".into())),
        );
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(
                    changes.default,
                    Some(DefaultChange::Set(Expression::Raw(
                        "CURRENT_TIMESTAMP".into()
                    )))
                );
            }
            other => panic!("expected AlterColumn, got {:?}", other),
        }
    }

    #[test]
    fn diff_add_default() {
        let mut old = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("status", PgType::Text));
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("users");
        t.add_column(
            Column::new("status", PgType::Text)
                .with_default(Expression::Literal("'active'".into())),
        );
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(
                    changes.default,
                    Some(DefaultChange::Set(Expression::Literal("'active'".into())))
                );
            }
            other => panic!("expected AlterColumn, got {:?}", other),
        }
    }

    #[test]
    fn diff_drop_default() {
        let mut old = Schema::new();
        let mut t = Table::new("users");
        t.add_column(
            Column::new("status", PgType::Text)
                .with_default(Expression::Literal("'active'".into())),
        );
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("status", PgType::Text));
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(changes.default, Some(DefaultChange::Drop));
            }
            other => panic!("expected AlterColumn, got {:?}", other),
        }
    }

    #[test]
    fn diff_add_fk_constraint() {
        let mut old = Schema::new();
        let mut old_posts = Table::new("posts");
        old_posts.add_column(Column::new("id", PgType::Uuid));
        old_posts.add_column(Column::new("author_id", PgType::Uuid));
        old.add_table(old_posts);

        let mut new = Schema::new();
        let mut new_posts = Table::new("posts");
        new_posts.add_column(Column::new("id", PgType::Uuid));
        new_posts.add_column(Column::new("author_id", PgType::Uuid));
        new_posts.add_constraint(Constraint::ForeignKey {
            name: Some("posts_author_fk".into()),
            columns: vec!["author_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        new.add_table(new_posts);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AddConstraint {
                table, constraint, ..
            } => {
                assert_eq!(table, "posts");
                match constraint {
                    Constraint::ForeignKey {
                        name, references, ..
                    } => {
                        assert_eq!(name.as_deref(), Some("posts_author_fk"));
                        assert_eq!(references.table, "users");
                    }
                    _ => panic!("expected ForeignKey constraint"),
                }
            }
            other => panic!("expected AddConstraint, got {:?}", other),
        }
    }

    #[test]
    fn diff_drop_fk_constraint() {
        let mut old = Schema::new();
        let mut old_posts = Table::new("posts");
        old_posts.add_column(Column::new("id", PgType::Uuid));
        old_posts.add_column(Column::new("author_id", PgType::Uuid));
        old_posts.add_constraint(Constraint::ForeignKey {
            name: Some("posts_author_fk".into()),
            columns: vec!["author_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        old.add_table(old_posts);

        let mut new = Schema::new();
        let mut new_posts = Table::new("posts");
        new_posts.add_column(Column::new("id", PgType::Uuid));
        new_posts.add_column(Column::new("author_id", PgType::Uuid));
        new.add_table(new_posts);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::DropConstraint { table, name } => {
                assert_eq!(table, "posts");
                assert_eq!(name, "posts_author_fk");
            }
            other => panic!("expected DropConstraint, got {:?}", other),
        }
    }

    #[test]
    fn diff_add_index() {
        let mut old = Schema::new();
        let mut t = Table::new("posts");
        t.add_column(Column::new("author_id", PgType::Uuid));
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("posts");
        t.add_column(Column::new("author_id", PgType::Uuid));
        t.add_index(Index {
            name: "posts_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: false,
            partial: None,
        });
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AddIndex { table, index } => {
                assert_eq!(table, "posts");
                assert_eq!(index.name, "posts_author_idx");
                assert_eq!(index.columns, vec!["author_id"]);
            }
            other => panic!("expected AddIndex, got {:?}", other),
        }
    }

    #[test]
    fn diff_drop_index() {
        let mut old = Schema::new();
        let mut t = Table::new("posts");
        t.add_column(Column::new("author_id", PgType::Uuid));
        t.add_index(Index {
            name: "posts_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: false,
            partial: None,
        });
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("posts");
        t.add_column(Column::new("author_id", PgType::Uuid));
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::DropIndex(name) => assert_eq!(name, "posts_author_idx"),
            other => panic!("expected DropIndex, got {:?}", other),
        }
    }

    #[test]
    fn diff_explicit_rename_with_metadata() {
        let mut old = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("name", PgType::Varchar(Some(100))));
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("full_name", PgType::Varchar(Some(100))));
        new.add_table(t);

        let renames = vec![Rename {
            table: "users".into(),
            from: "name".into(),
            to: "full_name".into(),
        }];

        let changes = diff(&old, &new, &renames);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AlterColumn {
                table,
                column,
                changes,
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "name");
                assert_eq!(changes.rename.as_deref(), Some("full_name"));
                // Same type, so no data_type change
                assert!(changes.data_type.is_none());
            }
            other => panic!("expected AlterColumn with rename, got {:?}", other),
        }
    }

    #[test]
    fn diff_column_rename_without_metadata_produces_drop_add() {
        let mut old = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("name", PgType::Text));
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("full_name", PgType::Text));
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        // Should produce a drop and an add, NOT an AlterColumn with rename
        assert_eq!(changes.len(), 2);

        let has_drop = changes.iter().any(|c| {
            matches!(c, Change::DropColumn { table, column }
                if table == "users" && column == "name")
        });
        let has_add = changes.iter().any(|c| {
            matches!(c, Change::AddColumn { table, column }
                if table == "users" && column.name == "full_name")
        });
        assert!(has_drop, "should have DropColumn for 'name'");
        assert!(has_add, "should have AddColumn for 'full_name'");

        // Verify no rename was produced
        let has_rename = changes
            .iter()
            .any(|c| matches!(c, Change::AlterColumn { changes, .. } if changes.rename.is_some()));
        assert!(!has_rename, "should not produce rename without metadata");
    }

    #[test]
    fn diff_rename_with_type_change() {
        let mut old = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("name", PgType::Varchar(Some(100))));
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("full_name", PgType::Text));
        new.add_table(t);

        let renames = vec![Rename {
            table: "users".into(),
            from: "name".into(),
            to: "full_name".into(),
        }];

        let changes = diff(&old, &new, &renames);
        assert_eq!(changes.len(), 1);
        match &changes[0] {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(changes.rename.as_deref(), Some("full_name"));
                let (from, to) = changes.data_type.as_ref().expect("should have type change");
                assert_eq!(*from, PgType::Varchar(Some(100)));
                assert_eq!(*to, PgType::Text);
            }
            other => panic!("expected AlterColumn, got {:?}", other),
        }
    }

    #[test]
    fn diff_multiple_changes_across_tables() {
        let mut old = Schema::new();
        old.add_table(users_table());
        old.add_table(posts_table());

        let mut new = Schema::new();

        // Modify users: add a column, change email nullability
        let mut new_users = Table::new("users");
        new_users.add_column(Column::new("id", PgType::Uuid));
        new_users.add_column(Column::new("email", PgType::Text).nullable()); // was NOT NULL
        new_users.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
        new_users.add_column(Column::new("bio", PgType::Text).nullable()); // new column
        new_users.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        new.add_table(new_users);

        // Modify posts: drop the FK constraint, change index
        let mut new_posts = Table::new("posts");
        new_posts.add_column(Column::new("id", PgType::Uuid));
        new_posts.add_column(Column::new("author_id", PgType::Uuid));
        new_posts.add_column(Column::new("title", PgType::Text));
        new_posts.add_constraint(Constraint::PrimaryKey {
            name: Some("posts_pkey".into()),
            columns: vec!["id".into()],
        });
        // No FK constraint (dropped)
        // Different index
        new_posts.add_index(Index {
            name: "posts_title_idx".into(),
            columns: vec!["title".into()],
            unique: false,
            partial: None,
        });
        new.add_table(new_posts);

        let changes = diff(&old, &new, &[]);

        // Expected: users.email nullable change, users.bio added,
        //           posts FK dropped, posts old index dropped, posts new index added
        assert!(!changes.is_empty());

        let has_nullable_change = changes.iter().any(|c| {
            matches!(c, Change::AlterColumn { table, column, changes }
                if table == "users" && column == "email" && changes.nullable == Some(true))
        });
        assert!(has_nullable_change, "should detect nullable change");

        let has_bio_add = changes.iter().any(|c| {
            matches!(c, Change::AddColumn { table, column }
                if table == "users" && column.name == "bio")
        });
        assert!(has_bio_add, "should detect new bio column");

        let has_fk_drop = changes.iter().any(|c| {
            matches!(c, Change::DropConstraint { table, name }
                if table == "posts" && name == "posts_author_fk")
        });
        assert!(has_fk_drop, "should detect FK drop");

        let has_old_idx_drop = changes
            .iter()
            .any(|c| matches!(c, Change::DropIndex(name) if name == "posts_author_idx"));
        assert!(has_old_idx_drop, "should detect old index drop");

        let has_new_idx_add = changes.iter().any(|c| {
            matches!(c, Change::AddIndex { table, index }
                if table == "posts" && index.name == "posts_title_idx")
        });
        assert!(has_new_idx_add, "should detect new index add");
    }

    #[test]
    fn diff_drops_before_adds_at_table_level() {
        let mut old = Schema::new();
        old.add_table(Table::new("alpha"));

        let mut new = Schema::new();
        new.add_table(Table::new("beta"));

        let changes = diff(&old, &new, &[]);
        assert_eq!(changes.len(), 2);
        assert!(
            matches!(&changes[0], Change::DropTable(name) if name == "alpha"),
            "drops should come before adds"
        );
        assert!(
            matches!(&changes[1], Change::AddTable(t) if t.name == "beta"),
            "adds should come after drops"
        );
    }

    #[test]
    fn diff_constraint_modification_produces_drop_and_add() {
        let mut old = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("id", PgType::Uuid));
        t.add_column(Column::new("email", PgType::Text));
        t.add_constraint(Constraint::Unique {
            name: Some("users_email_key".into()),
            columns: vec!["email".into()],
        });
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("users");
        t.add_column(Column::new("id", PgType::Uuid));
        t.add_column(Column::new("email", PgType::Text));
        // Same constraint name, but now includes id too
        t.add_constraint(Constraint::Unique {
            name: Some("users_email_key".into()),
            columns: vec!["id".into(), "email".into()],
        });
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        // Should drop the old constraint and add the new one
        let has_drop = changes
            .iter()
            .any(|c| matches!(c, Change::DropConstraint { name, .. } if name == "users_email_key"));
        let has_add = changes.iter().any(|c| {
            matches!(c, Change::AddConstraint { constraint, .. }
                if matches!(constraint, Constraint::Unique { columns, .. } if columns.len() == 2))
        });
        assert!(has_drop, "should drop the modified constraint");
        assert!(has_add, "should re-add the modified constraint");
    }

    #[test]
    fn diff_index_modification_produces_drop_and_add() {
        let mut old = Schema::new();
        let mut t = Table::new("posts");
        t.add_column(Column::new("author_id", PgType::Uuid));
        t.add_index(Index {
            name: "posts_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: false,
            partial: None,
        });
        old.add_table(t);

        let mut new = Schema::new();
        let mut t = Table::new("posts");
        t.add_column(Column::new("author_id", PgType::Uuid));
        t.add_index(Index {
            name: "posts_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: true, // Changed to unique
            partial: None,
        });
        new.add_table(t);

        let changes = diff(&old, &new, &[]);
        let has_drop = changes
            .iter()
            .any(|c| matches!(c, Change::DropIndex(name) if name == "posts_author_idx"));
        let has_add = changes.iter().any(|c| {
            matches!(c, Change::AddIndex { index, .. } if index.name == "posts_author_idx" && index.unique)
        });
        assert!(has_drop, "should drop modified index");
        assert!(has_add, "should re-add modified index");
    }
}
