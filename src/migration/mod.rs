// Migration generation — structural diff to SQL.

pub mod warnings;

use std::fmt::Write;
use std::path::{Path, PathBuf};

use crate::schema::diff::{Change, ColumnChanges, DefaultChange};
use crate::schema::{Column, Constraint, Index, Table};

/// Generate SQL statements from a list of structural changes.
///
/// Changes are reordered for correctness:
/// - DROP INDEX first (before tables that own them might be dropped)
/// - DROP CONSTRAINT (FKs must go before referenced tables are dropped)
/// - DROP COLUMN
/// - DROP TABLE
/// - CREATE TABLE (before FKs that reference them)
/// - ADD COLUMN
/// - ALTER COLUMN
/// - ADD CONSTRAINT (after tables and columns exist)
/// - CREATE INDEX (last, after tables are ready)
pub fn generate_sql(changes: &[Change]) -> String {
    let ordered = order_changes(changes);
    let mut out = String::new();

    for (i, change) in ordered.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        write_change(&mut out, change);
    }

    out
}

/// Write a migration file with a timestamped name.
///
/// Returns the path to the created file. The `timestamp` parameter allows
/// tests to inject a fixed value for determinism.
pub fn write_migration(
    dir: &Path,
    description: &str,
    sql: &str,
    timestamp: &str,
) -> std::io::Result<PathBuf> {
    let slug = slugify(description);
    let filename = format!("{timestamp}_{slug}.up.sql");
    let path = dir.join(filename);
    std::fs::write(&path, sql)?;
    Ok(path)
}

/// Convert a description to a filename-safe slug.
fn slugify(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

/// Reorder changes for safe DDL execution.
fn order_changes(changes: &[Change]) -> Vec<&Change> {
    let mut drop_indexes = Vec::new();
    let mut drop_constraints = Vec::new();
    let mut drop_columns = Vec::new();
    let mut drop_tables = Vec::new();
    let mut add_tables = Vec::new();
    let mut add_columns = Vec::new();
    let mut alter_columns = Vec::new();
    let mut add_constraints = Vec::new();
    let mut add_indexes = Vec::new();

    for change in changes {
        match change {
            Change::DropIndex(_) => drop_indexes.push(change),
            Change::DropConstraint { .. } => drop_constraints.push(change),
            Change::DropColumn { .. } => drop_columns.push(change),
            Change::DropTable(_) => drop_tables.push(change),
            Change::AddTable(_) => add_tables.push(change),
            Change::AddColumn { .. } => add_columns.push(change),
            Change::AlterColumn { .. } => alter_columns.push(change),
            Change::AddConstraint { .. } => add_constraints.push(change),
            Change::AddIndex { .. } => add_indexes.push(change),
        }
    }

    let mut ordered = Vec::with_capacity(changes.len());
    ordered.extend(drop_indexes);
    ordered.extend(drop_constraints);
    ordered.extend(drop_columns);
    ordered.extend(drop_tables);
    ordered.extend(add_tables);
    ordered.extend(add_columns);
    ordered.extend(alter_columns);
    ordered.extend(add_constraints);
    ordered.extend(add_indexes);
    ordered
}

/// Emit SQL for a single change.
fn write_change(out: &mut String, change: &Change) {
    match change {
        Change::AddTable(table) => write_create_table(out, table),
        Change::DropTable(name) => {
            let _ = writeln!(out, "DROP TABLE {name};");
        }
        Change::AddColumn { table, column } => write_add_column(out, table, column),
        Change::DropColumn { table, column } => {
            let _ = writeln!(out, "ALTER TABLE {table} DROP COLUMN {column};");
        }
        Change::AlterColumn {
            table,
            column,
            changes,
        } => write_alter_column(out, table, column, changes),
        Change::AddConstraint { table, constraint } => {
            write_add_constraint(out, table, constraint);
        }
        Change::DropConstraint { table, name } => {
            let _ = writeln!(out, "ALTER TABLE {table} DROP CONSTRAINT {name};");
        }
        Change::AddIndex { table, index } => write_create_index(out, table, index),
        Change::DropIndex(name) => {
            let _ = writeln!(out, "DROP INDEX {name};");
        }
    }
}

/// Emit CREATE TABLE with columns and inline constraints.
fn write_create_table(out: &mut String, table: &Table) {
    let _ = writeln!(out, "CREATE TABLE {} (", table.name);

    let mut parts: Vec<String> = Vec::new();

    for col in &table.columns {
        parts.push(format_column_def(col));
    }

    for constraint in &table.constraints {
        parts.push(format_constraint(constraint));
    }

    for (i, part) in parts.iter().enumerate() {
        let comma = if i + 1 < parts.len() { "," } else { "" };
        let _ = writeln!(out, "    {part}{comma}");
    }

    let _ = writeln!(out, ");");

    // Indexes are separate CREATE INDEX statements after the table
    for index in &table.indexes {
        write_create_index(out, &table.name, index);
    }
}

/// Format a column definition for CREATE TABLE or ADD COLUMN.
fn format_column_def(col: &Column) -> String {
    let mut def = format!("{} {}", col.name, col.pg_type);

    if !col.nullable {
        def.push_str(" NOT NULL");
    }

    if let Some(ref expr) = col.default {
        let _ = write!(def, " DEFAULT {expr}");
    }

    def
}

/// Emit ALTER TABLE ADD COLUMN.
fn write_add_column(out: &mut String, table: &str, col: &Column) {
    let def = format_column_def(col);
    let _ = writeln!(out, "ALTER TABLE {table} ADD COLUMN {def};");
}

/// Emit ALTER TABLE statements for column modifications.
///
/// Bundled changes are split into separate ALTER statements in the correct order:
/// 1. Rename (must happen first so subsequent statements use the new name)
/// 2. Type change (with USING clause)
/// 3. Nullability change
/// 4. Default change
fn write_alter_column(out: &mut String, table: &str, column: &str, changes: &ColumnChanges) {
    let effective_name = changes.rename.as_deref().unwrap_or(column);

    if let Some(ref new_name) = changes.rename {
        let _ = writeln!(
            out,
            "ALTER TABLE {table} RENAME COLUMN {column} TO {new_name};"
        );
    }

    if let Some((_, ref to_type)) = changes.data_type {
        let _ = writeln!(
            out,
            "ALTER TABLE {table} ALTER COLUMN {effective_name} TYPE {to_type} USING {effective_name}::{to_type};"
        );
    }

    match changes.nullable {
        Some(true) => {
            let _ = writeln!(
                out,
                "ALTER TABLE {table} ALTER COLUMN {effective_name} DROP NOT NULL;"
            );
        }
        Some(false) => {
            let _ = writeln!(
                out,
                "ALTER TABLE {table} ALTER COLUMN {effective_name} SET NOT NULL;"
            );
        }
        None => {}
    }

    match &changes.default {
        Some(DefaultChange::Set(expr)) => {
            let _ = writeln!(
                out,
                "ALTER TABLE {table} ALTER COLUMN {effective_name} SET DEFAULT {expr};"
            );
        }
        Some(DefaultChange::Drop) => {
            let _ = writeln!(
                out,
                "ALTER TABLE {table} ALTER COLUMN {effective_name} DROP DEFAULT;"
            );
        }
        None => {}
    }
}

/// Format a constraint clause for CREATE TABLE or ALTER TABLE ADD CONSTRAINT.
fn format_constraint(constraint: &Constraint) -> String {
    match constraint {
        Constraint::PrimaryKey { name, columns } => {
            let cols = columns.join(", ");
            match name {
                Some(n) => format!("CONSTRAINT {n} PRIMARY KEY ({cols})"),
                None => format!("PRIMARY KEY ({cols})"),
            }
        }
        Constraint::Unique { name, columns } => {
            let cols = columns.join(", ");
            match name {
                Some(n) => format!("CONSTRAINT {n} UNIQUE ({cols})"),
                None => format!("UNIQUE ({cols})"),
            }
        }
        Constraint::ForeignKey {
            name,
            columns,
            references,
            on_delete,
            on_update,
        } => {
            let cols = columns.join(", ");
            let ref_cols = references.columns.join(", ");
            let mut s = match name {
                Some(n) => format!(
                    "CONSTRAINT {n} FOREIGN KEY ({cols}) REFERENCES {}({ref_cols})",
                    references.table
                ),
                None => format!(
                    "FOREIGN KEY ({cols}) REFERENCES {}({ref_cols})",
                    references.table
                ),
            };
            if let Some(action) = on_delete {
                let _ = write!(s, " ON DELETE {action}");
            }
            if let Some(action) = on_update {
                let _ = write!(s, " ON UPDATE {action}");
            }
            s
        }
        Constraint::Check { name, expression } => match name {
            Some(n) => format!("CONSTRAINT {n} CHECK ({expression})"),
            None => format!("CHECK ({expression})"),
        },
    }
}

/// Emit ALTER TABLE ADD CONSTRAINT.
fn write_add_constraint(out: &mut String, table: &str, constraint: &Constraint) {
    let clause = format_constraint(constraint);
    let _ = writeln!(out, "ALTER TABLE {table} ADD {clause};");
}

/// Emit CREATE INDEX or CREATE UNIQUE INDEX.
fn write_create_index(out: &mut String, table: &str, index: &Index) {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let cols = index.columns.join(", ");
    let _ = write!(
        out,
        "CREATE {unique}INDEX {} ON {table} ({cols})",
        index.name
    );
    if let Some(ref where_clause) = index.partial {
        let _ = write!(out, " {where_clause}");
    }
    let _ = writeln!(out, ";");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::diff::{Change, ColumnChanges, DefaultChange};
    use crate::schema::types::{Expression, ForeignKeyRef, PgType, ReferentialAction};
    use crate::schema::{Column, Constraint, Index, Table};

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
        t.add_constraint(Constraint::Unique {
            name: Some("users_email_key".into()),
            columns: vec!["email".into()],
        });
        t
    }

    fn posts_table() -> Table {
        let mut t = Table::new("posts");
        t.add_column(Column::new("id", PgType::Uuid));
        t.add_column(Column::new("author_id", PgType::Uuid));
        t.add_column(Column::new("title", PgType::Text));
        t.add_column(Column::new("body", PgType::Text).nullable());
        t.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
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

    // ── Snapshot tests ───────────────────────────────────

    #[test]
    fn snapshot_create_table() {
        let changes = vec![Change::AddTable(users_table())];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_create_table_with_fk_and_index() {
        let changes = vec![Change::AddTable(posts_table())];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_drop_table() {
        let changes = vec![Change::DropTable("old_table".into())];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_add_column() {
        let changes = vec![Change::AddColumn {
            table: "users".into(),
            column: Column::new("bio", PgType::Text).nullable(),
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_drop_column() {
        let changes = vec![Change::DropColumn {
            table: "users".into(),
            column: "legacy_field".into(),
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_alter_column_type() {
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "age".into(),
            changes: ColumnChanges {
                data_type: Some((PgType::Integer, PgType::BigInt)),
                ..Default::default()
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_alter_column_set_not_null() {
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(false),
                ..Default::default()
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_alter_column_drop_not_null() {
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(true),
                ..Default::default()
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_alter_column_set_default() {
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "status".into(),
            changes: ColumnChanges {
                default: Some(DefaultChange::Set(Expression::Literal("'active'".into()))),
                ..Default::default()
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_alter_column_drop_default() {
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "created_at".into(),
            changes: ColumnChanges {
                default: Some(DefaultChange::Drop),
                ..Default::default()
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_alter_column_rename() {
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "name".into(),
            changes: ColumnChanges {
                rename: Some("full_name".into()),
                ..Default::default()
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_alter_column_bundled() {
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "name".into(),
            changes: ColumnChanges {
                rename: Some("full_name".into()),
                data_type: Some((PgType::Varchar(Some(100)), PgType::Text)),
                nullable: Some(false),
                default: Some(DefaultChange::Set(Expression::Literal("''".into()))),
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_add_constraint_pk() {
        let changes = vec![Change::AddConstraint {
            table: "users".into(),
            constraint: Constraint::PrimaryKey {
                name: Some("users_pkey".into()),
                columns: vec!["id".into()],
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_add_constraint_fk() {
        let changes = vec![Change::AddConstraint {
            table: "posts".into(),
            constraint: Constraint::ForeignKey {
                name: Some("posts_author_fk".into()),
                columns: vec!["author_id".into()],
                references: ForeignKeyRef {
                    table: "users".into(),
                    columns: vec!["id".into()],
                },
                on_delete: Some(ReferentialAction::Cascade),
                on_update: None,
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_add_constraint_unique() {
        let changes = vec![Change::AddConstraint {
            table: "users".into(),
            constraint: Constraint::Unique {
                name: Some("users_email_key".into()),
                columns: vec!["email".into()],
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_add_constraint_check() {
        let changes = vec![Change::AddConstraint {
            table: "products".into(),
            constraint: Constraint::Check {
                name: Some("positive_price".into()),
                expression: "price > 0".into(),
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_drop_constraint() {
        let changes = vec![Change::DropConstraint {
            table: "posts".into(),
            name: "posts_author_fk".into(),
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_add_index() {
        let changes = vec![Change::AddIndex {
            table: "posts".into(),
            index: Index {
                name: "posts_author_idx".into(),
                columns: vec!["author_id".into()],
                unique: false,
                partial: None,
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_add_unique_index() {
        let changes = vec![Change::AddIndex {
            table: "users".into(),
            index: Index {
                name: "users_email_idx".into(),
                columns: vec!["email".into()],
                unique: true,
                partial: None,
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_add_partial_index() {
        let changes = vec![Change::AddIndex {
            table: "users".into(),
            index: Index {
                name: "users_active_email_idx".into(),
                columns: vec!["email".into()],
                unique: false,
                partial: Some("WHERE active = true".into()),
            },
        }];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_drop_index() {
        let changes = vec![Change::DropIndex("posts_author_idx".into())];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_multi_table_migration() {
        let changes = vec![
            Change::AddTable(users_table()),
            Change::AddTable(posts_table()),
        ];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    #[test]
    fn snapshot_complex_migration() {
        // Simulates a realistic migration: drop old table, create new table,
        // modify existing table columns and constraints.
        let changes = vec![
            Change::DropIndex("legacy_idx".into()),
            Change::DropConstraint {
                table: "posts".into(),
                name: "posts_author_fk".into(),
            },
            Change::DropColumn {
                table: "users".into(),
                column: "legacy_field".into(),
            },
            Change::DropTable("legacy_table".into()),
            Change::AddTable({
                let mut t = Table::new("tags");
                t.add_column(Column::new("id", PgType::Integer));
                t.add_column(Column::new("name", PgType::Varchar(Some(100))));
                t.add_constraint(Constraint::PrimaryKey {
                    name: Some("tags_pkey".into()),
                    columns: vec!["id".into()],
                });
                t
            }),
            Change::AddColumn {
                table: "users".into(),
                column: Column::new("bio", PgType::Text).nullable(),
            },
            Change::AlterColumn {
                table: "users".into(),
                column: "email".into(),
                changes: ColumnChanges {
                    nullable: Some(false),
                    ..Default::default()
                },
            },
            Change::AddConstraint {
                table: "posts".into(),
                constraint: Constraint::ForeignKey {
                    name: Some("posts_author_fk".into()),
                    columns: vec!["author_id".into()],
                    references: ForeignKeyRef {
                        table: "users".into(),
                        columns: vec!["id".into()],
                    },
                    on_delete: Some(ReferentialAction::SetNull),
                    on_update: None,
                },
            },
            Change::AddIndex {
                table: "users".into(),
                index: Index {
                    name: "users_bio_idx".into(),
                    columns: vec!["bio".into()],
                    unique: false,
                    partial: Some("WHERE bio IS NOT NULL".into()),
                },
            },
        ];
        let sql = generate_sql(&changes);
        insta::assert_snapshot!(sql);
    }

    // ── Ordering test ────────────────────────────────────

    #[test]
    fn ordering_drops_before_creates() {
        let changes = vec![
            // Intentionally out of order
            Change::AddIndex {
                table: "t".into(),
                index: Index {
                    name: "new_idx".into(),
                    columns: vec!["col".into()],
                    unique: false,
                    partial: None,
                },
            },
            Change::AddTable(Table::new("new_table")),
            Change::DropTable("old_table".into()),
            Change::DropIndex("old_idx".into()),
            Change::AddConstraint {
                table: "t".into(),
                constraint: Constraint::Unique {
                    name: Some("t_col_key".into()),
                    columns: vec!["col".into()],
                },
            },
            Change::DropConstraint {
                table: "t".into(),
                name: "old_constraint".into(),
            },
        ];

        let ordered = order_changes(&changes);
        let labels: Vec<&str> = ordered
            .iter()
            .map(|c| match c {
                Change::DropIndex(_) => "drop_index",
                Change::DropConstraint { .. } => "drop_constraint",
                Change::DropColumn { .. } => "drop_column",
                Change::DropTable(_) => "drop_table",
                Change::AddTable(_) => "add_table",
                Change::AddColumn { .. } => "add_column",
                Change::AlterColumn { .. } => "alter_column",
                Change::AddConstraint { .. } => "add_constraint",
                Change::AddIndex { .. } => "add_index",
            })
            .collect();

        assert_eq!(
            labels,
            vec![
                "drop_index",
                "drop_constraint",
                "drop_table",
                "add_table",
                "add_constraint",
                "add_index",
            ]
        );
    }

    // ── Slugify tests ────────────────────────────────────

    #[test]
    fn slugify_simple() {
        assert_eq!(slugify("add users table"), "add_users_table");
    }

    #[test]
    fn slugify_special_chars() {
        assert_eq!(slugify("Add FK: posts→users"), "add_fk_posts_users");
    }

    #[test]
    fn slugify_extra_spaces() {
        assert_eq!(slugify("  multiple   spaces  "), "multiple_spaces");
    }

    // ── File writing test ────────────────────────────────

    #[test]
    fn write_migration_creates_file() {
        let dir = std::env::temp_dir().join("inara_test_migration");
        let _ = std::fs::create_dir_all(&dir);

        let path = write_migration(
            &dir,
            "add users table",
            "CREATE TABLE users ();\n",
            "20260214120000",
        )
        .unwrap();

        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "20260214120000_add_users_table.up.sql"
        );
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "CREATE TABLE users ();\n");

        // Cleanup
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    // ── Empty changeset ──────────────────────────────────

    #[test]
    fn empty_changeset_produces_empty_sql() {
        assert_eq!(generate_sql(&[]), "");
    }
}
