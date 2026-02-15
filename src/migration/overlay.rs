// Pending migrations overlay.
//
// Detects unapplied migrations by comparing migration files on disk against
// the _sqlx_migrations table, replays them in a temporary schema to produce
// a virtual "future" schema, then diffs against the live schema to show
// what changes are pending.

use std::collections::BTreeMap;
use std::path::Path;

use sqlx::PgPool;

use crate::schema::diff::{self, Change};
use crate::schema::introspect;

use super::loader::{self, MigrationFile};

/// Result of computing the pending overlay.
#[derive(Debug, Clone)]
pub struct PendingOverlay {
    /// The structural changes between the live schema and the virtual schema.
    pub changes: Vec<Change>,
    /// Number of pending migration files.
    pub pending_count: usize,
    /// Filenames of migrations that could not be applied in the temp schema.
    pub unparseable: Vec<String>,
}

impl PendingOverlay {
    /// Returns true if there are no pending changes.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.unparseable.is_empty()
    }

    /// Build a lookup of changes indexed by table name for efficient rendering.
    pub fn changes_by_table(&self) -> BTreeMap<String, Vec<&Change>> {
        let mut map: BTreeMap<String, Vec<&Change>> = BTreeMap::new();
        for change in &self.changes {
            match change {
                Change::AddTable(t) => {
                    map.entry(t.name.clone()).or_default().push(change);
                }
                Change::DropTable(name) => {
                    map.entry(name.clone()).or_default().push(change);
                }
                Change::AddColumn { table, .. }
                | Change::DropColumn { table, .. }
                | Change::AlterColumn { table, .. }
                | Change::AddConstraint { table, .. }
                | Change::DropConstraint { table, .. }
                | Change::AddIndex { table, .. } => {
                    map.entry(table.clone()).or_default().push(change);
                }
                Change::DropIndex(_) => {
                    // DropIndex doesn't carry table info; skip for per-table lookup
                }
            }
        }
        map
    }

    /// Check if a specific table is being added by pending migrations.
    pub fn is_table_added(&self, name: &str) -> bool {
        self.changes
            .iter()
            .any(|c| matches!(c, Change::AddTable(t) if t.name == name))
    }

    /// Check if a specific table is being dropped by pending migrations.
    pub fn is_table_dropped(&self, name: &str) -> bool {
        self.changes
            .iter()
            .any(|c| matches!(c, Change::DropTable(n) if n == name))
    }

    /// Check if a specific table is being modified (columns/constraints/indexes changed).
    pub fn is_table_modified(&self, name: &str) -> bool {
        self.changes.iter().any(|c| match c {
            Change::AddColumn { table, .. }
            | Change::DropColumn { table, .. }
            | Change::AlterColumn { table, .. }
            | Change::AddConstraint { table, .. }
            | Change::DropConstraint { table, .. }
            | Change::AddIndex { table, .. } => table == name,
            _ => false,
        })
    }

    /// Get the change marker for a specific column.
    pub fn column_marker(&self, table: &str, column: &str) -> Option<ChangeMarker> {
        for change in &self.changes {
            match change {
                Change::AddTable(t) if t.name == table => {
                    return Some(ChangeMarker::Added);
                }
                Change::DropTable(n) if n == table => {
                    return Some(ChangeMarker::Removed);
                }
                Change::AddColumn {
                    table: t,
                    column: c,
                } if t == table && c.name == column => {
                    return Some(ChangeMarker::Added);
                }
                Change::DropColumn {
                    table: t,
                    column: c,
                } if t == table && c == column => {
                    return Some(ChangeMarker::Removed);
                }
                Change::AlterColumn {
                    table: t,
                    column: c,
                    ..
                } if t == table && c == column => {
                    return Some(ChangeMarker::Modified);
                }
                _ => {}
            }
        }
        None
    }

    /// Get the change marker for a table header.
    pub fn table_marker(&self, name: &str) -> Option<ChangeMarker> {
        if self.is_table_added(name) {
            Some(ChangeMarker::Added)
        } else if self.is_table_dropped(name) {
            Some(ChangeMarker::Removed)
        } else if self.is_table_modified(name) {
            Some(ChangeMarker::Modified)
        } else {
            None
        }
    }
}

/// Visual marker for a schema element in the overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeMarker {
    /// Element is being added (+, green).
    Added,
    /// Element is being removed (-, red).
    Removed,
    /// Element is being modified (~, yellow).
    Modified,
}

impl ChangeMarker {
    /// The prefix character for inline display.
    pub fn prefix(&self) -> &'static str {
        match self {
            ChangeMarker::Added => "+ ",
            ChangeMarker::Removed => "- ",
            ChangeMarker::Modified => "~ ",
        }
    }
}

/// Query the _sqlx_migrations table for applied migration versions.
///
/// Returns a list of version timestamps (bigint, which corresponds to the
/// YYYYMMDDHHMMSS format used in filenames).
async fn applied_versions(pool: &PgPool) -> Result<Vec<i64>, sqlx::Error> {
    // Check if the _sqlx_migrations table exists first
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = '_sqlx_migrations'
        )",
    )
    .fetch_one(pool)
    .await?;

    if !exists {
        return Ok(Vec::new());
    }

    let rows: Vec<(i64,)> = sqlx::query_as("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(pool)
        .await?;

    Ok(rows.into_iter().map(|(v,)| v).collect())
}

/// Find migration files that have not been applied yet.
fn find_pending(migrations: &[MigrationFile], applied: &[i64]) -> Vec<MigrationFile> {
    migrations
        .iter()
        .filter(|m| {
            let version: i64 = m.timestamp.parse().unwrap_or(0);
            !applied.contains(&version)
        })
        .cloned()
        .collect()
}

/// Compute the pending migrations overlay.
///
/// 1. Scans the migrations directory for migration files
/// 2. Queries _sqlx_migrations for applied versions
/// 3. Replays unapplied migrations in a temporary schema
/// 4. Introspects the temporary schema
/// 5. Diffs the live schema against the virtual schema
pub async fn compute_overlay(
    pool: &PgPool,
    migrations_dir: &Path,
    schema_name: &str,
) -> Result<PendingOverlay, OverlayError> {
    // Load migration files from disk
    let all_migrations = loader::scan_migrations(migrations_dir).map_err(OverlayError::Io)?;

    // Get applied versions from the database
    let applied = applied_versions(pool)
        .await
        .map_err(OverlayError::Database)?;

    // Find unapplied migrations
    let pending = find_pending(&all_migrations, &applied);

    if pending.is_empty() {
        return Ok(PendingOverlay {
            changes: Vec::new(),
            pending_count: 0,
            unparseable: Vec::new(),
        });
    }

    let pending_count = pending.len();

    // Create a temporary schema, replay migrations, and introspect
    let temp_schema_name = format!("_inara_overlay_{}", std::process::id());

    // Create temporary schema
    sqlx::query(&format!("CREATE SCHEMA {temp_schema_name}"))
        .execute(pool)
        .await
        .map_err(OverlayError::Database)?;

    // Copy existing schema objects into the temp schema
    let copy_result = copy_schema(pool, schema_name, &temp_schema_name).await;

    let result = match copy_result {
        Ok(()) => {
            // Replay pending migrations in the temp schema
            let unparseable = replay_migrations(pool, &temp_schema_name, &pending).await;

            // Introspect both schemas
            let live_schema = introspect::introspect(pool, schema_name)
                .await
                .map_err(|e| OverlayError::Database(e.into()))?;
            let virtual_schema = introspect::introspect(pool, &temp_schema_name)
                .await
                .map_err(|e| OverlayError::Database(e.into()))?;

            // Diff live vs virtual
            let changes = diff::diff(&live_schema, &virtual_schema, &[]);

            Ok(PendingOverlay {
                changes,
                pending_count,
                unparseable,
            })
        }
        Err(e) => Err(e),
    };

    // Always clean up the temp schema
    let _ = sqlx::query(&format!("DROP SCHEMA {temp_schema_name} CASCADE"))
        .execute(pool)
        .await;

    result
}

/// Copy all objects from one schema to another using pg_dump-style recreation.
///
/// This uses a search_path trick: we set the search_path to the temp schema,
/// then replay the CREATE statements from the live schema.
async fn copy_schema(pool: &PgPool, source: &str, target: &str) -> Result<(), OverlayError> {
    // Get all table DDL from the source schema and recreate in target
    // We use a multi-step approach:
    // 1. Get table names
    // 2. For each table, get CREATE TABLE SQL via information_schema
    // 3. Execute in the target schema

    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_type = 'BASE TABLE'
         ORDER BY table_name",
    )
    .bind(source)
    .fetch_all(pool)
    .await
    .map_err(OverlayError::Database)?;

    for (table_name,) in &tables {
        // Generate CREATE TABLE ... AS SELECT with no data
        let create_sql = format!(
            "CREATE TABLE {target}.{table_name} (LIKE {source}.{table_name} INCLUDING ALL)"
        );
        sqlx::query(&create_sql)
            .execute(pool)
            .await
            .map_err(OverlayError::Database)?;
    }

    // Copy enums
    let enums: Vec<(String,)> = sqlx::query_as(
        "SELECT t.typname
         FROM pg_type t
         JOIN pg_namespace n ON t.typnamespace = n.oid
         WHERE n.nspname = $1 AND t.typtype = 'e'
         ORDER BY t.typname",
    )
    .bind(source)
    .fetch_all(pool)
    .await
    .map_err(OverlayError::Database)?;

    for (enum_name,) in &enums {
        // Get enum values
        let values: Vec<(String,)> = sqlx::query_as(
            "SELECT e.enumlabel
             FROM pg_enum e
             JOIN pg_type t ON e.enumtypid = t.oid
             JOIN pg_namespace n ON t.typnamespace = n.oid
             WHERE n.nspname = $1 AND t.typname = $2
             ORDER BY e.enumsortorder",
        )
        .bind(source)
        .bind(enum_name)
        .fetch_all(pool)
        .await
        .map_err(OverlayError::Database)?;

        let labels: Vec<String> = values.iter().map(|(v,)| format!("'{v}'")).collect();
        let create_sql = format!(
            "CREATE TYPE {target}.{enum_name} AS ENUM ({})",
            labels.join(", ")
        );
        // Enum creation may fail if it references types not yet created; ignore errors
        let _ = sqlx::query(&create_sql).execute(pool).await;
    }

    Ok(())
}

/// Replay migration SQL in the temporary schema.
///
/// Sets the search_path to the temp schema before executing each migration.
/// Returns filenames of migrations that failed to execute.
async fn replay_migrations(
    pool: &PgPool,
    temp_schema: &str,
    pending: &[MigrationFile],
) -> Vec<String> {
    let mut unparseable = Vec::new();

    for migration in pending {
        // Set search_path so unqualified table references go to temp schema
        let set_path = format!("SET search_path TO {temp_schema}, public");
        if sqlx::query(&set_path).execute(pool).await.is_err() {
            unparseable.push(filename_display(&migration.path));
            continue;
        }

        // Execute the migration SQL
        if sqlx::query(&migration.sql).execute(pool).await.is_err() {
            unparseable.push(filename_display(&migration.path));
        }
    }

    // Reset search_path
    let _ = sqlx::query("SET search_path TO public").execute(pool).await;

    unparseable
}

/// Extract just the filename from a path for display.
fn filename_display(path: &Path) -> String {
    path.file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| path.display().to_string())
}

/// Errors that can occur when computing the overlay.
#[derive(Debug, thiserror::Error)]
pub enum OverlayError {
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),
    #[error("database error: {0}")]
    Database(#[source] sqlx::Error),
}

impl From<introspect::IntrospectError> for sqlx::Error {
    fn from(e: introspect::IntrospectError) -> Self {
        match e {
            introspect::IntrospectError::Database(e) => e,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::diff::Change;
    use crate::schema::types::PgType;
    use crate::schema::{Column, Table};

    fn sample_overlay() -> PendingOverlay {
        PendingOverlay {
            changes: vec![
                Change::AddTable({
                    let mut t = Table::new("new_table");
                    t.add_column(Column::new("id", PgType::Uuid));
                    t
                }),
                Change::AddColumn {
                    table: "users".into(),
                    column: Column::new("bio", PgType::Text),
                },
                Change::DropColumn {
                    table: "users".into(),
                    column: "legacy".into(),
                },
                Change::AlterColumn {
                    table: "posts".into(),
                    column: "title".into(),
                    changes: crate::schema::diff::ColumnChanges {
                        nullable: Some(true),
                        ..Default::default()
                    },
                },
            ],
            pending_count: 2,
            unparseable: Vec::new(),
        }
    }

    #[test]
    fn overlay_is_empty() {
        let empty = PendingOverlay {
            changes: Vec::new(),
            pending_count: 0,
            unparseable: Vec::new(),
        };
        assert!(empty.is_empty());

        let non_empty = sample_overlay();
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn overlay_table_markers() {
        let overlay = sample_overlay();

        assert_eq!(overlay.table_marker("new_table"), Some(ChangeMarker::Added));
        assert_eq!(overlay.table_marker("users"), Some(ChangeMarker::Modified));
        assert_eq!(overlay.table_marker("posts"), Some(ChangeMarker::Modified));
        assert_eq!(overlay.table_marker("unaffected"), None);
    }

    #[test]
    fn overlay_column_markers() {
        let overlay = sample_overlay();

        // Columns in a newly added table are all "added"
        assert_eq!(
            overlay.column_marker("new_table", "id"),
            Some(ChangeMarker::Added)
        );

        // Explicitly added column
        assert_eq!(
            overlay.column_marker("users", "bio"),
            Some(ChangeMarker::Added)
        );

        // Dropped column
        assert_eq!(
            overlay.column_marker("users", "legacy"),
            Some(ChangeMarker::Removed)
        );

        // Modified column
        assert_eq!(
            overlay.column_marker("posts", "title"),
            Some(ChangeMarker::Modified)
        );

        // Unaffected column
        assert_eq!(overlay.column_marker("users", "email"), None);
    }

    #[test]
    fn overlay_changes_by_table() {
        let overlay = sample_overlay();
        let by_table = overlay.changes_by_table();

        assert!(by_table.contains_key("new_table"));
        assert!(by_table.contains_key("users"));
        assert!(by_table.contains_key("posts"));
        assert_eq!(by_table["users"].len(), 2); // AddColumn + DropColumn
        assert_eq!(by_table["posts"].len(), 1); // AlterColumn
    }

    #[test]
    fn change_marker_prefix() {
        assert_eq!(ChangeMarker::Added.prefix(), "+ ");
        assert_eq!(ChangeMarker::Removed.prefix(), "- ");
        assert_eq!(ChangeMarker::Modified.prefix(), "~ ");
    }

    #[test]
    fn find_pending_filters_applied() {
        let migrations = vec![
            MigrationFile {
                timestamp: "20260101000000".into(),
                description: "first".into(),
                path: "m1.up.sql".into(),
                sql: "SELECT 1".into(),
            },
            MigrationFile {
                timestamp: "20260102000000".into(),
                description: "second".into(),
                path: "m2.up.sql".into(),
                sql: "SELECT 2".into(),
            },
            MigrationFile {
                timestamp: "20260103000000".into(),
                description: "third".into(),
                path: "m3.up.sql".into(),
                sql: "SELECT 3".into(),
            },
        ];

        let applied = vec![20260101000000_i64, 20260102000000_i64];
        let pending = find_pending(&migrations, &applied);

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].timestamp, "20260103000000");
    }

    #[test]
    fn find_pending_all_applied() {
        let migrations = vec![MigrationFile {
            timestamp: "20260101000000".into(),
            description: "first".into(),
            path: "m1.up.sql".into(),
            sql: "SELECT 1".into(),
        }];

        let applied = vec![20260101000000_i64];
        let pending = find_pending(&migrations, &applied);
        assert!(pending.is_empty());
    }

    #[test]
    fn find_pending_none_applied() {
        let migrations = vec![
            MigrationFile {
                timestamp: "20260101000000".into(),
                description: "first".into(),
                path: "m1.up.sql".into(),
                sql: "SELECT 1".into(),
            },
            MigrationFile {
                timestamp: "20260102000000".into(),
                description: "second".into(),
                path: "m2.up.sql".into(),
                sql: "SELECT 2".into(),
            },
        ];

        let applied = vec![];
        let pending = find_pending(&migrations, &applied);
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn is_table_added_dropped_modified() {
        let overlay = PendingOverlay {
            changes: vec![
                Change::AddTable(Table::new("new_one")),
                Change::DropTable("old_one".into()),
                Change::AddColumn {
                    table: "modified_one".into(),
                    column: Column::new("col", PgType::Text),
                },
            ],
            pending_count: 1,
            unparseable: Vec::new(),
        };

        assert!(overlay.is_table_added("new_one"));
        assert!(!overlay.is_table_added("old_one"));

        assert!(overlay.is_table_dropped("old_one"));
        assert!(!overlay.is_table_dropped("new_one"));

        assert!(overlay.is_table_modified("modified_one"));
        assert!(!overlay.is_table_modified("new_one"));
    }

    #[test]
    fn unparseable_makes_non_empty() {
        let overlay = PendingOverlay {
            changes: Vec::new(),
            pending_count: 1,
            unparseable: vec!["bad_migration.up.sql".into()],
        };
        assert!(!overlay.is_empty());
    }
}
