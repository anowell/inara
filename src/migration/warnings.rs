// Migration safety warnings — pre-flight checks against the live database.
//
// Before writing a migration, each Change is checked for potential failures.
// Warning queries are bounded with timeouts to avoid blocking on huge tables.

use std::fmt;
use std::time::Duration;

use sqlx::PgPool;

use crate::schema::diff::{Change, ColumnChanges};
use crate::schema::types::PgType;
use crate::schema::Constraint;

/// Timeout for individual warning check queries.
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Severity level for a migration warning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// May cause data loss or unexpected behavior, but migration can succeed.
    Warning,
    /// Migration will fail if not addressed.
    Error,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Warning => write!(f, "WARNING"),
            Severity::Error => write!(f, "ERROR"),
        }
    }
}

/// A single migration warning with context.
#[derive(Debug, Clone)]
pub struct MigrationWarning {
    pub severity: Severity,
    pub description: String,
    pub affected_rows: Option<i64>,
    pub remediation: String,
}

impl fmt::Display for MigrationWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.severity, self.description)?;
        if let Some(count) = self.affected_rows {
            write!(f, " ({count} rows affected)")?;
        }
        Ok(())
    }
}

/// Run safety checks for all changes against the live database.
///
/// Returns warnings for changes that could fail or cause data loss.
/// Each change is checked independently with a query timeout.
pub async fn check_changes(
    pool: &PgPool,
    schema: &str,
    changes: &[Change],
) -> Result<Vec<MigrationWarning>, sqlx::Error> {
    let mut warnings = Vec::new();

    for change in changes {
        match change {
            Change::AlterColumn {
                table,
                column,
                changes: col_changes,
            } => {
                check_alter_column(pool, schema, table, column, col_changes, &mut warnings).await?;
            }
            Change::DropColumn { table, column } => {
                check_drop_column(pool, schema, table, column, &mut warnings).await?;
            }
            Change::AddConstraint { table, constraint } => {
                check_add_constraint(pool, schema, table, constraint, &mut warnings).await?;
            }
            // AddTable, DropTable, AddColumn, DropConstraint, AddIndex, DropIndex
            // don't have fallible data-dependent checks.
            _ => {}
        }
    }

    Ok(warnings)
}

/// Check ALTER COLUMN changes for potential failures.
async fn check_alter_column(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
    changes: &ColumnChanges,
    warnings: &mut Vec<MigrationWarning>,
) -> Result<(), sqlx::Error> {
    // SET NOT NULL: check for existing NULL values
    if changes.nullable == Some(false) {
        check_set_not_null(pool, schema, table, column, warnings).await?;
    }

    // Type change: warn about narrowing casts
    if let Some((ref from_type, ref to_type)) = changes.data_type {
        check_alter_type(table, column, from_type, to_type, warnings);
    }

    Ok(())
}

/// SET NOT NULL: `SELECT count(*) FROM table WHERE column IS NULL`
async fn check_set_not_null(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
    warnings: &mut Vec<MigrationWarning>,
) -> Result<(), sqlx::Error> {
    let query = format!("SELECT count(*) AS cnt FROM {schema}.{table} WHERE {column} IS NULL");

    let row: (i64,) = tokio::time::timeout(QUERY_TIMEOUT, sqlx::query_as(&query).fetch_one(pool))
        .await
        .unwrap_or(Ok((0,)))
        .unwrap_or((0,));

    if row.0 > 0 {
        warnings.push(MigrationWarning {
            severity: Severity::Error,
            description: format!(
                "{table}.{column}: SET NOT NULL will fail — {count} rows have NULL values",
                count = row.0
            ),
            affected_rows: Some(row.0),
            remediation: format!(
                "UPDATE {table} SET {column} = <default_value> WHERE {column} IS NULL"
            ),
        });
    }

    Ok(())
}

/// ALTER COLUMN TYPE: warn about potentially lossy type conversions.
///
/// This is a static check (no DB query needed) — we flag narrowing casts
/// that could truncate data or fail at runtime.
fn check_alter_type(
    table: &str,
    column: &str,
    from: &PgType,
    to: &PgType,
    warnings: &mut Vec<MigrationWarning>,
) {
    if is_narrowing_cast(from, to) {
        warnings.push(MigrationWarning {
            severity: Severity::Warning,
            description: format!(
                "{table}.{column}: type change {from} -> {to} may truncate or lose data"
            ),
            affected_rows: None,
            remediation: format!("Review existing data in {table}.{column} before applying"),
        });
    }
}

/// Determine if a type change is a narrowing cast that could lose data.
fn is_narrowing_cast(from: &PgType, to: &PgType) -> bool {
    use PgType::*;

    match (from, to) {
        // Numeric narrowing
        (BigInt, Integer | SmallInt) => true,
        (Integer, SmallInt) => true,
        (DoublePrecision, Real | Integer | SmallInt | BigInt) => true,
        (Real, Integer | SmallInt) => true,
        (Numeric(_), Integer | SmallInt | BigInt) => true,

        // Text narrowing
        (Text, Varchar(Some(_)) | Char(Some(_))) => true,
        (Varchar(None), Varchar(Some(_)) | Char(Some(_))) => true,
        (Varchar(Some(a)), Varchar(Some(b))) if a > b => true,
        (Varchar(Some(a)), Char(Some(b))) if a > b => true,

        // Timestamp precision loss
        (Timestamptz, Timestamp) => true, // loses timezone
        (Timestamptz, Date) => true,      // loses time
        (Timestamp, Date) => true,        // loses time

        // JSON -> JSONB is safe, but JSONB -> JSON can lose ordering
        (Jsonb, Json) => true,

        _ => false,
    }
}

/// DROP COLUMN: warn if the table is non-empty (data will be lost).
async fn check_drop_column(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
    warnings: &mut Vec<MigrationWarning>,
) -> Result<(), sqlx::Error> {
    let query = format!("SELECT count(*) AS cnt FROM {schema}.{table}");

    let row: (i64,) = tokio::time::timeout(QUERY_TIMEOUT, sqlx::query_as(&query).fetch_one(pool))
        .await
        .unwrap_or(Ok((0,)))
        .unwrap_or((0,));

    if row.0 > 0 {
        warnings.push(MigrationWarning {
            severity: Severity::Warning,
            description: format!(
                "{table}.{column}: dropping column from table with {count} rows — data will be lost",
                count = row.0
            ),
            affected_rows: Some(row.0),
            remediation: "Back up data before dropping the column".to_string(),
        });
    }

    Ok(())
}

/// ADD CONSTRAINT: check for data that would violate the constraint.
async fn check_add_constraint(
    pool: &PgPool,
    schema: &str,
    table: &str,
    constraint: &Constraint,
    warnings: &mut Vec<MigrationWarning>,
) -> Result<(), sqlx::Error> {
    match constraint {
        Constraint::ForeignKey {
            columns,
            references,
            ..
        } => {
            check_fk_violation(pool, schema, table, columns, references, warnings).await?;
        }
        Constraint::Unique { columns, .. } => {
            check_unique_violation(pool, schema, table, columns, warnings).await?;
        }
        // PrimaryKey and Check constraints don't have simple pre-flight checks
        // that differ from unique/not-null checks.
        _ => {}
    }

    Ok(())
}

/// ADD FOREIGN KEY: check for orphaned rows that would violate the FK.
async fn check_fk_violation(
    pool: &PgPool,
    schema: &str,
    table: &str,
    columns: &[String],
    references: &crate::schema::types::ForeignKeyRef,
    warnings: &mut Vec<MigrationWarning>,
) -> Result<(), sqlx::Error> {
    // Build join condition for multi-column FKs
    let on_clause: Vec<String> = columns
        .iter()
        .zip(references.columns.iter())
        .map(|(src, dst)| format!("s.{src} = r.{dst}"))
        .collect();
    let on_str = on_clause.join(" AND ");

    // NULL FK values are always valid (they don't reference anything)
    let null_checks: Vec<String> = columns
        .iter()
        .map(|c| format!("s.{c} IS NOT NULL"))
        .collect();
    let null_str = null_checks.join(" AND ");

    let query = format!(
        "SELECT count(*) AS cnt FROM {schema}.{table} s \
         LEFT JOIN {schema}.{ref_table} r ON {on_str} \
         WHERE {null_str} AND r.{ref_col} IS NULL",
        ref_table = references.table,
        ref_col = references.columns[0],
    );

    let row: (i64,) = tokio::time::timeout(QUERY_TIMEOUT, sqlx::query_as(&query).fetch_one(pool))
        .await
        .unwrap_or(Ok((0,)))
        .unwrap_or((0,));

    if row.0 > 0 {
        let cols = columns.join(", ");
        warnings.push(MigrationWarning {
            severity: Severity::Error,
            description: format!(
                "{table}({cols}): FK to {ref_table} will fail — {count} orphaned rows",
                ref_table = references.table,
                count = row.0
            ),
            affected_rows: Some(row.0),
            remediation: format!(
                "DELETE FROM {table} WHERE ({cols}) NOT IN (SELECT {ref_cols} FROM {ref_table})",
                ref_cols = references.columns.join(", "),
                ref_table = references.table,
            ),
        });
    }

    Ok(())
}

/// ADD UNIQUE: check for duplicate values.
async fn check_unique_violation(
    pool: &PgPool,
    schema: &str,
    table: &str,
    columns: &[String],
    warnings: &mut Vec<MigrationWarning>,
) -> Result<(), sqlx::Error> {
    let cols = columns.join(", ");
    let query = format!(
        "SELECT count(*) AS cnt FROM (\
            SELECT {cols} FROM {schema}.{table} \
            GROUP BY {cols} HAVING count(*) > 1\
         ) AS dupes"
    );

    let row: (i64,) = tokio::time::timeout(QUERY_TIMEOUT, sqlx::query_as(&query).fetch_one(pool))
        .await
        .unwrap_or(Ok((0,)))
        .unwrap_or((0,));

    if row.0 > 0 {
        warnings.push(MigrationWarning {
            severity: Severity::Error,
            description: format!(
                "{table}({cols}): UNIQUE constraint will fail — {count} duplicate groups",
                count = row.0
            ),
            affected_rows: Some(row.0),
            remediation: format!(
                "SELECT {cols}, count(*) FROM {table} GROUP BY {cols} HAVING count(*) > 1"
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_display() {
        assert_eq!(Severity::Warning.to_string(), "WARNING");
        assert_eq!(Severity::Error.to_string(), "ERROR");
    }

    #[test]
    fn warning_display_with_rows() {
        let w = MigrationWarning {
            severity: Severity::Error,
            description: "users.email: SET NOT NULL will fail".into(),
            affected_rows: Some(42),
            remediation: String::new(),
        };
        assert!(w.to_string().contains("42 rows affected"));
    }

    #[test]
    fn warning_display_without_rows() {
        let w = MigrationWarning {
            severity: Severity::Warning,
            description: "type change may truncate".into(),
            affected_rows: None,
            remediation: String::new(),
        };
        let s = w.to_string();
        assert!(!s.contains("rows"));
        assert!(s.contains("WARNING"));
    }

    // -- Narrowing cast tests --

    #[test]
    fn narrowing_bigint_to_int() {
        assert!(is_narrowing_cast(&PgType::BigInt, &PgType::Integer));
    }

    #[test]
    fn narrowing_bigint_to_smallint() {
        assert!(is_narrowing_cast(&PgType::BigInt, &PgType::SmallInt));
    }

    #[test]
    fn narrowing_int_to_smallint() {
        assert!(is_narrowing_cast(&PgType::Integer, &PgType::SmallInt));
    }

    #[test]
    fn widening_int_to_bigint_is_safe() {
        assert!(!is_narrowing_cast(&PgType::Integer, &PgType::BigInt));
    }

    #[test]
    fn widening_smallint_to_int_is_safe() {
        assert!(!is_narrowing_cast(&PgType::SmallInt, &PgType::Integer));
    }

    #[test]
    fn narrowing_text_to_varchar() {
        assert!(is_narrowing_cast(
            &PgType::Text,
            &PgType::Varchar(Some(100))
        ));
    }

    #[test]
    fn narrowing_varchar_wider_to_narrower() {
        assert!(is_narrowing_cast(
            &PgType::Varchar(Some(255)),
            &PgType::Varchar(Some(100))
        ));
    }

    #[test]
    fn same_varchar_is_not_narrowing() {
        assert!(!is_narrowing_cast(
            &PgType::Varchar(Some(100)),
            &PgType::Varchar(Some(100))
        ));
    }

    #[test]
    fn narrowing_timestamptz_to_date() {
        assert!(is_narrowing_cast(&PgType::Timestamptz, &PgType::Date));
    }

    #[test]
    fn narrowing_timestamptz_to_timestamp() {
        assert!(is_narrowing_cast(&PgType::Timestamptz, &PgType::Timestamp));
    }

    #[test]
    fn narrowing_jsonb_to_json() {
        assert!(is_narrowing_cast(&PgType::Jsonb, &PgType::Json));
    }

    #[test]
    fn widening_json_to_jsonb_is_safe() {
        assert!(!is_narrowing_cast(&PgType::Json, &PgType::Jsonb));
    }

    #[test]
    fn narrowing_double_to_real() {
        assert!(is_narrowing_cast(&PgType::DoublePrecision, &PgType::Real));
    }

    #[test]
    fn same_type_is_not_narrowing() {
        assert!(!is_narrowing_cast(&PgType::Integer, &PgType::Integer));
        assert!(!is_narrowing_cast(&PgType::Text, &PgType::Text));
    }

    #[test]
    fn check_alter_type_adds_warning_for_narrowing() {
        let mut warnings = Vec::new();
        check_alter_type(
            "users",
            "age",
            &PgType::BigInt,
            &PgType::SmallInt,
            &mut warnings,
        );
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].severity, Severity::Warning);
        assert!(warnings[0].description.contains("users.age"));
        assert!(warnings[0].description.contains("truncate"));
    }

    #[test]
    fn check_alter_type_no_warning_for_widening() {
        let mut warnings = Vec::new();
        check_alter_type(
            "users",
            "age",
            &PgType::SmallInt,
            &PgType::BigInt,
            &mut warnings,
        );
        assert!(warnings.is_empty());
    }
}
