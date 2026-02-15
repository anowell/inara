// Integration tests for migration safety warnings.
//
// These tests run against a real Postgres database and verify that
// warning checks correctly identify data that would cause migrations to fail.

use std::path::Path;

use inara::migration::warnings::{self, MigrationWarning, Severity};
use inara::schema::diff::{Change, ColumnChanges};
use inara::schema::types::{ForeignKeyRef, PgType};
use inara::schema::{Column, Constraint};
use sqlx::PgPool;

const TEST_SCHEMA: &str = "inara_test";

async fn setup_pool() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to database")
}

async fn run_fixture(pool: &PgPool, filename: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(filename);
    let sql = std::fs::read_to_string(path).expect("Failed to read fixture file");
    for statement in sql.split(';') {
        let stripped: String = statement
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let trimmed = stripped.trim();
        if trimmed.is_empty() {
            continue;
        }
        sqlx::query(trimmed)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to execute SQL: {trimmed}\nError: {e}"));
    }
}

async fn setup_test_schema(pool: &PgPool) {
    run_fixture(pool, "setup.sql").await;
}

async fn teardown_test_schema(pool: &PgPool) {
    run_fixture(pool, "teardown.sql").await;
}

async fn with_test_schema<F, Fut>(f: F)
where
    F: FnOnce(PgPool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let pool = setup_pool().await;
    setup_test_schema(&pool).await;
    f(pool.clone()).await;
    teardown_test_schema(&pool).await;
}

/// Insert known test data for warning checks.
async fn insert_test_data(pool: &PgPool) {
    // Insert users — bio is NULL for Bob
    sqlx::query(
        "INSERT INTO inara_test.users (id, email, name, status, age, bio, created_at) VALUES
         ('a0000000-0000-0000-0000-000000000001', 'alice@test.com', 'Alice', 'active', 30, 'Hello', '2025-01-01T00:00:00Z'),
         ('a0000000-0000-0000-0000-000000000002', 'bob@test.com', 'Bob', 'inactive', 25, NULL, '2025-06-15T12:00:00Z'),
         ('a0000000-0000-0000-0000-000000000003', 'carol@test.com', 'Carol', 'pending', 40, 'Hi', '2025-12-31T23:59:59Z')"
    )
    .execute(pool)
    .await
    .expect("insert users");

    // Insert posts — author_id references users
    sqlx::query(
        "INSERT INTO inara_test.posts (author_id, title, body, score, published, created_at) VALUES
         ('a0000000-0000-0000-0000-000000000001', 'Post 1', 'Body 1', 4.50, true, '2025-02-01T00:00:00Z'),
         ('a0000000-0000-0000-0000-000000000001', 'Post 2', NULL, 2.00, false, '2025-03-01T00:00:00Z'),
         ('a0000000-0000-0000-0000-000000000002', 'Post 3', 'Body 3', 0.00, true, '2025-04-01T00:00:00Z')"
    )
    .execute(pool)
    .await
    .expect("insert posts");
}

// ── SET NOT NULL warnings ───────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn warning_set_not_null_with_null_rows() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // bio has 1 NULL value (Bob)
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "bio".into(),
            changes: ColumnChanges {
                nullable: Some(false),
                ..Default::default()
            },
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert_eq!(warnings.len(), 1, "should have 1 warning");
        assert_eq!(warnings[0].severity, Severity::Error);
        assert_eq!(warnings[0].affected_rows, Some(1));
        assert!(
            warnings[0].description.contains("SET NOT NULL"),
            "description should mention SET NOT NULL: {}",
            warnings[0].description
        );
        assert!(
            warnings[0].description.contains("bio"),
            "description should mention column name"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn warning_set_not_null_no_nulls_no_warning() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // email has no NULL values
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(false),
                ..Default::default()
            },
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert!(
            warnings.is_empty(),
            "should have no warnings when no NULLs exist"
        );
    })
    .await;
}

// ── UNIQUE constraint warnings ──────────────────────────────────────

#[tokio::test]
#[ignore]
async fn warning_unique_with_duplicates() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // published has duplicates: true appears twice, false once
        let changes = vec![Change::AddConstraint {
            table: "posts".into(),
            constraint: Constraint::Unique {
                name: Some("posts_published_unique".into()),
                columns: vec!["published".into()],
            },
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert_eq!(warnings.len(), 1, "should have 1 warning");
        assert_eq!(warnings[0].severity, Severity::Error);
        assert!(warnings[0].affected_rows.unwrap() >= 1);
        assert!(
            warnings[0].description.contains("UNIQUE"),
            "description should mention UNIQUE: {}",
            warnings[0].description
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn warning_unique_no_duplicates_no_warning() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // email is already unique — adding a UNIQUE constraint should produce no warning
        let changes = vec![Change::AddConstraint {
            table: "users".into(),
            constraint: Constraint::Unique {
                name: Some("users_email_unique2".into()),
                columns: vec!["email".into()],
            },
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert!(warnings.is_empty(), "no duplicates means no warning");
    })
    .await;
}

// ── Foreign key warnings ────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn warning_fk_with_orphaned_rows() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // Insert an orphaned post referencing a non-existent category
        sqlx::query("INSERT INTO inara_test.categories (id, name) VALUES (1, 'Tech')")
            .execute(&pool)
            .await
            .expect("insert category");

        // Insert a post with category_id that doesn't exist
        // We'll create a temp column for this test
        sqlx::query("ALTER TABLE inara_test.posts ADD COLUMN category_id INTEGER")
            .execute(&pool)
            .await
            .expect("add category_id");

        // Set one post to valid category, another to invalid
        sqlx::query("UPDATE inara_test.posts SET category_id = 1 WHERE title = 'Post 1'")
            .execute(&pool)
            .await
            .expect("set valid category");

        sqlx::query("UPDATE inara_test.posts SET category_id = 999 WHERE title = 'Post 2'")
            .execute(&pool)
            .await
            .expect("set orphan category");

        // Now try adding FK from posts.category_id -> categories.id
        let changes = vec![Change::AddConstraint {
            table: "posts".into(),
            constraint: Constraint::ForeignKey {
                name: Some("posts_category_fk".into()),
                columns: vec!["category_id".into()],
                references: ForeignKeyRef {
                    table: "categories".into(),
                    columns: vec!["id".into()],
                },
                on_delete: None,
                on_update: None,
            },
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert_eq!(warnings.len(), 1, "should detect orphaned row");
        assert_eq!(warnings[0].severity, Severity::Error);
        assert_eq!(warnings[0].affected_rows, Some(1));
        assert!(warnings[0].description.contains("orphaned"));
    })
    .await;
}

// ── DROP COLUMN warnings ────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn warning_drop_column_nonempty_table() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let changes = vec![Change::DropColumn {
            table: "users".into(),
            column: "bio".into(),
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert_eq!(warnings.len(), 1, "should warn about data loss");
        assert_eq!(warnings[0].severity, Severity::Warning);
        assert_eq!(warnings[0].affected_rows, Some(3));
        assert!(warnings[0].description.contains("data will be lost"));
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn warning_drop_column_empty_table_no_warning() {
    with_test_schema(|pool| async move {
        // categories table is empty (no data inserted)
        let changes = vec![Change::DropColumn {
            table: "categories".into(),
            column: "name".into(),
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert!(
            warnings.is_empty(),
            "empty table should not trigger drop column warning"
        );
    })
    .await;
}

// ── ALTER COLUMN TYPE warnings ──────────────────────────────────────

#[tokio::test]
#[ignore]
async fn warning_alter_type_narrowing_cast() {
    with_test_schema(|pool| async move {
        // Narrowing cast: text -> varchar(10) is flagged
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "name".into(),
            changes: ColumnChanges {
                data_type: Some((PgType::Varchar(Some(255)), PgType::Varchar(Some(10)))),
                ..Default::default()
            },
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert_eq!(warnings.len(), 1, "should warn about narrowing cast");
        assert_eq!(warnings[0].severity, Severity::Warning);
        assert!(warnings[0].description.contains("truncate"));
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn warning_alter_type_widening_no_warning() {
    with_test_schema(|pool| async move {
        // Widening cast: integer -> bigint is safe
        let changes = vec![Change::AlterColumn {
            table: "users".into(),
            column: "age".into(),
            changes: ColumnChanges {
                data_type: Some((PgType::Integer, PgType::BigInt)),
                ..Default::default()
            },
        }];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert!(
            warnings.is_empty(),
            "widening cast should not produce warning"
        );
    })
    .await;
}

// ── Clean data — no warnings ────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn clean_data_no_warnings() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // Changes that should produce no warnings:
        // - Add a new nullable column (always safe)
        // - Drop an empty table (categories is empty)
        // - Add constraint on column with no violations
        let changes = vec![
            Change::AddColumn {
                table: "users".into(),
                column: Column::new("nickname", PgType::Text).nullable(),
            },
            Change::AddConstraint {
                table: "users".into(),
                constraint: Constraint::Unique {
                    name: Some("users_name_unique".into()),
                    columns: vec!["name".into()],
                },
            },
        ];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert!(
            warnings.is_empty(),
            "clean changes should produce no warnings, got: {:?}",
            warnings.iter().map(|w| &w.description).collect::<Vec<_>>()
        );
    })
    .await;
}

// ── Combined changes ────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn multiple_warnings_from_combined_changes() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let changes = vec![
            // SET NOT NULL on nullable column with NULLs -> error
            Change::AlterColumn {
                table: "users".into(),
                column: "bio".into(),
                changes: ColumnChanges {
                    nullable: Some(false),
                    ..Default::default()
                },
            },
            // UNIQUE on column with duplicates -> error
            Change::AddConstraint {
                table: "posts".into(),
                constraint: Constraint::Unique {
                    name: Some("posts_published_unique".into()),
                    columns: vec!["published".into()],
                },
            },
            // Narrowing type cast -> warning
            Change::AlterColumn {
                table: "users".into(),
                column: "name".into(),
                changes: ColumnChanges {
                    data_type: Some((PgType::Varchar(Some(255)), PgType::Varchar(Some(5)))),
                    ..Default::default()
                },
            },
        ];

        let warnings = warnings::check_changes(&pool, TEST_SCHEMA, &changes)
            .await
            .expect("check_changes should succeed");

        assert_eq!(
            warnings.len(),
            3,
            "should have 3 warnings, got: {:?}",
            warnings.iter().map(|w| &w.description).collect::<Vec<_>>()
        );

        let errors: Vec<&MigrationWarning> = warnings
            .iter()
            .filter(|w| w.severity == Severity::Error)
            .collect();
        let warns: Vec<&MigrationWarning> = warnings
            .iter()
            .filter(|w| w.severity == Severity::Warning)
            .collect();

        assert_eq!(errors.len(), 2, "should have 2 errors");
        assert_eq!(warns.len(), 1, "should have 1 warning");
    })
    .await;
}
