use std::path::Path;

use inara::schema::types::PgType;
use inara::tui::hud;
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

/// Insert known test data for HUD queries.
async fn insert_test_data(pool: &PgPool) {
    // Insert users
    sqlx::query(
        "INSERT INTO inara_test.users (id, email, name, status, age, bio, created_at) VALUES
         ('a0000000-0000-0000-0000-000000000001', 'alice@test.com', 'Alice', 'active', 30, 'Hello', '2025-01-01T00:00:00Z'),
         ('a0000000-0000-0000-0000-000000000002', 'bob@test.com', 'Bob', 'inactive', 25, NULL, '2025-06-15T12:00:00Z'),
         ('a0000000-0000-0000-0000-000000000003', 'carol@test.com', 'Carol', 'pending', 40, 'Hi', '2025-12-31T23:59:59Z')"
    )
    .execute(pool)
    .await
    .expect("insert users");

    // Insert posts
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

// ── Table-level HUD tests ──────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn hud_table_stats_row_count() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let stats = hud::query_table_stats(&pool, TEST_SCHEMA, "users")
            .await
            .expect("query_table_stats should succeed");

        assert_eq!(stats.row_count, 3, "users should have 3 rows");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_table_stats_size() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // Run ANALYZE to update pg_class stats
        sqlx::query("ANALYZE inara_test.users")
            .execute(&pool)
            .await
            .expect("analyze");

        let stats = hud::query_table_stats(&pool, TEST_SCHEMA, "users")
            .await
            .expect("query_table_stats should succeed");

        assert!(stats.size_bytes >= 0, "size should be non-negative");
        assert!(
            !stats.size_display.is_empty(),
            "size_display should not be empty"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_table_stats_indexed_columns() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // users table has indexes on: id (PK), email (unique), name+email (composite unique)
        let stats = hud::query_table_stats(&pool, TEST_SCHEMA, "users")
            .await
            .expect("query_table_stats should succeed");

        assert!(
            stats.indexed_columns.contains(&"id".to_string()),
            "id should be indexed (PK)"
        );
        assert!(
            stats.indexed_columns.contains(&"email".to_string()),
            "email should be indexed (unique constraint)"
        );
        assert!(
            stats.indexed_columns.contains(&"name".to_string()),
            "name should be indexed (composite unique index)"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_table_stats_posts() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let stats = hud::query_table_stats(&pool, TEST_SCHEMA, "posts")
            .await
            .expect("query_table_stats should succeed");

        assert_eq!(stats.row_count, 3, "posts should have 3 rows");

        // posts has indexes on: id (PK), author_id, (author_id, created_at), created_at (partial)
        assert!(
            stats.indexed_columns.contains(&"id".to_string()),
            "id should be indexed"
        );
        assert!(
            stats.indexed_columns.contains(&"author_id".to_string()),
            "author_id should be indexed"
        );
    })
    .await;
}

// ── Column-level HUD tests ─────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn hud_column_stats_null_count() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let stats = hud::query_column_stats(&pool, TEST_SCHEMA, "users", "bio", &PgType::Text)
            .await
            .expect("query_column_stats should succeed");

        // Alice has bio "Hello", Bob has NULL, Carol has "Hi"
        assert_eq!(stats.null_count, 1, "bio should have 1 null");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_column_stats_distinct_count() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let stats = hud::query_column_stats(
            &pool,
            TEST_SCHEMA,
            "users",
            "status",
            &PgType::Custom("status".into()),
        )
        .await
        .expect("query_column_stats should succeed");

        // 3 users with 3 different statuses: active, inactive, pending
        assert_eq!(
            stats.distinct_count, 3,
            "status should have 3 distinct values"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_column_stats_numeric_min_max_avg() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let stats = hud::query_column_stats(&pool, TEST_SCHEMA, "users", "age", &PgType::Integer)
            .await
            .expect("query_column_stats should succeed");

        // Ages: 30, 25, 40 (Bob has NULL age? No — check: age is nullable)
        // Actually per fixture, Alice=30, Bob=25, Carol=40
        assert_eq!(
            stats.min_value.as_deref(),
            Some("25"),
            "min age should be 25"
        );
        assert_eq!(
            stats.max_value.as_deref(),
            Some("40"),
            "max age should be 40"
        );
        assert!(
            stats.avg_value.is_some(),
            "avg should be present for numeric"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_column_stats_date_min_max() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let stats = hud::query_column_stats(
            &pool,
            TEST_SCHEMA,
            "users",
            "created_at",
            &PgType::Timestamptz,
        )
        .await
        .expect("query_column_stats should succeed");

        assert!(
            stats.min_value.is_some(),
            "min should be present for timestamptz"
        );
        assert!(
            stats.max_value.is_some(),
            "max should be present for timestamptz"
        );
        assert!(
            stats.avg_value.is_none(),
            "avg should NOT be present for timestamptz"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_column_stats_text_no_min_max() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let stats = hud::query_column_stats(
            &pool,
            TEST_SCHEMA,
            "users",
            "name",
            &PgType::Varchar(Some(255)),
        )
        .await
        .expect("query_column_stats should succeed");

        // Text/varchar columns should NOT have min/max/avg
        assert!(
            stats.min_value.is_none(),
            "text columns should not have min"
        );
        assert!(
            stats.max_value.is_none(),
            "text columns should not have max"
        );
        assert!(
            stats.avg_value.is_none(),
            "text columns should not have avg"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_column_stats_numeric_score() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        let stats = hud::query_column_stats(
            &pool,
            TEST_SCHEMA,
            "posts",
            "score",
            &PgType::Numeric(Some((5, 2))),
        )
        .await
        .expect("query_column_stats should succeed");

        // Scores: 4.50, 2.00, 0.00
        assert_eq!(stats.min_value.as_deref(), Some("0.00"), "min score");
        assert_eq!(stats.max_value.as_deref(), Some("4.50"), "max score");
        assert!(stats.avg_value.is_some(), "avg should be present");
        assert_eq!(stats.null_count, 0, "no null scores");
        assert_eq!(stats.distinct_count, 3, "3 distinct scores");
    })
    .await;
}

// ── Safety check tests ─────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn hud_safety_check_small_table() {
    with_test_schema(|pool| async move {
        insert_test_data(&pool).await;

        // Run ANALYZE so pg_class.reltuples is accurate
        sqlx::query("ANALYZE inara_test.users")
            .execute(&pool)
            .await
            .expect("analyze");

        // users table has only 3 rows — well below the threshold
        let result = hud::check_safety(&pool, TEST_SCHEMA, "users", "bio")
            .await
            .expect("check_safety should succeed");

        assert!(
            result.is_none(),
            "small table should not trigger safety warning"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_safety_check_large_unindexed_triggers_warning() {
    with_test_schema(|pool| async move {
        // Artificially set reltuples to a high value to simulate a large table
        // We do this by directly updating pg_class (requires superuser or owner)
        sqlx::query(
            "UPDATE pg_class
             SET reltuples = 500000
             WHERE relname = 'users'
               AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)",
        )
        .bind(TEST_SCHEMA)
        .execute(&pool)
        .await
        .expect("update reltuples");

        // bio column is NOT indexed
        let result = hud::check_safety(&pool, TEST_SCHEMA, "users", "bio")
            .await
            .expect("check_safety should succeed");

        assert!(
            result.is_some(),
            "large table with unindexed column should trigger warning"
        );
        let estimate = result.unwrap();
        assert!(estimate >= 100_000.0, "row estimate should be >= threshold");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_safety_check_large_indexed_no_warning() {
    with_test_schema(|pool| async move {
        // Set reltuples high
        sqlx::query(
            "UPDATE pg_class
             SET reltuples = 500000
             WHERE relname = 'users'
               AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)",
        )
        .bind(TEST_SCHEMA)
        .execute(&pool)
        .await
        .expect("update reltuples");

        // id column IS indexed (PK)
        let result = hud::check_safety(&pool, TEST_SCHEMA, "users", "id")
            .await
            .expect("check_safety should succeed");

        assert!(
            result.is_none(),
            "indexed column should not trigger warning even on large table"
        );
    })
    .await;
}

// ── Empty table edge case ───────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn hud_table_stats_empty_table() {
    with_test_schema(|pool| async move {
        // categories table has no inserted data
        let stats = hud::query_table_stats(&pool, TEST_SCHEMA, "categories")
            .await
            .expect("query_table_stats should succeed");

        assert_eq!(stats.row_count, 0, "empty table should have 0 rows");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn hud_column_stats_empty_table() {
    with_test_schema(|pool| async move {
        let stats =
            hud::query_column_stats(&pool, TEST_SCHEMA, "categories", "name", &PgType::Text)
                .await
                .expect("query_column_stats should succeed");

        assert_eq!(stats.null_count, 0, "empty table null count should be 0");
        assert_eq!(
            stats.distinct_count, 0,
            "empty table distinct count should be 0"
        );
    })
    .await;
}
