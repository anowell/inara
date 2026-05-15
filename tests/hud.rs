//! Integration tests for the Query HUD.
//!
//! Tier 0 (catalog / `pg_stats` loading) and Tier 1 (exact probes) both run
//! real SQL against a Postgres instance, so these are `#[ignore]`d and run via
//! `just test-integration`. They validate that the queries are *valid* and
//! return correct exact values; the pure interpretation logic is unit-tested
//! separately and needs no database.

use std::path::Path;

use inara::schema::profile_load::{load_column_profile, load_table_profile};
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

async fn with_test_schema<F, Fut>(f: F)
where
    F: FnOnce(PgPool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let pool = setup_pool().await;
    run_fixture(&pool, "setup.sql").await;
    f(pool.clone()).await;
    run_fixture(&pool, "teardown.sql").await;
}

/// Insert known test data and `ANALYZE` so planner statistics exist.
async fn insert_and_analyze(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO inara_test.users (id, email, name, status, age, bio, created_at) VALUES
         ('a0000000-0000-0000-0000-000000000001', 'alice@test.com', 'Alice', 'active', 30, 'Hello', '2025-01-01T00:00:00Z'),
         ('a0000000-0000-0000-0000-000000000002', 'bob@test.com', 'Bob', 'inactive', 25, NULL, '2025-06-15T12:00:00Z'),
         ('a0000000-0000-0000-0000-000000000003', 'carol@test.com', 'Carol', 'pending', 40, 'Hi', '2025-12-31T23:59:59Z')",
    )
    .execute(pool)
    .await
    .expect("insert users");

    sqlx::query(
        "INSERT INTO inara_test.posts (author_id, title, body, metadata, score, published, created_at) VALUES
         ('a0000000-0000-0000-0000-000000000001', 'Post 1', 'Body 1', '{\"views\": 10, \"featured\": true}', 4.50, true, '2025-02-01T00:00:00Z'),
         ('a0000000-0000-0000-0000-000000000001', 'Post 2', NULL, '{\"views\": 3, \"featured\": false}', 2.00, false, '2025-03-01T00:00:00Z'),
         ('a0000000-0000-0000-0000-000000000002', 'Post 3', 'Body 3', '{\"views\": 7}', 0.00, true, '2025-04-01T00:00:00Z')",
    )
    .execute(pool)
    .await
    .expect("insert posts");

    sqlx::query("ANALYZE inara_test.users")
        .execute(pool)
        .await
        .expect("analyze users");
    sqlx::query("ANALYZE inara_test.posts")
        .execute(pool)
        .await
        .expect("analyze posts");
}

// ── Tier 0: table profile ──────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn table_profile_estimates_rows_after_analyze() {
    with_test_schema(|pool| async move {
        insert_and_analyze(&pool).await;

        let profile = load_table_profile(&pool, TEST_SCHEMA, "users")
            .await
            .expect("load_table_profile should succeed");

        assert!(profile.is_analyzed(), "users has been analyzed");
        assert_eq!(profile.estimated_rows, Some(3), "users has 3 rows");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn table_profile_reports_sizes() {
    with_test_schema(|pool| async move {
        insert_and_analyze(&pool).await;

        let profile = load_table_profile(&pool, TEST_SCHEMA, "users")
            .await
            .expect("load_table_profile should succeed");

        assert!(profile.heap_bytes > 0, "non-empty table has a heap");
        assert!(profile.index_bytes > 0, "users has a PK + unique indexes");
        assert!(profile.total_bytes() >= profile.heap_bytes);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn table_profile_loads_for_empty_table() {
    with_test_schema(|pool| async move {
        // categories has no rows and has not been analyzed — must still load.
        let profile = load_table_profile(&pool, TEST_SCHEMA, "categories")
            .await
            .expect("load_table_profile should succeed for an empty table");

        assert!(profile.heap_bytes >= 0);
        assert!(profile.index_bytes >= 0);
    })
    .await;
}

// ── Tier 0: column profile ─────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn column_profile_reports_null_fraction() {
    with_test_schema(|pool| async move {
        insert_and_analyze(&pool).await;

        // bio: Alice "Hello", Bob NULL, Carol "Hi" — 1 of 3 is null.
        let profile = load_column_profile(&pool, TEST_SCHEMA, "users", "bio")
            .await
            .expect("load_column_profile should succeed");

        assert!(profile.analyzed, "bio has pg_stats after ANALYZE");
        assert!(
            profile.null_frac > 0.0,
            "bio has a null value, expected null_frac > 0, got {}",
            profile.null_frac
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn column_profile_loads_enum_column() {
    with_test_schema(|pool| async move {
        insert_and_analyze(&pool).await;

        // status is a Postgres enum — exercises the anyarray->text cast path.
        let profile = load_column_profile(&pool, TEST_SCHEMA, "users", "status")
            .await
            .expect("load_column_profile should succeed for an enum column");

        assert!(profile.analyzed);
        assert!(
            profile.n_distinct != 0.0,
            "status has a distinctness signal"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn column_profile_unavailable_when_not_analyzed() {
    with_test_schema(|pool| async move {
        // No inserts, no ANALYZE — pg_stats has no row for this column.
        let profile = load_column_profile(&pool, TEST_SCHEMA, "users", "bio")
            .await
            .expect("load_column_profile should succeed even with no stats");

        assert!(
            !profile.analyzed,
            "an unanalyzed column reports no statistics"
        );
    })
    .await;
}

// ── Tier 1: exact probes ───────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn probe_table_counts_rows_exactly() {
    with_test_schema(|pool| async move {
        insert_and_analyze(&pool).await;

        let probe = hud::probe_table(&pool, TEST_SCHEMA, "users")
            .await
            .expect("probe_table should succeed");

        assert_eq!(probe.exact_rows, 3);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn probe_table_handles_empty_table() {
    with_test_schema(|pool| async move {
        let probe = hud::probe_table(&pool, TEST_SCHEMA, "categories")
            .await
            .expect("probe_table should succeed");

        assert_eq!(probe.exact_rows, 0);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn probe_column_counts_distinct() {
    with_test_schema(|pool| async move {
        insert_and_analyze(&pool).await;

        // bio: "Hello", NULL, "Hi" — 2 distinct non-null values.
        let probe = hud::probe_column(&pool, TEST_SCHEMA, "users", "bio", &PgType::Text)
            .await
            .expect("probe_column should succeed");

        assert_eq!(probe.exact_rows, 3);
        assert_eq!(probe.exact_distinct, Some(2));
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn probe_column_distinct_for_enum() {
    with_test_schema(|pool| async move {
        insert_and_analyze(&pool).await;

        let probe = hud::probe_column(
            &pool,
            TEST_SCHEMA,
            "users",
            "status",
            &PgType::Custom("status".into()),
        )
        .await
        .expect("probe_column should succeed");

        assert_eq!(probe.exact_distinct, Some(3), "3 distinct statuses");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn probe_column_samples_jsonb_keys() {
    with_test_schema(|pool| async move {
        insert_and_analyze(&pool).await;

        let probe = hud::probe_column(&pool, TEST_SCHEMA, "posts", "metadata", &PgType::Jsonb)
            .await
            .expect("probe_column should succeed for jsonb");

        assert_eq!(probe.exact_rows, 3);
        assert!(probe.jsonb_sample_rows > 0, "a sample bound was applied");

        let keys: Vec<&str> = probe.jsonb_keys.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"views"), "sampled keys: {keys:?}");
        assert!(keys.contains(&"featured"), "sampled keys: {keys:?}");

        // `views` is present in all 3 rows, `featured` in 2.
        let count_of = |needle: &str| {
            probe
                .jsonb_keys
                .iter()
                .find(|(k, _)| k == needle)
                .map(|(_, n)| *n)
        };
        assert_eq!(count_of("views"), Some(3));
        assert_eq!(count_of("featured"), Some(2));
    })
    .await;
}
