use std::path::Path;

use inara::schema::introspect::introspect;
use inara::schema::types::PgType;
use inara::schema::{Constraint, CustomTypeKind};
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
    // Execute each statement separately (sqlx doesn't support multi-statement by default)
    for statement in sql.split(';') {
        // Strip comment lines before checking if the statement is empty
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

/// Helper: run setup, execute test, teardown regardless of result
async fn with_test_schema<F, Fut>(f: F)
where
    F: FnOnce(PgPool) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let pool = setup_pool().await;
    setup_test_schema(&pool).await;

    // Run the test
    f(pool.clone()).await;

    teardown_test_schema(&pool).await;
}

// ── Core introspection tests ────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn introspect_empty_database() {
    let pool = setup_pool().await;

    // Create an empty schema, introspect it, then drop it
    sqlx::query("CREATE SCHEMA IF NOT EXISTS inara_empty_test")
        .execute(&pool)
        .await
        .expect("create schema");

    let schema = introspect(&pool, "inara_empty_test")
        .await
        .expect("introspect should succeed on empty schema");

    assert!(schema.tables.is_empty(), "expected no tables");
    assert!(schema.enums.is_empty(), "expected no enums");
    assert!(schema.types.is_empty(), "expected no custom types");

    sqlx::query("DROP SCHEMA IF EXISTS inara_empty_test CASCADE")
        .execute(&pool)
        .await
        .expect("cleanup");
}

#[tokio::test]
#[ignore]
async fn introspect_tables_exist() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let table_names: Vec<&str> = schema.table_names().collect();
        assert!(table_names.contains(&"users"), "missing users table");
        assert!(table_names.contains(&"posts"), "missing posts table");
        assert!(table_names.contains(&"comments"), "missing comments table");
        assert!(
            table_names.contains(&"categories"),
            "missing categories table"
        );
        assert_eq!(table_names.len(), 4, "expected exactly 4 tables");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_columns() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let users = schema.table("users").expect("users table");

        // Check column count
        assert_eq!(users.columns.len(), 7, "users should have 7 columns");

        // Check specific columns
        let id = users.column("id").expect("id column");
        assert_eq!(id.pg_type, PgType::Uuid);
        assert!(!id.nullable);

        let email = users.column("email").expect("email column");
        // email uses a domain type, so it maps to Custom
        assert_eq!(email.pg_type, PgType::Custom("email".into()));
        assert!(!email.nullable);

        let name = users.column("name").expect("name column");
        assert_eq!(name.pg_type, PgType::Varchar(Some(255)));
        assert!(!name.nullable);

        let age = users.column("age").expect("age column");
        assert_eq!(age.pg_type, PgType::Integer);
        assert!(age.nullable);

        let bio = users.column("bio").expect("bio column");
        assert_eq!(bio.pg_type, PgType::Text);
        assert!(bio.nullable);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_column_defaults() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let users = schema.table("users").expect("users table");
        let id = users.column("id").expect("id column");
        assert!(
            id.default.is_some(),
            "id should have a default (gen_random_uuid)"
        );

        let created_at = users.column("created_at").expect("created_at column");
        assert!(
            created_at.default.is_some(),
            "created_at should have a default (now())"
        );

        let age = users.column("age").expect("age column");
        assert!(age.default.is_none(), "age should have no default");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_primary_keys() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        // Users PK
        let users = schema.table("users").expect("users table");
        let pk = users.primary_key().expect("users should have a PK");
        match pk {
            Constraint::PrimaryKey { columns, .. } => {
                assert_eq!(columns, &["id"]);
            }
            _ => panic!("expected PrimaryKey"),
        }

        // Posts PK (bigserial)
        let posts = schema.table("posts").expect("posts table");
        let pk = posts.primary_key().expect("posts should have a PK");
        match pk {
            Constraint::PrimaryKey { columns, .. } => {
                assert_eq!(columns, &["id"]);
            }
            _ => panic!("expected PrimaryKey"),
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_foreign_keys() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let posts = schema.table("posts").expect("posts table");
        let fks = posts.foreign_keys();
        assert_eq!(fks.len(), 1, "posts should have 1 FK");

        match &fks[0] {
            Constraint::ForeignKey {
                name,
                columns,
                references,
                on_delete,
                on_update,
            } => {
                assert_eq!(name.as_deref(), Some("posts_author_fk"));
                assert_eq!(columns, &["author_id"]);
                assert_eq!(references.table, "users");
                assert_eq!(references.columns, &["id"]);
                assert_eq!(
                    *on_delete,
                    Some(inara::schema::types::ReferentialAction::Cascade)
                );
                assert_eq!(
                    *on_update,
                    Some(inara::schema::types::ReferentialAction::NoAction)
                );
            }
            _ => panic!("expected ForeignKey"),
        }

        // Comments should have 2 FKs
        let comments = schema.table("comments").expect("comments table");
        let fks = comments.foreign_keys();
        assert_eq!(fks.len(), 2, "comments should have 2 FKs");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_self_referential_fk() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let categories = schema.table("categories").expect("categories table");
        let fks = categories.foreign_keys();
        assert_eq!(fks.len(), 1, "categories should have 1 self-referential FK");

        match &fks[0] {
            Constraint::ForeignKey {
                name,
                columns,
                references,
                on_delete,
                ..
            } => {
                assert_eq!(name.as_deref(), Some("categories_parent_fk"));
                assert_eq!(columns, &["parent_id"]);
                assert_eq!(references.table, "categories");
                assert_eq!(references.columns, &["id"]);
                assert_eq!(
                    *on_delete,
                    Some(inara::schema::types::ReferentialAction::SetNull)
                );
            }
            _ => panic!("expected ForeignKey"),
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_unique_constraints() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let users = schema.table("users").expect("users table");
        let uniques: Vec<_> = users
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::Unique { .. }))
            .collect();
        assert!(
            !uniques.is_empty(),
            "users should have at least one unique constraint"
        );

        let email_unique = uniques
            .iter()
            .find(|c| match c {
                Constraint::Unique { name, .. } => name.as_deref() == Some("users_email_key"),
                _ => false,
            })
            .expect("should find users_email_key unique constraint");

        match email_unique {
            Constraint::Unique { columns, .. } => {
                assert_eq!(columns, &["email"]);
            }
            _ => panic!("expected Unique"),
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_check_constraints() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let users = schema.table("users").expect("users table");
        let checks: Vec<_> = users
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::Check { .. }))
            .collect();

        // Should have users_age_check, but NOT the NOT NULL checks
        let age_check = checks
            .iter()
            .find(|c| match c {
                Constraint::Check { name, .. } => name.as_deref() == Some("users_age_check"),
                _ => false,
            })
            .expect("should find users_age_check");

        match age_check {
            Constraint::Check { expression, .. } => {
                assert!(
                    expression.contains("age"),
                    "check expression should reference age"
                );
            }
            _ => panic!("expected Check"),
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_indexes() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let posts = schema.table("posts").expect("posts table");

        // Should have: posts_author_idx, posts_author_created_idx, posts_published_idx
        assert!(
            posts.indexes.len() >= 3,
            "posts should have at least 3 indexes, got {}",
            posts.indexes.len()
        );

        // Simple index
        let author_idx = posts
            .indexes
            .iter()
            .find(|i| i.name == "posts_author_idx")
            .expect("should find posts_author_idx");
        assert_eq!(author_idx.columns, vec!["author_id"]);
        assert!(!author_idx.unique);
        assert!(author_idx.partial.is_none());

        // Composite index
        let composite_idx = posts
            .indexes
            .iter()
            .find(|i| i.name == "posts_author_created_idx")
            .expect("should find posts_author_created_idx");
        assert_eq!(composite_idx.columns, vec!["author_id", "created_at"]);
        assert!(!composite_idx.unique);

        // Partial index
        let partial_idx = posts
            .indexes
            .iter()
            .find(|i| i.name == "posts_published_idx")
            .expect("should find posts_published_idx");
        assert!(
            partial_idx.partial.is_some(),
            "posts_published_idx should be a partial index"
        );
        let filter = partial_idx.partial.as_ref().unwrap();
        assert!(
            filter.contains("published"),
            "partial index filter should reference 'published'"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_unique_index() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let users = schema.table("users").expect("users table");
        let unique_idx = users
            .indexes
            .iter()
            .find(|i| i.name == "users_name_email_idx")
            .expect("should find users_name_email_idx");
        assert!(unique_idx.unique, "users_name_email_idx should be unique");
        assert_eq!(unique_idx.columns, vec!["name", "email"]);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_enums() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        assert_eq!(schema.enums.len(), 2, "should have 2 enum types");

        let status = schema.enum_type("status").expect("status enum");
        assert_eq!(status.variants, vec!["active", "inactive", "pending"]);

        let priority = schema.enum_type("priority").expect("priority enum");
        assert_eq!(priority.variants, vec!["low", "medium", "high", "critical"]);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_enum_column_maps_to_custom() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let users = schema.table("users").expect("users table");
        let status_col = users.column("status").expect("status column");
        assert_eq!(
            status_col.pg_type,
            PgType::Custom("status".into()),
            "enum column should map to PgType::Custom"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_array_columns() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let posts = schema.table("posts").expect("posts table");
        let tags = posts.column("tags").expect("tags column");
        assert_eq!(
            tags.pg_type,
            PgType::Array(Box::new(PgType::Text)),
            "tags should be text[]"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_composite_type() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let address = schema.types.get("address").expect("address composite type");
        match &address.kind {
            CustomTypeKind::Composite { fields } => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "street");
                assert_eq!(fields[0].1, PgType::Text);
                assert_eq!(fields[1].0, "city");
                assert_eq!(fields[1].1, PgType::Text);
                assert_eq!(fields[2].0, "zip");
                assert_eq!(fields[2].1, PgType::Varchar(Some(10)));
            }
            other => panic!("expected Composite, got {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_domain_type() {
    with_test_schema(|pool| async move {
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        let email = schema.types.get("email").expect("email domain type");
        match &email.kind {
            CustomTypeKind::Domain {
                base_type,
                constraints,
            } => {
                assert_eq!(*base_type, PgType::Text);
                assert!(
                    !constraints.is_empty(),
                    "email domain should have check constraints"
                );
            }
            other => panic!("expected Domain, got {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn introspect_excludes_system_schemas() {
    with_test_schema(|pool| async move {
        // Introspecting pg_catalog should give us nothing useful
        // (we should never load pg_catalog tables as user tables)
        let schema = introspect(&pool, TEST_SCHEMA)
            .await
            .expect("introspect failed");

        // None of the system tables should appear
        for name in schema.table_names() {
            assert!(
                !name.starts_with("pg_"),
                "system table {name} should not appear"
            );
        }
    })
    .await;
}
