// Migration file loader and indexing.
//
// Scans a migrations/ directory for *.up.sql files, parses their metadata
// (timestamp, description), and builds an index mapping tables/columns to
// the migrations that affected them.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::pattern::{self, MigrationPattern};

/// A reference to a single migration file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationRef {
    /// Timestamp from the filename (YYYYMMDDHHMMSS).
    pub timestamp: String,
    /// Human-readable description from the filename.
    pub description: String,
    /// Path to the migration file.
    pub path: PathBuf,
    /// Relevant SQL excerpt (the statement that affected the table/column).
    pub excerpt: String,
}

/// A loaded migration file with its full content.
#[derive(Debug, Clone)]
pub struct MigrationFile {
    /// Timestamp from the filename.
    pub timestamp: String,
    /// Human-readable description from the filename.
    pub description: String,
    /// Path to the migration file.
    pub path: PathBuf,
    /// Full SQL content of the file.
    pub sql: String,
}

/// Index mapping schema elements to the migrations that affected them.
///
/// Uses BTreeMap for deterministic ordering.
#[derive(Debug, Clone, Default)]
pub struct MigrationIndex {
    /// Table name → migrations affecting that table.
    pub tables: BTreeMap<String, Vec<MigrationRef>>,
    /// "table.column" → migrations affecting that column.
    pub columns: BTreeMap<String, Vec<MigrationRef>>,
    /// All loaded migration files, ordered by timestamp.
    pub migrations: Vec<MigrationFile>,
}

/// Parse a migration filename into (timestamp, description).
///
/// Expected format: `YYYYMMDDHHMMSS_description.up.sql`
/// Returns `None` if the filename doesn't match the expected pattern.
pub fn parse_filename(filename: &str) -> Option<(String, String)> {
    let stem = filename.strip_suffix(".up.sql")?;
    let (timestamp, description) = stem.split_once('_')?;

    // Validate timestamp is 14 digits
    if timestamp.len() != 14 || !timestamp.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    // Convert underscores to spaces for readability
    let description = description.replace('_', " ");
    Some((timestamp.to_string(), description))
}

/// Scan a directory for migration files and load them.
///
/// Returns migration files sorted by timestamp (ascending).
pub fn scan_migrations(dir: &Path) -> Result<Vec<MigrationFile>, std::io::Error> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut migrations = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|f| f.to_str()) {
            Some(f) => f.to_string(),
            None => continue,
        };

        if !filename.ends_with(".up.sql") {
            continue;
        }

        let (timestamp, description) = match parse_filename(&filename) {
            Some(parsed) => parsed,
            None => continue,
        };

        let sql = std::fs::read_to_string(&path)?;

        migrations.push(MigrationFile {
            timestamp,
            description,
            path,
            sql,
        });
    }

    // Sort by timestamp ascending
    migrations.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    Ok(migrations)
}

/// Scan a directory for migration files using a detected pattern.
///
/// Handles all layout types: flat files, flat up/down files, subdirectory
/// layouts with up.sql/down.sql or named SQL files.
/// Returns migration files sorted by prefix (ascending).
pub fn scan_migrations_with_pattern(
    dir: &Path,
    pat: &MigrationPattern,
) -> Result<Vec<MigrationFile>, std::io::Error> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut migrations = Vec::new();

    match &pat.layout {
        pattern::Layout::Flat | pattern::Layout::FlatUpDown => {
            // Flat layouts: files directly in the migrations directory
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }

                let filename = match path.file_name().and_then(|f| f.to_str()) {
                    Some(f) => f.to_string(),
                    None => continue,
                };

                // Determine the name stem by stripping the extension
                let stem = match &pat.layout {
                    pattern::Layout::FlatUpDown => {
                        if !filename.ends_with(".up.sql") {
                            continue;
                        }
                        filename.strip_suffix(".up.sql").unwrap_or(&filename)
                    }
                    pattern::Layout::Flat => {
                        if !filename.ends_with(".sql") || filename.ends_with(".down.sql") {
                            continue;
                        }
                        filename.strip_suffix(".sql").unwrap_or(&filename)
                    }
                    _ => unreachable!(),
                };

                let (timestamp, description) = match pat.parse_name(stem) {
                    Some(parsed) => parsed,
                    None => continue,
                };

                let sql = std::fs::read_to_string(&path)?;
                migrations.push(MigrationFile {
                    timestamp,
                    description,
                    path,
                    sql,
                });
            }
        }
        pattern::Layout::SubdirUpDown => {
            // Subdirectory layout: each migration is a directory containing up.sql
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }

                let dir_name = match path.file_name().and_then(|f| f.to_str()) {
                    Some(f) => f.to_string(),
                    None => continue,
                };

                let up_path = path.join("up.sql");
                if !up_path.is_file() {
                    continue;
                }

                let (timestamp, description) = match pat.parse_name(&dir_name) {
                    Some(parsed) => parsed,
                    None => continue,
                };

                let sql = std::fs::read_to_string(&up_path)?;
                migrations.push(MigrationFile {
                    timestamp,
                    description,
                    path: up_path,
                    sql,
                });
            }
        }
        pattern::Layout::SubdirSingleFile(sql_filename) => {
            // Subdirectory layout: each migration is a directory containing a named SQL file
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }

                let dir_name = match path.file_name().and_then(|f| f.to_str()) {
                    Some(f) => f.to_string(),
                    None => continue,
                };

                let sql_path = path.join(sql_filename);
                if !sql_path.is_file() {
                    continue;
                }

                let (timestamp, description) = match pat.parse_name(&dir_name) {
                    Some(parsed) => parsed,
                    None => continue,
                };

                let sql = std::fs::read_to_string(&sql_path)?;
                migrations.push(MigrationFile {
                    timestamp,
                    description,
                    path: sql_path,
                    sql,
                });
            }
        }
    }

    // Sort by timestamp/prefix ascending
    migrations.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    Ok(migrations)
}

/// A SQL statement affecting a table or column, extracted from migration SQL.
#[derive(Debug, Clone)]
struct SqlEffect {
    /// The table name affected.
    table: String,
    /// Column names affected (empty for table-level operations like CREATE TABLE).
    columns: Vec<String>,
    /// The SQL statement text (for the excerpt).
    statement: String,
}

/// Analyze SQL content to extract table and column effects.
///
/// Uses simple regex-like pattern matching for common DDL statements.
/// This is best-effort — non-standard SQL still loads without crashing.
fn analyze_sql(sql: &str) -> Vec<SqlEffect> {
    let mut effects = Vec::new();

    for statement in split_statements(sql) {
        let normalized = normalize_whitespace(&statement);
        let upper = normalized.to_uppercase();

        if let Some(effect) = parse_create_table(&upper, &statement) {
            effects.push(effect);
        } else if let Some(effect) = parse_alter_table(&upper, &statement) {
            effects.push(effect);
        } else if let Some(effect) = parse_drop_table(&upper, &statement) {
            effects.push(effect);
        }
    }

    effects
}

/// Split SQL content into individual statements on semicolons.
///
/// Simple split — doesn't handle semicolons inside string literals,
/// but good enough for DDL migration files.
fn split_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Normalize whitespace: collapse runs of whitespace to single spaces.
fn normalize_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Extract table name from a CREATE TABLE statement.
///
/// Handles: `CREATE TABLE name (`, `CREATE TABLE IF NOT EXISTS name (`
fn parse_create_table(upper: &str, original: &str) -> Option<SqlEffect> {
    let rest = upper.strip_prefix("CREATE TABLE ")?;
    let rest = rest.strip_prefix("IF NOT EXISTS ").unwrap_or(rest);

    let table = extract_identifier(rest)?;

    // Extract column names from the CREATE TABLE body
    let columns = extract_create_table_columns(original);

    Some(SqlEffect {
        table,
        columns,
        statement: truncate_statement(original),
    })
}

/// Extract column names from a CREATE TABLE body.
///
/// Looks for lines between parentheses that look like column definitions
/// (identifier followed by a type keyword).
fn extract_create_table_columns(sql: &str) -> Vec<String> {
    let mut columns = Vec::new();

    // Find content between first ( and last )
    let open = match sql.find('(') {
        Some(i) => i + 1,
        None => return columns,
    };
    let close = match sql.rfind(')') {
        Some(i) => i,
        None => return columns,
    };
    if open >= close {
        return columns;
    }

    let body = &sql[open..close];
    for part in body.split(',') {
        let trimmed = part.trim();
        let upper = trimmed.to_uppercase();

        // Skip constraint lines
        if upper.starts_with("CONSTRAINT ")
            || upper.starts_with("PRIMARY KEY")
            || upper.starts_with("FOREIGN KEY")
            || upper.starts_with("UNIQUE")
            || upper.starts_with("CHECK")
        {
            continue;
        }

        // First word is the column name
        if let Some(name) = extract_identifier(trimmed) {
            // Validate it looks like a column def (has a type after the name)
            let rest = trimmed[name.len()..].trim();
            if !rest.is_empty() {
                columns.push(name);
            }
        }
    }

    columns
}

/// Extract table name and column effects from ALTER TABLE statements.
///
/// Handles:
/// - ALTER TABLE name ADD COLUMN col_name ...
/// - ALTER TABLE name ALTER COLUMN col_name ...
/// - ALTER TABLE name DROP COLUMN col_name
/// - ALTER TABLE name RENAME COLUMN old TO new
/// - ALTER TABLE name ADD CONSTRAINT ...
/// - ALTER TABLE name DROP CONSTRAINT ...
fn parse_alter_table(upper: &str, original: &str) -> Option<SqlEffect> {
    let rest = upper.strip_prefix("ALTER TABLE ")?;
    let table = extract_identifier(rest)?;
    let after_table = rest[table.len()..].trim();

    let mut columns = Vec::new();

    if let Some(rest) = after_table.strip_prefix("ADD COLUMN ") {
        if let Some(col) = extract_identifier(rest) {
            columns.push(col);
        }
    } else if let Some(rest) = after_table.strip_prefix("ALTER COLUMN ") {
        if let Some(col) = extract_identifier(rest) {
            columns.push(col);
        }
    } else if let Some(rest) = after_table.strip_prefix("DROP COLUMN ") {
        let rest = rest.strip_prefix("IF EXISTS ").unwrap_or(rest);
        if let Some(col) = extract_identifier(rest) {
            columns.push(col);
        }
    } else if let Some(rest) = after_table.strip_prefix("RENAME COLUMN ") {
        if let Some(old_name) = extract_identifier(rest) {
            columns.push(old_name.clone());
            // Also capture the new name after TO
            let after_old = rest[old_name.len()..].trim();
            if let Some(rest) = after_old.strip_prefix("TO ") {
                if let Some(new_name) = extract_identifier(rest) {
                    columns.push(new_name);
                }
            }
        }
    }
    // ADD CONSTRAINT, DROP CONSTRAINT, RENAME TABLE — table-level effects only

    Some(SqlEffect {
        table,
        columns,
        statement: truncate_statement(original),
    })
}

/// Extract table name from DROP TABLE statements.
fn parse_drop_table(upper: &str, original: &str) -> Option<SqlEffect> {
    let rest = upper.strip_prefix("DROP TABLE ")?;
    let rest = rest.strip_prefix("IF EXISTS ").unwrap_or(rest);
    let table = extract_identifier(rest)?;

    Some(SqlEffect {
        table,
        columns: Vec::new(),
        statement: truncate_statement(original),
    })
}

/// Extract an SQL identifier from the start of a string.
///
/// Handles unquoted identifiers (alphanumeric + underscore) and
/// double-quoted identifiers.
fn extract_identifier(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    if let Some(stripped) = s.strip_prefix('"') {
        // Quoted identifier
        let end = stripped.find('"')?;
        let name = &stripped[..end];
        if name.is_empty() {
            return None;
        }
        Some(name.to_lowercase())
    } else {
        // Unquoted identifier
        let end = s
            .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .unwrap_or(s.len());
        if end == 0 {
            return None;
        }
        Some(s[..end].to_lowercase())
    }
}

/// Truncate a SQL statement to a reasonable excerpt length.
fn truncate_statement(s: &str) -> String {
    let normalized = normalize_whitespace(s);
    if normalized.len() <= 120 {
        normalized
    } else {
        format!("{}...", &normalized[..117])
    }
}

/// Build a MigrationIndex from a list of loaded migration files.
pub fn build_index(migrations: &[MigrationFile]) -> MigrationIndex {
    let mut index = MigrationIndex {
        tables: BTreeMap::new(),
        columns: BTreeMap::new(),
        migrations: migrations.to_vec(),
    };

    for migration in migrations {
        let effects = analyze_sql(&migration.sql);

        for effect in effects {
            // Add table-level reference
            let table_refs = index.tables.entry(effect.table.clone()).or_default();
            // Avoid duplicate references from the same migration
            if !table_refs
                .iter()
                .any(|r| r.timestamp == migration.timestamp)
            {
                table_refs.push(MigrationRef {
                    timestamp: migration.timestamp.clone(),
                    description: migration.description.clone(),
                    path: migration.path.clone(),
                    excerpt: effect.statement.clone(),
                });
            }

            // Add column-level references
            for col in &effect.columns {
                let key = format!("{}.{}", effect.table, col);
                let col_refs = index.columns.entry(key).or_default();
                col_refs.push(MigrationRef {
                    timestamp: migration.timestamp.clone(),
                    description: migration.description.clone(),
                    path: migration.path.clone(),
                    excerpt: effect.statement.clone(),
                });
            }
        }
    }

    index
}

/// Load migrations from a directory and build the index.
///
/// This is the main entry point — combines scanning and indexing.
pub fn load_and_index(dir: &Path) -> Result<MigrationIndex, std::io::Error> {
    let migrations = scan_migrations(dir)?;
    Ok(build_index(&migrations))
}

/// Load migrations using a detected pattern and build the index.
pub fn load_and_index_with_pattern(
    dir: &Path,
    pat: &MigrationPattern,
) -> Result<MigrationIndex, std::io::Error> {
    let migrations = scan_migrations_with_pattern(dir, pat)?;
    Ok(build_index(&migrations))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // --- Filename parsing ---

    #[test]
    fn parse_valid_filename() {
        let result = parse_filename("20260214120000_add_users_table.up.sql");
        assert_eq!(
            result,
            Some(("20260214120000".into(), "add users table".into()))
        );
    }

    #[test]
    fn parse_filename_single_word_description() {
        let result = parse_filename("20260214120000_initial.up.sql");
        assert_eq!(result, Some(("20260214120000".into(), "initial".into())));
    }

    #[test]
    fn parse_filename_not_up_sql() {
        assert_eq!(parse_filename("20260214120000_foo.down.sql"), None);
    }

    #[test]
    fn parse_filename_wrong_timestamp_length() {
        assert_eq!(parse_filename("2026_foo.up.sql"), None);
    }

    #[test]
    fn parse_filename_no_underscore() {
        assert_eq!(parse_filename("20260214120000.up.sql"), None);
    }

    #[test]
    fn parse_filename_non_digit_timestamp() {
        assert_eq!(parse_filename("2026021412abcd_foo.up.sql"), None);
    }

    // --- SQL analysis ---

    #[test]
    fn analyze_create_table() {
        let sql = "CREATE TABLE users (\n    id uuid NOT NULL,\n    email text\n);";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "users");
        assert!(effects[0].columns.contains(&"id".to_string()));
        assert!(effects[0].columns.contains(&"email".to_string()));
    }

    #[test]
    fn analyze_create_table_if_not_exists() {
        let sql = "CREATE TABLE IF NOT EXISTS users (id uuid);";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "users");
    }

    #[test]
    fn analyze_alter_table_add_column() {
        let sql = "ALTER TABLE users ADD COLUMN bio text;";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "users");
        assert_eq!(effects[0].columns, vec!["bio"]);
    }

    #[test]
    fn analyze_alter_table_alter_column() {
        let sql = "ALTER TABLE users ALTER COLUMN email SET NOT NULL;";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "users");
        assert_eq!(effects[0].columns, vec!["email"]);
    }

    #[test]
    fn analyze_alter_table_drop_column() {
        let sql = "ALTER TABLE users DROP COLUMN legacy_field;";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "users");
        assert_eq!(effects[0].columns, vec!["legacy_field"]);
    }

    #[test]
    fn analyze_alter_table_rename_column() {
        let sql = "ALTER TABLE users RENAME COLUMN name TO full_name;";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "users");
        assert!(effects[0].columns.contains(&"name".to_string()));
        assert!(effects[0].columns.contains(&"full_name".to_string()));
    }

    #[test]
    fn analyze_drop_table() {
        let sql = "DROP TABLE legacy_table;";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "legacy_table");
        assert!(effects[0].columns.is_empty());
    }

    #[test]
    fn analyze_drop_table_if_exists() {
        let sql = "DROP TABLE IF EXISTS legacy_table;";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "legacy_table");
    }

    #[test]
    fn analyze_alter_table_add_constraint() {
        let sql = "ALTER TABLE posts ADD CONSTRAINT posts_author_fk FOREIGN KEY (author_id) REFERENCES users(id);";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "posts");
        // Constraint is table-level, no column effects
        assert!(effects[0].columns.is_empty());
    }

    #[test]
    fn analyze_multiple_statements() {
        let sql = "\
CREATE TABLE users (
    id uuid NOT NULL,
    email text
);

ALTER TABLE users ADD COLUMN bio text;

CREATE TABLE posts (
    id uuid NOT NULL,
    author_id uuid NOT NULL
);";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 3);
        assert_eq!(effects[0].table, "users");
        assert_eq!(effects[1].table, "users");
        assert_eq!(effects[1].columns, vec!["bio"]);
        assert_eq!(effects[2].table, "posts");
    }

    #[test]
    fn analyze_create_table_skips_constraints() {
        let sql = "CREATE TABLE posts (
    id uuid NOT NULL,
    author_id uuid NOT NULL,
    CONSTRAINT posts_pkey PRIMARY KEY (id),
    FOREIGN KEY (author_id) REFERENCES users(id)
);";
        let effects = analyze_sql(sql);
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].table, "posts");
        // Should only have id and author_id, not constraint names
        assert_eq!(effects[0].columns.len(), 2);
        assert!(effects[0].columns.contains(&"id".to_string()));
        assert!(effects[0].columns.contains(&"author_id".to_string()));
    }

    #[test]
    fn analyze_nonstandard_sql_doesnt_crash() {
        let sql =
            "-- This is a comment\nSELECT 1;\nINSERT INTO foo VALUES (1);\nSOME RANDOM STUFF;";
        let effects = analyze_sql(sql);
        // Should not crash, may produce zero effects
        assert!(effects.is_empty());
    }

    #[test]
    fn analyze_empty_sql() {
        let effects = analyze_sql("");
        assert!(effects.is_empty());
    }

    // --- Index building ---

    #[test]
    fn build_index_from_migrations() {
        let migrations = vec![
            MigrationFile {
                timestamp: "20260101000000".into(),
                description: "create users".into(),
                path: PathBuf::from("migrations/20260101000000_create_users.up.sql"),
                sql: "CREATE TABLE users (\n    id uuid NOT NULL,\n    email text\n);".into(),
            },
            MigrationFile {
                timestamp: "20260102000000".into(),
                description: "add bio to users".into(),
                path: PathBuf::from("migrations/20260102000000_add_bio_to_users.up.sql"),
                sql: "ALTER TABLE users ADD COLUMN bio text;".into(),
            },
        ];

        let index = build_index(&migrations);

        // Table index
        assert!(index.tables.contains_key("users"));
        let user_refs = &index.tables["users"];
        assert_eq!(user_refs.len(), 2);
        assert_eq!(user_refs[0].timestamp, "20260101000000");
        assert_eq!(user_refs[1].timestamp, "20260102000000");

        // Column index
        assert!(index.columns.contains_key("users.id"));
        assert!(index.columns.contains_key("users.email"));
        assert!(index.columns.contains_key("users.bio"));
        let bio_refs = &index.columns["users.bio"];
        assert_eq!(bio_refs.len(), 1);
        assert_eq!(bio_refs[0].description, "add bio to users");
    }

    #[test]
    fn build_index_deduplicates_table_refs() {
        // A migration with multiple statements affecting the same table
        // should only produce one table-level reference.
        let migrations = vec![MigrationFile {
            timestamp: "20260101000000".into(),
            description: "setup users".into(),
            path: PathBuf::from("m.up.sql"),
            sql:
                "ALTER TABLE users ADD COLUMN bio text;\nALTER TABLE users ADD COLUMN avatar text;"
                    .into(),
        }];

        let index = build_index(&migrations);
        assert_eq!(index.tables["users"].len(), 1);
        // But columns should each have their own ref
        assert_eq!(index.columns["users.bio"].len(), 1);
        assert_eq!(index.columns["users.avatar"].len(), 1);
    }

    // --- File scanning ---

    #[test]
    fn scan_nonexistent_dir_returns_empty() {
        let result = scan_migrations(Path::new("/nonexistent/path"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn scan_and_index_integration() {
        let dir = std::env::temp_dir().join("inara_test_scan_index");
        let _ = fs::create_dir_all(&dir);

        // Create test migration files
        fs::write(
            dir.join("20260101000000_create_users.up.sql"),
            "CREATE TABLE users (\n    id uuid NOT NULL,\n    email text\n);\n",
        )
        .unwrap();
        fs::write(
            dir.join("20260102000000_add_posts.up.sql"),
            "CREATE TABLE posts (\n    id uuid NOT NULL,\n    author_id uuid NOT NULL\n);\n",
        )
        .unwrap();
        fs::write(
            dir.join("20260103000000_add_bio.up.sql"),
            "ALTER TABLE users ADD COLUMN bio text;\n",
        )
        .unwrap();
        // Non-migration file should be ignored
        fs::write(dir.join("README.md"), "not a migration").unwrap();
        // Down migration should be ignored
        fs::write(
            dir.join("20260101000000_create_users.down.sql"),
            "DROP TABLE users;",
        )
        .unwrap();

        let index = load_and_index(&dir).unwrap();

        // Should have 3 migrations
        assert_eq!(index.migrations.len(), 3);
        // Sorted by timestamp
        assert_eq!(index.migrations[0].timestamp, "20260101000000");
        assert_eq!(index.migrations[1].timestamp, "20260102000000");
        assert_eq!(index.migrations[2].timestamp, "20260103000000");

        // Table index
        assert!(index.tables.contains_key("users"));
        assert!(index.tables.contains_key("posts"));
        assert_eq!(index.tables["users"].len(), 2); // create + add_bio
        assert_eq!(index.tables["posts"].len(), 1);

        // Column index
        assert!(index.columns.contains_key("users.bio"));

        // Cleanup
        let _ = fs::remove_dir_all(&dir);
    }

    // --- Extract identifier ---

    #[test]
    fn extract_unquoted_identifier() {
        assert_eq!(extract_identifier("users ("), Some("users".into()));
    }

    #[test]
    fn extract_quoted_identifier() {
        assert_eq!(extract_identifier("\"Users\" ("), Some("users".into()));
    }

    #[test]
    fn extract_identifier_empty() {
        assert_eq!(extract_identifier(""), None);
        assert_eq!(extract_identifier("   "), None);
    }

    #[test]
    fn extract_identifier_special_start() {
        assert_eq!(extract_identifier("(foo)"), None);
    }

    // --- Excerpt truncation ---

    #[test]
    fn truncate_short_statement() {
        let s = "ALTER TABLE users ADD COLUMN bio text";
        assert_eq!(truncate_statement(s), s);
    }

    #[test]
    fn truncate_long_statement() {
        let s = "a ".repeat(100);
        let result = truncate_statement(&s);
        assert!(result.len() <= 120);
        assert!(result.ends_with("..."));
    }
}
