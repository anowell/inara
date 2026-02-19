use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Inara project configuration loaded from `inara.toml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct InaraConfig {
    /// PostgreSQL connection URL.
    pub database_url: Option<String>,
    /// Path to the migrations directory (relative to config file).
    pub migrations_dir: Option<String>,
    /// Type mapping configuration.
    pub types: TypesConfig,
}

/// Type mapping section of the config.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TypesConfig {
    /// Target language for type mapping (e.g., "rust", "typescript"). Defaults to "rust".
    pub language: Option<String>,
    /// PG type name → language type string overrides.
    pub overrides: BTreeMap<String, String>,
}

/// Result of config discovery: the parsed config and the directory it was found in.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: InaraConfig,
    /// Directory containing the config file (used as base for relative paths).
    pub config_dir: PathBuf,
}

/// VCS root markers that stop upward directory traversal.
const VCS_MARKERS: &[&str] = &[".git", ".jj", ".hg"];

/// Find the config file by walking up from `start_dir`.
///
/// Looks for `inara.toml` (preferred) or `.inara.toml` (legacy) in each
/// directory, stopping at a VCS root or the filesystem root.
pub fn find_config_file(start_dir: &Path) -> Option<PathBuf> {
    let mut dir = start_dir.to_path_buf();
    loop {
        // Prefer inara.toml over .inara.toml
        let candidate = dir.join("inara.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
        let legacy = dir.join(".inara.toml");
        if legacy.is_file() {
            return Some(legacy);
        }

        // Stop at VCS root
        if VCS_MARKERS.iter().any(|m| dir.join(m).exists()) {
            return None;
        }

        // Move up
        if !dir.pop() {
            return None;
        }
    }
}

/// Load and parse a config file.
pub fn load_config(path: &Path) -> Result<InaraConfig, String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("failed to read config: {e}"))?;
    toml::from_str(&content).map_err(|e| format!("failed to parse config: {e}"))
}

/// Discover the config file from the current directory and load it.
///
/// Returns `None` if no config file is found (not an error).
/// Logs a warning if a config file is found but cannot be parsed.
pub fn find_and_load() -> Option<LoadedConfig> {
    let cwd = std::env::current_dir().ok()?;
    let config_path = find_config_file(&cwd)?;
    let config_dir = config_path.parent()?.to_path_buf();
    let config = match load_config(&config_path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Warning: failed to parse {}: {e}", config_path.display());
            return None;
        }
    };
    Some(LoadedConfig { config, config_dir })
}

/// Resolve the migrations directory.
///
/// Priority:
/// 1. `MIGRATIONS_DIR` environment variable (absolute or relative to CWD)
/// 2. Config file `migrations_dir` value (relative to config dir)
/// 3. Convention: `migrations/` relative to config dir (or CWD if no config)
///
/// Returns `None` if the resolved directory doesn't exist or contains no `.sql` files.
pub fn resolve_migrations_dir(
    config: Option<&InaraConfig>,
    config_dir: Option<&Path>,
) -> Option<PathBuf> {
    let env_val = std::env::var("MIGRATIONS_DIR").ok();
    resolve_migrations_dir_inner(config, config_dir, env_val.as_deref())
}

/// Inner implementation that accepts the env var value as a parameter for testability.
fn resolve_migrations_dir_inner(
    config: Option<&InaraConfig>,
    config_dir: Option<&Path>,
    env_migrations_dir: Option<&str>,
) -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;

    // 1. Environment variable
    if let Some(env_dir) = env_migrations_dir {
        let path = PathBuf::from(env_dir);
        let abs = if path.is_absolute() {
            path
        } else {
            cwd.join(path)
        };
        return validate_migrations_dir(&abs);
    }

    let base = config_dir.unwrap_or(&cwd);

    // 2. Config file setting
    if let Some(config) = config {
        if let Some(ref dir) = config.migrations_dir {
            let path = base.join(dir);
            return validate_migrations_dir(&path);
        }
    }

    // 3. Convention: migrations/ relative to base
    let conventional = base.join("migrations");
    validate_migrations_dir(&conventional)
}

/// Validate that a directory exists and contains at least one `.sql` file.
fn validate_migrations_dir(path: &Path) -> Option<PathBuf> {
    if !path.is_dir() {
        return None;
    }
    let has_sql = std::fs::read_dir(path)
        .ok()?
        .filter_map(|e| e.ok())
        .any(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "sql")
                .unwrap_or(false)
        });
    if has_sql {
        Some(path.to_path_buf())
    } else {
        None
    }
}

/// Resolve the database URL from the config file.
///
/// Returns the config value only — CLI args and env vars are handled elsewhere.
pub fn resolve_database_url(config: &InaraConfig) -> Option<&str> {
    config.database_url.as_deref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn tempdir() -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("inara-config-test-{}-{}", std::process::id(), id));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    fn write_file(path: &Path, content: &str) {
        std::fs::write(path, content).expect("write_file");
    }

    // --- find_config_file ---

    #[test]
    fn find_config_in_current_dir() {
        let dir = tempdir();
        write_file(&dir.join("inara.toml"), "[types]\n");
        let result = find_config_file(&dir);
        assert_eq!(result, Some(dir.join("inara.toml")));
    }

    #[test]
    fn find_config_in_parent_dir() {
        let parent = tempdir();
        let child = parent.join("subdir");
        std::fs::create_dir_all(&child).unwrap();
        write_file(&parent.join("inara.toml"), "[types]\n");
        let result = find_config_file(&child);
        assert_eq!(result, Some(parent.join("inara.toml")));
    }

    #[test]
    fn find_legacy_config() {
        let dir = tempdir();
        write_file(&dir.join(".inara.toml"), "[types]\n");
        let result = find_config_file(&dir);
        assert_eq!(result, Some(dir.join(".inara.toml")));
    }

    #[test]
    fn prefer_inara_toml_over_legacy() {
        let dir = tempdir();
        write_file(&dir.join("inara.toml"), "# new\n");
        write_file(&dir.join(".inara.toml"), "# old\n");
        let result = find_config_file(&dir);
        assert_eq!(result, Some(dir.join("inara.toml")));
    }

    #[test]
    fn stop_at_vcs_root() {
        let root = tempdir();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        let child = root.join("deep").join("nested");
        std::fs::create_dir_all(&child).unwrap();
        // Config above VCS root should not be found
        // (but we aren't placing one above, so just verify None)
        let result = find_config_file(&child);
        assert_eq!(result, None);
    }

    #[test]
    fn stop_at_jj_root() {
        let root = tempdir();
        std::fs::create_dir_all(root.join(".jj")).unwrap();
        let child = root.join("sub");
        std::fs::create_dir_all(&child).unwrap();
        let result = find_config_file(&child);
        assert_eq!(result, None);
    }

    #[test]
    fn find_config_at_vcs_root() {
        let root = tempdir();
        std::fs::create_dir_all(root.join(".jj")).unwrap();
        write_file(&root.join("inara.toml"), "[types]\n");
        let child = root.join("sub");
        std::fs::create_dir_all(&child).unwrap();
        let result = find_config_file(&child);
        assert_eq!(result, Some(root.join("inara.toml")));
    }

    #[test]
    fn no_config_returns_none() {
        let dir = tempdir();
        std::fs::create_dir_all(dir.join(".git")).unwrap();
        let result = find_config_file(&dir);
        assert_eq!(result, None);
    }

    // --- load_config ---

    #[test]
    fn load_full_config() {
        let dir = tempdir();
        let path = dir.join("inara.toml");
        write_file(
            &path,
            r#"
database_url = "postgres://user:pass@localhost/mydb"
migrations_dir = "db/migrations"

[types]
language = "rust"

[types.overrides]
uuid = "MyUuid"
timestamptz = "MyDateTime"
"#,
        );
        let config = load_config(&path).unwrap();
        assert_eq!(
            config.database_url.as_deref(),
            Some("postgres://user:pass@localhost/mydb")
        );
        assert_eq!(config.migrations_dir.as_deref(), Some("db/migrations"));
        assert_eq!(config.types.language.as_deref(), Some("rust"));
        assert_eq!(config.types.overrides.get("uuid").unwrap(), "MyUuid");
        assert_eq!(
            config.types.overrides.get("timestamptz").unwrap(),
            "MyDateTime"
        );
    }

    #[test]
    fn load_minimal_config() {
        let dir = tempdir();
        let path = dir.join("inara.toml");
        write_file(&path, "");
        let config = load_config(&path).unwrap();
        assert!(config.database_url.is_none());
        assert!(config.migrations_dir.is_none());
        assert!(config.types.overrides.is_empty());
    }

    #[test]
    fn load_config_missing_file() {
        let result = load_config(Path::new("/nonexistent/inara.toml"));
        assert!(result.is_err());
    }

    // --- resolve_migrations_dir ---

    #[test]
    fn resolve_conventional_migrations_dir() {
        let dir = tempdir();
        let mig = dir.join("migrations");
        std::fs::create_dir_all(&mig).unwrap();
        write_file(&mig.join("001_init.up.sql"), "CREATE TABLE t();");

        let result = resolve_migrations_dir(None, Some(&dir));
        assert_eq!(result, Some(mig));
    }

    #[test]
    fn resolve_config_migrations_dir() {
        let dir = tempdir();
        let mig = dir.join("db").join("migrations");
        std::fs::create_dir_all(&mig).unwrap();
        write_file(&mig.join("001.sql"), "CREATE TABLE t();");

        let config = InaraConfig {
            migrations_dir: Some("db/migrations".into()),
            ..Default::default()
        };
        let result = resolve_migrations_dir(Some(&config), Some(&dir));
        assert_eq!(result, Some(mig));
    }

    #[test]
    fn resolve_env_var_migrations_dir() {
        let dir = tempdir();
        let mig = dir.join("custom").join("migs");
        std::fs::create_dir_all(&mig).unwrap();
        write_file(&mig.join("001.sql"), "CREATE TABLE t();");

        // Env var takes priority over config and convention
        let config = InaraConfig {
            migrations_dir: Some("db/migrations".into()),
            ..Default::default()
        };
        let result =
            resolve_migrations_dir_inner(Some(&config), Some(&dir), Some(mig.to_str().unwrap()));
        assert_eq!(result, Some(mig));
    }

    #[test]
    fn resolve_none_when_dir_missing() {
        let dir = tempdir();
        let result = resolve_migrations_dir(None, Some(&dir));
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_none_when_no_sql_files() {
        let dir = tempdir();
        let mig = dir.join("migrations");
        std::fs::create_dir_all(&mig).unwrap();
        write_file(&mig.join("README.md"), "empty");

        let result = resolve_migrations_dir(None, Some(&dir));
        assert_eq!(result, None);
    }

    // --- resolve_database_url ---

    #[test]
    fn resolve_database_url_from_config() {
        let config = InaraConfig {
            database_url: Some("postgres://localhost/db".into()),
            ..Default::default()
        };
        assert_eq!(
            resolve_database_url(&config),
            Some("postgres://localhost/db")
        );
    }

    #[test]
    fn resolve_database_url_none_when_missing() {
        let config = InaraConfig::default();
        assert_eq!(resolve_database_url(&config), None);
    }
}
