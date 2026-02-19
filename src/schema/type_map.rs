use std::collections::BTreeMap;
use std::path::Path;

use super::types::PgType;

/// Target language for type mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    /// Rust — feature-aware (chrono/time/jiff), Cargo.toml scanning.
    Rust,
    /// TypeScript — static mapping table.
    TypeScript,
}

impl Language {
    /// Parse a language name from a config string.
    ///
    /// Returns `None` for unrecognized values.
    pub fn from_config(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "rust" => Some(Language::Rust),
            "typescript" | "ts" => Some(Language::TypeScript),
            _ => None,
        }
    }
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Language::Rust => write!(f, "rust"),
            Language::TypeScript => write!(f, "typescript"),
        }
    }
}

/// Detected sqlx feature flags that affect type mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedFeatures {
    pub chrono: bool,
    pub time: bool,
    pub jiff: bool,
}

impl DetectedFeatures {
    /// No features detected (default sqlx types only).
    pub fn none() -> Self {
        Self {
            chrono: false,
            time: false,
            jiff: false,
        }
    }
}

/// Maps Postgres types to language-specific type strings.
///
/// Dispatches on the target language. Holds detected sqlx feature flags
/// (Rust-only) and user overrides. Feature detection reads the target
/// project's Cargo.toml to find which datetime crate is in use.
/// User overrides take precedence over all defaults.
#[derive(Debug, Clone)]
pub struct TypeMapper {
    language: Language,
    features: DetectedFeatures,
    overrides: BTreeMap<String, String>,
}

/// Static mapping table for PgType → default Rust type (no feature flags).
/// These match sqlx::postgres::types defaults.
const RUST_MAPPINGS: &[(PgType, &str)] = &[
    (PgType::Boolean, "bool"),
    (PgType::SmallInt, "i16"),
    (PgType::Integer, "i32"),
    (PgType::BigInt, "i64"),
    (PgType::Real, "f32"),
    (PgType::DoublePrecision, "f64"),
    (PgType::Text, "String"),
    (PgType::Bytea, "Vec<u8>"),
    (PgType::Uuid, "uuid::Uuid"),
    (PgType::Json, "serde_json::Value"),
    (PgType::Jsonb, "serde_json::Value"),
    (PgType::Interval, "sqlx::postgres::types::PgInterval"),
];

/// Static mapping table for PgType → TypeScript type.
const TYPESCRIPT_MAPPINGS: &[(PgType, &str)] = &[
    (PgType::Boolean, "boolean"),
    (PgType::SmallInt, "number"),
    (PgType::Integer, "number"),
    (PgType::BigInt, "bigint"),
    (PgType::Real, "number"),
    (PgType::DoublePrecision, "number"),
    (PgType::Text, "string"),
    (PgType::Bytea, "Buffer"),
    (PgType::Uuid, "string"),
    (PgType::Json, "unknown"),
    (PgType::Jsonb, "unknown"),
    (PgType::Timestamp, "Date"),
    (PgType::Timestamptz, "Date"),
    (PgType::Date, "string"),
    (PgType::Time, "string"),
    (PgType::Timetz, "string"),
    (PgType::Interval, "string"),
];

impl Default for TypeMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeMapper {
    /// Create a new TypeMapper for Rust with default mappings (no features, no overrides).
    pub fn new() -> Self {
        Self {
            language: Language::Rust,
            features: DetectedFeatures::none(),
            overrides: BTreeMap::new(),
        }
    }

    /// Create a TypeMapper for the given language with no overrides.
    pub fn for_language(language: Language) -> Self {
        Self {
            language,
            features: DetectedFeatures::none(),
            overrides: BTreeMap::new(),
        }
    }

    /// Create a Rust TypeMapper by detecting features from a Cargo.toml file.
    ///
    /// Returns a mapper with defaults if the file can't be read or parsed.
    pub fn from_cargo_toml(path: &Path) -> Self {
        let features = detect_features(path).unwrap_or_else(|_| DetectedFeatures::none());
        Self {
            language: Language::Rust,
            features,
            overrides: BTreeMap::new(),
        }
    }

    /// Create a Rust TypeMapper with explicit features (useful for testing).
    pub fn with_features(features: DetectedFeatures) -> Self {
        Self {
            language: Language::Rust,
            features,
            overrides: BTreeMap::new(),
        }
    }

    /// Add user overrides from a map of pg_type_name → type_string.
    pub fn with_overrides(mut self, overrides: BTreeMap<String, String>) -> Self {
        self.overrides = overrides;
        self
    }

    /// Get the target language.
    pub fn language(&self) -> Language {
        self.language
    }

    /// Get the detected features (Rust-only; always `DetectedFeatures::none()` for other languages).
    pub fn features(&self) -> &DetectedFeatures {
        &self.features
    }

    /// Look up the language-specific type string for a given PgType,
    /// wrapping for nullability if needed.
    ///
    /// Rust: `Option<T>` / `T`
    /// TypeScript: `T | null` / `T`
    pub fn type_annotation(&self, pg_type: &PgType, nullable: bool) -> String {
        let base = self.language_type(pg_type);
        if nullable {
            match self.language {
                Language::Rust => format!("Option<{base}>"),
                Language::TypeScript => format!("{base} | null"),
            }
        } else {
            base
        }
    }

    /// Look up the language-specific type string for a given PgType.
    ///
    /// Priority: user overrides → language-specific mapping → fallback.
    pub fn language_type(&self, pg_type: &PgType) -> String {
        // 1. Check user overrides (keyed by the PgType display string)
        let pg_type_str = pg_type.to_string();
        if let Some(override_type) = self.overrides.get(&pg_type_str) {
            return override_type.clone();
        }

        match self.language {
            Language::Rust => self.rust_type_inner(pg_type, &pg_type_str),
            Language::TypeScript => self.typescript_type_inner(pg_type, &pg_type_str),
        }
    }

    /// Rust type resolution (after overrides checked).
    fn rust_type_inner(&self, pg_type: &PgType, pg_type_str: &str) -> String {
        // 2. Feature-aware mappings (datetime types)
        if let Some(mapped) = self.feature_aware_mapping(pg_type) {
            return mapped.to_string();
        }

        // 3. Static default mappings
        for (pg, rust) in RUST_MAPPINGS {
            if pg == pg_type {
                return rust.to_string();
            }
        }

        // 4. Parameterized type mappings
        match pg_type {
            PgType::Varchar(_) | PgType::Char(_) => return "String".to_string(),
            PgType::Numeric(_) => return "rust_decimal::Decimal".to_string(),
            PgType::Array(inner) => {
                let inner_type = self.language_type(inner);
                return format!("Vec<{inner_type}>");
            }
            PgType::Custom(name) => return name.clone(),
            _ => {}
        }

        // 5. Fallback for anything unmapped
        pg_type_str.to_string()
    }

    /// TypeScript type resolution (after overrides checked).
    fn typescript_type_inner(&self, pg_type: &PgType, pg_type_str: &str) -> String {
        // 2. Static mappings
        for (pg, ts) in TYPESCRIPT_MAPPINGS {
            if pg == pg_type {
                return ts.to_string();
            }
        }

        // 3. Parameterized type mappings
        match pg_type {
            PgType::Varchar(_) | PgType::Char(_) => return "string".to_string(),
            PgType::Numeric(_) => return "string".to_string(),
            PgType::Array(inner) => {
                let inner_type = self.language_type(inner);
                return format!("{inner_type}[]");
            }
            PgType::Custom(name) => return name.clone(),
            _ => {}
        }

        // 4. Fallback for anything unmapped
        pg_type_str.to_string()
    }

    /// Feature-aware mapping for datetime types (Rust only).
    fn feature_aware_mapping(&self, pg_type: &PgType) -> Option<&'static str> {
        match pg_type {
            PgType::Timestamp => {
                if self.features.chrono {
                    Some("chrono::NaiveDateTime")
                } else if self.features.time {
                    Some("time::PrimitiveDateTime")
                } else if self.features.jiff {
                    Some("jiff::civil::DateTime")
                } else {
                    Some("sqlx::types::chrono::NaiveDateTime")
                }
            }
            PgType::Timestamptz => {
                if self.features.chrono {
                    Some("chrono::DateTime<Utc>")
                } else if self.features.time {
                    Some("time::OffsetDateTime")
                } else if self.features.jiff {
                    Some("jiff::Timestamp")
                } else {
                    Some("sqlx::types::chrono::DateTime<Utc>")
                }
            }
            PgType::Date => {
                if self.features.chrono {
                    Some("chrono::NaiveDate")
                } else if self.features.time {
                    Some("time::Date")
                } else if self.features.jiff {
                    Some("jiff::civil::Date")
                } else {
                    Some("sqlx::types::chrono::NaiveDate")
                }
            }
            PgType::Time => {
                if self.features.chrono {
                    Some("chrono::NaiveTime")
                } else if self.features.time {
                    Some("time::Time")
                } else if self.features.jiff {
                    Some("jiff::civil::Time")
                } else {
                    Some("sqlx::types::chrono::NaiveTime")
                }
            }
            PgType::Timetz => {
                if self.features.time {
                    Some("time::OffsetDateTime")
                } else {
                    Some("(chrono::NaiveTime, chrono::FixedOffset)")
                }
            }
            _ => None,
        }
    }
}

/// Detect sqlx feature flags from a Cargo.toml file.
///
/// Looks for `chrono`, `time`, or `jiff-sqlx` in:
/// - `sqlx` dependency features (e.g., `sqlx = { features = ["chrono"] }`)
/// - Direct crate dependencies (e.g., `chrono = "0.4"`)
fn detect_features(cargo_toml_path: &Path) -> Result<DetectedFeatures, String> {
    let content = std::fs::read_to_string(cargo_toml_path)
        .map_err(|e| format!("failed to read Cargo.toml: {e}"))?;

    let doc: toml::Value =
        toml::from_str(&content).map_err(|e| format!("failed to parse Cargo.toml: {e}"))?;

    let mut features = DetectedFeatures::none();

    // Check sqlx dependency features
    if let Some(deps) = doc.get("dependencies") {
        // Check sqlx features list
        if let Some(sqlx) = deps.get("sqlx") {
            if let Some(feature_list) = sqlx.get("features").and_then(|f| f.as_array()) {
                for feature in feature_list {
                    if let Some(f) = feature.as_str() {
                        match f {
                            "chrono" => features.chrono = true,
                            "time" => features.time = true,
                            _ => {}
                        }
                    }
                }
            }
        }

        // Check for direct crate dependencies
        if deps.get("chrono").is_some() {
            features.chrono = true;
        }
        if deps.get("time").is_some() {
            features.time = true;
        }
        if deps.get("jiff-sqlx").is_some() {
            features.jiff = true;
        }
    }

    Ok(features)
}

/// Load user type overrides from a Cargo.toml's `[package.metadata.inara.type_overrides]` table
/// or a standalone inara config file.
pub fn load_overrides(cargo_toml_path: &Path) -> BTreeMap<String, String> {
    let mut overrides = BTreeMap::new();

    // Try Cargo.toml [package.metadata.inara.type_overrides]
    if let Ok(content) = std::fs::read_to_string(cargo_toml_path) {
        if let Ok(doc) = toml::from_str::<toml::Value>(&content) {
            if let Some(table) = doc
                .get("package")
                .and_then(|p| p.get("metadata"))
                .and_then(|m| m.get("inara"))
                .and_then(|i| i.get("type_overrides"))
                .and_then(|t| t.as_table())
            {
                for (key, value) in table {
                    if let Some(v) = value.as_str() {
                        overrides.insert(key.clone(), v.to_string());
                    }
                }
            }
        }
    }

    // Also try .inara.toml in the same directory
    if let Some(dir) = cargo_toml_path.parent() {
        let inara_config = dir.join(".inara.toml");
        if let Ok(content) = std::fs::read_to_string(&inara_config) {
            if let Ok(doc) = toml::from_str::<toml::Value>(&content) {
                if let Some(table) = doc.get("type_overrides").and_then(|t| t.as_table()) {
                    for (key, value) in table {
                        if let Some(v) = value.as_str() {
                            overrides.insert(key.clone(), v.to_string());
                        }
                    }
                }
            }
        }
    }

    overrides
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // --- Language enum ---

    #[test]
    fn language_from_config_rust() {
        assert_eq!(Language::from_config("rust"), Some(Language::Rust));
        assert_eq!(Language::from_config("Rust"), Some(Language::Rust));
        assert_eq!(Language::from_config("RUST"), Some(Language::Rust));
    }

    #[test]
    fn language_from_config_typescript() {
        assert_eq!(
            Language::from_config("typescript"),
            Some(Language::TypeScript)
        );
        assert_eq!(
            Language::from_config("TypeScript"),
            Some(Language::TypeScript)
        );
        assert_eq!(Language::from_config("ts"), Some(Language::TypeScript));
        assert_eq!(Language::from_config("TS"), Some(Language::TypeScript));
    }

    #[test]
    fn language_from_config_unknown() {
        assert_eq!(Language::from_config("python"), None);
        assert_eq!(Language::from_config(""), None);
    }

    #[test]
    fn language_display() {
        assert_eq!(Language::Rust.to_string(), "rust");
        assert_eq!(Language::TypeScript.to_string(), "typescript");
    }

    // --- Default (Rust) type mappings ---

    #[test]
    fn default_boolean() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.language_type(&PgType::Boolean), "bool");
    }

    #[test]
    fn default_integer_types() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.language_type(&PgType::SmallInt), "i16");
        assert_eq!(mapper.language_type(&PgType::Integer), "i32");
        assert_eq!(mapper.language_type(&PgType::BigInt), "i64");
    }

    #[test]
    fn default_float_types() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.language_type(&PgType::Real), "f32");
        assert_eq!(mapper.language_type(&PgType::DoublePrecision), "f64");
    }

    #[test]
    fn default_text_types() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.language_type(&PgType::Text), "String");
        assert_eq!(mapper.language_type(&PgType::Varchar(None)), "String");
        assert_eq!(mapper.language_type(&PgType::Varchar(Some(255))), "String");
        assert_eq!(mapper.language_type(&PgType::Char(Some(1))), "String");
    }

    #[test]
    fn default_uuid() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.language_type(&PgType::Uuid), "uuid::Uuid");
    }

    #[test]
    fn default_bytea() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.language_type(&PgType::Bytea), "Vec<u8>");
    }

    #[test]
    fn default_json_types() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.language_type(&PgType::Json), "serde_json::Value");
        assert_eq!(mapper.language_type(&PgType::Jsonb), "serde_json::Value");
    }

    #[test]
    fn default_numeric() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.language_type(&PgType::Numeric(None)),
            "rust_decimal::Decimal"
        );
        assert_eq!(
            mapper.language_type(&PgType::Numeric(Some((10, 2)))),
            "rust_decimal::Decimal"
        );
    }

    #[test]
    fn default_interval() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.language_type(&PgType::Interval),
            "sqlx::postgres::types::PgInterval"
        );
    }

    #[test]
    fn default_array() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.language_type(&PgType::Array(Box::new(PgType::Integer))),
            "Vec<i32>"
        );
        assert_eq!(
            mapper.language_type(&PgType::Array(Box::new(PgType::Text))),
            "Vec<String>"
        );
    }

    #[test]
    fn default_custom_type_returns_name() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.language_type(&PgType::Custom("user_role".into())),
            "user_role"
        );
    }

    // --- Datetime types without features (default sqlx) ---

    #[test]
    fn default_timestamp_no_features() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.language_type(&PgType::Timestamp),
            "sqlx::types::chrono::NaiveDateTime"
        );
    }

    #[test]
    fn default_timestamptz_no_features() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.language_type(&PgType::Timestamptz),
            "sqlx::types::chrono::DateTime<Utc>"
        );
    }

    #[test]
    fn default_date_no_features() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.language_type(&PgType::Date),
            "sqlx::types::chrono::NaiveDate"
        );
    }

    #[test]
    fn default_time_no_features() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.language_type(&PgType::Time),
            "sqlx::types::chrono::NaiveTime"
        );
    }

    // --- Feature detection: chrono ---

    #[test]
    fn chrono_feature_timestamp() {
        let mapper = TypeMapper::with_features(DetectedFeatures {
            chrono: true,
            time: false,
            jiff: false,
        });
        assert_eq!(
            mapper.language_type(&PgType::Timestamp),
            "chrono::NaiveDateTime"
        );
        assert_eq!(
            mapper.language_type(&PgType::Timestamptz),
            "chrono::DateTime<Utc>"
        );
        assert_eq!(mapper.language_type(&PgType::Date), "chrono::NaiveDate");
        assert_eq!(mapper.language_type(&PgType::Time), "chrono::NaiveTime");
    }

    // --- Feature detection: time ---

    #[test]
    fn time_feature_timestamp() {
        let mapper = TypeMapper::with_features(DetectedFeatures {
            chrono: false,
            time: true,
            jiff: false,
        });
        assert_eq!(
            mapper.language_type(&PgType::Timestamp),
            "time::PrimitiveDateTime"
        );
        assert_eq!(
            mapper.language_type(&PgType::Timestamptz),
            "time::OffsetDateTime"
        );
        assert_eq!(mapper.language_type(&PgType::Date), "time::Date");
        assert_eq!(mapper.language_type(&PgType::Time), "time::Time");
        assert_eq!(
            mapper.language_type(&PgType::Timetz),
            "time::OffsetDateTime"
        );
    }

    // --- Feature detection: jiff ---

    #[test]
    fn jiff_feature_timestamp() {
        let mapper = TypeMapper::with_features(DetectedFeatures {
            chrono: false,
            time: false,
            jiff: true,
        });
        assert_eq!(
            mapper.language_type(&PgType::Timestamp),
            "jiff::civil::DateTime"
        );
        assert_eq!(
            mapper.language_type(&PgType::Timestamptz),
            "jiff::Timestamp"
        );
        assert_eq!(mapper.language_type(&PgType::Date), "jiff::civil::Date");
        assert_eq!(mapper.language_type(&PgType::Time), "jiff::civil::Time");
    }

    // --- Chrono takes precedence when both chrono and time are present ---

    #[test]
    fn chrono_takes_precedence_over_time() {
        let mapper = TypeMapper::with_features(DetectedFeatures {
            chrono: true,
            time: true,
            jiff: false,
        });
        assert_eq!(
            mapper.language_type(&PgType::Timestamptz),
            "chrono::DateTime<Utc>"
        );
    }

    // --- User overrides ---

    #[test]
    fn override_takes_precedence() {
        let mut overrides = BTreeMap::new();
        overrides.insert("uuid".to_string(), "MyUuid".to_string());
        overrides.insert("text".to_string(), "&str".to_string());

        let mapper = TypeMapper::new().with_overrides(overrides);
        assert_eq!(mapper.language_type(&PgType::Uuid), "MyUuid");
        assert_eq!(mapper.language_type(&PgType::Text), "&str");
        // Non-overridden types still work
        assert_eq!(mapper.language_type(&PgType::Integer), "i32");
    }

    #[test]
    fn override_takes_precedence_over_features() {
        let mut overrides = BTreeMap::new();
        overrides.insert("timestamptz".to_string(), "MyDateTime".to_string());

        let mapper = TypeMapper::with_features(DetectedFeatures {
            chrono: true,
            time: false,
            jiff: false,
        })
        .with_overrides(overrides);

        assert_eq!(mapper.language_type(&PgType::Timestamptz), "MyDateTime");
    }

    // --- Cargo.toml feature detection ---

    #[test]
    fn detect_chrono_from_sqlx_features() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"

[dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "chrono"] }
"#,
        );

        let features = detect_features(&cargo_toml).unwrap();
        assert!(features.chrono);
        assert!(!features.time);
        assert!(!features.jiff);
    }

    #[test]
    fn detect_time_from_sqlx_features() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"

[dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "time"] }
"#,
        );

        let features = detect_features(&cargo_toml).unwrap();
        assert!(!features.chrono);
        assert!(features.time);
    }

    #[test]
    fn detect_jiff_from_direct_dependency() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"

[dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
jiff-sqlx = "0.1"
"#,
        );

        let features = detect_features(&cargo_toml).unwrap();
        assert!(!features.chrono);
        assert!(!features.time);
        assert!(features.jiff);
    }

    #[test]
    fn detect_chrono_from_direct_dependency() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"

[dependencies]
sqlx = "0.8"
chrono = "0.4"
"#,
        );

        let features = detect_features(&cargo_toml).unwrap();
        assert!(features.chrono);
    }

    #[test]
    fn detect_no_features_when_none_present() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"

[dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
"#,
        );

        let features = detect_features(&cargo_toml).unwrap();
        assert!(!features.chrono);
        assert!(!features.time);
        assert!(!features.jiff);
    }

    #[test]
    fn detect_features_missing_file_returns_error() {
        let result = detect_features(Path::new("/nonexistent/Cargo.toml"));
        assert!(result.is_err());
    }

    // --- Override loading ---

    #[test]
    fn load_overrides_from_cargo_toml_metadata() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"

[package.metadata.inara.type_overrides]
uuid = "MyUuid"
timestamptz = "MyDateTime"
"#,
        );

        let overrides = load_overrides(&cargo_toml);
        assert_eq!(overrides.get("uuid").unwrap(), "MyUuid");
        assert_eq!(overrides.get("timestamptz").unwrap(), "MyDateTime");
    }

    #[test]
    fn load_overrides_from_inara_config() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"
"#,
        );

        let inara_config = dir.join(".inara.toml");
        write_file(
            &inara_config,
            r#"
[type_overrides]
jsonb = "MyJson"
"#,
        );

        let overrides = load_overrides(&cargo_toml);
        assert_eq!(overrides.get("jsonb").unwrap(), "MyJson");
    }

    #[test]
    fn load_overrides_empty_when_no_config() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"
"#,
        );

        let overrides = load_overrides(&cargo_toml);
        assert!(overrides.is_empty());
    }

    // --- Full integration: from_cargo_toml ---

    #[test]
    fn from_cargo_toml_with_chrono() {
        let dir = tempdir();
        let cargo_toml = dir.join("Cargo.toml");
        write_file(
            &cargo_toml,
            r#"
[package]
name = "test"
version = "0.1.0"
edition = "2021"

[dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "chrono"] }
"#,
        );

        let mapper = TypeMapper::from_cargo_toml(&cargo_toml);
        assert_eq!(
            mapper.language_type(&PgType::Timestamptz),
            "chrono::DateTime<Utc>"
        );
        assert_eq!(mapper.language_type(&PgType::Integer), "i32");
    }

    #[test]
    fn from_cargo_toml_missing_file_uses_defaults() {
        let mapper = TypeMapper::from_cargo_toml(Path::new("/nonexistent/Cargo.toml"));
        // Should still work with defaults
        assert_eq!(mapper.language_type(&PgType::Integer), "i32");
    }

    // --- Nullable-aware annotation ---

    #[test]
    fn annotation_non_nullable() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.type_annotation(&PgType::Text, false), "String");
        assert_eq!(mapper.type_annotation(&PgType::Uuid, false), "uuid::Uuid");
    }

    #[test]
    fn annotation_nullable_wraps_option() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.type_annotation(&PgType::Text, true),
            "Option<String>"
        );
        assert_eq!(
            mapper.type_annotation(&PgType::Uuid, true),
            "Option<uuid::Uuid>"
        );
        assert_eq!(
            mapper.type_annotation(&PgType::Jsonb, true),
            "Option<serde_json::Value>"
        );
    }

    #[test]
    fn annotation_nullable_with_features() {
        let mapper = TypeMapper::with_features(DetectedFeatures {
            chrono: true,
            time: false,
            jiff: false,
        });
        assert_eq!(
            mapper.type_annotation(&PgType::Timestamptz, true),
            "Option<chrono::DateTime<Utc>>"
        );
        assert_eq!(
            mapper.type_annotation(&PgType::Timestamptz, false),
            "chrono::DateTime<Utc>"
        );
    }

    // --- All common types covered ---

    #[test]
    fn all_common_types_have_mappings() {
        let mapper = TypeMapper::new();
        let types = vec![
            PgType::Boolean,
            PgType::SmallInt,
            PgType::Integer,
            PgType::BigInt,
            PgType::Real,
            PgType::DoublePrecision,
            PgType::Numeric(None),
            PgType::Text,
            PgType::Varchar(None),
            PgType::Varchar(Some(255)),
            PgType::Char(None),
            PgType::Bytea,
            PgType::Uuid,
            PgType::Timestamp,
            PgType::Timestamptz,
            PgType::Date,
            PgType::Time,
            PgType::Timetz,
            PgType::Interval,
            PgType::Json,
            PgType::Jsonb,
            PgType::Array(Box::new(PgType::Integer)),
        ];

        for pg_type in types {
            let mapped = mapper.language_type(&pg_type);
            assert!(
                !mapped.is_empty(),
                "PgType {} should have a non-empty mapping",
                pg_type
            );
            // Ensure it's not just the raw PG type string (except for custom types)
            if !matches!(pg_type, PgType::Custom(_)) {
                assert_ne!(
                    mapped,
                    pg_type.to_string(),
                    "PgType {} should map to a language type, not itself",
                    pg_type
                );
            }
        }
    }

    // --- TypeScript language ---

    #[test]
    fn typescript_boolean() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Boolean), "boolean");
    }

    #[test]
    fn typescript_integer_types() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::SmallInt), "number");
        assert_eq!(mapper.language_type(&PgType::Integer), "number");
    }

    #[test]
    fn typescript_bigint() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::BigInt), "bigint");
    }

    #[test]
    fn typescript_float_types() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Real), "number");
        assert_eq!(mapper.language_type(&PgType::DoublePrecision), "number");
    }

    #[test]
    fn typescript_numeric() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Numeric(None)), "string");
        assert_eq!(
            mapper.language_type(&PgType::Numeric(Some((10, 2)))),
            "string"
        );
    }

    #[test]
    fn typescript_text_types() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Text), "string");
        assert_eq!(mapper.language_type(&PgType::Varchar(None)), "string");
        assert_eq!(mapper.language_type(&PgType::Varchar(Some(255))), "string");
        assert_eq!(mapper.language_type(&PgType::Char(Some(1))), "string");
    }

    #[test]
    fn typescript_bytea() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Bytea), "Buffer");
    }

    #[test]
    fn typescript_uuid() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Uuid), "string");
    }

    #[test]
    fn typescript_json_types() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Json), "unknown");
        assert_eq!(mapper.language_type(&PgType::Jsonb), "unknown");
    }

    #[test]
    fn typescript_timestamp_types() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Timestamp), "Date");
        assert_eq!(mapper.language_type(&PgType::Timestamptz), "Date");
    }

    #[test]
    fn typescript_date_time_interval() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language_type(&PgType::Date), "string");
        assert_eq!(mapper.language_type(&PgType::Time), "string");
        assert_eq!(mapper.language_type(&PgType::Timetz), "string");
        assert_eq!(mapper.language_type(&PgType::Interval), "string");
    }

    #[test]
    fn typescript_custom_type_returns_name() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(
            mapper.language_type(&PgType::Custom("user_role".into())),
            "user_role"
        );
    }

    #[test]
    fn typescript_array_wrapping() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(
            mapper.language_type(&PgType::Array(Box::new(PgType::Integer))),
            "number[]"
        );
        assert_eq!(
            mapper.language_type(&PgType::Array(Box::new(PgType::Text))),
            "string[]"
        );
    }

    #[test]
    fn typescript_nullable_wrapping() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.type_annotation(&PgType::Text, true), "string | null");
        assert_eq!(
            mapper.type_annotation(&PgType::Integer, true),
            "number | null"
        );
        assert_eq!(
            mapper.type_annotation(&PgType::Jsonb, true),
            "unknown | null"
        );
    }

    #[test]
    fn typescript_non_nullable() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.type_annotation(&PgType::Text, false), "string");
        assert_eq!(mapper.type_annotation(&PgType::Integer, false), "number");
    }

    #[test]
    fn typescript_overrides_work() {
        let mut overrides = BTreeMap::new();
        overrides.insert("jsonb".to_string(), "Record<string, unknown>".to_string());
        let mapper = TypeMapper::for_language(Language::TypeScript).with_overrides(overrides);
        assert_eq!(
            mapper.language_type(&PgType::Jsonb),
            "Record<string, unknown>"
        );
        // Non-overridden types still use defaults
        assert_eq!(mapper.language_type(&PgType::Text), "string");
    }

    #[test]
    fn typescript_override_takes_precedence() {
        let mut overrides = BTreeMap::new();
        overrides.insert("bigint".to_string(), "string".to_string());
        let mapper = TypeMapper::for_language(Language::TypeScript).with_overrides(overrides);
        assert_eq!(mapper.language_type(&PgType::BigInt), "string");
    }

    #[test]
    fn typescript_nullable_with_override() {
        let mut overrides = BTreeMap::new();
        overrides.insert("jsonb".to_string(), "any".to_string());
        let mapper = TypeMapper::for_language(Language::TypeScript).with_overrides(overrides);
        assert_eq!(mapper.type_annotation(&PgType::Jsonb, true), "any | null");
    }

    #[test]
    fn typescript_all_common_types_have_mappings() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        // Types where the TS name differs from the PG name
        let types_with_different_names = vec![
            PgType::SmallInt,
            PgType::Integer,
            PgType::Real,
            PgType::DoublePrecision,
            PgType::Numeric(None),
            PgType::Text,
            PgType::Varchar(None),
            PgType::Varchar(Some(255)),
            PgType::Char(None),
            PgType::Bytea,
            PgType::Uuid,
            PgType::Timestamp,
            PgType::Timestamptz,
            PgType::Date,
            PgType::Time,
            PgType::Timetz,
            PgType::Interval,
            PgType::Json,
            PgType::Jsonb,
            PgType::Array(Box::new(PgType::Integer)),
        ];

        for pg_type in &types_with_different_names {
            let mapped = mapper.language_type(pg_type);
            assert!(
                !mapped.is_empty(),
                "PgType {} should have a non-empty TS mapping",
                pg_type
            );
            assert_ne!(
                mapped,
                pg_type.to_string(),
                "PgType {} should map to a TS type, not its PG name",
                pg_type
            );
        }

        // boolean and bigint legitimately share names with their PG types
        assert_eq!(mapper.language_type(&PgType::Boolean), "boolean");
        assert_eq!(mapper.language_type(&PgType::BigInt), "bigint");
    }

    // --- for_language constructor ---

    #[test]
    fn for_language_rust_same_as_new() {
        let new_mapper = TypeMapper::new();
        let lang_mapper = TypeMapper::for_language(Language::Rust);
        assert_eq!(
            new_mapper.language_type(&PgType::Integer),
            lang_mapper.language_type(&PgType::Integer)
        );
        assert_eq!(new_mapper.language(), Language::Rust);
        assert_eq!(lang_mapper.language(), Language::Rust);
    }

    #[test]
    fn for_language_typescript() {
        let mapper = TypeMapper::for_language(Language::TypeScript);
        assert_eq!(mapper.language(), Language::TypeScript);
    }

    // --- Test helpers ---

    use std::sync::atomic::{AtomicU32, Ordering};
    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn tempdir() -> std::path::PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("inara-test-{}-{}", std::process::id(), id));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    fn write_file(path: &Path, content: &str) {
        // Dedent: strip common leading whitespace from all non-empty lines
        let lines: Vec<&str> = content.lines().collect();
        let min_indent = lines
            .iter()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.len() - l.trim_start().len())
            .min()
            .unwrap_or(0);
        let dedented: String = lines
            .iter()
            .map(|l| {
                if l.len() >= min_indent {
                    &l[min_indent..]
                } else {
                    l
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(dedented.as_bytes()).unwrap();
    }
}
