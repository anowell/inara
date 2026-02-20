// Migration naming pattern detection and conforming filename generation.
//
// Detects patterns from existing migration files and generates new filenames
// that match the project's conventions. Supports frameworks like sqlx, Diesel,
// Rails, Prisma, Flyway, golang-migrate, TypeORM, and more.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Layout of migration files within the migrations directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Layout {
    /// Single SQL files directly in the migrations dir.
    /// e.g., `migrations/20240101120000_create_users.sql`
    Flat,
    /// Up/down SQL files directly in the migrations dir.
    /// e.g., `migrations/20240101120000_create_users.up.sql`
    FlatUpDown,
    /// Subdirectories with up.sql/down.sql inside.
    /// e.g., `migrations/20240101120000_create_users/up.sql`
    SubdirUpDown,
    /// Subdirectories with a single named SQL file inside.
    /// e.g., `migrations/20240101120000_create_users/migration.sql`
    SubdirSingleFile(String),
}

/// Kind of version prefix used in migration names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixKind {
    /// 14-digit timestamp: `YYYYMMDDHHMMSS`
    Timestamp14,
    /// Underscore-segmented timestamp: `YYYY_MM_DD_HHMMSS`
    TimestampSegmented,
    /// 10-digit Unix epoch seconds.
    EpochSeconds,
    /// 13-digit Unix epoch milliseconds.
    EpochMillis,
    /// Zero-padded sequential number with a fixed width.
    Sequential { width: usize },
    /// Flyway-style version prefix: `V1`, `V2_1`, `V1.2.3`
    FlywayVersion,
}

/// Separator between the prefix and description.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Separator {
    /// Single underscore: `_`
    Underscore,
    /// Hyphen: `-`
    Hyphen,
    /// Double underscore: `__`
    DoubleUnderscore,
}

impl Separator {
    pub fn as_str(&self) -> &'static str {
        match self {
            Separator::Underscore => "_",
            Separator::Hyphen => "-",
            Separator::DoubleUnderscore => "__",
        }
    }
}

/// Case convention for the description portion of the filename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CaseConvention {
    /// `add_users_table`
    SnakeCase,
    /// `AddUsersTable`
    PascalCase,
    /// `add-users-table`
    KebabCase,
}

/// A detected migration naming pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPattern {
    pub layout: Layout,
    pub prefix: PrefixKind,
    pub separator: Separator,
    pub case_convention: CaseConvention,
}

impl Default for MigrationPattern {
    /// sqlx defaults: flat up/down files, 14-digit timestamp, underscore, snake_case.
    fn default() -> Self {
        Self {
            layout: Layout::FlatUpDown,
            prefix: PrefixKind::Timestamp14,
            separator: Separator::Underscore,
            case_convention: CaseConvention::SnakeCase,
        }
    }
}

/// Intermediate data extracted from a discovered SQL file on disk.
#[derive(Debug)]
struct DiscoveredFile {
    /// The "name stem" containing version + description (e.g., "20240101_create_users").
    name: String,
    /// Whether this file is inside a subdirectory of the migrations dir.
    in_subdir: bool,
    /// The SQL filename itself (e.g., "up.sql", "migration.sql", or the full name for flat).
    sql_filename: String,
    /// Whether the flat filename ends with `.up.sql`.
    has_up_suffix: bool,
}

/// Detect the migration naming pattern from existing files in a directory.
///
/// Scans the directory (one level deep for subdirectories) and analyzes
/// filenames to determine the naming convention. Falls back to sqlx defaults
/// when no migrations exist or the pattern is ambiguous.
pub fn detect_pattern(dir: &Path) -> MigrationPattern {
    let files = discover_files(dir);
    if files.is_empty() {
        return MigrationPattern::default();
    }

    let layout = detect_layout(dir, &files);
    let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
    let prefix = detect_prefix(&names);
    let separator = detect_separator(&names, &prefix);
    let case_convention = detect_case(&names, &prefix, separator);

    MigrationPattern {
        layout,
        prefix,
        separator,
        case_convention,
    }
}

/// Recursively discover SQL files under a directory (one level of subdirs).
fn discover_files(dir: &Path) -> Vec<DiscoveredFile> {
    let mut files = Vec::new();

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return files,
    };

    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        let entry_name = entry.file_name();
        let name_str = entry_name.to_string_lossy();

        if path.is_file() && name_str.ends_with(".sql") {
            // Skip known non-migration files
            if is_non_migration_file(&name_str) {
                continue;
            }

            let has_up_suffix = name_str.ends_with(".up.sql");
            let stem = if has_up_suffix {
                name_str.strip_suffix(".up.sql").unwrap_or(&name_str)
            } else if name_str.ends_with(".down.sql") {
                // Skip down migrations for pattern detection; they mirror up files
                continue;
            } else {
                name_str.strip_suffix(".sql").unwrap_or(&name_str)
            };

            files.push(DiscoveredFile {
                name: stem.to_string(),
                in_subdir: false,
                sql_filename: name_str.to_string(),
                has_up_suffix,
            });
        } else if path.is_dir() && !name_str.starts_with('.') {
            // Look inside subdirectories for SQL files
            if let Ok(sub_entries) = std::fs::read_dir(&path) {
                for sub_entry in sub_entries.filter_map(|e| e.ok()) {
                    let sub_path = sub_entry.path();
                    let sub_name = sub_entry.file_name();
                    let sub_name_str = sub_name.to_string_lossy();

                    if sub_path.is_file() && sub_name_str.ends_with(".sql") {
                        // For subdir layouts, the "name" is the directory name
                        files.push(DiscoveredFile {
                            name: name_str.to_string(),
                            in_subdir: true,
                            sql_filename: sub_name_str.to_string(),
                            has_up_suffix: false,
                        });
                        break; // One SQL file per subdir is enough for detection
                    }
                }
            }
        }
    }

    files
}

/// Recursively discover all `.sql` files under a directory.
///
/// Used by `validate_migrations_dir` and `scan_migrations` to find migration
/// files in both flat and subdirectory layouts.
pub fn discover_sql_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    discover_sql_recursive(dir, &mut files, 0);
    files.sort();
    files
}

fn discover_sql_recursive(dir: &Path, files: &mut Vec<PathBuf>, depth: usize) {
    // Limit recursion to 2 levels (migrations/ and one subdir level)
    if depth > 1 {
        return;
    }

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if path.is_file() && name_str.ends_with(".sql") && !is_non_migration_file(&name_str) {
            files.push(path);
        } else if path.is_dir() && !name_str.starts_with('.') {
            discover_sql_recursive(&path, files, depth + 1);
        }
    }
}

/// Check if a filename is a non-migration file.
///
/// Only checks for hidden files (dot-prefixed) since callers already filter
/// for `.sql` extension, making `.md`/`.txt`/`.lock`/`.snapshot` checks redundant.
fn is_non_migration_file(name: &str) -> bool {
    name.starts_with('.')
}

/// Detect the file layout from discovered files.
fn detect_layout(dir: &Path, files: &[DiscoveredFile]) -> Layout {
    let mut flat_count = 0;
    let mut flat_up_count = 0;
    let mut subdir_up_down = 0;
    let mut subdir_single_name: Option<String> = None;
    let mut subdir_single_count = 0;

    for file in files {
        if file.in_subdir {
            if file.sql_filename == "up.sql" || file.sql_filename == "down.sql" {
                subdir_up_down += 1;
            } else {
                subdir_single_count += 1;
                if subdir_single_name.is_none() {
                    subdir_single_name = Some(file.sql_filename.clone());
                }
            }
        } else if file.has_up_suffix {
            flat_up_count += 1;
        } else {
            flat_count += 1;
        }
    }

    let _ = dir; // used for context if needed later

    if subdir_up_down > flat_count + flat_up_count && subdir_up_down > subdir_single_count {
        Layout::SubdirUpDown
    } else if subdir_single_count > flat_count + flat_up_count
        && subdir_single_count > subdir_up_down
    {
        Layout::SubdirSingleFile(subdir_single_name.unwrap_or_else(|| "migration.sql".to_string()))
    } else if flat_up_count >= flat_count {
        Layout::FlatUpDown
    } else {
        Layout::Flat
    }
}

/// Detect the prefix kind from migration name stems.
fn detect_prefix(names: &[&str]) -> PrefixKind {
    let mut votes: HashMap<&str, usize> = HashMap::new();

    for name in names {
        let kind = classify_prefix(name);
        *votes.entry(kind).or_default() += 1;
    }

    // Pick the most common prefix kind
    let winner = votes
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(kind, _)| kind)
        .unwrap_or("timestamp14");

    match winner {
        "timestamp14" => PrefixKind::Timestamp14,
        "timestamp_segmented" => PrefixKind::TimestampSegmented,
        "epoch_seconds" => PrefixKind::EpochSeconds,
        "epoch_millis" => PrefixKind::EpochMillis,
        "flyway" => PrefixKind::FlywayVersion,
        s if s.starts_with("sequential_") => {
            let width: usize = s
                .strip_prefix("sequential_")
                .unwrap_or("4")
                .parse()
                .unwrap_or(4);
            PrefixKind::Sequential { width }
        }
        _ => PrefixKind::Timestamp14,
    }
}

/// Classify a single name stem's prefix kind, returning a string tag.
fn classify_prefix(name: &str) -> &'static str {
    // Flyway: starts with V followed by digit
    if name.starts_with('V') || name.starts_with('v') {
        let rest = &name[1..];
        if rest.starts_with(|c: char| c.is_ascii_digit()) {
            return "flyway";
        }
    }

    // TimestampSegmented: YYYY_MM_DD_HHMMSS (4_2_2_6 pattern)
    if name.len() >= 19 {
        let bytes = name.as_bytes();
        if bytes[..4].iter().all(|b| b.is_ascii_digit())
            && bytes[4] == b'_'
            && bytes[5..7].iter().all(|b| b.is_ascii_digit())
            && bytes[7] == b'_'
            && bytes[8..10].iter().all(|b| b.is_ascii_digit())
            && bytes[10] == b'_'
            && bytes[11..17].iter().all(|b| b.is_ascii_digit())
        {
            return "timestamp_segmented";
        }
    }

    // Extract the leading digit run
    let digit_end = name
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(name.len());
    let digits = &name[..digit_end];

    match digits.len() {
        14 => "timestamp14",
        13 => "epoch_millis",
        10 => "epoch_seconds",
        3..=6 => {
            // Leak a static tag for the width — we use fixed tags for common widths
            match digits.len() {
                3 => "sequential_3",
                4 => "sequential_4",
                5 => "sequential_5",
                6 => "sequential_6",
                _ => "sequential_4",
            }
        }
        _ => "timestamp14", // fallback
    }
}

/// Extract the prefix portion of a name stem.
pub fn extract_prefix(name: &str, kind: &PrefixKind) -> Option<String> {
    match kind {
        PrefixKind::Timestamp14 => {
            if name.len() >= 14 && name[..14].chars().all(|c| c.is_ascii_digit()) {
                Some(name[..14].to_string())
            } else {
                None
            }
        }
        PrefixKind::TimestampSegmented => {
            // YYYY_MM_DD_HHMMSS = 17 chars
            if name.len() >= 17 {
                let candidate = &name[..17];
                let parts: Vec<&str> = candidate.split('_').collect();
                if parts.len() == 4
                    && parts[0].len() == 4
                    && parts[1].len() == 2
                    && parts[2].len() == 2
                    && parts[3].len() == 6
                    && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit()))
                {
                    Some(candidate.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        }
        PrefixKind::EpochSeconds => {
            let end = name
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(name.len());
            if end == 10 {
                Some(name[..10].to_string())
            } else {
                None
            }
        }
        PrefixKind::EpochMillis => {
            let end = name
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(name.len());
            if end == 13 {
                Some(name[..13].to_string())
            } else {
                None
            }
        }
        PrefixKind::Sequential { width } => {
            let end = name
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(name.len());
            if end == *width && name[..end].chars().all(|c| c.is_ascii_digit()) {
                Some(name[..end].to_string())
            } else {
                None
            }
        }
        PrefixKind::FlywayVersion => {
            if name.starts_with('V') || name.starts_with('v') {
                let rest = &name[1..];
                // Version part: digits separated by dots or underscores (V1, V2_1, V1.2.3)
                // Stop at separator characters that aren't part of the version
                let mut end = 0;
                let mut last_was_digit = false;
                for (i, c) in rest.char_indices() {
                    if c.is_ascii_digit() {
                        end = i + 1;
                        last_was_digit = true;
                    } else if (c == '.' || c == '_') && last_was_digit {
                        // Could be version separator (V1.2) or end separator (V1__)
                        // Peek ahead: if next char is a digit, it's part of the version
                        if rest[i + 1..].starts_with(|c: char| c.is_ascii_digit()) {
                            last_was_digit = false;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if end > 0 {
                    Some(name[..end + 1].to_string())
                } else {
                    None
                }
            } else {
                None
            }
        }
    }
}

/// Detect the separator between prefix and description.
fn detect_separator(names: &[&str], prefix: &PrefixKind) -> Separator {
    let mut votes: HashMap<Separator, usize> = HashMap::new();

    for name in names {
        if let Some(prefix_str) = extract_prefix(name, prefix) {
            let rest = &name[prefix_str.len()..];
            if rest.starts_with("__") {
                *votes.entry(Separator::DoubleUnderscore).or_default() += 1;
            } else if rest.starts_with('_') {
                *votes.entry(Separator::Underscore).or_default() += 1;
            } else if rest.starts_with('-') {
                *votes.entry(Separator::Hyphen).or_default() += 1;
            }
        }
    }

    votes
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(sep, _)| sep)
        .unwrap_or(Separator::Underscore)
}

/// Detect the description case convention.
fn detect_case(names: &[&str], prefix: &PrefixKind, separator: Separator) -> CaseConvention {
    let mut votes: HashMap<CaseConvention, usize> = HashMap::new();

    for name in names {
        if let Some(desc) = extract_description(name, prefix, separator) {
            if !desc.is_empty() {
                *votes.entry(classify_case(&desc)).or_default() += 1;
            }
        }
    }

    votes
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(conv, _)| conv)
        .unwrap_or(CaseConvention::SnakeCase)
}

/// Extract the description portion of a name stem (after prefix and separator).
fn extract_description(name: &str, prefix: &PrefixKind, separator: Separator) -> Option<String> {
    let prefix_str = extract_prefix(name, prefix)?;
    let rest = &name[prefix_str.len()..];
    let desc = match separator {
        Separator::DoubleUnderscore => rest.strip_prefix("__"),
        Separator::Underscore => rest.strip_prefix('_'),
        Separator::Hyphen => rest.strip_prefix('-'),
    };
    desc.map(|d| d.to_string())
}

/// Classify a description string's case convention.
fn classify_case(desc: &str) -> CaseConvention {
    if desc.contains('-') {
        CaseConvention::KebabCase
    } else if desc.contains('_') {
        CaseConvention::SnakeCase
    } else if desc.chars().any(|c| c.is_uppercase()) {
        CaseConvention::PascalCase
    } else {
        // Single word or ambiguous — default to snake_case
        CaseConvention::SnakeCase
    }
}

// ── Name generation ──────────────────────────────────────────────────

impl MigrationPattern {
    /// Format a description according to the case convention.
    pub fn format_description(&self, description: &str) -> String {
        // First normalize to words
        let words: Vec<&str> = description
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|w| !w.is_empty())
            .collect();

        if words.is_empty() {
            return String::new();
        }

        match self.case_convention {
            CaseConvention::SnakeCase => words
                .iter()
                .map(|w| w.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("_"),
            CaseConvention::KebabCase => words
                .iter()
                .map(|w| w.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("-"),
            CaseConvention::PascalCase => words
                .iter()
                .map(|w| {
                    let mut chars = w.chars();
                    match chars.next() {
                        Some(c) => {
                            let mut s = c.to_uppercase().to_string();
                            s.extend(chars.map(|c| c.to_ascii_lowercase()));
                            s
                        }
                        None => String::new(),
                    }
                })
                .collect::<Vec<_>>()
                .join(""),
        }
    }

    /// Generate the next prefix value.
    ///
    /// For timestamp-based prefixes, uses the provided unix timestamp.
    /// For sequential/versioned prefixes, uses existing prefixes to determine the next value.
    pub fn next_prefix(&self, existing_prefixes: &[&str], now_secs: u64) -> String {
        match &self.prefix {
            PrefixKind::Timestamp14 => format_timestamp_14(now_secs),
            PrefixKind::TimestampSegmented => format_timestamp_segmented(now_secs),
            PrefixKind::EpochSeconds => format!("{now_secs}"),
            PrefixKind::EpochMillis => format!("{}000", now_secs),
            PrefixKind::Sequential { width } => {
                let max_val = existing_prefixes
                    .iter()
                    .filter_map(|p| p.parse::<u64>().ok())
                    .max()
                    .unwrap_or(0);
                format!("{:0>width$}", max_val + 1, width = *width)
            }
            PrefixKind::FlywayVersion => {
                let max_ver = existing_prefixes
                    .iter()
                    .filter_map(|p| {
                        p.strip_prefix('V')
                            .or_else(|| p.strip_prefix('v'))
                            .and_then(|v| v.split(['.', '_']).next())
                            .and_then(|n| n.parse::<u64>().ok())
                    })
                    .max()
                    .unwrap_or(0);
                format!("V{}", max_ver + 1)
            }
        }
    }

    /// Generate the full path for a new up-migration file.
    ///
    /// Combines prefix, separator, description, and layout into the correct
    /// path structure relative to the migrations directory.
    pub fn generate_path(&self, dir: &Path, description: &str, prefix_value: &str) -> PathBuf {
        let desc = self.format_description(description);
        let sep = self.separator.as_str();
        let stem = format!("{prefix_value}{sep}{desc}");

        match &self.layout {
            Layout::Flat => dir.join(format!("{stem}.sql")),
            Layout::FlatUpDown => dir.join(format!("{stem}.up.sql")),
            Layout::SubdirUpDown => dir.join(&stem).join("up.sql"),
            Layout::SubdirSingleFile(filename) => dir.join(&stem).join(filename),
        }
    }

    /// Generate the path for a down-migration file.
    ///
    /// For layouts that support reversible migrations (FlatUpDown, SubdirUpDown),
    /// generates the corresponding down file. For Flat and SubdirSingleFile layouts,
    /// appends `.down.sql` as a best-effort convention.
    pub fn generate_down_path(&self, dir: &Path, description: &str, prefix_value: &str) -> PathBuf {
        let desc = self.format_description(description);
        let sep = self.separator.as_str();
        let stem = format!("{prefix_value}{sep}{desc}");

        match &self.layout {
            Layout::Flat => dir.join(format!("{stem}.down.sql")),
            Layout::FlatUpDown => dir.join(format!("{stem}.down.sql")),
            Layout::SubdirUpDown => dir.join(&stem).join("down.sql"),
            Layout::SubdirSingleFile(_) => dir.join(&stem).join("down.sql"),
        }
    }

    /// Write a migration file using this pattern.
    ///
    /// Creates any necessary subdirectories for subdir layouts.
    pub fn write_migration(
        &self,
        dir: &Path,
        description: &str,
        sql: &str,
        prefix_value: &str,
    ) -> std::io::Result<PathBuf> {
        let path = self.generate_path(dir, description, prefix_value);

        // Create parent directories for subdir layouts
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(&path, sql)?;
        Ok(path)
    }

    /// Parse a name stem into (prefix, description) using this pattern.
    pub fn parse_name(&self, name: &str) -> Option<(String, String)> {
        let prefix_str = extract_prefix(name, &self.prefix)?;
        let rest = &name[prefix_str.len()..];

        let desc = match self.separator {
            Separator::DoubleUnderscore => rest.strip_prefix("__"),
            Separator::Underscore => rest.strip_prefix('_'),
            Separator::Hyphen => rest.strip_prefix('-'),
        }?;

        if desc.is_empty() {
            return None;
        }

        // Convert description to human-readable form (spaces)
        let human_desc = match self.case_convention {
            CaseConvention::SnakeCase => desc.replace('_', " "),
            CaseConvention::KebabCase => desc.replace('-', " "),
            CaseConvention::PascalCase => split_pascal_case(desc),
        };

        Some((prefix_str, human_desc))
    }

    /// Collect existing prefix values from name stems for next_prefix calculation.
    pub fn collect_prefixes(&self, names: &[&str]) -> Vec<String> {
        names
            .iter()
            .filter_map(|name| extract_prefix(name, &self.prefix))
            .collect()
    }
}

/// Split a PascalCase string into space-separated lowercase words.
fn split_pascal_case(s: &str) -> String {
    let mut words = Vec::new();
    let mut current = String::new();

    for ch in s.chars() {
        if ch.is_uppercase() && !current.is_empty() {
            words.push(current.to_lowercase());
            current = String::new();
        }
        current.push(ch);
    }
    if !current.is_empty() {
        words.push(current.to_lowercase());
    }

    words.join(" ")
}

/// Format a unix timestamp as YYYYMMDDHHMMSS.
fn format_timestamp_14(secs: u64) -> String {
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;
    let (year, month, day) = days_to_civil(days as i64);
    format!("{year:04}{month:02}{day:02}{hours:02}{minutes:02}{seconds:02}")
}

/// Format a unix timestamp as YYYY_MM_DD_HHMMSS.
fn format_timestamp_segmented(secs: u64) -> String {
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;
    let (year, month, day) = days_to_civil(days as i64);
    format!("{year:04}_{month:02}_{day:02}_{hours:02}{minutes:02}{seconds:02}")
}

/// Convert days since Unix epoch to (year, month, day).
/// Algorithm from Howard Hinnant's chrono-compatible date algorithms.
fn days_to_civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn tempdir() -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("inara-pattern-test-{}-{}", std::process::id(), id));
        let _ = fs::create_dir_all(&dir);
        dir
    }

    // ── Default pattern ──────────────────────────────────────────

    #[test]
    fn default_pattern_is_sqlx() {
        let pattern = MigrationPattern::default();
        assert_eq!(pattern.layout, Layout::FlatUpDown);
        assert_eq!(pattern.prefix, PrefixKind::Timestamp14);
        assert_eq!(pattern.separator, Separator::Underscore);
        assert_eq!(pattern.case_convention, CaseConvention::SnakeCase);
    }

    // ── Empty directory returns default ──────────────────────────

    #[test]
    fn detect_empty_dir_returns_default() {
        let dir = tempdir();
        let pattern = detect_pattern(&dir);
        assert_eq!(pattern, MigrationPattern::default());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn detect_nonexistent_dir_returns_default() {
        let pattern = detect_pattern(Path::new("/nonexistent/path"));
        assert_eq!(pattern, MigrationPattern::default());
    }

    // ── sqlx-style detection ─────────────────────────────────────

    #[test]
    fn detect_sqlx_pattern() {
        let dir = tempdir();
        fs::write(
            dir.join("20260101120000_create_users.up.sql"),
            "CREATE TABLE users();",
        )
        .unwrap();
        fs::write(
            dir.join("20260102120000_add_posts.up.sql"),
            "CREATE TABLE posts();",
        )
        .unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.layout, Layout::FlatUpDown);
        assert_eq!(pattern.prefix, PrefixKind::Timestamp14);
        assert_eq!(pattern.separator, Separator::Underscore);
        assert_eq!(pattern.case_convention, CaseConvention::SnakeCase);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Flat SQL (no up/down suffix) ─────────────────────────────

    #[test]
    fn detect_flat_sql_pattern() {
        let dir = tempdir();
        fs::write(
            dir.join("20260101120000_create_users.sql"),
            "CREATE TABLE users();",
        )
        .unwrap();
        fs::write(
            dir.join("20260102120000_add_posts.sql"),
            "CREATE TABLE posts();",
        )
        .unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.layout, Layout::Flat);
        assert_eq!(pattern.prefix, PrefixKind::Timestamp14);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Sequential (Django-style) ────────────────────────────────

    #[test]
    fn detect_sequential_pattern() {
        let dir = tempdir();
        fs::write(dir.join("0001_initial.sql"), "CREATE TABLE t();").unwrap();
        fs::write(dir.join("0002_add_users.sql"), "CREATE TABLE users();").unwrap();
        fs::write(dir.join("0003_add_posts.sql"), "CREATE TABLE posts();").unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.layout, Layout::Flat);
        assert_eq!(pattern.prefix, PrefixKind::Sequential { width: 4 });
        assert_eq!(pattern.separator, Separator::Underscore);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Epoch milliseconds (TypeORM-style) ───────────────────────

    #[test]
    fn detect_epoch_millis_pattern() {
        let dir = tempdir();
        fs::write(
            dir.join("1721930481972-CreateUsers.sql"),
            "CREATE TABLE users();",
        )
        .unwrap();
        fs::write(
            dir.join("1721930491234-AddPosts.sql"),
            "CREATE TABLE posts();",
        )
        .unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.prefix, PrefixKind::EpochMillis);
        assert_eq!(pattern.separator, Separator::Hyphen);
        assert_eq!(pattern.case_convention, CaseConvention::PascalCase);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Flyway-style ─────────────────────────────────────────────

    #[test]
    fn detect_flyway_pattern() {
        let dir = tempdir();
        fs::write(dir.join("V1__create_users.sql"), "CREATE TABLE users();").unwrap();
        fs::write(dir.join("V2__add_posts.sql"), "CREATE TABLE posts();").unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.prefix, PrefixKind::FlywayVersion);
        assert_eq!(pattern.separator, Separator::DoubleUnderscore);
        assert_eq!(pattern.case_convention, CaseConvention::SnakeCase);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Diesel-style (subdir with up.sql/down.sql) ───────────────

    #[test]
    fn detect_diesel_subdir_pattern() {
        let dir = tempdir();
        let m1 = dir.join("20260101120000_create_users");
        fs::create_dir_all(&m1).unwrap();
        fs::write(m1.join("up.sql"), "CREATE TABLE users();").unwrap();
        fs::write(m1.join("down.sql"), "DROP TABLE users;").unwrap();

        let m2 = dir.join("20260102120000_add_posts");
        fs::create_dir_all(&m2).unwrap();
        fs::write(m2.join("up.sql"), "CREATE TABLE posts();").unwrap();
        fs::write(m2.join("down.sql"), "DROP TABLE posts;").unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.layout, Layout::SubdirUpDown);
        assert_eq!(pattern.prefix, PrefixKind::Timestamp14);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Prisma-style (subdir with migration.sql) ─────────────────

    #[test]
    fn detect_prisma_subdir_pattern() {
        let dir = tempdir();
        let m1 = dir.join("20260101120000_create_users");
        fs::create_dir_all(&m1).unwrap();
        fs::write(m1.join("migration.sql"), "CREATE TABLE users();").unwrap();

        let m2 = dir.join("20260102120000_add_posts");
        fs::create_dir_all(&m2).unwrap();
        fs::write(m2.join("migration.sql"), "CREATE TABLE posts();").unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(
            pattern.layout,
            Layout::SubdirSingleFile("migration.sql".into())
        );
        assert_eq!(pattern.prefix, PrefixKind::Timestamp14);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Laravel-style (segmented timestamp) ──────────────────────

    #[test]
    fn detect_laravel_pattern() {
        let dir = tempdir();
        fs::write(
            dir.join("2026_01_01_120000_create_users.sql"),
            "CREATE TABLE users();",
        )
        .unwrap();
        fs::write(
            dir.join("2026_01_02_120000_add_posts.sql"),
            "CREATE TABLE posts();",
        )
        .unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.prefix, PrefixKind::TimestampSegmented);
        assert_eq!(pattern.separator, Separator::Underscore);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Kebab-case (Sequelize-style) ─────────────────────────────

    #[test]
    fn detect_kebab_case_pattern() {
        let dir = tempdir();
        fs::write(
            dir.join("20260101120000-create-users.sql"),
            "CREATE TABLE users();",
        )
        .unwrap();
        fs::write(
            dir.join("20260102120000-add-posts.sql"),
            "CREATE TABLE posts();",
        )
        .unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.separator, Separator::Hyphen);
        assert_eq!(pattern.case_convention, CaseConvention::KebabCase);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Epoch seconds (golang-migrate style) ─────────────────────

    #[test]
    fn detect_epoch_seconds_pattern() {
        let dir = tempdir();
        fs::write(
            dir.join("1500360784_create_users.up.sql"),
            "CREATE TABLE users();",
        )
        .unwrap();
        fs::write(
            dir.join("1500360884_add_posts.up.sql"),
            "CREATE TABLE posts();",
        )
        .unwrap();

        let pattern = detect_pattern(&dir);
        assert_eq!(pattern.prefix, PrefixKind::EpochSeconds);
        assert_eq!(pattern.layout, Layout::FlatUpDown);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Description formatting ───────────────────────────────────

    #[test]
    fn format_description_snake_case() {
        let pattern = MigrationPattern::default();
        assert_eq!(
            pattern.format_description("add users table"),
            "add_users_table"
        );
    }

    #[test]
    fn format_description_kebab_case() {
        let pattern = MigrationPattern {
            case_convention: CaseConvention::KebabCase,
            ..Default::default()
        };
        assert_eq!(
            pattern.format_description("add users table"),
            "add-users-table"
        );
    }

    #[test]
    fn format_description_pascal_case() {
        let pattern = MigrationPattern {
            case_convention: CaseConvention::PascalCase,
            ..Default::default()
        };
        assert_eq!(
            pattern.format_description("add users table"),
            "AddUsersTable"
        );
    }

    #[test]
    fn format_description_normalizes_special_chars() {
        let pattern = MigrationPattern::default();
        assert_eq!(
            pattern.format_description("Add FK: posts→users"),
            "add_fk_posts_users"
        );
    }

    #[test]
    fn format_description_empty() {
        let pattern = MigrationPattern::default();
        assert_eq!(pattern.format_description(""), "");
    }

    // ── Next prefix generation ───────────────────────────────────

    #[test]
    fn next_prefix_timestamp14() {
        let pattern = MigrationPattern::default();
        // 2026-02-20 12:00:00 UTC = 1771588800 seconds
        let prefix = pattern.next_prefix(&[], 1771588800);
        assert_eq!(prefix, "20260220120000");
    }

    #[test]
    fn next_prefix_segmented() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::TimestampSegmented,
            ..Default::default()
        };
        let prefix = pattern.next_prefix(&[], 1771588800);
        assert_eq!(prefix, "2026_02_20_120000");
    }

    #[test]
    fn next_prefix_epoch_seconds() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::EpochSeconds,
            ..Default::default()
        };
        let prefix = pattern.next_prefix(&[], 1500360784);
        assert_eq!(prefix, "1500360784");
    }

    #[test]
    fn next_prefix_epoch_millis() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::EpochMillis,
            ..Default::default()
        };
        let prefix = pattern.next_prefix(&[], 1721930481);
        assert_eq!(prefix, "1721930481000");
    }

    #[test]
    fn next_prefix_sequential() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::Sequential { width: 4 },
            ..Default::default()
        };
        let prefix = pattern.next_prefix(&["0001", "0002", "0003"], 0);
        assert_eq!(prefix, "0004");
    }

    #[test]
    fn next_prefix_sequential_empty() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::Sequential { width: 4 },
            ..Default::default()
        };
        let prefix = pattern.next_prefix(&[], 0);
        assert_eq!(prefix, "0001");
    }

    #[test]
    fn next_prefix_flyway() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::FlywayVersion,
            ..Default::default()
        };
        let prefix = pattern.next_prefix(&["V1", "V2", "V3"], 0);
        assert_eq!(prefix, "V4");
    }

    #[test]
    fn next_prefix_flyway_empty() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::FlywayVersion,
            ..Default::default()
        };
        let prefix = pattern.next_prefix(&[], 0);
        assert_eq!(prefix, "V1");
    }

    // ── Path generation ──────────────────────────────────────────

    #[test]
    fn generate_path_flat() {
        let pattern = MigrationPattern {
            layout: Layout::Flat,
            ..Default::default()
        };
        let path = pattern.generate_path(Path::new("/m"), "add users", "20260101120000");
        assert_eq!(path, PathBuf::from("/m/20260101120000_add_users.sql"));
    }

    #[test]
    fn generate_path_flat_up_down() {
        let pattern = MigrationPattern::default();
        let path = pattern.generate_path(Path::new("/m"), "add users", "20260101120000");
        assert_eq!(path, PathBuf::from("/m/20260101120000_add_users.up.sql"));
    }

    #[test]
    fn generate_path_subdir_up_down() {
        let pattern = MigrationPattern {
            layout: Layout::SubdirUpDown,
            ..Default::default()
        };
        let path = pattern.generate_path(Path::new("/m"), "add users", "20260101120000");
        assert_eq!(path, PathBuf::from("/m/20260101120000_add_users/up.sql"));
    }

    #[test]
    fn generate_path_subdir_single_file() {
        let pattern = MigrationPattern {
            layout: Layout::SubdirSingleFile("migration.sql".into()),
            ..Default::default()
        };
        let path = pattern.generate_path(Path::new("/m"), "add users", "20260101120000");
        assert_eq!(
            path,
            PathBuf::from("/m/20260101120000_add_users/migration.sql")
        );
    }

    #[test]
    fn generate_path_flyway_double_underscore() {
        let pattern = MigrationPattern {
            layout: Layout::Flat,
            prefix: PrefixKind::FlywayVersion,
            separator: Separator::DoubleUnderscore,
            case_convention: CaseConvention::SnakeCase,
        };
        let path = pattern.generate_path(Path::new("/m"), "create users", "V3");
        assert_eq!(path, PathBuf::from("/m/V3__create_users.sql"));
    }

    #[test]
    fn generate_path_typeorm_style() {
        let pattern = MigrationPattern {
            layout: Layout::Flat,
            prefix: PrefixKind::EpochMillis,
            separator: Separator::Hyphen,
            case_convention: CaseConvention::PascalCase,
        };
        let path = pattern.generate_path(Path::new("/m"), "create users table", "1721930481972");
        assert_eq!(path, PathBuf::from("/m/1721930481972-CreateUsersTable.sql"));
    }

    // ── Down path generation ────────────────────────────────────

    #[test]
    fn generate_down_path_flat_up_down() {
        let pattern = MigrationPattern::default();
        let path = pattern.generate_down_path(Path::new("/m"), "add users", "20260101120000");
        assert_eq!(path, PathBuf::from("/m/20260101120000_add_users.down.sql"));
    }

    #[test]
    fn generate_down_path_subdir() {
        let pattern = MigrationPattern {
            layout: Layout::SubdirUpDown,
            ..Default::default()
        };
        let path = pattern.generate_down_path(Path::new("/m"), "add users", "20260101120000");
        assert_eq!(path, PathBuf::from("/m/20260101120000_add_users/down.sql"));
    }

    // ── Parse name ───────────────────────────────────────────────

    #[test]
    fn parse_name_sqlx_style() {
        let pattern = MigrationPattern::default();
        let result = pattern.parse_name("20260101120000_create_users");
        assert_eq!(
            result,
            Some(("20260101120000".into(), "create users".into()))
        );
    }

    #[test]
    fn parse_name_flyway_style() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::FlywayVersion,
            separator: Separator::DoubleUnderscore,
            ..Default::default()
        };
        let result = pattern.parse_name("V2__add_posts_table");
        assert_eq!(result, Some(("V2".into(), "add posts table".into())));
    }

    #[test]
    fn parse_name_kebab_style() {
        let pattern = MigrationPattern {
            separator: Separator::Hyphen,
            case_convention: CaseConvention::KebabCase,
            ..Default::default()
        };
        let result = pattern.parse_name("20260101120000-create-users");
        assert_eq!(
            result,
            Some(("20260101120000".into(), "create users".into()))
        );
    }

    #[test]
    fn parse_name_pascal_style() {
        let pattern = MigrationPattern {
            separator: Separator::Hyphen,
            case_convention: CaseConvention::PascalCase,
            ..Default::default()
        };
        let result = pattern.parse_name("20260101120000-CreateUsers");
        assert_eq!(
            result,
            Some(("20260101120000".into(), "create users".into()))
        );
    }

    #[test]
    fn parse_name_invalid() {
        let pattern = MigrationPattern::default();
        assert_eq!(pattern.parse_name("not_a_migration"), None);
        assert_eq!(pattern.parse_name(""), None);
    }

    // ── Write migration ──────────────────────────────────────────

    #[test]
    fn write_migration_flat() {
        let dir = tempdir();
        let pattern = MigrationPattern::default();
        let path = pattern
            .write_migration(
                &dir,
                "add users table",
                "CREATE TABLE users();\n",
                "20260214120000",
            )
            .unwrap();

        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "20260214120000_add_users_table.up.sql"
        );
        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "CREATE TABLE users();\n");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_migration_subdir() {
        let dir = tempdir();
        let pattern = MigrationPattern {
            layout: Layout::SubdirUpDown,
            ..Default::default()
        };
        let path = pattern
            .write_migration(
                &dir,
                "add users",
                "CREATE TABLE users();\n",
                "20260214120000",
            )
            .unwrap();

        assert_eq!(path.file_name().unwrap().to_str().unwrap(), "up.sql");
        assert!(path
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("add_users"));
        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "CREATE TABLE users();\n");

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Discover SQL files ───────────────────────────────────────

    #[test]
    fn discover_sql_files_flat() {
        let dir = tempdir();
        fs::write(dir.join("001_init.up.sql"), "sql").unwrap();
        fs::write(dir.join("002_add.up.sql"), "sql").unwrap();
        fs::write(dir.join("README.md"), "not sql").unwrap();

        let files = discover_sql_files(&dir);
        assert_eq!(files.len(), 2);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_sql_files_recursive() {
        let dir = tempdir();
        let sub = dir.join("20260101_init");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("up.sql"), "sql").unwrap();
        fs::write(sub.join("down.sql"), "sql").unwrap();

        let files = discover_sql_files(&dir);
        assert_eq!(files.len(), 2);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_sql_files_skips_hidden() {
        let dir = tempdir();
        let hidden = dir.join(".snapshots");
        fs::create_dir_all(&hidden).unwrap();
        fs::write(hidden.join("001.sql"), "sql").unwrap();
        fs::write(dir.join("001_init.sql"), "sql").unwrap();

        let files = discover_sql_files(&dir);
        assert_eq!(files.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    // ── Collect prefixes ─────────────────────────────────────────

    #[test]
    fn collect_prefixes_sequential() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::Sequential { width: 4 },
            ..Default::default()
        };
        let names = ["0001_init", "0002_add_users", "0003_add_posts"];
        let prefixes = pattern.collect_prefixes(&names);
        assert_eq!(prefixes, vec!["0001", "0002", "0003"]);
    }

    #[test]
    fn collect_prefixes_flyway() {
        let pattern = MigrationPattern {
            prefix: PrefixKind::FlywayVersion,
            ..Default::default()
        };
        let names = ["V1__init", "V2__add_users"];
        let prefixes = pattern.collect_prefixes(&names);
        assert_eq!(prefixes, vec!["V1", "V2"]);
    }

    // ── Classify prefix ──────────────────────────────────────────

    #[test]
    fn classify_prefix_14_digit() {
        assert_eq!(classify_prefix("20260101120000_create"), "timestamp14");
    }

    #[test]
    fn classify_prefix_segmented() {
        assert_eq!(
            classify_prefix("2026_01_01_120000_create"),
            "timestamp_segmented"
        );
    }

    #[test]
    fn classify_prefix_epoch_millis() {
        assert_eq!(classify_prefix("1721930481972-Create"), "epoch_millis");
    }

    #[test]
    fn classify_prefix_epoch_seconds() {
        assert_eq!(classify_prefix("1500360784_create"), "epoch_seconds");
    }

    #[test]
    fn classify_prefix_sequential() {
        assert_eq!(classify_prefix("0001_init"), "sequential_4");
        assert_eq!(classify_prefix("000001_init"), "sequential_6");
    }

    #[test]
    fn classify_prefix_flyway() {
        assert_eq!(classify_prefix("V1__create"), "flyway");
        assert_eq!(classify_prefix("V2_1__add"), "flyway");
    }

    // ── Separator as_str ─────────────────────────────────────────

    #[test]
    fn separator_as_str() {
        assert_eq!(Separator::Underscore.as_str(), "_");
        assert_eq!(Separator::Hyphen.as_str(), "-");
        assert_eq!(Separator::DoubleUnderscore.as_str(), "__");
    }

    // ── Case classification ──────────────────────────────────────

    #[test]
    fn classify_snake_case() {
        assert_eq!(
            classify_case("create_users_table"),
            CaseConvention::SnakeCase
        );
    }

    #[test]
    fn classify_kebab_case() {
        assert_eq!(
            classify_case("create-users-table"),
            CaseConvention::KebabCase
        );
    }

    #[test]
    fn classify_pascal_case() {
        assert_eq!(
            classify_case("CreateUsersTable"),
            CaseConvention::PascalCase
        );
    }

    #[test]
    fn classify_single_word() {
        assert_eq!(classify_case("initial"), CaseConvention::SnakeCase);
    }

    // ── Split PascalCase ─────────────────────────────────────────

    #[test]
    fn split_pascal_case_basic() {
        assert_eq!(split_pascal_case("CreateUsers"), "create users");
    }

    #[test]
    fn split_pascal_case_single_word() {
        assert_eq!(split_pascal_case("Initial"), "initial");
    }

    #[test]
    fn split_pascal_case_multi_word() {
        assert_eq!(split_pascal_case("AddEmailToUsers"), "add email to users");
    }
}
