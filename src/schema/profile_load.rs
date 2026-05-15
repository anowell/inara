//! Tier-0 statistics loading — the IO that populates
//! [`profile`](crate::schema::profile) structs.
//!
//! Everything here reads *catalog and planner metadata only* (`pg_class`,
//! `pg_table_size()`, `pg_stat_user_tables`, the `pg_stats` view). None of it
//! scans a user table's heap, so it is instant regardless of table size and
//! needs no safety gate. Exact, heap-touching probes are Tier 1 and live
//! elsewhere.
//!
//! ## The `anyarray` problem
//!
//! `pg_stats.most_common_vals` and `histogram_bounds` are typed `anyarray`,
//! which sqlx cannot decode. They *can* be cast to `text` (the array's text
//! rendering, e.g. `{active,pending}`), which [`parse_pg_array`] then parses
//! back into a `Vec<String>`. Frequencies (`most_common_freqs`) are a concrete
//! `real[]` and decode directly.

use sqlx::PgPool;

use super::profile::{ColumnProfile, TableProfile};

/// Load the Tier-0 [`TableProfile`] for one table. Catalog-only; instant.
pub async fn load_table_profile(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> Result<TableProfile, sqlx::Error> {
    let row: Option<TableStatRow> = sqlx::query_as(
        "SELECT
             c.reltuples::bigint                                  AS reltuples,
             pg_table_size(c.oid)::bigint                         AS heap_bytes,
             pg_indexes_size(c.oid)::bigint                       AS index_bytes,
             COALESCE(s.n_tup_ins, 0)::bigint                     AS inserts,
             COALESCE(s.n_tup_upd, 0)::bigint                     AS updates,
             COALESCE(s.n_tup_del, 0)::bigint                     AS deletes,
             EXTRACT(DAY FROM now() - GREATEST(s.last_analyze, s.last_autoanalyze))::bigint
                                                                  AS analyze_age_days
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_optional(pool)
    .await?;

    Ok(match row {
        Some(r) => TableProfile {
            // PG14+ reports reltuples = -1 for a never-analyzed table.
            estimated_rows: (r.reltuples >= 0).then_some(r.reltuples),
            heap_bytes: r.heap_bytes,
            index_bytes: r.index_bytes,
            inserts: r.inserts,
            updates: r.updates,
            deletes: r.deletes,
            analyze_age_days: r.analyze_age_days,
        },
        None => TableProfile {
            estimated_rows: None,
            heap_bytes: 0,
            index_bytes: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
            analyze_age_days: None,
        },
    })
}

/// Load the Tier-0 [`ColumnProfile`] for one column from the `pg_stats` view.
///
/// A missing `pg_stats` row yields [`ColumnProfile::unavailable`] — the column
/// has never been analyzed, or the current role cannot see its statistics.
pub async fn load_column_profile(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<ColumnProfile, sqlx::Error> {
    let row: Option<ColumnStatRow> = sqlx::query_as(
        "SELECT
             null_frac,
             n_distinct,
             avg_width,
             correlation,
             most_common_vals::text  AS mcv_text,
             most_common_freqs,
             histogram_bounds::text  AS histogram_text
         FROM pg_stats
         WHERE schemaname = $1 AND tablename = $2 AND attname = $3",
    )
    .bind(schema)
    .bind(table)
    .bind(column)
    .fetch_optional(pool)
    .await?;

    Ok(match row {
        Some(r) => ColumnProfile {
            null_frac: r.null_frac as f64,
            n_distinct: r.n_distinct as f64,
            avg_width: r.avg_width,
            correlation: r.correlation.map(|c| c as f64),
            mcv: r
                .mcv_text
                .as_deref()
                .map(parse_pg_array)
                .unwrap_or_default(),
            mcv_freqs: r
                .most_common_freqs
                .unwrap_or_default()
                .into_iter()
                .map(|f| f as f64)
                .collect(),
            histogram: r
                .histogram_text
                .as_deref()
                .map(parse_pg_array)
                .unwrap_or_default(),
            analyzed: true,
        },
        None => ColumnProfile::unavailable(),
    })
}

#[derive(sqlx::FromRow)]
struct TableStatRow {
    reltuples: i64,
    heap_bytes: i64,
    index_bytes: i64,
    inserts: i64,
    updates: i64,
    deletes: i64,
    analyze_age_days: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct ColumnStatRow {
    null_frac: f32,
    n_distinct: f32,
    avg_width: i32,
    correlation: Option<f32>,
    mcv_text: Option<String>,
    most_common_freqs: Option<Vec<f32>>,
    histogram_text: Option<String>,
}

/// Parse a one-dimensional Postgres array literal (`{a,b,"c, d"}`) into its
/// elements.
///
/// Handles the output format `array_out` produces: optional double-quoting,
/// `\"` / `\\` escapes inside quotes, and unquoted `NULL` for null elements
/// (rendered here as the literal string `NULL`). Anything that does not look
/// like an array literal yields an empty vec.
pub fn parse_pg_array(literal: &str) -> Vec<String> {
    let bytes = literal.trim();
    let Some(inner) = bytes.strip_prefix('{').and_then(|s| s.strip_suffix('}')) else {
        return Vec::new();
    };
    if inner.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut chars = inner.chars().peekable();
    loop {
        // Skip leading whitespace before an element.
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }

        let element = if chars.peek() == Some(&'"') {
            chars.next(); // opening quote
            let mut s = String::new();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => {
                        if let Some(escaped) = chars.next() {
                            s.push(escaped);
                        }
                    }
                    '"' => break,
                    _ => s.push(c),
                }
            }
            s
        } else {
            let mut s = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                s.push(c);
                chars.next();
            }
            let trimmed = s.trim();
            // An unquoted, case-insensitive NULL is a null element.
            if trimmed.eq_ignore_ascii_case("NULL") {
                "NULL".to_string()
            } else {
                trimmed.to_string()
            }
        };
        out.push(element);

        // Skip whitespace, then expect a comma or the end.
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }
        match chars.next() {
            Some(',') => continue,
            _ => break,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_array() {
        assert_eq!(parse_pg_array("{}"), Vec::<String>::new());
    }

    #[test]
    fn parse_non_array_yields_empty() {
        assert_eq!(parse_pg_array(""), Vec::<String>::new());
        assert_eq!(parse_pg_array("not an array"), Vec::<String>::new());
    }

    #[test]
    fn parse_simple_unquoted() {
        assert_eq!(
            parse_pg_array("{active,pending,disabled}"),
            vec!["active", "pending", "disabled"]
        );
    }

    #[test]
    fn parse_numeric_elements() {
        assert_eq!(parse_pg_array("{0,42,18222}"), vec!["0", "42", "18222"]);
    }

    #[test]
    fn parse_quoted_with_comma() {
        // Postgres array output escapes interior quotes with a backslash.
        assert_eq!(
            parse_pg_array(r#"{"smith, j",plain,"o\"brien"}"#),
            vec!["smith, j", "plain", r#"o"brien"#]
        );
    }

    #[test]
    fn parse_quoted_with_escapes() {
        assert_eq!(parse_pg_array(r#"{"a\"b","c\\d"}"#), vec![r#"a"b"#, r"c\d"]);
    }

    #[test]
    fn parse_null_element() {
        assert_eq!(parse_pg_array("{a,NULL,b}"), vec!["a", "NULL", "b"]);
    }

    #[test]
    fn parse_trims_unquoted_whitespace() {
        assert_eq!(parse_pg_array("{ a , b }"), vec!["a", "b"]);
    }

    #[test]
    fn parse_single_element() {
        assert_eq!(parse_pg_array("{solo}"), vec!["solo"]);
    }
}
