use std::sync::{Arc, Mutex};

use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;
use sqlx::PgPool;

use crate::schema::types::PgType;

/// Row count threshold above which we warn before running column stats
/// on an unindexed column.
const LARGE_TABLE_THRESHOLD: f32 = 100_000.0;

/// HUD query state — stored in `AppState` when mode is HUD.
#[derive(Debug, Clone)]
pub struct HudState {
    /// What we're querying.
    pub target: HudTarget,
    /// Current query status.
    pub status: HudStatus,
}

/// What the HUD is focused on.
#[derive(Debug, Clone)]
pub enum HudTarget {
    /// Table-level stats.
    Table { name: String },
    /// Column-level stats.
    Column {
        table: String,
        column: String,
        pg_type: PgType,
    },
}

/// Query lifecycle status.
#[derive(Debug, Clone)]
pub enum HudStatus {
    /// Query is running.
    Loading,
    /// Table-level results.
    TableResult(TableStats),
    /// Column-level results.
    ColumnResult(ColumnStats),
    /// Safety warning — table is large and column is not indexed.
    SafetyWarning {
        row_estimate: f32,
        table: String,
        column: String,
        pg_type: PgType,
    },
    /// Query failed.
    Error(String),
}

/// Table-level statistics returned by the HUD query.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: i64,
    pub size_bytes: i64,
    pub size_display: String,
    pub indexed_columns: Vec<String>,
}

/// Column-level statistics returned by the HUD query.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub null_count: i64,
    pub distinct_count: i64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub avg_value: Option<String>,
}

/// Shared handle for receiving async query results.
pub type HudResultHandle = Arc<Mutex<Option<HudStatus>>>;

/// Create a new result handle for async query delivery.
pub fn new_result_handle() -> HudResultHandle {
    Arc::new(Mutex::new(None))
}

// ── Async query functions ─────────────────────────────────────────────

/// Spawn a background task that queries table-level stats and writes
/// the result into the shared handle.
pub fn spawn_table_query(pool: PgPool, schema: String, table: String, handle: HudResultHandle) {
    tokio::spawn(async move {
        let result = query_table_stats(&pool, &schema, &table).await;
        let status = match result {
            Ok(stats) => HudStatus::TableResult(stats),
            Err(e) => HudStatus::Error(e.to_string()),
        };
        if let Ok(mut guard) = handle.lock() {
            *guard = Some(status);
        }
    });
}

/// Spawn a background task that checks safety and then queries column stats.
pub fn spawn_column_query(
    pool: PgPool,
    schema: String,
    table: String,
    column: String,
    pg_type: PgType,
    handle: HudResultHandle,
) {
    tokio::spawn(async move {
        let result = query_column_stats(&pool, &schema, &table, &column, &pg_type).await;
        let status = match result {
            Ok(stats) => HudStatus::ColumnResult(stats),
            Err(e) => HudStatus::Error(e.to_string()),
        };
        if let Ok(mut guard) = handle.lock() {
            *guard = Some(status);
        }
    });
}

/// Check reltuples and index coverage to determine if a safety warning is needed.
pub async fn check_safety(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<Option<f32>, sqlx::Error> {
    // Get estimated row count from pg_class
    let row: (f32,) = sqlx::query_as(
        "SELECT COALESCE(c.reltuples, 0)
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await?;

    let reltuples = row.0;

    if reltuples < LARGE_TABLE_THRESHOLD {
        return Ok(None); // Safe to query
    }

    // Check if column has an index
    let indexed: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1
            FROM pg_index ix
            JOIN pg_class ci ON ci.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = ix.indrelid
            JOIN pg_class ct ON ct.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = ct.relnamespace
            WHERE n.nspname = $1
              AND ct.relname = $2
              AND a.attname = $3
              AND a.attnum = ANY(ix.indkey)
        )",
    )
    .bind(schema)
    .bind(table)
    .bind(column)
    .fetch_one(pool)
    .await?;

    if indexed {
        Ok(None) // Indexed, safe to query
    } else {
        Ok(Some(reltuples)) // Needs warning
    }
}

/// Spawn a safety check, writing a warning or triggering the column query.
pub fn spawn_safety_check(
    pool: PgPool,
    schema: String,
    table: String,
    column: String,
    pg_type: PgType,
    handle: HudResultHandle,
) {
    tokio::spawn(async move {
        match check_safety(&pool, &schema, &table, &column).await {
            Ok(Some(row_estimate)) => {
                // Needs confirmation
                let status = HudStatus::SafetyWarning {
                    row_estimate,
                    table,
                    column,
                    pg_type,
                };
                if let Ok(mut guard) = handle.lock() {
                    *guard = Some(status);
                }
            }
            Ok(None) => {
                // Safe — run the query directly
                let result = query_column_stats(&pool, &schema, &table, &column, &pg_type).await;
                let status = match result {
                    Ok(stats) => HudStatus::ColumnResult(stats),
                    Err(e) => HudStatus::Error(e.to_string()),
                };
                if let Ok(mut guard) = handle.lock() {
                    *guard = Some(status);
                }
            }
            Err(e) => {
                if let Ok(mut guard) = handle.lock() {
                    *guard = Some(HudStatus::Error(e.to_string()));
                }
            }
        }
    });
}

// ── Database queries ──────────────────────────────────────────────────

pub async fn query_table_stats(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> Result<TableStats, sqlx::Error> {
    // Row count (exact via count(*))
    let count_query = format!(
        "SELECT COUNT(*) FROM {}.{}",
        quote_ident(schema),
        quote_ident(table)
    );
    let row_count: (i64,) = sqlx::query_as(&count_query).fetch_one(pool).await?;

    // Table size from pg_class (relpages * 8192)
    let size_row: (i64,) = sqlx::query_as(
        "SELECT (c.relpages * 8192)::bigint
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await?;

    // Indexed columns
    let indexed_rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT a.attname
         FROM pg_index ix
         JOIN pg_class ci ON ci.oid = ix.indexrelid
         JOIN pg_attribute a ON a.attrelid = ix.indrelid
         JOIN pg_class ct ON ct.oid = ix.indrelid
         JOIN pg_namespace n ON n.oid = ct.relnamespace
         WHERE n.nspname = $1
           AND ct.relname = $2
           AND a.attnum = ANY(ix.indkey)
           AND a.attnum > 0
         ORDER BY a.attname",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await?;

    let indexed_columns: Vec<String> = indexed_rows.into_iter().map(|r| r.0).collect();
    let size_display = format_size(size_row.0);

    Ok(TableStats {
        row_count: row_count.0,
        size_bytes: size_row.0,
        size_display,
        indexed_columns,
    })
}

pub async fn query_column_stats(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
    pg_type: &PgType,
) -> Result<ColumnStats, sqlx::Error> {
    let col_ref = format!(
        "{}.{}.{}",
        quote_ident(schema),
        quote_ident(table),
        quote_ident(column)
    );
    let table_ref = format!("{}.{}", quote_ident(schema), quote_ident(table));

    // Base stats: null count and distinct count
    let base_query = format!(
        "SELECT
            COUNT(*) FILTER (WHERE {col} IS NULL) AS null_count,
            COUNT(DISTINCT {col}) AS distinct_count
         FROM {tbl}",
        col = col_ref,
        tbl = table_ref,
    );
    let base: (i64, i64) = sqlx::query_as(&base_query).fetch_one(pool).await?;

    let mut stats = ColumnStats {
        null_count: base.0,
        distinct_count: base.1,
        min_value: None,
        max_value: None,
        avg_value: None,
    };

    // min/max for numeric and date/time types
    if is_numeric_type(pg_type) {
        let agg_query = format!(
            "SELECT
                MIN({col})::text,
                MAX({col})::text,
                AVG({col})::text
             FROM {tbl}",
            col = col_ref,
            tbl = table_ref,
        );
        let agg: (Option<String>, Option<String>, Option<String>) =
            sqlx::query_as(&agg_query).fetch_one(pool).await?;
        stats.min_value = agg.0;
        stats.max_value = agg.1;
        stats.avg_value = agg.2;
    } else if is_temporal_type(pg_type) {
        let agg_query = format!(
            "SELECT
                MIN({col})::text,
                MAX({col})::text
             FROM {tbl}",
            col = col_ref,
            tbl = table_ref,
        );
        let agg: (Option<String>, Option<String>) =
            sqlx::query_as(&agg_query).fetch_one(pool).await?;
        stats.min_value = agg.0;
        stats.max_value = agg.1;
    }

    Ok(stats)
}

// ── Type classification ───────────────────────────────────────────────

fn is_numeric_type(pg_type: &PgType) -> bool {
    matches!(
        pg_type,
        PgType::SmallInt
            | PgType::Integer
            | PgType::BigInt
            | PgType::Real
            | PgType::DoublePrecision
            | PgType::Numeric(_)
    )
}

fn is_temporal_type(pg_type: &PgType) -> bool {
    matches!(
        pg_type,
        PgType::Timestamp | PgType::Timestamptz | PgType::Date | PgType::Time | PgType::Timetz
    )
}

// ── Helpers ───────────────────────────────────────────────────────────

/// Quote a SQL identifier to prevent injection.
fn quote_ident(ident: &str) -> String {
    // Double any existing quotes and wrap in quotes
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Format byte size into human-readable string.
fn format_size(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1024 * 1024;
    const GB: i64 = 1024 * 1024 * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

// ── Rendering ─────────────────────────────────────────────────────────

const HUD_BORDER_STYLE: Style = Style::new().fg(Color::Magenta);
const HUD_LABEL_STYLE: Style = Style::new().fg(Color::DarkGray);
const HUD_VALUE_STYLE: Style = Style::new().fg(Color::White);
const HUD_WARN_STYLE: Style = Style::new().fg(Color::Yellow);
const HUD_ERROR_STYLE: Style = Style::new().fg(Color::Red);

/// Render the HUD overlay on top of the existing frame.
pub fn render_hud(frame: &mut Frame, area: Rect, hud: &HudState) {
    let (title, lines) = match &hud.status {
        HudStatus::Loading => {
            let title = hud_title(&hud.target);
            let lines = vec![Line::from(Span::styled("  Loading...", HUD_LABEL_STYLE))];
            (title, lines)
        }
        HudStatus::TableResult(stats) => render_table_stats(&hud.target, stats),
        HudStatus::ColumnResult(stats) => render_column_stats(&hud.target, stats),
        HudStatus::SafetyWarning {
            row_estimate,
            table,
            column,
            ..
        } => render_safety_warning(row_estimate, table, column),
        HudStatus::Error(msg) => {
            let title = hud_title(&hud.target);
            let lines = vec![Line::from(Span::styled(
                format!("  Error: {msg}"),
                HUD_ERROR_STYLE,
            ))];
            (title, lines)
        }
    };

    let content_height = lines.len() as u16 + 2; // +2 for border
    let content_width = lines
        .iter()
        .map(|l| l.spans.iter().map(|s| s.width()).sum::<usize>())
        .max()
        .unwrap_or(20) as u16
        + 4; // +4 for border + padding

    let popup = centered_rect(
        content_width.min(area.width.saturating_sub(4)),
        content_height.min(area.height.saturating_sub(2)),
        area,
    );

    frame.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(HUD_BORDER_STYLE)
        .title(format!(" {title} "))
        .title_style(
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        );

    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let paragraph = Paragraph::new(lines);
    frame.render_widget(paragraph, inner);
}

fn hud_title(target: &HudTarget) -> String {
    match target {
        HudTarget::Table { name } => format!("HUD: {name}"),
        HudTarget::Column { table, column, .. } => format!("HUD: {table}.{column}"),
    }
}

fn render_table_stats(target: &HudTarget, stats: &TableStats) -> (String, Vec<Line<'static>>) {
    let title = hud_title(target);

    let indexed = if stats.indexed_columns.is_empty() {
        "none".to_string()
    } else {
        stats.indexed_columns.join(", ")
    };

    let lines = vec![
        hud_kv("Rows", &format_number(stats.row_count)),
        hud_kv("Size", &stats.size_display),
        hud_kv("Indexed", &indexed),
    ];

    (title, lines)
}

fn render_column_stats(target: &HudTarget, stats: &ColumnStats) -> (String, Vec<Line<'static>>) {
    let title = hud_title(target);

    let mut lines = vec![
        hud_kv("Nulls", &format_number(stats.null_count)),
        hud_kv("Distinct", &format_number(stats.distinct_count)),
    ];

    if let Some(min) = &stats.min_value {
        lines.push(hud_kv("Min", min));
    }
    if let Some(max) = &stats.max_value {
        lines.push(hud_kv("Max", max));
    }
    if let Some(avg) = &stats.avg_value {
        lines.push(hud_kv("Avg", avg));
    }

    (title, lines)
}

fn render_safety_warning(
    row_estimate: &f32,
    table: &str,
    column: &str,
) -> (String, Vec<Line<'static>>) {
    let title = format!("HUD: {table}.{column}");
    let lines = vec![
        Line::from(Span::styled(
            format!("  Table has ~{} rows", format_number(*row_estimate as i64)),
            HUD_WARN_STYLE,
        )),
        Line::from(Span::styled(
            format!("  Column \"{column}\" is not indexed"),
            HUD_WARN_STYLE,
        )),
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            "  Press 'y' to query, Esc to cancel",
            HUD_LABEL_STYLE,
        )),
    ];
    (title, lines)
}

/// Create a key-value line for the HUD.
fn hud_kv(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {label}: "), HUD_LABEL_STYLE),
        Span::styled(value.to_string(), HUD_VALUE_STYLE),
    ])
}

/// Format a number with thousand separators.
fn format_number(n: i64) -> String {
    if n < 1000 {
        return n.to_string();
    }

    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// Calculate a centered rectangle within the given area.
fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

// ── Unit tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
    }

    #[test]
    fn format_size_kilobytes() {
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(2048), "2.0 KB");
        assert_eq!(format_size(1536), "1.5 KB");
    }

    #[test]
    fn format_size_megabytes() {
        assert_eq!(format_size(1048576), "1.0 MB");
        assert_eq!(format_size(10 * 1048576), "10.0 MB");
    }

    #[test]
    fn format_size_gigabytes() {
        assert_eq!(format_size(1073741824), "1.0 GB");
    }

    #[test]
    fn format_number_small() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(42), "42");
        assert_eq!(format_number(999), "999");
    }

    #[test]
    fn format_number_thousands() {
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(12345), "12,345");
        assert_eq!(format_number(1234567), "1,234,567");
    }

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn quote_ident_with_quotes() {
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    #[test]
    fn is_numeric_type_positive() {
        assert!(is_numeric_type(&PgType::Integer));
        assert!(is_numeric_type(&PgType::BigInt));
        assert!(is_numeric_type(&PgType::Numeric(Some((10, 2)))));
        assert!(is_numeric_type(&PgType::DoublePrecision));
    }

    #[test]
    fn is_numeric_type_negative() {
        assert!(!is_numeric_type(&PgType::Text));
        assert!(!is_numeric_type(&PgType::Uuid));
        assert!(!is_numeric_type(&PgType::Timestamptz));
    }

    #[test]
    fn is_temporal_type_positive() {
        assert!(is_temporal_type(&PgType::Timestamp));
        assert!(is_temporal_type(&PgType::Timestamptz));
        assert!(is_temporal_type(&PgType::Date));
    }

    #[test]
    fn is_temporal_type_negative() {
        assert!(!is_temporal_type(&PgType::Integer));
        assert!(!is_temporal_type(&PgType::Text));
    }

    #[test]
    fn hud_title_table() {
        let target = HudTarget::Table {
            name: "users".into(),
        };
        assert_eq!(hud_title(&target), "HUD: users");
    }

    #[test]
    fn hud_title_column() {
        let target = HudTarget::Column {
            table: "users".into(),
            column: "email".into(),
            pg_type: PgType::Text,
        };
        assert_eq!(hud_title(&target), "HUD: users.email");
    }

    #[test]
    fn table_stats_rendering() {
        let target = HudTarget::Table {
            name: "users".into(),
        };
        let stats = TableStats {
            row_count: 1500,
            size_bytes: 8192,
            size_display: "8.0 KB".into(),
            indexed_columns: vec!["id".into(), "email".into()],
        };
        let (title, lines) = render_table_stats(&target, &stats);
        assert_eq!(title, "HUD: users");
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn column_stats_rendering_numeric() {
        let target = HudTarget::Column {
            table: "users".into(),
            column: "age".into(),
            pg_type: PgType::Integer,
        };
        let stats = ColumnStats {
            null_count: 10,
            distinct_count: 50,
            min_value: Some("18".into()),
            max_value: Some("99".into()),
            avg_value: Some("42.5".into()),
        };
        let (title, lines) = render_column_stats(&target, &stats);
        assert_eq!(title, "HUD: users.age");
        assert_eq!(lines.len(), 5); // nulls, distinct, min, max, avg
    }

    #[test]
    fn column_stats_rendering_text() {
        let target = HudTarget::Column {
            table: "users".into(),
            column: "name".into(),
            pg_type: PgType::Text,
        };
        let stats = ColumnStats {
            null_count: 0,
            distinct_count: 100,
            min_value: None,
            max_value: None,
            avg_value: None,
        };
        let (_title, lines) = render_column_stats(&target, &stats);
        assert_eq!(lines.len(), 2); // nulls, distinct only
    }

    #[test]
    fn safety_warning_rendering() {
        let (title, lines) = render_safety_warning(&150_000.0, "big_table", "unindexed_col");
        assert_eq!(title, "HUD: big_table.unindexed_col");
        assert_eq!(lines.len(), 4);
    }

    #[test]
    fn centered_rect_calculation() {
        let area = Rect::new(0, 0, 80, 24);
        let popup = centered_rect(40, 10, area);
        assert_eq!(popup.x, 20);
        assert_eq!(popup.y, 7);
        assert_eq!(popup.width, 40);
        assert_eq!(popup.height, 10);
    }

    #[test]
    fn centered_rect_larger_than_area() {
        let area = Rect::new(0, 0, 20, 10);
        let popup = centered_rect(40, 20, area);
        // Should clamp to area dimensions
        assert_eq!(popup.width, 20);
        assert_eq!(popup.height, 10);
    }
}
