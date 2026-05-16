//! The Query HUD — a fast, instant-feeling glimpse at a table or column.
//!
//! The HUD is built on a **tiered** model:
//!
//! * **Tier 0** — catalog & planner statistics. The default. It reads only
//!   metadata Postgres already maintains ([`profile_load`]), never a user
//!   table's heap, so opening the HUD is a lookup, not a query — instant
//!   regardless of table size, and needing no safety gate.
//! * **Tier 1** — bounded *exact* probes (`COUNT(*)`, `COUNT(DISTINCT)`, JSONB
//!   key sampling). These do touch the heap, so they are opt-in: the HUD
//!   *offers* escalation and the user presses a key to run it.
//!
//! Measurement is kept separate from interpretation: the data layer
//! ([`profile`](crate::schema::profile)) holds raw numbers, the pure schema
//! layer ([`insight`](crate::schema::insight)) turns them into meaning, and
//! this module only dispatches and renders.

use std::sync::{Arc, Mutex};

use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;
use sqlx::PgPool;

use crate::schema::insight::{
    self, ColumnContext, ColumnInsights, ColumnTag, Distinctness, Ordering, Skew, Sparsity,
    TableRole, TypeClass,
};
use crate::schema::profile::{ColumnProfile, TableProfile};
use crate::schema::profile_load::{load_column_profile, load_table_profile};
use crate::schema::types::PgType;
use crate::schema::{IndexMethod, Table};

/// A table is "cheap" to probe exactly when it has fewer rows than this.
const CHEAP_ROW_LIMIT: i64 = 50_000;
/// …or when its heap is smaller than this many bytes (32 MiB).
const CHEAP_BYTE_LIMIT: i64 = 32 * 1024 * 1024;
/// Rows sampled when extracting top-level JSONB keys (Tier 1, kept bounded).
const JSONB_SAMPLE_ROWS: i64 = 500;
/// More secondary indexes than this and the HUD says "heavily indexed".
const HEAVY_INDEX_COUNT: usize = 6;

// ── HUD state ─────────────────────────────────────────────────────────

/// HUD query state — stored in `AppState` when mode is HUD.
#[derive(Debug, Clone)]
pub struct HudState {
    /// The Postgres schema being inspected. Multi-schema support is tracked
    /// separately; for now this is always `public`.
    pub schema: String,
    /// What we're inspecting.
    pub target: HudTarget,
    /// Current load/result status.
    pub status: HudStatus,
}

/// What the HUD is focused on.
#[derive(Debug, Clone)]
pub enum HudTarget {
    /// A table.
    Table { name: String },
    /// A column.
    Column {
        table: String,
        column: String,
        pg_type: PgType,
    },
}

/// HUD lifecycle status.
#[derive(Debug, Clone)]
pub enum HudStatus {
    /// Tier-0 load in flight.
    Loading,
    /// An `ANALYZE` is in flight; the HUD reloads with fresh stats when done.
    Analyzing,
    /// Tier-0 table result.
    Table(Box<TableHud>),
    /// Tier-0 column result.
    Column(Box<ColumnHud>),
    /// Load failed.
    Error(String),
}

/// Tier-0 result for a table, plus the escalation offer.
#[derive(Debug, Clone)]
pub struct TableHud {
    pub profile: TableProfile,
    pub role: Option<TableRole>,
    pub indexes: IndexSummary,
    pub escalation: Escalation,
}

/// Tier-0 result for a column, plus interpretation and the escalation offer.
#[derive(Debug, Clone)]
pub struct ColumnHud {
    pub profile: ColumnProfile,
    /// Table row estimate — needed to resolve `n_distinct` ratios.
    pub estimated_rows: Option<i64>,
    /// The owning table holds no rows — statistics can never be collected,
    /// so the HUD names it empty rather than offering `ANALYZE`.
    pub table_is_empty: bool,
    pub pg_type: PgType,
    pub type_class: TypeClass,
    pub insights: ColumnInsights,
    /// Enum variants, when the column's type is a Postgres enum.
    pub enum_variants: Option<Vec<String>>,
    /// `table.column` the foreign key points at, when this column is an FK.
    pub fk_target: Option<String>,
    /// Indexes covering this column, as `(name, method)`.
    pub covering_indexes: Vec<(String, IndexMethod)>,
    pub escalation: Escalation,
}

/// State of the Tier-1 escalation offer for a HUD result.
#[derive(Debug, Clone)]
pub enum Escalation {
    /// Deeper profiling does not apply to this target.
    Unavailable,
    /// Exact profiling is offered. `cheap` reflects the escalation
    /// heuristics — when false, the probe may scan a large table.
    Offered { cheap: bool },
    /// A Tier-1 probe is in flight.
    Running,
    /// A Tier-1 probe completed.
    Done(ExactProbe),
    /// A Tier-1 probe failed.
    Failed(String),
}

/// Results of a Tier-1 exact probe.
#[derive(Debug, Clone, Default)]
pub struct ExactProbe {
    /// Exact `COUNT(*)`.
    pub exact_rows: i64,
    /// Exact `COUNT(DISTINCT col)`, when the column type supports it.
    pub exact_distinct: Option<i64>,
    /// Sampled top-level JSONB keys with their occurrence counts.
    pub jsonb_keys: Vec<(String, i64)>,
    /// Number of rows sampled for [`Self::jsonb_keys`].
    pub jsonb_sample_rows: i64,
}

/// A compact summary of a table's indexes, built purely from the schema model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexSummary {
    /// The table has a primary key.
    pub has_pk: bool,
    /// Number of `UNIQUE` constraints.
    pub unique_count: usize,
    /// Access method of each secondary (non-PK, non-unique) index.
    pub secondary: Vec<IndexMethod>,
    /// FK column lists with no covering index — an operational hazard.
    pub unindexed_fks: Vec<String>,
}

/// Schema-derived context for a column HUD, gathered before the async load so
/// the async task does pure IO.
#[derive(Debug, Clone, Default)]
pub struct ColumnHudInput {
    pub ctx: ColumnContext,
    pub enum_variants: Option<Vec<String>>,
    pub fk_target: Option<String>,
    pub covering_indexes: Vec<(String, IndexMethod)>,
}

/// Shared handle for receiving async query results.
pub type HudResultHandle = Arc<Mutex<Option<HudStatus>>>;

/// Create a new result handle for async query delivery.
pub fn new_result_handle() -> HudResultHandle {
    Arc::new(Mutex::new(None))
}

fn deliver(handle: &HudResultHandle, status: HudStatus) {
    if let Ok(mut guard) = handle.lock() {
        *guard = Some(status);
    }
}

// ── Tier 0: spawning catalog loads ────────────────────────────────────

/// Spawn the Tier-0 catalog load for a table.
pub fn spawn_table_hud(pool: PgPool, schema: String, table: Table, handle: HudResultHandle) {
    tokio::spawn(async move {
        let status = match build_table_hud(&pool, &schema, &table).await {
            Ok(hud) => HudStatus::Table(Box::new(hud)),
            Err(e) => HudStatus::Error(e.to_string()),
        };
        deliver(&handle, status);
    });
}

/// Spawn the Tier-0 catalog load for a column.
pub fn spawn_column_hud(
    pool: PgPool,
    schema: String,
    table: String,
    column: String,
    pg_type: PgType,
    input: ColumnHudInput,
    handle: HudResultHandle,
) {
    tokio::spawn(async move {
        let status = match build_column_hud(&pool, &schema, &table, &column, pg_type, input).await {
            Ok(hud) => HudStatus::Column(Box::new(hud)),
            Err(e) => HudStatus::Error(e.to_string()),
        };
        deliver(&handle, status);
    });
}

async fn build_table_hud(
    pool: &PgPool,
    schema: &str,
    table: &Table,
) -> Result<TableHud, sqlx::Error> {
    let profile = load_table_profile(pool, schema, &table.name).await?;
    let role = insight::infer_table_role(&profile, table);
    let indexes = summarize_indexes(table);
    let escalation = Escalation::Offered {
        cheap: is_cheap(&profile),
    };
    Ok(TableHud {
        profile,
        role,
        indexes,
        escalation,
    })
}

async fn build_column_hud(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
    pg_type: PgType,
    input: ColumnHudInput,
) -> Result<ColumnHud, sqlx::Error> {
    // The table profile is catalog-only and instant; we need its row estimate
    // to resolve the column's `n_distinct` ratio form.
    let table_profile = load_table_profile(pool, schema, table).await?;
    let estimated_rows = table_profile.estimated_rows;
    let table_is_empty = table_profile.is_empty();
    let profile = load_column_profile(pool, schema, table, column).await?;
    let type_class = insight::classify_type(&pg_type);
    let insights = insight::derive_column_insights(&profile, &pg_type, estimated_rows, input.ctx);

    let escalation = Escalation::Offered {
        cheap: is_cheap(&table_profile),
    };

    Ok(ColumnHud {
        profile,
        estimated_rows,
        table_is_empty,
        pg_type,
        type_class,
        insights,
        enum_variants: input.enum_variants,
        fk_target: input.fk_target,
        covering_indexes: input.covering_indexes,
        escalation,
    })
}

// ── ANALYZE: refreshing planner statistics ────────────────────────────

/// Spawn an `ANALYZE` on `table`, then reload the table HUD.
///
/// On success the refreshed Tier-0 result is delivered. If the role lacks
/// permission to analyze the table, an error status is delivered instead.
pub fn spawn_analyze_table(pool: PgPool, schema: String, table: Table, handle: HudResultHandle) {
    tokio::spawn(async move {
        let status = match run_analyze(&pool, &schema, &table.name).await {
            Ok(()) => match build_table_hud(&pool, &schema, &table).await {
                Ok(hud) => HudStatus::Table(Box::new(hud)),
                Err(e) => HudStatus::Error(e.to_string()),
            },
            Err(msg) => HudStatus::Error(msg),
        };
        deliver(&handle, status);
    });
}

/// Spawn an `ANALYZE` on `table`, then reload the column HUD.
pub fn spawn_analyze_column(
    pool: PgPool,
    schema: String,
    table: String,
    column: String,
    pg_type: PgType,
    input: ColumnHudInput,
    handle: HudResultHandle,
) {
    tokio::spawn(async move {
        let status = match run_analyze(&pool, &schema, &table).await {
            Ok(()) => {
                match build_column_hud(&pool, &schema, &table, &column, pg_type, input).await {
                    Ok(hud) => HudStatus::Column(Box::new(hud)),
                    Err(e) => HudStatus::Error(e.to_string()),
                }
            }
            Err(msg) => HudStatus::Error(msg),
        };
        deliver(&handle, status);
    });
}

/// Run `ANALYZE` on a table after verifying the role may do so.
///
/// `ANALYZE` only emits a warning (not an error) when the role lacks
/// privileges, so permission is checked up front and reported clearly.
async fn run_analyze(pool: &PgPool, schema: &str, table: &str) -> Result<(), String> {
    if !can_analyze(pool, schema, table)
        .await
        .map_err(|e| e.to_string())?
    {
        return Err(format!(
            "ANALYZE: permission denied — must own \"{table}\" or be superuser"
        ));
    }
    let sql = format!("ANALYZE {}.{}", quote_ident(schema), quote_ident(table));
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map_err(|e| format!("ANALYZE failed: {e}"))?;
    Ok(())
}

/// Whether the current role may `ANALYZE` the table — i.e. it owns the table
/// (directly or via role membership) or is a superuser.
async fn can_analyze(pool: &PgPool, schema: &str, table: &str) -> Result<bool, sqlx::Error> {
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT pg_catalog.pg_has_role(current_user, c.relowner, 'USAGE')
                OR current_setting('is_superuser')::bool
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(b,)| b).unwrap_or(false))
}

// ── Tier 1: spawning exact probes ─────────────────────────────────────

/// Spawn a Tier-1 exact probe for a table, escalating an existing result.
pub fn spawn_table_escalation(
    pool: PgPool,
    schema: String,
    name: String,
    mut hud: TableHud,
    handle: HudResultHandle,
) {
    tokio::spawn(async move {
        hud.escalation = match probe_table(&pool, &schema, &name).await {
            Ok(probe) => Escalation::Done(probe),
            Err(e) => Escalation::Failed(e.to_string()),
        };
        deliver(&handle, HudStatus::Table(Box::new(hud)));
    });
}

/// Spawn a Tier-1 exact probe for a column, escalating an existing result.
pub fn spawn_column_escalation(
    pool: PgPool,
    schema: String,
    table: String,
    column: String,
    pg_type: PgType,
    mut hud: ColumnHud,
    handle: HudResultHandle,
) {
    tokio::spawn(async move {
        hud.escalation = match probe_column(&pool, &schema, &table, &column, &pg_type).await {
            Ok(probe) => Escalation::Done(probe),
            Err(e) => Escalation::Failed(e.to_string()),
        };
        deliver(&handle, HudStatus::Column(Box::new(hud)));
    });
}

pub async fn probe_table(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> Result<ExactProbe, sqlx::Error> {
    let sql = format!(
        "SELECT COUNT(*) FROM {}.{}",
        quote_ident(schema),
        quote_ident(table)
    );
    let (exact_rows,): (i64,) = sqlx::query_as(&sql).fetch_one(pool).await?;
    Ok(ExactProbe {
        exact_rows,
        ..ExactProbe::default()
    })
}

pub async fn probe_column(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
    pg_type: &PgType,
) -> Result<ExactProbe, sqlx::Error> {
    let table_ref = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let col_ref = quote_ident(column);

    let class = insight::classify_type(pg_type);

    // `json` has no equality operator, so COUNT(DISTINCT) is invalid for it;
    // `jsonb` is profiled by key sampling instead of a distinct count.
    let want_distinct = !matches!(pg_type, PgType::Json | PgType::Jsonb);

    let probe = if want_distinct {
        let sql = format!(
            "SELECT COUNT(*)::bigint, COUNT(DISTINCT {col})::bigint FROM {tbl}",
            col = col_ref,
            tbl = table_ref,
        );
        let (exact_rows, exact_distinct): (i64, i64) = sqlx::query_as(&sql).fetch_one(pool).await?;
        ExactProbe {
            exact_rows,
            exact_distinct: Some(exact_distinct),
            ..ExactProbe::default()
        }
    } else {
        let sql = format!("SELECT COUNT(*)::bigint FROM {table_ref}");
        let (exact_rows,): (i64,) = sqlx::query_as(&sql).fetch_one(pool).await?;
        ExactProbe {
            exact_rows,
            ..ExactProbe::default()
        }
    };

    if class == TypeClass::Json && matches!(pg_type, PgType::Jsonb) {
        let mut probe = probe;
        probe.jsonb_sample_rows = JSONB_SAMPLE_ROWS;
        probe.jsonb_keys = probe_jsonb_keys(pool, &table_ref, &col_ref).await?;
        Ok(probe)
    } else {
        Ok(probe)
    }
}

/// Sample top-level JSONB keys from a bounded number of rows — deliberately
/// kept fast (a `LIMIT`ed scan), never a full-table key inference.
async fn probe_jsonb_keys(
    pool: &PgPool,
    table_ref: &str,
    col_ref: &str,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let sql = format!(
        "SELECT k, COUNT(*)::bigint AS n
         FROM (
             SELECT jsonb_object_keys({col}) AS k
             FROM (
                 SELECT {col} FROM {tbl}
                 WHERE {col} IS NOT NULL
                 LIMIT {limit}
             ) sample
         ) keys
         GROUP BY k
         ORDER BY n DESC, k
         LIMIT 25",
        col = col_ref,
        tbl = table_ref,
        limit = JSONB_SAMPLE_ROWS,
    );
    sqlx::query_as(&sql).fetch_all(pool).await
}

// ── Escalation heuristics & index summary (pure) ──────────────────────

/// Whether the escalation heuristics consider exact probing of this table
/// cheap enough to run without hesitation.
pub fn is_cheap(profile: &TableProfile) -> bool {
    let small_rows = matches!(profile.estimated_rows, Some(r) if r < CHEAP_ROW_LIMIT);
    let small_heap = profile.heap_bytes < CHEAP_BYTE_LIMIT;
    small_rows || small_heap
}

/// Build the [`IndexSummary`] for a table from the schema model alone.
pub fn summarize_indexes(table: &Table) -> IndexSummary {
    use crate::schema::Constraint;

    let has_pk = table.primary_key().is_some();
    let unique_count = table
        .constraints
        .iter()
        .filter(|c| matches!(c, Constraint::Unique { .. }))
        .count();
    let secondary: Vec<IndexMethod> = table.indexes.iter().map(|i| i.method.clone()).collect();

    let unindexed_fks = table
        .foreign_keys()
        .iter()
        .filter_map(|c| match c {
            Constraint::ForeignKey { columns, .. } if !fk_is_covered(table, columns) => {
                Some(columns.join(", "))
            }
            _ => None,
        })
        .collect();

    IndexSummary {
        has_pk,
        unique_count,
        secondary,
        unindexed_fks,
    }
}

/// Whether the given FK columns are covered by an index whose leading columns
/// match — a PK, a unique constraint, or a secondary index.
fn fk_is_covered(table: &Table, fk_columns: &[String]) -> bool {
    use crate::schema::Constraint;

    let starts_with =
        |cols: &[String]| cols.len() >= fk_columns.len() && cols[..fk_columns.len()] == *fk_columns;

    let constraint_covers = table.constraints.iter().any(|c| match c {
        Constraint::PrimaryKey { columns, .. } | Constraint::Unique { columns, .. } => {
            starts_with(columns)
        }
        _ => false,
    });
    let index_covers = table.indexes.iter().any(|i| starts_with(&i.columns));
    constraint_covers || index_covers
}

// ── Helpers ───────────────────────────────────────────────────────────

/// Quote a SQL identifier to prevent injection.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Format a byte size into a human-readable string.
fn format_size(bytes: i64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.1} GB", b / GB)
    } else if b >= MB {
        format!("{:.1} MB", b / MB)
    } else if b >= KB {
        format!("{:.1} KB", b / KB)
    } else {
        format!("{bytes} B")
    }
}

/// Format an exact integer with thousands separators.
fn format_number(n: i64) -> String {
    let neg = n < 0;
    let s = n.unsigned_abs().to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3 + 1);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    if neg {
        out.push('-');
    }
    out.chars().rev().collect()
}

/// Format an *estimated* count compactly: `942`, `12k`, `1.4M`, `3.1B`.
fn format_approx(n: i64) -> String {
    let a = n.unsigned_abs();
    if a < 1_000 {
        n.to_string()
    } else if a < 1_000_000 {
        format!("{}k", (n as f64 / 1_000.0).round() as i64)
    } else if a < 1_000_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    }
}

/// Render a `0.0..=1.0` fraction as a whole-number percent.
fn format_pct(frac: f64) -> String {
    format!("{}%", (frac * 100.0).round() as i64)
}

// ── Rendering ─────────────────────────────────────────────────────────

const HUD_BORDER_STYLE: Style = Style::new().fg(Color::Magenta);
const HUD_LABEL_STYLE: Style = Style::new().fg(Color::DarkGray);
const HUD_VALUE_STYLE: Style = Style::new().fg(Color::White);
const HUD_TAG_STYLE: Style = Style::new().fg(Color::Cyan);
const HUD_WARN_STYLE: Style = Style::new().fg(Color::Yellow);
const HUD_ERROR_STYLE: Style = Style::new().fg(Color::Red);
const HUD_DIM_STYLE: Style = Style::new().fg(Color::DarkGray);

/// Render the HUD overlay on top of the existing frame.
pub fn render_hud(frame: &mut Frame, area: Rect, hud: &HudState) {
    let title = hud_title(&hud.target);
    let lines = match &hud.status {
        HudStatus::Loading => vec![dim_line("  loading…")],
        HudStatus::Analyzing => vec![dim_line("  running ANALYZE — collecting statistics…")],
        HudStatus::Table(t) => table_hud_lines(t),
        HudStatus::Column(c) => column_hud_lines(c),
        HudStatus::Error(msg) => vec![Line::from(Span::styled(
            format!("  error: {msg}"),
            HUD_ERROR_STYLE,
        ))],
    };

    let content_height = lines.len() as u16 + 2;
    let content_width = lines
        .iter()
        .map(|l| l.spans.iter().map(|s| s.width()).sum::<usize>())
        .max()
        .unwrap_or(20) as u16
        + 4;

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
    frame.render_widget(Paragraph::new(lines), inner);
}

fn hud_title(target: &HudTarget) -> String {
    match target {
        HudTarget::Table { name } => format!("HUD: {name}"),
        HudTarget::Column { table, column, .. } => format!("HUD: {table}.{column}"),
    }
}

// ── Table rendering ───────────────────────────────────────────────────

/// Build the rendered lines for a table HUD. Pure — also used by snapshot tests.
pub fn table_hud_lines(hud: &TableHud) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    // Role badge.
    if let Some(role) = hud.role {
        lines.push(tag_line(&[role.to_string()]));
    }

    // Rows.
    if hud.profile.is_empty() {
        lines.push(fact("rows", "empty"));
    } else {
        match hud.profile.estimated_rows {
            Some(rows) => lines.push(fact("rows", &format!("~{}", format_approx(rows)))),
            None => lines.push(warn_fact("rows", "unknown — never analyzed")),
        }
    }

    // Size.
    lines.push(fact(
        "size",
        &format!(
            "{} heap · {} indexes",
            format_size(hud.profile.heap_bytes),
            format_size(hud.profile.index_bytes),
        ),
    ));

    // Index summary.
    lines.push(fact("indexes", &render_index_summary(&hud.indexes)));
    for fk in &hud.indexes.unindexed_fks {
        lines.push(warn_line(&format!("FK without index: {fk}")));
    }

    // Stale-stats note (decision Q3 — surfacing it is itself a signal).
    if let Some(line) = analyze_age_line(&hud.profile) {
        lines.push(line);
    }
    // Never analyzed — offer to collect statistics, unless the table is
    // empty (ANALYZE cannot produce statistics from zero rows).
    if hud.profile.analyze_age_days.is_none() && !hud.profile.is_empty() {
        lines.push(analyze_hint());
    }

    escalation_lines(&hud.escalation, &mut lines, escalation_table_summary);
    lines
}

fn render_index_summary(s: &IndexSummary) -> String {
    let mut parts: Vec<String> = Vec::new();
    if s.has_pk {
        parts.push("PK".into());
    }
    if s.unique_count > 0 {
        parts.push(format!("{} UNIQUE", s.unique_count));
    }
    if s.secondary.len() > HEAVY_INDEX_COUNT {
        parts.push(format!("heavily indexed ({})", s.secondary.len()));
    } else {
        // Group secondary indexes by method, preserving first-seen order.
        let mut groups: Vec<(String, usize)> = Vec::new();
        for m in &s.secondary {
            let name = m.to_string();
            match groups.iter_mut().find(|(g, _)| *g == name) {
                Some((_, count)) => *count += 1,
                None => groups.push((name, 1)),
            }
        }
        for (name, count) in groups {
            parts.push(if count > 1 {
                format!("{count}×{name}")
            } else {
                name
            });
        }
    }
    if parts.is_empty() {
        "none".into()
    } else {
        parts.join(" · ")
    }
}

fn escalation_table_summary(probe: &ExactProbe) -> Vec<Line<'static>> {
    vec![fact("exact rows", &format_number(probe.exact_rows))]
}

// ── Column rendering ──────────────────────────────────────────────────

/// Build the rendered lines for a column HUD. Pure — also used by snapshot tests.
pub fn column_hud_lines(hud: &ColumnHud) -> Vec<Line<'static>> {
    let mut lines = Vec::new();

    // Header: type plus interpretive tags.
    let mut header = vec![Span::styled(format!("  {}", hud.pg_type), HUD_VALUE_STYLE)];
    let tag_text: Vec<String> = hud
        .insights
        .tags
        .iter()
        .map(|t| tag_label(*t).into())
        .collect();
    if !tag_text.is_empty() {
        header.push(Span::styled("  ·  ", HUD_DIM_STYLE));
        header.push(Span::styled(tag_text.join(", "), HUD_TAG_STYLE));
    }
    lines.push(Line::from(header));

    // No statistics at all — make that explicit (decision Q3).
    if !hud.profile.analyzed {
        if hud.table_is_empty {
            // The table is empty — ANALYZE cannot produce statistics from
            // zero rows, so say that plainly rather than offering `a`.
            lines.push(warn_line("table is empty — no rows to profile"));
        } else {
            lines.push(warn_line("no statistics — column not analyzed"));
            lines.push(analyze_hint());
        }
        // FK target is structural and still worth showing.
        if let Some(target) = &hud.fk_target {
            lines.push(fact("→", target));
        }
        escalation_lines(&hud.escalation, &mut lines, escalation_column_summary);
        return lines;
    }

    // Universal: sparsity / fill.
    if let Some(sparsity) = hud.insights.sparsity {
        lines.push(sparsity_line(sparsity, hud.profile.null_frac));
    }

    // Type-dispatched body.
    match hud.type_class {
        TypeClass::Text => render_text_column(hud, &mut lines),
        TypeClass::Boolean => render_boolean_column(hud, &mut lines),
        TypeClass::Integer | TypeClass::Numeric => render_numeric_column(hud, &mut lines),
        TypeClass::Temporal => render_temporal_column(hud, &mut lines),
        TypeClass::Uuid => render_uuid_column(hud, &mut lines),
        TypeClass::Json => render_json_column(hud, &mut lines),
        TypeClass::Array => render_array_column(hud, &mut lines),
        TypeClass::Other => render_other_column(hud, &mut lines),
    }

    // FK target line — high-value, shown for every FK column.
    if let Some(target) = &hud.fk_target {
        lines.push(fact("→", target));
        // An FK with no covering index is a major operational hazard.
        if hud.insights.tags.contains(&ColumnTag::FkUnindexed) {
            lines.push(warn_line("foreign key has no index"));
        }
    }

    // Ordering / append correlation.
    if let Some(ord) = hud.insights.ordering {
        if let Some(text) = ordering_text(ord) {
            lines.push(fact("ordering", text));
        }
    }

    escalation_lines(&hud.escalation, &mut lines, escalation_column_summary);
    lines
}

fn render_text_column(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    push_distinctness(hud, lines);
    if hud.profile.avg_width > 0 {
        lines.push(fact("avg", &format!("{} chars", hud.profile.avg_width)));
    }
    push_top_values(&hud.profile, lines);
}

fn render_boolean_column(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    // Boolean MCV are rendered by Postgres as `t` / `f`.
    let freq_of = |needle: &str| {
        hud.profile
            .mcv
            .iter()
            .position(|v| v == needle)
            .and_then(|i| hud.profile.mcv_freqs.get(i).copied())
    };
    match (freq_of("t"), freq_of("f")) {
        (Some(t), _) => lines.push(fact("TRUE", &format_pct(t))),
        (None, Some(f)) => lines.push(fact("FALSE", &format_pct(f))),
        (None, None) => {}
    }
}

fn render_numeric_column(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    if let Some((lo, hi)) = hud.profile.value_range() {
        lines.push(fact("range", &format!("{lo} → {hi}")));
    }
    if let Some(median) = hud.profile.approx_median() {
        lines.push(fact("p50", &format!("~{median}")));
    }
    match insight::numeric_skew(&hud.profile) {
        Some(Skew::RightSkewed) => lines.push(fact("shape", "right-skewed")),
        Some(Skew::LeftSkewed) => lines.push(fact("shape", "left-skewed")),
        _ => {}
    }
    push_distinctness(hud, lines);
}

fn render_temporal_column(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    if let Some((lo, hi)) = hud.profile.value_range() {
        lines.push(fact("range", &format!("{lo} → {hi}")));
    }
    push_distinctness(hud, lines);
}

fn render_uuid_column(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    push_distinctness(hud, lines);
}

fn render_json_column(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    if hud.profile.avg_width > 0 {
        lines.push(fact("avg size", &format_size(hud.profile.avg_width as i64)));
    }
    let gin = hud
        .covering_indexes
        .iter()
        .any(|(_, m)| matches!(m, IndexMethod::Gin));
    if gin {
        lines.push(fact("index", "GIN"));
    }
}

fn render_array_column(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    if hud.profile.avg_width > 0 {
        lines.push(fact("avg size", &format_size(hud.profile.avg_width as i64)));
    }
    push_distinctness(hud, lines);
}

fn render_other_column(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    // Enums are the high-value case here: list the variants.
    if let Some(variants) = &hud.enum_variants {
        lines.push(fact("enum", &format!("{} values", variants.len())));
        if !variants.is_empty() {
            lines.push(Line::from(Span::styled(
                format!("  {}", variants.join(" · ")),
                HUD_TAG_STYLE,
            )));
        }
    } else {
        push_distinctness(hud, lines);
    }
}

fn push_distinctness(hud: &ColumnHud, lines: &mut Vec<Line<'static>>) {
    let Some(distinctness) = hud.insights.distinctness else {
        return;
    };
    let abs = hud.profile.distinct_estimate(hud.estimated_rows);
    let value = match (distinctness, abs) {
        (Distinctness::Constant, _) => "≈1 (constant)".to_string(),
        (_, Some(a)) => format!(
            "~{} ({})",
            format_approx(a as i64),
            distinctness_word(distinctness)
        ),
        (_, None) => distinctness_word(distinctness).to_string(),
    };
    lines.push(fact("distinct", &value));
}

fn push_top_values(profile: &ColumnProfile, lines: &mut Vec<Line<'static>>) {
    if profile.mcv.is_empty() {
        return;
    }
    let parts: Vec<String> = profile
        .mcv
        .iter()
        .take(3)
        .enumerate()
        .map(|(i, v)| match profile.mcv_freqs.get(i) {
            Some(&f) => format!("{v} {}", format_pct(f)),
            None => v.clone(),
        })
        .collect();
    lines.push(fact("top", &parts.join(", ")));
}

fn distinctness_word(d: Distinctness) -> &'static str {
    match d {
        Distinctness::Constant => "constant",
        Distinctness::Categorical => "categorical",
        Distinctness::Grouped => "distinct",
        Distinctness::Unique => "≈unique",
    }
}

fn sparsity_line(s: Sparsity, null_frac: f64) -> Line<'static> {
    match s {
        Sparsity::Full => fact("filled", "100%"),
        Sparsity::MostlyFull => fact("filled", &format_pct(1.0 - null_frac)),
        Sparsity::Sparse => warn_fact(
            "filled",
            &format!("{} — mostly unused", format_pct(1.0 - null_frac)),
        ),
        Sparsity::Empty => warn_fact("filled", "0% — entirely null"),
    }
}

fn ordering_text(o: Ordering) -> Option<&'static str> {
    match o {
        Ordering::Clustered => Some("insertion-ordered (BRIN-friendly)"),
        Ordering::Loose => Some("partially ordered"),
        Ordering::Scattered => None,
    }
}

fn tag_label(t: ColumnTag) -> &'static str {
    match t {
        ColumnTag::EnumIsh => "enum-ish",
        ColumnTag::Freeform => "freeform",
        ColumnTag::MostlyUnique => "mostly unique",
        ColumnTag::LikelyIdentifier => "likely identifier",
        ColumnTag::MonotonicId => "monotonic id",
        ColumnTag::AppendCorrelated => "append-correlated",
        ColumnTag::FkWellIndexed => "FK · indexed",
        ColumnTag::FkUnindexed => "FK · UNINDEXED",
    }
}

// ── Shared rendering helpers ──────────────────────────────────────────

fn escalation_lines(
    escalation: &Escalation,
    lines: &mut Vec<Line<'static>>,
    on_done: impl Fn(&ExactProbe) -> Vec<Line<'static>>,
) {
    match escalation {
        Escalation::Unavailable => {}
        Escalation::Offered { cheap } => {
            lines.push(divider());
            let hint = if *cheap {
                "  p  profile — exact counts".to_string()
            } else {
                "  p  profile — exact counts (may scan a large table)".to_string()
            };
            lines.push(Line::from(Span::styled(hint, HUD_DIM_STYLE)));
        }
        Escalation::Running => {
            lines.push(divider());
            lines.push(dim_line("  profiling…"));
        }
        Escalation::Done(probe) => {
            lines.push(divider());
            lines.extend(on_done(probe));
        }
        Escalation::Failed(e) => {
            lines.push(divider());
            lines.push(Line::from(Span::styled(
                format!("  profile failed: {e}"),
                HUD_ERROR_STYLE,
            )));
        }
    }
}

fn escalation_column_summary(probe: &ExactProbe) -> Vec<Line<'static>> {
    let mut lines = vec![fact("exact rows", &format_number(probe.exact_rows))];
    if let Some(distinct) = probe.exact_distinct {
        lines.push(fact("exact distinct", &format_number(distinct)));
    }
    if !probe.jsonb_keys.is_empty() {
        lines.push(fact(
            "keys",
            &format!("sampled {} rows", probe.jsonb_sample_rows),
        ));
        let keys: Vec<String> = probe
            .jsonb_keys
            .iter()
            .take(8)
            .map(|(k, _)| k.clone())
            .collect();
        lines.push(Line::from(Span::styled(
            format!("  {}", keys.join(" · ")),
            HUD_TAG_STYLE,
        )));
    }
    lines
}

fn analyze_age_line(profile: &TableProfile) -> Option<Line<'static>> {
    match profile.analyze_age_days {
        Some(days) if profile.stats_are_stale() => Some(warn_fact(
            "analyzed",
            &format!("{days}d ago — stats may be stale"),
        )),
        Some(days) => Some(Line::from(vec![
            Span::styled("  analyzed: ", HUD_LABEL_STYLE),
            Span::styled(format!("{days}d ago"), HUD_DIM_STYLE),
        ])),
        None => Some(warn_fact("analyzed", "never")),
    }
}

/// A `label: value` fact line.
fn fact(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {label}: "), HUD_LABEL_STYLE),
        Span::styled(value.to_string(), HUD_VALUE_STYLE),
    ])
}

/// A `label: value` fact line styled as a warning.
fn warn_fact(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("  {label}: "), HUD_LABEL_STYLE),
        Span::styled(value.to_string(), HUD_WARN_STYLE),
    ])
}

/// A standalone warning line.
fn warn_line(text: &str) -> Line<'static> {
    Line::from(Span::styled(format!("  ⚠ {text}"), HUD_WARN_STYLE))
}

/// Hint line offering the `a` key to run `ANALYZE`. Shown only when the
/// target has no statistics.
fn analyze_hint() -> Line<'static> {
    Line::from(Span::styled(
        "  a  analyze — collect statistics".to_string(),
        HUD_DIM_STYLE,
    ))
}

fn dim_line(text: &str) -> Line<'static> {
    Line::from(Span::styled(text.to_string(), HUD_DIM_STYLE))
}

fn tag_line(tags: &[String]) -> Line<'static> {
    Line::from(Span::styled(
        format!("  {}", tags.join(", ")),
        HUD_TAG_STYLE,
    ))
}

fn divider() -> Line<'static> {
    Line::from(Span::styled("  ─────", HUD_DIM_STYLE))
}

/// Calculate a centered rectangle within the given area.
fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::insight::ColumnContext;
    use crate::schema::{Column, Constraint, Index};

    /// Flatten rendered lines to plain text, one line per row.
    fn lines_to_text(lines: &[Line<'static>]) -> String {
        lines
            .iter()
            .map(|l| {
                l.spans
                    .iter()
                    .map(|s| s.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn analyzed(n_distinct: f64) -> ColumnProfile {
        ColumnProfile {
            n_distinct,
            analyzed: true,
            ..ColumnProfile::unavailable()
        }
    }

    fn column_hud(pg_type: PgType, profile: ColumnProfile) -> ColumnHud {
        let type_class = insight::classify_type(&pg_type);
        let insights = insight::derive_column_insights(
            &profile,
            &pg_type,
            Some(1_000_000),
            ColumnContext::default(),
        );
        ColumnHud {
            profile,
            estimated_rows: Some(1_000_000),
            table_is_empty: false,
            pg_type,
            type_class,
            insights,
            enum_variants: None,
            fk_target: None,
            covering_indexes: Vec::new(),
            escalation: Escalation::Offered { cheap: true },
        }
    }

    // ── pure helpers ──

    #[test]
    fn format_size_units() {
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(2048), "2.0 KB");
        assert_eq!(format_size(10 * 1024 * 1024), "10.0 MB");
        assert_eq!(format_size(3 * 1024 * 1024 * 1024), "3.0 GB");
    }

    #[test]
    fn format_number_separators() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1_234_567), "1,234,567");
        assert_eq!(format_number(-4200), "-4,200");
    }

    #[test]
    fn format_approx_compacts() {
        assert_eq!(format_approx(942), "942");
        assert_eq!(format_approx(12_000), "12k");
        assert_eq!(format_approx(1_400_000), "1.4M");
        assert_eq!(format_approx(3_100_000_000), "3.1B");
    }

    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn is_cheap_by_rows_or_size() {
        let mut p = TableProfile {
            estimated_rows: Some(10_000),
            heap_bytes: 1 << 30, // 1 GiB
            index_bytes: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
            analyze_age_days: Some(0),
        };
        assert!(is_cheap(&p)); // cheap by row count
        p.estimated_rows = Some(10_000_000);
        assert!(!is_cheap(&p)); // huge rows + huge heap
        p.heap_bytes = 1024;
        assert!(is_cheap(&p)); // cheap by heap size
    }

    // ── index summary ──

    fn fk_table() -> Table {
        let mut t = Table::new("posts");
        t.add_column(Column::new("id", PgType::Uuid));
        t.add_column(Column::new("author_id", PgType::Uuid));
        t.add_constraint(Constraint::PrimaryKey {
            name: None,
            columns: vec!["id".into()],
        });
        t.add_constraint(Constraint::ForeignKey {
            name: None,
            columns: vec!["author_id".into()],
            references: crate::schema::types::ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: None,
            on_update: None,
        });
        t
    }

    #[test]
    fn summarize_indexes_flags_unindexed_fk() {
        let summary = summarize_indexes(&fk_table());
        assert!(summary.has_pk);
        assert_eq!(summary.unindexed_fks, vec!["author_id".to_string()]);
    }

    #[test]
    fn summarize_indexes_clears_warning_when_fk_indexed() {
        let mut t = fk_table();
        t.add_index(Index {
            name: "posts_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: false,
            partial: None,
            method: IndexMethod::Btree,
        });
        let summary = summarize_indexes(&t);
        assert!(summary.unindexed_fks.is_empty());
    }

    #[test]
    fn render_index_summary_groups_methods() {
        let summary = IndexSummary {
            has_pk: true,
            unique_count: 2,
            secondary: vec![IndexMethod::Gin, IndexMethod::Btree, IndexMethod::Btree],
            unindexed_fks: vec![],
        };
        assert_eq!(
            render_index_summary(&summary),
            "PK · 2 UNIQUE · gin · 2×btree"
        );
    }

    #[test]
    fn render_index_summary_heavy() {
        let summary = IndexSummary {
            has_pk: true,
            unique_count: 0,
            secondary: vec![IndexMethod::Btree; 9],
            unindexed_fks: vec![],
        };
        assert_eq!(render_index_summary(&summary), "PK · heavily indexed (9)");
    }

    // ── rendering snapshots ──

    #[test]
    fn snapshot_text_enum_column() {
        let mut p = analyzed(4.0);
        p.mcv = vec!["pending".into(), "active".into(), "disabled".into()];
        p.mcv_freqs = vec![0.51, 0.33, 0.16];
        p.avg_width = 8;
        let hud = column_hud(PgType::Text, p);
        insta::assert_snapshot!(lines_to_text(&column_hud_lines(&hud)));
    }

    #[test]
    fn snapshot_numeric_column() {
        let mut p = analyzed(-1.0);
        p.histogram = vec!["0".into(), "42".into(), "18222".into()];
        p.correlation = Some(0.99);
        let hud = column_hud(PgType::BigInt, p);
        insta::assert_snapshot!(lines_to_text(&column_hud_lines(&hud)));
    }

    #[test]
    fn snapshot_boolean_column() {
        let mut p = analyzed(2.0);
        p.mcv = vec!["t".into(), "f".into()];
        p.mcv_freqs = vec![0.987, 0.013];
        let hud = column_hud(PgType::Boolean, p);
        insta::assert_snapshot!(lines_to_text(&column_hud_lines(&hud)));
    }

    #[test]
    fn snapshot_unanalyzed_column() {
        let hud = column_hud(PgType::Text, ColumnProfile::unavailable());
        insta::assert_snapshot!(lines_to_text(&column_hud_lines(&hud)));
    }

    #[test]
    fn snapshot_unindexed_fk_column() {
        let mut hud = column_hud(PgType::Uuid, analyzed(-1.0));
        hud.fk_target = Some("users.id".into());
        hud.insights = insight::derive_column_insights(
            &hud.profile,
            &hud.pg_type,
            hud.estimated_rows,
            ColumnContext {
                is_foreign_key: true,
                is_indexed: false,
                ..ColumnContext::default()
            },
        );
        insta::assert_snapshot!(lines_to_text(&column_hud_lines(&hud)));
    }

    #[test]
    fn snapshot_enum_column() {
        let mut hud = column_hud(PgType::Custom("job_status".into()), analyzed(5.0));
        hud.enum_variants = Some(vec![
            "draft".into(),
            "queued".into(),
            "running".into(),
            "failed".into(),
            "complete".into(),
        ]);
        insta::assert_snapshot!(lines_to_text(&column_hud_lines(&hud)));
    }

    #[test]
    fn snapshot_table_hud() {
        let hud = TableHud {
            profile: TableProfile {
                estimated_rows: Some(82_000_000),
                heap_bytes: 14 * 1024 * 1024 * 1024,
                index_bytes: 9 * 1024 * 1024 * 1024,
                inserts: 82_000_000,
                updates: 10,
                deletes: 0,
                analyze_age_days: Some(2),
            },
            role: Some(TableRole::EventLog),
            indexes: IndexSummary {
                has_pk: true,
                unique_count: 1,
                secondary: vec![IndexMethod::Gin],
                unindexed_fks: vec![],
            },
            escalation: Escalation::Offered { cheap: false },
        };
        insta::assert_snapshot!(lines_to_text(&table_hud_lines(&hud)));
    }

    #[test]
    fn snapshot_table_hud_escalated() {
        let hud = TableHud {
            profile: TableProfile {
                estimated_rows: Some(940),
                heap_bytes: 64 * 1024,
                index_bytes: 16 * 1024,
                inserts: 940,
                updates: 0,
                deletes: 0,
                analyze_age_days: Some(0),
            },
            role: Some(TableRole::LookupTable),
            indexes: IndexSummary {
                has_pk: true,
                unique_count: 0,
                secondary: vec![],
                unindexed_fks: vec![],
            },
            escalation: Escalation::Done(ExactProbe {
                exact_rows: 942,
                ..ExactProbe::default()
            }),
        };
        insta::assert_snapshot!(lines_to_text(&table_hud_lines(&hud)));
    }

    #[test]
    fn unanalyzed_column_has_no_type_dispatch_body() {
        let hud = column_hud(PgType::Integer, ColumnProfile::unavailable());
        let text = lines_to_text(&column_hud_lines(&hud));
        assert!(text.contains("not analyzed"));
        assert!(!text.contains("range:"));
    }

    fn table_hud(analyze_age_days: Option<i64>, heap_bytes: i64) -> TableHud {
        TableHud {
            profile: TableProfile {
                estimated_rows: analyze_age_days.map(|_| 10),
                heap_bytes,
                index_bytes: 0,
                inserts: 0,
                updates: 0,
                deletes: 0,
                analyze_age_days,
            },
            role: None,
            indexes: IndexSummary::default(),
            escalation: Escalation::Offered { cheap: true },
        }
    }

    #[test]
    fn unanalyzed_column_offers_analyze_hint() {
        let hud = column_hud(PgType::Integer, ColumnProfile::unavailable());
        assert!(lines_to_text(&column_hud_lines(&hud)).contains("a  analyze"));
    }

    #[test]
    fn analyzed_column_omits_analyze_hint() {
        let hud = column_hud(PgType::Integer, analyzed(-1.0));
        assert!(!lines_to_text(&column_hud_lines(&hud)).contains("a  analyze"));
    }

    #[test]
    fn never_analyzed_nonempty_table_offers_analyze_hint() {
        let hud = table_hud(None, 8 * 1024);
        assert!(lines_to_text(&table_hud_lines(&hud)).contains("a  analyze"));
    }

    #[test]
    fn analyzed_table_omits_analyze_hint() {
        let hud = table_hud(Some(2), 8 * 1024);
        assert!(!lines_to_text(&table_hud_lines(&hud)).contains("a  analyze"));
    }

    #[test]
    fn empty_table_column_shows_empty_not_unanalyzed() {
        let mut hud = column_hud(PgType::Integer, ColumnProfile::unavailable());
        hud.table_is_empty = true;
        let text = lines_to_text(&column_hud_lines(&hud));
        assert!(text.contains("table is empty"));
        assert!(!text.contains("not analyzed"));
        assert!(!text.contains("a  analyze"));
    }

    #[test]
    fn zero_heap_table_is_empty_without_analyze() {
        // heap_bytes == 0 marks a never-written table as empty with no query.
        let hud = table_hud(None, 0);
        let text = lines_to_text(&table_hud_lines(&hud));
        assert!(text.contains("rows: empty"));
        assert!(!text.contains("a  analyze"));
    }

    #[test]
    fn type_class_dispatch_is_exhaustive() {
        // Every TypeClass renders without panicking.
        for ty in [
            PgType::Text,
            PgType::Boolean,
            PgType::Integer,
            PgType::Numeric(None),
            PgType::Timestamptz,
            PgType::Uuid,
            PgType::Jsonb,
            PgType::Array(Box::new(PgType::Text)),
            PgType::Bytea,
        ] {
            let hud = column_hud(ty.clone(), analyzed(10.0));
            let _ = column_hud_lines(&hud);
            assert_eq!(hud.type_class, insight::classify_type(&ty));
        }
    }
}
