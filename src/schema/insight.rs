//! Interpretation of raw [`profile`](crate::schema::profile) measurements into
//! human-meaningful signals — the *interpretation* half of the two-layer HUD
//! model.
//!
//! Every function here is pure: no IO, no database, fully unit-testable. The
//! cognitively useful logic the HUD is built around — distinctness buckets,
//! sparsity, correlation-to-ordering, table-role scoring — all lives here so
//! it can be exercised without a Postgres connection.

use super::profile::{ColumnProfile, TableProfile};
use super::types::PgType;
use super::{Constraint, Table};

// ── Type dispatch ─────────────────────────────────────────────────────

/// A coarse grouping of [`PgType`] that drives both insight derivation and
/// per-type HUD rendering. One source of truth for "what kind of column is
/// this".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeClass {
    /// `text`, `varchar`, `char` (and text-like domains).
    Text,
    /// `boolean`.
    Boolean,
    /// Integer types — `smallint`, `integer`, `bigint`.
    Integer,
    /// Non-integer numbers — `real`, `double precision`, `numeric`.
    Numeric,
    /// Date/time types.
    Temporal,
    /// `uuid`.
    Uuid,
    /// `json` / `jsonb`.
    Json,
    /// Array types.
    Array,
    /// Anything else (`bytea`, `interval`, enums, composites, …).
    Other,
}

/// Classify a [`PgType`] into a [`TypeClass`].
///
/// Note that enum columns are `PgType::Custom` and classify as [`TypeClass::Other`];
/// the caller distinguishes a true enum by consulting the schema's enum table.
pub fn classify_type(ty: &PgType) -> TypeClass {
    match ty {
        PgType::Text | PgType::Varchar(_) | PgType::Char(_) => TypeClass::Text,
        PgType::Boolean => TypeClass::Boolean,
        PgType::SmallInt | PgType::Integer | PgType::BigInt => TypeClass::Integer,
        PgType::Real | PgType::DoublePrecision | PgType::Numeric(_) => TypeClass::Numeric,
        PgType::Timestamp | PgType::Timestamptz | PgType::Date | PgType::Time | PgType::Timetz => {
            TypeClass::Temporal
        }
        PgType::Uuid => TypeClass::Uuid,
        PgType::Json | PgType::Jsonb => TypeClass::Json,
        PgType::Array(_) => TypeClass::Array,
        PgType::Bytea | PgType::Interval | PgType::Custom(_) => TypeClass::Other,
    }
}

// ── Distinctness ──────────────────────────────────────────────────────

/// Cardinality classification — the design's "distinctness classification".
///
/// Far more cognitively useful than a raw distinct count: it answers "what
/// shape is this column" in one word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distinctness {
    /// Effectively one value across the whole table — constant-ish.
    Constant,
    /// A small fixed set (`< 20`) — enum-like / categorical.
    Categorical,
    /// Many distinct values, but well short of one per row — grouped.
    Grouped,
    /// Distinct count ≈ row count — identifier-like.
    Unique,
}

/// Below this absolute distinct count a column reads as a small fixed set.
const CATEGORICAL_MAX: f64 = 20.0;
/// At or above this distinct-to-rowcount ratio a column reads as unique.
const UNIQUE_RATIO: f64 = 0.95;

/// Classify a column's cardinality from its profile.
///
/// Returns `None` when there is no distinctness signal at all (column
/// unanalyzed, or `n_distinct == 0`).
pub fn classify_distinctness(
    profile: &ColumnProfile,
    estimated_rows: Option<i64>,
) -> Option<Distinctness> {
    let abs = profile.distinct_estimate(estimated_rows);
    let ratio = profile.distinct_ratio(estimated_rows);

    if let Some(a) = abs {
        if a <= 1.5 {
            return Some(Distinctness::Constant);
        }
        if a < CATEGORICAL_MAX {
            return Some(Distinctness::Categorical);
        }
    }
    if let Some(r) = ratio {
        if r >= UNIQUE_RATIO {
            return Some(Distinctness::Unique);
        }
    }
    if abs.is_some() || ratio.is_some() {
        return Some(Distinctness::Grouped);
    }
    None
}

// ── Sparsity ──────────────────────────────────────────────────────────

/// Null-density classification — the design's "sparsity" signal, from
/// `null_frac`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sparsity {
    /// No NULLs at all.
    Full,
    /// A minority of rows are NULL.
    MostlyFull,
    /// A majority of rows are NULL — "mostly unused".
    Sparse,
    /// Every row is NULL.
    Empty,
}

/// Classify null density. Pure over `null_frac` alone.
pub fn classify_sparsity(null_frac: f64) -> Sparsity {
    if null_frac >= 0.9995 {
        Sparsity::Empty
    } else if null_frac >= 0.6 {
        Sparsity::Sparse
    } else if null_frac > 0.0 {
        Sparsity::MostlyFull
    } else {
        Sparsity::Full
    }
}

// ── Ordering ──────────────────────────────────────────────────────────

/// Physical-ordering classification, from `pg_stats.correlation`.
///
/// This is, per the design, a "criminally underused" signal: it reveals
/// insertion order, locality, and BRIN-friendliness essentially for free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    /// `|correlation| ≈ 1` — rows are physically stored in column order.
    /// Strong insertion ordering; BRIN-friendly; append-correlated.
    Clustered,
    /// Moderate correlation — some locality.
    Loose,
    /// `correlation ≈ 0` — no relationship between physical and logical order.
    Scattered,
}

/// Classify physical ordering. `None` when Postgres reports no correlation.
pub fn classify_ordering(correlation: Option<f64>) -> Option<Ordering> {
    let c = correlation?.abs();
    Some(if c >= 0.95 {
        Ordering::Clustered
    } else if c >= 0.5 {
        Ordering::Loose
    } else {
        Ordering::Scattered
    })
}

// ── Numeric skew ──────────────────────────────────────────────────────

/// Distribution shape inferred from histogram bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Skew {
    /// Median sits roughly mid-range.
    Symmetric,
    /// Long tail toward larger values.
    RightSkewed,
    /// Long tail toward smaller values.
    LeftSkewed,
}

/// Infer distribution shape by comparing the histogram median to the range
/// midpoint. Pure; works only when the histogram parses as numbers.
pub fn numeric_skew(profile: &ColumnProfile) -> Option<Skew> {
    let (lo, hi) = profile.value_range()?;
    let median = profile.approx_median()?;
    let lo: f64 = lo.trim().parse().ok()?;
    let hi: f64 = hi.trim().parse().ok()?;
    let median: f64 = median.trim().parse().ok()?;
    let span = hi - lo;
    if span <= 0.0 {
        return Some(Skew::Symmetric);
    }
    // Where the median falls within the range, 0.0 (=min) .. 1.0 (=max).
    let position = (median - lo) / span;
    Some(if position < 0.35 {
        // Median bunched near the low end → long right tail.
        Skew::RightSkewed
    } else if position > 0.65 {
        Skew::LeftSkewed
    } else {
        Skew::Symmetric
    })
}

// ── Column insights ───────────────────────────────────────────────────

/// Relational facts about a column the caller supplies from the schema model
/// and `RelationMap`. Keeping these as plain inputs leaves `insight.rs` free
/// of any dependency on relation-graph machinery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColumnContext {
    /// The column participates in its table's primary key.
    pub is_primary_key: bool,
    /// The column is the source side of a foreign key.
    pub is_foreign_key: bool,
    /// The column is covered by at least one index.
    pub is_indexed: bool,
}

/// A short interpretive badge for a column. The HUD renders a handful of
/// these alongside the raw figures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnTag {
    /// A small fixed value set — behaves like an enum.
    EnumIsh,
    /// High-cardinality free text.
    Freeform,
    /// Distinct count ≈ row count.
    MostlyUnique,
    /// Looks like a row identifier.
    LikelyIdentifier,
    /// Monotonically increasing integer id (unique + strongly clustered).
    MonotonicId,
    /// Append-correlated — physically ordered, BRIN-friendly.
    AppendCorrelated,
    /// Foreign key backed by a covering index.
    FkWellIndexed,
    /// Foreign key with **no** covering index — a real operational hazard.
    FkUnindexed,
}

impl ColumnTag {
    /// Whether this tag is a warning the HUD should style as such.
    pub fn is_warning(&self) -> bool {
        matches!(self, ColumnTag::FkUnindexed)
    }
}

/// The fully interpreted picture of a column: classifications plus badges.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInsights {
    /// Cardinality bucket, when a signal exists.
    pub distinctness: Option<Distinctness>,
    /// Null density, when the column has been analyzed.
    pub sparsity: Option<Sparsity>,
    /// Physical ordering, when correlation is reported.
    pub ordering: Option<Ordering>,
    /// Interpretive badges, ordered most- to least-salient.
    pub tags: Vec<ColumnTag>,
}

/// Derive every insight for a column from its profile, type, and relational
/// context. The single entry point the HUD calls.
pub fn derive_column_insights(
    profile: &ColumnProfile,
    pg_type: &PgType,
    estimated_rows: Option<i64>,
    ctx: ColumnContext,
) -> ColumnInsights {
    let class = classify_type(pg_type);
    let distinctness = classify_distinctness(profile, estimated_rows);
    let sparsity = profile
        .analyzed
        .then(|| classify_sparsity(profile.null_frac));
    let ordering = classify_ordering(profile.correlation);

    let mut tags = Vec::new();

    // Foreign-key indexing is the highest-value signal — surface it first.
    if ctx.is_foreign_key {
        tags.push(if ctx.is_indexed {
            ColumnTag::FkWellIndexed
        } else {
            ColumnTag::FkUnindexed
        });
    }

    let unique = distinctness == Some(Distinctness::Unique);
    let clustered = ordering == Some(Ordering::Clustered);

    // Identifier-shaped: a primary key, or a unique indexed column.
    if ctx.is_primary_key || (unique && ctx.is_indexed) {
        tags.push(ColumnTag::LikelyIdentifier);
    }
    // A clustered, unique integer is a monotonically assigned id.
    if class == TypeClass::Integer && unique && clustered {
        tags.push(ColumnTag::MonotonicId);
    } else if unique {
        tags.push(ColumnTag::MostlyUnique);
    }
    // Categorical columns read as enum-ish (booleans are trivially 2-valued).
    if distinctness == Some(Distinctness::Categorical) && class != TypeClass::Boolean {
        tags.push(ColumnTag::EnumIsh);
    }
    // Grouped/unique text with no identifier role is free text.
    if class == TypeClass::Text
        && matches!(distinctness, Some(Distinctness::Grouped))
        && !ctx.is_primary_key
    {
        tags.push(ColumnTag::Freeform);
    }
    // Append correlation — only worth a badge when not already implied by a
    // MonotonicId tag.
    if clustered && !tags.contains(&ColumnTag::MonotonicId) {
        tags.push(ColumnTag::AppendCorrelated);
    }

    ColumnInsights {
        distinctness,
        sparsity,
        ordering,
        tags,
    }
}

// ── Table role ────────────────────────────────────────────────────────

/// Inferred high-level role of a table — the design's "table role inference".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableRole {
    /// Append-heavy event log: bigint PK, a creation timestamp, few updates.
    EventLog,
    /// Many-to-many join table: 2+ FKs, composite PK, narrow schema.
    JunctionTable,
    /// Small, low-churn reference table.
    LookupTable,
}

impl std::fmt::Display for TableRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TableRole::EventLog => "event log",
            TableRole::JunctionTable => "junction table",
            TableRole::LookupTable => "lookup table",
        };
        f.write_str(s)
    }
}

/// Column names that, on a temporal column, signal an insertion timestamp.
const CREATION_TIMESTAMP_NAMES: &[&str] = &[
    "created_at",
    "inserted_at",
    "recorded_at",
    "logged_at",
    "occurred_at",
    "event_time",
    "timestamp",
];

/// Infer a table's role, or `None` when no signal is strong enough.
///
/// Per design decision Q6, saying nothing is better than mislabeling — every
/// branch here demands a confident combination of signals before committing.
pub fn infer_table_role(profile: &TableProfile, table: &Table) -> Option<TableRole> {
    let column_count = table.columns.len();
    let fk_count = table.foreign_keys().len();
    let pk_columns = primary_key_column_count(table);

    // Junction table: most specific — check first.
    if fk_count >= 2 && pk_columns >= 2 && column_count <= 6 {
        return Some(TableRole::JunctionTable);
    }

    // Event log: bigint single-column PK + a creation timestamp + insert-heavy.
    if has_bigint_singleton_pk(table)
        && has_creation_timestamp(table)
        && profile.inserts > 0
        && profile.inserts >= profile.updates.saturating_mul(5)
    {
        return Some(TableRole::EventLog);
    }

    // Lookup table: small, narrow, and not a junction table.
    if let Some(rows) = profile.estimated_rows {
        if rows < 500 && column_count <= 8 && fk_count < 2 {
            return Some(TableRole::LookupTable);
        }
    }

    None
}

fn primary_key_column_count(table: &Table) -> usize {
    match table.primary_key() {
        Some(Constraint::PrimaryKey { columns, .. }) => columns.len(),
        _ => 0,
    }
}

fn has_bigint_singleton_pk(table: &Table) -> bool {
    let Some(Constraint::PrimaryKey { columns, .. }) = table.primary_key() else {
        return false;
    };
    let [only] = columns.as_slice() else {
        return false;
    };
    matches!(table.column(only).map(|c| &c.pg_type), Some(PgType::BigInt))
}

fn has_creation_timestamp(table: &Table) -> bool {
    table.columns.iter().any(|c| {
        matches!(classify_type(&c.pg_type), TypeClass::Temporal)
            && CREATION_TIMESTAMP_NAMES.contains(&c.name.to_lowercase().as_str())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::ForeignKeyRef;
    use crate::schema::Column;

    fn profile(analyzed: bool) -> ColumnProfile {
        ColumnProfile {
            analyzed,
            ..ColumnProfile::unavailable()
        }
    }

    // ── type classification ──

    #[test]
    fn classify_type_groups() {
        assert_eq!(classify_type(&PgType::Text), TypeClass::Text);
        assert_eq!(classify_type(&PgType::Varchar(Some(8))), TypeClass::Text);
        assert_eq!(classify_type(&PgType::Boolean), TypeClass::Boolean);
        assert_eq!(classify_type(&PgType::BigInt), TypeClass::Integer);
        assert_eq!(classify_type(&PgType::Numeric(None)), TypeClass::Numeric);
        assert_eq!(classify_type(&PgType::Timestamptz), TypeClass::Temporal);
        assert_eq!(classify_type(&PgType::Uuid), TypeClass::Uuid);
        assert_eq!(classify_type(&PgType::Jsonb), TypeClass::Json);
        assert_eq!(
            classify_type(&PgType::Array(Box::new(PgType::Text))),
            TypeClass::Array
        );
        assert_eq!(
            classify_type(&PgType::Custom("mood".into())),
            TypeClass::Other
        );
    }

    // ── distinctness ──

    #[test]
    fn distinctness_none_when_unanalyzed() {
        assert_eq!(classify_distinctness(&profile(false), Some(1000)), None);
    }

    #[test]
    fn distinctness_none_when_no_signal() {
        // analyzed but n_distinct == 0
        assert_eq!(classify_distinctness(&profile(true), Some(1000)), None);
    }

    #[test]
    fn distinctness_constant() {
        let mut p = profile(true);
        p.n_distinct = 1.0;
        assert_eq!(
            classify_distinctness(&p, Some(1_000_000)),
            Some(Distinctness::Constant)
        );
    }

    #[test]
    fn distinctness_categorical() {
        let mut p = profile(true);
        p.n_distinct = 5.0;
        assert_eq!(
            classify_distinctness(&p, Some(1_000_000)),
            Some(Distinctness::Categorical)
        );
    }

    #[test]
    fn distinctness_unique_from_ratio() {
        let mut p = profile(true);
        p.n_distinct = -1.0; // every row distinct
        assert_eq!(
            classify_distinctness(&p, Some(1_000_000)),
            Some(Distinctness::Unique)
        );
    }

    #[test]
    fn distinctness_grouped() {
        let mut p = profile(true);
        p.n_distinct = 12_000.0;
        assert_eq!(
            classify_distinctness(&p, Some(1_000_000)),
            Some(Distinctness::Grouped)
        );
    }

    // ── sparsity ──

    #[test]
    fn sparsity_buckets() {
        assert_eq!(classify_sparsity(0.0), Sparsity::Full);
        assert_eq!(classify_sparsity(0.02), Sparsity::MostlyFull);
        assert_eq!(classify_sparsity(0.75), Sparsity::Sparse);
        assert_eq!(classify_sparsity(1.0), Sparsity::Empty);
    }

    // ── ordering ──

    #[test]
    fn ordering_buckets() {
        assert_eq!(classify_ordering(None), None);
        assert_eq!(classify_ordering(Some(0.99)), Some(Ordering::Clustered));
        assert_eq!(classify_ordering(Some(-0.98)), Some(Ordering::Clustered));
        assert_eq!(classify_ordering(Some(0.7)), Some(Ordering::Loose));
        assert_eq!(classify_ordering(Some(0.1)), Some(Ordering::Scattered));
    }

    // ── skew ──

    #[test]
    fn skew_right_when_median_low() {
        let mut p = profile(true);
        p.histogram = vec!["0".into(), "5".into(), "10000".into()];
        assert_eq!(numeric_skew(&p), Some(Skew::RightSkewed));
    }

    #[test]
    fn skew_symmetric_when_median_centered() {
        let mut p = profile(true);
        p.histogram = vec!["0".into(), "50".into(), "100".into()];
        assert_eq!(numeric_skew(&p), Some(Skew::Symmetric));
    }

    #[test]
    fn skew_none_for_non_numeric_histogram() {
        let mut p = profile(true);
        p.histogram = vec!["apple".into(), "mango".into(), "zebra".into()];
        assert_eq!(numeric_skew(&p), None);
    }

    // ── column insights ──

    #[test]
    fn insights_flag_unindexed_fk_as_warning() {
        let ctx = ColumnContext {
            is_foreign_key: true,
            is_indexed: false,
            ..ColumnContext::default()
        };
        let insights = derive_column_insights(&profile(true), &PgType::Uuid, Some(100), ctx);
        assert!(insights.tags.contains(&ColumnTag::FkUnindexed));
        assert!(ColumnTag::FkUnindexed.is_warning());
    }

    #[test]
    fn insights_flag_indexed_fk() {
        let ctx = ColumnContext {
            is_foreign_key: true,
            is_indexed: true,
            ..ColumnContext::default()
        };
        let insights = derive_column_insights(&profile(true), &PgType::Uuid, Some(100), ctx);
        assert!(insights.tags.contains(&ColumnTag::FkWellIndexed));
    }

    #[test]
    fn insights_monotonic_id() {
        let mut p = profile(true);
        p.n_distinct = -1.0; // unique
        p.correlation = Some(1.0); // clustered
        let ctx = ColumnContext {
            is_primary_key: true,
            is_indexed: true,
            ..ColumnContext::default()
        };
        let insights = derive_column_insights(&p, &PgType::BigInt, Some(1_000_000), ctx);
        assert!(insights.tags.contains(&ColumnTag::MonotonicId));
        assert!(insights.tags.contains(&ColumnTag::LikelyIdentifier));
        // AppendCorrelated is subsumed by MonotonicId and should not appear.
        assert!(!insights.tags.contains(&ColumnTag::AppendCorrelated));
    }

    #[test]
    fn insights_enum_ish_text() {
        let mut p = profile(true);
        p.n_distinct = 4.0;
        let insights =
            derive_column_insights(&p, &PgType::Text, Some(1_000_000), ColumnContext::default());
        assert_eq!(insights.distinctness, Some(Distinctness::Categorical));
        assert!(insights.tags.contains(&ColumnTag::EnumIsh));
    }

    #[test]
    fn insights_freeform_text() {
        let mut p = profile(true);
        p.n_distinct = 12_000.0;
        let insights =
            derive_column_insights(&p, &PgType::Text, Some(1_000_000), ColumnContext::default());
        assert!(insights.tags.contains(&ColumnTag::Freeform));
    }

    #[test]
    fn insights_sparsity_absent_when_unanalyzed() {
        let insights = derive_column_insights(
            &profile(false),
            &PgType::Text,
            Some(100),
            ColumnContext::default(),
        );
        assert_eq!(insights.sparsity, None);
    }

    // ── table role ──

    fn table_profile() -> TableProfile {
        TableProfile {
            estimated_rows: Some(0),
            heap_bytes: 0,
            index_bytes: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
            analyze_age_days: Some(0),
        }
    }

    fn pk(cols: &[&str]) -> Constraint {
        Constraint::PrimaryKey {
            name: None,
            columns: cols.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn fk(col: &str, target: &str) -> Constraint {
        Constraint::ForeignKey {
            name: None,
            columns: vec![col.into()],
            references: ForeignKeyRef {
                table: target.into(),
                columns: vec!["id".into()],
            },
            on_delete: None,
            on_update: None,
        }
    }

    #[test]
    fn role_junction_table() {
        let mut t = Table::new("post_tags");
        t.add_column(Column::new("post_id", PgType::Uuid));
        t.add_column(Column::new("tag_id", PgType::Uuid));
        t.add_constraint(pk(&["post_id", "tag_id"]));
        t.add_constraint(fk("post_id", "posts"));
        t.add_constraint(fk("tag_id", "tags"));
        assert_eq!(
            infer_table_role(&table_profile(), &t),
            Some(TableRole::JunctionTable)
        );
    }

    #[test]
    fn role_event_log() {
        let mut t = Table::new("events");
        t.add_column(Column::new("id", PgType::BigInt));
        t.add_column(Column::new("created_at", PgType::Timestamptz));
        t.add_column(Column::new("payload", PgType::Jsonb));
        t.add_constraint(pk(&["id"]));
        let mut p = table_profile();
        p.estimated_rows = Some(5_000_000);
        p.inserts = 5_000_000;
        p.updates = 10;
        assert_eq!(infer_table_role(&p, &t), Some(TableRole::EventLog));
    }

    #[test]
    fn role_not_event_log_when_update_heavy() {
        let mut t = Table::new("accounts");
        t.add_column(Column::new("id", PgType::BigInt));
        t.add_column(Column::new("created_at", PgType::Timestamptz));
        t.add_constraint(pk(&["id"]));
        let mut p = table_profile();
        p.estimated_rows = Some(5_000_000);
        p.inserts = 1_000;
        p.updates = 1_000_000; // heavy churn — not an append-only log
        assert_eq!(infer_table_role(&p, &t), None);
    }

    #[test]
    fn role_lookup_table() {
        let mut t = Table::new("countries");
        t.add_column(Column::new("code", PgType::Char(Some(2))));
        t.add_column(Column::new("name", PgType::Text));
        t.add_constraint(pk(&["code"]));
        let mut p = table_profile();
        p.estimated_rows = Some(195);
        assert_eq!(infer_table_role(&p, &t), Some(TableRole::LookupTable));
    }

    #[test]
    fn role_none_for_ambiguous_table() {
        // Large, single-FK, no creation timestamp — nothing confident.
        let mut t = Table::new("orders");
        t.add_column(Column::new("id", PgType::Uuid));
        t.add_column(Column::new("customer_id", PgType::Uuid));
        t.add_column(Column::new("total", PgType::Numeric(None)));
        t.add_constraint(pk(&["id"]));
        t.add_constraint(fk("customer_id", "customers"));
        let mut p = table_profile();
        p.estimated_rows = Some(2_000_000);
        assert_eq!(infer_table_role(&p, &t), None);
    }
}
