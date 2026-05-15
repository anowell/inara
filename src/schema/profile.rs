//! Raw catalog & `pg_stats` measurements — the *measurement* half of the
//! two-layer HUD model.
//!
//! These structs hold values exactly as Postgres reports them, with no
//! interpretation. Interpretation lives in [`crate::schema::insight`]; the IO
//! that populates these structs lives in the data layer
//! ([`crate::schema::profile_load`]).
//!
//! Keeping measurement and interpretation apart means every heuristic can be
//! unit-tested against hand-built profiles with no database.

/// Catalog-level measurements for a table.
///
/// Sourced entirely from metadata Postgres already maintains — `pg_class`,
/// `pg_table_size()` / `pg_indexes_size()`, and `pg_stat_user_tables`. None of
/// it touches the table's heap, so loading a `TableProfile` is instant
/// regardless of table size.
#[derive(Debug, Clone, PartialEq)]
pub struct TableProfile {
    /// Estimated live row count (`pg_class.reltuples`, rounded).
    ///
    /// `None` when the table has never been analyzed — Postgres reports
    /// `reltuples = -1` in that case (PG14+). A missing estimate is itself a
    /// useful anomaly signal.
    pub estimated_rows: Option<i64>,
    /// Heap (main fork) size in bytes (`pg_table_size`).
    pub heap_bytes: i64,
    /// Combined size of every index on the table in bytes (`pg_indexes_size`).
    pub index_bytes: i64,
    /// Lifetime tuple inserts (`pg_stat_user_tables.n_tup_ins`).
    pub inserts: i64,
    /// Lifetime tuple updates (`n_tup_upd`).
    pub updates: i64,
    /// Lifetime tuple deletes (`n_tup_del`).
    pub deletes: i64,
    /// Whole days since the most recent `ANALYZE` (manual or autovacuum).
    ///
    /// `None` when the table has never been analyzed.
    pub analyze_age_days: Option<i64>,
}

impl TableProfile {
    /// Total on-disk footprint: heap plus indexes.
    pub fn total_bytes(&self) -> i64 {
        self.heap_bytes.saturating_add(self.index_bytes)
    }

    /// Whether planner statistics are present for this table.
    ///
    /// `false` means the table has never been analyzed; every derived figure
    /// should then be shown as unavailable rather than as a real value.
    pub fn is_analyzed(&self) -> bool {
        self.estimated_rows.is_some()
    }

    /// Whether `ANALYZE` last ran long enough ago that the statistics should
    /// be treated as suspect. The threshold is deliberately generous — this
    /// is a soft "take the numbers with salt" hint, not a hard error.
    pub fn stats_are_stale(&self) -> bool {
        matches!(self.analyze_age_days, Some(days) if days >= STALE_ANALYZE_DAYS)
    }
}

/// `ANALYZE` older than this many days marks the stats as stale.
pub const STALE_ANALYZE_DAYS: i64 = 30;

/// `pg_stats` measurements for a single column.
///
/// Values are as Postgres reports them. The array-typed stats columns
/// (`most_common_vals`, `histogram_bounds`) are `anyarray` and cannot be
/// decoded without the element type, so the loader casts them to `text[]`;
/// every value here is therefore the text rendering of the underlying datum.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnProfile {
    /// Fraction of rows where the column is NULL, `0.0..=1.0` (`null_frac`).
    pub null_frac: f64,
    /// `pg_stats.n_distinct`, in Postgres' own encoding:
    ///
    /// * `> 0` — an absolute estimated distinct-value count.
    /// * `< 0` — the *negated* ratio of distinct values to row count
    ///   (e.g. `-1.0` means every row is distinct).
    /// * `0` — distinctness unknown.
    pub n_distinct: f64,
    /// Average stored width of the column in bytes (`avg_width`).
    pub avg_width: i32,
    /// Correlation between physical row order and ascending column order,
    /// `-1.0..=1.0` (`correlation`). `None` when Postgres reports none.
    pub correlation: Option<f64>,
    /// Most common values, each rendered as text. Paired index-wise with
    /// [`Self::mcv_freqs`].
    pub mcv: Vec<String>,
    /// Frequency of each [`Self::mcv`] entry, `0.0..=1.0` (`most_common_freqs`).
    pub mcv_freqs: Vec<f64>,
    /// Histogram bucket bounds, each rendered as text (`histogram_bounds`).
    /// Ascending; the first entry is the approximate minimum and the last the
    /// approximate maximum.
    pub histogram: Vec<String>,
    /// `false` when no `pg_stats` row exists for the column — it has never
    /// been analyzed, or the current role lacks privileges to see its stats.
    pub analyzed: bool,
}

impl ColumnProfile {
    /// A profile standing in for "no statistics available".
    pub fn unavailable() -> Self {
        Self {
            null_frac: 0.0,
            n_distinct: 0.0,
            avg_width: 0,
            correlation: None,
            mcv: Vec::new(),
            mcv_freqs: Vec::new(),
            histogram: Vec::new(),
            analyzed: false,
        }
    }

    /// Resolve [`Self::n_distinct`] to an absolute estimated distinct count,
    /// using the table's estimated row count to expand the ratio form.
    ///
    /// Returns `None` when distinctness is unknown (`n_distinct == 0`), the
    /// column is unanalyzed, or a ratio is given without a known row count.
    pub fn distinct_estimate(&self, estimated_rows: Option<i64>) -> Option<f64> {
        if !self.analyzed {
            return None;
        }
        if self.n_distinct > 0.0 {
            Some(self.n_distinct)
        } else if self.n_distinct < 0.0 {
            estimated_rows
                .filter(|&r| r >= 0)
                .map(|r| (-self.n_distinct) * r as f64)
        } else {
            None
        }
    }

    /// The distinct-to-rowcount ratio, `0.0..=1.0`, when it can be determined.
    ///
    /// The ratio form of `n_distinct` carries this directly; the absolute form
    /// needs a row count to divide by.
    pub fn distinct_ratio(&self, estimated_rows: Option<i64>) -> Option<f64> {
        if !self.analyzed {
            return None;
        }
        if self.n_distinct < 0.0 {
            Some((-self.n_distinct).min(1.0))
        } else if self.n_distinct > 0.0 {
            estimated_rows
                .filter(|&r| r > 0)
                .map(|r| (self.n_distinct / r as f64).min(1.0))
        } else {
            None
        }
    }

    /// Approximate `(minimum, maximum)` drawn from the histogram bounds.
    pub fn value_range(&self) -> Option<(&str, &str)> {
        match (self.histogram.first(), self.histogram.last()) {
            (Some(lo), Some(hi)) if self.histogram.len() >= 2 => Some((lo.as_str(), hi.as_str())),
            _ => None,
        }
    }

    /// The middle histogram bound — an approximate median (p50).
    pub fn approx_median(&self) -> Option<&str> {
        if self.histogram.len() < 3 {
            return None;
        }
        self.histogram
            .get(self.histogram.len() / 2)
            .map(String::as_str)
    }

    /// The most common value and its frequency, when statistics carry one.
    pub fn top_value(&self) -> Option<(&str, f64)> {
        match (self.mcv.first(), self.mcv_freqs.first()) {
            (Some(v), Some(&f)) => Some((v.as_str(), f)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyzed_profile() -> ColumnProfile {
        ColumnProfile {
            null_frac: 0.0,
            n_distinct: 0.0,
            avg_width: 4,
            correlation: None,
            mcv: Vec::new(),
            mcv_freqs: Vec::new(),
            histogram: Vec::new(),
            analyzed: true,
        }
    }

    #[test]
    fn total_bytes_sums_heap_and_index() {
        let p = TableProfile {
            estimated_rows: Some(100),
            heap_bytes: 8_000,
            index_bytes: 2_000,
            inserts: 0,
            updates: 0,
            deletes: 0,
            analyze_age_days: Some(1),
        };
        assert_eq!(p.total_bytes(), 10_000);
        assert!(p.is_analyzed());
        assert!(!p.stats_are_stale());
    }

    #[test]
    fn unanalyzed_table_reports_no_estimate() {
        let p = TableProfile {
            estimated_rows: None,
            heap_bytes: 0,
            index_bytes: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
            analyze_age_days: None,
        };
        assert!(!p.is_analyzed());
        assert!(!p.stats_are_stale());
    }

    #[test]
    fn old_analyze_is_stale() {
        let p = TableProfile {
            estimated_rows: Some(1),
            heap_bytes: 0,
            index_bytes: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
            analyze_age_days: Some(200),
        };
        assert!(p.stats_are_stale());
    }

    #[test]
    fn unavailable_profile_is_not_analyzed() {
        let p = ColumnProfile::unavailable();
        assert!(!p.analyzed);
        assert_eq!(p.distinct_estimate(Some(1000)), None);
        assert_eq!(p.distinct_ratio(Some(1000)), None);
    }

    #[test]
    fn distinct_estimate_absolute_form() {
        let mut p = analyzed_profile();
        p.n_distinct = 42.0;
        assert_eq!(p.distinct_estimate(None), Some(42.0));
        assert_eq!(p.distinct_estimate(Some(10_000)), Some(42.0));
    }

    #[test]
    fn distinct_estimate_ratio_form_needs_rowcount() {
        let mut p = analyzed_profile();
        p.n_distinct = -1.0; // every row distinct
        assert_eq!(p.distinct_estimate(None), None);
        assert_eq!(p.distinct_estimate(Some(10_000)), Some(10_000.0));

        p.n_distinct = -0.5;
        assert_eq!(p.distinct_estimate(Some(10_000)), Some(5_000.0));
    }

    #[test]
    fn distinct_estimate_unknown_when_zero() {
        let p = analyzed_profile(); // n_distinct == 0.0
        assert_eq!(p.distinct_estimate(Some(10_000)), None);
    }

    #[test]
    fn distinct_ratio_from_negative_form() {
        let mut p = analyzed_profile();
        p.n_distinct = -0.95;
        assert_eq!(p.distinct_ratio(None), Some(0.95));
    }

    #[test]
    fn distinct_ratio_from_absolute_form() {
        let mut p = analyzed_profile();
        p.n_distinct = 250.0;
        assert_eq!(p.distinct_ratio(Some(1_000)), Some(0.25));
        assert_eq!(p.distinct_ratio(None), None);
    }

    #[test]
    fn distinct_ratio_clamps_to_one() {
        let mut p = analyzed_profile();
        // Estimate can exceed rowcount on stale stats; ratio must still cap.
        p.n_distinct = 5_000.0;
        assert_eq!(p.distinct_ratio(Some(1_000)), Some(1.0));
    }

    #[test]
    fn value_range_and_median_from_histogram() {
        let mut p = analyzed_profile();
        p.histogram = vec!["0".into(), "42".into(), "18222".into()];
        assert_eq!(p.value_range(), Some(("0", "18222")));
        assert_eq!(p.approx_median(), Some("42"));
    }

    #[test]
    fn value_range_none_for_short_histogram() {
        let mut p = analyzed_profile();
        p.histogram = vec!["7".into()];
        assert_eq!(p.value_range(), None);
        assert_eq!(p.approx_median(), None);
    }

    #[test]
    fn top_value_pairs_mcv_with_freq() {
        let mut p = analyzed_profile();
        p.mcv = vec!["active".into(), "pending".into()];
        p.mcv_freqs = vec![0.7, 0.2];
        assert_eq!(p.top_value(), Some(("active", 0.7)));
    }

    #[test]
    fn top_value_none_without_mcv() {
        let p = analyzed_profile();
        assert_eq!(p.top_value(), None);
    }
}
