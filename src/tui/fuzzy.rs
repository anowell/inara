use nucleo_matcher::pattern::{AtomKind, CaseMatching, Normalization, Pattern};
use nucleo_matcher::{Config, Matcher, Utf32Str};
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::migration::loader::MigrationIndex;
use crate::schema::Schema;
use crate::tui::goto::GotoTarget;

/// What category of symbols to search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchFilter {
    /// All symbol types (tables, columns, enums, types).
    All,
    /// Only table names.
    Tables,
    /// Only column names (displayed as table.column).
    Columns,
    /// Migrations (placeholder).
    Migrations,
    /// Goto navigation picker (pre-populated candidates).
    GotoPick,
}

impl SearchFilter {
    /// Display label for the search overlay title.
    pub fn label(&self) -> &'static str {
        match self {
            SearchFilter::All => "Find Symbol",
            SearchFilter::Tables => "Find Table",
            SearchFilter::Columns => "Find Column",
            SearchFilter::Migrations => "Find Migration",
            SearchFilter::GotoPick => "Goto",
        }
    }
}

/// A searchable symbol extracted from the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Symbol {
    /// Display text used for matching and rendering.
    pub display: String,
    /// The kind of symbol (for filtering).
    pub kind: SymbolKind,
}

/// Classification of a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolKind {
    Table,
    Column,
    Enum,
    Type,
    Migration,
}

impl SymbolKind {
    fn tag(&self) -> &'static str {
        match self {
            SymbolKind::Table => "table",
            SymbolKind::Column => "column",
            SymbolKind::Enum => "enum",
            SymbolKind::Type => "type",
            SymbolKind::Migration => "migration",
        }
    }
}

/// A scored search result.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub symbol: Symbol,
    pub score: u32,
}

/// Extract all searchable symbols from a schema.
pub fn extract_symbols(schema: &Schema) -> Vec<Symbol> {
    let mut symbols = Vec::new();

    for name in schema.tables.keys() {
        symbols.push(Symbol {
            display: name.clone(),
            kind: SymbolKind::Table,
        });

        if let Some(table) = schema.tables.get(name) {
            for col in &table.columns {
                symbols.push(Symbol {
                    display: format!("{}.{}", name, col.name),
                    kind: SymbolKind::Column,
                });
            }
        }
    }

    for name in schema.enums.keys() {
        symbols.push(Symbol {
            display: name.clone(),
            kind: SymbolKind::Enum,
        });
    }

    for name in schema.types.keys() {
        symbols.push(Symbol {
            display: name.clone(),
            kind: SymbolKind::Type,
        });
    }

    symbols
}

/// Perform fuzzy matching on symbols, returning scored results sorted by relevance.
pub fn fuzzy_match(symbols: &[Symbol], query: &str, filter: SearchFilter) -> Vec<SearchResult> {
    // Filter symbols by kind first
    let filtered: Vec<&Symbol> = symbols
        .iter()
        .filter(|s| match filter {
            SearchFilter::All => true,
            SearchFilter::Tables => s.kind == SymbolKind::Table,
            SearchFilter::Columns => s.kind == SymbolKind::Column,
            SearchFilter::Migrations => s.kind == SymbolKind::Migration,
            SearchFilter::GotoPick => false, // handled separately
        })
        .collect();

    if query.is_empty() {
        // Return all filtered symbols with score 0 (unscored, natural order)
        return filtered
            .into_iter()
            .map(|s| SearchResult {
                symbol: s.clone(),
                score: 0,
            })
            .collect();
    }

    let mut matcher = Matcher::new(Config::DEFAULT);
    let pattern = Pattern::new(
        query,
        CaseMatching::Ignore,
        Normalization::Smart,
        AtomKind::Fuzzy,
    );

    let mut results: Vec<SearchResult> = filtered
        .into_iter()
        .filter_map(|s| {
            let mut buf = Vec::new();
            let haystack = Utf32Str::new(&s.display, &mut buf);
            let score = pattern.score(haystack, &mut matcher)?;
            Some(SearchResult {
                symbol: s.clone(),
                score,
            })
        })
        .collect();

    // Sort by score descending (higher is better)
    results.sort_by(|a, b| b.score.cmp(&a.score));
    results
}

/// Search state held in AppState during search mode.
#[derive(Debug, Clone)]
pub struct SearchState {
    /// The search query string.
    pub query: String,
    /// What kind of symbols to search.
    pub filter: SearchFilter,
    /// All symbols extracted from the schema (cached on search start).
    pub symbols: Vec<Symbol>,
    /// Current match results.
    pub results: Vec<SearchResult>,
    /// Index of the selected result (0-based).
    pub selected: usize,
    /// Goto targets (only set when filter is GotoPick).
    pub goto_targets: Vec<GotoTarget>,
}

impl SearchState {
    /// Create a new search state for the given schema and filter.
    pub fn new(schema: &Schema, migrations: &MigrationIndex, filter: SearchFilter) -> Self {
        let mut symbols = extract_symbols(schema);
        // Add migration symbols when searching migrations
        if filter == SearchFilter::Migrations || filter == SearchFilter::All {
            for migration in &migrations.migrations {
                symbols.push(Symbol {
                    display: format!("{} — {}", migration.timestamp, migration.description),
                    kind: SymbolKind::Migration,
                });
            }
        }
        let results = fuzzy_match(&symbols, "", filter);
        Self {
            query: String::new(),
            filter,
            symbols,
            results,
            selected: 0,
            goto_targets: Vec::new(),
        }
    }

    /// Update the query and recompute results.
    pub fn set_query(mut self, query: String) -> Self {
        if self.filter == SearchFilter::GotoPick {
            // For goto picker, filter the pre-built symbols using fuzzy match on All
            self.results = fuzzy_match(&self.symbols, &query, SearchFilter::All);
        } else {
            self.results = fuzzy_match(&self.symbols, &query, self.filter);
        }
        self.query = query;
        self.selected = 0;
        self
    }

    /// Append a character to the query.
    pub fn push_char(self, ch: char) -> Self {
        let mut query = self.query.clone();
        query.push(ch);
        self.set_query(query)
    }

    /// Remove the last character from the query.
    pub fn pop_char(self) -> Self {
        let mut query = self.query.clone();
        query.pop();
        self.set_query(query)
    }

    /// Move selection down.
    pub fn select_next(mut self) -> Self {
        if !self.results.is_empty() {
            self.selected = (self.selected + 1).min(self.results.len() - 1);
        }
        self
    }

    /// Move selection up.
    pub fn select_prev(mut self) -> Self {
        self.selected = self.selected.saturating_sub(1);
        self
    }

    /// Get the currently selected result.
    pub fn selected_result(&self) -> Option<&SearchResult> {
        self.results.get(self.selected)
    }

    /// Create a search state pre-populated with goto navigation targets.
    ///
    /// The picker shows the targets as selectable items. Fuzzy filtering
    /// works on the target labels.
    pub fn from_goto_targets(targets: Vec<GotoTarget>) -> Self {
        let symbols: Vec<Symbol> = targets
            .iter()
            .map(|t| Symbol {
                display: t.label.clone(),
                kind: SymbolKind::Table, // kind is irrelevant for goto picker
            })
            .collect();

        let results: Vec<SearchResult> = symbols
            .iter()
            .map(|s| SearchResult {
                symbol: s.clone(),
                score: 0,
            })
            .collect();

        Self {
            query: String::new(),
            filter: SearchFilter::GotoPick,
            symbols,
            results,
            selected: 0,
            goto_targets: targets,
        }
    }

    /// Get the selected goto target (only valid when filter is GotoPick).
    pub fn selected_goto_target(&self) -> Option<&GotoTarget> {
        if self.filter != SearchFilter::GotoPick {
            return None;
        }
        // The selected result's label matches a goto target label
        let selected = self.selected_result()?;
        self.goto_targets
            .iter()
            .find(|t| t.label == selected.symbol.display)
    }
}

/// Space menu items.
pub const SPACE_MENU_ITEMS: &[(&str, &str)] = &[
    ("f", "Find all symbols"),
    ("t", "Find table"),
    ("c", "Find column"),
    ("m", "Find migration"),
    ("p", "Pending migrations"),
    ("?", "Help"),
];

/// Render the space menu overlay.
pub fn render_space_menu(frame: &mut Frame, area: Rect) {
    let menu_width: u16 = 26;
    let menu_height: u16 = SPACE_MENU_ITEMS.len() as u16 + 2; // borders

    // Center horizontally, place near top
    let x = area.x + area.width.saturating_sub(menu_width) / 2;
    let y = area.y + 2;
    let menu_area = Rect::new(
        x,
        y,
        menu_width.min(area.width),
        menu_height.min(area.height.saturating_sub(2)),
    );

    // Clear the area behind the menu
    frame.render_widget(Clear, menu_area);

    let lines: Vec<Line<'static>> = SPACE_MENU_ITEMS
        .iter()
        .map(|(key, label)| {
            Line::from(vec![
                Span::styled(
                    format!("  {key}"),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(format!("  {label}"), Style::default().fg(Color::White)),
            ])
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(" Space ");

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, menu_area);
}

/// Render the search overlay.
pub fn render_search_overlay(frame: &mut Frame, area: Rect, search: &SearchState) {
    let overlay_width: u16 = 50.min(area.width.saturating_sub(4));
    let max_results: u16 = 10;
    let overlay_height: u16 = (max_results + 3).min(area.height.saturating_sub(2)); // input + border + results

    // Center horizontally, place near top
    let x = area.x + area.width.saturating_sub(overlay_width) / 2;
    let y = area.y + 1;
    let overlay_area = Rect::new(x, y, overlay_width, overlay_height);

    // Clear the area behind the overlay
    frame.render_widget(Clear, overlay_area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green))
        .title(format!(" {} ", search.filter.label()));

    let inner = block.inner(overlay_area);
    frame.render_widget(block, overlay_area);

    // Build lines: input line + results
    let mut lines: Vec<Line<'static>> = Vec::new();

    // Input line with cursor indicator
    lines.push(Line::from(vec![
        Span::styled("> ", Style::default().fg(Color::Green)),
        Span::styled(search.query.clone(), Style::default().fg(Color::White)),
        Span::styled("_", Style::default().fg(Color::DarkGray)),
    ]));

    // Results
    let visible_count = (inner.height as usize).saturating_sub(1); // minus input line
    let result_count = search.results.len().min(visible_count);
    for (i, result) in search.results.iter().take(result_count).enumerate() {
        let is_selected = i == search.selected;
        let kind_tag = result.symbol.kind.tag();

        let style = if is_selected {
            Style::default()
                .fg(Color::White)
                .bg(Color::Indexed(236))
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };

        let tag_style = if is_selected {
            Style::default().fg(Color::DarkGray).bg(Color::Indexed(236))
        } else {
            Style::default().fg(Color::DarkGray)
        };

        lines.push(Line::from(vec![
            Span::styled(format!("  {}", result.symbol.display), style),
            Span::styled(format!("  {kind_tag}"), tag_style),
        ]));
    }

    let paragraph = Paragraph::new(lines);
    frame.render_widget(paragraph, inner);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::PgType;
    use crate::schema::{Column, EnumType, Schema, Table};

    fn sample_schema() -> Schema {
        let mut schema = Schema::new();

        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("email", PgType::Text));
        users.add_column(Column::new("name", PgType::Text));
        schema.add_table(users);

        let mut posts = Table::new("posts");
        posts.add_column(Column::new("id", PgType::Uuid));
        posts.add_column(Column::new("title", PgType::Text));
        posts.add_column(Column::new("author_id", PgType::Uuid));
        schema.add_table(posts);

        let mut comments = Table::new("comments");
        comments.add_column(Column::new("id", PgType::Uuid));
        comments.add_column(Column::new("body", PgType::Text));
        comments.add_column(Column::new("post_id", PgType::Uuid));
        schema.add_table(comments);

        schema.add_enum(EnumType {
            name: "mood".into(),
            variants: vec!["happy".into(), "sad".into()],
        });

        schema
    }

    #[test]
    fn extract_symbols_includes_tables() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let tables: Vec<&str> = symbols
            .iter()
            .filter(|s| s.kind == SymbolKind::Table)
            .map(|s| s.display.as_str())
            .collect();
        assert!(tables.contains(&"users"));
        assert!(tables.contains(&"posts"));
        assert!(tables.contains(&"comments"));
    }

    #[test]
    fn extract_symbols_includes_columns() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let columns: Vec<&str> = symbols
            .iter()
            .filter(|s| s.kind == SymbolKind::Column)
            .map(|s| s.display.as_str())
            .collect();
        assert!(columns.contains(&"users.email"));
        assert!(columns.contains(&"posts.title"));
        assert!(columns.contains(&"comments.body"));
    }

    #[test]
    fn extract_symbols_includes_enums() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let enums: Vec<&str> = symbols
            .iter()
            .filter(|s| s.kind == SymbolKind::Enum)
            .map(|s| s.display.as_str())
            .collect();
        assert!(enums.contains(&"mood"));
    }

    #[test]
    fn fuzzy_match_usr_matches_users() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let results = fuzzy_match(&symbols, "usr", SearchFilter::Tables);
        assert!(!results.is_empty(), "should match at least one table");
        assert_eq!(
            results[0].symbol.display, "users",
            "users should be the top match for 'usr'"
        );
    }

    #[test]
    fn fuzzy_match_email_matches_columns() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let results = fuzzy_match(&symbols, "email", SearchFilter::Columns);
        assert!(!results.is_empty(), "should match columns containing email");
        assert!(
            results.iter().any(|r| r.symbol.display == "users.email"),
            "users.email should be in results"
        );
    }

    #[test]
    fn fuzzy_match_empty_query_returns_all_filtered() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let results = fuzzy_match(&symbols, "", SearchFilter::Tables);
        assert_eq!(results.len(), 3, "should return all 3 tables");
    }

    #[test]
    fn fuzzy_match_all_filter_includes_all_kinds() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let results = fuzzy_match(&symbols, "", SearchFilter::All);
        // 3 tables + 9 columns + 1 enum = 13
        assert_eq!(results.len(), 13);
    }

    #[test]
    fn fuzzy_match_migration_filter_returns_empty() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let results = fuzzy_match(&symbols, "anything", SearchFilter::Migrations);
        assert!(results.is_empty(), "migrations are placeholder");
    }

    #[test]
    fn fuzzy_match_no_match_returns_empty() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        let results = fuzzy_match(&symbols, "zzzznotfound", SearchFilter::All);
        assert!(results.is_empty());
    }

    #[test]
    fn fuzzy_match_ranks_exact_higher() {
        let schema = sample_schema();
        let symbols = extract_symbols(&schema);
        // "posts" should rank higher than "comments.post_id" when searching for "posts"
        let results = fuzzy_match(&symbols, "posts", SearchFilter::All);
        assert!(!results.is_empty());
        // The table "posts" should be among the top results
        assert!(
            results[0].symbol.display == "posts"
                || results
                    .iter()
                    .position(|r| r.symbol.display == "posts")
                    .unwrap_or(usize::MAX)
                    < 3,
            "exact match 'posts' should be near the top"
        );
    }

    #[test]
    fn search_state_push_char_updates_results() {
        let schema = sample_schema();
        let state = SearchState::new(&schema, &MigrationIndex::default(), SearchFilter::All);
        assert_eq!(state.results.len(), 13); // all symbols

        let state = state.push_char('u');
        assert!(state.results.len() < 13, "filtering should reduce results");
        assert!(
            state.results.iter().any(|r| r.symbol.display == "users"),
            "users should match 'u'"
        );
    }

    #[test]
    fn search_state_pop_char_widens_results() {
        let schema = sample_schema();
        let state = SearchState::new(&schema, &MigrationIndex::default(), SearchFilter::All);
        let state = state.push_char('u').push_char('s').push_char('e');
        let narrow_count = state.results.len();

        let state = state.pop_char();
        assert!(
            state.results.len() >= narrow_count,
            "popping a char should widen or maintain results"
        );
    }

    #[test]
    fn search_state_select_navigation() {
        let schema = sample_schema();
        let state = SearchState::new(&schema, &MigrationIndex::default(), SearchFilter::Tables);
        assert_eq!(state.selected, 0);
        assert_eq!(state.results.len(), 3);

        let state = state.select_next();
        assert_eq!(state.selected, 1);

        let state = state.select_next();
        assert_eq!(state.selected, 2);

        // Clamp at end
        let state = state.select_next();
        assert_eq!(state.selected, 2);

        let state = state.select_prev();
        assert_eq!(state.selected, 1);

        let state = state.select_prev().select_prev();
        assert_eq!(state.selected, 0); // clamped at 0
    }

    #[test]
    fn search_state_selected_result() {
        let schema = sample_schema();
        let state = SearchState::new(&schema, &MigrationIndex::default(), SearchFilter::Tables);
        let result = state.selected_result();
        assert!(result.is_some(), "should have selected result");
    }

    #[test]
    fn search_state_empty_results_selected() {
        let schema = sample_schema();
        let state = SearchState::new(
            &schema,
            &MigrationIndex::default(),
            SearchFilter::Migrations,
        );
        assert!(state.selected_result().is_none());
    }

    #[test]
    fn search_state_typing_resets_selection() {
        let schema = sample_schema();
        let state = SearchState::new(&schema, &MigrationIndex::default(), SearchFilter::Tables);
        let state = state.select_next().select_next();
        assert_eq!(state.selected, 2);

        let state = state.push_char('u');
        assert_eq!(state.selected, 0, "typing should reset selection to 0");
    }

    #[test]
    fn search_filter_labels() {
        assert_eq!(SearchFilter::All.label(), "Find Symbol");
        assert_eq!(SearchFilter::Tables.label(), "Find Table");
        assert_eq!(SearchFilter::Columns.label(), "Find Column");
        assert_eq!(SearchFilter::Migrations.label(), "Find Migration");
    }

    #[test]
    fn large_schema_performance() {
        // Test with 150 tables, each with 10 columns = 1500+ symbols
        let mut schema = Schema::new();
        for i in 0..150 {
            let mut table = Table::new(format!("table_{i:03}"));
            for j in 0..10 {
                table.add_column(Column::new(format!("col_{j}"), PgType::Text));
            }
            schema.add_table(table);
        }

        let symbols = extract_symbols(&schema);
        assert_eq!(symbols.len(), 150 + 1500); // 150 tables + 1500 columns

        // This should complete quickly (no lag)
        let results = fuzzy_match(&symbols, "table_05", SearchFilter::All);
        assert!(!results.is_empty());
    }
}
