use std::collections::BTreeSet;
use std::time::Instant;

use strum::Display;

use super::fuzzy::SearchState;
use super::goto::{GotoFocus, GotoTarget};
use super::hud::{HudState, HudStatus};
use crate::schema::relations::RelationMap;
use crate::schema::type_map::TypeMapper;
use crate::schema::Schema;

/// The TUI application mode. Determines which input handler processes keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum Mode {
    Normal,
    Edit,
    Rename,
    Search,
    HUD,
    Command,
    SpaceMenu,
    MigrationPreview,
}

/// Metadata for a rename operation. Recorded so the diff engine can
/// distinguish renames from drop+create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameMetadata {
    /// The table containing the renamed element (or the table itself if renaming a table).
    pub table: String,
    /// The original name.
    pub from: String,
    /// The new name.
    pub to: String,
}

/// What kind of element is being renamed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameTarget {
    Table(String),
    Column(String, String),
}

/// State for the migration preview overlay.
#[derive(Debug, Clone)]
pub struct MigrationPreviewState {
    /// The generated SQL to display.
    pub sql: String,
    /// Migration description (from `:w description`).
    pub description: String,
    /// Scroll offset in the preview.
    pub scroll: usize,
}

/// Pending key state for multi-key sequences (e.g. `gg`, `g r`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingKey {
    None,
    G,
}

/// What type of schema element is currently focused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FocusTarget {
    /// An enum type declaration (header line).
    Enum(String),
    /// A variant line inside an enum block.
    EnumVariant(String, usize),
    /// A closing brace for an enum block.
    EnumClose(String),
    /// A custom type declaration (single-line for domain/range, header for composite).
    Type(String),
    /// A field line inside a composite type block.
    TypeField(String, usize),
    /// A closing brace for a composite type block.
    TypeClose(String),
    /// A table header line (`table name {` or `table name { ... N columns ... }`).
    Table(String),
    /// A column line inside a table block.
    Column(String, String),
    /// A blank separator line inside a table block.
    Separator(String),
    /// A constraint line inside a table block.
    Constraint(String, usize),
    /// An index line inside a table block.
    Index(String, usize),
    /// A closing brace for a table block.
    TableClose(String),
    /// A blank line between top-level blocks.
    Blank,
}

impl FocusTarget {
    /// Returns the table name if this target is inside a table block.
    pub fn table_name(&self) -> Option<&str> {
        match self {
            FocusTarget::Table(n)
            | FocusTarget::Column(n, _)
            | FocusTarget::Separator(n)
            | FocusTarget::Constraint(n, _)
            | FocusTarget::Index(n, _)
            | FocusTarget::TableClose(n) => Some(n),
            _ => None,
        }
    }
}

/// A single line in the document with its focus target.
#[derive(Debug, Clone)]
pub struct DocLine {
    pub target: FocusTarget,
}

/// The single application state struct passed to all renderers.
///
/// State transitions are immutable: methods return a new `AppState` rather
/// than mutating in place.
#[derive(Debug, Clone)]
pub struct AppState {
    /// The introspected database schema.
    pub schema: Schema,
    /// The original schema before any edits (set on first edit).
    pub original_schema: Option<Schema>,
    /// Current application mode.
    pub mode: Mode,
    /// Cursor position (line index in the document).
    pub cursor: usize,
    /// Viewport scroll offset (first visible line).
    pub viewport_offset: usize,
    /// Viewport height in lines (updated on resize).
    pub viewport_height: usize,
    /// Pending key for multi-key sequences.
    pub pending_key: PendingKey,
    /// Command-mode input buffer.
    pub command_buf: String,
    /// Whether the application should quit.
    pub should_quit: bool,
    /// Connection display string (masked URL).
    pub connection_info: String,
    /// Set of table names that are currently expanded.
    pub expanded: BTreeSet<String>,
    /// The flat document model — one entry per visible line.
    pub doc: Vec<DocLine>,
    /// Active search state (present when mode is Search).
    pub search: Option<SearchState>,
    /// Edit mode: the text buffer being edited.
    pub edit_buffer: Vec<String>,
    /// Edit mode: cursor row in the text buffer.
    pub edit_cursor_row: usize,
    /// Edit mode: cursor column in the text buffer.
    pub edit_cursor_col: usize,
    /// Edit mode: the name of the table being edited.
    pub edit_table: Option<String>,
    /// Edit mode: parse error message to display.
    pub edit_error: Option<String>,
    /// Rename mode: the input buffer for the new name.
    pub rename_buf: String,
    /// Rename mode: what element is being renamed.
    pub rename_target: Option<RenameTarget>,
    /// Accumulated rename metadata for the diff engine.
    pub renames: Vec<RenameMetadata>,
    /// Set of table names that have been edited (for visual diff hints).
    pub edited_tables: BTreeSet<String>,
    /// HUD overlay state (present when mode is HUD).
    pub hud: Option<HudState>,
    /// Precomputed relation map for O(1) FK/index lookups.
    pub relation_map: RelationMap,
    /// Transient status message shown in the status bar (e.g., "no references found").
    pub status_message: Option<String>,
    /// When the pending key was set (for timeout).
    pub pending_key_time: Option<Instant>,
    /// Migration preview state (present when mode is MigrationPreview).
    pub migration_preview: Option<MigrationPreviewState>,
    /// PG→Rust type mapper (feature-aware, with user overrides).
    pub type_mapper: TypeMapper,
    /// Whether to show Rust type annotations alongside PG types.
    pub show_rust_types: bool,
}

impl AppState {
    /// Create a new application state from a loaded schema.
    pub fn new(schema: Schema, connection_info: String) -> Self {
        let expanded = BTreeSet::new();
        let doc = build_document(&schema, &expanded);
        let relation_map = RelationMap::build(&schema);
        Self {
            schema,
            original_schema: None,
            mode: Mode::Normal,
            cursor: 0,
            viewport_offset: 0,
            viewport_height: 0,
            pending_key: PendingKey::None,
            command_buf: String::new(),
            should_quit: false,
            connection_info,
            expanded,
            doc,
            search: None,
            edit_buffer: Vec::new(),
            edit_cursor_row: 0,
            edit_cursor_col: 0,
            edit_table: None,
            edit_error: None,
            rename_buf: String::new(),
            rename_target: None,
            renames: Vec::new(),
            edited_tables: BTreeSet::new(),
            hud: None,
            relation_map,
            status_message: None,
            pending_key_time: None,
            migration_preview: None,
            type_mapper: TypeMapper::new(),
            show_rust_types: false,
        }
    }

    /// Total number of lines in the rendered document.
    pub fn line_count(&self) -> usize {
        self.doc.len()
    }

    /// Returns the focus target for the current cursor position.
    pub fn focus(&self) -> Option<&FocusTarget> {
        self.doc.get(self.cursor).map(|l| &l.target)
    }

    /// Transition to a new mode, resetting mode-specific state.
    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self.pending_key = PendingKey::None;
        if mode == Mode::Command {
            self.command_buf = String::new();
        }
        if mode != Mode::Search {
            self.search = None;
        }
        if mode == Mode::Rename {
            self.rename_buf = String::new();
        }
        if mode != Mode::HUD {
            self.hud = None;
        }
        if mode != Mode::MigrationPreview {
            self.migration_preview = None;
        }
        // Clear status message on any mode transition
        self.status_message = None;
        self
    }

    /// Enter search mode with the given filter.
    pub fn enter_search(mut self, filter: super::fuzzy::SearchFilter) -> Self {
        self.search = Some(SearchState::new(&self.schema, filter));
        self.mode = Mode::Search;
        self.pending_key = PendingKey::None;
        self
    }

    /// Set the HUD state.
    pub fn with_hud(mut self, hud: Option<HudState>) -> Self {
        self.hud = hud;
        self
    }

    /// Update HUD query status (called when async result arrives).
    pub fn with_hud_status(mut self, status: HudStatus) -> Self {
        if let Some(ref mut hud) = self.hud {
            hud.status = status;
        }
        self
    }

    /// Set the pending key state.
    pub fn with_pending_key(mut self, key: PendingKey) -> Self {
        self.pending_key_time = if key != PendingKey::None {
            Some(Instant::now())
        } else {
            None
        };
        self.pending_key = key;
        self
    }

    /// Set a transient status message.
    pub fn with_status(mut self, msg: impl Into<String>) -> Self {
        self.status_message = Some(msg.into());
        self
    }

    /// Clear the status message.
    pub fn clear_status(mut self) -> Self {
        self.status_message = None;
        self
    }

    /// Check if the pending key has timed out.
    pub fn is_pending_key_expired(&self, timeout: std::time::Duration) -> bool {
        self.pending_key_time
            .map(|t| t.elapsed() >= timeout)
            .unwrap_or(false)
    }

    /// Jump focus to a goto target.
    ///
    /// Expands the parent table if needed, rebuilds the document,
    /// and moves the cursor to the target element.
    pub fn jump_to_goto(mut self, target: &GotoTarget) -> Self {
        match &target.focus {
            GotoFocus::Table(name) => {
                if !self.expanded.contains(name) {
                    self.expanded.insert(name.clone());
                    self.rebuild_doc();
                }
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Table(name.clone()))
                {
                    self.cursor = pos;
                }
            }
            GotoFocus::Column(table, col) => {
                if !self.expanded.contains(table) {
                    self.expanded.insert(table.clone());
                    self.rebuild_doc();
                }
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Column(table.clone(), col.clone()))
                {
                    self.cursor = pos;
                }
            }
            GotoFocus::Enum(name) => {
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Enum(name.clone()))
                {
                    self.cursor = pos;
                }
            }
            GotoFocus::Type(name) => {
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Type(name.clone()))
                {
                    self.cursor = pos;
                }
            }
        }
        self.scroll_to_cursor()
    }

    /// Enter search mode pre-populated with goto picker results.
    pub fn enter_goto_picker(mut self, targets: Vec<GotoTarget>) -> Self {
        self.search = Some(super::fuzzy::SearchState::from_goto_targets(targets));
        self.mode = Mode::Search;
        self.pending_key = PendingKey::None;
        self.pending_key_time = None;
        self
    }

    /// Move cursor down by `n` lines, clamped to bounds.
    pub fn cursor_down(mut self, n: usize) -> Self {
        let max = self.line_count().saturating_sub(1);
        self.cursor = (self.cursor + n).min(max);
        self.scroll_to_cursor()
    }

    /// Move cursor up by `n` lines, clamped to bounds.
    pub fn cursor_up(mut self, n: usize) -> Self {
        self.cursor = self.cursor.saturating_sub(n);
        self.scroll_to_cursor()
    }

    /// Jump cursor to an absolute position.
    pub fn cursor_to(mut self, pos: usize) -> Self {
        let max = self.line_count().saturating_sub(1);
        self.cursor = pos.min(max);
        self.scroll_to_cursor()
    }

    /// Set viewport height (called on resize).
    pub fn with_viewport_height(mut self, height: usize) -> Self {
        self.viewport_height = height;
        self.scroll_to_cursor()
    }

    /// Jump focus to a symbol from search results.
    ///
    /// For tables: jumps to the table header (expanding it).
    /// For columns: jumps to the column line (expanding the parent table).
    /// For enums/types: jumps to the header line.
    pub fn jump_to_symbol(mut self, symbol: &super::fuzzy::Symbol) -> Self {
        use super::fuzzy::SymbolKind;

        match symbol.kind {
            SymbolKind::Table => {
                // Expand the table, rebuild doc, find and jump to it
                if !self.expanded.contains(&symbol.display) {
                    self.expanded.insert(symbol.display.clone());
                    self.rebuild_doc();
                }
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Table(symbol.display.clone()))
                {
                    self.cursor = pos;
                }
            }
            SymbolKind::Column => {
                // display is "table.column"
                if let Some((table_name, col_name)) = symbol.display.split_once('.') {
                    if !self.expanded.contains(table_name) {
                        self.expanded.insert(table_name.to_string());
                        self.rebuild_doc();
                    }
                    if let Some(pos) = self.doc.iter().position(|l| {
                        l.target
                            == FocusTarget::Column(table_name.to_string(), col_name.to_string())
                    }) {
                        self.cursor = pos;
                    }
                }
            }
            SymbolKind::Enum => {
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Enum(symbol.display.clone()))
                {
                    self.cursor = pos;
                }
            }
            SymbolKind::Type => {
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Type(symbol.display.clone()))
                {
                    self.cursor = pos;
                }
            }
        }

        self.scroll_to_cursor()
    }

    /// Set the quit flag.
    pub fn quit(mut self) -> Self {
        self.should_quit = true;
        self
    }

    /// Append a character to the command buffer.
    pub fn command_push(mut self, ch: char) -> Self {
        self.command_buf.push(ch);
        self
    }

    /// Remove the last character from the command buffer.
    pub fn command_pop(mut self) -> Self {
        self.command_buf.pop();
        self
    }

    /// Snapshot the original schema if this is the first edit.
    pub fn ensure_original_schema(mut self) -> Self {
        if self.original_schema.is_none() {
            self.original_schema = Some(self.schema.clone());
        }
        self
    }

    /// Returns true if any tables have been edited.
    pub fn has_edits(&self) -> bool {
        !self.edited_tables.is_empty()
    }

    /// Clear edit tracking state after a migration has been written.
    /// Sets original_schema = current schema, clears renames and edited_tables.
    pub fn clear_edit_state(mut self) -> Self {
        self.original_schema = Some(self.schema.clone());
        self.renames.clear();
        self.edited_tables.clear();
        self
    }

    /// Set a transient status message for the status bar (alias for `with_status`).
    pub fn with_status_message(self, msg: String) -> Self {
        self.with_status(msg)
    }

    /// Set the type mapper (e.g., after loading from Cargo.toml).
    pub fn with_type_mapper(mut self, mapper: TypeMapper) -> Self {
        self.type_mapper = mapper;
        self
    }

    /// Toggle Rust type annotation display on/off.
    pub fn toggle_rust_types(mut self) -> Self {
        self.show_rust_types = !self.show_rust_types;
        self
    }

    /// Toggle expand/collapse for the table under the cursor.
    pub fn toggle_expand(mut self) -> Self {
        let table_name = match self.focus() {
            Some(target) => target.table_name().map(|s| s.to_string()),
            None => None,
        };
        if let Some(name) = table_name {
            if self.expanded.contains(&name) {
                self.expanded.remove(&name);
            } else {
                self.expanded.insert(name);
            }
            self.rebuild_doc();
        }
        self
    }

    /// Jump cursor to the next table header.
    pub fn next_table(mut self) -> Self {
        for i in (self.cursor + 1)..self.doc.len() {
            if matches!(self.doc[i].target, FocusTarget::Table(_)) {
                self.cursor = i;
                return self.scroll_to_cursor();
            }
        }
        // Wrap or stay
        self
    }

    /// Jump cursor to the previous table header.
    pub fn prev_table(mut self) -> Self {
        if self.cursor == 0 {
            return self;
        }
        for i in (0..self.cursor).rev() {
            if matches!(self.doc[i].target, FocusTarget::Table(_)) {
                self.cursor = i;
                return self.scroll_to_cursor();
            }
        }
        self
    }

    /// Rebuild the document after expand/collapse or schema changes, preserving cursor context.
    pub fn rebuild_doc(&mut self) {
        let old_target = self.focus().cloned();
        self.doc = build_document(&self.schema, &self.expanded);
        self.relation_map = RelationMap::build(&self.schema);

        // Try to find the same target in the new doc
        if let Some(ref target) = old_target {
            // For table-related targets when collapsing, jump to the table header
            let search_target = match target {
                FocusTarget::Column(t, _)
                | FocusTarget::Separator(t)
                | FocusTarget::Constraint(t, _)
                | FocusTarget::Index(t, _)
                | FocusTarget::TableClose(t) => Some(FocusTarget::Table(t.clone())),
                other => Some(other.clone()),
            };

            if let Some(ref st) = search_target {
                if let Some(pos) = self.doc.iter().position(|l| &l.target == st) {
                    self.cursor = pos;
                    return;
                }
            }
        }

        // Clamp cursor
        let max = self.line_count().saturating_sub(1);
        self.cursor = self.cursor.min(max);
    }

    /// Adjust viewport so the cursor is visible.
    fn scroll_to_cursor(mut self) -> Self {
        if self.viewport_height == 0 {
            return self;
        }
        // Ensure cursor is within the visible viewport
        if self.cursor < self.viewport_offset {
            self.viewport_offset = self.cursor;
        } else if self.cursor >= self.viewport_offset + self.viewport_height {
            self.viewport_offset = self.cursor - self.viewport_height + 1;
        }
        self
    }
}

/// Build the flat document model from a schema and expanded set.
///
/// The document is a list of lines, one per visible element. Collapsed tables
/// show a single summary line. Expanded tables show all internal lines.
/// Enums and custom types are always expanded (they're typically short).
pub fn build_document(schema: &Schema, expanded: &BTreeSet<String>) -> Vec<DocLine> {
    let mut lines = Vec::new();
    let mut first = true;

    // Enums
    for enum_type in schema.enums.values() {
        if !first {
            lines.push(DocLine {
                target: FocusTarget::Blank,
            });
        }
        first = false;
        lines.push(DocLine {
            target: FocusTarget::Enum(enum_type.name.clone()),
        });
        if enum_type.variants.is_empty() {
            // Single-line enum: `enum name { }` — no close brace line
        } else {
            for (i, _variant) in enum_type.variants.iter().enumerate() {
                lines.push(DocLine {
                    target: FocusTarget::EnumVariant(enum_type.name.clone(), i),
                });
            }
            lines.push(DocLine {
                target: FocusTarget::EnumClose(enum_type.name.clone()),
            });
        }
    }

    // Custom types
    for custom_type in schema.types.values() {
        if !first {
            lines.push(DocLine {
                target: FocusTarget::Blank,
            });
        }
        first = false;

        lines.push(DocLine {
            target: FocusTarget::Type(custom_type.name.clone()),
        });

        if let crate::schema::CustomTypeKind::Composite { fields } = &custom_type.kind {
            if !fields.is_empty() {
                for (i, _) in fields.iter().enumerate() {
                    lines.push(DocLine {
                        target: FocusTarget::TypeField(custom_type.name.clone(), i),
                    });
                }
                lines.push(DocLine {
                    target: FocusTarget::TypeClose(custom_type.name.clone()),
                });
            }
        }
    }

    // Tables
    for table in schema.tables.values() {
        if !first {
            lines.push(DocLine {
                target: FocusTarget::Blank,
            });
        }
        first = false;

        lines.push(DocLine {
            target: FocusTarget::Table(table.name.clone()),
        });

        if expanded.contains(&table.name) {
            let is_empty = table.columns.is_empty()
                && table.constraints.is_empty()
                && table.indexes.is_empty();
            if is_empty {
                // Empty table shows as `table name { }` — single line, no close
            } else {
                // Build the inline constraint sets just like render.rs
                let single_pk_cols = single_column_pk_set(&table.constraints);
                let single_unique_cols = single_column_unique_set(&table.constraints);

                // Columns
                for col in &table.columns {
                    lines.push(DocLine {
                        target: FocusTarget::Column(table.name.clone(), col.name.clone()),
                    });
                }

                // Separate constraints
                let separate_constraints: Vec<usize> = table
                    .constraints
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| {
                        should_render_constraint_separately(c, &single_pk_cols, &single_unique_cols)
                    })
                    .map(|(i, _)| i)
                    .collect();

                if !separate_constraints.is_empty() {
                    lines.push(DocLine {
                        target: FocusTarget::Separator(table.name.clone()),
                    });
                    for &ci in &separate_constraints {
                        lines.push(DocLine {
                            target: FocusTarget::Constraint(table.name.clone(), ci),
                        });
                    }
                }

                // Indexes
                if !table.indexes.is_empty() {
                    if separate_constraints.is_empty() {
                        lines.push(DocLine {
                            target: FocusTarget::Separator(table.name.clone()),
                        });
                    }
                    for (i, _) in table.indexes.iter().enumerate() {
                        lines.push(DocLine {
                            target: FocusTarget::Index(table.name.clone(), i),
                        });
                    }
                }

                lines.push(DocLine {
                    target: FocusTarget::TableClose(table.name.clone()),
                });
            }
        }
    }

    lines
}

/// Collect single-column PK column names (for inline rendering).
fn single_column_pk_set(constraints: &[crate::schema::Constraint]) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for c in constraints {
        if let crate::schema::Constraint::PrimaryKey { columns, .. } = c {
            if columns.len() == 1 {
                set.insert(columns[0].clone());
            }
        }
    }
    set
}

/// Collect single-column unique constraint column names (for inline rendering).
fn single_column_unique_set(constraints: &[crate::schema::Constraint]) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for c in constraints {
        if let crate::schema::Constraint::Unique { columns, .. } = c {
            if columns.len() == 1 {
                set.insert(columns[0].clone());
            }
        }
    }
    set
}

/// Determine if a constraint should be rendered as a separate line.
fn should_render_constraint_separately(
    constraint: &crate::schema::Constraint,
    single_pk_cols: &BTreeSet<String>,
    single_unique_cols: &BTreeSet<String>,
) -> bool {
    match constraint {
        crate::schema::Constraint::PrimaryKey { columns, .. } => {
            columns.len() > 1 || (columns.len() == 1 && !single_pk_cols.contains(&columns[0]))
        }
        crate::schema::Constraint::Unique { columns, .. } => {
            columns.len() > 1 || (columns.len() == 1 && !single_unique_cols.contains(&columns[0]))
        }
        crate::schema::Constraint::ForeignKey { .. } | crate::schema::Constraint::Check { .. } => {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_state() -> AppState {
        let mut schema = Schema::new();
        // Add several tables for navigation testing
        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            schema.add_table(crate::schema::Table::new(name));
        }
        AppState::new(schema, "postgres://user:***@localhost/testdb".into())
    }

    #[test]
    fn initial_state() {
        let state = sample_state();
        assert_eq!(state.mode, Mode::Normal);
        assert_eq!(state.cursor, 0);
        assert_eq!(state.viewport_offset, 0);
        assert!(!state.should_quit);
        assert!(state.command_buf.is_empty());
        assert_eq!(state.pending_key, PendingKey::None);
    }

    #[test]
    fn mode_display() {
        assert_eq!(Mode::Normal.to_string(), "Normal");
        assert_eq!(Mode::Edit.to_string(), "Edit");
        assert_eq!(Mode::Rename.to_string(), "Rename");
        assert_eq!(Mode::Search.to_string(), "Search");
        assert_eq!(Mode::HUD.to_string(), "HUD");
        assert_eq!(Mode::Command.to_string(), "Command");
        assert_eq!(Mode::SpaceMenu.to_string(), "SpaceMenu");
        assert_eq!(Mode::MigrationPreview.to_string(), "MigrationPreview");
    }

    #[test]
    fn cursor_down_clamps() {
        let state = sample_state();
        // 5 tables with blank separators between them = 9 lines
        assert_eq!(state.line_count(), 9);

        let state = state.cursor_down(1);
        assert_eq!(state.cursor, 1);

        let state = state.cursor_down(100);
        assert_eq!(state.cursor, 8); // clamped to last
    }

    #[test]
    fn cursor_up_clamps() {
        let state = sample_state().cursor_to(3);
        assert_eq!(state.cursor, 3);

        let state = state.cursor_up(1);
        assert_eq!(state.cursor, 2);

        let state = state.cursor_up(100);
        assert_eq!(state.cursor, 0); // clamped to first
    }

    #[test]
    fn cursor_to_clamps() {
        let state = sample_state();
        let state = state.cursor_to(100);
        assert_eq!(state.cursor, 8);

        let state = state.cursor_to(2);
        assert_eq!(state.cursor, 2);
    }

    #[test]
    fn scroll_follows_cursor() {
        let state = sample_state().with_viewport_height(3);
        assert_eq!(state.viewport_offset, 0);

        // Move cursor past viewport
        let state = state.cursor_down(4);
        assert_eq!(state.cursor, 4);
        assert_eq!(state.viewport_offset, 2); // cursor at 4, viewport shows 2..5

        // Move cursor back up past viewport
        let state = state.cursor_up(4);
        assert_eq!(state.cursor, 0);
        assert_eq!(state.viewport_offset, 0);
    }

    #[test]
    fn mode_transition_resets_pending_key() {
        let state = sample_state()
            .with_pending_key(PendingKey::G)
            .with_mode(Mode::Command);
        assert_eq!(state.mode, Mode::Command);
        assert_eq!(state.pending_key, PendingKey::None);
    }

    #[test]
    fn command_mode_clears_buffer() {
        let state = sample_state().with_mode(Mode::Command).command_push('q');
        assert_eq!(state.command_buf, "q");

        // Re-entering command mode clears buffer
        let state = state.with_mode(Mode::Normal).with_mode(Mode::Command);
        assert!(state.command_buf.is_empty());
    }

    #[test]
    fn command_buffer_operations() {
        let state = sample_state().with_mode(Mode::Command);
        let state = state.command_push('q').command_push('u').command_push('i');
        assert_eq!(state.command_buf, "qui");

        let state = state.command_pop();
        assert_eq!(state.command_buf, "qu");

        let state = state.command_pop().command_pop().command_pop();
        assert!(state.command_buf.is_empty()); // pop on empty is no-op
    }

    #[test]
    fn quit_flag() {
        let state = sample_state();
        assert!(!state.should_quit);
        let state = state.quit();
        assert!(state.should_quit);
    }

    #[test]
    fn immutable_transitions() {
        let original = sample_state();
        let moved = original.clone().cursor_down(2);
        // Clone ensures original is independent
        assert_eq!(moved.cursor, 2);
    }

    #[test]
    fn empty_schema_line_count() {
        let state = AppState::new(Schema::new(), String::new());
        assert_eq!(state.line_count(), 0);
        // cursor_down on empty should stay at 0
        let state = state.cursor_down(1);
        assert_eq!(state.cursor, 0);
    }

    #[test]
    fn half_page_scroll() {
        let state = sample_state().with_viewport_height(4);
        let half = state.viewport_height / 2;
        let state = state.cursor_down(half);
        assert_eq!(state.cursor, 2);

        let state = state.cursor_down(half);
        assert_eq!(state.cursor, 4);

        let state = state.cursor_up(half);
        assert_eq!(state.cursor, 2);
    }

    #[test]
    fn focus_target_on_collapsed_tables() {
        let state = sample_state();
        // All tables start collapsed. Doc should have table headers with blank lines between.
        assert_eq!(state.focus(), Some(&FocusTarget::Table("alpha".into())));

        let state = state.cursor_down(1);
        assert_eq!(state.focus(), Some(&FocusTarget::Blank));

        let state = state.cursor_down(1);
        assert_eq!(state.focus(), Some(&FocusTarget::Table("bravo".into())));
    }

    #[test]
    fn toggle_expand_collapse() {
        let state = sample_state();
        assert!(state.expanded.is_empty());

        // Toggle expand on "alpha"
        let state = state.toggle_expand();
        assert!(state.expanded.contains("alpha"));

        // Toggle collapse
        let state = state.toggle_expand();
        assert!(!state.expanded.contains("alpha"));
    }

    #[test]
    fn expand_table_with_columns() {
        use crate::schema::types::PgType;
        use crate::schema::{Column, Constraint, Table};

        let mut schema = Schema::new();
        let mut table = Table::new("users");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("name", PgType::Text));
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        schema.add_table(table);

        let state = AppState::new(schema, String::new());
        assert_eq!(state.line_count(), 1); // Just the table header

        // Expand
        let state = state.toggle_expand();
        // Header + id + name + close = 4 lines
        assert_eq!(state.line_count(), 4);
        assert_eq!(state.focus(), Some(&FocusTarget::Table("users".into())));

        // Check doc structure
        assert!(matches!(state.doc[0].target, FocusTarget::Table(_)));
        assert!(matches!(state.doc[1].target, FocusTarget::Column(_, _)));
        assert!(matches!(state.doc[2].target, FocusTarget::Column(_, _)));
        assert!(matches!(state.doc[3].target, FocusTarget::TableClose(_)));
    }

    #[test]
    fn next_prev_table() {
        let state = sample_state();
        assert_eq!(state.cursor, 0);
        assert_eq!(state.focus(), Some(&FocusTarget::Table("alpha".into())));

        let state = state.next_table();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("bravo".into())));

        let state = state.next_table();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("charlie".into())));

        let state = state.prev_table();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("bravo".into())));

        // prev from first table stays
        let state = state.prev_table().prev_table().prev_table();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("alpha".into())));
    }

    #[test]
    fn next_table_at_end_stays() {
        let state = sample_state();
        // Jump to last table (echo)
        let state = state.next_table().next_table().next_table().next_table();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("echo".into())));

        let state = state.next_table();
        // Should stay on echo since no more tables
        assert_eq!(state.focus(), Some(&FocusTarget::Table("echo".into())));
    }

    #[test]
    fn document_with_enums_and_tables() {
        use crate::schema::{EnumType, Table};

        let mut schema = Schema::new();
        schema.add_enum(EnumType {
            name: "mood".into(),
            variants: vec!["happy".into(), "sad".into()],
        });
        schema.add_table(Table::new("users"));

        let state = AppState::new(schema, String::new());
        // enum header + 2 variants + close + blank + table header = 6
        assert_eq!(state.line_count(), 6);
        assert_eq!(state.focus(), Some(&FocusTarget::Enum("mood".into())));
    }

    #[test]
    fn collapse_moves_cursor_to_table_header() {
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        let mut table = Table::new("users");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("name", PgType::Text));
        schema.add_table(table);

        let state = AppState::new(schema, String::new());
        // Expand and navigate to a column
        let state = state.toggle_expand();
        let state = state.cursor_down(1); // now on "id" column
        assert!(matches!(state.focus(), Some(FocusTarget::Column(_, _))));

        // Collapse — cursor should jump to table header
        let state = state.toggle_expand();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("users".into())));
    }
}
