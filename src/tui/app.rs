use std::collections::BTreeSet;
use std::time::Instant;

use strum::Display;

use super::fuzzy::SearchState;
use super::goto::{GotoFocus, GotoTarget};
use super::hud::{HudState, HudStatus};
use crate::migration::loader::MigrationIndex;
use crate::migration::overlay::{EditOverlay, PendingOverlay};
use crate::migration::pattern::MigrationPattern;
use crate::schema::relations::RelationMap;
use crate::schema::type_map::TypeMapper;
use crate::schema::Schema;

/// The TUI application mode. Determines which input handler processes keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum Mode {
    Normal,
    Rename,
    DefaultPrompt,
    Search,
    HUD,
    Command,
    SpaceMenu,
    GotoMenu,
    ChangeMenu,
    MigrationPreview,
    LlmPending,
    LlmPreview,
    Help,
    InDocSearch,
    ChangePreview,
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

/// Identifies the column being given a new default value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultPromptTarget {
    /// The table containing the column.
    pub table: String,
    /// The column name.
    pub column: String,
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
    /// Safety warnings from pre-flight checks.
    /// `None` = checks still running, `Some` = checks complete.
    pub warnings: Option<Vec<crate::migration::warnings::MigrationWarning>>,
}

/// State for the LLM preview overlay.
#[derive(Debug, Clone)]
pub struct LlmPreviewState {
    /// The LLM-generated SQL to display.
    pub sql: String,
    /// What kind of LLM operation produced this preview.
    pub kind: LlmPreviewKind,
    /// Scroll offset in the preview.
    pub scroll: usize,
}

/// What kind of LLM operation is being previewed.
#[derive(Debug, Clone)]
pub enum LlmPreviewKind {
    /// `:ai <prompt>` — edit the migration SQL.
    AiEdit {
        /// The original migration SQL before LLM edit.
        original_sql: String,
        /// The migration description.
        description: String,
    },
    /// `:generate-down` — generate a down migration.
    GenerateDown {
        /// The up migration SQL (for finding the matching .down.sql path).
        up_sql: String,
        /// The migration description.
        description: String,
    },
}

/// State for the change preview overlay (Space d).
#[derive(Debug, Clone)]
pub struct ChangePreviewState {
    /// Human-readable summary lines of changes.
    pub summary: Vec<String>,
    /// Generated migration SQL (if any).
    pub sql: Option<String>,
    /// Whether to show SQL view (toggled with `s`).
    pub show_sql: bool,
    /// Scroll offset.
    pub scroll: usize,
}

/// Pending key state for multi-key sequences (e.g. `gg`, `g r`, `]g`, `[g`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingKey {
    None,
    G,
    CloseBracket,
    OpenBracket,
}

/// Direction for in-document search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchDirection {
    Forward,
    Backward,
}

/// State for in-document text search (`/` and `?`).
#[derive(Debug, Clone)]
pub struct InDocSearchState {
    /// The search query string.
    pub query: String,
    /// Search direction (forward or backward).
    pub direction: SearchDirection,
    /// Document line indices that match the query (sorted ascending).
    pub matches: Vec<usize>,
    /// Index into `matches` for the current/highlighted match.
    pub current: Option<usize>,
    /// Cursor position when search was initiated (for relative navigation).
    pub origin_cursor: usize,
}

/// Maximum number of entries in the undo/redo stacks.
const UNDO_STACK_MAX: usize = 100;

/// A snapshot of the schema editing state at a point in time.
#[derive(Debug, Clone)]
pub struct UndoSnapshot {
    pub schema: Schema,
    pub original_schema: Option<Schema>,
    pub renames: Vec<RenameMetadata>,
    pub edited_tables: BTreeSet<String>,
}

/// Classic two-stack undo/redo history for schema editing actions.
///
/// On each edit, the pre-edit state is pushed to `undo_stack` and `redo_stack`
/// is cleared. Undo pops from `undo_stack` and pushes to `redo_stack`. Redo
/// does the reverse.
#[derive(Debug, Clone)]
pub struct UndoHistory {
    undo_stack: Vec<UndoSnapshot>,
    redo_stack: Vec<UndoSnapshot>,
}

impl Default for UndoHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl UndoHistory {
    pub fn new() -> Self {
        Self {
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
        }
    }

    /// Record the current state before an edit. Clears redo history.
    pub fn push(&mut self, snapshot: UndoSnapshot) {
        self.redo_stack.clear();
        self.undo_stack.push(snapshot);
        if self.undo_stack.len() > UNDO_STACK_MAX {
            self.undo_stack.remove(0);
        }
    }

    /// Pop the most recent undo snapshot. Caller must push current state
    /// to `push_redo` separately.
    pub fn pop_undo(&mut self) -> Option<UndoSnapshot> {
        self.undo_stack.pop()
    }

    /// Push current state to the redo stack (called during undo).
    pub fn push_redo(&mut self, snapshot: UndoSnapshot) {
        self.redo_stack.push(snapshot);
    }

    /// Pop the most recent redo snapshot. Caller must push current state
    /// to undo stack separately.
    pub fn pop_redo(&mut self) -> Option<UndoSnapshot> {
        self.redo_stack.pop()
    }

    /// Push current state to the undo stack (called during redo).
    pub fn push_undo(&mut self, snapshot: UndoSnapshot) {
        self.undo_stack.push(snapshot);
        if self.undo_stack.len() > UNDO_STACK_MAX {
            self.undo_stack.remove(0);
        }
    }

    pub fn can_undo(&self) -> bool {
        !self.undo_stack.is_empty()
    }

    pub fn can_redo(&self) -> bool {
        !self.redo_stack.is_empty()
    }

    /// Clear all history (e.g., after writing a migration).
    pub fn clear(&mut self) {
        self.undo_stack.clear();
        self.redo_stack.clear();
    }
}

/// Maximum number of entries in the jump list.
const JUMP_LIST_MAX: usize = 100;

/// An entry in the navigation jump list.
#[derive(Debug, Clone)]
pub struct JumpEntry {
    /// Cursor position at the time of the jump.
    pub cursor: usize,
    /// Focus target at the time of the jump (for robust restoration after doc rebuilds).
    pub target: FocusTarget,
}

/// Navigation history for jump-back/forward (like vim's jump list).
///
/// Tracks positions visited via jump operations (goto, search, tab).
/// `cursor` is always in `0..=entries.len()`. When `cursor == entries.len()`,
/// the user is at the "present" position (not in the history).
#[derive(Debug, Clone)]
pub struct JumpList {
    entries: Vec<JumpEntry>,
    cursor: usize,
}

impl Default for JumpList {
    fn default() -> Self {
        Self::new()
    }
}

impl JumpList {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            cursor: 0,
        }
    }

    /// Record the current position before a jump.
    ///
    /// If in the middle of the list, forward entries are truncated
    /// (like browser history when navigating to a new page).
    pub fn record(&mut self, entry: JumpEntry) {
        self.entries.truncate(self.cursor);
        self.entries.push(entry);
        if self.entries.len() > JUMP_LIST_MAX {
            self.entries.remove(0);
        }
        self.cursor = self.entries.len();
    }

    /// Move backward in the jump list.
    ///
    /// On first backward from the present, saves `current` so Ctrl-i can return.
    pub fn go_back(&mut self, current: JumpEntry) -> Option<JumpEntry> {
        if self.entries.is_empty() {
            return None;
        }
        if self.cursor == self.entries.len() {
            // At the present — save current position so forward can return here
            self.entries.push(current);
            if self.entries.len() < 2 {
                return None;
            }
            self.cursor = self.entries.len() - 2;
        } else if self.cursor > 0 {
            self.cursor -= 1;
        } else {
            return None;
        }
        Some(self.entries[self.cursor].clone())
    }

    /// Move forward in the jump list.
    pub fn go_forward(&mut self) -> Option<JumpEntry> {
        if self.cursor + 1 < self.entries.len() {
            self.cursor += 1;
            Some(self.entries[self.cursor].clone())
        } else {
            None
        }
    }
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

    /// Returns the parent node name for any expandable target (table, enum, or type).
    pub fn node_name(&self) -> Option<&str> {
        match self {
            FocusTarget::Table(n)
            | FocusTarget::Column(n, _)
            | FocusTarget::Separator(n)
            | FocusTarget::Constraint(n, _)
            | FocusTarget::Index(n, _)
            | FocusTarget::TableClose(n)
            | FocusTarget::Enum(n)
            | FocusTarget::EnumVariant(n, _)
            | FocusTarget::EnumClose(n)
            | FocusTarget::Type(n)
            | FocusTarget::TypeField(n, _)
            | FocusTarget::TypeClose(n) => Some(n),
            FocusTarget::Blank => None,
        }
    }
}

/// A single line in the document with its focus target.
#[derive(Debug, Clone)]
pub struct DocLine {
    pub target: FocusTarget,
    /// Whether this line represents a removed element (ghost line).
    pub ghost: bool,
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
    /// DefaultPrompt mode: the input buffer for the default expression.
    pub default_prompt_buf: String,
    /// DefaultPrompt mode: identifies the column being edited.
    pub default_prompt_target: Option<DefaultPromptTarget>,
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
    /// Migration file index for migration-aware browsing.
    pub migration_index: MigrationIndex,
    /// Migration preview state (present when mode is MigrationPreview).
    pub migration_preview: Option<MigrationPreviewState>,
    /// Transient status message shown in the status bar (e.g., "no references found").
    pub status_message: Option<String>,
    /// When the pending key was set (for timeout).
    pub pending_key_time: Option<Instant>,
    /// PG→language type mapper (language-aware, with user overrides).
    pub type_mapper: TypeMapper,
    /// Whether to show language-specific type annotations alongside PG types.
    pub show_language_types: bool,
    /// Pending migrations overlay state (present when overlay is active).
    pub pending_overlay: Option<PendingOverlay>,
    /// Whether the pending overlay is currently visible.
    pub show_pending_overlay: bool,
    /// LLM preview state (present when mode is LlmPreview).
    pub llm_preview: Option<LlmPreviewState>,
    /// Pending LLM operation description (shown during LlmPending mode).
    pub llm_pending_message: Option<String>,
    /// The mode the user was in before entering Help mode (for showing
    /// the correct keybindings).
    pub help_source_mode: Mode,
    /// In-document search state (persists across mode transitions for n/N navigation).
    pub in_doc_search: Option<InDocSearchState>,
    /// Navigation jump list for Ctrl-o (back) / Ctrl-i (forward).
    pub jump_list: JumpList,
    /// Change preview state (present when mode is ChangePreview).
    pub change_preview: Option<ChangePreviewState>,
    /// Edit overlay — computed from diff(original_schema, current_schema).
    pub edit_overlay: Option<EditOverlay>,
    /// Whether to show edit change markers in the gutter (default: true).
    pub show_edit_changes: bool,
    /// Resolved migrations directory (None if not found or no .sql files).
    pub migrations_dir: Option<std::path::PathBuf>,
    /// Detected migration naming pattern for conforming filename generation.
    pub migration_pattern: MigrationPattern,
    /// Undo/redo history for schema editing actions.
    pub undo_history: UndoHistory,
}

impl AppState {
    /// Create a new application state from a loaded schema.
    pub fn new(
        schema: Schema,
        connection_info: String,
        migrations_dir: Option<std::path::PathBuf>,
    ) -> Self {
        let expanded = BTreeSet::new();
        let doc = build_document(&schema, &expanded);
        let relation_map = RelationMap::build(&schema);
        let migration_pattern = migrations_dir
            .as_deref()
            .map(crate::migration::pattern::detect_pattern)
            .unwrap_or_default();
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
            default_prompt_buf: String::new(),
            default_prompt_target: None,
            rename_buf: String::new(),
            rename_target: None,
            renames: Vec::new(),
            edited_tables: BTreeSet::new(),
            hud: None,
            relation_map,
            migration_index: MigrationIndex::default(),
            migration_preview: None,
            status_message: None,
            pending_key_time: None,
            type_mapper: TypeMapper::new(),
            show_language_types: false,
            pending_overlay: None,
            show_pending_overlay: false,
            llm_preview: None,
            llm_pending_message: None,
            help_source_mode: Mode::Normal,
            in_doc_search: None,
            jump_list: JumpList::new(),
            change_preview: None,
            edit_overlay: None,
            show_edit_changes: true,
            migrations_dir,
            migration_pattern,
            undo_history: UndoHistory::new(),
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
        if mode == Mode::DefaultPrompt {
            self.default_prompt_buf = String::new();
        }
        if mode != Mode::DefaultPrompt {
            self.default_prompt_target = None;
        }
        if mode != Mode::HUD {
            self.hud = None;
        }
        if mode != Mode::MigrationPreview {
            self.migration_preview = None;
        }
        if mode != Mode::LlmPreview {
            self.llm_preview = None;
        }
        if mode != Mode::LlmPending {
            self.llm_pending_message = None;
        }
        if mode != Mode::ChangePreview {
            self.change_preview = None;
        }
        // Clear status message on any mode transition
        self.status_message = None;
        self
    }

    /// Enter search mode with the given filter.
    pub fn enter_search(mut self, filter: super::fuzzy::SearchFilter) -> Self {
        self.search = Some(SearchState::new(
            &self.schema,
            &self.migration_index,
            filter,
        ));
        self.mode = Mode::Search;
        self.pending_key = PendingKey::None;
        self
    }

    /// Set the migration index.
    pub fn with_migration_index(mut self, index: MigrationIndex) -> Self {
        self.migration_index = index;
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
                if !self.expanded.contains(name) {
                    self.expanded.insert(name.clone());
                    self.rebuild_doc();
                }
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Enum(name.clone()))
                {
                    self.cursor = pos;
                }
            }
            GotoFocus::Type(name) => {
                if !self.expanded.contains(name) {
                    self.expanded.insert(name.clone());
                    self.rebuild_doc();
                }
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Type(name.clone()))
                {
                    self.cursor = pos;
                }
            }
        }
        self.scroll_to_focus()
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
                if !self.expanded.contains(&symbol.display) {
                    self.expanded.insert(symbol.display.clone());
                    self.rebuild_doc();
                }
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Enum(symbol.display.clone()))
                {
                    self.cursor = pos;
                }
            }
            SymbolKind::Type => {
                if !self.expanded.contains(&symbol.display) {
                    self.expanded.insert(symbol.display.clone());
                    self.rebuild_doc();
                }
                if let Some(pos) = self
                    .doc
                    .iter()
                    .position(|l| l.target == FocusTarget::Type(symbol.display.clone()))
                {
                    self.cursor = pos;
                }
            }
            SymbolKind::Migration => {
                // Migrations are informational in search — no document position to jump to
            }
        }

        self.scroll_to_focus()
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

    /// Returns true if the schema has diverged from the original.
    ///
    /// Uses structural comparison so the check is robust regardless of
    /// how edits were made (quick actions, external editor, AI, etc.).
    pub fn has_edits(&self) -> bool {
        match &self.original_schema {
            Some(original) => original != &self.schema,
            None => false,
        }
    }

    /// Clear edit tracking state after a migration has been written.
    /// Sets original_schema = current schema, clears renames and edited_tables.
    pub fn clear_edit_state(mut self) -> Self {
        self.original_schema = Some(self.schema.clone());
        self.renames.clear();
        self.edited_tables.clear();
        self.edit_overlay = None;
        self.undo_history.clear();
        self
    }

    /// Take a snapshot of the current editing state for undo history.
    ///
    /// Call this **before** mutating the schema so the snapshot captures
    /// the state to restore on undo.
    pub fn push_undo_snapshot(&mut self) {
        self.undo_history.push(UndoSnapshot {
            schema: self.schema.clone(),
            original_schema: self.original_schema.clone(),
            renames: self.renames.clone(),
            edited_tables: self.edited_tables.clone(),
        });
    }

    /// Undo the last schema editing action.
    pub fn undo(mut self) -> Self {
        let current = UndoSnapshot {
            schema: self.schema.clone(),
            original_schema: self.original_schema.clone(),
            renames: self.renames.clone(),
            edited_tables: self.edited_tables.clone(),
        };

        if let Some(snapshot) = self.undo_history.pop_undo() {
            self.undo_history.push_redo(current);

            self.schema = snapshot.schema;
            self.original_schema = snapshot.original_schema;
            self.renames = snapshot.renames;
            self.edited_tables = snapshot.edited_tables;
            self.rebuild_doc();
            self.recompute_edit_overlay();

            // If schema matches original, clear edit state entirely
            if !self.has_edits() {
                self.original_schema = None;
                self.edit_overlay = None;
            }

            self.with_status("Undo")
        } else {
            self.with_status("Already at oldest change")
        }
    }

    /// Redo a previously undone schema editing action.
    pub fn redo(mut self) -> Self {
        let current = UndoSnapshot {
            schema: self.schema.clone(),
            original_schema: self.original_schema.clone(),
            renames: self.renames.clone(),
            edited_tables: self.edited_tables.clone(),
        };

        if let Some(snapshot) = self.undo_history.pop_redo() {
            self.undo_history.push_undo(current);

            self.schema = snapshot.schema;
            self.original_schema = snapshot.original_schema;
            self.renames = snapshot.renames;
            self.edited_tables = snapshot.edited_tables;
            self.rebuild_doc();
            self.recompute_edit_overlay();
            self.with_status("Redo")
        } else {
            self.with_status("Already at newest change")
        }
    }

    /// Recompute the edit overlay from the diff between original and current schema.
    pub fn recompute_edit_overlay(&mut self) {
        use crate::schema::diff::Rename;

        self.edit_overlay = self.original_schema.as_ref().map(|original| {
            let renames: Vec<Rename> = self
                .renames
                .iter()
                .map(|r| Rename {
                    table: r.table.clone(),
                    from: r.from.clone(),
                    to: r.to.clone(),
                })
                .collect();
            EditOverlay::compute(original, &self.schema, &renames)
        });
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

    /// Toggle language-specific type annotation display on/off.
    pub fn toggle_language_types(mut self) -> Self {
        self.show_language_types = !self.show_language_types;
        self
    }

    /// Enter in-document search mode.
    pub fn enter_in_doc_search(mut self, direction: SearchDirection) -> Self {
        self.in_doc_search = Some(InDocSearchState {
            query: String::new(),
            direction,
            matches: Vec::new(),
            current: None,
            origin_cursor: self.cursor,
        });
        self.mode = Mode::InDocSearch;
        self.pending_key = PendingKey::None;
        self
    }

    /// Compute search match line indices for the given query (case-insensitive).
    fn compute_search_matches(&self, query: &str) -> Vec<usize> {
        let query_lower = query.to_lowercase();
        self.doc
            .iter()
            .enumerate()
            .filter(|(_, line)| {
                let text = super::view::line_plain_text(self, &line.target);
                text.to_lowercase().contains(&query_lower)
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Recompute in-document search matches after query changes.
    ///
    /// Uses `view::line_plain_text` to extract searchable text from each line.
    pub fn recompute_search_matches(mut self) -> Self {
        let search = match self.in_doc_search.take() {
            Some(s) => s,
            None => return self,
        };

        if search.query.is_empty() {
            self.in_doc_search = Some(InDocSearchState {
                matches: Vec::new(),
                current: None,
                ..search
            });
            return self;
        }

        let matches = self.compute_search_matches(&search.query);

        let current = if matches.is_empty() {
            None
        } else {
            match search.direction {
                SearchDirection::Forward => {
                    let idx = matches
                        .iter()
                        .position(|&m| m >= search.origin_cursor)
                        .unwrap_or(0);
                    Some(idx)
                }
                SearchDirection::Backward => {
                    let idx = matches
                        .iter()
                        .rposition(|&m| m <= search.origin_cursor)
                        .unwrap_or(matches.len() - 1);
                    Some(idx)
                }
            }
        };

        // Move cursor to current match for incremental feedback
        if let Some(idx) = current {
            if let Some(&line_pos) = matches.get(idx) {
                self.cursor = line_pos;
            }
        }

        self.in_doc_search = Some(InDocSearchState {
            matches,
            current,
            ..search
        });
        self.scroll_to_cursor()
    }

    /// Jump to the next in-document search match (wrapping).
    pub fn next_search_match(mut self) -> Self {
        let search = match self.in_doc_search.take() {
            Some(s) => s,
            None => return self,
        };

        if search.query.is_empty() {
            self.in_doc_search = Some(search);
            return self;
        }

        let matches = self.compute_search_matches(&search.query);

        if matches.is_empty() {
            self.in_doc_search = Some(InDocSearchState {
                matches,
                current: None,
                ..search
            });
            return self.with_status("pattern not found");
        }

        // Find next match after cursor (wrapping)
        let next = matches.iter().position(|&m| m > self.cursor).unwrap_or(0);
        self.cursor = matches[next];

        let match_num = next + 1;
        let total = matches.len();
        self.in_doc_search = Some(InDocSearchState {
            matches,
            current: Some(next),
            ..search
        });
        self.with_status(format!("[{match_num}/{total}]"))
            .scroll_to_focus()
    }

    /// Jump to the previous in-document search match (wrapping).
    pub fn prev_search_match(mut self) -> Self {
        let search = match self.in_doc_search.take() {
            Some(s) => s,
            None => return self,
        };

        if search.query.is_empty() {
            self.in_doc_search = Some(search);
            return self;
        }

        let matches = self.compute_search_matches(&search.query);

        if matches.is_empty() {
            self.in_doc_search = Some(InDocSearchState {
                matches,
                current: None,
                ..search
            });
            return self.with_status("pattern not found");
        }

        // Find previous match before cursor (wrapping)
        let prev = matches
            .iter()
            .rposition(|&m| m < self.cursor)
            .unwrap_or(matches.len() - 1);
        self.cursor = matches[prev];

        let match_num = prev + 1;
        let total = matches.len();
        self.in_doc_search = Some(InDocSearchState {
            matches,
            current: Some(prev),
            ..search
        });
        self.with_status(format!("[{match_num}/{total}]"))
            .scroll_to_focus()
    }

    /// Set the pending overlay data.
    pub fn with_pending_overlay(mut self, overlay: Option<PendingOverlay>) -> Self {
        self.pending_overlay = overlay;
        self
    }

    /// Toggle the pending overlay visibility on/off.
    pub fn toggle_pending_overlay(mut self) -> Self {
        self.show_pending_overlay = !self.show_pending_overlay;
        self
    }

    /// Record the current position as a jump point (call before executing a jump).
    pub fn record_jump(mut self) -> Self {
        if let Some(target) = self.focus().cloned() {
            self.jump_list.record(JumpEntry {
                cursor: self.cursor,
                target,
            });
        }
        self
    }

    /// Jump backward in the jump list (Ctrl-o).
    pub fn jump_back(mut self) -> Self {
        let current = match self.focus().cloned() {
            Some(target) => JumpEntry {
                cursor: self.cursor,
                target,
            },
            None => return self,
        };
        if let Some(entry) = self.jump_list.go_back(current) {
            self.restore_jump(&entry)
        } else {
            self
        }
    }

    /// Jump forward in the jump list (Ctrl-i).
    pub fn jump_forward(mut self) -> Self {
        if let Some(entry) = self.jump_list.go_forward() {
            self.restore_jump(&entry)
        } else {
            self
        }
    }

    /// Restore cursor position from a jump entry.
    fn restore_jump(mut self, entry: &JumpEntry) -> Self {
        // For Blank targets, use cursor position directly (all Blanks are identical)
        if entry.target == FocusTarget::Blank {
            let max = self.line_count().saturating_sub(1);
            self.cursor = entry.cursor.min(max);
            return self.scroll_to_cursor();
        }
        // Try to find the target in the current doc
        if let Some(pos) = self.doc.iter().position(|l| l.target == entry.target) {
            self.cursor = pos;
        } else {
            // Fallback to cursor position, clamped
            let max = self.line_count().saturating_sub(1);
            self.cursor = entry.cursor.min(max);
        }
        self.scroll_to_cursor()
    }

    /// Toggle expand/collapse for the node (table, enum, or type) under the cursor.
    pub fn toggle_expand(mut self) -> Self {
        let node_name = match self.focus() {
            Some(target) => target.node_name().map(|s| s.to_string()),
            None => None,
        };
        if let Some(name) = node_name {
            if self.expanded.contains(&name) {
                self.expanded.remove(&name);
            } else {
                self.expanded.insert(name);
            }
            self.rebuild_doc();
        }
        self
    }

    /// Names of all expandable nodes (tables, enums, types).
    fn all_node_names(&self) -> BTreeSet<String> {
        self.schema
            .enums
            .keys()
            .chain(self.schema.types.keys())
            .chain(self.schema.tables.keys())
            .cloned()
            .collect()
    }

    /// Toggle expand/collapse for all nodes (tables, enums, types).
    ///
    /// If any node is collapsed, expand all. If all are already expanded, collapse all.
    pub fn toggle_expand_all(mut self) -> Self {
        let all_names = self.all_node_names();
        if all_names.iter().all(|n| self.expanded.contains(n)) {
            self.expanded.clear();
        } else {
            self.expanded = all_names;
        }
        self.rebuild_doc();
        self
    }

    /// Expand all nodes (tables, enums, types).
    pub fn expand_all(mut self) -> Self {
        self.expanded = self.all_node_names();
        self.rebuild_doc();
        self
    }

    /// Collapse all nodes (tables, enums, types).
    pub fn collapse_all(mut self) -> Self {
        self.expanded.clear();
        self.rebuild_doc();
        self
    }

    /// Jump cursor to the next line with an edit change marker.
    pub fn next_change(mut self) -> Self {
        if self.edit_overlay.is_none() {
            return self.with_status("No edit changes");
        }
        for i in (self.cursor + 1)..self.doc.len() {
            if self.has_edit_marker(i) {
                self.cursor = i;
                return self.scroll_to_focus();
            }
        }
        // Wrap to beginning
        for i in 0..self.cursor {
            if self.has_edit_marker(i) {
                self.cursor = i;
                return self.scroll_to_focus();
            }
        }
        self.with_status("No more changes")
    }

    /// Jump cursor to the previous line with an edit change marker.
    pub fn prev_change(mut self) -> Self {
        if self.edit_overlay.is_none() {
            return self.with_status("No edit changes");
        }
        if self.cursor > 0 {
            for i in (0..self.cursor).rev() {
                if self.has_edit_marker(i) {
                    self.cursor = i;
                    return self.scroll_to_focus();
                }
            }
        }
        // Wrap to end
        for i in (self.cursor + 1..self.doc.len()).rev() {
            if self.has_edit_marker(i) {
                self.cursor = i;
                return self.scroll_to_focus();
            }
        }
        self.with_status("No more changes")
    }

    /// Check if a document line has an edit change marker.
    fn has_edit_marker(&self, index: usize) -> bool {
        let Some(doc_line) = self.doc.get(index) else {
            return false;
        };
        if doc_line.ghost {
            return true;
        }
        let Some(overlay) = &self.edit_overlay else {
            return false;
        };
        match &doc_line.target {
            FocusTarget::Table(name)
            | FocusTarget::TableClose(name)
            | FocusTarget::Separator(name) => overlay.table_marker(name).is_some(),
            FocusTarget::Column(table, col) => overlay.column_marker(table, col).is_some(),
            FocusTarget::Constraint(table, _) | FocusTarget::Index(table, _) => {
                overlay.table_marker(table).is_some()
            }
            _ => false,
        }
    }

    /// Jump cursor to the next table header.
    pub fn next_table(mut self) -> Self {
        for i in (self.cursor + 1)..self.doc.len() {
            if matches!(self.doc[i].target, FocusTarget::Table(_)) {
                self.cursor = i;
                return self.scroll_to_focus();
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
                return self.scroll_to_focus();
            }
        }
        self
    }

    /// Rebuild the document after expand/collapse or schema changes, preserving cursor context.
    pub fn rebuild_doc(&mut self) {
        let old_target = self.focus().cloned();
        if self.show_edit_changes {
            self.doc = build_document_with_ghosts(
                &self.schema,
                &self.expanded,
                self.edit_overlay.as_ref(),
            );
        } else {
            self.doc = build_document(&self.schema, &self.expanded);
        }
        self.relation_map = RelationMap::build(&self.schema);

        // Try to find the same target in the new doc
        if let Some(ref target) = old_target {
            // First, try to find the exact same target
            if let Some(pos) = self.doc.iter().position(|l| &l.target == target) {
                self.cursor = pos;
                return;
            }

            // Fall back: for sub-node targets (e.g. after collapse), jump to the parent header
            let fallback = match target {
                FocusTarget::Column(t, _)
                | FocusTarget::Separator(t)
                | FocusTarget::Constraint(t, _)
                | FocusTarget::Index(t, _)
                | FocusTarget::TableClose(t) => Some(FocusTarget::Table(t.clone())),
                FocusTarget::EnumVariant(n, _) | FocusTarget::EnumClose(n) => {
                    Some(FocusTarget::Enum(n.clone()))
                }
                FocusTarget::TypeField(n, _) | FocusTarget::TypeClose(n) => {
                    Some(FocusTarget::Type(n.clone()))
                }
                _ => None,
            };

            if let Some(ref fb) = fallback {
                if let Some(pos) = self.doc.iter().position(|l| &l.target == fb) {
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

    /// Smart scroll for goto/jump operations.
    ///
    /// Priority:
    /// 1. Center cursor — if full node also fits in view, this is ideal.
    /// 2. Show full node (centered in viewport) — cursor is always inside.
    /// 3. Center cursor as fallback when node is too large for viewport.
    fn scroll_to_focus(mut self) -> Self {
        if self.viewport_height == 0 {
            return self;
        }

        let (node_start, node_end) = self.node_range_at(self.cursor);
        let vh = self.viewport_height;
        let max_offset = self.doc.len().saturating_sub(vh);

        // Centered offset for cursor
        let centered = self.cursor.saturating_sub(vh / 2).min(max_offset);

        // 1. Center cursor — check if full node is visible at that position
        if node_start >= centered && node_end < centered + vh {
            self.viewport_offset = centered;
            return self;
        }

        // 2. Show full node centered in viewport (cursor is always inside the node)
        let node_height = node_end - node_start + 1;
        if node_height <= vh {
            let offset = node_start
                .saturating_sub((vh - node_height) / 2)
                .min(max_offset);
            self.viewport_offset = offset;
            return self;
        }

        // 3. Fallback: center cursor (node too large for viewport)
        self.viewport_offset = centered;
        self
    }

    /// Find the range (start, end inclusive) of the block containing the given line.
    ///
    /// Blocks are separated by `Blank` lines. Returns `(pos, pos)` for blank
    /// lines or out-of-bounds positions.
    fn node_range_at(&self, pos: usize) -> (usize, usize) {
        if self.doc.is_empty() || pos >= self.doc.len() {
            return (pos, pos);
        }

        if matches!(self.doc[pos].target, FocusTarget::Blank) {
            return (pos, pos);
        }

        // A collapsed table/enum/type (single-line header with no expanded body)
        // forms its own single-line block even without surrounding blanks.
        if self.is_single_line_block(pos) {
            return (pos, pos);
        }

        // Search backward for block start (line after a Blank, or start of doc)
        let start = (0..pos)
            .rev()
            .find(|&i| matches!(self.doc[i].target, FocusTarget::Blank))
            .map(|i| i + 1)
            .unwrap_or(0);

        // Search forward for block end (line before a Blank, or end of doc)
        let end = ((pos + 1)..self.doc.len())
            .find(|&i| matches!(self.doc[i].target, FocusTarget::Blank))
            .map(|i| i - 1)
            .unwrap_or(self.doc.len() - 1);

        (start, end)
    }

    /// Returns true when the line at `pos` is a single-line block header
    /// (e.g. a collapsed table, empty enum, or single-line type).
    fn is_single_line_block(&self, pos: usize) -> bool {
        match &self.doc[pos].target {
            FocusTarget::Table(name) => {
                // Collapsed if the next line is NOT part of this table's body
                pos + 1 >= self.doc.len()
                    || !matches!(
                        &self.doc[pos + 1].target,
                        FocusTarget::Column(n, _) | FocusTarget::TableClose(n) if n == name
                    )
            }
            FocusTarget::Enum(name) => {
                pos + 1 >= self.doc.len()
                    || !matches!(
                        &self.doc[pos + 1].target,
                        FocusTarget::EnumVariant(n, _) if n == name
                    )
            }
            FocusTarget::Type(name) => {
                pos + 1 >= self.doc.len()
                    || !matches!(
                        &self.doc[pos + 1].target,
                        FocusTarget::TypeField(n, _) if n == name
                    )
            }
            _ => false,
        }
    }
}

/// Build the flat document model from a schema and expanded set.
///
/// The document is a list of lines, one per visible element. Collapsed nodes
/// show a single summary line. Expanded nodes show all internal lines.
/// All node types (enums, types, tables) support collapse/expand.
///
/// Blank separators are inserted only when the previous block was multi-line,
/// so expanding a node doesn't shift its header down.
pub fn build_document(schema: &Schema, expanded: &BTreeSet<String>) -> Vec<DocLine> {
    build_document_with_ghosts(schema, expanded, None)
}

/// Build the document model, optionally inserting ghost lines for removed elements.
///
/// When `edit_overlay` is provided, ghost lines are inserted for dropped columns
/// and dropped tables so the user can see (and revert) removals.
pub fn build_document_with_ghosts(
    schema: &Schema,
    expanded: &BTreeSet<String>,
    edit_overlay: Option<&EditOverlay>,
) -> Vec<DocLine> {
    let mut lines = Vec::new();
    // Track whether the previous block spanned multiple lines.
    // A blank separator is inserted only when the previous block was
    // multi-line, so expanding a node doesn't shift its header down.
    let mut prev_multiline: Option<bool> = None;

    // Enums
    for enum_type in schema.enums.values() {
        let is_expanded = expanded.contains(&enum_type.name);
        let is_multiline = is_expanded && !enum_type.variants.is_empty();
        if let Some(prev) = prev_multiline {
            if prev {
                lines.push(DocLine {
                    target: FocusTarget::Blank,
                    ghost: false,
                });
            }
        }
        prev_multiline = Some(is_multiline);
        lines.push(DocLine {
            target: FocusTarget::Enum(enum_type.name.clone()),
            ghost: false,
        });
        if is_expanded {
            if enum_type.variants.is_empty() {
                // Single-line enum: `enum name { }` — no close brace line
            } else {
                for (i, _variant) in enum_type.variants.iter().enumerate() {
                    lines.push(DocLine {
                        target: FocusTarget::EnumVariant(enum_type.name.clone(), i),
                        ghost: false,
                    });
                }
                lines.push(DocLine {
                    target: FocusTarget::EnumClose(enum_type.name.clone()),
                    ghost: false,
                });
            }
        }
    }

    // Custom types
    for custom_type in schema.types.values() {
        let is_expanded = expanded.contains(&custom_type.name);
        let is_multiline = is_expanded
            && matches!(
                &custom_type.kind,
                crate::schema::CustomTypeKind::Composite { fields } if !fields.is_empty()
            );
        if let Some(prev) = prev_multiline {
            if prev {
                lines.push(DocLine {
                    target: FocusTarget::Blank,
                    ghost: false,
                });
            }
        }
        prev_multiline = Some(is_multiline);

        lines.push(DocLine {
            target: FocusTarget::Type(custom_type.name.clone()),
            ghost: false,
        });

        if is_expanded {
            if let crate::schema::CustomTypeKind::Composite { fields } = &custom_type.kind {
                if !fields.is_empty() {
                    for (i, _) in fields.iter().enumerate() {
                        lines.push(DocLine {
                            target: FocusTarget::TypeField(custom_type.name.clone(), i),
                            ghost: false,
                        });
                    }
                    lines.push(DocLine {
                        target: FocusTarget::TypeClose(custom_type.name.clone()),
                        ghost: false,
                    });
                }
            }
        }
    }

    // Collect dropped table names from edit overlay for ghost table headers
    let dropped_tables: Vec<String> = edit_overlay
        .iter()
        .flat_map(|ov| ov.changes.iter())
        .filter_map(|c| match c {
            crate::schema::diff::Change::DropTable(name) => Some(name.clone()),
            _ => None,
        })
        .collect();

    // Tables
    for table in schema.tables.values() {
        let is_expanded = expanded.contains(&table.name);
        let is_empty =
            table.columns.is_empty() && table.constraints.is_empty() && table.indexes.is_empty();
        let is_multiline = is_expanded && !is_empty;
        if let Some(prev) = prev_multiline {
            if prev {
                lines.push(DocLine {
                    target: FocusTarget::Blank,
                    ghost: false,
                });
            }
        }
        prev_multiline = Some(is_multiline);

        lines.push(DocLine {
            target: FocusTarget::Table(table.name.clone()),
            ghost: false,
        });

        if is_expanded {
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
                        ghost: false,
                    });
                }

                // Ghost lines for dropped columns (from edit overlay)
                if let Some(overlay) = edit_overlay {
                    for change in &overlay.changes {
                        if let crate::schema::diff::Change::DropColumn {
                            table: t,
                            column: c,
                        } = change
                        {
                            if t == &table.name {
                                lines.push(DocLine {
                                    target: FocusTarget::Column(table.name.clone(), c.clone()),
                                    ghost: true,
                                });
                            }
                        }
                    }
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
                        ghost: false,
                    });
                    for &ci in &separate_constraints {
                        lines.push(DocLine {
                            target: FocusTarget::Constraint(table.name.clone(), ci),
                            ghost: false,
                        });
                    }
                }

                // Indexes
                if !table.indexes.is_empty() {
                    if separate_constraints.is_empty() {
                        lines.push(DocLine {
                            target: FocusTarget::Separator(table.name.clone()),
                            ghost: false,
                        });
                    }
                    for (i, _) in table.indexes.iter().enumerate() {
                        lines.push(DocLine {
                            target: FocusTarget::Index(table.name.clone(), i),
                            ghost: false,
                        });
                    }
                }

                lines.push(DocLine {
                    target: FocusTarget::TableClose(table.name.clone()),
                    ghost: false,
                });
            }
        }
    }

    // Ghost lines for dropped tables
    for dropped_name in &dropped_tables {
        // Ghost tables are single-line; use same prev_multiline logic
        if let Some(prev) = prev_multiline {
            if prev {
                lines.push(DocLine {
                    target: FocusTarget::Blank,
                    ghost: false,
                });
            }
        }
        prev_multiline = Some(false);
        lines.push(DocLine {
            target: FocusTarget::Table(dropped_name.clone()),
            ghost: true,
        });
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
        AppState::new(schema, "postgres://user:***@localhost/testdb".into(), None)
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
        assert_eq!(Mode::Rename.to_string(), "Rename");
        assert_eq!(Mode::DefaultPrompt.to_string(), "DefaultPrompt");
        assert_eq!(Mode::Search.to_string(), "Search");
        assert_eq!(Mode::HUD.to_string(), "HUD");
        assert_eq!(Mode::Command.to_string(), "Command");
        assert_eq!(Mode::SpaceMenu.to_string(), "SpaceMenu");
        assert_eq!(Mode::GotoMenu.to_string(), "GotoMenu");
        assert_eq!(Mode::ChangeMenu.to_string(), "ChangeMenu");
        assert_eq!(Mode::MigrationPreview.to_string(), "MigrationPreview");
        assert_eq!(Mode::LlmPending.to_string(), "LlmPending");
        assert_eq!(Mode::LlmPreview.to_string(), "LlmPreview");
        assert_eq!(Mode::Help.to_string(), "Help");
        assert_eq!(Mode::InDocSearch.to_string(), "InDocSearch");
        assert_eq!(Mode::ChangePreview.to_string(), "ChangePreview");
    }

    #[test]
    fn cursor_down_clamps() {
        let state = sample_state();
        // 5 collapsed tables with no separators = 5 lines
        assert_eq!(state.line_count(), 5);

        let state = state.cursor_down(1);
        assert_eq!(state.cursor, 1);

        let state = state.cursor_down(100);
        assert_eq!(state.cursor, 4); // clamped to last
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
        assert_eq!(state.cursor, 4);

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
        let state = AppState::new(Schema::new(), String::new(), None);
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
        // All tables start collapsed. No blank lines between collapsed tables.
        assert_eq!(state.focus(), Some(&FocusTarget::Table("alpha".into())));

        let state = state.cursor_down(1);
        assert_eq!(state.focus(), Some(&FocusTarget::Table("bravo".into())));

        let state = state.cursor_down(1);
        assert_eq!(state.focus(), Some(&FocusTarget::Table("charlie".into())));
    }

    #[test]
    fn collapsed_tables_have_no_blank_separators() {
        let state = sample_state();
        // All 5 tables collapsed → no blank lines at all
        assert_eq!(state.line_count(), 5);
        for line in &state.doc {
            assert!(
                !matches!(line.target, FocusTarget::Blank),
                "collapsed-only doc should have no blank lines"
            );
        }
    }

    #[test]
    fn expanded_table_gets_blank_after_not_before() {
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        schema.add_table(Table::new("aaa"));
        let mut mid = Table::new("bbb");
        mid.add_column(Column::new("id", PgType::Uuid));
        schema.add_table(mid);
        schema.add_table(Table::new("ccc"));

        let mut state = AppState::new(schema, String::new(), None);
        // All collapsed: aaa(0) bbb(1) ccc(2) — no blanks
        assert_eq!(state.line_count(), 3);

        // Expand bbb: no blank before bbb (prev was single-line), blank after
        // aaa(0) bbb(1) id(2) close(3) blank(4) ccc(5)
        state.expanded.insert("bbb".into());
        state.rebuild_doc();
        assert_eq!(state.line_count(), 6);
        assert_eq!(state.doc[0].target, FocusTarget::Table("aaa".into()));
        assert_eq!(state.doc[1].target, FocusTarget::Table("bbb".into()));
        assert!(matches!(state.doc[2].target, FocusTarget::Column(_, _)));
        assert_eq!(state.doc[3].target, FocusTarget::TableClose("bbb".into()));
        assert_eq!(state.doc[4].target, FocusTarget::Blank);
        assert_eq!(state.doc[5].target, FocusTarget::Table("ccc".into()));
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
    fn toggle_expand_all_expands_when_none_expanded() {
        let state = sample_state();
        assert!(state.expanded.is_empty());

        let state = state.toggle_expand_all();
        assert_eq!(state.expanded.len(), 5);
        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            assert!(state.expanded.contains(name));
        }
    }

    #[test]
    fn toggle_expand_all_collapses_when_all_expanded() {
        let state = sample_state();

        // Expand all, then toggle again should collapse all
        let state = state.toggle_expand_all();
        assert_eq!(state.expanded.len(), 5);

        let state = state.toggle_expand_all();
        assert!(state.expanded.is_empty());
    }

    #[test]
    fn toggle_expand_all_expands_when_partially_expanded() {
        let state = sample_state();

        // Expand just one table
        let state = state.toggle_expand();
        assert_eq!(state.expanded.len(), 1);
        assert!(state.expanded.contains("alpha"));

        // Toggle all should expand the rest
        let state = state.toggle_expand_all();
        assert_eq!(state.expanded.len(), 5);
        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            assert!(state.expanded.contains(name));
        }
    }

    #[test]
    fn expand_all_expands_every_node() {
        let state = sample_state();
        assert!(state.expanded.is_empty());
        let state = state.expand_all();
        assert_eq!(state.expanded.len(), 5);
        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            assert!(state.expanded.contains(name));
        }
    }

    #[test]
    fn collapse_all_clears_expanded() {
        let state = sample_state().expand_all();
        assert_eq!(state.expanded.len(), 5);
        let state = state.collapse_all();
        assert!(state.expanded.is_empty());
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

        let state = AppState::new(schema, String::new(), None);
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

        let state = AppState::new(schema, String::new(), None);
        // Both collapsed: mood(0) users(1)
        assert_eq!(state.line_count(), 2);
        assert_eq!(state.focus(), Some(&FocusTarget::Enum("mood".into())));

        // Expand enum: mood(0) happy(1) sad(2) close(3) blank(4) users(5)
        let state = state.toggle_expand();
        assert_eq!(state.line_count(), 6);
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

        let state = AppState::new(schema, String::new(), None);
        // Expand and navigate to a column
        let state = state.toggle_expand();
        let state = state.cursor_down(1); // now on "id" column
        assert!(matches!(state.focus(), Some(FocusTarget::Column(_, _))));

        // Collapse — cursor should jump to table header
        let state = state.toggle_expand();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("users".into())));
    }

    // --- node_range_at tests ---

    #[test]
    fn node_range_collapsed_tables() {
        let state = sample_state();
        // Doc: alpha(0) bravo(1) charlie(2) delta(3) echo(4) — no blanks between collapsed tables
        assert_eq!(state.node_range_at(0), (0, 0)); // alpha alone
        assert_eq!(state.node_range_at(1), (1, 1)); // bravo alone
        assert_eq!(state.node_range_at(2), (2, 2)); // charlie alone
        assert_eq!(state.node_range_at(4), (4, 4)); // echo alone
    }

    #[test]
    fn node_range_expanded_table() {
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        let mut table = Table::new("aaa_users");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("name", PgType::Text));
        schema.add_table(table);
        schema.add_table(Table::new("zzz_posts"));

        let mut state = AppState::new(schema, String::new(), None);
        state.expanded.insert("aaa_users".into());
        state.rebuild_doc();
        // Doc: aaa_users(0) id(1) name(2) close(3) blank(4) zzz_posts(5)

        assert_eq!(state.node_range_at(0), (0, 3)); // table header
        assert_eq!(state.node_range_at(1), (0, 3)); // column inside
        assert_eq!(state.node_range_at(2), (0, 3)); // column inside
        assert_eq!(state.node_range_at(3), (0, 3)); // close brace
        assert_eq!(state.node_range_at(4), (4, 4)); // blank
        assert_eq!(state.node_range_at(5), (5, 5)); // zzz_posts collapsed
    }

    // --- scroll_to_focus tests ---

    #[test]
    fn scroll_to_focus_centers_cursor_when_node_fits() {
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        // Create a table with 5 columns (7 lines expanded: header + 5 cols + close)
        let mut table = Table::new("users");
        for name in ["a", "b", "c", "d", "e"] {
            table.add_column(Column::new(name, PgType::Text));
        }
        schema.add_table(table);
        // Add padding tables so doc is long enough to need scrolling
        for i in 0..10 {
            schema.add_table(Table::new(format!("t{i:02}")));
        }

        let mut state = AppState::new(schema, String::new(), None);
        state.expanded.insert("users".into());
        state.rebuild_doc();
        let state = state.with_viewport_height(20);

        // Jump cursor to "users" table header (line 0)
        // Node is lines 0-6, viewport 20 lines — node fits easily
        let state = state.cursor_to(0);
        let state = AppState {
            cursor: state.cursor,
            ..state
        }
        .scroll_to_focus();
        // Centered offset for cursor 0 = 0 (can't go negative)
        // Node 0-6 fits in viewport starting at 0
        assert_eq!(state.viewport_offset, 0);
    }

    #[test]
    fn scroll_to_focus_centers_node_when_centering_cursor_would_clip_node() {
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        // 10 padding tables before the target
        for i in 0..10 {
            schema.add_table(Table::new(format!("a{i:02}")));
        }
        // Target table with 8 columns (10 lines: header + 8 cols + close)
        let mut table = Table::new("target");
        for i in 0..8 {
            table.add_column(Column::new(format!("col{i}"), PgType::Text));
        }
        schema.add_table(table);

        let mut state = AppState::new(schema, String::new(), None);
        state.expanded.insert("target".into());
        state.rebuild_doc();
        let state = state.with_viewport_height(12);

        // Find the close brace position for "target"
        let close_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::TableClose("target".into()))
            .unwrap();

        // Jump to the close brace — centering cursor would clip the table header
        let mut state = state.cursor_to(close_pos);
        state = AppState {
            cursor: state.cursor,
            ..state
        }
        .scroll_to_focus();

        // Node should be fully visible
        let node_start = close_pos - 9; // header is 9 lines before close
        assert!(state.viewport_offset <= node_start);
        assert!(close_pos < state.viewport_offset + state.viewport_height);
    }

    #[test]
    fn scroll_to_focus_centers_cursor_for_large_node() {
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        // 5 padding tables
        for i in 0..5 {
            schema.add_table(Table::new(format!("a{i:02}")));
        }
        // Large table with 20 columns (22 lines: header + 20 cols + close)
        let mut table = Table::new("big");
        for i in 0..20 {
            table.add_column(Column::new(format!("col{i:02}"), PgType::Text));
        }
        schema.add_table(table);

        let mut state = AppState::new(schema, String::new(), None);
        state.expanded.insert("big".into());
        state.rebuild_doc();
        let state = state.with_viewport_height(10);

        // Find a column in the middle of the big table
        let mid_col = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Column("big".into(), "col10".into()))
            .unwrap();

        let mut state = state.cursor_to(mid_col);
        state = AppState {
            cursor: state.cursor,
            ..state
        }
        .scroll_to_focus();

        // Node is 22 lines, viewport is 10 — can't show full node
        // Should center cursor: offset ≈ mid_col - 5
        let expected = mid_col.saturating_sub(5);
        assert_eq!(state.viewport_offset, expected);
    }

    // --- JumpList unit tests ---

    #[test]
    fn jump_list_new_is_empty() {
        let jl = JumpList::new();
        assert!(jl.entries.is_empty());
        assert_eq!(jl.cursor, 0);
    }

    #[test]
    fn jump_list_record_adds_entries() {
        let mut jl = JumpList::new();
        jl.record(JumpEntry {
            cursor: 0,
            target: FocusTarget::Table("alpha".into()),
        });
        jl.record(JumpEntry {
            cursor: 2,
            target: FocusTarget::Table("bravo".into()),
        });
        assert_eq!(jl.entries.len(), 2);
        assert_eq!(jl.cursor, 2);
    }

    #[test]
    fn jump_list_back_empty_returns_none() {
        let mut jl = JumpList::new();
        let current = JumpEntry {
            cursor: 0,
            target: FocusTarget::Table("alpha".into()),
        };
        assert!(jl.go_back(current).is_none());
    }

    #[test]
    fn jump_list_forward_at_end_returns_none() {
        let mut jl = JumpList::new();
        jl.record(JumpEntry {
            cursor: 0,
            target: FocusTarget::Table("alpha".into()),
        });
        assert!(jl.go_forward().is_none());
    }

    #[test]
    fn jump_list_back_and_forward() {
        let mut jl = JumpList::new();
        jl.record(JumpEntry {
            cursor: 0,
            target: FocusTarget::Table("alpha".into()),
        });
        jl.record(JumpEntry {
            cursor: 2,
            target: FocusTarget::Table("bravo".into()),
        });

        // Go back from present (charlie)
        let current = JumpEntry {
            cursor: 4,
            target: FocusTarget::Table("charlie".into()),
        };
        let entry = jl.go_back(current).unwrap();
        assert_eq!(entry.cursor, 2); // bravo

        let entry = jl.go_back(JumpEntry {
            cursor: 0,
            target: FocusTarget::Blank,
        });
        assert_eq!(entry.unwrap().cursor, 0); // alpha

        // Forward back to bravo
        let entry = jl.go_forward().unwrap();
        assert_eq!(entry.cursor, 2); // bravo

        // Forward to charlie (saved by first go_back)
        let entry = jl.go_forward().unwrap();
        assert_eq!(entry.cursor, 4); // charlie

        // No more forward
        assert!(jl.go_forward().is_none());
    }

    #[test]
    fn jump_list_new_jump_truncates_forward() {
        let mut jl = JumpList::new();
        jl.record(JumpEntry {
            cursor: 0,
            target: FocusTarget::Table("alpha".into()),
        });
        jl.record(JumpEntry {
            cursor: 2,
            target: FocusTarget::Table("bravo".into()),
        });

        // Go back to bravo
        let current = JumpEntry {
            cursor: 4,
            target: FocusTarget::Table("charlie".into()),
        };
        let _ = jl.go_back(current);
        assert_eq!(jl.cursor, 1); // pointing at bravo

        // New jump from bravo — truncates charlie
        jl.record(JumpEntry {
            cursor: 2,
            target: FocusTarget::Table("bravo".into()),
        });
        assert_eq!(jl.entries.len(), 2); // [alpha, bravo]
        assert!(jl.go_forward().is_none());
    }

    // --- AppState jump list integration ---

    #[test]
    fn record_jump_and_jump_back() {
        let state = sample_state();
        // Record alpha position, then move to bravo
        let state = state.record_jump().next_table();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("bravo".into())));

        // Jump back to alpha
        let state = state.jump_back();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("alpha".into())));
    }

    #[test]
    fn jump_forward_after_back() {
        let state = sample_state();
        let state = state.record_jump().next_table(); // bravo
        let state = state.jump_back(); // alpha
        let state = state.jump_forward(); // bravo
        assert_eq!(state.focus(), Some(&FocusTarget::Table("bravo".into())));
    }

    #[test]
    fn jump_back_with_no_history_stays() {
        let state = sample_state();
        let state = state.jump_back();
        assert_eq!(state.cursor, 0);
    }

    #[test]
    fn jump_forward_with_no_forward_stays() {
        let state = sample_state();
        let state = state.jump_forward();
        assert_eq!(state.cursor, 0);
    }

    #[test]
    fn jump_back_through_multiple_jumps() {
        let state = sample_state();
        let state = state.record_jump().next_table(); // bravo
        let state = state.record_jump().next_table(); // charlie
        let state = state.record_jump().next_table(); // delta

        let state = state.jump_back();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("charlie".into())));
        let state = state.jump_back();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("bravo".into())));
        let state = state.jump_back();
        assert_eq!(state.focus(), Some(&FocusTarget::Table("alpha".into())));
    }

    // --- Ghost lines (build_document_with_ghosts) ---

    #[test]
    fn ghost_lines_for_dropped_column() {
        use crate::migration::overlay::EditOverlay;
        use crate::schema::diff::Change;
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        schema.add_table(users);

        let overlay = EditOverlay {
            changes: vec![Change::DropColumn {
                table: "users".into(),
                column: "legacy".into(),
            }],
        };

        let mut expanded = BTreeSet::new();
        expanded.insert("users".into());

        let doc = build_document_with_ghosts(&schema, &expanded, Some(&overlay));
        // Expected: users(0) id(1) ghost-legacy(2) close(3)
        assert_eq!(doc.len(), 4);
        assert!(!doc[0].ghost); // table header
        assert!(!doc[1].ghost); // id column
        assert!(doc[2].ghost); // ghost: dropped "legacy"
        assert_eq!(
            doc[2].target,
            FocusTarget::Column("users".into(), "legacy".into())
        );
        assert!(!doc[3].ghost); // close brace
    }

    #[test]
    fn ghost_lines_for_dropped_table() {
        use crate::migration::overlay::EditOverlay;
        use crate::schema::diff::Change;

        let schema = Schema::new();

        let overlay = EditOverlay {
            changes: vec![Change::DropTable("legacy_table".into())],
        };

        let doc = build_document_with_ghosts(&schema, &BTreeSet::new(), Some(&overlay));
        // Should have a single ghost table line
        assert_eq!(doc.len(), 1);
        assert!(doc[0].ghost);
        assert_eq!(doc[0].target, FocusTarget::Table("legacy_table".into()));
    }

    #[test]
    fn no_ghost_lines_without_overlay() {
        let mut schema = Schema::new();
        schema.add_table(crate::schema::Table::new("users"));

        let doc = build_document_with_ghosts(&schema, &BTreeSet::new(), None);
        for line in &doc {
            assert!(!line.ghost);
        }
    }

    // --- next_change / prev_change ---

    #[test]
    fn next_change_finds_modified_line() {
        use crate::migration::overlay::EditOverlay;
        use crate::schema::diff::{Change, ColumnChanges};
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("email", PgType::Text));
        schema.add_table(users);

        let mut state = AppState::new(schema, String::new(), None).with_viewport_height(20);
        state.expanded.insert("users".into());
        state.edit_overlay = Some(EditOverlay {
            changes: vec![Change::AlterColumn {
                table: "users".into(),
                column: "email".into(),
                changes: ColumnChanges {
                    nullable: Some(true),
                    ..Default::default()
                },
            }],
        });
        state.rebuild_doc();

        // Cursor starts at 0 (table header, which is modified)
        let state = state.next_change();
        // Should find email column (which is the altered column)
        assert!(matches!(
            state.focus(),
            Some(FocusTarget::Column(ref t, ref c)) if t == "users" && c == "email"
        ));
    }

    #[test]
    fn next_change_wraps_around() {
        use crate::migration::overlay::EditOverlay;
        use crate::schema::diff::Change;
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("bio", PgType::Text));
        schema.add_table(users);

        let mut state = AppState::new(schema, String::new(), None).with_viewport_height(20);
        state.expanded.insert("users".into());
        state.edit_overlay = Some(EditOverlay {
            changes: vec![Change::AddColumn {
                table: "users".into(),
                column: Column::new("bio", PgType::Text),
            }],
        });
        state.rebuild_doc();

        // Move to the last line (close brace)
        let last = state.line_count().saturating_sub(1);
        let state = state.cursor_to(last);
        // next_change should wrap to the table header or bio column
        let state = state.next_change();
        // Should have wrapped to an earlier position
        assert!(state.cursor < last);
    }

    #[test]
    fn prev_change_wraps_around() {
        use crate::migration::overlay::EditOverlay;
        use crate::schema::diff::Change;
        use crate::schema::types::PgType;
        use crate::schema::{Column, Table};

        let mut schema = Schema::new();
        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("bio", PgType::Text));
        schema.add_table(users);

        let mut state = AppState::new(schema, String::new(), None).with_viewport_height(20);
        state.expanded.insert("users".into());
        state.edit_overlay = Some(EditOverlay {
            changes: vec![Change::AddColumn {
                table: "users".into(),
                column: Column::new("bio", PgType::Text),
            }],
        });
        state.rebuild_doc();

        // Cursor at 0, prev_change should wrap to a later position
        let state = state.prev_change();
        assert!(state.cursor > 0);
    }

    #[test]
    fn next_change_no_overlay_shows_status() {
        let state = sample_state();
        let state = state.next_change();
        assert_eq!(state.status_message.as_deref(), Some("No edit changes"));
    }

    // --- UndoHistory unit tests ---

    #[test]
    fn undo_history_new_is_empty() {
        let h = UndoHistory::new();
        assert!(!h.can_undo());
        assert!(!h.can_redo());
    }

    #[test]
    fn undo_history_push_enables_undo() {
        let mut h = UndoHistory::new();
        h.push(UndoSnapshot {
            schema: Schema::new(),
            original_schema: None,
            renames: Vec::new(),
            edited_tables: BTreeSet::new(),
        });
        assert!(h.can_undo());
        assert!(!h.can_redo());
    }

    #[test]
    fn undo_history_pop_undo_returns_snapshot() {
        let mut h = UndoHistory::new();
        let mut schema = Schema::new();
        schema.add_table(crate::schema::Table::new("marker"));
        h.push(UndoSnapshot {
            schema: schema.clone(),
            original_schema: None,
            renames: Vec::new(),
            edited_tables: BTreeSet::new(),
        });
        let snapshot = h.pop_undo().unwrap();
        assert!(snapshot.schema.table("marker").is_some());
        assert!(!h.can_undo());
    }

    #[test]
    fn undo_history_redo_round_trip() {
        let mut h = UndoHistory::new();
        let snapshot = UndoSnapshot {
            schema: Schema::new(),
            original_schema: None,
            renames: Vec::new(),
            edited_tables: BTreeSet::new(),
        };
        h.push(snapshot.clone());

        // Pop undo, push redo
        let popped = h.pop_undo().unwrap();
        h.push_redo(UndoSnapshot {
            schema: Schema::new(),
            original_schema: None,
            renames: Vec::new(),
            edited_tables: BTreeSet::new(),
        });
        assert!(h.can_redo());

        // Pop redo
        let _redo_snap = h.pop_redo().unwrap();
        assert!(!h.can_redo());
        // Push undo should work
        h.push_undo(popped);
        assert!(h.can_undo());
    }

    #[test]
    fn undo_history_push_clears_redo() {
        let mut h = UndoHistory::new();
        let snapshot = UndoSnapshot {
            schema: Schema::new(),
            original_schema: None,
            renames: Vec::new(),
            edited_tables: BTreeSet::new(),
        };
        h.push(snapshot.clone());
        let _ = h.pop_undo();
        h.push_redo(snapshot.clone());
        assert!(h.can_redo());

        // New edit clears redo
        h.push(snapshot);
        assert!(!h.can_redo());
    }

    #[test]
    fn undo_history_clear() {
        let mut h = UndoHistory::new();
        h.push(UndoSnapshot {
            schema: Schema::new(),
            original_schema: None,
            renames: Vec::new(),
            edited_tables: BTreeSet::new(),
        });
        h.clear();
        assert!(!h.can_undo());
        assert!(!h.can_redo());
    }
}
