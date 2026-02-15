use strum::Display;

use crate::schema::Schema;

/// The TUI application mode. Determines which input handler processes keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum Mode {
    Normal,
    Edit,
    Search,
    HUD,
    Command,
}

/// Pending key state for multi-key sequences (e.g. `gg`, `g r`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingKey {
    None,
    G,
}

/// The single application state struct passed to all renderers.
///
/// State transitions are immutable: methods return a new `AppState` rather
/// than mutating in place.
#[derive(Debug, Clone)]
pub struct AppState {
    /// The introspected database schema.
    pub schema: Schema,
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
}

impl AppState {
    /// Create a new application state from a loaded schema.
    pub fn new(schema: Schema, connection_info: String) -> Self {
        Self {
            schema,
            mode: Mode::Normal,
            cursor: 0,
            viewport_offset: 0,
            viewport_height: 0,
            pending_key: PendingKey::None,
            command_buf: String::new(),
            should_quit: false,
            connection_info,
        }
    }

    /// Total number of lines in the rendered document.
    pub fn line_count(&self) -> usize {
        // One line per table name (collapsed view for the shell)
        self.schema.tables.len()
    }

    /// Transition to a new mode, resetting mode-specific state.
    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self.pending_key = PendingKey::None;
        if mode == Mode::Command {
            self.command_buf = String::new();
        }
        self
    }

    /// Set the pending key state.
    pub fn with_pending_key(mut self, key: PendingKey) -> Self {
        self.pending_key = key;
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
        assert_eq!(Mode::Search.to_string(), "Search");
        assert_eq!(Mode::HUD.to_string(), "HUD");
        assert_eq!(Mode::Command.to_string(), "Command");
    }

    #[test]
    fn cursor_down_clamps() {
        let state = sample_state();
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
}
