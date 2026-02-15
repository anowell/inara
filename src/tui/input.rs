use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::{AppState, Mode, PendingKey};
use super::edit;

/// Process a key event and return the new application state.
///
/// Dispatches to the appropriate handler based on the current mode.
pub fn handle_key(state: AppState, key: KeyEvent) -> AppState {
    // Ctrl-c always quits regardless of mode
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return state.quit();
    }

    match state.mode {
        Mode::Normal => handle_normal(state, key),
        Mode::Command => handle_command(state, key),
        Mode::Edit => edit::handle_edit(state, key),
        Mode::Rename => edit::handle_rename(state, key),
        // Other modes are placeholders for future beads
        Mode::Search | Mode::HUD => {
            if key.code == KeyCode::Esc {
                state.with_mode(Mode::Normal)
            } else {
                state
            }
        }
    }
}

/// Handle key events in Normal mode.
fn handle_normal(state: AppState, key: KeyEvent) -> AppState {
    // Check for pending key sequences first
    if state.pending_key == PendingKey::G {
        return handle_g_sequence(state, key);
    }

    match key.code {
        // Movement
        KeyCode::Char('j') | KeyCode::Down => state.cursor_down(1),
        KeyCode::Char('k') | KeyCode::Up => state.cursor_up(1),
        KeyCode::Char('G') => {
            let last = state.line_count().saturating_sub(1);
            state.cursor_to(last)
        }
        KeyCode::Char('g') => state.with_pending_key(PendingKey::G),
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let half = state.viewport_height / 2;
            state.cursor_down(half.max(1))
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let half = state.viewport_height / 2;
            state.cursor_up(half.max(1))
        }

        // Expand/collapse
        KeyCode::Enter => state.toggle_expand(),

        // Table jumping
        KeyCode::Tab => state.next_table(),
        KeyCode::BackTab => state.prev_table(),

        // Mode transitions
        KeyCode::Char(':') => state.with_mode(Mode::Command),
        KeyCode::Char('e') => edit::enter_edit_mode(state),
        KeyCode::Char('r') => edit::enter_rename_mode(state),

        // Ignore unmapped keys
        _ => state,
    }
}

/// Handle the second key in a `g` prefix sequence.
fn handle_g_sequence(state: AppState, key: KeyEvent) -> AppState {
    let state = state.with_pending_key(PendingKey::None);
    match key.code {
        KeyCode::Char('g') => state.cursor_to(0), // gg -> first line
        // Future: gr, go, gi, gm, gc, gt for goto navigation
        _ => state, // Unknown g-sequence, just cancel
    }
}

/// Handle key events in Command mode.
fn handle_command(state: AppState, key: KeyEvent) -> AppState {
    match key.code {
        KeyCode::Esc => state.with_mode(Mode::Normal),
        KeyCode::Enter => execute_command(state),
        KeyCode::Backspace => {
            let state = state.command_pop();
            // If buffer is empty after backspace, exit command mode
            if state.command_buf.is_empty() {
                state.with_mode(Mode::Normal)
            } else {
                state
            }
        }
        KeyCode::Char(ch) => state.command_push(ch),
        _ => state,
    }
}

/// Execute the current command buffer content.
fn execute_command(state: AppState) -> AppState {
    let cmd = state.command_buf.trim().to_string();
    let state = state.with_mode(Mode::Normal);
    match cmd.as_str() {
        "q" => state.quit(),
        // Future: :w, :w!, :ai, etc.
        _ => state, // Unknown command, ignore
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

    use super::*;
    use crate::schema::{Schema, Table};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn key_with_mod(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent {
            code,
            modifiers,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    fn sample_state() -> AppState {
        let mut schema = Schema::new();
        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            schema.add_table(Table::new(name));
        }
        AppState::new(schema, "test".into()).with_viewport_height(10)
    }

    // --- Normal mode movement ---

    #[test]
    fn normal_j_moves_down() {
        let state = handle_key(sample_state(), key(KeyCode::Char('j')));
        assert_eq!(state.cursor, 1);
    }

    #[test]
    fn normal_k_moves_up() {
        let state = sample_state().cursor_to(2);
        let state = handle_key(state, key(KeyCode::Char('k')));
        assert_eq!(state.cursor, 1);
    }

    #[test]
    fn normal_arrow_keys() {
        let state = handle_key(sample_state(), key(KeyCode::Down));
        assert_eq!(state.cursor, 1);

        let state = handle_key(state, key(KeyCode::Up));
        assert_eq!(state.cursor, 0);
    }

    #[test]
    fn normal_big_g_jumps_to_last() {
        let state = handle_key(sample_state(), key(KeyCode::Char('G')));
        // 5 tables with 4 blank separators = 9 lines, last at index 8
        assert_eq!(state.cursor, 8);
    }

    #[test]
    fn normal_gg_jumps_to_first() {
        let state = sample_state().cursor_to(3);
        // First 'g' sets pending
        let state = handle_key(state, key(KeyCode::Char('g')));
        assert_eq!(state.pending_key, PendingKey::G);
        // Second 'g' jumps to first
        let state = handle_key(state, key(KeyCode::Char('g')));
        assert_eq!(state.cursor, 0);
        assert_eq!(state.pending_key, PendingKey::None);
    }

    #[test]
    fn normal_ctrl_d_half_page_down() {
        let state = sample_state().with_viewport_height(4);
        let state = handle_key(
            state,
            key_with_mod(KeyCode::Char('d'), KeyModifiers::CONTROL),
        );
        assert_eq!(state.cursor, 2); // half of 4
    }

    #[test]
    fn normal_ctrl_u_half_page_up() {
        let state = sample_state().with_viewport_height(4).cursor_to(4);
        let state = handle_key(
            state,
            key_with_mod(KeyCode::Char('u'), KeyModifiers::CONTROL),
        );
        assert_eq!(state.cursor, 2);
    }

    // --- Ctrl-c quits from any mode ---

    #[test]
    fn ctrl_c_quits_from_normal() {
        let state = handle_key(
            sample_state(),
            key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(state.should_quit);
    }

    #[test]
    fn ctrl_c_quits_from_command() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key(
            state,
            key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(state.should_quit);
    }

    // --- Command mode ---

    #[test]
    fn colon_enters_command_mode() {
        let state = handle_key(sample_state(), key(KeyCode::Char(':')));
        assert_eq!(state.mode, Mode::Command);
        assert!(state.command_buf.is_empty());
    }

    #[test]
    fn command_q_quits() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key(state, key(KeyCode::Char('q')));
        assert_eq!(state.command_buf, "q");
        let state = handle_key(state, key(KeyCode::Enter));
        assert!(state.should_quit);
    }

    #[test]
    fn command_esc_returns_to_normal() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key(state, key(KeyCode::Char('q')));
        let state = handle_key(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn command_backspace_exits_when_empty() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key(state, key(KeyCode::Char('x')));
        assert_eq!(state.command_buf, "x");
        let state = handle_key(state, key(KeyCode::Backspace));
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn command_unknown_does_nothing() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key(state, key(KeyCode::Char('z')));
        let state = handle_key(state, key(KeyCode::Enter));
        assert!(!state.should_quit);
        assert_eq!(state.mode, Mode::Normal);
    }

    // --- Placeholder modes (Esc returns to Normal) ---

    #[test]
    fn esc_exits_placeholder_modes() {
        for mode in [Mode::Search, Mode::HUD] {
            let state = sample_state().with_mode(mode);
            let state = handle_key(state, key(KeyCode::Esc));
            assert_eq!(state.mode, Mode::Normal, "Esc should exit {mode:?}");
        }
    }

    // --- Enter toggles expand/collapse ---

    #[test]
    fn enter_toggles_expand() {
        let state = sample_state();
        assert!(state.expanded.is_empty());

        let state = handle_key(state, key(KeyCode::Enter));
        assert!(state.expanded.contains("alpha"));

        let state = handle_key(state, key(KeyCode::Enter));
        assert!(!state.expanded.contains("alpha"));
    }

    // --- Tab/Shift-Tab jumps between tables ---

    #[test]
    fn tab_jumps_to_next_table() {
        let state = sample_state();
        assert_eq!(
            state.focus(),
            Some(&crate::tui::app::FocusTarget::Table("alpha".into()))
        );

        let state = handle_key(state, key(KeyCode::Tab));
        assert_eq!(
            state.focus(),
            Some(&crate::tui::app::FocusTarget::Table("bravo".into()))
        );
    }

    #[test]
    fn shift_tab_jumps_to_prev_table() {
        let state = sample_state();
        let state = handle_key(state, key(KeyCode::Tab)); // bravo
        let state = handle_key(state, key(KeyCode::Tab)); // charlie

        let state = handle_key(state, key(KeyCode::BackTab));
        assert_eq!(
            state.focus(),
            Some(&crate::tui::app::FocusTarget::Table("bravo".into()))
        );
    }

    // --- g-sequence edge cases ---

    #[test]
    fn g_then_unknown_cancels() {
        let state = sample_state().cursor_to(2);
        let state = handle_key(state, key(KeyCode::Char('g')));
        let state = handle_key(state, key(KeyCode::Char('x'))); // unknown
        assert_eq!(state.pending_key, PendingKey::None);
        assert_eq!(state.cursor, 2); // unchanged
    }
}
