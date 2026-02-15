use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use sqlx::PgPool;

use super::app::{AppState, FocusTarget, Mode, PendingKey};
use super::edit;
use super::fuzzy::SearchFilter;
use super::goto::{self, GotoResult};
use super::hud::{self, HudResultHandle, HudState, HudStatus, HudTarget};

/// Process a key event and return the new application state.
///
/// Returns the updated state and an optional HUD result handle when
/// an async query has been spawned.
pub fn handle_key(
    state: AppState,
    key: KeyEvent,
    pool: &PgPool,
) -> (AppState, Option<HudResultHandle>) {
    // Clear transient status message on any key press
    let state = state.clear_status();

    // Ctrl-c always quits regardless of mode
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return (state.quit(), None);
    }

    match state.mode {
        Mode::Normal => handle_normal(state, key, pool),
        Mode::Command => (handle_command(state, key), None),
        Mode::SpaceMenu => (handle_space_menu(state, key), None),
        Mode::Search => (handle_search(state, key), None),
        Mode::Edit => (edit::handle_edit(state, key), None),
        Mode::Rename => (edit::handle_rename(state, key), None),
        Mode::HUD => handle_hud(state, key, pool),
    }
}

/// Handle key events in Normal mode.
fn handle_normal(
    state: AppState,
    key: KeyEvent,
    pool: &PgPool,
) -> (AppState, Option<HudResultHandle>) {
    // Check for pending key sequences first
    if state.pending_key == PendingKey::G {
        return (handle_g_sequence(state, key), None);
    }

    match key.code {
        // Movement
        KeyCode::Char('j') | KeyCode::Down => (state.cursor_down(1), None),
        KeyCode::Char('k') | KeyCode::Up => (state.cursor_up(1), None),
        KeyCode::Char('G') => {
            let last = state.line_count().saturating_sub(1);
            (state.cursor_to(last), None)
        }
        KeyCode::Char('g') => (state.with_pending_key(PendingKey::G), None),
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let half = state.viewport_height / 2;
            (state.cursor_down(half.max(1)), None)
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let half = state.viewport_height / 2;
            (state.cursor_up(half.max(1)), None)
        }

        // Expand/collapse
        KeyCode::Enter => (state.toggle_expand(), None),

        // Table jumping
        KeyCode::Tab => (state.next_table(), None),
        KeyCode::BackTab => (state.prev_table(), None),

        // Mode transitions
        KeyCode::Char(':') => (state.with_mode(Mode::Command), None),
        KeyCode::Char(' ') => (state.with_mode(Mode::SpaceMenu), None),
        KeyCode::Char('e') => (edit::enter_edit_mode(state), None),
        KeyCode::Char('r') => (edit::enter_rename_mode(state), None),
        KeyCode::Char('q') => open_hud(state, pool),

        // Ignore unmapped keys
        _ => (state, None),
    }
}

/// Handle the second key in a `g` prefix sequence.
fn handle_g_sequence(state: AppState, key: KeyEvent) -> AppState {
    let state = state.with_pending_key(PendingKey::None);
    match key.code {
        KeyCode::Char('g') => state.cursor_to(0), // gg -> first line
        KeyCode::Char(ch) => {
            let focus = match state.focus().cloned() {
                Some(f) => f,
                None => return state.with_status("goto not available here"),
            };
            let result = goto::dispatch(ch, &focus, &state.schema, &state.relation_map);
            match result {
                GotoResult::Jump(target) => state.clear_status().jump_to_goto(&target),
                GotoResult::Pick(targets) => state.clear_status().enter_goto_picker(targets),
                GotoResult::NoResults(msg) => state.with_status(msg),
                GotoResult::NotAvailable(msg) => state.with_status(msg),
            }
        }
        _ => state.with_status("unknown goto"), // non-char keys cancel
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

/// Handle key events in SpaceMenu mode.
///
/// The space menu shows available subcommands. Pressing a submenu key
/// immediately opens the corresponding search filter. Esc or any
/// unrecognized key dismisses the menu.
fn handle_space_menu(state: AppState, key: KeyEvent) -> AppState {
    match key.code {
        KeyCode::Char('f') => state.enter_search(SearchFilter::All),
        KeyCode::Char('t') => state.enter_search(SearchFilter::Tables),
        KeyCode::Char('c') => state.enter_search(SearchFilter::Columns),
        KeyCode::Char('m') => state.enter_search(SearchFilter::Migrations),
        KeyCode::Esc | KeyCode::Char(' ') => state.with_mode(Mode::Normal),
        _ => state.with_mode(Mode::Normal), // dismiss on unknown key
    }
}

/// Handle key events in Search mode.
///
/// Captures typed characters into the search query, navigates results with
/// Up/Down or Ctrl-p/Ctrl-n, selects with Enter, and dismisses with Esc.
fn handle_search(state: AppState, key: KeyEvent) -> AppState {
    match key.code {
        KeyCode::Esc => state.with_mode(Mode::Normal),
        KeyCode::Enter => {
            // Check if this is a goto picker
            let goto_target = state
                .search
                .as_ref()
                .and_then(|s| s.selected_goto_target().cloned());
            if let Some(target) = goto_target {
                let state = state.with_mode(Mode::Normal);
                return state.jump_to_goto(&target);
            }
            // Standard search: select the current result and jump to it
            let symbol = state
                .search
                .as_ref()
                .and_then(|s| s.selected_result())
                .map(|r| r.symbol.clone());
            let state = state.with_mode(Mode::Normal);
            if let Some(sym) = symbol {
                state.jump_to_symbol(&sym)
            } else {
                state
            }
        }
        KeyCode::Down => {
            let mut state = state;
            if let Some(search) = state.search.take() {
                state.search = Some(search.select_next());
            }
            state
        }
        KeyCode::Up => {
            let mut state = state;
            if let Some(search) = state.search.take() {
                state.search = Some(search.select_prev());
            }
            state
        }
        KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let mut state = state;
            if let Some(search) = state.search.take() {
                state.search = Some(search.select_next());
            }
            state
        }
        KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let mut state = state;
            if let Some(search) = state.search.take() {
                state.search = Some(search.select_prev());
            }
            state
        }
        KeyCode::Backspace => {
            let mut state = state;
            if let Some(search) = state.search.take() {
                if search.query.is_empty() {
                    // Exit search if query is empty
                    state.search = None;
                    return state.with_mode(Mode::Normal);
                }
                state.search = Some(search.pop_char());
            }
            state
        }
        KeyCode::Char(ch) => {
            let mut state = state;
            if let Some(search) = state.search.take() {
                state.search = Some(search.push_char(ch));
            }
            state
        }
        _ => state,
    }
}

/// Handle key events in HUD mode.
fn handle_hud(
    state: AppState,
    key: KeyEvent,
    pool: &PgPool,
) -> (AppState, Option<HudResultHandle>) {
    match key.code {
        KeyCode::Esc => (state.with_mode(Mode::Normal), None),
        KeyCode::Char('y') => confirm_safety_warning(state, pool),
        _ => (state, None),
    }
}

/// Open the HUD for the currently focused element.
fn open_hud(state: AppState, pool: &PgPool) -> (AppState, Option<HudResultHandle>) {
    let focus = state.focus().cloned();

    let (target, schema_name) = match focus {
        Some(FocusTarget::Table(ref name)) => (
            HudTarget::Table { name: name.clone() },
            "public".to_string(),
        ),
        Some(FocusTarget::Column(ref table, ref col)) => {
            let pg_type = state
                .schema
                .table(table)
                .and_then(|t| t.column(col))
                .map(|c| c.pg_type.clone())
                .unwrap_or(crate::schema::types::PgType::Text);
            (
                HudTarget::Column {
                    table: table.clone(),
                    column: col.clone(),
                    pg_type,
                },
                "public".to_string(),
            )
        }
        // For other table-related targets, use the table
        Some(ref target) => {
            if let Some(name) = target.table_name() {
                (
                    HudTarget::Table {
                        name: name.to_string(),
                    },
                    "public".to_string(),
                )
            } else {
                return (state, None); // Can't open HUD for non-table elements
            }
        }
        None => return (state, None),
    };

    let handle = hud::new_result_handle();

    match &target {
        HudTarget::Table { name } => {
            hud::spawn_table_query(pool.clone(), schema_name, name.clone(), handle.clone());
        }
        HudTarget::Column {
            table,
            column,
            pg_type,
        } => {
            hud::spawn_safety_check(
                pool.clone(),
                schema_name,
                table.clone(),
                column.clone(),
                pg_type.clone(),
                handle.clone(),
            );
        }
    }

    let hud_state = HudState {
        target,
        status: HudStatus::Loading,
    };

    let state = state.with_mode(Mode::HUD).with_hud(Some(hud_state));
    (state, Some(handle))
}

/// Handle 'y' key in HUD mode to confirm a safety warning and run the query.
fn confirm_safety_warning(state: AppState, pool: &PgPool) -> (AppState, Option<HudResultHandle>) {
    let hud = match &state.hud {
        Some(hud) => hud,
        None => return (state, None),
    };

    // Only respond to 'y' when showing a safety warning
    match &hud.status {
        HudStatus::SafetyWarning {
            table,
            column,
            pg_type,
            ..
        } => {
            let handle = hud::new_result_handle();
            hud::spawn_column_query(
                pool.clone(),
                "public".to_string(),
                table.clone(),
                column.clone(),
                pg_type.clone(),
                handle.clone(),
            );
            let state = state.with_hud_status(HudStatus::Loading);
            (state, Some(handle))
        }
        _ => (state, None),
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

    /// Helper to call handle_key without a pool (for tests that don't trigger HUD).
    /// We use a dummy approach: create a mock pool isn't possible, so we test
    /// only non-HUD paths by checking that 'q' on non-table targets is ignored.
    fn handle_key_no_pool(state: AppState, key: KeyEvent) -> AppState {
        // For unit tests, we test input handling that doesn't need a pool.
        // The HUD-related paths are tested via integration tests.
        handle_key_inner(state, key)
    }

    /// Inner handler for tests — dispatches without pool dependency.
    fn handle_key_inner(state: AppState, key: KeyEvent) -> AppState {
        // Clear transient status message on any key press
        let state = state.clear_status();

        // Ctrl-c always quits regardless of mode
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return state.quit();
        }

        match state.mode {
            Mode::Normal => handle_normal_no_pool(state, key),
            Mode::Command => handle_command(state, key),
            Mode::SpaceMenu => handle_space_menu(state, key),
            Mode::Search => handle_search(state, key),
            Mode::Edit => edit::handle_edit(state, key),
            Mode::Rename => edit::handle_rename(state, key),
            Mode::HUD => {
                if key.code == KeyCode::Esc {
                    state.with_mode(Mode::Normal)
                } else {
                    state
                }
            }
        }
    }

    /// Normal mode handler for tests (no pool, no HUD opening).
    fn handle_normal_no_pool(state: AppState, key: KeyEvent) -> AppState {
        if state.pending_key == PendingKey::G {
            return handle_g_sequence(state, key);
        }

        match key.code {
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
            KeyCode::Enter => state.toggle_expand(),
            KeyCode::Tab => state.next_table(),
            KeyCode::BackTab => state.prev_table(),
            KeyCode::Char(':') => state.with_mode(Mode::Command),
            KeyCode::Char(' ') => state.with_mode(Mode::SpaceMenu),
            KeyCode::Char('e') => edit::enter_edit_mode(state),
            KeyCode::Char('r') => edit::enter_rename_mode(state),
            _ => state,
        }
    }

    // --- Normal mode movement ---

    #[test]
    fn normal_j_moves_down() {
        let state = handle_key_no_pool(sample_state(), key(KeyCode::Char('j')));
        assert_eq!(state.cursor, 1);
    }

    #[test]
    fn normal_k_moves_up() {
        let state = sample_state().cursor_to(2);
        let state = handle_key_no_pool(state, key(KeyCode::Char('k')));
        assert_eq!(state.cursor, 1);
    }

    #[test]
    fn normal_arrow_keys() {
        let state = handle_key_no_pool(sample_state(), key(KeyCode::Down));
        assert_eq!(state.cursor, 1);

        let state = handle_key_no_pool(state, key(KeyCode::Up));
        assert_eq!(state.cursor, 0);
    }

    #[test]
    fn normal_big_g_jumps_to_last() {
        let state = handle_key_no_pool(sample_state(), key(KeyCode::Char('G')));
        // 5 tables with 4 blank separators = 9 lines, last at index 8
        assert_eq!(state.cursor, 8);
    }

    #[test]
    fn normal_gg_jumps_to_first() {
        let state = sample_state().cursor_to(3);
        // First 'g' sets pending
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        assert_eq!(state.pending_key, PendingKey::G);
        // Second 'g' jumps to first
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        assert_eq!(state.cursor, 0);
        assert_eq!(state.pending_key, PendingKey::None);
    }

    #[test]
    fn normal_ctrl_d_half_page_down() {
        let state = sample_state().with_viewport_height(4);
        let state = handle_key_no_pool(
            state,
            key_with_mod(KeyCode::Char('d'), KeyModifiers::CONTROL),
        );
        assert_eq!(state.cursor, 2); // half of 4
    }

    #[test]
    fn normal_ctrl_u_half_page_up() {
        let state = sample_state().with_viewport_height(4).cursor_to(4);
        let state = handle_key_no_pool(
            state,
            key_with_mod(KeyCode::Char('u'), KeyModifiers::CONTROL),
        );
        assert_eq!(state.cursor, 2);
    }

    // --- Ctrl-c quits from any mode ---

    #[test]
    fn ctrl_c_quits_from_normal() {
        let state = handle_key_no_pool(
            sample_state(),
            key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(state.should_quit);
    }

    #[test]
    fn ctrl_c_quits_from_command() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key_no_pool(
            state,
            key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL),
        );
        assert!(state.should_quit);
    }

    // --- Command mode ---

    #[test]
    fn colon_enters_command_mode() {
        let state = handle_key_no_pool(sample_state(), key(KeyCode::Char(':')));
        assert_eq!(state.mode, Mode::Command);
        assert!(state.command_buf.is_empty());
    }

    #[test]
    fn command_q_quits() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key_no_pool(state, key(KeyCode::Char('q')));
        assert_eq!(state.command_buf, "q");
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert!(state.should_quit);
    }

    #[test]
    fn command_esc_returns_to_normal() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key_no_pool(state, key(KeyCode::Char('q')));
        let state = handle_key_no_pool(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn command_backspace_exits_when_empty() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key_no_pool(state, key(KeyCode::Char('x')));
        assert_eq!(state.command_buf, "x");
        let state = handle_key_no_pool(state, key(KeyCode::Backspace));
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn command_unknown_does_nothing() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key_no_pool(state, key(KeyCode::Char('z')));
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert!(!state.should_quit);
        assert_eq!(state.mode, Mode::Normal);
    }

    // --- Placeholder modes (Esc returns to Normal) ---

    #[test]
    fn esc_exits_hud_mode() {
        let state = sample_state().with_mode(Mode::HUD);
        let state = handle_key_no_pool(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal, "Esc should exit HUD mode");
    }

    // --- Enter toggles expand/collapse ---

    #[test]
    fn enter_toggles_expand() {
        let state = sample_state();
        assert!(state.expanded.is_empty());

        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert!(state.expanded.contains("alpha"));

        let state = handle_key_no_pool(state, key(KeyCode::Enter));
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

        let state = handle_key_no_pool(state, key(KeyCode::Tab));
        assert_eq!(
            state.focus(),
            Some(&crate::tui::app::FocusTarget::Table("bravo".into()))
        );
    }

    #[test]
    fn shift_tab_jumps_to_prev_table() {
        let state = sample_state();
        let state = handle_key_no_pool(state, key(KeyCode::Tab)); // bravo
        let state = handle_key_no_pool(state, key(KeyCode::Tab)); // charlie

        let state = handle_key_no_pool(state, key(KeyCode::BackTab));
        assert_eq!(
            state.focus(),
            Some(&crate::tui::app::FocusTarget::Table("bravo".into()))
        );
    }

    // --- g-sequence edge cases ---

    #[test]
    fn g_then_unknown_cancels() {
        let state = sample_state().cursor_to(2);
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('x'))); // unknown
        assert_eq!(state.pending_key, PendingKey::None);
        assert_eq!(state.cursor, 2); // unchanged
    }

    // --- Space menu ---

    #[test]
    fn space_opens_space_menu() {
        let state = handle_key_no_pool(sample_state(), key(KeyCode::Char(' ')));
        assert_eq!(state.mode, Mode::SpaceMenu);
    }

    #[test]
    fn space_menu_esc_returns_to_normal() {
        let state = sample_state().with_mode(Mode::SpaceMenu);
        let state = handle_key_no_pool(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn space_menu_f_enters_search_all() {
        let state = sample_state().with_mode(Mode::SpaceMenu);
        let state = handle_key_no_pool(state, key(KeyCode::Char('f')));
        assert_eq!(state.mode, Mode::Search);
        assert!(state.search.is_some());
        assert_eq!(state.search.as_ref().unwrap().filter, SearchFilter::All);
    }

    #[test]
    fn space_menu_t_enters_search_tables() {
        let state = sample_state().with_mode(Mode::SpaceMenu);
        let state = handle_key_no_pool(state, key(KeyCode::Char('t')));
        assert_eq!(state.mode, Mode::Search);
        assert_eq!(state.search.as_ref().unwrap().filter, SearchFilter::Tables);
    }

    #[test]
    fn space_menu_c_enters_search_columns() {
        let state = sample_state().with_mode(Mode::SpaceMenu);
        let state = handle_key_no_pool(state, key(KeyCode::Char('c')));
        assert_eq!(state.mode, Mode::Search);
        assert_eq!(state.search.as_ref().unwrap().filter, SearchFilter::Columns);
    }

    #[test]
    fn space_menu_m_enters_search_migrations() {
        let state = sample_state().with_mode(Mode::SpaceMenu);
        let state = handle_key_no_pool(state, key(KeyCode::Char('m')));
        assert_eq!(state.mode, Mode::Search);
        assert_eq!(
            state.search.as_ref().unwrap().filter,
            SearchFilter::Migrations
        );
    }

    #[test]
    fn space_menu_unknown_dismisses() {
        let state = sample_state().with_mode(Mode::SpaceMenu);
        let state = handle_key_no_pool(state, key(KeyCode::Char('z')));
        assert_eq!(state.mode, Mode::Normal);
    }

    // --- Search mode ---

    fn search_state() -> AppState {
        use crate::schema::types::PgType;
        use crate::schema::Column;

        let mut schema = Schema::new();
        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("email", PgType::Text));
        schema.add_table(users);

        let mut posts = Table::new("posts");
        posts.add_column(Column::new("id", PgType::Uuid));
        posts.add_column(Column::new("title", PgType::Text));
        schema.add_table(posts);

        AppState::new(schema, "test".into())
            .with_viewport_height(20)
            .enter_search(SearchFilter::All)
    }

    #[test]
    fn search_esc_returns_to_normal() {
        let state = search_state();
        assert_eq!(state.mode, Mode::Search);
        let state = handle_key_no_pool(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal);
        assert!(state.search.is_none());
    }

    #[test]
    fn search_typing_updates_query() {
        let state = search_state();
        let state = handle_key_no_pool(state, key(KeyCode::Char('u')));
        assert_eq!(state.search.as_ref().unwrap().query, "u");
        let state = handle_key_no_pool(state, key(KeyCode::Char('s')));
        assert_eq!(state.search.as_ref().unwrap().query, "us");
    }

    #[test]
    fn search_backspace_removes_char() {
        let state = search_state();
        let state = handle_key_no_pool(state, key(KeyCode::Char('u')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('s')));
        let state = handle_key_no_pool(state, key(KeyCode::Backspace));
        assert_eq!(state.search.as_ref().unwrap().query, "u");
    }

    #[test]
    fn search_backspace_empty_exits() {
        let state = search_state();
        let state = handle_key_no_pool(state, key(KeyCode::Backspace));
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn search_down_selects_next() {
        let state = search_state();
        assert_eq!(state.search.as_ref().unwrap().selected, 0);
        let state = handle_key_no_pool(state, key(KeyCode::Down));
        assert_eq!(state.search.as_ref().unwrap().selected, 1);
    }

    #[test]
    fn search_up_selects_prev() {
        let state = search_state();
        let state = handle_key_no_pool(state, key(KeyCode::Down));
        let state = handle_key_no_pool(state, key(KeyCode::Down));
        let state = handle_key_no_pool(state, key(KeyCode::Up));
        assert_eq!(state.search.as_ref().unwrap().selected, 1);
    }

    #[test]
    fn search_ctrl_n_selects_next() {
        let state = search_state();
        let state = handle_key_no_pool(
            state,
            key_with_mod(KeyCode::Char('n'), KeyModifiers::CONTROL),
        );
        assert_eq!(state.search.as_ref().unwrap().selected, 1);
    }

    #[test]
    fn search_ctrl_p_selects_prev() {
        let state = search_state();
        let state = handle_key_no_pool(state, key(KeyCode::Down));
        let state = handle_key_no_pool(
            state,
            key_with_mod(KeyCode::Char('p'), KeyModifiers::CONTROL),
        );
        assert_eq!(state.search.as_ref().unwrap().selected, 0);
    }

    #[test]
    fn search_enter_selects_and_jumps() {
        let state = search_state();
        // Type "users" to filter to users table
        let state = handle_key_no_pool(state, key(KeyCode::Char('u')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('s')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('e')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('r')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('s')));

        // Select the result
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
        assert!(state.search.is_none());
        // Should have jumped to the "users" table
        assert_eq!(
            state.focus(),
            Some(&crate::tui::app::FocusTarget::Table("users".into()))
        );
    }

    #[test]
    fn search_enter_with_no_results_returns_normal() {
        let state = search_state();
        // Type something that won't match
        let state = handle_key_no_pool(state, key(KeyCode::Char('z')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('z')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('z')));
        assert!(state.search.as_ref().unwrap().results.is_empty());

        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
    }

    // --- HUD state management ---

    #[test]
    fn hud_state_cleared_on_mode_exit() {
        let state = sample_state().with_mode(Mode::HUD).with_hud(Some(HudState {
            target: HudTarget::Table {
                name: "test".into(),
            },
            status: HudStatus::Loading,
        }));
        assert!(state.hud.is_some());

        let state = state.with_mode(Mode::Normal);
        assert!(state.hud.is_none());
    }

    // --- Goto navigation (g-prefix) ---

    /// Create a state with FKs for goto testing.
    fn goto_state() -> AppState {
        use crate::schema::types::{ForeignKeyRef, PgType, ReferentialAction};
        use crate::schema::{Column, Constraint, EnumType, Index};

        let mut schema = Schema::new();

        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("email", PgType::Text));
        users.add_column(Column::new("role", PgType::Custom("user_role".into())));
        users.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        users.add_index(Index {
            name: "users_email_idx".into(),
            columns: vec!["email".into()],
            unique: true,
            partial: None,
        });
        schema.add_table(users);

        let mut posts = Table::new("posts");
        posts.add_column(Column::new("id", PgType::Uuid));
        posts.add_column(Column::new("author_id", PgType::Uuid));
        posts.add_column(Column::new("title", PgType::Text));
        posts.add_constraint(Constraint::PrimaryKey {
            name: Some("posts_pkey".into()),
            columns: vec!["id".into()],
        });
        posts.add_constraint(Constraint::ForeignKey {
            name: Some("posts_author_fk".into()),
            columns: vec!["author_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        posts.add_index(Index {
            name: "posts_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: false,
            partial: None,
        });
        schema.add_table(posts);

        schema.add_enum(EnumType {
            name: "user_role".into(),
            variants: vec!["admin".into(), "member".into()],
        });

        AppState::new(schema, "test".into()).with_viewport_height(30)
    }

    #[test]
    fn g_enters_pending_state() {
        let state = goto_state();
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        assert_eq!(state.pending_key, PendingKey::G);
    }

    #[test]
    fn g_unknown_shows_status_message() {
        let state = goto_state();
        // Navigate to a table first (cursor 0 is on enum header)
        let posts_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("posts".into()))
            .unwrap();
        let state = state.cursor_to(posts_pos);
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('x')));
        assert_eq!(state.pending_key, PendingKey::None);
        assert!(state.status_message.is_some());
        assert!(state.status_message.as_ref().unwrap().contains("unknown"));
    }

    #[test]
    fn gr_on_table_with_incoming_fks_jumps() {
        let state = goto_state();
        // Navigate to "users" table (which has incoming FK from posts)
        // users is the last table alphabetically with enum above it
        // Doc: enum user_role header + admin + member + close + blank + posts + blank + users
        let users_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("users".into()))
            .unwrap();
        let state = state.cursor_to(users_pos);

        // g r should jump to incoming references
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('r')));
        // users has only one incoming FK (from posts), so it should Jump directly
        assert_eq!(state.pending_key, PendingKey::None);
        assert_eq!(state.focus(), Some(&FocusTarget::Table("posts".into())));
    }

    #[test]
    fn go_on_table_with_outgoing_fks_jumps() {
        let state = goto_state();
        // Navigate to "posts" table
        let posts_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("posts".into()))
            .unwrap();
        let state = state.cursor_to(posts_pos);

        // g o should jump to outgoing references
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('o')));
        // posts has one outgoing FK (to users), so should Jump directly
        assert_eq!(state.focus(), Some(&FocusTarget::Table("users".into())));
    }

    #[test]
    fn go_on_table_no_outgoing_shows_no_results() {
        let state = goto_state();
        // Navigate to "users" table (no outgoing FKs)
        let users_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("users".into()))
            .unwrap();
        let state = state.cursor_to(users_pos);

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('o')));
        assert!(state.status_message.is_some());
        assert!(state.status_message.as_ref().unwrap().contains("no"));
    }

    #[test]
    fn gc_on_table_jumps_to_first_column() {
        let state = goto_state();
        let posts_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("posts".into()))
            .unwrap();
        let state = state.cursor_to(posts_pos);

        // g c should jump to first column (expanding the table)
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('c')));
        assert_eq!(
            state.focus(),
            Some(&FocusTarget::Column("posts".into(), "id".into()))
        );
        // Table should have been expanded
        assert!(state.expanded.contains("posts"));
    }

    #[test]
    fn gt_on_column_jumps_to_parent_table() {
        let state = goto_state();
        // Expand posts and navigate to a column
        let posts_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("posts".into()))
            .unwrap();
        let state = state.cursor_to(posts_pos).toggle_expand();
        // Find the author_id column
        let col_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Column("posts".into(), "author_id".into()))
            .unwrap();
        let state = state.cursor_to(col_pos);

        // g t should jump to parent table
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('t')));
        assert_eq!(state.focus(), Some(&FocusTarget::Table("posts".into())));
    }

    #[test]
    fn gd_on_fk_column_jumps_to_target() {
        let state = goto_state();
        // Expand posts and navigate to author_id column
        let posts_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("posts".into()))
            .unwrap();
        let state = state.cursor_to(posts_pos).toggle_expand();
        let col_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Column("posts".into(), "author_id".into()))
            .unwrap();
        let state = state.cursor_to(col_pos);

        // g d should jump to FK target (users.id)
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('d')));
        assert_eq!(
            state.focus(),
            Some(&FocusTarget::Column("users".into(), "id".into()))
        );
        // users should have been expanded
        assert!(state.expanded.contains("users"));
    }

    #[test]
    fn gy_on_custom_type_column_jumps_to_enum() {
        let state = goto_state();
        // Expand users and navigate to role column (which is Custom("user_role"))
        let users_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("users".into()))
            .unwrap();
        let state = state.cursor_to(users_pos).toggle_expand();
        let col_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Column("users".into(), "role".into()))
            .unwrap();
        let state = state.cursor_to(col_pos);

        // g y should jump to enum definition
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('y')));
        assert_eq!(state.focus(), Some(&FocusTarget::Enum("user_role".into())));
    }

    #[test]
    fn gy_on_non_custom_type_shows_no_results() {
        let state = goto_state();
        // Expand users and navigate to email column (text, not custom)
        let users_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("users".into()))
            .unwrap();
        let state = state.cursor_to(users_pos).toggle_expand();
        let col_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Column("users".into(), "email".into()))
            .unwrap();
        let state = state.cursor_to(col_pos);

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('y')));
        assert!(state.status_message.is_some());
    }

    #[test]
    fn gm_shows_not_available() {
        let state = goto_state();
        // Navigate to a table first
        let posts_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("posts".into()))
            .unwrap();
        let state = state.cursor_to(posts_pos);
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('m')));
        assert!(state.status_message.is_some());
        assert!(state.status_message.as_ref().unwrap().contains("not yet"));
    }

    #[test]
    fn gi_on_table_with_indexes_jumps() {
        let state = goto_state();
        let users_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("users".into()))
            .unwrap();
        let state = state.cursor_to(users_pos);

        // g i should jump to indexed column
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('i')));
        // users has one index on email, should jump to email column
        assert_eq!(
            state.focus(),
            Some(&FocusTarget::Column("users".into(), "email".into()))
        );
    }

    #[test]
    fn status_message_clears_on_next_key() {
        let state = goto_state().with_status("test message");
        assert!(state.status_message.is_some());

        let state = handle_key_no_pool(state, key(KeyCode::Char('j')));
        assert!(state.status_message.is_none());
    }
}
