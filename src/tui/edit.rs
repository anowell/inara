use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::{AppState, FocusTarget, Mode, RenameMetadata, RenameTarget};
use crate::schema::parse::parse_single_table;
use crate::schema::render::render_single_table;

/// Enter edit mode for the table under the cursor.
///
/// Renders the focused table to its declarative text, populates the edit
/// buffer, and transitions to Edit mode.
pub fn enter_edit_mode(state: AppState) -> AppState {
    let table_name = match state.focus() {
        Some(target) => target.table_name().map(|s| s.to_string()),
        None => return state,
    };

    let table_name = match table_name {
        Some(name) => name,
        None => return state,
    };

    let table = match state.schema.table(&table_name) {
        Some(t) => t,
        None => return state,
    };

    let rendered = render_single_table(table);
    let lines: Vec<String> = rendered.lines().map(|l| l.to_string()).collect();

    let mut state = state.ensure_original_schema();
    state.edit_buffer = lines;
    state.edit_cursor_row = 0;
    state.edit_cursor_col = 0;
    state.edit_table = Some(table_name);
    state.edit_error = None;
    state.mode = Mode::Edit;
    state.pending_key = super::app::PendingKey::None;
    state
}

/// Handle a key event in Edit mode.
pub fn handle_edit(state: AppState, key: KeyEvent) -> AppState {
    match key.code {
        KeyCode::Esc => exit_edit_mode(state),
        KeyCode::Char(ch) => edit_insert_char(state, ch),
        KeyCode::Backspace => edit_backspace(state),
        KeyCode::Delete => edit_delete(state),
        KeyCode::Enter => edit_newline(state),
        KeyCode::Left => edit_cursor_left(state),
        KeyCode::Right => edit_cursor_right(state),
        KeyCode::Up => edit_cursor_up(state),
        KeyCode::Down => edit_cursor_down(state),
        KeyCode::Home => {
            let mut state = state;
            state.edit_cursor_col = 0;
            state
        }
        KeyCode::End => {
            let mut state = state;
            let row = state.edit_cursor_row;
            state.edit_cursor_col = state.edit_buffer.get(row).map(|l| l.len()).unwrap_or(0);
            state
        }
        _ => state,
    }
}

/// Exit edit mode: parse the edited text, update schema if valid.
fn exit_edit_mode(mut state: AppState) -> AppState {
    let text = state.edit_buffer.join("\n");
    let text = if text.ends_with('\n') {
        text
    } else {
        format!("{text}\n")
    };

    match parse_single_table(&text) {
        Ok(new_table) => {
            let table_name = match state.edit_table.take() {
                Some(name) => name,
                None => return state.with_mode(Mode::Normal),
            };

            // If the table name changed in the edit, that's a rename
            if new_table.name != table_name {
                state.renames.push(RenameMetadata {
                    table: table_name.clone(),
                    from: table_name.clone(),
                    to: new_table.name.clone(),
                });
                state.schema.tables.remove(&table_name);
                state.edited_tables.remove(&table_name);
                state.edited_tables.insert(new_table.name.clone());
            } else {
                state.edited_tables.insert(table_name.clone());
                state.schema.tables.remove(&table_name);
            }

            state.schema.add_table(new_table);
            state.edit_buffer.clear();
            state.edit_error = None;
            state.rebuild_doc();
            state.mode = Mode::Normal;
            state.pending_key = super::app::PendingKey::None;
            state
        }
        Err(e) => {
            state.edit_error = Some(format!("line {}, col {}: {}", e.line, e.col, e.message));
            state
        }
    }
}

fn edit_insert_char(mut state: AppState, ch: char) -> AppState {
    let row = state.edit_cursor_row;
    if row >= state.edit_buffer.len() {
        state.edit_buffer.push(String::new());
    }
    let col = state.edit_cursor_col.min(state.edit_buffer[row].len());
    state.edit_buffer[row].insert(col, ch);
    state.edit_cursor_col = col + ch.len_utf8();
    state.edit_error = None;
    state
}

fn edit_backspace(mut state: AppState) -> AppState {
    let row = state.edit_cursor_row;
    let col = state.edit_cursor_col;

    if col > 0 {
        let line = &state.edit_buffer[row];
        // Find the byte position of the character before col
        let prev_char_start = line[..col]
            .char_indices()
            .next_back()
            .map(|(i, _)| i)
            .unwrap_or(0);
        state.edit_buffer[row].remove(prev_char_start);
        state.edit_cursor_col = prev_char_start;
    } else if row > 0 {
        // Join with previous line
        let current_line = state.edit_buffer.remove(row);
        state.edit_cursor_row = row - 1;
        state.edit_cursor_col = state.edit_buffer[row - 1].len();
        state.edit_buffer[row - 1].push_str(&current_line);
    }
    state.edit_error = None;
    state
}

fn edit_delete(mut state: AppState) -> AppState {
    let row = state.edit_cursor_row;
    let col = state.edit_cursor_col;
    let line_len = state.edit_buffer.get(row).map(|l| l.len()).unwrap_or(0);

    if col < line_len {
        state.edit_buffer[row].remove(col);
    } else if row + 1 < state.edit_buffer.len() {
        // Join with next line
        let next_line = state.edit_buffer.remove(row + 1);
        state.edit_buffer[row].push_str(&next_line);
    }
    state.edit_error = None;
    state
}

fn edit_newline(mut state: AppState) -> AppState {
    let row = state.edit_cursor_row;
    let col = state
        .edit_cursor_col
        .min(state.edit_buffer.get(row).map(|l| l.len()).unwrap_or(0));

    let rest = state.edit_buffer[row][col..].to_string();
    state.edit_buffer[row].truncate(col);
    state.edit_buffer.insert(row + 1, rest);
    state.edit_cursor_row = row + 1;
    state.edit_cursor_col = 0;
    state.edit_error = None;
    state
}

fn edit_cursor_left(mut state: AppState) -> AppState {
    if state.edit_cursor_col > 0 {
        let line = &state.edit_buffer[state.edit_cursor_row];
        // Move back one character
        state.edit_cursor_col = line[..state.edit_cursor_col]
            .char_indices()
            .next_back()
            .map(|(i, _)| i)
            .unwrap_or(0);
    }
    state
}

fn edit_cursor_right(mut state: AppState) -> AppState {
    let line_len = state
        .edit_buffer
        .get(state.edit_cursor_row)
        .map(|l| l.len())
        .unwrap_or(0);
    if state.edit_cursor_col < line_len {
        let line = &state.edit_buffer[state.edit_cursor_row];
        let ch = line[state.edit_cursor_col..]
            .chars()
            .next()
            .map(|c| c.len_utf8())
            .unwrap_or(0);
        state.edit_cursor_col += ch;
    }
    state
}

fn edit_cursor_up(mut state: AppState) -> AppState {
    if state.edit_cursor_row > 0 {
        state.edit_cursor_row -= 1;
        let line_len = state
            .edit_buffer
            .get(state.edit_cursor_row)
            .map(|l| l.len())
            .unwrap_or(0);
        state.edit_cursor_col = state.edit_cursor_col.min(line_len);
    }
    state
}

fn edit_cursor_down(mut state: AppState) -> AppState {
    if state.edit_cursor_row + 1 < state.edit_buffer.len() {
        state.edit_cursor_row += 1;
        let line_len = state
            .edit_buffer
            .get(state.edit_cursor_row)
            .map(|l| l.len())
            .unwrap_or(0);
        state.edit_cursor_col = state.edit_cursor_col.min(line_len);
    }
    state
}

/// Enter rename mode for the focused element.
pub fn enter_rename_mode(state: AppState) -> AppState {
    let target = match state.focus() {
        Some(FocusTarget::Table(name)) => RenameTarget::Table(name.clone()),
        Some(FocusTarget::Column(table, col)) => RenameTarget::Column(table.clone(), col.clone()),
        _ => return state, // Can only rename tables and columns
    };

    let mut state = state.ensure_original_schema();
    state.rename_target = Some(target);
    state.rename_buf = String::new();
    state.mode = Mode::Rename;
    state.pending_key = super::app::PendingKey::None;
    state
}

/// Handle a key event in Rename mode.
pub fn handle_rename(state: AppState, key: KeyEvent) -> AppState {
    // Allow Ctrl-c to propagate (handled at top level)
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return state;
    }

    match key.code {
        KeyCode::Esc => cancel_rename(state),
        KeyCode::Enter => confirm_rename(state),
        KeyCode::Backspace => {
            let mut state = state;
            state.rename_buf.pop();
            state
        }
        KeyCode::Char(ch) if ch.is_ascii_alphanumeric() || ch == '_' => {
            let mut state = state;
            state.rename_buf.push(ch);
            state
        }
        _ => state,
    }
}

fn cancel_rename(mut state: AppState) -> AppState {
    state.rename_target = None;
    state.rename_buf.clear();
    state.with_mode(Mode::Normal)
}

fn confirm_rename(mut state: AppState) -> AppState {
    let new_name = state.rename_buf.clone();
    if new_name.is_empty() {
        return cancel_rename(state);
    }

    let target = match state.rename_target.take() {
        Some(t) => t,
        None => return state.with_mode(Mode::Normal),
    };

    match target {
        RenameTarget::Table(old_name) => {
            if old_name == new_name {
                return state.with_mode(Mode::Normal);
            }
            if let Some(mut table) = state.schema.tables.remove(&old_name) {
                table.name = new_name.clone();
                state.schema.add_table(table);
                state.renames.push(RenameMetadata {
                    table: old_name.clone(),
                    from: old_name.clone(),
                    to: new_name.clone(),
                });
                state.edited_tables.remove(&old_name);
                state.edited_tables.insert(new_name);
                state = state.ensure_original_schema();
                state.rebuild_doc();
            }
        }
        RenameTarget::Column(table_name, old_col_name) => {
            if old_col_name == new_name {
                return state.with_mode(Mode::Normal);
            }
            if let Some(table) = state.schema.tables.get_mut(&table_name) {
                // Rename the column in the table
                for col in &mut table.columns {
                    if col.name == old_col_name {
                        col.name = new_name.clone();
                        break;
                    }
                }
                // Also rename in constraints that reference this column
                for constraint in &mut table.constraints {
                    rename_column_in_constraint(constraint, &old_col_name, &new_name);
                }
                // Also rename in indexes
                for index in &mut table.indexes {
                    for col in &mut index.columns {
                        if col == &old_col_name {
                            *col = new_name.clone();
                        }
                    }
                }
                state.renames.push(RenameMetadata {
                    table: table_name.clone(),
                    from: old_col_name,
                    to: new_name,
                });
                state.edited_tables.insert(table_name);
                state = state.ensure_original_schema();
                state.rebuild_doc();
            }
        }
    }

    state.rename_buf.clear();
    state.mode = Mode::Normal;
    state.pending_key = super::app::PendingKey::None;
    state
}

fn rename_column_in_constraint(
    constraint: &mut crate::schema::Constraint,
    old_name: &str,
    new_name: &str,
) {
    match constraint {
        crate::schema::Constraint::PrimaryKey { columns, .. }
        | crate::schema::Constraint::Unique { columns, .. }
        | crate::schema::Constraint::ForeignKey { columns, .. } => {
            for col in columns {
                if col == old_name {
                    *col = new_name.to_string();
                }
            }
        }
        crate::schema::Constraint::Check { .. } => {
            // Check constraint expressions use raw text; don't attempt rename
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{Expression, PgType};
    use crate::schema::{Column, Constraint, Schema, Table};
    use crate::tui::app::AppState;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: crossterm::event::KeyEventKind::Press,
            state: crossterm::event::KeyEventState::NONE,
        }
    }

    fn users_table() -> Table {
        let mut table = Table::new("users");
        table.add_column(
            Column::new("id", PgType::Uuid)
                .with_default(Expression::FunctionCall("gen_random_uuid()".into())),
        );
        table.add_column(Column::new("email", PgType::Text));
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        table.add_constraint(Constraint::Unique {
            name: Some("users_email_key".into()),
            columns: vec!["email".into()],
        });
        table
    }

    fn state_with_users() -> AppState {
        let mut schema = Schema::new();
        schema.add_table(users_table());
        AppState::new(schema, String::new())
            .with_viewport_height(20)
            .toggle_expand() // expand "users" table
    }

    // ── Edit mode entry/exit ──────────────────────────────────────

    #[test]
    fn enter_edit_on_table_header() {
        let state = state_with_users();
        assert_eq!(state.mode, Mode::Normal);

        let state = enter_edit_mode(state);
        assert_eq!(state.mode, Mode::Edit);
        assert_eq!(state.edit_table, Some("users".into()));
        assert!(!state.edit_buffer.is_empty());
        assert!(state.edit_buffer[0].contains("table users"));
    }

    #[test]
    fn enter_edit_on_column_uses_parent_table() {
        let state = state_with_users().cursor_down(1); // move to first column
        assert!(matches!(state.focus(), Some(FocusTarget::Column(_, _))));

        let state = enter_edit_mode(state);
        assert_eq!(state.mode, Mode::Edit);
        assert_eq!(state.edit_table, Some("users".into()));
    }

    #[test]
    fn enter_edit_on_blank_line_does_nothing() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("a"));
        schema.add_table(Table::new("b"));
        let state = AppState::new(schema, String::new())
            .with_viewport_height(20)
            .cursor_down(1); // blank between tables

        assert!(matches!(state.focus(), Some(FocusTarget::Blank)));
        let state = enter_edit_mode(state);
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn edit_sets_original_schema() {
        let state = state_with_users();
        assert!(state.original_schema.is_none());

        let state = enter_edit_mode(state);
        assert!(state.original_schema.is_some());
    }

    // ── Edit→parse round-trip ─────────────────────────────────────

    #[test]
    fn edit_exit_without_changes_returns_to_normal() {
        let state = state_with_users();

        let state = enter_edit_mode(state);
        // Exit immediately without editing
        let state = handle_edit(state, key(KeyCode::Esc));

        assert_eq!(state.mode, Mode::Normal);
        assert!(state.edit_error.is_none());
        // Table still exists with same structure (constraint names are stripped
        // by the render→parse roundtrip, but columns and types are preserved)
        let table = state.schema.table("users").expect("users table");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[1].name, "email");
        assert!(table.primary_key().is_some());
    }

    #[test]
    fn edit_add_column_round_trip() {
        let state = state_with_users();
        let state = enter_edit_mode(state);

        // Find the closing brace line and insert a new column before it
        let close_idx = state
            .edit_buffer
            .iter()
            .position(|l| l.trim() == "}")
            .expect("should have closing brace");

        let mut state = state;
        // Navigate to just before the closing brace
        state.edit_cursor_row = close_idx;
        state.edit_cursor_col = 0;

        // Insert a new column line: "    name  text  NOT NULL\n"
        let new_line = "    name  text  NOT NULL";
        state.edit_buffer.insert(close_idx, new_line.to_string());

        // Exit edit mode (parse)
        let state = handle_edit(state, key(KeyCode::Esc));

        assert_eq!(state.mode, Mode::Normal);
        assert!(state.edit_error.is_none());
        let table = state.schema.table("users").expect("users table");
        assert_eq!(table.columns.len(), 3);
        assert!(table.column("name").is_some());
        assert!(state.edited_tables.contains("users"));
    }

    #[test]
    fn edit_parse_error_stays_in_edit_mode() {
        let state = state_with_users();
        let state = enter_edit_mode(state);

        // Corrupt the buffer
        let mut state = state;
        state.edit_buffer[0] = "invalid !!!".to_string();

        let state = handle_edit(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Edit);
        assert!(state.edit_error.is_some());
    }

    // ── Text editing operations ────────────────────────────────────

    #[test]
    fn edit_insert_char() {
        let state = state_with_users();
        let mut state = enter_edit_mode(state);
        state.edit_cursor_row = 0;
        state.edit_cursor_col = 0;

        let state = handle_edit(state, key(KeyCode::Char('x')));
        assert!(state.edit_buffer[0].starts_with('x'));
        assert_eq!(state.edit_cursor_col, 1);
    }

    #[test]
    fn edit_backspace_within_line() {
        let state = state_with_users();
        let mut state = enter_edit_mode(state);
        state.edit_cursor_row = 0;
        state.edit_cursor_col = 5;

        let before = state.edit_buffer[0].clone();
        let state = handle_edit(state, key(KeyCode::Backspace));
        assert_eq!(state.edit_buffer[0].len(), before.len() - 1);
        assert_eq!(state.edit_cursor_col, 4);
    }

    #[test]
    fn edit_backspace_joins_lines() {
        let state = state_with_users();
        let mut state = enter_edit_mode(state);
        let line_count = state.edit_buffer.len();
        state.edit_cursor_row = 1;
        state.edit_cursor_col = 0;

        let state = handle_edit(state, key(KeyCode::Backspace));
        assert_eq!(state.edit_buffer.len(), line_count - 1);
        assert_eq!(state.edit_cursor_row, 0);
    }

    #[test]
    fn edit_enter_splits_line() {
        let state = state_with_users();
        let mut state = enter_edit_mode(state);
        let line_count = state.edit_buffer.len();
        state.edit_cursor_row = 0;
        state.edit_cursor_col = 5;

        let state = handle_edit(state, key(KeyCode::Enter));
        assert_eq!(state.edit_buffer.len(), line_count + 1);
        assert_eq!(state.edit_cursor_row, 1);
        assert_eq!(state.edit_cursor_col, 0);
    }

    #[test]
    fn edit_cursor_movement() {
        let state = state_with_users();
        let mut state = enter_edit_mode(state);
        state.edit_cursor_row = 0;
        state.edit_cursor_col = 3;

        let state = handle_edit(state, key(KeyCode::Left));
        assert_eq!(state.edit_cursor_col, 2);

        let state = handle_edit(state, key(KeyCode::Right));
        assert_eq!(state.edit_cursor_col, 3);

        let state = handle_edit(state, key(KeyCode::Down));
        assert_eq!(state.edit_cursor_row, 1);

        let state = handle_edit(state, key(KeyCode::Up));
        assert_eq!(state.edit_cursor_row, 0);

        let state = handle_edit(state, key(KeyCode::Home));
        assert_eq!(state.edit_cursor_col, 0);

        let state = handle_edit(state, key(KeyCode::End));
        assert_eq!(state.edit_cursor_col, state.edit_buffer[0].len());
    }

    // ── Rename mode ──────────────────────────────────────────────

    #[test]
    fn rename_table() {
        let state = state_with_users();
        assert!(matches!(
            state.focus(),
            Some(FocusTarget::Table(ref n)) if n == "users"
        ));

        let state = enter_rename_mode(state);
        assert_eq!(state.mode, Mode::Rename);

        // Type "accounts"
        let state = handle_rename(state, key(KeyCode::Char('a')));
        let state = handle_rename(state, key(KeyCode::Char('c')));
        let state = handle_rename(state, key(KeyCode::Char('c')));
        let state = handle_rename(state, key(KeyCode::Char('o')));
        let state = handle_rename(state, key(KeyCode::Char('u')));
        let state = handle_rename(state, key(KeyCode::Char('n')));
        let state = handle_rename(state, key(KeyCode::Char('t')));
        let state = handle_rename(state, key(KeyCode::Char('s')));
        let state = handle_rename(state, key(KeyCode::Enter));

        assert_eq!(state.mode, Mode::Normal);
        assert!(state.schema.table("accounts").is_some());
        assert!(state.schema.table("users").is_none());
        assert_eq!(state.renames.len(), 1);
        assert_eq!(state.renames[0].from, "users");
        assert_eq!(state.renames[0].to, "accounts");
        assert!(state.edited_tables.contains("accounts"));
    }

    #[test]
    fn rename_column() {
        let state = state_with_users().cursor_down(1); // focus on first column
        assert!(matches!(
            state.focus(),
            Some(FocusTarget::Column(_, ref col)) if col == "id"
        ));

        let state = enter_rename_mode(state);
        assert_eq!(state.mode, Mode::Rename);

        // Type "user_id"
        let state = "user_id"
            .chars()
            .fold(state, |s, ch| handle_rename(s, key(KeyCode::Char(ch))));
        let state = handle_rename(state, key(KeyCode::Enter));

        assert_eq!(state.mode, Mode::Normal);
        let table = state.schema.table("users").expect("users table");
        assert!(table.column("user_id").is_some());
        assert!(table.column("id").is_none());
        assert_eq!(state.renames.len(), 1);
        assert_eq!(state.renames[0].table, "users");
        assert_eq!(state.renames[0].from, "id");
        assert_eq!(state.renames[0].to, "user_id");
    }

    #[test]
    fn rename_column_updates_pk_constraint() {
        let state = state_with_users().cursor_down(1); // focus on "id" column
        let state = enter_rename_mode(state);

        let state = "uid"
            .chars()
            .fold(state, |s, ch| handle_rename(s, key(KeyCode::Char(ch))));
        let state = handle_rename(state, key(KeyCode::Enter));

        let table = state.schema.table("users").expect("users table");
        let pk = table.primary_key().expect("should have PK");
        match pk {
            Constraint::PrimaryKey { columns, .. } => {
                assert_eq!(columns, &["uid"]);
            }
            _ => panic!("expected PK"),
        }
    }

    #[test]
    fn rename_cancel_on_esc() {
        let state = state_with_users();
        let state = enter_rename_mode(state);
        let state = handle_rename(state, key(KeyCode::Char('x')));
        let state = handle_rename(state, key(KeyCode::Esc));

        assert_eq!(state.mode, Mode::Normal);
        assert!(state.renames.is_empty());
        assert!(state.schema.table("users").is_some());
    }

    #[test]
    fn rename_empty_name_cancels() {
        let state = state_with_users();
        let state = enter_rename_mode(state);
        let state = handle_rename(state, key(KeyCode::Enter)); // confirm with empty name

        assert_eq!(state.mode, Mode::Normal);
        assert!(state.renames.is_empty());
    }

    #[test]
    fn rename_same_name_is_noop() {
        let state = state_with_users();
        let state = enter_rename_mode(state);

        // Type the same name "users"
        let state = "users"
            .chars()
            .fold(state, |s, ch| handle_rename(s, key(KeyCode::Char(ch))));
        let state = handle_rename(state, key(KeyCode::Enter));

        assert_eq!(state.mode, Mode::Normal);
        assert!(state.renames.is_empty()); // no rename recorded
    }

    #[test]
    fn rename_on_blank_does_nothing() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("a"));
        schema.add_table(Table::new("b"));
        let state = AppState::new(schema, String::new())
            .with_viewport_height(20)
            .cursor_down(1); // blank line

        let state = enter_rename_mode(state);
        assert_eq!(state.mode, Mode::Normal); // Can't rename blank
    }

    // ── Multiple edits accumulate ─────────────────────────────────

    #[test]
    fn multiple_edits_accumulate() {
        let mut schema = Schema::new();
        let mut alpha = Table::new("alpha");
        alpha.add_column(Column::new("id", PgType::Uuid));
        schema.add_table(alpha);

        let mut bravo = Table::new("bravo");
        bravo.add_column(Column::new("id", PgType::Uuid));
        schema.add_table(bravo);

        let state = AppState::new(schema, String::new()).with_viewport_height(20);
        // BTreeMap orders: alpha(0), blank(1), bravo(2)
        assert!(matches!(
            state.focus(),
            Some(FocusTarget::Table(ref n)) if n == "alpha"
        ));

        // Edit first table (alpha)
        let state = enter_edit_mode(state);
        let state = handle_edit(state, key(KeyCode::Esc));
        assert!(state.edited_tables.contains("alpha"));

        // Navigate to second table and edit
        let state = state.next_table();
        assert!(matches!(
            state.focus(),
            Some(FocusTarget::Table(ref n)) if n == "bravo"
        ));
        let state = enter_edit_mode(state);
        let state = handle_edit(state, key(KeyCode::Esc));
        assert!(state.edited_tables.contains("bravo"));

        // Both should be marked as edited
        assert_eq!(state.edited_tables.len(), 2);
        // Original schema should be set only once
        assert!(state.original_schema.is_some());
    }

    // ── Original schema tracking ──────────────────────────────────

    #[test]
    fn original_schema_set_once() {
        let state = state_with_users();
        let original = state.schema.clone();

        let state = enter_edit_mode(state);
        let state = handle_edit(state, key(KeyCode::Esc));

        // Original should match what we started with
        assert_eq!(state.original_schema.as_ref(), Some(&original));

        // Second edit shouldn't change original
        let state = enter_edit_mode(state);
        let state = handle_edit(state, key(KeyCode::Esc));
        assert_eq!(state.original_schema.as_ref(), Some(&original));
    }
}
