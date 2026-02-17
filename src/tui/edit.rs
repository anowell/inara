use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::{AppState, DefaultPromptTarget, FocusTarget, Mode, RenameMetadata, RenameTarget};
use crate::schema::render::render_single_table;
use crate::schema::types::Expression;
use crate::schema::{Constraint, Index};

/// An editor request produced by the `e` keybinding.
///
/// The event loop receives this and spawns `$EDITOR` with the rendered text.
#[derive(Debug, Clone)]
pub struct EditorRequest {
    pub table_name: String,
    pub rendered_text: String,
}

// ── Quick actions (column-context only) ─────────────────────────

/// Toggle nullable for the focused column.
pub fn toggle_nullable(state: AppState) -> AppState {
    let (table_name, col_name) = match state.focus() {
        Some(FocusTarget::Column(t, c)) => (t.clone(), c.clone()),
        _ => return state,
    };

    let mut state = state.ensure_original_schema();
    if let Some(table) = state.schema.tables.get_mut(&table_name) {
        if let Some(col) = table.columns.iter_mut().find(|c| c.name == col_name) {
            col.nullable = !col.nullable;
        }
    }
    state.edited_tables.insert(table_name);
    state.rebuild_doc();
    state
}

/// Toggle a single-column UNIQUE constraint for the focused column.
///
/// If a single-column UNIQUE already exists on this column, removes it.
/// Multi-column UNIQUE constraints are never touched.
pub fn toggle_column_unique(state: AppState) -> AppState {
    let (table_name, col_name) = match state.focus() {
        Some(FocusTarget::Column(t, c)) => (t.clone(), c.clone()),
        _ => return state,
    };

    let mut state = state.ensure_original_schema();
    if let Some(table) = state.schema.tables.get_mut(&table_name) {
        // Check if a single-column UNIQUE exists for this column
        let existing_idx = table.constraints.iter().position(|c| {
            matches!(c, Constraint::Unique { columns, .. } if columns.len() == 1 && columns[0] == col_name)
        });

        if let Some(idx) = existing_idx {
            table.constraints.remove(idx);
        } else {
            let name = format!("{table_name}_{col_name}_key");
            table.add_constraint(Constraint::Unique {
                name: Some(name),
                columns: vec![col_name],
            });
        }
    }
    state.edited_tables.insert(table_name);
    state.rebuild_doc();
    state
}

/// Toggle a single-column btree index for the focused column.
///
/// If a single-column index already exists on this column, removes it.
/// Multi-column indexes are never touched.
pub fn toggle_column_index(state: AppState) -> AppState {
    let (table_name, col_name) = match state.focus() {
        Some(FocusTarget::Column(t, c)) => (t.clone(), c.clone()),
        _ => return state,
    };

    let mut state = state.ensure_original_schema();
    if let Some(table) = state.schema.tables.get_mut(&table_name) {
        let existing_idx = table
            .indexes
            .iter()
            .position(|idx| idx.columns.len() == 1 && idx.columns[0] == col_name);

        if let Some(idx) = existing_idx {
            table.indexes.remove(idx);
        } else {
            let name = format!("{table_name}_{col_name}_idx");
            table.indexes.push(Index {
                name,
                columns: vec![col_name],
                unique: false,
                partial: None,
            });
        }
    }
    state.edited_tables.insert(table_name);
    state.rebuild_doc();
    state
}

// ── Default prompt ──────────────────────────────────────────────

/// Enter DefaultPrompt mode for the focused column.
///
/// Pre-fills the prompt buffer with the column's current default (if any).
pub fn enter_default_prompt(state: AppState) -> AppState {
    let (table_name, col_name) = match state.focus() {
        Some(FocusTarget::Column(t, c)) => (t.clone(), c.clone()),
        _ => return state,
    };

    let current_default = state
        .schema
        .table(&table_name)
        .and_then(|t| t.column(&col_name))
        .and_then(|c| c.default.as_ref())
        .map(|d| d.to_string())
        .unwrap_or_default();

    let mut state = state.ensure_original_schema();
    state.default_prompt_target = Some(DefaultPromptTarget {
        table: table_name,
        column: col_name,
    });
    state.default_prompt_buf = current_default;
    state.mode = Mode::DefaultPrompt;
    state.pending_key = super::app::PendingKey::None;
    state
}

/// Handle a key event in DefaultPrompt mode.
pub fn handle_default_prompt(state: AppState, key: KeyEvent) -> AppState {
    // Allow Ctrl-c to propagate (handled at top level)
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return state;
    }

    match key.code {
        KeyCode::Esc => cancel_default_prompt(state),
        KeyCode::Enter => confirm_default_prompt(state),
        KeyCode::Backspace => {
            let mut state = state;
            state.default_prompt_buf.pop();
            state
        }
        KeyCode::Char(ch) => {
            let mut state = state;
            state.default_prompt_buf.push(ch);
            state
        }
        _ => state,
    }
}

fn cancel_default_prompt(mut state: AppState) -> AppState {
    state.default_prompt_target = None;
    state.default_prompt_buf.clear();
    state.with_mode(Mode::Normal)
}

fn confirm_default_prompt(mut state: AppState) -> AppState {
    let target = match state.default_prompt_target.take() {
        Some(t) => t,
        None => return state.with_mode(Mode::Normal),
    };

    let text = state.default_prompt_buf.trim().to_string();
    state.default_prompt_buf.clear();

    if let Some(table) = state.schema.tables.get_mut(&target.table) {
        if let Some(col) = table.columns.iter_mut().find(|c| c.name == target.column) {
            if text.is_empty() {
                col.default = None;
            } else {
                col.default = Some(classify_expression(&text));
            }
        }
    }

    state.edited_tables.insert(target.table);
    state.rebuild_doc();
    state.mode = Mode::Normal;
    state.pending_key = super::app::PendingKey::None;
    state
}

/// Classify a default expression string into an Expression variant.
///
/// Reuses the same logic as the parser (parse.rs:370-380).
fn classify_expression(s: &str) -> Expression {
    if s.contains('(') && s.ends_with(')') {
        Expression::FunctionCall(s.to_string())
    } else if s.starts_with('\'')
        || s.starts_with('-')
        || s.chars().next().is_some_and(|c| c.is_ascii_digit())
    {
        Expression::Literal(s.to_string())
    } else {
        Expression::Raw(s.to_string())
    }
}

// ── External editor request ─────────────────────────────────────

/// Prepare an editor request for the table under the cursor.
///
/// Returns `(state, Some(EditorRequest))` if a table is focused,
/// or `(state, None)` if no table context is available.
pub fn prepare_editor_request(state: AppState) -> (AppState, Option<EditorRequest>) {
    let table_name = match state.focus() {
        Some(target) => target.table_name().map(|s| s.to_string()),
        None => return (state, None),
    };

    let table_name = match table_name {
        Some(name) => name,
        None => return (state, None),
    };

    let table = match state.schema.table(&table_name) {
        Some(t) => t,
        None => return (state, None),
    };

    let rendered = render_single_table(table);

    let request = EditorRequest {
        table_name,
        rendered_text: rendered,
    };

    (state, Some(request))
}

// ── Rename mode (unchanged from original) ───────────────────────

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

    // ── Quick actions: toggle_nullable ───────────────────────────

    #[test]
    fn toggle_nullable_flips_true_to_false() {
        let state = state_with_users().cursor_down(2); // "email" column (NOT NULL)
        assert!(matches!(
            state.focus(),
            Some(FocusTarget::Column(_, ref c)) if c == "email"
        ));

        let col = state
            .schema
            .table("users")
            .unwrap()
            .column("email")
            .unwrap();
        assert!(!col.nullable);

        let state = toggle_nullable(state);
        let col = state
            .schema
            .table("users")
            .unwrap()
            .column("email")
            .unwrap();
        assert!(col.nullable);
        assert!(state.edited_tables.contains("users"));
        assert!(state.original_schema.is_some());
    }

    #[test]
    fn toggle_nullable_flips_false_to_true() {
        let state = state_with_users().cursor_down(2);
        let state = toggle_nullable(state); // make nullable
        let state = toggle_nullable(state); // make NOT NULL again
        let col = state
            .schema
            .table("users")
            .unwrap()
            .column("email")
            .unwrap();
        assert!(!col.nullable);
    }

    #[test]
    fn toggle_nullable_noop_on_non_column() {
        let state = state_with_users(); // cursor on table header
        let state = toggle_nullable(state);
        assert!(state.edited_tables.is_empty());
    }

    // ── Quick actions: toggle_column_unique ──────────────────────

    #[test]
    fn toggle_unique_adds_constraint() {
        // Start with a table that has no unique on "id" (just a PK)
        let state = state_with_users().cursor_down(1); // "id" column
        let table = state.schema.table("users").unwrap();
        let has_unique_on_id = table
            .constraints
            .iter()
            .any(|c| matches!(c, Constraint::Unique { columns, .. } if columns == &["id"]));
        assert!(!has_unique_on_id);

        let state = toggle_column_unique(state);
        let table = state.schema.table("users").unwrap();
        let has_unique_on_id = table
            .constraints
            .iter()
            .any(|c| matches!(c, Constraint::Unique { columns, .. } if columns == &["id"]));
        assert!(has_unique_on_id);
        assert!(state.edited_tables.contains("users"));
    }

    #[test]
    fn toggle_unique_removes_existing() {
        let state = state_with_users().cursor_down(2); // "email" column (has unique)
        let table = state.schema.table("users").unwrap();
        let has_unique_on_email = table
            .constraints
            .iter()
            .any(|c| matches!(c, Constraint::Unique { columns, .. } if columns == &["email"]));
        assert!(has_unique_on_email);

        let state = toggle_column_unique(state);
        let table = state.schema.table("users").unwrap();
        let has_unique_on_email = table
            .constraints
            .iter()
            .any(|c| matches!(c, Constraint::Unique { columns, .. } if columns == &["email"]));
        assert!(!has_unique_on_email);
    }

    #[test]
    fn toggle_unique_preserves_multi_col() {
        let mut schema = Schema::new();
        let mut table = Table::new("orders");
        table.add_column(Column::new("user_id", PgType::Uuid));
        table.add_column(Column::new("product_id", PgType::Uuid));
        table.add_constraint(Constraint::Unique {
            name: Some("orders_user_product_key".into()),
            columns: vec!["user_id".into(), "product_id".into()],
        });
        schema.add_table(table);

        let mut state = AppState::new(schema, String::new())
            .with_viewport_height(20)
            .toggle_expand();
        state = state.cursor_down(1); // "product_id" column (BTreeMap order)

        let state = toggle_column_unique(state);
        let table = state.schema.table("orders").unwrap();
        // Multi-col unique should still exist
        let multi = table
            .constraints
            .iter()
            .any(|c| matches!(c, Constraint::Unique { columns, .. } if columns.len() == 2));
        assert!(multi, "Multi-column unique should be preserved");
    }

    #[test]
    fn toggle_unique_noop_on_non_column() {
        let state = state_with_users(); // table header
        let state = toggle_column_unique(state);
        assert!(state.edited_tables.is_empty());
    }

    // ── Quick actions: toggle_column_index ───────────────────────

    #[test]
    fn toggle_index_adds_index() {
        let state = state_with_users().cursor_down(2); // "email"
        let table = state.schema.table("users").unwrap();
        let has_idx = table
            .indexes
            .iter()
            .any(|i| i.columns == ["email"] && i.columns.len() == 1);
        assert!(!has_idx);

        let state = toggle_column_index(state);
        let table = state.schema.table("users").unwrap();
        let idx = table
            .indexes
            .iter()
            .find(|i| i.columns == ["email"] && i.columns.len() == 1);
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().name, "users_email_idx");
        assert!(!idx.unwrap().unique);
        assert!(state.edited_tables.contains("users"));
    }

    #[test]
    fn toggle_index_removes_existing() {
        let state = state_with_users().cursor_down(2);
        let state = toggle_column_index(state); // add
        let state = toggle_column_index(state); // remove
        let table = state.schema.table("users").unwrap();
        let has_idx = table
            .indexes
            .iter()
            .any(|i| i.columns == ["email"] && i.columns.len() == 1);
        assert!(!has_idx);
    }

    #[test]
    fn toggle_index_noop_on_non_column() {
        let state = state_with_users();
        let state = toggle_column_index(state);
        assert!(state.edited_tables.is_empty());
    }

    #[test]
    fn quick_actions_all_call_ensure_original_schema() {
        let state = state_with_users().cursor_down(2);
        assert!(state.original_schema.is_none());

        let state = toggle_nullable(state);
        assert!(state.original_schema.is_some());
    }

    // ── DefaultPrompt mode ──────────────────────────────────────

    #[test]
    fn enter_default_prompt_on_column() {
        let state = state_with_users().cursor_down(1); // "id" column (has default)
        let state = enter_default_prompt(state);

        assert_eq!(state.mode, Mode::DefaultPrompt);
        assert!(state.default_prompt_target.is_some());
        let target = state.default_prompt_target.as_ref().unwrap();
        assert_eq!(target.table, "users");
        assert_eq!(target.column, "id");
        // Pre-filled with existing default
        assert_eq!(state.default_prompt_buf, "gen_random_uuid()");
    }

    #[test]
    fn enter_default_prompt_on_column_without_default() {
        let state = state_with_users().cursor_down(2); // "email" (no default)
        let state = enter_default_prompt(state);

        assert_eq!(state.mode, Mode::DefaultPrompt);
        assert!(state.default_prompt_buf.is_empty());
    }

    #[test]
    fn enter_default_prompt_noop_on_non_column() {
        let state = state_with_users(); // table header
        let state = enter_default_prompt(state);
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn default_prompt_confirm_sets_expression() {
        let state = state_with_users().cursor_down(2); // "email"
        let state = enter_default_prompt(state);

        // Type "now()"
        let state = handle_default_prompt(state, key(KeyCode::Char('n')));
        let state = handle_default_prompt(state, key(KeyCode::Char('o')));
        let state = handle_default_prompt(state, key(KeyCode::Char('w')));
        let state = handle_default_prompt(state, key(KeyCode::Char('(')));
        let state = handle_default_prompt(state, key(KeyCode::Char(')')));
        let state = handle_default_prompt(state, key(KeyCode::Enter));

        assert_eq!(state.mode, Mode::Normal);
        let col = state
            .schema
            .table("users")
            .unwrap()
            .column("email")
            .unwrap();
        assert_eq!(col.default, Some(Expression::FunctionCall("now()".into())));
        assert!(state.edited_tables.contains("users"));
    }

    #[test]
    fn default_prompt_confirm_empty_clears_default() {
        let state = state_with_users().cursor_down(1); // "id" (has default)
        let col = state.schema.table("users").unwrap().column("id").unwrap();
        assert!(col.default.is_some());

        let state = enter_default_prompt(state);
        // Clear the pre-filled buffer
        let mut state = state;
        state.default_prompt_buf.clear();
        let state = handle_default_prompt(state, key(KeyCode::Enter));

        assert_eq!(state.mode, Mode::Normal);
        let col = state.schema.table("users").unwrap().column("id").unwrap();
        assert!(col.default.is_none());
    }

    #[test]
    fn default_prompt_cancel_returns_unchanged() {
        let state = state_with_users().cursor_down(2);
        let state = enter_default_prompt(state);
        let state = handle_default_prompt(state, key(KeyCode::Char('x')));
        let state = handle_default_prompt(state, key(KeyCode::Esc));

        assert_eq!(state.mode, Mode::Normal);
        let col = state
            .schema
            .table("users")
            .unwrap()
            .column("email")
            .unwrap();
        assert!(col.default.is_none()); // not changed
    }

    #[test]
    fn default_prompt_backspace() {
        let state = state_with_users().cursor_down(2);
        let state = enter_default_prompt(state);
        let state = handle_default_prompt(state, key(KeyCode::Char('a')));
        let state = handle_default_prompt(state, key(KeyCode::Char('b')));
        assert_eq!(state.default_prompt_buf, "ab");
        let state = handle_default_prompt(state, key(KeyCode::Backspace));
        assert_eq!(state.default_prompt_buf, "a");
    }

    // ── Expression classification ───────────────────────────────

    #[test]
    fn classify_function_call() {
        assert_eq!(
            classify_expression("now()"),
            Expression::FunctionCall("now()".into())
        );
        assert_eq!(
            classify_expression("gen_random_uuid()"),
            Expression::FunctionCall("gen_random_uuid()".into())
        );
    }

    #[test]
    fn classify_literal() {
        assert_eq!(classify_expression("42"), Expression::Literal("42".into()));
        assert_eq!(
            classify_expression("'hello'"),
            Expression::Literal("'hello'".into())
        );
        assert_eq!(classify_expression("-1"), Expression::Literal("-1".into()));
    }

    #[test]
    fn classify_raw() {
        assert_eq!(
            classify_expression("CURRENT_TIMESTAMP"),
            Expression::Raw("CURRENT_TIMESTAMP".into())
        );
        assert_eq!(classify_expression("true"), Expression::Raw("true".into()));
    }

    // ── Editor request ──────────────────────────────────────────

    #[test]
    fn prepare_editor_request_on_table() {
        let state = state_with_users();
        let (state, request) = prepare_editor_request(state);

        assert!(request.is_some());
        let req = request.unwrap();
        assert_eq!(req.table_name, "users");
        assert!(req.rendered_text.contains("table users"));
        // original_schema is NOT set until spawn_editor confirms changes
        assert!(state.original_schema.is_none());
    }

    #[test]
    fn prepare_editor_request_on_column() {
        let state = state_with_users().cursor_down(1);
        let (_state, request) = prepare_editor_request(state);

        assert!(request.is_some());
        assert_eq!(request.unwrap().table_name, "users");
    }

    #[test]
    fn prepare_editor_request_on_blank_returns_none() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("a"));
        schema.add_table(Table::new("b"));
        let state = AppState::new(schema, String::new())
            .with_viewport_height(20)
            .cursor_down(1); // blank line
        let (_state, request) = prepare_editor_request(state);
        assert!(request.is_none());
    }

    // ── Rename mode (preserved tests) ───────────────────────────

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
        let state = "accounts"
            .chars()
            .fold(state, |s, ch| handle_rename(s, key(KeyCode::Char(ch))));
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

        let state = "users"
            .chars()
            .fold(state, |s, ch| handle_rename(s, key(KeyCode::Char(ch))));
        let state = handle_rename(state, key(KeyCode::Enter));

        assert_eq!(state.mode, Mode::Normal);
        assert!(state.renames.is_empty());
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
        assert_eq!(state.mode, Mode::Normal);
    }
}
