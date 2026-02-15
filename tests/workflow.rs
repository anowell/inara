// Integration tests for the end-to-end edit → diff → migrate workflow.
//
// These tests exercise the full workflow without requiring a database:
// edit a table in-memory → trigger :w → verify generated SQL file content.

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

use inara::schema::types::{Expression, PgType};
use inara::schema::{Column, Constraint, Schema, Table};
use inara::tui::app::{AppState, FocusTarget, Mode};
use inara::tui::edit;

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent {
        code,
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    }
}

/// Simulate typing characters into the given mode.
fn type_string(mut state: AppState, s: &str) -> AppState {
    for ch in s.chars() {
        state = dispatch(state, key(KeyCode::Char(ch)));
    }
    state
}

/// Dispatch a key event without a pool (for non-HUD paths).
fn dispatch(state: AppState, key: KeyEvent) -> AppState {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return state.quit();
    }

    match state.mode {
        Mode::Normal => match key.code {
            KeyCode::Char(':') => state.with_mode(Mode::Command),
            KeyCode::Char('e') => edit::enter_edit_mode(state),
            KeyCode::Char('r') => edit::enter_rename_mode(state),
            KeyCode::Enter => state.toggle_expand(),
            KeyCode::Tab => state.next_table(),
            KeyCode::Char('j') | KeyCode::Down => state.cursor_down(1),
            KeyCode::Char('k') | KeyCode::Up => state.cursor_up(1),
            _ => state,
        },
        Mode::Command => inara::tui::input::handle_command_for_test(state, key),
        Mode::Edit => edit::handle_edit(state, key),
        Mode::Rename => edit::handle_rename(state, key),
        Mode::MigrationPreview => inara::tui::input::handle_migration_preview_for_test(state, key),
        _ => state,
    }
}

/// Build a test schema with users and posts tables.
fn test_schema() -> Schema {
    let mut schema = Schema::new();

    let mut users = Table::new("users");
    users.add_column(Column::new("id", PgType::Uuid));
    users.add_column(Column::new("email", PgType::Text));
    users.add_column(
        Column::new("created_at", PgType::Timestamptz)
            .with_default(Expression::FunctionCall("now()".into())),
    );
    users.add_constraint(Constraint::PrimaryKey {
        name: Some("users_pkey".into()),
        columns: vec!["id".into()],
    });
    schema.add_table(users);

    let mut posts = Table::new("posts");
    posts.add_column(Column::new("id", PgType::Uuid));
    posts.add_column(Column::new("title", PgType::Text));
    posts.add_column(Column::new("body", PgType::Text).nullable());
    posts.add_constraint(Constraint::PrimaryKey {
        name: Some("posts_pkey".into()),
        columns: vec!["id".into()],
    });
    schema.add_table(posts);

    schema
}

/// Navigate to the "users" table (alphabetically after "posts" in BTreeMap).
fn navigate_to_users(state: AppState) -> AppState {
    // BTreeMap order: posts, users. So cursor 0 = posts, Tab = users.
    state.next_table()
}

/// Assert the cursor is on a specific table.
fn assert_on_table(state: &AppState, name: &str) {
    match state.focus() {
        Some(FocusTarget::Table(n)) => assert_eq!(n, name, "expected focus on table {name}"),
        other => panic!("expected Table({name}), got {other:?}"),
    }
}

/// Enter edit mode for the current table, insert a column, and exit.
fn add_column_via_edit(state: AppState, col_text: &str) -> AppState {
    let state = dispatch(state, key(KeyCode::Enter)); // expand table
    let state = dispatch(state, key(KeyCode::Char('e'))); // enter edit mode
    assert_eq!(state.mode, Mode::Edit);

    let mut state = state;
    let close_idx = state
        .edit_buffer
        .iter()
        .position(|l| l.trim() == "}")
        .expect("closing brace");
    state.edit_buffer.insert(close_idx, col_text.to_string());

    let state = dispatch(state, key(KeyCode::Esc));
    if let Some(ref err) = state.edit_error {
        panic!(
            "Parse error after edit: {err}\nBuffer:\n{}",
            state.edit_buffer.join("\n")
        );
    }
    assert_eq!(state.mode, Mode::Normal);
    state
}

// ── Test: edit a table → :w → verify generated SQL ──────────────────

#[test]
fn edit_table_add_column_then_write_migration() {
    let state = AppState::new(test_schema(), "test".into()).with_viewport_height(40);
    let state = navigate_to_users(state);
    assert_on_table(&state, "users");

    let state = add_column_via_edit(state, "    bio           text");
    assert!(state.original_schema.is_some());
    assert!(
        state.edited_tables.contains("users"),
        "edited_tables: {:?}",
        state.edited_tables
    );
    assert!(state.schema.table("users").unwrap().column("bio").is_some());

    // Enter command mode and type :w add_bio_to_users
    let state = dispatch(state, key(KeyCode::Char(':')));
    assert_eq!(state.mode, Mode::Command);
    let state = type_string(state, "w add_bio_to_users");
    let state = dispatch(state, key(KeyCode::Enter));

    // Should be in migration preview
    assert_eq!(state.mode, Mode::MigrationPreview);
    let preview = state.migration_preview.as_ref().expect("preview");
    assert!(
        preview.sql.contains("ADD COLUMN bio"),
        "SQL should contain ADD COLUMN bio, got:\n{}",
        preview.sql
    );
    assert_eq!(preview.description, "add_bio_to_users");

    // Confirm the migration
    let state = dispatch(state, key(KeyCode::Enter));
    assert_eq!(state.mode, Mode::Normal);
    assert!(state.renames.is_empty());
    assert!(state.edited_tables.is_empty());

    let msg = state.status_message.as_deref().unwrap_or("");
    assert!(msg.starts_with("Migration written:"));
    assert!(msg.contains("add_bio_to_users"));

    // Cleanup migration file
    let _ = std::fs::remove_dir_all("migrations");
}

// ── Test: rename column → :w → verify ALTER RENAME (not drop+add) ──

#[test]
fn rename_column_then_write_migration() {
    let state = AppState::new(test_schema(), "test".into()).with_viewport_height(40);
    let state = navigate_to_users(state);
    assert_on_table(&state, "users");

    // Expand users and navigate to email column
    let state = dispatch(state, key(KeyCode::Enter)); // expand users
    let state = dispatch(state, key(KeyCode::Char('j'))); // id column
    let state = dispatch(state, key(KeyCode::Char('j'))); // email column
    assert!(
        matches!(
            state.focus(),
            Some(FocusTarget::Column(t, c)) if t == "users" && c == "email"
        ),
        "expected focus on users.email, got {:?}",
        state.focus()
    );

    let state = dispatch(state, key(KeyCode::Char('r'))); // enter rename mode
    assert_eq!(state.mode, Mode::Rename);

    // Type new name "user_email"
    let state = type_string(state, "user_email");
    let state = dispatch(state, key(KeyCode::Enter)); // confirm rename
    assert_eq!(state.mode, Mode::Normal);

    // Verify rename metadata
    assert_eq!(state.renames.len(), 1);
    assert_eq!(state.renames[0].from, "email");
    assert_eq!(state.renames[0].to, "user_email");

    // Trigger :w
    let state = dispatch(state, key(KeyCode::Char(':')));
    let state = type_string(state, "w rename_email_column");
    let state = dispatch(state, key(KeyCode::Enter));

    assert_eq!(state.mode, Mode::MigrationPreview);
    let preview = state.migration_preview.as_ref().expect("preview");

    // Should have RENAME COLUMN, not DROP+ADD
    assert!(
        preview.sql.contains("RENAME COLUMN email TO user_email"),
        "SQL should contain RENAME COLUMN, got:\n{}",
        preview.sql
    );
    assert!(
        !preview.sql.contains("DROP COLUMN"),
        "Should NOT contain DROP COLUMN for a rename"
    );

    // Confirm
    let state = dispatch(state, key(KeyCode::Enter));
    assert_eq!(state.mode, Mode::Normal);
    assert!(state.renames.is_empty());

    // Cleanup
    let _ = std::fs::remove_dir_all("migrations");
}

// ── Test: multiple table edits → single migration ──────────────────

#[test]
fn multiple_table_edits_single_migration() {
    let state = AppState::new(test_schema(), "test".into()).with_viewport_height(40);

    // Edit posts (first in BTreeMap order): add "author_id" column
    assert_on_table(&state, "posts");
    let state = add_column_via_edit(state, "    author_id     uuid");
    assert!(state.edited_tables.contains("posts"));

    // Navigate to users table and edit: add "phone" column
    let state = state.next_table();
    assert_on_table(&state, "users");
    let state = add_column_via_edit(state, "    phone         text");
    assert!(state.edited_tables.contains("users"));
    assert_eq!(state.edited_tables.len(), 2);

    // Trigger :w
    let state = dispatch(state, key(KeyCode::Char(':')));
    let state = type_string(state, "w multi_table_changes");
    let state = dispatch(state, key(KeyCode::Enter));

    assert_eq!(state.mode, Mode::MigrationPreview);
    let preview = state.migration_preview.as_ref().expect("preview");

    // Should contain changes for both tables
    assert!(preview.sql.contains("posts"), "SQL should mention posts");
    assert!(preview.sql.contains("users"), "SQL should mention users");
    assert!(preview.sql.contains("phone"), "SQL should add phone column");
    assert!(
        preview.sql.contains("author_id"),
        "SQL should add author_id column"
    );

    // Confirm and verify file
    let state = dispatch(state, key(KeyCode::Enter));
    assert_eq!(state.mode, Mode::Normal);
    let msg = state.status_message.as_deref().unwrap_or("");
    assert!(msg.starts_with("Migration written:"));

    // Cleanup
    let _ = std::fs::remove_dir_all("migrations");
}

// ── Test: :w with no edits shows message ───────────────────────────

#[test]
fn write_with_no_edits_shows_message() {
    let state = AppState::new(test_schema(), "test".into()).with_viewport_height(40);

    let state = dispatch(state, key(KeyCode::Char(':')));
    let state = type_string(state, "w");
    let state = dispatch(state, key(KeyCode::Enter));

    assert_eq!(state.mode, Mode::Normal);
    assert_eq!(
        state.status_message.as_deref(),
        Some("No schema changes to migrate")
    );
}

// ── Test: cancel migration preview preserves edit state ────────────

#[test]
fn cancel_preview_preserves_edit_state() {
    let state = AppState::new(test_schema(), "test".into()).with_viewport_height(40);
    let state = navigate_to_users(state);

    // Make an edit
    let state = add_column_via_edit(state, "    nickname      text");
    assert!(state.edited_tables.contains("users"));

    // Open preview
    let state = dispatch(state, key(KeyCode::Char(':')));
    let state = type_string(state, "w test");
    let state = dispatch(state, key(KeyCode::Enter));
    assert_eq!(state.mode, Mode::MigrationPreview);

    // Cancel
    let state = dispatch(state, key(KeyCode::Esc));
    assert_eq!(state.mode, Mode::Normal);

    // Edit state should still be present
    assert!(state.original_schema.is_some());
    assert!(state.edited_tables.contains("users"));
    assert!(state
        .schema
        .table("users")
        .unwrap()
        .column("nickname")
        .is_some());
}

// ── Test: auto-generated description ───────────────────────────────

#[test]
fn auto_generated_description_for_write() {
    let state = AppState::new(test_schema(), "test".into()).with_viewport_height(40);
    let state = navigate_to_users(state);

    let state = add_column_via_edit(state, "    avatar        text");

    // :w without description
    let state = dispatch(state, key(KeyCode::Char(':')));
    let state = type_string(state, "w");
    let state = dispatch(state, key(KeyCode::Enter));

    assert_eq!(state.mode, Mode::MigrationPreview);
    let preview = state.migration_preview.as_ref().expect("preview");
    assert!(
        !preview.description.is_empty(),
        "Should auto-generate a description"
    );
    assert!(
        preview.description.contains("avatar"),
        "Description should mention the changed column, got: {}",
        preview.description
    );
}

// ── Test: write clears edit state so second :w shows no changes ────

#[test]
fn second_write_after_confirm_shows_no_changes() {
    let state = AppState::new(test_schema(), "test".into()).with_viewport_height(40);
    let state = navigate_to_users(state);

    let state = add_column_via_edit(state, "    tmp           text");

    // First :w and confirm
    let state = dispatch(state, key(KeyCode::Char(':')));
    let state = type_string(state, "w first_write");
    let state = dispatch(state, key(KeyCode::Enter));
    let state = dispatch(state, key(KeyCode::Enter)); // confirm

    assert_eq!(state.mode, Mode::Normal);

    // Second :w should show no changes
    let state = dispatch(state, key(KeyCode::Char(':')));
    let state = type_string(state, "w second_write");
    let state = dispatch(state, key(KeyCode::Enter));

    assert_eq!(state.mode, Mode::Normal);
    assert_eq!(
        state.status_message.as_deref(),
        Some("No schema changes to migrate")
    );

    // Cleanup
    let _ = std::fs::remove_dir_all("migrations");
}
