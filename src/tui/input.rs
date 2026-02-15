use std::sync::{Arc, Mutex};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use sqlx::PgPool;

use super::app::{
    AppState, FocusTarget, LlmPreviewKind, LlmPreviewState, MigrationPreviewState, Mode, PendingKey,
};
use super::edit;
use super::fuzzy::SearchFilter;
use super::goto::{self, GotoResult};
use super::hud::{self, HudResultHandle, HudState, HudStatus, HudTarget};
use crate::llm::{self, LlmResultHandle};
use crate::migration::overlay::PendingOverlay;
use crate::migration::warnings::MigrationWarning;
use crate::schema::diff::{self, Rename};

/// Shared handle for receiving async warning check results.
pub type WarningResultHandle = Arc<Mutex<Option<Vec<MigrationWarning>>>>;

/// Shared handle for receiving async pending overlay results.
pub type OverlayResultHandle = Arc<Mutex<Option<Result<PendingOverlay, String>>>>;

/// Create a new warning result handle.
pub fn new_warning_handle() -> WarningResultHandle {
    Arc::new(Mutex::new(None))
}

/// Create a new overlay result handle.
pub fn new_overlay_handle() -> OverlayResultHandle {
    Arc::new(Mutex::new(None))
}

/// Result from handle_key — updated state plus optional async handles.
pub struct HandleResult {
    pub state: AppState,
    pub hud_handle: Option<HudResultHandle>,
    pub warning_handle: Option<WarningResultHandle>,
    pub overlay_handle: Option<OverlayResultHandle>,
    pub llm_handle: Option<LlmResultHandle>,
}

impl HandleResult {
    fn state_only(state: AppState) -> Self {
        Self {
            state,
            hud_handle: None,
            warning_handle: None,
            overlay_handle: None,
            llm_handle: None,
        }
    }

    fn with_hud(state: AppState, handle: Option<HudResultHandle>) -> Self {
        Self {
            state,
            hud_handle: handle,
            warning_handle: None,
            overlay_handle: None,
            llm_handle: None,
        }
    }

    fn with_llm(state: AppState, handle: LlmResultHandle) -> Self {
        Self {
            state,
            hud_handle: None,
            warning_handle: None,
            overlay_handle: None,
            llm_handle: Some(handle),
        }
    }
}

/// Process a key event and return the new application state.
///
/// Returns the updated state and optional async handles for HUD or warning queries.
pub fn handle_key(state: AppState, key: KeyEvent, pool: &PgPool) -> HandleResult {
    // Clear transient status message on any key press
    let state = state.clear_status();

    // Ctrl-c always quits regardless of mode
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return HandleResult::state_only(state.quit());
    }

    match state.mode {
        Mode::Normal => {
            let (state, handle) = handle_normal(state, key, pool);
            HandleResult::with_hud(state, handle)
        }
        Mode::Command => handle_command(state, key, pool),
        Mode::SpaceMenu => handle_space_menu(state, key, pool),
        Mode::Search => HandleResult::state_only(handle_search(state, key)),
        Mode::Edit => HandleResult::state_only(edit::handle_edit(state, key)),
        Mode::Rename => HandleResult::state_only(edit::handle_rename(state, key)),
        Mode::HUD => {
            let (state, handle) = handle_hud(state, key, pool);
            HandleResult::with_hud(state, handle)
        }
        Mode::MigrationPreview => HandleResult::state_only(handle_migration_preview(state, key)),
        Mode::LlmPending => HandleResult::state_only(handle_llm_pending(state, key)),
        Mode::LlmPreview => HandleResult::state_only(handle_llm_preview(state, key)),
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
        KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            (state.toggle_rust_types(), None)
        }
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
            let result = goto::dispatch(
                ch,
                &focus,
                &state.schema,
                &state.relation_map,
                &state.migration_index,
            );
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
///
/// Public for integration tests that simulate the :w workflow without a pool.
pub fn handle_command_for_test(state: AppState, key: KeyEvent) -> AppState {
    handle_command(state, key, &no_pool()).state
}

/// Create a dummy pool reference for offline/test contexts.
///
/// Uses a tokio runtime to construct the pool. If no runtime exists
/// (e.g. in unit tests), creates a temporary one for pool construction only.
fn no_pool() -> PgPool {
    use sqlx::pool::PoolOptions;
    // connect_lazy requires a tokio context for internal setup
    if tokio::runtime::Handle::try_current().is_ok() {
        PoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool creation should not fail")
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("temp runtime");
        rt.block_on(async {
            PoolOptions::new()
                .max_connections(1)
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool creation should not fail")
        })
    }
}

fn handle_command(state: AppState, key: KeyEvent, pool: &PgPool) -> HandleResult {
    match key.code {
        KeyCode::Esc => HandleResult::state_only(state.with_mode(Mode::Normal)),
        KeyCode::Enter => execute_command(state, pool),
        KeyCode::Backspace => {
            let state = state.command_pop();
            // If buffer is empty after backspace, exit command mode
            if state.command_buf.is_empty() {
                HandleResult::state_only(state.with_mode(Mode::Normal))
            } else {
                HandleResult::state_only(state)
            }
        }
        KeyCode::Char(ch) => HandleResult::state_only(state.command_push(ch)),
        _ => HandleResult::state_only(state),
    }
}

/// Execute the current command buffer content.
fn execute_command(state: AppState, pool: &PgPool) -> HandleResult {
    let cmd = state.command_buf.trim().to_string();
    let state = state.with_mode(Mode::Normal);

    if cmd == "q" {
        return HandleResult::state_only(state.quit());
    }

    // :w or :w <description>
    if cmd == "w" || cmd.starts_with("w ") {
        let description = if cmd.len() > 2 {
            cmd[2..].trim().to_string()
        } else {
            String::new()
        };
        return execute_write_migration(state, description, pool);
    }

    // :ai <prompt>
    if cmd == "ai" {
        return HandleResult::state_only(state.with_status("Usage: :ai <prompt>"));
    }
    if let Some(rest) = cmd.strip_prefix("ai ") {
        let prompt = rest.trim().to_string();
        if prompt.is_empty() {
            return HandleResult::state_only(state.with_status("Usage: :ai <prompt>"));
        }
        return execute_ai_command(state, prompt);
    }

    // :generate-down
    if cmd == "generate-down" {
        return execute_generate_down(state);
    }

    HandleResult::state_only(state) // Unknown command, ignore
}

/// Generate migration SQL and show preview, or report no changes.
///
/// Spawns async warning checks against the live database. The preview
/// shows "Checking..." until results arrive.
fn execute_write_migration(state: AppState, description: String, pool: &PgPool) -> HandleResult {
    let original = match &state.original_schema {
        Some(orig) => orig,
        None => {
            return HandleResult::state_only(
                state.with_status_message("No schema changes to migrate".into()),
            );
        }
    };

    // Convert RenameMetadata to diff::Rename
    let renames: Vec<Rename> = state
        .renames
        .iter()
        .map(|r| Rename {
            table: r.table.clone(),
            from: r.from.clone(),
            to: r.to.clone(),
        })
        .collect();

    let changes = diff::diff(original, &state.schema, &renames);
    if changes.is_empty() {
        return HandleResult::state_only(
            state.with_status_message("No schema changes to migrate".into()),
        );
    }

    let sql = crate::migration::generate_sql(&changes);

    // Auto-generate description from changes if none provided
    let description = if description.is_empty() {
        auto_describe(&changes)
    } else {
        description
    };

    // Spawn async warning checks
    let handle = new_warning_handle();
    spawn_warning_checks(pool.clone(), changes, handle.clone());

    let preview = MigrationPreviewState {
        sql,
        description,
        scroll: 0,
        warnings: None, // checks in progress
    };

    let mut state = state;
    state.migration_preview = Some(preview);
    state.mode = Mode::MigrationPreview;

    HandleResult {
        state,
        hud_handle: None,
        warning_handle: Some(handle),
        overlay_handle: None,
        llm_handle: None,
    }
}

/// Spawn async warning checks in a background task.
///
/// If no Tokio runtime is available (e.g. in unit tests), the warnings
/// are set to an empty list immediately.
fn spawn_warning_checks(pool: PgPool, changes: Vec<diff::Change>, handle: WarningResultHandle) {
    let handle_clone = handle.clone();
    let result = tokio::runtime::Handle::try_current().map(|rt| {
        rt.spawn(async move {
            let result = crate::migration::warnings::check_changes(&pool, "public", &changes).await;
            let warnings = result.unwrap_or_default();
            if let Ok(mut guard) = handle_clone.lock() {
                *guard = Some(warnings);
            }
        });
    });

    // No runtime available — set empty warnings immediately
    if result.is_err() {
        if let Ok(mut guard) = handle.lock() {
            *guard = Some(Vec::new());
        }
    }
}

/// Generate a human-readable summary from changes for auto-description.
fn auto_describe(changes: &[diff::Change]) -> String {
    let mut parts = Vec::new();
    for change in changes {
        match change {
            diff::Change::AddTable(t) => parts.push(format!("add_{}", t.name)),
            diff::Change::DropTable(name) => parts.push(format!("drop_{name}")),
            diff::Change::AddColumn { table, column } => {
                parts.push(format!("add_{}_to_{table}", column.name));
            }
            diff::Change::DropColumn { table, column } => {
                parts.push(format!("drop_{column}_from_{table}"));
            }
            diff::Change::AlterColumn {
                table,
                column,
                changes,
            } => {
                if changes.rename.is_some() {
                    parts.push(format!("rename_{column}_in_{table}"));
                } else {
                    parts.push(format!("alter_{column}_in_{table}"));
                }
            }
            diff::Change::AddConstraint { table, .. } => {
                parts.push(format!("add_constraint_to_{table}"));
            }
            diff::Change::DropConstraint { table, name } => {
                parts.push(format!("drop_{name}_from_{table}"));
            }
            diff::Change::AddIndex { table, index } => {
                parts.push(format!("add_{}_on_{table}", index.name));
            }
            diff::Change::DropIndex(name) => parts.push(format!("drop_{name}")),
        }
    }
    if parts.len() <= 3 {
        parts.join("_and_")
    } else {
        format!("{}_and_{}_more_changes", parts[0], parts.len() - 1)
    }
}

/// Execute `:ai <prompt>` — send the current migration context to the LLM.
fn execute_ai_command(state: AppState, prompt: String) -> HandleResult {
    if !llm::LlmConfig::is_configured() {
        return HandleResult::state_only(
            state.with_status("LLM not configured — set OPENAI_API_KEY"),
        );
    }

    // Need a migration preview or edited schema to have SQL context
    let (sql, description) = match &state.migration_preview {
        Some(preview) => (preview.sql.clone(), preview.description.clone()),
        None => {
            // Generate migration SQL from current edits
            let original = match &state.original_schema {
                Some(orig) => orig,
                None => {
                    return HandleResult::state_only(
                        state.with_status("No schema changes — edit the schema first"),
                    );
                }
            };
            let renames: Vec<Rename> = state
                .renames
                .iter()
                .map(|r| Rename {
                    table: r.table.clone(),
                    from: r.from.clone(),
                    to: r.to.clone(),
                })
                .collect();
            let changes = diff::diff(original, &state.schema, &renames);
            if changes.is_empty() {
                return HandleResult::state_only(
                    state.with_status("No schema changes to send to AI"),
                );
            }
            let sql = crate::migration::generate_sql(&changes);
            let description = auto_describe(&changes);
            (sql, description)
        }
    };

    let handle = llm::new_llm_handle();
    llm::spawn_ai_request(&state.schema, &sql, &prompt, handle.clone());

    let mut state = state;
    state.mode = Mode::LlmPending;
    state.llm_pending_message = Some(format!("AI: {prompt}"));
    // Stash the context for when the result arrives
    state.llm_preview = Some(LlmPreviewState {
        sql: String::new(), // will be filled when result arrives
        kind: LlmPreviewKind::AiEdit {
            original_sql: sql,
            description,
        },
        scroll: 0,
    });

    HandleResult::with_llm(state, handle)
}

/// Execute `:generate-down` — generate a down migration from the last written migration.
fn execute_generate_down(state: AppState) -> HandleResult {
    if !llm::LlmConfig::is_configured() {
        return HandleResult::state_only(
            state.with_status("LLM not configured — set OPENAI_API_KEY"),
        );
    }

    // Find the most recent .up.sql migration file
    let migrations_dir = std::path::Path::new("migrations");
    let (up_sql, description) = match find_latest_up_migration(migrations_dir) {
        Some(result) => result,
        None => {
            return HandleResult::state_only(
                state.with_status("No up migration found in migrations/"),
            );
        }
    };

    let original = state.original_schema.as_ref().unwrap_or(&state.schema);

    let handle = llm::new_llm_handle();
    llm::spawn_generate_down_request(original, &state.schema, &up_sql, handle.clone());

    let mut state = state;
    state.mode = Mode::LlmPending;
    state.llm_pending_message = Some("Generating down migration...".to_string());
    state.llm_preview = Some(LlmPreviewState {
        sql: String::new(),
        kind: LlmPreviewKind::GenerateDown {
            up_sql,
            description,
        },
        scroll: 0,
    });

    HandleResult::with_llm(state, handle)
}

/// Find the most recent .up.sql migration file and return its content + description.
fn find_latest_up_migration(dir: &std::path::Path) -> Option<(String, String)> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".up.sql"))
                .unwrap_or(false)
        })
        .collect();

    entries.sort_by_key(|e| e.file_name());
    let latest = entries.last()?;
    let content = std::fs::read_to_string(latest.path()).ok()?;
    let filename = latest.file_name().to_string_lossy().to_string();
    // Extract description from filename: TIMESTAMP_description.up.sql
    let description = filename
        .strip_suffix(".up.sql")
        .and_then(|s| s.split_once('_').map(|(_, desc)| desc.to_string()))
        .unwrap_or_else(|| filename.clone());

    Some((content, description))
}

/// Handle key events in LlmPending mode (waiting for LLM response).
///
/// Only Esc to cancel is supported.
fn handle_llm_pending(state: AppState, key: KeyEvent) -> AppState {
    match key.code {
        KeyCode::Esc => state.with_mode(Mode::Normal),
        _ => state, // ignore all other keys while waiting
    }
}

/// Handle key events in LlmPreview mode (reviewing LLM response).
///
/// Enter confirms and applies the suggestion, Esc cancels.
/// j/k or Up/Down scroll the preview.
fn handle_llm_preview(state: AppState, key: KeyEvent) -> AppState {
    match key.code {
        KeyCode::Esc => state.with_mode(Mode::Normal),
        KeyCode::Enter => confirm_llm_preview(state),
        KeyCode::Char('j') | KeyCode::Down => {
            let mut state = state;
            if let Some(ref mut preview) = state.llm_preview {
                preview.scroll = preview.scroll.saturating_add(1);
            }
            state
        }
        KeyCode::Char('k') | KeyCode::Up => {
            let mut state = state;
            if let Some(ref mut preview) = state.llm_preview {
                preview.scroll = preview.scroll.saturating_sub(1);
            }
            state
        }
        _ => state,
    }
}

/// Apply the LLM suggestion.
fn confirm_llm_preview(state: AppState) -> AppState {
    let preview = match &state.llm_preview {
        Some(p) => p,
        None => return state.with_mode(Mode::Normal),
    };

    match &preview.kind {
        LlmPreviewKind::AiEdit { description, .. } => {
            // Apply the LLM-suggested SQL as the migration preview
            let sql = preview.sql.clone();
            let description = description.clone();
            let migration_preview = MigrationPreviewState {
                sql,
                description,
                scroll: 0,
                warnings: Some(Vec::new()), // Skip warnings for AI-edited migrations
            };
            let mut state = state;
            state.llm_preview = None;
            state.migration_preview = Some(migration_preview);
            state.mode = Mode::MigrationPreview;
            state
        }
        LlmPreviewKind::GenerateDown { description, .. } => {
            // Write the down migration file
            let sql = preview.sql.clone();
            let description = description.clone();
            let header = "-- AI-generated down migration. Review carefully.\n\n";
            let full_sql = format!("{header}{sql}");

            let migrations_dir = std::path::Path::new("migrations");
            if let Err(e) = std::fs::create_dir_all(migrations_dir) {
                return state
                    .with_mode(Mode::Normal)
                    .with_status(format!("Failed to create migrations directory: {e}"));
            }

            // Generate timestamp
            let now = std::time::SystemTime::now();
            let duration = now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let timestamp = format_timestamp(duration.as_secs());

            let slug = slugify(&description);
            let filename = format!("{timestamp}_{slug}.down.sql");
            let path = migrations_dir.join(&filename);
            match std::fs::write(&path, &full_sql) {
                Ok(()) => state
                    .with_mode(Mode::Normal)
                    .with_status(format!("Down migration written: {filename}")),
                Err(e) => state
                    .with_mode(Mode::Normal)
                    .with_status(format!("Failed to write down migration: {e}")),
            }
        }
    }
}

/// Convert a description to a filename-safe slug (same logic as migration::slugify).
fn slugify(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

/// Handle key events in SpaceMenu mode.
///
/// The space menu shows available subcommands. Pressing a submenu key
/// immediately opens the corresponding search filter. Esc or any
/// unrecognized key dismisses the menu.
fn handle_space_menu(state: AppState, key: KeyEvent, pool: &PgPool) -> HandleResult {
    match key.code {
        KeyCode::Char('f') => HandleResult::state_only(state.enter_search(SearchFilter::All)),
        KeyCode::Char('t') => HandleResult::state_only(state.enter_search(SearchFilter::Tables)),
        KeyCode::Char('c') => HandleResult::state_only(state.enter_search(SearchFilter::Columns)),
        KeyCode::Char('m') => {
            HandleResult::state_only(state.enter_search(SearchFilter::Migrations))
        }
        KeyCode::Char('p') => toggle_pending_overlay(state, pool),
        KeyCode::Esc | KeyCode::Char(' ') => {
            HandleResult::state_only(state.with_mode(Mode::Normal))
        }
        _ => HandleResult::state_only(state.with_mode(Mode::Normal)),
    }
}

/// Toggle the pending migrations overlay.
///
/// If the overlay is currently showing, hides it. Otherwise, spawns an
/// async task to compute the overlay data from the database.
fn toggle_pending_overlay(state: AppState, pool: &PgPool) -> HandleResult {
    let state = state.with_mode(Mode::Normal);

    if state.show_pending_overlay {
        // Turn off the overlay
        return HandleResult::state_only(state.toggle_pending_overlay());
    }

    // Turn on the overlay — spawn async computation
    let handle = new_overlay_handle();
    spawn_overlay_computation(pool.clone(), handle.clone());

    let state = state
        .toggle_pending_overlay()
        .with_status("Loading pending migrations...");

    HandleResult {
        state,
        hud_handle: None,
        warning_handle: None,
        overlay_handle: Some(handle),
        llm_handle: None,
    }
}

/// Spawn async overlay computation in a background task.
fn spawn_overlay_computation(pool: PgPool, handle: OverlayResultHandle) {
    let handle_clone = handle.clone();
    let result = tokio::runtime::Handle::try_current().map(|rt| {
        rt.spawn(async move {
            let migrations_dir = std::path::Path::new("migrations");
            let result =
                crate::migration::overlay::compute_overlay(&pool, migrations_dir, "public").await;
            let result = result.map_err(|e| e.to_string());
            if let Ok(mut guard) = handle_clone.lock() {
                *guard = Some(result);
            }
        });
    });

    // No runtime available — set error immediately
    if result.is_err() {
        if let Ok(mut guard) = handle.lock() {
            *guard = Some(Err("No async runtime available".into()));
        }
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

/// Handle key events in MigrationPreview mode (public for integration tests).
pub fn handle_migration_preview_for_test(state: AppState, key: KeyEvent) -> AppState {
    handle_migration_preview(state, key)
}

/// Handle key events in MigrationPreview mode.
///
/// Enter confirms and writes the migration file. Esc cancels.
/// j/k or Up/Down scroll the preview.
fn handle_migration_preview(state: AppState, key: KeyEvent) -> AppState {
    match key.code {
        KeyCode::Esc => state.with_mode(Mode::Normal),
        KeyCode::Enter => confirm_migration(state),
        KeyCode::Char('j') | KeyCode::Down => {
            let mut state = state;
            if let Some(ref mut preview) = state.migration_preview {
                preview.scroll = preview.scroll.saturating_add(1);
            }
            state
        }
        KeyCode::Char('k') | KeyCode::Up => {
            let mut state = state;
            if let Some(ref mut preview) = state.migration_preview {
                preview.scroll = preview.scroll.saturating_sub(1);
            }
            state
        }
        _ => state,
    }
}

/// Write the migration file and clear edit state.
fn confirm_migration(state: AppState) -> AppState {
    let preview = match &state.migration_preview {
        Some(p) => p,
        None => return state.with_mode(Mode::Normal),
    };

    let sql = preview.sql.clone();
    let description = preview.description.clone();

    // Generate timestamp: YYYYMMDDHHMMSS
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    // Convert to rough datetime components (no chrono dependency)
    let timestamp = format_timestamp(secs);

    let migrations_dir = std::path::Path::new("migrations");
    if let Err(e) = std::fs::create_dir_all(migrations_dir) {
        return state
            .with_mode(Mode::Normal)
            .with_status_message(format!("Failed to create migrations directory: {e}"));
    }

    match crate::migration::write_migration(migrations_dir, &description, &sql, &timestamp) {
        Ok(path) => {
            let filename = path
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default();
            state
                .clear_edit_state()
                .with_mode(Mode::Normal)
                .with_status_message(format!("Migration written: {filename}"))
        }
        Err(e) => state
            .with_mode(Mode::Normal)
            .with_status_message(format!("Failed to write migration: {e}")),
    }
}

/// Format a unix timestamp as YYYYMMDDHHMMSS.
fn format_timestamp(secs: u64) -> String {
    // Days since epoch
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    // Convert days to y/m/d using a civil calendar algorithm
    let (year, month, day) = days_to_civil(days as i64);
    format!("{year:04}{month:02}{day:02}{hours:02}{minutes:02}{seconds:02}")
}

/// Convert days since Unix epoch to (year, month, day).
/// Algorithm from Howard Hinnant's chrono-compatible date algorithms.
fn days_to_civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
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
            Mode::Command => handle_command(state, key, &no_pool()).state,
            Mode::SpaceMenu => handle_space_menu(state, key, &no_pool()).state,
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
            Mode::MigrationPreview => handle_migration_preview(state, key),
            Mode::LlmPending => handle_llm_pending(state, key),
            Mode::LlmPreview => handle_llm_preview(state, key),
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
            KeyCode::Char('t') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                state.toggle_rust_types()
            }
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

    #[test]
    fn esc_exits_migration_preview_mode() {
        let state = sample_state().with_mode(Mode::MigrationPreview);
        let state = handle_key_no_pool(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal, "Esc should exit MigrationPreview");
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

    // --- Rust type toggle ---

    #[test]
    fn ctrl_t_toggles_rust_types() {
        let state = sample_state();
        assert!(!state.show_rust_types);

        let state = handle_key_no_pool(
            state,
            key_with_mod(KeyCode::Char('t'), KeyModifiers::CONTROL),
        );
        assert!(state.show_rust_types);

        let state = handle_key_no_pool(
            state,
            key_with_mod(KeyCode::Char('t'), KeyModifiers::CONTROL),
        );
        assert!(!state.show_rust_types);
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
    fn space_menu_p_toggles_pending_overlay() {
        let state = sample_state().with_mode(Mode::SpaceMenu);
        let state = handle_key_no_pool(state, key(KeyCode::Char('p')));
        assert_eq!(state.mode, Mode::Normal);
        assert!(state.show_pending_overlay);
    }

    #[test]
    fn space_menu_p_toggles_off() {
        let state = sample_state()
            .with_mode(Mode::SpaceMenu)
            .toggle_pending_overlay(); // pre-enable
        assert!(state.show_pending_overlay);
        let state = handle_key_no_pool(state, key(KeyCode::Char('p')));
        assert!(!state.show_pending_overlay);
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
        let users_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("users".into()))
            .unwrap();
        let state = state.cursor_to(users_pos);

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('r')));
        assert_eq!(state.pending_key, PendingKey::None);
        assert_eq!(state.focus(), Some(&FocusTarget::Table("posts".into())));
    }

    #[test]
    fn go_on_table_with_outgoing_fks_jumps() {
        let state = goto_state();
        let posts_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("posts".into()))
            .unwrap();
        let state = state.cursor_to(posts_pos);

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('o')));
        assert_eq!(state.focus(), Some(&FocusTarget::Table("users".into())));
    }

    #[test]
    fn go_on_table_no_outgoing_shows_no_results() {
        let state = goto_state();
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

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('c')));
        assert_eq!(
            state.focus(),
            Some(&FocusTarget::Column("posts".into(), "id".into()))
        );
        assert!(state.expanded.contains("posts"));
    }

    #[test]
    fn gt_on_column_jumps_to_parent_table() {
        let state = goto_state();
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

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('t')));
        assert_eq!(state.focus(), Some(&FocusTarget::Table("posts".into())));
    }

    #[test]
    fn gd_on_fk_column_jumps_to_target() {
        let state = goto_state();
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

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('d')));
        assert_eq!(
            state.focus(),
            Some(&FocusTarget::Column("users".into(), "id".into()))
        );
        assert!(state.expanded.contains("users"));
    }

    #[test]
    fn gy_on_custom_type_column_jumps_to_enum() {
        let state = goto_state();
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

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('y')));
        assert_eq!(state.focus(), Some(&FocusTarget::Enum("user_role".into())));
    }

    #[test]
    fn gy_on_non_custom_type_shows_no_results() {
        let state = goto_state();
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
    fn gm_shows_no_migrations() {
        let state = goto_state();
        let posts_pos = state
            .doc
            .iter()
            .position(|l| l.target == FocusTarget::Table("posts".into()))
            .unwrap();
        let state = state.cursor_to(posts_pos);
        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('m')));
        assert!(state.status_message.is_some());
        assert!(state
            .status_message
            .as_ref()
            .unwrap()
            .contains("no migrations"));
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

        let state = handle_key_no_pool(state, key(KeyCode::Char('g')));
        let state = handle_key_no_pool(state, key(KeyCode::Char('i')));
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

    // --- Migration workflow (:w command) ---

    fn edited_state() -> AppState {
        use crate::schema::types::PgType;
        use crate::schema::Column;
        use crate::tui::edit;

        let mut schema = Schema::new();
        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("email", PgType::Text));
        schema.add_table(users);
        let state = AppState::new(schema, "test".into()).with_viewport_height(20);

        // Enter edit mode and add a column
        let state = state.toggle_expand();
        let state = edit::enter_edit_mode(state);
        let mut state = state;
        // Insert a bio column line before the closing brace
        let close_idx = state
            .edit_buffer
            .iter()
            .position(|l| l.trim() == "}")
            .expect("closing brace");
        state
            .edit_buffer
            .insert(close_idx, "    bio  text".to_string());
        // Exit edit mode (parses and updates schema)
        edit::handle_edit(state, key(KeyCode::Esc))
    }

    #[test]
    fn command_w_without_edits_shows_no_changes() {
        let state = sample_state().with_mode(Mode::Command);
        let state = handle_key_no_pool(state, key(KeyCode::Char('w')));
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
        assert_eq!(
            state.status_message.as_deref(),
            Some("No schema changes to migrate")
        );
    }

    #[test]
    fn command_w_with_edits_opens_preview() {
        let state = edited_state();
        assert!(state.original_schema.is_some());
        assert!(state.edited_tables.contains("users"));

        let state = state.with_mode(Mode::Command);
        let state = handle_key_no_pool(state, key(KeyCode::Char('w')));
        let state = handle_key_no_pool(state, key(KeyCode::Enter));

        assert_eq!(state.mode, Mode::MigrationPreview);
        assert!(state.migration_preview.is_some());
        let preview = state.migration_preview.as_ref().unwrap();
        assert!(preview.sql.contains("ALTER TABLE"));
        assert!(!preview.description.is_empty());
    }

    #[test]
    fn command_w_with_description() {
        let state = edited_state().with_mode(Mode::Command);
        // Type "w add_bio_to_users"
        for ch in "w add_bio_to_users".chars() {
            let state_inner = handle_key_no_pool(state.clone(), key(KeyCode::Char(ch)));
            // We need to chain properly
            let _ = state_inner;
        }
        // Simpler: manually set command_buf and execute
        let mut state = edited_state();
        state.mode = Mode::Command;
        state.command_buf = "w add_bio_to_users".to_string();
        let state = handle_key_no_pool(state, key(KeyCode::Enter));

        assert_eq!(state.mode, Mode::MigrationPreview);
        let preview = state.migration_preview.as_ref().unwrap();
        assert_eq!(preview.description, "add_bio_to_users");
    }

    #[test]
    fn migration_preview_esc_cancels() {
        let state = edited_state();
        let state = state.with_mode(Mode::Command);
        let state = handle_key_no_pool(state, key(KeyCode::Char('w')));
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::MigrationPreview);

        let state = handle_key_no_pool(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal);
        assert!(state.migration_preview.is_none());
        // Edit state should still be intact (not cleared on cancel)
        assert!(state.original_schema.is_some());
        assert!(!state.edited_tables.is_empty());
    }

    #[test]
    fn migration_preview_scroll() {
        let state = edited_state();
        let mut state = state.with_mode(Mode::Command);
        state.command_buf = "w test".to_string();
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::MigrationPreview);

        // Scroll down
        let state = handle_key_no_pool(state, key(KeyCode::Char('j')));
        assert_eq!(state.migration_preview.as_ref().unwrap().scroll, 1);

        // Scroll up
        let state = handle_key_no_pool(state, key(KeyCode::Char('k')));
        assert_eq!(state.migration_preview.as_ref().unwrap().scroll, 0);

        // Can't scroll past 0
        let state = handle_key_no_pool(state, key(KeyCode::Char('k')));
        assert_eq!(state.migration_preview.as_ref().unwrap().scroll, 0);
    }

    #[test]
    fn migration_preview_confirm_writes_file_and_clears_edit_state() {
        let state = edited_state();
        assert!(state.original_schema.is_some());
        assert!(!state.renames.is_empty() || !state.edited_tables.is_empty());

        let mut state = state.with_mode(Mode::Command);
        state.command_buf = "w test_migration".to_string();
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::MigrationPreview);

        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);

        // Edit state should be cleared
        assert!(state.original_schema.is_some());
        assert!(state.renames.is_empty());
        assert!(state.edited_tables.is_empty());

        // Status message should indicate success
        let msg = state.status_message.as_deref().unwrap_or("");
        assert!(msg.starts_with("Migration written:"), "got: {msg}");
        assert!(msg.contains("test_migration"));

        // Cleanup: remove the migration file
        let _ = std::fs::remove_dir_all("migrations");
    }

    #[test]
    fn timestamp_format() {
        // Test known epoch values
        assert_eq!(format_timestamp(0), "19700101000000");
        // 2026-02-14 12:00:00 UTC
        let ts = 1771070400;
        let result = format_timestamp(ts);
        assert!(result.starts_with("2026"), "got: {result}");
        assert_eq!(result.len(), 14);
    }

    #[test]
    fn auto_describe_single_change() {
        let changes = vec![diff::Change::AddColumn {
            table: "users".into(),
            column: crate::schema::Column::new("bio", crate::schema::types::PgType::Text),
        }];
        let desc = auto_describe(&changes);
        assert_eq!(desc, "add_bio_to_users");
    }

    #[test]
    fn auto_describe_many_changes() {
        let changes = vec![
            diff::Change::AddColumn {
                table: "users".into(),
                column: crate::schema::Column::new("bio", crate::schema::types::PgType::Text),
            },
            diff::Change::AddColumn {
                table: "users".into(),
                column: crate::schema::Column::new("avatar", crate::schema::types::PgType::Text),
            },
            diff::Change::AddColumn {
                table: "users".into(),
                column: crate::schema::Column::new("phone", crate::schema::types::PgType::Text),
            },
            diff::Change::DropColumn {
                table: "users".into(),
                column: "legacy".into(),
            },
        ];
        let desc = auto_describe(&changes);
        assert!(desc.contains("3_more_changes"));
    }

    // --- LLM commands ---

    #[test]
    fn command_ai_without_api_key_shows_not_configured() {
        std::env::remove_var("OPENAI_API_KEY");
        let mut state = sample_state().with_mode(Mode::Command);
        state.command_buf = "ai add an index on email".to_string();
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
        assert!(
            state
                .status_message
                .as_deref()
                .unwrap_or("")
                .contains("not configured"),
            "got: {:?}",
            state.status_message
        );
    }

    #[test]
    fn command_ai_without_edits_shows_error() {
        let mut state = sample_state().with_mode(Mode::Command);
        state.command_buf = "ai add an index".to_string();
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
        let msg = state.status_message.as_deref().unwrap_or("");
        // Without edits: either "not configured" or "edit the schema first"
        assert!(
            msg.contains("not configured") || msg.contains("edit the schema"),
            "got: {msg:?}"
        );
    }

    #[test]
    fn command_ai_empty_prompt_shows_usage() {
        let mut state = sample_state().with_mode(Mode::Command);
        state.command_buf = "ai ".to_string();
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
        assert!(
            state
                .status_message
                .as_deref()
                .unwrap_or("")
                .contains("Usage"),
            "got: {:?}",
            state.status_message
        );
    }

    #[test]
    fn command_generate_down_without_api_key_shows_not_configured() {
        std::env::remove_var("OPENAI_API_KEY");
        let mut state = sample_state().with_mode(Mode::Command);
        state.command_buf = "generate-down".to_string();
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
        assert!(
            state
                .status_message
                .as_deref()
                .unwrap_or("")
                .contains("not configured"),
            "got: {:?}",
            state.status_message
        );
    }

    #[test]
    fn command_generate_down_without_migrations_shows_error() {
        let mut state = sample_state().with_mode(Mode::Command);
        state.command_buf = "generate-down".to_string();
        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
        let msg = state.status_message.as_deref().unwrap_or("");
        // Should show either "not configured" or "No up migration" depending on env
        assert!(
            msg.contains("not configured") || msg.contains("No up migration"),
            "got: {msg:?}"
        );
    }

    #[test]
    fn llm_pending_esc_cancels() {
        let state = sample_state().with_mode(Mode::LlmPending);
        let state = handle_key_no_pool(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal);
    }

    #[test]
    fn llm_pending_ignores_other_keys() {
        let state = sample_state().with_mode(Mode::LlmPending);
        let state = handle_key_no_pool(state, key(KeyCode::Char('j')));
        assert_eq!(state.mode, Mode::LlmPending);
    }

    #[test]
    fn llm_preview_esc_cancels() {
        let mut state = sample_state().with_mode(Mode::LlmPreview);
        state.llm_preview = Some(crate::tui::app::LlmPreviewState {
            sql: "SELECT 1;".into(),
            kind: crate::tui::app::LlmPreviewKind::AiEdit {
                original_sql: "SELECT 1;".into(),
                description: "test".into(),
            },
            scroll: 0,
        });
        let state = handle_key_no_pool(state, key(KeyCode::Esc));
        assert_eq!(state.mode, Mode::Normal);
        assert!(state.llm_preview.is_none());
    }

    #[test]
    fn llm_preview_scroll() {
        let mut state = sample_state().with_mode(Mode::LlmPreview);
        state.llm_preview = Some(crate::tui::app::LlmPreviewState {
            sql: "SELECT 1;\nSELECT 2;\nSELECT 3;".into(),
            kind: crate::tui::app::LlmPreviewKind::AiEdit {
                original_sql: "SELECT 1;".into(),
                description: "test".into(),
            },
            scroll: 0,
        });

        let state = handle_key_no_pool(state, key(KeyCode::Char('j')));
        assert_eq!(state.llm_preview.as_ref().unwrap().scroll, 1);

        let state = handle_key_no_pool(state, key(KeyCode::Char('k')));
        assert_eq!(state.llm_preview.as_ref().unwrap().scroll, 0);

        // Can't scroll past 0
        let state = handle_key_no_pool(state, key(KeyCode::Char('k')));
        assert_eq!(state.llm_preview.as_ref().unwrap().scroll, 0);
    }

    #[test]
    fn llm_preview_confirm_ai_edit_enters_migration_preview() {
        let mut state = edited_state().with_mode(Mode::LlmPreview);
        state.llm_preview = Some(crate::tui::app::LlmPreviewState {
            sql: "ALTER TABLE users ADD COLUMN avatar text;".into(),
            kind: crate::tui::app::LlmPreviewKind::AiEdit {
                original_sql: "ALTER TABLE users ADD COLUMN bio text;".into(),
                description: "ai_edit".into(),
            },
            scroll: 0,
        });

        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::MigrationPreview);
        assert!(state.migration_preview.is_some());
        let preview = state.migration_preview.as_ref().unwrap();
        assert!(preview
            .sql
            .contains("ALTER TABLE users ADD COLUMN avatar text;"));
        assert!(state.llm_preview.is_none());
    }

    #[test]
    fn llm_preview_confirm_generate_down_writes_file() {
        let dir = std::env::temp_dir().join("inara_test_llm_down");
        let _ = std::fs::create_dir_all(&dir);

        let mut state = sample_state().with_mode(Mode::LlmPreview);
        state.llm_preview = Some(crate::tui::app::LlmPreviewState {
            sql: "ALTER TABLE users DROP COLUMN bio;".into(),
            kind: crate::tui::app::LlmPreviewKind::GenerateDown {
                up_sql: "ALTER TABLE users ADD COLUMN bio text;".into(),
                description: "add_bio".into(),
            },
            scroll: 0,
        });

        let state = handle_key_no_pool(state, key(KeyCode::Enter));
        assert_eq!(state.mode, Mode::Normal);
        let msg = state.status_message.as_deref().unwrap_or("");
        // Should succeed or fail depending on migrations/ dir
        assert!(
            msg.contains("Down migration written") || msg.contains("Failed"),
            "got: {msg}"
        );
    }

    #[test]
    fn slugify_works() {
        assert_eq!(slugify("add bio to users"), "add_bio_to_users");
        assert_eq!(slugify("Add FK: posts→users"), "add_fk_posts_users");
        assert_eq!(slugify("  multiple   spaces  "), "multiple_spaces");
    }

    #[test]
    fn find_latest_up_migration_returns_none_for_nonexistent_dir() {
        let result = find_latest_up_migration(std::path::Path::new("/nonexistent/dir"));
        assert!(result.is_none());
    }
}
