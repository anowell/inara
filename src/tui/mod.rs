pub mod app;
pub mod edit;
pub mod fuzzy;
pub mod goto;
pub mod help;
pub mod hud;
pub mod input;
pub mod view;

use std::io;
use std::time::Duration;

use color_eyre::eyre::Result;
use crossterm::event::{self, Event, KeyEventKind};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{cursor, execute};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use self::app::{AppState, Mode};
use self::hud::HudResultHandle;
use self::input::{handle_key, OverlayResultHandle, WarningResultHandle};

/// Tick rate for the event loop poll interval.
const TICK_RATE: Duration = Duration::from_millis(50);

/// Initialize the terminal for TUI rendering.
///
/// Enters raw mode and the alternate screen. Returns a Terminal instance.
fn init_terminal() -> Result<ratatui::Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>> {
    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, cursor::Hide)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let terminal = ratatui::Terminal::new(backend)?;
    Ok(terminal)
}

/// Restore the terminal to its original state.
///
/// This must be called on exit, including panics. Leaves raw mode and the
/// alternate screen, and shows the cursor.
fn restore_terminal() {
    // Best-effort: ignore errors during cleanup
    let _ = terminal::disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen, cursor::Show);
}

/// Spawn an external editor for the given table, then apply changes.
///
/// 1. Write rendered text to a temp file.
/// 2. Suspend the TUI and launch `$EDITOR` (falling back to `$VISUAL`, then `hx`).
/// 3. Resume the TUI.
/// 4. Parse the edited file and update the schema.
fn spawn_editor(
    terminal: &mut ratatui::Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>,
    state: AppState,
    req: edit::EditorRequest,
) -> Result<AppState> {
    use std::io::Write;

    // Write to temp file (include PID to avoid collisions between concurrent instances)
    let tmp_dir = std::env::temp_dir();
    let tmp_path = tmp_dir.join(format!(
        "inara-{}-{}.inara",
        req.table_name,
        std::process::id()
    ));
    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(req.rendered_text.as_bytes())?;
    }

    // Determine editor
    let editor = std::env::var("EDITOR")
        .or_else(|_| std::env::var("VISUAL"))
        .unwrap_or_else(|_| "hx".to_string());

    // Suspend TUI
    restore_terminal();

    // Launch editor
    let status = std::process::Command::new(&editor).arg(&tmp_path).status();

    // Resume TUI
    terminal::enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen, cursor::Hide)?;
    terminal.clear()?;

    // Check editor exit status
    match status {
        Ok(s) if !s.success() => {
            let _ = std::fs::remove_file(&tmp_path);
            return Ok(state.with_status(format!("{editor} exited with {s}")));
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Ok(state.with_status(format!("Failed to launch {editor}: {e}")));
        }
        _ => {}
    }

    // Read back and clean up
    let edited_text = match std::fs::read_to_string(&tmp_path) {
        Ok(text) => {
            let _ = std::fs::remove_file(&tmp_path);
            text
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Ok(state.with_status(format!("Failed to read temp file: {e}")));
        }
    };

    // No changes?
    if edited_text.trim() == req.rendered_text.trim() {
        return Ok(state.with_status("No changes"));
    }

    // Parse the edited text
    let text = if edited_text.ends_with('\n') {
        edited_text
    } else {
        format!("{edited_text}\n")
    };

    match crate::schema::parse::parse_single_table(&text) {
        Ok(new_table) => {
            let mut state = state.ensure_original_schema();
            state.push_undo_snapshot();
            let table_name = req.table_name;

            // Handle rename
            if new_table.name != table_name {
                state.renames.push(app::RenameMetadata {
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
            state.rebuild_doc();
            state.recompute_edit_overlay();
            Ok(state)
        }
        Err(e) => Ok(state.with_status(format!(
            "Parse error: line {}, col {}: {}",
            e.line, e.col, e.message
        ))),
    }
}

/// Install a color-eyre panic hook that restores the terminal before printing.
///
/// Without this, a panic would leave the terminal in raw mode with the
/// alternate screen active, making the error message invisible.
fn install_panic_hook() -> Result<()> {
    let (panic_hook, eyre_hook) = color_eyre::config::HookBuilder::default().into_hooks();
    let panic_hook = panic_hook.into_panic_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        restore_terminal();
        panic_hook(panic_info);
    }));
    eyre_hook.install()?;
    Ok(())
}

/// Initialize tracing to log to a file instead of stdout/stderr.
///
/// TUI applications cannot log to stdout because it would corrupt the
/// terminal display. Logs go to `inara.log` in the current directory.
fn init_file_tracing() {
    let file_appender = tracing_appender::rolling::never(".", "inara.log");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(file_appender)
        .with_ansi(false)
        .init();
}

/// Run the TUI application.
///
/// This is the main entry point called from `main()`. It connects to the
/// database, loads the schema, and runs the interactive event loop.
pub async fn run(
    database_url: &str,
    connection_info: String,
    migrations_dir: Option<std::path::PathBuf>,
    config_overrides: std::collections::BTreeMap<String, String>,
    config_language: Option<String>,
) -> Result<()> {
    // Initialize file-based tracing before entering TUI mode
    init_file_tracing();

    eprintln!("Connecting to {connection_info}...");
    tracing::info!("Connecting to database...");
    let connect_timeout = Duration::from_secs(5);
    let connect = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(connect_timeout)
        .connect(database_url);
    let pool = match tokio::time::timeout(connect_timeout + Duration::from_secs(1), connect).await {
        Ok(Ok(pool)) => pool,
        Ok(Err(e)) => {
            return Err(color_eyre::eyre::eyre!(
                "could not connect to {connection_info}: {e}"
            ));
        }
        Err(_) => {
            return Err(color_eyre::eyre::eyre!(
                "connection timed out after 5s — could not connect to {connection_info}"
            ));
        }
    };
    tracing::info!("Connected. Loading schema...");

    let schema = crate::schema::introspect::introspect(&pool, "public").await?;
    let table_count = schema.tables.len();
    let enum_count = schema.enums.len();
    let type_count = schema.types.len();
    tracing::info!(
        "Schema loaded: {table_count} tables, {enum_count} enums, {type_count} custom types"
    );

    // Install panic hook before entering raw mode
    install_panic_hook()?;

    let mut terminal = init_terminal()?;

    // Determine target language from config (default: Rust)
    let language = match config_language.as_deref() {
        Some(s) => match crate::schema::type_map::Language::from_config(s) {
            Some(lang) => lang,
            None => {
                tracing::warn!("Unrecognized language '{s}' in config, falling back to rust");
                crate::schema::type_map::Language::Rust
            }
        },
        None => crate::schema::type_map::Language::Rust,
    };
    tracing::info!("Type mapper language: {language}");

    // Build type mapper based on language
    let cargo_toml_path = std::path::Path::new("Cargo.toml");
    let type_mapper = {
        let mut mapper = match language {
            crate::schema::type_map::Language::Rust => {
                // Rust: detect features from Cargo.toml
                crate::schema::type_map::TypeMapper::from_cargo_toml(cargo_toml_path)
            }
            _ => crate::schema::type_map::TypeMapper::for_language(language),
        };

        // Merge overrides: Cargo.toml metadata / .inara.toml (legacy) for Rust only
        let mut overrides = if language == crate::schema::type_map::Language::Rust {
            crate::schema::type_map::load_overrides(cargo_toml_path)
        } else {
            std::collections::BTreeMap::new()
        };
        // Config file overrides ([types.overrides]) apply for all languages
        overrides.extend(config_overrides);
        if !overrides.is_empty() {
            tracing::info!("Loaded {} type override(s)", overrides.len());
            mapper = mapper.with_overrides(overrides);
        }
        if language == crate::schema::type_map::Language::Rust {
            let features = mapper.features();
            tracing::info!(
                "Rust features: chrono={}, time={}, jiff={}",
                features.chrono,
                features.time,
                features.jiff
            );
        }
        mapper
    };

    let state =
        AppState::new(schema, connection_info, migrations_dir).with_type_mapper(type_mapper);

    let result = run_event_loop(&mut terminal, state, pool);

    restore_terminal();

    result
}

/// The main event loop. Polls for crossterm events and redraws on changes.
fn run_event_loop(
    terminal: &mut ratatui::Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>,
    mut state: AppState,
    pool: sqlx::PgPool,
) -> Result<()> {
    let mut hud_handle: Option<HudResultHandle> = None;
    let mut warning_handle: Option<WarningResultHandle> = None;
    let mut overlay_handle: Option<OverlayResultHandle> = None;
    let mut llm_handle: Option<crate::llm::LlmResultHandle> = None;

    loop {
        // Update viewport height from terminal size
        let area = terminal.size()?;
        // Content area height = total height - header(1) - status bar(1) - content borders(2)
        let content_height = area.height.saturating_sub(4) as usize;
        state = state.with_viewport_height(content_height);

        // Poll for HUD query results
        if let Some(ref handle) = hud_handle {
            if let Ok(mut guard) = handle.lock() {
                if let Some(status) = guard.take() {
                    state = state.with_hud_status(status);
                }
            }
        }

        // Poll for warning check results
        if let Some(ref handle) = warning_handle {
            if let Ok(mut guard) = handle.lock() {
                if let Some(warnings) = guard.take() {
                    if let Some(ref mut preview) = state.migration_preview {
                        preview.warnings = Some(warnings);
                    }
                }
            }
        }

        // Poll for overlay computation results
        let overlay_result = overlay_handle
            .as_ref()
            .and_then(|h| h.lock().ok().and_then(|mut guard| guard.take()));
        if let Some(result) = overlay_result {
            match result {
                Ok(overlay) => {
                    if overlay.is_empty() {
                        state = state
                            .with_pending_overlay(None)
                            .with_status("No pending migrations");
                        state.show_pending_overlay = false;
                    } else {
                        let count = overlay.pending_count;
                        let change_count = overlay.changes.len();
                        let unparseable_count = overlay.unparseable.len();
                        let mut msg =
                            format!("{count} pending migration(s), {change_count} change(s)");
                        if unparseable_count > 0 {
                            msg.push_str(&format!(", {unparseable_count} unparseable"));
                        }
                        state = state.with_pending_overlay(Some(overlay)).with_status(msg);
                    }
                }
                Err(err) => {
                    state = state
                        .with_pending_overlay(None)
                        .with_status(format!("Overlay error: {err}"));
                    state.show_pending_overlay = false;
                }
            }
            overlay_handle = None;
        }

        // Poll for LLM results
        if let Some(ref handle) = llm_handle {
            if let Ok(mut guard) = handle.lock() {
                if let Some(result) = guard.take() {
                    match result {
                        crate::llm::LlmResult::Success(sql) => {
                            if let Some(ref mut preview) = state.llm_preview {
                                preview.sql = sql;
                            }
                            state.mode = Mode::LlmPreview;
                        }
                        crate::llm::LlmResult::Error(err) => {
                            state = state
                                .with_mode(Mode::Normal)
                                .with_status(format!("LLM error: {err}"));
                        }
                    }
                }
            }
        }

        // Check pending key timeout (1 second)
        if state.is_pending_key_expired(Duration::from_secs(1)) {
            state = state
                .with_pending_key(app::PendingKey::None)
                .with_status("key sequence cancelled (timeout)");
        }

        terminal.draw(|frame| draw(frame, &state))?;

        if state.should_quit {
            break;
        }

        // Poll for events with timeout (non-blocking)
        if event::poll(TICK_RATE)? {
            match event::read()? {
                Event::Key(key) => {
                    // Only handle key press events (ignore release/repeat)
                    if key.kind == KeyEventKind::Press {
                        let result = handle_key(state, key, &pool);
                        state = result.state;
                        if let Some(h) = result.hud_handle {
                            hud_handle = Some(h);
                        }
                        if let Some(h) = result.warning_handle {
                            warning_handle = Some(h);
                        }
                        if let Some(h) = result.overlay_handle {
                            overlay_handle = Some(h);
                        }
                        if let Some(h) = result.llm_handle {
                            llm_handle = Some(h);
                        }
                        // Spawn external editor if requested
                        if let Some(req) = result.editor_request {
                            state = spawn_editor(terminal, state, req)?;
                        }
                        // Clear handles when leaving respective modes
                        if state.mode != Mode::HUD {
                            hud_handle = None;
                        }
                        if state.mode != Mode::MigrationPreview {
                            warning_handle = None;
                        }
                        if state.mode != Mode::LlmPending && state.mode != Mode::LlmPreview {
                            llm_handle = None;
                        }
                    }
                }
                Event::Resize(_, _) => {
                    // Terminal will redraw on next iteration with updated size
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Render the TUI layout to a frame.
fn draw(frame: &mut Frame, state: &AppState) {
    let area = frame.area();

    // Three-row layout: header (1 line), content (fill), status bar (1 line)
    let layout = Layout::vertical([
        Constraint::Length(1), // header
        Constraint::Min(1),    // content
        Constraint::Length(1), // status bar
    ])
    .split(area);

    draw_header(frame, layout[0], state);
    draw_content(frame, layout[1], state);
    draw_status_bar(frame, layout[2], state);

    // Render overlays on top of the content area
    match state.mode {
        Mode::SpaceMenu => fuzzy::render_space_menu(frame, layout[1]),
        Mode::GotoMenu => fuzzy::render_goto_menu(frame, layout[1], state),
        Mode::ChangeMenu => fuzzy::render_change_menu(frame, layout[1], state),
        Mode::Search => {
            if let Some(ref search) = state.search {
                fuzzy::render_search_overlay(frame, layout[1], search);
            }
        }
        Mode::MigrationPreview => {
            if let Some(ref preview) = state.migration_preview {
                render_migration_preview(frame, layout[1], preview);
            }
        }
        Mode::LlmPending => {
            render_llm_pending(frame, layout[1], state);
        }
        Mode::LlmPreview => {
            if let Some(ref preview) = state.llm_preview {
                render_llm_preview(frame, layout[1], preview);
            }
        }
        Mode::Help => {
            help::render_help(frame, layout[1], state.help_source_mode);
        }
        Mode::ChangePreview => {
            if let Some(ref preview) = state.change_preview {
                render_change_preview(frame, layout[1], preview);
            }
        }
        _ => {}
    }

    // HUD overlay renders on top of everything
    if let Some(ref hud_state) = state.hud {
        hud::render_hud(frame, area, hud_state);
    }
}

/// Render the header bar with app name and connection info.
fn draw_header(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    let header = Line::from(vec![
        Span::styled(
            " inara ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled(&state.connection_info, Style::default().fg(Color::DarkGray)),
    ]);
    frame.render_widget(Paragraph::new(header), area);
}

/// Render the main content area with the schema document.
fn draw_content(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Schema ");

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let visible_lines = view::render_document(state);
    let content = Paragraph::new(visible_lines);
    frame.render_widget(content, inner);
}

/// Render the migration preview overlay.
fn render_migration_preview(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    preview: &app::MigrationPreviewState,
) {
    use crate::migration::warnings::Severity;
    use ratatui::widgets::Clear;

    // Build the full content: warnings section + SQL
    let mut content_lines: Vec<Line> = Vec::new();

    // Warnings section (above SQL)
    match &preview.warnings {
        None => {
            // Checks still running
            content_lines.push(Line::from(Span::styled(
                " Checking for potential issues...",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::ITALIC),
            )));
            content_lines.push(Line::from(""));
        }
        Some(warnings) if !warnings.is_empty() => {
            for w in warnings {
                let (icon, color) = match w.severity {
                    Severity::Error => ("!!", Color::Red),
                    Severity::Warning => ("!!", Color::Yellow),
                };
                content_lines.push(Line::from(vec![
                    Span::styled(
                        format!(" {icon} "),
                        Style::default().fg(color).add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(format!("[{}] ", w.severity), Style::default().fg(color)),
                    Span::styled(&w.description, Style::default().fg(Color::White)),
                ]));
                // Show remediation as indented hint
                content_lines.push(Line::from(Span::styled(
                    format!("      Fix: {}", w.remediation),
                    Style::default().fg(Color::DarkGray),
                )));
            }
            content_lines.push(Line::from(""));
        }
        Some(_) => {
            // No warnings — skip section
        }
    }

    // SQL lines
    for sql_line in preview.sql.lines() {
        content_lines.push(Line::from(Span::styled(
            sql_line.to_string(),
            Style::default().fg(Color::White),
        )));
    }

    let total_lines = content_lines.len();

    // Use the full content area for the overlay
    frame.render_widget(Clear, area);

    let title = format!(" Migration: {} ", preview.description);
    let border_color = match &preview.warnings {
        Some(ws) if ws.iter().any(|w| w.severity == Severity::Error) => Color::Red,
        Some(ws) if !ws.is_empty() => Color::Yellow,
        _ => Color::Green,
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title(title)
        .title_style(
            Style::default()
                .fg(border_color)
                .add_modifier(Modifier::BOLD),
        );

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let visible_height = inner.height as usize;
    let max_scroll = total_lines.saturating_sub(visible_height);
    let scroll = preview.scroll.min(max_scroll);

    let mut lines: Vec<Line> = content_lines
        .into_iter()
        .skip(scroll)
        .take(visible_height.saturating_sub(2)) // leave room for footer
        .collect();

    // Add separator and footer instructions
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled(
            " Enter",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" confirm  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" cancel", Style::default().fg(Color::DarkGray)),
    ]));

    let content = Paragraph::new(lines);
    frame.render_widget(content, inner);
}

/// Render the change preview overlay (Space d).
fn render_change_preview(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    preview: &app::ChangePreviewState,
) {
    use ratatui::widgets::Clear;

    let mut content_lines: Vec<Line> = Vec::new();

    if preview.show_sql {
        // SQL view
        if let Some(ref sql) = preview.sql {
            for sql_line in sql.lines() {
                content_lines.push(Line::from(Span::styled(
                    sql_line.to_string(),
                    Style::default().fg(Color::White),
                )));
            }
        } else {
            content_lines.push(Line::from(Span::styled(
                " No SQL generated",
                Style::default().fg(Color::DarkGray),
            )));
        }
    } else {
        // Summary view
        for line in &preview.summary {
            let (prefix, color) = if line.starts_with('+') {
                ("+", Color::Green)
            } else if line.starts_with('-') {
                ("-", Color::Red)
            } else if line.starts_with('~') {
                ("~", Color::Yellow)
            } else {
                (" ", Color::White)
            };
            let text = line.strip_prefix(prefix).unwrap_or(line);
            content_lines.push(Line::from(vec![
                Span::styled(
                    format!(" {prefix} "),
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::styled(text.to_string(), Style::default().fg(Color::White)),
            ]));
        }
    }

    let total_lines = content_lines.len();

    frame.render_widget(Clear, area);

    let title = if preview.show_sql {
        " Change Preview — SQL "
    } else {
        " Change Preview — Summary "
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
        .title(title)
        .title_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let visible_height = inner.height as usize;
    let max_scroll = total_lines.saturating_sub(visible_height);
    let scroll = preview.scroll.min(max_scroll);

    let mut lines: Vec<Line> = content_lines
        .into_iter()
        .skip(scroll)
        .take(visible_height.saturating_sub(2))
        .collect();

    // Footer
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled(
            " s",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            if preview.show_sql {
                " summary  "
            } else {
                " sql  "
            },
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" close", Style::default().fg(Color::DarkGray)),
    ]));

    let content = Paragraph::new(lines);
    frame.render_widget(content, inner);
}

/// Render the LLM pending overlay (loading indicator).
fn render_llm_pending(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    use ratatui::widgets::Clear;

    frame.render_widget(Clear, area);

    let msg = state
        .llm_pending_message
        .as_deref()
        .unwrap_or("Waiting for LLM...");

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Magenta))
        .title(" AI ")
        .title_style(
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        );

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            format!("  {msg}"),
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::ITALIC),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  Esc to cancel",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let content = Paragraph::new(lines);
    frame.render_widget(content, inner);
}

/// Render the LLM preview overlay (reviewing response).
fn render_llm_preview(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    preview: &app::LlmPreviewState,
) {
    use ratatui::widgets::Clear;

    frame.render_widget(Clear, area);

    let title = match &preview.kind {
        app::LlmPreviewKind::AiEdit { .. } => " AI Suggestion ",
        app::LlmPreviewKind::GenerateDown { .. } => " AI Down Migration ",
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Magenta))
        .title(title)
        .title_style(
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        );

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let mut content_lines: Vec<Line> = Vec::new();

    // AI-generated notice
    content_lines.push(Line::from(Span::styled(
        " AI-generated — review carefully",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::ITALIC),
    )));
    content_lines.push(Line::from(""));

    // SQL content
    for sql_line in preview.sql.lines() {
        content_lines.push(Line::from(Span::styled(
            sql_line.to_string(),
            Style::default().fg(Color::White),
        )));
    }

    let total_lines = content_lines.len();
    let visible_height = inner.height as usize;
    let max_scroll = total_lines.saturating_sub(visible_height);
    let scroll = preview.scroll.min(max_scroll);

    let mut lines: Vec<Line> = content_lines
        .into_iter()
        .skip(scroll)
        .take(visible_height.saturating_sub(2)) // leave room for footer
        .collect();

    // Footer instructions
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled(
            " Enter",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" confirm  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" cancel", Style::default().fg(Color::DarkGray)),
    ]));

    let content = Paragraph::new(lines);
    frame.render_widget(content, inner);
}

/// Render the status bar with current mode and context info.
fn draw_status_bar(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    let mode_style = match state.mode {
        Mode::Normal => Style::default().fg(Color::Black).bg(Color::Blue),
        Mode::Rename => Style::default().fg(Color::Black).bg(Color::Yellow),
        Mode::DefaultPrompt => Style::default().fg(Color::Black).bg(Color::Yellow),
        Mode::Search => Style::default().fg(Color::Black).bg(Color::Green),
        Mode::HUD => Style::default().fg(Color::Black).bg(Color::Magenta),
        Mode::Command => Style::default().fg(Color::Black).bg(Color::Red),
        Mode::SpaceMenu => Style::default().fg(Color::Black).bg(Color::Cyan),
        Mode::GotoMenu => Style::default().fg(Color::Black).bg(Color::Cyan),
        Mode::ChangeMenu => Style::default().fg(Color::Black).bg(Color::Cyan),
        Mode::MigrationPreview => Style::default().fg(Color::Black).bg(Color::Green),
        Mode::LlmPending => Style::default().fg(Color::Black).bg(Color::Magenta),
        Mode::LlmPreview => Style::default().fg(Color::Black).bg(Color::Magenta),
        Mode::Help => Style::default().fg(Color::Black).bg(Color::Blue),
        Mode::InDocSearch => Style::default().fg(Color::Black).bg(Color::Green),
        Mode::ChangePreview => Style::default().fg(Color::Black).bg(Color::Yellow),
    };

    let mode_label = format!(" {} ", state.mode);

    let mut spans = vec![Span::styled(mode_label, mode_style)];

    // Show pending key indicator
    match &state.pending_key {
        app::PendingKey::None => {}
        app::PendingKey::G => {
            spans.push(Span::styled(" g...", Style::default().fg(Color::Yellow)));
        }
        app::PendingKey::CloseBracket => {
            spans.push(Span::styled(" ]...", Style::default().fg(Color::Yellow)));
        }
        app::PendingKey::OpenBracket => {
            spans.push(Span::styled(" [...", Style::default().fg(Color::Yellow)));
        }
    }

    // Show transient status message
    if let Some(ref msg) = state.status_message {
        spans.push(Span::styled(
            format!(" {msg}"),
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Show command buffer in command mode
    if state.mode == Mode::Command {
        spans.push(Span::raw(" :"));
        spans.push(Span::styled(
            &state.command_buf,
            Style::default().fg(Color::White),
        ));
    }

    // Show rename prompt
    if state.mode == Mode::Rename {
        let label = match &state.rename_target {
            Some(app::RenameTarget::Table(_)) => " Rename table: ",
            Some(app::RenameTarget::Column(_, _)) => " Rename column: ",
            None => " Rename: ",
        };
        spans.push(Span::styled(label, Style::default().fg(Color::Yellow)));
        spans.push(Span::styled(
            &state.rename_buf,
            Style::default().fg(Color::White),
        ));
    }

    // Show in-document search prompt
    if state.mode == Mode::InDocSearch {
        if let Some(ref search) = state.in_doc_search {
            let prefix = match search.direction {
                app::SearchDirection::Forward => "/",
                app::SearchDirection::Backward => "?",
            };
            spans.push(Span::raw(format!(" {prefix}")));
            spans.push(Span::styled(
                &search.query,
                Style::default().fg(Color::White),
            ));
            if !search.matches.is_empty() {
                let idx = search.current.map(|c| c + 1).unwrap_or(0);
                let total = search.matches.len();
                spans.push(Span::styled(
                    format!(" [{idx}/{total}]"),
                    Style::default().fg(Color::DarkGray),
                ));
            } else if !search.query.is_empty() {
                spans.push(Span::styled(
                    " [no matches]",
                    Style::default().fg(Color::Red),
                ));
            }
        }
    }

    // Show default prompt
    if state.mode == Mode::DefaultPrompt {
        spans.push(Span::styled(
            " DEFAULT expr: ",
            Style::default().fg(Color::Yellow),
        ));
        spans.push(Span::styled(
            &state.default_prompt_buf,
            Style::default().fg(Color::White),
        ));
    }

    // Show status message
    if let Some(ref msg) = state.status_message {
        spans.push(Span::styled(
            format!(" {msg}"),
            Style::default().fg(Color::Green),
        ));
    }

    // Show pending overlay indicator
    if state.show_pending_overlay {
        if let Some(ref overlay) = state.pending_overlay {
            let count = overlay.pending_count;
            spans.push(Span::styled(
                format!(" [{count} pending]"),
                Style::default().fg(Color::Magenta),
            ));
            if !overlay.unparseable.is_empty() {
                let n = overlay.unparseable.len();
                spans.push(Span::styled(
                    format!(" ({n} unparseable)"),
                    Style::default().fg(Color::Red),
                ));
            }
        }
    }

    // Right-aligned table count
    let table_count = state.schema.tables.len();
    let right_info = format!("{table_count} tables ");
    let left_width: usize = spans.iter().map(|s| s.width()).sum();
    let padding = (area.width as usize).saturating_sub(left_width + right_info.len());
    spans.push(Span::raw(" ".repeat(padding)));
    spans.push(Span::styled(
        right_info,
        Style::default().fg(Color::DarkGray),
    ));

    let status_bar = Line::from(spans);
    frame.render_widget(Paragraph::new(status_bar), area);
}
