pub mod app;
pub mod edit;
pub mod fuzzy;
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
use self::input::handle_key;

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
pub async fn run(database_url: &str, connection_info: String) -> Result<()> {
    // Initialize file-based tracing before entering TUI mode
    init_file_tracing();

    tracing::info!("Connecting to database...");
    let pool = sqlx::PgPool::connect(database_url).await?;
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

    let state = AppState::new(schema, connection_info);

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
                        let (new_state, new_handle) = handle_key(state, key, &pool);
                        state = new_state;
                        if let Some(h) = new_handle {
                            hud_handle = Some(h);
                        }
                        // Clear handle when leaving HUD mode
                        if state.mode != Mode::HUD {
                            hud_handle = None;
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
        Mode::Search => {
            if let Some(ref search) = state.search {
                fuzzy::render_search_overlay(frame, layout[1], search);
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

/// Render the main content area with the schema document or edit buffer.
fn draw_content(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    if state.mode == Mode::Edit {
        draw_edit_content(frame, area, state);
    } else {
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
}

/// Render the edit mode content area with the text buffer.
fn draw_edit_content(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    let title = match &state.edit_table {
        Some(name) => format!(" Editing: {name} "),
        None => " Editing ".to_string(),
    };

    let border_color = if state.edit_error.is_some() {
        Color::Red
    } else {
        Color::Yellow
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title(title);

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let lines: Vec<Line> = state
        .edit_buffer
        .iter()
        .enumerate()
        .map(|(i, line)| {
            if i == state.edit_cursor_row {
                // Show cursor position with a highlighted character
                let col = state.edit_cursor_col.min(line.len());
                let before = &line[..col];
                let cursor_char = line.get(col..col + 1).unwrap_or(" ");
                let after = if col < line.len() {
                    &line[col + 1..]
                } else {
                    ""
                };
                Line::from(vec![
                    Span::styled(before.to_string(), Style::default().fg(Color::White)),
                    Span::styled(
                        cursor_char.to_string(),
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(after.to_string(), Style::default().fg(Color::White)),
                ])
            } else {
                Line::from(Span::styled(
                    line.to_string(),
                    Style::default().fg(Color::White),
                ))
            }
        })
        .collect();

    let content = Paragraph::new(lines);
    frame.render_widget(content, inner);
}

/// Render the status bar with current mode and context info.
fn draw_status_bar(frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
    let mode_style = match state.mode {
        Mode::Normal => Style::default().fg(Color::Black).bg(Color::Blue),
        Mode::Edit => Style::default().fg(Color::Black).bg(Color::Yellow),
        Mode::Rename => Style::default().fg(Color::Black).bg(Color::Yellow),
        Mode::Search => Style::default().fg(Color::Black).bg(Color::Green),
        Mode::HUD => Style::default().fg(Color::Black).bg(Color::Magenta),
        Mode::Command => Style::default().fg(Color::Black).bg(Color::Red),
        Mode::SpaceMenu => Style::default().fg(Color::Black).bg(Color::Cyan),
    };

    let mode_label = format!(" {} ", state.mode);

    let mut spans = vec![Span::styled(mode_label, mode_style)];

    // Show pending key indicator
    if state.pending_key != app::PendingKey::None {
        spans.push(Span::styled(" g", Style::default().fg(Color::Yellow)));
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

    // Show edit error
    if let Some(ref err) = state.edit_error {
        spans.push(Span::styled(
            format!(" Error: {err}"),
            Style::default().fg(Color::Red),
        ));
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
