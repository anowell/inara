use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use super::app::Mode;

/// A keybinding entry for display in the help overlay.
struct Binding {
    key: &'static str,
    desc: &'static str,
}

/// Normal mode keybindings (top-level only — nested sequences like `g r` are not shown).
const NORMAL_BINDINGS: &[Binding] = &[
    Binding {
        key: "j / Down",
        desc: "Move down",
    },
    Binding {
        key: "k / Up",
        desc: "Move up",
    },
    Binding {
        key: "gg",
        desc: "Jump to first",
    },
    Binding {
        key: "G",
        desc: "Jump to last",
    },
    Binding {
        key: "Ctrl-d",
        desc: "Half-page down",
    },
    Binding {
        key: "Ctrl-u",
        desc: "Half-page up",
    },
    Binding {
        key: "Ctrl-f",
        desc: "Full-page down",
    },
    Binding {
        key: "Ctrl-b",
        desc: "Full-page up",
    },
    Binding {
        key: "Enter",
        desc: "Toggle expand/collapse",
    },
    Binding {
        key: "Shift-Enter",
        desc: "Expand/collapse all",
    },
    Binding {
        key: "Tab",
        desc: "Next table",
    },
    Binding {
        key: "Shift-Tab",
        desc: "Previous table",
    },
    Binding {
        key: "Ctrl-o",
        desc: "Jump back",
    },
    Binding {
        key: "Ctrl-i",
        desc: "Jump forward",
    },
    Binding {
        key: "Space",
        desc: "Command palette",
    },
    Binding {
        key: ":",
        desc: "Command mode",
    },
    Binding {
        key: "e",
        desc: "Edit in $EDITOR",
    },
    Binding {
        key: "c",
        desc: "Change...",
    },
    Binding {
        key: "q",
        desc: "Query HUD",
    },
    Binding {
        key: "g",
        desc: "Goto...",
    },
    Binding {
        key: "/",
        desc: "Search forward",
    },
    Binding {
        key: "?",
        desc: "Search backward",
    },
    Binding {
        key: "n",
        desc: "Next match",
    },
    Binding {
        key: "N",
        desc: "Previous match",
    },
    Binding {
        key: "Ctrl-t",
        desc: "Toggle Rust types",
    },
    Binding {
        key: "]g",
        desc: "Next change",
    },
    Binding {
        key: "[g",
        desc: "Previous change",
    },
    Binding {
        key: "u",
        desc: "Undo",
    },
    Binding {
        key: "U",
        desc: "Redo",
    },
    Binding {
        key: "Ctrl-z",
        desc: "Revert change at cursor",
    },
];

const COMMAND_BINDINGS: &[Binding] = &[
    Binding {
        key: "Enter",
        desc: "Execute command",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
    Binding {
        key: "Backspace",
        desc: "Delete character",
    },
];

const SEARCH_BINDINGS: &[Binding] = &[
    Binding {
        key: "Enter",
        desc: "Select result",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
    Binding {
        key: "Down / Ctrl-n",
        desc: "Next result",
    },
    Binding {
        key: "Up / Ctrl-p",
        desc: "Previous result",
    },
    Binding {
        key: "Backspace",
        desc: "Delete character",
    },
];

const HUD_BINDINGS: &[Binding] = &[
    Binding {
        key: "Esc",
        desc: "Close HUD",
    },
    Binding {
        key: "y",
        desc: "Confirm safety warning",
    },
];

const DEFAULT_PROMPT_BINDINGS: &[Binding] = &[
    Binding {
        key: "Enter",
        desc: "Confirm default",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
    Binding {
        key: "Backspace",
        desc: "Delete character",
    },
];

const RENAME_BINDINGS: &[Binding] = &[
    Binding {
        key: "Enter",
        desc: "Confirm rename",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
];

const MIGRATION_PREVIEW_BINDINGS: &[Binding] = &[
    Binding {
        key: "Enter",
        desc: "Confirm and write",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
    Binding {
        key: "j / Down",
        desc: "Scroll down",
    },
    Binding {
        key: "k / Up",
        desc: "Scroll up",
    },
];

const SPACE_MENU_BINDINGS: &[Binding] = &[
    Binding {
        key: "f",
        desc: "Find all symbols",
    },
    Binding {
        key: "t",
        desc: "Find table",
    },
    Binding {
        key: "c",
        desc: "Find column",
    },
    Binding {
        key: "m",
        desc: "Find migration",
    },
    Binding {
        key: "p",
        desc: "Pending migrations",
    },
    Binding {
        key: "g",
        desc: "Toggle edit markers",
    },
    Binding {
        key: "d",
        desc: "Change preview",
    },
    Binding {
        key: "?",
        desc: "Help",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
];

const LLM_PENDING_BINDINGS: &[Binding] = &[Binding {
    key: "Esc",
    desc: "Cancel",
}];

const LLM_PREVIEW_BINDINGS: &[Binding] = &[
    Binding {
        key: "Enter",
        desc: "Accept suggestion",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
    Binding {
        key: "j / Down",
        desc: "Scroll down",
    },
    Binding {
        key: "k / Up",
        desc: "Scroll up",
    },
];

const CHANGE_MENU_BINDINGS: &[Binding] = &[
    Binding {
        key: "r",
        desc: "Rename element",
    },
    Binding {
        key: "R",
        desc: "Rename table (node)",
    },
    Binding {
        key: "n",
        desc: "Toggle nullable",
    },
    Binding {
        key: "u",
        desc: "Toggle unique",
    },
    Binding {
        key: "i",
        desc: "Toggle index",
    },
    Binding {
        key: "d",
        desc: "Set/clear default",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
];

const GOTO_MENU_BINDINGS: &[Binding] = &[
    Binding {
        key: "g",
        desc: "First line",
    },
    Binding {
        key: "r",
        desc: "Incoming refs",
    },
    Binding {
        key: "o",
        desc: "Outgoing refs (table)",
    },
    Binding {
        key: "d",
        desc: "FK target (column)",
    },
    Binding {
        key: "i",
        desc: "Indexes",
    },
    Binding {
        key: "c",
        desc: "First column (table)",
    },
    Binding {
        key: "t",
        desc: "Types / parent table",
    },
    Binding {
        key: "y",
        desc: "Type definition (column)",
    },
    Binding {
        key: "m",
        desc: "Migrations",
    },
    Binding {
        key: "Esc",
        desc: "Cancel",
    },
];

const CHANGE_PREVIEW_BINDINGS: &[Binding] = &[
    Binding {
        key: "s",
        desc: "Toggle SQL view",
    },
    Binding {
        key: "j / Down",
        desc: "Scroll down",
    },
    Binding {
        key: "k / Up",
        desc: "Scroll up",
    },
    Binding {
        key: "Esc",
        desc: "Close",
    },
];

const IN_DOC_SEARCH_BINDINGS: &[Binding] = &[
    Binding {
        key: "Enter",
        desc: "Confirm search",
    },
    Binding {
        key: "Esc",
        desc: "Cancel search",
    },
    Binding {
        key: "Backspace",
        desc: "Delete character",
    },
];

/// Get the keybinding list for a given mode.
fn bindings_for_mode(mode: Mode) -> &'static [Binding] {
    match mode {
        Mode::Normal | Mode::Help => NORMAL_BINDINGS,
        Mode::Command => COMMAND_BINDINGS,
        Mode::Search => SEARCH_BINDINGS,
        Mode::HUD => HUD_BINDINGS,
        Mode::DefaultPrompt => DEFAULT_PROMPT_BINDINGS,
        Mode::Rename => RENAME_BINDINGS,
        Mode::MigrationPreview => MIGRATION_PREVIEW_BINDINGS,
        Mode::SpaceMenu => SPACE_MENU_BINDINGS,
        Mode::GotoMenu => GOTO_MENU_BINDINGS,
        Mode::ChangeMenu => CHANGE_MENU_BINDINGS,
        Mode::LlmPending => LLM_PENDING_BINDINGS,
        Mode::LlmPreview => LLM_PREVIEW_BINDINGS,
        Mode::InDocSearch => IN_DOC_SEARCH_BINDINGS,
        Mode::ChangePreview => CHANGE_PREVIEW_BINDINGS,
    }
}

/// Render the help overlay centered in the given area.
///
/// Shows keybindings for `source_mode` — the mode the user was in before
/// pressing `?`.
pub fn render_help(frame: &mut Frame, area: Rect, source_mode: Mode) {
    let bindings = bindings_for_mode(source_mode);

    let title = format!(" Help — {} ", source_mode);
    let key_col_width = bindings.iter().map(|b| b.key.len()).max().unwrap_or(0);

    let mut lines: Vec<Line<'static>> = bindings
        .iter()
        .map(|b| {
            Line::from(vec![
                Span::styled(
                    format!("  {:width$}", b.key, width = key_col_width),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(format!("  {}", b.desc), Style::default().fg(Color::White)),
            ])
        })
        .collect();

    // Footer
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  Press ? or Esc to close",
        Style::default().fg(Color::DarkGray),
    )));

    let content_height = lines.len() as u16 + 2; // +2 for borders
    let content_width = {
        // key_col_width + desc max width + padding
        let desc_max = bindings.iter().map(|b| b.desc.len()).max().unwrap_or(0);
        (key_col_width + desc_max + 6) as u16 // 2 indent + 2 gap + 2 border
    }
    .max(title.len() as u16 + 2);

    let overlay_width = content_width.min(area.width.saturating_sub(4));
    let overlay_height = content_height.min(area.height.saturating_sub(2));

    // Center horizontally, place near top
    let x = area.x + area.width.saturating_sub(overlay_width) / 2;
    let y = area.y + 2;
    let overlay_area = Rect::new(x, y, overlay_width, overlay_height);

    frame.render_widget(Clear, overlay_area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue))
        .title(title)
        .title_style(
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        );

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, overlay_area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_bindings_not_empty() {
        let bindings = bindings_for_mode(Mode::Normal);
        assert!(!bindings.is_empty());
    }

    #[test]
    fn help_mode_shows_normal_bindings() {
        // Help mode should show the same bindings as Normal mode
        let help = bindings_for_mode(Mode::Help);
        let normal = bindings_for_mode(Mode::Normal);
        assert_eq!(help.len(), normal.len());
    }

    #[test]
    fn all_modes_have_bindings() {
        let modes = [
            Mode::Normal,
            Mode::DefaultPrompt,
            Mode::Rename,
            Mode::Search,
            Mode::HUD,
            Mode::Command,
            Mode::SpaceMenu,
            Mode::GotoMenu,
            Mode::ChangeMenu,
            Mode::MigrationPreview,
            Mode::LlmPending,
            Mode::LlmPreview,
            Mode::Help,
            Mode::InDocSearch,
            Mode::ChangePreview,
        ];
        for mode in modes {
            let bindings = bindings_for_mode(mode);
            assert!(
                !bindings.is_empty(),
                "Mode {:?} should have at least one binding",
                mode
            );
        }
    }

    #[test]
    fn space_menu_includes_help_key() {
        let bindings = bindings_for_mode(Mode::SpaceMenu);
        assert!(
            bindings.iter().any(|b| b.key == "?"),
            "Space menu should include the ? key"
        );
    }

    #[test]
    fn normal_mode_shows_change_as_prefix() {
        let bindings = bindings_for_mode(Mode::Normal);
        let change = bindings.iter().find(|b| b.key == "c");
        assert!(
            change.is_some(),
            "Normal mode should show c as a change key"
        );
        assert!(
            change.unwrap().desc.contains("Change"),
            "c should be described as Change"
        );
    }

    #[test]
    fn normal_mode_shows_goto_as_prefix() {
        let bindings = bindings_for_mode(Mode::Normal);
        let goto = bindings.iter().find(|b| b.key == "g");
        assert!(goto.is_some(), "Normal mode should show g as a goto key");
        assert!(
            goto.unwrap().desc.contains("Goto"),
            "g should be described as Goto"
        );
    }
}
