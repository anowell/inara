use std::collections::BTreeSet;

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

use super::app::{AppState, FocusTarget};
use crate::migration::overlay::ChangeMarker;
use crate::schema::{Constraint, CustomTypeKind};

/// Style constants for syntax highlighting.
const KEYWORD_STYLE: Style = Style::new().fg(Color::Yellow);
const TYPE_STYLE: Style = Style::new().fg(Color::Cyan);
const NAME_STYLE: Style = Style::new().fg(Color::White);
const DIM_STYLE: Style = Style::new().fg(Color::DarkGray);
const NORMAL_STYLE: Style = Style::new().fg(Color::White);

/// Search match highlight background color.
const SEARCH_MATCH_BG: Color = Color::Indexed(58); // dark olive/yellow
/// Current search match highlight background color.
const SEARCH_CURRENT_BG: Color = Color::Indexed(94); // orange/brown

/// Extract plain text from a document line for search matching.
///
/// Returns the concatenated content of all spans that would be rendered
/// for this line, without focus highlighting or overlay markers.
pub fn line_plain_text(state: &AppState, target: &FocusTarget) -> String {
    let spans = match target {
        FocusTarget::Enum(name) => render_enum_header(state, name),
        FocusTarget::EnumVariant(name, idx) => render_enum_variant(state, name, *idx),
        FocusTarget::EnumClose(_) => vec![Span::styled("}", NORMAL_STYLE)],
        FocusTarget::Type(name) => render_type_header(state, name),
        FocusTarget::TypeField(name, idx) => render_type_field(state, name, *idx),
        FocusTarget::TypeClose(_) => vec![Span::styled("}", NORMAL_STYLE)],
        FocusTarget::Table(name) => render_table_header(state, name),
        FocusTarget::Column(table, col) => render_column_line(state, table, col),
        FocusTarget::Separator(_) => vec![Span::raw("")],
        FocusTarget::Constraint(table, idx) => render_constraint_line(state, table, *idx),
        FocusTarget::Index(table, idx) => render_index_line(state, table, *idx),
        FocusTarget::TableClose(_) => vec![Span::styled("}", NORMAL_STYLE)],
        FocusTarget::Blank => vec![Span::raw("")],
    };
    spans.iter().map(|s| s.content.as_ref()).collect()
}

/// Apply a background color to all spans in a line (for search highlighting).
fn apply_bg(line: Line<'static>, bg: Color) -> Line<'static> {
    let spans: Vec<Span> = line
        .spans
        .into_iter()
        .map(|s| Span::styled(s.content.into_owned(), s.style.bg(bg)))
        .collect();
    Line::from(spans)
}

/// Render visible document lines as styled ratatui Lines.
///
/// Reads from AppState to determine what to render and where the cursor is.
/// Returns the lines visible in the current viewport.
pub fn render_document(state: &AppState) -> Vec<Line<'static>> {
    if state.doc.is_empty() {
        return vec![Line::from(Span::styled(
            "No schema elements found.",
            DIM_STYLE,
        ))];
    }

    let viewport_end = (state.viewport_offset + state.viewport_height).min(state.doc.len());

    // Pre-compute search match sets for highlighting
    let search_matches: Option<&[usize]> = state
        .in_doc_search
        .as_ref()
        .filter(|s| !s.matches.is_empty())
        .map(|s| s.matches.as_slice());
    let current_match_line: Option<usize> = state
        .in_doc_search
        .as_ref()
        .and_then(|s| s.current.and_then(|c| s.matches.get(c).copied()));

    state.doc[state.viewport_offset..viewport_end]
        .iter()
        .enumerate()
        .map(|(i, doc_line)| {
            let line_index = state.viewport_offset + i;
            let is_focused = line_index == state.cursor;
            let line = if doc_line.ghost {
                render_ghost_line(state, &doc_line.target, is_focused)
            } else {
                render_line(state, &doc_line.target, is_focused)
            };

            // Apply search match highlighting (focus style takes priority)
            if !is_focused {
                if let Some(matches) = search_matches {
                    if Some(line_index) == current_match_line {
                        return apply_bg(line, SEARCH_CURRENT_BG);
                    } else if matches.binary_search(&line_index).is_ok() {
                        return apply_bg(line, SEARCH_MATCH_BG);
                    }
                }
            }
            line
        })
        .collect()
}

/// Get the overlay change marker for a focus target, if any.
fn overlay_marker(state: &AppState, target: &FocusTarget) -> Option<ChangeMarker> {
    if !state.show_pending_overlay {
        return None;
    }
    let overlay = state.pending_overlay.as_ref()?;
    match target {
        FocusTarget::Table(name) | FocusTarget::TableClose(name) | FocusTarget::Separator(name) => {
            overlay.table_marker(name)
        }
        FocusTarget::Column(table, col) => overlay.column_marker(table, col),
        FocusTarget::Constraint(table, _) | FocusTarget::Index(table, _) => {
            overlay.table_marker(table)
        }
        _ => None,
    }
}

/// Get the edit change marker for a focus target, if any.
fn edit_marker(state: &AppState, target: &FocusTarget) -> Option<ChangeMarker> {
    if !state.show_edit_changes {
        return None;
    }
    let overlay = state.edit_overlay.as_ref()?;
    match target {
        FocusTarget::Table(name) | FocusTarget::TableClose(name) | FocusTarget::Separator(name) => {
            overlay.table_marker(name)
        }
        FocusTarget::Column(table, col) => overlay.column_marker(table, col),
        FocusTarget::Constraint(table, _) | FocusTarget::Index(table, _) => {
            overlay.table_marker(table)
        }
        _ => None,
    }
}

/// Get the style for a change marker.
fn marker_style(marker: ChangeMarker) -> Style {
    match marker {
        ChangeMarker::Added => OVERLAY_ADDED_STYLE,
        ChangeMarker::Removed => OVERLAY_REMOVED_STYLE,
        ChangeMarker::Modified => OVERLAY_MODIFIED_STYLE,
    }
}

/// Render a single document line based on its FocusTarget.
fn render_line(state: &AppState, target: &FocusTarget, is_focused: bool) -> Line<'static> {
    render_line_inner(state, target, is_focused, false)
}

/// Render a ghost line (removed element) with dim+strikethrough styling.
fn render_ghost_line(state: &AppState, target: &FocusTarget, is_focused: bool) -> Line<'static> {
    render_line_inner(state, target, is_focused, true)
}

/// Inner render function that handles both normal and ghost lines.
fn render_line_inner(
    state: &AppState,
    target: &FocusTarget,
    is_focused: bool,
    is_ghost: bool,
) -> Line<'static> {
    let marker = if is_ghost {
        Some(ChangeMarker::Removed)
    } else {
        edit_marker(state, target).or_else(|| overlay_marker(state, target))
    };

    let mut spans = if is_ghost {
        // Ghost lines render from original_schema data with dim+strikethrough
        render_ghost_spans(state, target)
    } else {
        match target {
            FocusTarget::Enum(name) => render_enum_header(state, name),
            FocusTarget::EnumVariant(name, idx) => render_enum_variant(state, name, *idx),
            FocusTarget::EnumClose(_) => vec![Span::styled("}", NORMAL_STYLE)],
            FocusTarget::Type(name) => render_type_header(state, name),
            FocusTarget::TypeField(name, idx) => render_type_field(state, name, *idx),
            FocusTarget::TypeClose(_) => vec![Span::styled("}", NORMAL_STYLE)],
            FocusTarget::Table(name) => render_table_header(state, name),
            FocusTarget::Column(table, col) => render_column_line(state, table, col),
            FocusTarget::Separator(_) => vec![Span::raw("")],
            FocusTarget::Constraint(table, idx) => render_constraint_line(state, table, *idx),
            FocusTarget::Index(table, idx) => render_index_line(state, table, *idx),
            FocusTarget::TableClose(_) => vec![Span::styled("}", NORMAL_STYLE)],
            FocusTarget::Blank => vec![Span::raw("")],
        }
    };

    // Prepend overlay marker if applicable
    if let Some(m) = marker {
        spans.insert(0, Span::styled(m.prefix().to_string(), marker_style(m)));
    }

    if is_ghost {
        // Apply dim+strikethrough to all spans for ghost lines
        let spans: Vec<Span> = spans
            .into_iter()
            .map(|s| {
                Span::styled(
                    s.content.into_owned(),
                    DIM_STYLE.add_modifier(Modifier::CROSSED_OUT),
                )
            })
            .collect();
        if is_focused {
            let spans: Vec<Span> = spans
                .into_iter()
                .map(|s| Span::styled(s.content.into_owned(), s.style.bg(Color::Indexed(236))))
                .collect();
            Line::from(spans)
        } else {
            Line::from(spans)
        }
    } else if is_focused {
        // Apply focus background to all spans
        let spans: Vec<Span> = spans
            .into_iter()
            .map(|s| {
                Span::styled(
                    s.content.into_owned(),
                    s.style.bg(Color::Indexed(236)).add_modifier(Modifier::BOLD),
                )
            })
            .collect();
        Line::from(spans)
    } else {
        Line::from(spans)
    }
}

/// Render an enum header: `enum name {` or `enum name { }`
fn render_enum_header(state: &AppState, name: &str) -> Vec<Span<'static>> {
    let enum_type = state.schema.enums.get(name);
    let is_empty = enum_type.map(|e| e.variants.is_empty()).unwrap_or(true);
    let is_expanded = state.expanded.contains(name);

    let mut spans = vec![
        Span::styled("enum ", KEYWORD_STYLE),
        Span::styled(name.to_string(), NAME_STYLE),
        Span::styled(" {", NORMAL_STYLE),
    ];
    if is_expanded && is_empty {
        spans.push(Span::styled(" }", NORMAL_STYLE));
    } else if !is_expanded {
        if is_empty {
            spans.push(Span::styled(" }", NORMAL_STYLE));
        } else {
            let variant_count = enum_type.map(|e| e.variants.len()).unwrap_or(0);
            spans.push(Span::styled(
                format!(" ... {variant_count} variants ... }}"),
                DIM_STYLE,
            ));
        }
    }
    spans
}

/// Render a single enum variant line.
fn render_enum_variant(state: &AppState, name: &str, idx: usize) -> Vec<Span<'static>> {
    let variant = state
        .schema
        .enums
        .get(name)
        .and_then(|e| e.variants.get(idx))
        .cloned()
        .unwrap_or_default();
    vec![Span::styled(format!("    {variant}"), NORMAL_STYLE)]
}

/// Render a custom type header line.
fn render_type_header(state: &AppState, name: &str) -> Vec<Span<'static>> {
    let custom_type = match state.schema.types.get(name) {
        Some(ct) => ct,
        None => return vec![Span::styled(format!("type {name}"), NORMAL_STYLE)],
    };

    match &custom_type.kind {
        CustomTypeKind::Domain {
            base_type,
            constraints,
        } => {
            let mut spans = vec![
                Span::styled("domain ", KEYWORD_STYLE),
                Span::styled(name.to_string(), NAME_STYLE),
                Span::styled(format!(" {base_type}"), TYPE_STYLE),
            ];
            for c in constraints {
                spans.push(Span::styled(format!(" {c}"), KEYWORD_STYLE));
            }
            spans
        }
        CustomTypeKind::Composite { fields } => {
            let is_expanded = state.expanded.contains(name);
            let mut spans = vec![
                Span::styled("composite ", KEYWORD_STYLE),
                Span::styled(name.to_string(), NAME_STYLE),
                Span::styled(" {", NORMAL_STYLE),
            ];
            if is_expanded && fields.is_empty() {
                spans.push(Span::styled(" }", NORMAL_STYLE));
            } else if !is_expanded {
                if fields.is_empty() {
                    spans.push(Span::styled(" }", NORMAL_STYLE));
                } else {
                    let field_count = fields.len();
                    spans.push(Span::styled(
                        format!(" ... {field_count} fields ... }}"),
                        DIM_STYLE,
                    ));
                }
            }
            spans
        }
        CustomTypeKind::Range { subtype } => {
            vec![
                Span::styled("range ", KEYWORD_STYLE),
                Span::styled(name.to_string(), NAME_STYLE),
                Span::styled(format!(" {subtype}"), TYPE_STYLE),
            ]
        }
    }
}

/// Render a composite type field line.
fn render_type_field(state: &AppState, name: &str, idx: usize) -> Vec<Span<'static>> {
    let custom_type = match state.schema.types.get(name) {
        Some(ct) => ct,
        None => return vec![Span::raw("")],
    };

    if let CustomTypeKind::Composite { fields } = &custom_type.kind {
        if let Some((field_name, pg_type)) = fields.get(idx) {
            let max_name_len = fields.iter().map(|(n, _)| n.len()).max().unwrap_or(0);
            let spans = if state.show_rust_types {
                let rust_type = state.type_mapper.rust_type(pg_type);
                let name_part = format!("{field_name}: ");
                let name_width = max_name_len + 2; // +2 for ": "
                vec![
                    Span::styled(format!("    {name_part:<name_width$}"), NAME_STYLE),
                    Span::styled(rust_type, TYPE_STYLE),
                ]
            } else {
                vec![
                    Span::styled(format!("    {field_name:<max_name_len$}  "), NAME_STYLE),
                    Span::styled(pg_type.to_string(), TYPE_STYLE),
                ]
            };
            return spans;
        }
    }
    vec![Span::raw("")]
}

/// Overlay marker styles.
const OVERLAY_ADDED_STYLE: Style = Style::new().fg(Color::Green);
const OVERLAY_REMOVED_STYLE: Style = Style::new().fg(Color::Red);
const OVERLAY_MODIFIED_STYLE: Style = Style::new().fg(Color::Yellow);

/// Render a table header line.
///
/// Collapsed: `table name { ... N columns ... }`
/// Expanded:  `table name {` (or `table name { }` if empty)
/// Edited tables show a `~ ` prefix as a visual diff hint.
fn render_table_header(state: &AppState, name: &str) -> Vec<Span<'static>> {
    let table = match state.schema.table(name) {
        Some(t) => t,
        None => return vec![Span::styled(format!("table {name}"), NORMAL_STYLE)],
    };

    let is_expanded = state.expanded.contains(name);
    let is_empty =
        table.columns.is_empty() && table.constraints.is_empty() && table.indexes.is_empty();

    let mut spans = Vec::new();

    spans.push(Span::styled("table ", KEYWORD_STYLE));
    spans.push(Span::styled(name.to_string(), NAME_STYLE));
    spans.push(Span::styled(" {", NORMAL_STYLE));

    if is_expanded && is_empty {
        spans.push(Span::styled(" }", NORMAL_STYLE));
    } else if !is_expanded {
        let col_count = table.columns.len();
        spans.push(Span::styled(
            format!(" ... {col_count} columns ... }}"),
            DIM_STYLE,
        ));
    }

    spans
}

/// Render a column line inside an expanded table.
fn render_column_line(state: &AppState, table_name: &str, col_name: &str) -> Vec<Span<'static>> {
    let table = match state.schema.table(table_name) {
        Some(t) => t,
        None => return vec![Span::raw("")],
    };

    let col = match table.column(col_name) {
        Some(c) => c,
        None => return vec![Span::raw("")],
    };

    let single_pk_cols = single_column_pk_set(&table.constraints);
    let single_unique_cols = single_column_unique_set(&table.constraints);

    let max_name_len = table
        .columns
        .iter()
        .map(|c| c.name.len())
        .max()
        .unwrap_or(0);

    // Build suffix parts
    let has_suffix = !col.nullable
        || col.default.is_some()
        || single_pk_cols.contains(&col.name)
        || single_unique_cols.contains(&col.name);

    let mut spans = if state.show_rust_types {
        // Struct field syntax: "    name: RustType"
        let rust_type = state
            .type_mapper
            .rust_type_annotation(&col.pg_type, col.nullable);
        let max_type_len = table
            .columns
            .iter()
            .map(|c| {
                state
                    .type_mapper
                    .rust_type_annotation(&c.pg_type, c.nullable)
                    .len()
            })
            .max()
            .unwrap_or(0);

        let padded_type = if has_suffix {
            format!("{rust_type:<max_type_len$}")
        } else {
            rust_type
        };

        // Pad "name: " so types align vertically (colon stays adjacent to name)
        let name_part = format!("{}: ", col.name);
        let name_width = max_name_len + 2; // +2 for ": "
        vec![
            Span::styled(format!("    {name_part:<name_width$}"), NAME_STYLE),
            Span::styled(padded_type, TYPE_STYLE),
        ]
    } else {
        // PG type syntax: "    name  pg_type"
        let type_str = col.pg_type.to_string();
        let max_type_len = table
            .columns
            .iter()
            .map(|c| c.pg_type.to_string().len())
            .max()
            .unwrap_or(0);

        let padded_type = if has_suffix {
            format!("{type_str:<max_type_len$}")
        } else {
            type_str
        };

        vec![
            Span::styled(format!("    {:<max_name_len$}  ", col.name), NAME_STYLE),
            Span::styled(padded_type, TYPE_STYLE),
        ]
    };

    if !col.nullable {
        spans.push(Span::styled("  NOT NULL", KEYWORD_STYLE));
    }
    if let Some(default) = &col.default {
        spans.push(Span::styled("  DEFAULT ", KEYWORD_STYLE));
        spans.push(Span::styled(default.to_string(), NORMAL_STYLE));
    }
    if single_pk_cols.contains(&col.name) {
        spans.push(Span::styled("  PRIMARY KEY", KEYWORD_STYLE));
    }
    if single_unique_cols.contains(&col.name) {
        spans.push(Span::styled("  UNIQUE", KEYWORD_STYLE));
    }

    spans
}

/// Render a constraint line inside an expanded table.
fn render_constraint_line(state: &AppState, table_name: &str, idx: usize) -> Vec<Span<'static>> {
    let table = match state.schema.table(table_name) {
        Some(t) => t,
        None => return vec![Span::raw("")],
    };

    let constraint = match table.constraints.get(idx) {
        Some(c) => c,
        None => return vec![Span::raw("")],
    };

    match constraint {
        Constraint::PrimaryKey { columns, .. } => {
            let cols = columns.join(", ");
            vec![
                Span::styled("    PRIMARY KEY", KEYWORD_STYLE),
                Span::styled(format!(" ({cols})"), NORMAL_STYLE),
            ]
        }
        Constraint::Unique { columns, .. } => {
            let cols = columns.join(", ");
            vec![
                Span::styled("    UNIQUE", KEYWORD_STYLE),
                Span::styled(format!(" ({cols})"), NORMAL_STYLE),
            ]
        }
        Constraint::ForeignKey {
            columns,
            references,
            on_delete,
            on_update,
            ..
        } => {
            let cols = columns.join(", ");
            let ref_cols = references.columns.join(", ");
            let mut spans = vec![
                Span::styled("    FOREIGN KEY", KEYWORD_STYLE),
                Span::styled(format!(" ({cols}) "), NORMAL_STYLE),
                Span::styled("REFERENCES ", KEYWORD_STYLE),
                Span::styled(format!("{}({ref_cols})", references.table), NORMAL_STYLE),
            ];
            if let Some(action) = on_delete {
                spans.push(Span::styled(" ON DELETE ", KEYWORD_STYLE));
                spans.push(Span::styled(action.to_string(), KEYWORD_STYLE));
            }
            if let Some(action) = on_update {
                spans.push(Span::styled(" ON UPDATE ", KEYWORD_STYLE));
                spans.push(Span::styled(action.to_string(), KEYWORD_STYLE));
            }
            spans
        }
        Constraint::Check { expression, .. } => {
            vec![
                Span::styled("    CHECK", KEYWORD_STYLE),
                Span::styled(format!(" ({expression})"), NORMAL_STYLE),
            ]
        }
    }
}

/// Render an index line inside an expanded table.
fn render_index_line(state: &AppState, table_name: &str, idx: usize) -> Vec<Span<'static>> {
    let table = match state.schema.table(table_name) {
        Some(t) => t,
        None => return vec![Span::raw("")],
    };

    let index = match table.indexes.get(idx) {
        Some(i) => i,
        None => return vec![Span::raw("")],
    };

    let cols = index.columns.join(", ");
    let mut spans = if index.unique {
        vec![
            Span::styled("    UNIQUE INDEX ", KEYWORD_STYLE),
            Span::styled(format!("{}({cols})", index.name), NORMAL_STYLE),
        ]
    } else {
        vec![
            Span::styled("    INDEX ", KEYWORD_STYLE),
            Span::styled(format!("{}({cols})", index.name), NORMAL_STYLE),
        ]
    };

    if let Some(where_clause) = &index.partial {
        spans.push(Span::styled(format!(" {where_clause}"), DIM_STYLE));
    }

    spans
}

/// Render spans for a ghost line, looking up data from original_schema.
fn render_ghost_spans(state: &AppState, target: &FocusTarget) -> Vec<Span<'static>> {
    let original = match &state.original_schema {
        Some(s) => s,
        None => return vec![Span::styled("  (removed)", DIM_STYLE)],
    };

    match target {
        FocusTarget::Table(name) => {
            // Ghost table header: render from original schema
            let table = original.table(name);
            let col_count = table.map(|t| t.columns.len()).unwrap_or(0);
            vec![
                Span::styled("table ", KEYWORD_STYLE),
                Span::styled(name.to_string(), NAME_STYLE),
                Span::styled(format!(" {{ ... {col_count} columns ... }}"), DIM_STYLE),
            ]
        }
        FocusTarget::Column(table_name, col_name) => {
            // Ghost column: render from original schema
            let col = original.table(table_name).and_then(|t| t.column(col_name));
            match col {
                Some(col) => {
                    let type_str = col.pg_type.to_string();
                    let mut spans = vec![
                        Span::styled(format!("    {}  ", col.name), NAME_STYLE),
                        Span::styled(type_str, TYPE_STYLE),
                    ];
                    if !col.nullable {
                        spans.push(Span::styled("  NOT NULL", KEYWORD_STYLE));
                    }
                    spans
                }
                None => vec![Span::styled(format!("    {col_name}"), DIM_STYLE)],
            }
        }
        _ => vec![Span::styled("  (removed)", DIM_STYLE)],
    }
}

/// Collect single-column PK column names.
fn single_column_pk_set(constraints: &[Constraint]) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for c in constraints {
        if let Constraint::PrimaryKey { columns, .. } = c {
            if columns.len() == 1 {
                set.insert(columns[0].clone());
            }
        }
    }
    set
}

/// Collect single-column unique constraint column names.
fn single_column_unique_set(constraints: &[Constraint]) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for c in constraints {
        if let Constraint::Unique { columns, .. } = c {
            if columns.len() == 1 {
                set.insert(columns[0].clone());
            }
        }
    }
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{Expression, ForeignKeyRef, PgType, ReferentialAction};
    use crate::schema::{
        Column, Constraint, CustomType, CustomTypeKind, EnumType, Index, Schema, Table,
    };
    use crate::tui::app::AppState;

    fn spans_to_string(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    fn simple_state() -> AppState {
        let mut schema = Schema::new();
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
        schema.add_table(table);
        AppState::new(schema, String::new(), None)
    }

    #[test]
    fn empty_schema_shows_placeholder() {
        let state = AppState::new(Schema::new(), String::new(), None).with_viewport_height(10);
        let lines = render_document(&state);
        assert_eq!(lines.len(), 1);
        assert_eq!(spans_to_string(&lines[0]), "No schema elements found.");
    }

    #[test]
    fn collapsed_table_shows_summary() {
        let state = simple_state().with_viewport_height(10);
        let lines = render_document(&state);
        assert_eq!(lines.len(), 1);
        let text = spans_to_string(&lines[0]);
        assert!(text.contains("table "));
        assert!(text.contains("users"));
        assert!(text.contains("2 columns"));
    }

    #[test]
    fn expanded_table_shows_columns() {
        let state = simple_state().with_viewport_height(20).toggle_expand();
        let lines = render_document(&state);
        // header + 2 columns + close = 4
        assert_eq!(lines.len(), 4);

        let header_text = spans_to_string(&lines[0]);
        assert!(header_text.contains("table "));
        assert!(header_text.contains("users"));
        assert!(header_text.contains("{"));
        assert!(!header_text.contains("columns")); // no summary when expanded

        let id_text = spans_to_string(&lines[1]);
        assert!(id_text.contains("id"));
        assert!(id_text.contains("uuid"));
        assert!(id_text.contains("NOT NULL"));
        assert!(id_text.contains("PRIMARY KEY"));
        assert!(id_text.contains("DEFAULT"));
        assert!(id_text.contains("gen_random_uuid()"));

        let email_text = spans_to_string(&lines[2]);
        assert!(email_text.contains("email"));
        assert!(email_text.contains("text"));
        assert!(email_text.contains("NOT NULL"));
        assert!(email_text.contains("UNIQUE"));

        let close_text = spans_to_string(&lines[3]);
        assert_eq!(close_text, "}");
    }

    #[test]
    fn focus_highlights_current_line() {
        let state = simple_state().with_viewport_height(10);
        let lines = render_document(&state);
        // The focused line (cursor=0) should have the highlight background
        assert!(lines[0]
            .spans
            .iter()
            .any(|s| s.style.bg == Some(Color::Indexed(236))));
    }

    #[test]
    fn enum_renders_with_syntax_highlighting() {
        let mut schema = Schema::new();
        schema.add_enum(EnumType {
            name: "mood".into(),
            variants: vec!["happy".into(), "sad".into()],
        });
        let state = AppState::new(schema, String::new(), None)
            .with_viewport_height(20)
            .toggle_expand(); // expand the enum
        let lines = render_document(&state);
        // header + 2 variants + close = 4
        assert_eq!(lines.len(), 4);

        let header_text = spans_to_string(&lines[0]);
        assert!(header_text.contains("enum "));
        assert!(header_text.contains("mood"));

        let variant_text = spans_to_string(&lines[1]);
        assert!(variant_text.contains("happy"));
    }

    #[test]
    fn table_with_fk_renders_constraints() {
        let mut schema = Schema::new();
        let mut posts = Table::new("posts");
        posts.add_column(Column::new("id", PgType::Uuid));
        posts.add_column(Column::new("author_id", PgType::Uuid));
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

        let mut state = AppState::new(schema, String::new(), None).with_viewport_height(30);
        state = state.toggle_expand();
        let lines = render_document(&state);

        // Find the FK line
        let fk_text: String = lines
            .iter()
            .map(|l| spans_to_string(l))
            .find(|s| s.contains("FOREIGN KEY"))
            .expect("should have FK line");
        assert!(fk_text.contains("REFERENCES"));
        assert!(fk_text.contains("users(id)"));
        assert!(fk_text.contains("ON DELETE"));
        assert!(fk_text.contains("CASCADE"));

        // Find index line
        let idx_text: String = lines
            .iter()
            .map(|l| spans_to_string(l))
            .find(|s| s.contains("INDEX "))
            .expect("should have index line");
        assert!(idx_text.contains("posts_author_idx(author_id)"));
    }

    #[test]
    fn custom_type_domain_renders() {
        let mut schema = Schema::new();
        schema.add_type(CustomType {
            name: "email_addr".into(),
            kind: CustomTypeKind::Domain {
                base_type: PgType::Text,
                constraints: vec!["CHECK (VALUE ~ '^.+@.+$')".into()],
            },
        });
        let state = AppState::new(schema, String::new(), None).with_viewport_height(10);
        let lines = render_document(&state);
        assert_eq!(lines.len(), 1);
        let text = spans_to_string(&lines[0]);
        assert!(text.contains("domain "));
        assert!(text.contains("email_addr"));
        assert!(text.contains("text"));
    }

    #[test]
    fn custom_type_composite_renders() {
        let mut schema = Schema::new();
        schema.add_type(CustomType {
            name: "address".into(),
            kind: CustomTypeKind::Composite {
                fields: vec![
                    ("street".into(), PgType::Text),
                    ("city".into(), PgType::Text),
                ],
            },
        });
        let state = AppState::new(schema, String::new(), None)
            .with_viewport_height(10)
            .toggle_expand(); // expand the composite type
        let lines = render_document(&state);
        // header + 2 fields + close = 4
        assert_eq!(lines.len(), 4);

        let header_text = spans_to_string(&lines[0]);
        assert!(header_text.contains("composite "));
        assert!(header_text.contains("address"));

        let field_text = spans_to_string(&lines[1]);
        assert!(field_text.contains("street"));
        assert!(field_text.contains("text"));
    }

    #[test]
    fn viewport_clips_lines() {
        let mut schema = Schema::new();
        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            schema.add_table(Table::new(name));
        }
        // 5 collapsed tables = 5 lines, viewport only shows 3
        let state = AppState::new(schema, String::new(), None).with_viewport_height(3);
        let lines = render_document(&state);
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn empty_table_expanded_renders_single_line() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("empty"));
        let state = AppState::new(schema, String::new(), None)
            .with_viewport_height(10)
            .toggle_expand();
        let lines = render_document(&state);
        // Empty table when expanded: just the header with `{ }`
        assert_eq!(lines.len(), 1);
        let text = spans_to_string(&lines[0]);
        assert!(text.contains("table "));
        assert!(text.contains("empty"));
        assert!(text.contains("{ }"));
    }

    // --- Rust type annotation tests ---

    #[test]
    fn column_shows_rust_type_when_enabled() {
        let state = simple_state()
            .with_viewport_height(20)
            .toggle_expand()
            .toggle_rust_types();
        assert!(state.show_rust_types);
        let lines = render_document(&state);

        let id_text = spans_to_string(&lines[1]);
        assert!(
            id_text.contains("id:") && id_text.contains("uuid::Uuid"),
            "should show struct field syntax, got: {id_text}"
        );
        assert!(
            !id_text.contains("//"),
            "should not use comment syntax, got: {id_text}"
        );

        let email_text = spans_to_string(&lines[2]);
        assert!(
            email_text.contains("email:") && email_text.contains("String"),
            "should show struct field syntax, got: {email_text}"
        );
    }

    #[test]
    fn column_hides_rust_type_when_disabled() {
        let state = simple_state().with_viewport_height(20).toggle_expand();
        assert!(!state.show_rust_types);
        let lines = render_document(&state);

        let id_text = spans_to_string(&lines[1]);
        assert!(
            !id_text.contains("//"),
            "should NOT show Rust type annotation"
        );
    }

    #[test]
    fn rust_type_toggle_on_off() {
        let state = simple_state()
            .with_viewport_height(20)
            .toggle_expand()
            .toggle_rust_types();
        assert!(state.show_rust_types);

        let state = state.toggle_rust_types();
        assert!(!state.show_rust_types);
        let lines = render_document(&state);
        let id_text = spans_to_string(&lines[1]);
        assert!(!id_text.contains("//"), "after toggle off, no annotation");
    }

    #[test]
    fn rust_type_with_chrono_feature() {
        use crate::schema::type_map::{DetectedFeatures, TypeMapper};

        let mut schema = Schema::new();
        let mut table = Table::new("events");
        table.add_column(Column::new("created_at", PgType::Timestamptz));
        schema.add_table(table);

        let mapper = TypeMapper::with_features(DetectedFeatures {
            chrono: true,
            time: false,
            jiff: false,
        });

        let state = AppState::new(schema, String::new(), None)
            .with_type_mapper(mapper)
            .with_viewport_height(20)
            .toggle_expand()
            .toggle_rust_types();

        let lines = render_document(&state);
        let text = spans_to_string(&lines[1]);
        assert!(
            text.contains("created_at:") && text.contains("chrono::DateTime<Utc>"),
            "should show chrono type as struct field, got: {text}"
        );
    }

    #[test]
    fn composite_type_shows_rust_type() {
        let mut schema = Schema::new();
        schema.add_type(CustomType {
            name: "address".into(),
            kind: CustomTypeKind::Composite {
                fields: vec![
                    ("street".into(), PgType::Text),
                    ("zip".into(), PgType::Integer),
                ],
            },
        });
        let state = AppState::new(schema, String::new(), None)
            .with_viewport_height(20)
            .toggle_expand() // expand the composite type
            .toggle_rust_types();
        let lines = render_document(&state);

        let street_text = spans_to_string(&lines[1]);
        assert!(
            street_text.contains("street:") && street_text.contains("String"),
            "composite field should show struct field syntax, got: {street_text}"
        );

        let zip_text = spans_to_string(&lines[2]);
        assert!(
            zip_text.contains("zip:") && zip_text.contains("i32"),
            "composite field should show struct field syntax, got: {zip_text}"
        );
    }

    #[test]
    fn nullable_column_shows_option_type() {
        let mut schema = Schema::new();
        let mut table = Table::new("posts");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("title", PgType::Text));
        table.add_column(Column::new("body", PgType::Text).nullable());
        table.add_column(Column::new("metadata", PgType::Jsonb).nullable());
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("posts_pkey".into()),
            columns: vec!["id".into()],
        });
        schema.add_table(table);

        let state = AppState::new(schema, String::new(), None)
            .with_viewport_height(20)
            .toggle_expand()
            .toggle_rust_types();

        let lines = render_document(&state);

        // Non-nullable columns: plain type
        let id_text = spans_to_string(&lines[1]);
        assert!(
            id_text.contains("id:") && id_text.contains("uuid::Uuid"),
            "non-nullable should show plain type, got: {id_text}"
        );
        assert!(
            !id_text.contains("Option"),
            "non-nullable should not have Option, got: {id_text}"
        );

        let title_text = spans_to_string(&lines[2]);
        assert!(
            title_text.contains("title:") && title_text.contains("String"),
            "non-nullable should show plain type, got: {title_text}"
        );

        // Nullable columns: Option<T>
        let body_text = spans_to_string(&lines[3]);
        assert!(
            body_text.contains("body:") && body_text.contains("Option<String>"),
            "nullable should show Option<T>, got: {body_text}"
        );

        let meta_text = spans_to_string(&lines[4]);
        assert!(
            meta_text.contains("metadata:") && meta_text.contains("Option<serde_json::Value>"),
            "nullable should show Option<T>, got: {meta_text}"
        );
    }

    // --- Pending overlay annotation tests ---

    use crate::migration::overlay::{ChangeMarker, PendingOverlay};
    use crate::schema::diff::Change;

    /// Build a state with overlay data. BTreeMap order: posts < users.
    /// "posts" is in the AddTable change, "users" has AddColumn change.
    fn overlay_state() -> AppState {
        let mut schema = Schema::new();
        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("email", PgType::Text));
        schema.add_table(users);
        schema.add_table(Table::new("posts"));

        let overlay = PendingOverlay {
            changes: vec![
                Change::AddColumn {
                    table: "users".into(),
                    column: Column::new("bio", PgType::Text),
                },
                Change::AddTable({
                    let mut t = Table::new("posts");
                    t.add_column(Column::new("id", PgType::Uuid));
                    t
                }),
            ],
            pending_count: 1,
            unparseable: Vec::new(),
        };

        AppState::new(schema, String::new(), None)
            .with_viewport_height(20)
            .with_pending_overlay(Some(overlay))
            .toggle_pending_overlay()
    }

    #[test]
    fn overlay_marker_on_modified_table() {
        let state = overlay_state();
        let lines = render_document(&state);
        // BTreeMap order: posts, users. "users" is modified (AddColumn).
        let users_line = lines
            .iter()
            .map(|l| spans_to_string(l))
            .find(|s| s.contains("users"))
            .expect("should have users table line");
        assert!(
            users_line.starts_with("~ "),
            "modified table should have ~ prefix, got: {users_line}"
        );
    }

    #[test]
    fn overlay_marker_on_added_column() {
        // Expand "users" (second table in BTreeMap) to see columns.
        let mut state = overlay_state();
        // Navigate to users (Tab from posts)
        state = state.next_table().toggle_expand();
        let lines = render_document(&state);
        // "email" column in users should have no marker (not in overlay changes)
        let email_line = lines
            .iter()
            .map(|l| spans_to_string(l))
            .find(|s| s.contains("email"))
            .expect("should have email column line");
        assert!(
            !email_line.starts_with("+ ")
                && !email_line.starts_with("- ")
                && !email_line.starts_with("~ "),
            "unaffected column should have no marker, got: {email_line}"
        );
    }

    #[test]
    fn overlay_marker_on_added_table() {
        let state = overlay_state();
        let lines = render_document(&state);
        // "posts" is first in BTreeMap order, and is being AddTable'd
        let posts_line = spans_to_string(&lines[0]);
        assert!(
            posts_line.starts_with("+ "),
            "added table should have + prefix, got: {posts_line}"
        );
    }

    #[test]
    fn overlay_markers_hidden_when_not_active() {
        let state = overlay_state().toggle_pending_overlay(); // turn off
        assert!(!state.show_pending_overlay);
        let lines = render_document(&state);
        // First line is "posts" which would have "+" when overlay is on
        let header = spans_to_string(&lines[0]);
        assert!(
            !header.starts_with("~ ") && !header.starts_with("+ ") && !header.starts_with("- "),
            "no overlay markers when overlay is off, got: {header}"
        );
    }

    #[test]
    fn overlay_marker_style_colors() {
        assert_eq!(marker_style(ChangeMarker::Added).fg, Some(Color::Green));
        assert_eq!(marker_style(ChangeMarker::Removed).fg, Some(Color::Red));
        assert_eq!(marker_style(ChangeMarker::Modified).fg, Some(Color::Yellow));
    }

    // --- line_plain_text tests ---

    #[test]
    fn line_plain_text_table_collapsed() {
        let state = simple_state().with_viewport_height(10);
        let text = line_plain_text(&state, &FocusTarget::Table("users".into()));
        assert!(text.contains("table"), "should contain 'table' keyword");
        assert!(text.contains("users"), "should contain table name");
        assert!(
            text.contains("columns"),
            "collapsed table should mention columns"
        );
    }

    #[test]
    fn line_plain_text_table_expanded() {
        let state = simple_state().with_viewport_height(10).toggle_expand();
        let text = line_plain_text(&state, &FocusTarget::Table("users".into()));
        assert!(text.contains("table"), "should contain 'table' keyword");
        assert!(text.contains("users"), "should contain table name");
        assert!(
            !text.contains("columns"),
            "expanded table should not mention columns"
        );
    }

    #[test]
    fn line_plain_text_column() {
        let state = simple_state().with_viewport_height(10).toggle_expand();
        let text = line_plain_text(&state, &FocusTarget::Column("users".into(), "email".into()));
        assert!(text.contains("email"), "should contain column name");
        assert!(text.contains("text"), "should contain type name");
    }

    #[test]
    fn line_plain_text_enum() {
        let mut schema = Schema::new();
        schema.add_enum(EnumType {
            name: "mood".into(),
            variants: vec!["happy".into(), "sad".into()],
        });
        let state = AppState::new(schema, String::new(), None).with_viewport_height(10);
        let text = line_plain_text(&state, &FocusTarget::Enum("mood".into()));
        assert!(text.contains("enum"), "should contain 'enum' keyword");
        assert!(text.contains("mood"), "should contain enum name");
    }

    #[test]
    fn line_plain_text_blank() {
        let state = simple_state();
        let text = line_plain_text(&state, &FocusTarget::Blank);
        assert!(text.is_empty(), "blank line should have empty text");
    }

    // --- Search highlighting tests ---

    #[test]
    fn search_highlight_applied_to_matching_lines() {
        use crate::tui::app::{InDocSearchState, SearchDirection};

        let mut state = simple_state().with_viewport_height(10);
        // Doc: just "users" table header (collapsed), 1 line
        state.in_doc_search = Some(InDocSearchState {
            query: "users".into(),
            direction: SearchDirection::Forward,
            matches: vec![0],
            current: Some(0),
            origin_cursor: 0,
        });
        // Move cursor away so focus highlight doesn't override
        state.cursor = 999;
        let lines = render_document(&state);
        // Line 0 should have search highlight background
        assert!(
            lines[0]
                .spans
                .iter()
                .any(|s| s.style.bg == Some(SEARCH_CURRENT_BG)),
            "current match should have search highlight bg"
        );
    }
}
