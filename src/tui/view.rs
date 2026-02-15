use std::collections::BTreeSet;

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

use super::app::{AppState, FocusTarget};
use crate::schema::{Constraint, CustomTypeKind};

/// Style constants for syntax highlighting.
const KEYWORD_STYLE: Style = Style::new().fg(Color::Yellow);
const TYPE_STYLE: Style = Style::new().fg(Color::Cyan);
const NAME_STYLE: Style = Style::new().fg(Color::White);
const DIM_STYLE: Style = Style::new().fg(Color::DarkGray);
const NORMAL_STYLE: Style = Style::new().fg(Color::White);

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

    state.doc[state.viewport_offset..viewport_end]
        .iter()
        .enumerate()
        .map(|(i, doc_line)| {
            let line_index = state.viewport_offset + i;
            let is_focused = line_index == state.cursor;
            render_line(state, &doc_line.target, is_focused)
        })
        .collect()
}

/// Render a single document line based on its FocusTarget.
fn render_line(state: &AppState, target: &FocusTarget, is_focused: bool) -> Line<'static> {
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

    if is_focused {
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

    let mut spans = vec![
        Span::styled("enum ", KEYWORD_STYLE),
        Span::styled(name.to_string(), NAME_STYLE),
        Span::styled(" {", NORMAL_STYLE),
    ];
    if is_empty {
        spans.push(Span::styled(" }", NORMAL_STYLE));
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
            let mut spans = vec![
                Span::styled("composite ", KEYWORD_STYLE),
                Span::styled(name.to_string(), NAME_STYLE),
                Span::styled(" {", NORMAL_STYLE),
            ];
            if fields.is_empty() {
                spans.push(Span::styled(" }", NORMAL_STYLE));
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
            return vec![
                Span::styled(format!("    {field_name:<max_name_len$}  "), NAME_STYLE),
                Span::styled(pg_type.to_string(), TYPE_STYLE),
            ];
        }
    }
    vec![Span::raw("")]
}

/// Style for the edit indicator on modified tables.
const EDITED_STYLE: Style = Style::new().fg(Color::Yellow);

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
    let is_edited = state.edited_tables.contains(name);

    let mut spans = Vec::new();

    if is_edited {
        spans.push(Span::styled("~ ", EDITED_STYLE));
    }

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
    let max_type_len = table
        .columns
        .iter()
        .map(|c| c.pg_type.to_string().len())
        .max()
        .unwrap_or(0);

    let type_str = col.pg_type.to_string();

    // Build suffix parts
    let has_suffix = !col.nullable
        || col.default.is_some()
        || single_pk_cols.contains(&col.name)
        || single_unique_cols.contains(&col.name);

    let padded_type = if has_suffix {
        format!("{type_str:<max_type_len$}")
    } else {
        type_str.clone()
    };

    let mut spans = vec![
        Span::styled(format!("    {:<max_name_len$}  ", col.name), NAME_STYLE),
        Span::styled(padded_type, TYPE_STYLE),
    ];

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
        AppState::new(schema, String::new())
    }

    #[test]
    fn empty_schema_shows_placeholder() {
        let state = AppState::new(Schema::new(), String::new()).with_viewport_height(10);
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
        let state = AppState::new(schema, String::new()).with_viewport_height(20);
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

        let mut state = AppState::new(schema, String::new()).with_viewport_height(30);
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
        let state = AppState::new(schema, String::new()).with_viewport_height(10);
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
        let state = AppState::new(schema, String::new()).with_viewport_height(10);
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
        // 5 tables + 4 blanks = 9 lines, viewport only shows 3
        let state = AppState::new(schema, String::new()).with_viewport_height(3);
        let lines = render_document(&state);
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn empty_table_expanded_renders_single_line() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("empty"));
        let state = AppState::new(schema, String::new())
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
}
