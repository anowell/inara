use crate::schema::relations::RelationMap;
use crate::schema::types::PgType;
use crate::schema::Schema;

use super::app::FocusTarget;

/// A goto navigation result. When a goto action finds targets, it returns
/// either a single jump target or multiple candidates for the picker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GotoResult {
    /// Jump directly to a single target.
    Jump(GotoTarget),
    /// Multiple candidates — open picker to choose.
    Pick(Vec<GotoTarget>),
    /// No results found.
    NoResults(&'static str),
    /// Feature not yet available.
    NotAvailable(&'static str),
}

/// A single goto navigation target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GotoTarget {
    /// Display label for the picker.
    pub label: String,
    /// The focus target to jump to.
    pub focus: GotoFocus,
}

/// Where a goto action should land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GotoFocus {
    /// Jump to a table header.
    Table(String),
    /// Jump to a specific column.
    Column(String, String),
    /// Jump to an enum definition.
    Enum(String),
    /// Jump to a custom type definition.
    Type(String),
}

/// Dispatch a goto key based on the current focus context.
///
/// Returns a `GotoResult` describing what should happen.
pub fn dispatch(
    key: char,
    focus: &FocusTarget,
    schema: &Schema,
    relations: &RelationMap,
) -> GotoResult {
    match focus {
        FocusTarget::Table(table) => dispatch_table(key, table, schema, relations),
        FocusTarget::Column(table, column) => {
            dispatch_column(key, table, column, schema, relations)
        }
        // For other table-related targets, delegate to table context
        FocusTarget::Separator(table)
        | FocusTarget::Constraint(table, _)
        | FocusTarget::Index(table, _)
        | FocusTarget::TableClose(table) => dispatch_table(key, table, schema, relations),
        _ => GotoResult::NoResults("goto not available here"),
    }
}

/// Dispatch goto for table focus context.
fn dispatch_table(key: char, table: &str, schema: &Schema, relations: &RelationMap) -> GotoResult {
    match key {
        'r' => goto_incoming_fks_table(table, relations),
        'o' => goto_outgoing_fks_table(table, relations),
        'i' => goto_indexes_table(table, schema),
        'c' => goto_first_column(table, schema),
        't' => goto_types_used(table, schema),
        'm' => GotoResult::NotAvailable("migrations not yet available"),
        _ => GotoResult::NoResults("unknown goto"),
    }
}

/// Dispatch goto for column focus context.
fn dispatch_column(
    key: char,
    table: &str,
    column: &str,
    schema: &Schema,
    relations: &RelationMap,
) -> GotoResult {
    match key {
        'r' => goto_incoming_fks_column(table, column, relations),
        'd' => goto_fk_target(table, column, schema),
        't' => GotoResult::Jump(GotoTarget {
            label: table.to_string(),
            focus: GotoFocus::Table(table.to_string()),
        }),
        'i' => goto_indexes_column(table, column, schema, relations),
        'y' => goto_type_definition(table, column, schema),
        'm' => GotoResult::NotAvailable("migrations not yet available"),
        _ => GotoResult::NoResults("unknown goto"),
    }
}

/// Table context: incoming FK references (tables that reference this one).
fn goto_incoming_fks_table(table: &str, relations: &RelationMap) -> GotoResult {
    let incoming = relations.incoming(table);
    if incoming.is_empty() {
        return GotoResult::NoResults("no incoming references");
    }

    let targets: Vec<GotoTarget> = incoming
        .iter()
        .map(|fk| {
            let cols = fk.columns.join(", ");
            GotoTarget {
                label: format!("{}.{}", fk.table, cols),
                focus: GotoFocus::Table(fk.table.clone()),
            }
        })
        .collect();

    if targets.len() == 1 {
        GotoResult::Jump(targets.into_iter().next().expect("checked non-empty"))
    } else {
        GotoResult::Pick(targets)
    }
}

/// Table context: outgoing FK references (tables this one references).
fn goto_outgoing_fks_table(table: &str, relations: &RelationMap) -> GotoResult {
    let outgoing = relations.outgoing(table);
    if outgoing.is_empty() {
        return GotoResult::NoResults("no outgoing references");
    }

    let targets: Vec<GotoTarget> = outgoing
        .iter()
        .map(|fk| {
            let ref_cols = fk.references.columns.join(", ");
            GotoTarget {
                label: format!("{}.{}", fk.references.table, ref_cols),
                focus: GotoFocus::Table(fk.references.table.clone()),
            }
        })
        .collect();

    if targets.len() == 1 {
        GotoResult::Jump(targets.into_iter().next().expect("checked non-empty"))
    } else {
        GotoResult::Pick(targets)
    }
}

/// Table context: indexes on this table.
fn goto_indexes_table(table: &str, schema: &Schema) -> GotoResult {
    let tbl = match schema.table(table) {
        Some(t) => t,
        None => return GotoResult::NoResults("table not found"),
    };

    if tbl.indexes.is_empty() {
        return GotoResult::NoResults("no indexes on this table");
    }

    // Jump to the first column of the first index
    // For single index, jump to first indexed column; for multiple, show picker
    let targets: Vec<GotoTarget> = tbl
        .indexes
        .iter()
        .map(|idx| {
            let cols = idx.columns.join(", ");
            let first_col = idx.columns.first().cloned().unwrap_or_default();
            GotoTarget {
                label: format!("{} ({})", idx.name, cols),
                focus: GotoFocus::Column(table.to_string(), first_col),
            }
        })
        .collect();

    if targets.len() == 1 {
        GotoResult::Jump(targets.into_iter().next().expect("checked non-empty"))
    } else {
        GotoResult::Pick(targets)
    }
}

/// Table context: jump to first column.
fn goto_first_column(table: &str, schema: &Schema) -> GotoResult {
    let tbl = match schema.table(table) {
        Some(t) => t,
        None => return GotoResult::NoResults("table not found"),
    };

    match tbl.columns.first() {
        Some(col) => GotoResult::Jump(GotoTarget {
            label: col.name.clone(),
            focus: GotoFocus::Column(table.to_string(), col.name.clone()),
        }),
        None => GotoResult::NoResults("table has no columns"),
    }
}

/// Table context: types (enums/custom) used by columns in this table.
fn goto_types_used(table: &str, schema: &Schema) -> GotoResult {
    let tbl = match schema.table(table) {
        Some(t) => t,
        None => return GotoResult::NoResults("table not found"),
    };

    let mut targets = Vec::new();
    for col in &tbl.columns {
        collect_custom_type_targets(&col.pg_type, &col.name, schema, &mut targets);
    }

    // Deduplicate by focus target
    targets.dedup_by(|a, b| a.focus == b.focus);

    match targets.len() {
        0 => GotoResult::NoResults("no custom types used"),
        1 => GotoResult::Jump(targets.into_iter().next().expect("checked non-empty")),
        _ => GotoResult::Pick(targets),
    }
}

/// Column context: incoming FK references to this specific column.
fn goto_incoming_fks_column(table: &str, column: &str, relations: &RelationMap) -> GotoResult {
    let incoming = relations.incoming(table);
    // Filter to FKs that reference this specific column
    let matching: Vec<&crate::schema::relations::ForeignKeyInfo> = incoming
        .iter()
        .filter(|fk| fk.references.columns.contains(&column.to_string()))
        .collect();

    if matching.is_empty() {
        return GotoResult::NoResults("no incoming references to this column");
    }

    let targets: Vec<GotoTarget> = matching
        .iter()
        .map(|fk| {
            let cols = fk.columns.join(", ");
            // Jump to the FK source column
            let source_col = fk.columns.first().cloned().unwrap_or_default();
            GotoTarget {
                label: format!("{}.{}", fk.table, cols),
                focus: GotoFocus::Column(fk.table.clone(), source_col),
            }
        })
        .collect();

    if targets.len() == 1 {
        GotoResult::Jump(targets.into_iter().next().expect("checked non-empty"))
    } else {
        GotoResult::Pick(targets)
    }
}

/// Column context: jump to FK target definition (the table/column this FK points to).
fn goto_fk_target(table: &str, column: &str, schema: &Schema) -> GotoResult {
    let tbl = match schema.table(table) {
        Some(t) => t,
        None => return GotoResult::NoResults("table not found"),
    };

    // Find FK constraints that include this column
    let fk_targets: Vec<GotoTarget> = tbl
        .constraints
        .iter()
        .filter_map(|c| {
            if let crate::schema::Constraint::ForeignKey {
                columns,
                references,
                ..
            } = c
            {
                if columns.contains(&column.to_string()) {
                    let ref_col = references.columns.first().cloned().unwrap_or_default();
                    Some(GotoTarget {
                        label: format!("{}.{}", references.table, ref_col),
                        focus: GotoFocus::Column(references.table.clone(), ref_col),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    match fk_targets.len() {
        0 => GotoResult::NoResults("column is not a foreign key"),
        1 => GotoResult::Jump(fk_targets.into_iter().next().expect("checked non-empty")),
        _ => GotoResult::Pick(fk_targets),
    }
}

/// Column context: indexes containing this column.
fn goto_indexes_column(
    table: &str,
    column: &str,
    schema: &Schema,
    relations: &RelationMap,
) -> GotoResult {
    let idx_names = relations.indexes_for_column(table, column);
    if idx_names.is_empty() {
        return GotoResult::NoResults("no indexes on this column");
    }

    let tbl = match schema.table(table) {
        Some(t) => t,
        None => return GotoResult::NoResults("table not found"),
    };

    let targets: Vec<GotoTarget> = idx_names
        .iter()
        .filter_map(|name| {
            tbl.indexes.iter().find(|idx| &idx.name == name).map(|idx| {
                let cols = idx.columns.join(", ");
                GotoTarget {
                    label: format!("{} ({})", idx.name, cols),
                    // Stay on the column — index lines aren't always visible
                    focus: GotoFocus::Column(table.to_string(), column.to_string()),
                }
            })
        })
        .collect();

    match targets.len() {
        0 => GotoResult::NoResults("no indexes on this column"),
        1 => GotoResult::Jump(targets.into_iter().next().expect("checked non-empty")),
        _ => GotoResult::Pick(targets),
    }
}

/// Column context: jump to enum/custom type definition.
fn goto_type_definition(table: &str, column: &str, schema: &Schema) -> GotoResult {
    let tbl = match schema.table(table) {
        Some(t) => t,
        None => return GotoResult::NoResults("table not found"),
    };

    let col = match tbl.column(column) {
        Some(c) => c,
        None => return GotoResult::NoResults("column not found"),
    };

    let mut targets = Vec::new();
    collect_custom_type_targets(&col.pg_type, &col.name, schema, &mut targets);

    match targets.len() {
        0 => GotoResult::NoResults("column type is not a custom type"),
        1 => GotoResult::Jump(targets.into_iter().next().expect("checked non-empty")),
        _ => GotoResult::Pick(targets),
    }
}

/// Collect goto targets for custom types found in a PgType (including inside arrays).
fn collect_custom_type_targets(
    pg_type: &PgType,
    col_name: &str,
    schema: &Schema,
    targets: &mut Vec<GotoTarget>,
) {
    match pg_type {
        PgType::Custom(name) => {
            if schema.enums.contains_key(name) {
                targets.push(GotoTarget {
                    label: format!("{col_name} → enum {name}"),
                    focus: GotoFocus::Enum(name.clone()),
                });
            } else if schema.types.contains_key(name) {
                targets.push(GotoTarget {
                    label: format!("{col_name} → type {name}"),
                    focus: GotoFocus::Type(name.clone()),
                });
            }
        }
        PgType::Array(inner) => {
            collect_custom_type_targets(inner, col_name, schema, targets);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{ForeignKeyRef, ReferentialAction};
    use crate::schema::{Column, Constraint, EnumType, Index, Table};

    fn test_schema() -> Schema {
        let mut schema = Schema::new();

        // users table
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

        // posts table with FK to users
        let mut posts = Table::new("posts");
        posts.add_column(Column::new("id", PgType::Uuid));
        posts.add_column(Column::new("author_id", PgType::Uuid));
        posts.add_column(Column::new("title", PgType::Text));
        posts.add_column(Column::new("status", PgType::Custom("post_status".into())));
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

        // comments table with FKs to both users and posts
        let mut comments = Table::new("comments");
        comments.add_column(Column::new("id", PgType::Uuid));
        comments.add_column(Column::new("post_id", PgType::Uuid));
        comments.add_column(Column::new("author_id", PgType::Uuid));
        comments.add_column(Column::new("body", PgType::Text));
        comments.add_constraint(Constraint::ForeignKey {
            name: Some("comments_post_fk".into()),
            columns: vec!["post_id".into()],
            references: ForeignKeyRef {
                table: "posts".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        comments.add_constraint(Constraint::ForeignKey {
            name: Some("comments_author_fk".into()),
            columns: vec!["author_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::SetNull),
            on_update: None,
        });
        comments.add_index(Index {
            name: "comments_post_idx".into(),
            columns: vec!["post_id".into()],
            unique: false,
            partial: None,
        });
        schema.add_table(comments);

        // enum type
        schema.add_enum(EnumType {
            name: "user_role".into(),
            variants: vec!["admin".into(), "member".into()],
        });

        // enum type for posts
        schema.add_enum(EnumType {
            name: "post_status".into(),
            variants: vec!["draft".into(), "published".into()],
        });

        schema
    }

    // --- Table context: incoming FKs (g r) ---

    #[test]
    fn table_goto_incoming_refs() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("users".into());

        let result = dispatch('r', &focus, &schema, &relations);
        // users has incoming from posts and comments
        match result {
            GotoResult::Pick(targets) => {
                assert_eq!(targets.len(), 2);
                let tables: Vec<&str> = targets
                    .iter()
                    .map(|t| match &t.focus {
                        GotoFocus::Table(name) => name.as_str(),
                        _ => panic!("expected table focus"),
                    })
                    .collect();
                assert!(tables.contains(&"posts"));
                assert!(tables.contains(&"comments"));
            }
            other => panic!("expected Pick, got {other:?}"),
        }
    }

    #[test]
    fn table_goto_incoming_refs_single() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("posts".into());

        let result = dispatch('r', &focus, &schema, &relations);
        // posts has incoming only from comments
        match result {
            GotoResult::Jump(target) => {
                assert_eq!(target.focus, GotoFocus::Table("comments".into()));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }

    #[test]
    fn table_goto_incoming_refs_none() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("comments".into());

        let result = dispatch('r', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Table context: outgoing FKs (g o) ---

    #[test]
    fn table_goto_outgoing_refs() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("comments".into());

        let result = dispatch('o', &focus, &schema, &relations);
        // comments has outgoing to posts and users
        match result {
            GotoResult::Pick(targets) => {
                assert_eq!(targets.len(), 2);
            }
            other => panic!("expected Pick, got {other:?}"),
        }
    }

    #[test]
    fn table_goto_outgoing_refs_none() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("users".into());

        let result = dispatch('o', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Table context: indexes (g i) ---

    #[test]
    fn table_goto_indexes() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("users".into());

        let result = dispatch('i', &focus, &schema, &relations);
        match result {
            GotoResult::Jump(target) => {
                assert!(target.label.contains("users_email_idx"));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }

    #[test]
    fn table_goto_indexes_none() {
        // Create a table without indexes
        let mut schema = Schema::new();
        schema.add_table(Table::new("empty"));
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("empty".into());

        let result = dispatch('i', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Table context: first column (g c) ---

    #[test]
    fn table_goto_first_column() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("users".into());

        let result = dispatch('c', &focus, &schema, &relations);
        match result {
            GotoResult::Jump(target) => {
                assert_eq!(target.focus, GotoFocus::Column("users".into(), "id".into()));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }

    // --- Table context: types used (g t) ---

    #[test]
    fn table_goto_types_used() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("users".into());

        let result = dispatch('t', &focus, &schema, &relations);
        match result {
            GotoResult::Jump(target) => {
                assert_eq!(target.focus, GotoFocus::Enum("user_role".into()));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }

    #[test]
    fn table_goto_types_none() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("comments".into());

        let result = dispatch('t', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Table context: migrations (g m) ---

    #[test]
    fn table_goto_migrations_not_available() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("users".into());

        let result = dispatch('m', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NotAvailable(_)));
    }

    // --- Column context: parent table (g t) ---

    #[test]
    fn column_goto_parent_table() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Column("posts".into(), "author_id".into());

        let result = dispatch('t', &focus, &schema, &relations);
        match result {
            GotoResult::Jump(target) => {
                assert_eq!(target.focus, GotoFocus::Table("posts".into()));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }

    // --- Column context: FK target (g d) ---

    #[test]
    fn column_goto_fk_target() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Column("posts".into(), "author_id".into());

        let result = dispatch('d', &focus, &schema, &relations);
        match result {
            GotoResult::Jump(target) => {
                assert_eq!(target.focus, GotoFocus::Column("users".into(), "id".into()));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }

    #[test]
    fn column_goto_fk_target_not_fk() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Column("users".into(), "email".into());

        let result = dispatch('d', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Column context: incoming refs (g r) ---

    #[test]
    fn column_goto_incoming_refs() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Column("users".into(), "id".into());

        let result = dispatch('r', &focus, &schema, &relations);
        // users.id is referenced by posts.author_id and comments.author_id
        match result {
            GotoResult::Pick(targets) => {
                assert_eq!(targets.len(), 2);
            }
            other => panic!("expected Pick, got {other:?}"),
        }
    }

    // --- Column context: indexes (g i) ---

    #[test]
    fn column_goto_indexes() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Column("posts".into(), "author_id".into());

        let result = dispatch('i', &focus, &schema, &relations);
        match result {
            GotoResult::Jump(target) => {
                assert!(target.label.contains("posts_author_idx"));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }

    #[test]
    fn column_goto_indexes_none() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Column("posts".into(), "title".into());

        let result = dispatch('i', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Column context: type definition (g y) ---

    #[test]
    fn column_goto_type_definition_enum() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Column("users".into(), "role".into());

        let result = dispatch('y', &focus, &schema, &relations);
        match result {
            GotoResult::Jump(target) => {
                assert_eq!(target.focus, GotoFocus::Enum("user_role".into()));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }

    #[test]
    fn column_goto_type_definition_not_custom() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Column("users".into(), "email".into());

        let result = dispatch('y', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Unknown keys ---

    #[test]
    fn unknown_key_returns_no_results() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Table("users".into());

        let result = dispatch('x', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Non-table focus ---

    #[test]
    fn blank_focus_returns_no_results() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Blank;

        let result = dispatch('r', &focus, &schema, &relations);
        assert!(matches!(result, GotoResult::NoResults(_)));
    }

    // --- Constraint/Index focus delegates to table ---

    #[test]
    fn constraint_focus_delegates_to_table() {
        let schema = test_schema();
        let relations = RelationMap::build(&schema);
        let focus = FocusTarget::Constraint("posts".into(), 0);

        let result = dispatch('r', &focus, &schema, &relations);
        // Should act as if table "posts" is focused
        match result {
            GotoResult::Jump(target) => {
                assert_eq!(target.focus, GotoFocus::Table("comments".into()));
            }
            other => panic!("expected Jump, got {other:?}"),
        }
    }
}
