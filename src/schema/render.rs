use std::collections::BTreeSet;
use std::fmt::Write;

use super::{Column, Constraint, CustomType, CustomTypeKind, EnumType, Index, Schema, Table};

/// Render a complete schema to the declarative text format.
pub fn render_schema(schema: &Schema) -> String {
    let mut out = String::new();
    let mut first = true;

    for enum_type in schema.enums.values() {
        if !first {
            out.push('\n');
        }
        render_enum(&mut out, enum_type);
        first = false;
    }

    for custom_type in schema.types.values() {
        if !first {
            out.push('\n');
        }
        render_custom_type(&mut out, custom_type);
        first = false;
    }

    for table in schema.tables.values() {
        if !first {
            out.push('\n');
        }
        render_table(&mut out, table);
        first = false;
    }

    out
}

/// Render an enum type: `enum name { variant1, variant2, ... }`
fn render_enum(out: &mut String, enum_type: &EnumType) {
    let _ = write!(out, "enum {} {{", enum_type.name);
    if enum_type.variants.is_empty() {
        out.push_str(" }\n");
        return;
    }
    out.push('\n');
    for variant in &enum_type.variants {
        let _ = writeln!(out, "    {variant}");
    }
    out.push_str("}\n");
}

/// Render a custom type (domain, composite, range).
fn render_custom_type(out: &mut String, custom_type: &CustomType) {
    match &custom_type.kind {
        CustomTypeKind::Domain {
            base_type,
            constraints,
        } => {
            let _ = write!(out, "domain {} {base_type}", custom_type.name);
            for constraint in constraints {
                let _ = write!(out, " {constraint}");
            }
            out.push('\n');
        }
        CustomTypeKind::Composite { fields } => {
            let _ = write!(out, "composite {} {{", custom_type.name);
            if fields.is_empty() {
                out.push_str(" }\n");
                return;
            }
            out.push('\n');

            let max_name_len = fields.iter().map(|(n, _)| n.len()).max().unwrap_or(0);
            for (name, pg_type) in fields {
                let _ = writeln!(out, "    {name:<max_name_len$}  {pg_type}");
            }
            out.push_str("}\n");
        }
        CustomTypeKind::Range { subtype } => {
            let _ = writeln!(out, "range {} {subtype}", custom_type.name);
        }
    }
}

/// Render a single table to its declarative text format.
///
/// This produces the same output as `render_schema` would for a schema
/// containing only this table. Used by the edit mode to pre-fill the
/// text editor with a table's current declaration.
pub fn render_single_table(table: &Table) -> String {
    let mut out = String::new();
    render_table(&mut out, table);
    out
}

/// Render a table in the declarative format.
fn render_table(out: &mut String, table: &Table) {
    let _ = write!(out, "table {} {{", table.name);

    if table.columns.is_empty() && table.constraints.is_empty() && table.indexes.is_empty() {
        out.push_str(" }\n");
        return;
    }
    out.push('\n');

    // Build sets of single-column constraints to render inline.
    let single_pk_cols = single_column_pk_set(&table.constraints);
    let single_unique_cols = single_column_unique_set(&table.constraints);

    // Compute alignment widths.
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

    // Render columns.
    for col in &table.columns {
        render_column(
            out,
            col,
            max_name_len,
            max_type_len,
            &single_pk_cols,
            &single_unique_cols,
        );
    }

    // Render multi-column or named constraints as separate lines.
    let mut has_separator = false;
    for constraint in &table.constraints {
        if should_render_constraint_separately(constraint, &single_pk_cols, &single_unique_cols) {
            if !has_separator {
                out.push('\n');
                has_separator = true;
            }
            render_constraint(out, constraint);
        }
    }

    // Render indexes.
    if !table.indexes.is_empty() {
        if !has_separator {
            out.push('\n');
        }
        for index in &table.indexes {
            render_index(out, index);
        }
    }

    out.push_str("}\n");
}

/// Render a single column line with alignment and inline constraints.
fn render_column(
    out: &mut String,
    col: &Column,
    max_name_len: usize,
    max_type_len: usize,
    single_pk_cols: &BTreeSet<String>,
    single_unique_cols: &BTreeSet<String>,
) {
    let type_str = col.pg_type.to_string();

    // Build the suffix (everything after the type) to avoid trailing whitespace.
    let mut suffix = String::new();
    if !col.nullable {
        suffix.push_str("  NOT NULL");
    }
    if let Some(default) = &col.default {
        let _ = write!(suffix, "  DEFAULT {default}");
    }
    if single_pk_cols.contains(&col.name) {
        suffix.push_str("  PRIMARY KEY");
    }
    if single_unique_cols.contains(&col.name) {
        suffix.push_str("  UNIQUE");
    }

    if suffix.is_empty() {
        let _ = writeln!(out, "    {:<max_name_len$}  {type_str}", col.name);
    } else {
        let _ = writeln!(
            out,
            "    {:<max_name_len$}  {type_str:<max_type_len$}{suffix}",
            col.name
        );
    }
}

/// Determine if a constraint should be rendered as a separate line (not inline on a column).
fn should_render_constraint_separately(
    constraint: &Constraint,
    single_pk_cols: &BTreeSet<String>,
    single_unique_cols: &BTreeSet<String>,
) -> bool {
    match constraint {
        Constraint::PrimaryKey { columns, .. } => {
            // Render separately if multi-column or if not in inline set
            columns.len() > 1 || (columns.len() == 1 && !single_pk_cols.contains(&columns[0]))
        }
        Constraint::Unique { columns, .. } => {
            columns.len() > 1 || (columns.len() == 1 && !single_unique_cols.contains(&columns[0]))
        }
        // Foreign keys and checks always render separately.
        Constraint::ForeignKey { .. } | Constraint::Check { .. } => true,
    }
}

/// Render a constraint as a separate line within the table block.
fn render_constraint(out: &mut String, constraint: &Constraint) {
    match constraint {
        Constraint::PrimaryKey { columns, .. } => {
            let cols = columns.join(", ");
            let _ = writeln!(out, "    PRIMARY KEY ({cols})");
        }
        Constraint::Unique { columns, .. } => {
            let cols = columns.join(", ");
            let _ = writeln!(out, "    UNIQUE ({cols})");
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
            let _ = write!(
                out,
                "    FOREIGN KEY ({cols}) REFERENCES {}({ref_cols})",
                references.table
            );
            if let Some(action) = on_delete {
                let _ = write!(out, " ON DELETE {action}");
            }
            if let Some(action) = on_update {
                let _ = write!(out, " ON UPDATE {action}");
            }
            out.push('\n');
        }
        Constraint::Check { expression, .. } => {
            let _ = writeln!(out, "    CHECK ({expression})");
        }
    }
}

/// Render an index line within the table block.
fn render_index(out: &mut String, index: &Index) {
    let cols = index.columns.join(", ");
    if index.unique {
        let _ = write!(out, "    UNIQUE INDEX {}({cols})", index.name);
    } else {
        let _ = write!(out, "    INDEX {}({cols})", index.name);
    }
    if let Some(where_clause) = &index.partial {
        let _ = write!(out, " {where_clause}");
    }
    out.push('\n');
}

/// Collect single-column primary key column names for inline rendering.
///
/// Only the first single-column PK is considered (there should only be one PK per table).
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

/// Collect single-column unique constraint column names for inline rendering.
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

    // ── Fixtures ──────────────────────────────────────────────────

    fn simple_users_table() -> Schema {
        let mut schema = Schema::new();
        let mut table = Table::new("users");
        table.add_column(
            Column::new("id", PgType::Uuid)
                .with_default(Expression::FunctionCall("gen_random_uuid()".into())),
        );
        table.add_column(Column::new("email", PgType::Text));
        table.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        table.add_constraint(Constraint::Unique {
            name: Some("users_email_key".into()),
            columns: vec!["email".into()],
        });
        schema.add_table(table);
        schema
    }

    fn multi_table_with_fks() -> Schema {
        let mut schema = Schema::new();

        let mut users = Table::new("users");
        users.add_column(Column::new("id", PgType::Uuid));
        users.add_column(Column::new("name", PgType::Text));
        users.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        schema.add_table(users);

        let mut posts = Table::new("posts");
        posts.add_column(Column::new("id", PgType::Uuid));
        posts.add_column(Column::new("author_id", PgType::Uuid));
        posts.add_column(Column::new("title", PgType::Text));
        posts.add_column(Column::new("body", PgType::Text).nullable());
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

        schema
    }

    fn all_constraint_types() -> Schema {
        let mut schema = Schema::new();

        let mut table = Table::new("orders");
        table.add_column(Column::new("id", PgType::BigInt));
        table.add_column(Column::new("tenant_id", PgType::Uuid));
        table.add_column(Column::new("user_id", PgType::Uuid));
        table.add_column(Column::new("amount", PgType::Numeric(Some((10, 2)))));
        table.add_column(Column::new("status", PgType::Custom("order_status".into())));

        // Composite PK
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("orders_pkey".into()),
            columns: vec!["tenant_id".into(), "id".into()],
        });
        // Single-column unique
        table.add_constraint(Constraint::Unique {
            name: Some("orders_id_key".into()),
            columns: vec!["id".into()],
        });
        // Multi-column unique
        table.add_constraint(Constraint::Unique {
            name: Some("orders_tenant_user_key".into()),
            columns: vec!["tenant_id".into(), "user_id".into()],
        });
        // FK with both actions
        table.add_constraint(Constraint::ForeignKey {
            name: Some("orders_user_fk".into()),
            columns: vec!["user_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::SetNull),
            on_update: Some(ReferentialAction::Cascade),
        });
        // Check constraint
        table.add_constraint(Constraint::Check {
            name: Some("orders_amount_positive".into()),
            expression: "amount > 0".into(),
        });

        // Indexes
        table.add_index(Index {
            name: "orders_user_idx".into(),
            columns: vec!["user_id".into()],
            unique: false,
            partial: None,
        });
        table.add_index(Index {
            name: "orders_status_idx".into(),
            columns: vec!["status".into()],
            unique: false,
            partial: Some("WHERE status != 'completed'".into()),
        });

        schema.add_table(table);
        schema
    }

    fn schema_with_enums() -> Schema {
        let mut schema = Schema::new();

        schema.add_enum(EnumType {
            name: "mood".into(),
            variants: vec!["happy".into(), "sad".into(), "neutral".into()],
        });
        schema.add_enum(EnumType {
            name: "status".into(),
            variants: vec!["active".into(), "inactive".into(), "pending".into()],
        });

        let mut table = Table::new("profiles");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("mood", PgType::Custom("mood".into())).nullable());
        table.add_column(Column::new("status", PgType::Custom("status".into())));
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("profiles_pkey".into()),
            columns: vec!["id".into()],
        });
        schema.add_table(table);

        schema
    }

    fn complex_schema() -> Schema {
        let mut schema = Schema::new();

        schema.add_enum(EnumType {
            name: "role".into(),
            variants: vec!["admin".into(), "member".into(), "guest".into()],
        });

        schema.add_type(CustomType {
            name: "email_address".into(),
            kind: CustomTypeKind::Domain {
                base_type: PgType::Text,
                constraints: vec!["CHECK (VALUE ~ '^.+@.+$')".into()],
            },
        });

        // users table
        let mut users = Table::new("users");
        users.add_column(
            Column::new("id", PgType::Uuid)
                .with_default(Expression::FunctionCall("gen_random_uuid()".into())),
        );
        users.add_column(Column::new("email", PgType::Custom("email_address".into())));
        users.add_column(Column::new("name", PgType::Varchar(Some(255))));
        users.add_column(Column::new("role", PgType::Custom("role".into())));
        users.add_column(Column::new("tags", PgType::Array(Box::new(PgType::Text))).nullable());
        users.add_column(Column::new("metadata", PgType::Jsonb).nullable());
        users.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
        users.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        users.add_constraint(Constraint::Unique {
            name: Some("users_email_key".into()),
            columns: vec!["email".into()],
        });
        users.add_index(Index {
            name: "users_email_idx".into(),
            columns: vec!["email".into()],
            unique: true,
            partial: None,
        });
        schema.add_table(users);

        // teams table
        let mut teams = Table::new("teams");
        teams.add_column(Column::new("id", PgType::Uuid));
        teams.add_column(Column::new("name", PgType::Text));
        teams.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
        teams.add_constraint(Constraint::PrimaryKey {
            name: Some("teams_pkey".into()),
            columns: vec!["id".into()],
        });
        schema.add_table(teams);

        // team_members join table
        let mut members = Table::new("team_members");
        members.add_column(Column::new("team_id", PgType::Uuid));
        members.add_column(Column::new("user_id", PgType::Uuid));
        members.add_column(
            Column::new("joined_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
        members.add_constraint(Constraint::PrimaryKey {
            name: Some("team_members_pkey".into()),
            columns: vec!["team_id".into(), "user_id".into()],
        });
        members.add_constraint(Constraint::ForeignKey {
            name: Some("team_members_team_fk".into()),
            columns: vec!["team_id".into()],
            references: ForeignKeyRef {
                table: "teams".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        members.add_constraint(Constraint::ForeignKey {
            name: Some("team_members_user_fk".into()),
            columns: vec!["user_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        members.add_index(Index {
            name: "team_members_user_idx".into(),
            columns: vec!["user_id".into()],
            unique: false,
            partial: None,
        });
        schema.add_table(members);

        schema
    }

    // ── Snapshot tests ────────────────────────────────────────────

    #[test]
    fn snapshot_simple_table() {
        let schema = simple_users_table();
        let rendered = render_schema(&schema);
        insta::assert_snapshot!(rendered);
    }

    #[test]
    fn snapshot_multi_table_with_fks() {
        let schema = multi_table_with_fks();
        let rendered = render_schema(&schema);
        insta::assert_snapshot!(rendered);
    }

    #[test]
    fn snapshot_all_constraint_types() {
        let schema = all_constraint_types();
        let rendered = render_schema(&schema);
        insta::assert_snapshot!(rendered);
    }

    #[test]
    fn snapshot_schema_with_enums() {
        let schema = schema_with_enums();
        let rendered = render_schema(&schema);
        insta::assert_snapshot!(rendered);
    }

    #[test]
    fn snapshot_complex_schema() {
        let schema = complex_schema();
        let rendered = render_schema(&schema);
        insta::assert_snapshot!(rendered);
    }

    // ── Unit tests ────────────────────────────────────────────────

    #[test]
    fn empty_schema_renders_empty() {
        let schema = Schema::new();
        let rendered = render_schema(&schema);
        assert_eq!(rendered, "");
    }

    #[test]
    fn empty_table_renders_correctly() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("empty"));
        let rendered = render_schema(&schema);
        assert_eq!(rendered, "table empty { }\n");
    }

    #[test]
    fn enum_renders_correctly() {
        let mut schema = Schema::new();
        schema.add_enum(EnumType {
            name: "color".into(),
            variants: vec!["red".into(), "green".into(), "blue".into()],
        });
        let rendered = render_schema(&schema);
        let expected = "enum color {\n    red\n    green\n    blue\n}\n";
        assert_eq!(rendered, expected);
    }

    #[test]
    fn empty_enum_renders_correctly() {
        let mut schema = Schema::new();
        schema.add_enum(EnumType {
            name: "empty".into(),
            variants: vec![],
        });
        let rendered = render_schema(&schema);
        assert_eq!(rendered, "enum empty { }\n");
    }

    #[test]
    fn rendering_is_deterministic() {
        let schema = complex_schema();
        let a = render_schema(&schema);
        let b = render_schema(&schema);
        assert_eq!(a, b);
    }

    #[test]
    fn custom_type_domain_renders() {
        let mut schema = Schema::new();
        schema.add_type(CustomType {
            name: "positive_int".into(),
            kind: CustomTypeKind::Domain {
                base_type: PgType::Integer,
                constraints: vec!["CHECK (VALUE > 0)".into()],
            },
        });
        let rendered = render_schema(&schema);
        assert_eq!(rendered, "domain positive_int integer CHECK (VALUE > 0)\n");
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
                    ("zip".into(), PgType::Varchar(Some(10))),
                ],
            },
        });
        let rendered = render_schema(&schema);
        insta::assert_snapshot!(rendered);
    }

    #[test]
    fn custom_type_range_renders() {
        let mut schema = Schema::new();
        schema.add_type(CustomType {
            name: "float_range".into(),
            kind: CustomTypeKind::Range {
                subtype: PgType::DoublePrecision,
            },
        });
        let rendered = render_schema(&schema);
        assert_eq!(rendered, "range float_range double precision\n");
    }

    #[test]
    fn index_with_unique_and_partial() {
        let mut schema = Schema::new();
        let mut table = Table::new("events");
        table.add_column(Column::new("id", PgType::BigInt));
        table.add_column(Column::new("active", PgType::Boolean));
        table.add_index(Index {
            name: "events_active_idx".into(),
            columns: vec!["active".into()],
            unique: true,
            partial: Some("WHERE active = true".into()),
        });
        schema.add_table(table);
        let rendered = render_schema(&schema);
        assert!(rendered.contains("UNIQUE INDEX events_active_idx(active) WHERE active = true"));
    }

    #[test]
    fn fk_with_both_actions() {
        let mut schema = Schema::new();
        let mut table = Table::new("child");
        table.add_column(Column::new("id", PgType::Integer));
        table.add_column(Column::new("parent_id", PgType::Integer));
        table.add_constraint(Constraint::ForeignKey {
            name: Some("child_parent_fk".into()),
            columns: vec!["parent_id".into()],
            references: ForeignKeyRef {
                table: "parent".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: Some(ReferentialAction::SetNull),
        });
        schema.add_table(table);
        let rendered = render_schema(&schema);
        assert!(rendered.contains(
            "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL"
        ));
    }
}
