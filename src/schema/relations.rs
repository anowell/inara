use std::collections::BTreeMap;

use super::types::{ForeignKeyRef, ReferentialAction};
use super::{Constraint, Schema};

/// Precomputed foreign key info for O(1) navigation lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyInfo {
    /// The constraint name, if any.
    pub name: Option<String>,
    /// The table this FK belongs to (source for outgoing, target for incoming).
    pub table: String,
    /// The FK columns on the source table.
    pub columns: Vec<String>,
    /// The referenced table and columns.
    pub references: ForeignKeyRef,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

/// Precomputed relation map for O(1) FK and index navigation.
///
/// Built once from a `Schema` and rebuilt when schema changes.
/// Navigation uses the relation map, never scans the full schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationMap {
    /// table_name -> Vec<ForeignKeyInfo> for FKs pointing *to* this table.
    pub incoming_fks: BTreeMap<String, Vec<ForeignKeyInfo>>,
    /// table_name -> Vec<ForeignKeyInfo> for FKs defined *on* this table.
    pub outgoing_fks: BTreeMap<String, Vec<ForeignKeyInfo>>,
    /// (table_name, column_name) -> Vec<index_name> for indexes covering this column.
    pub column_indexes: BTreeMap<(String, String), Vec<String>>,
}

impl RelationMap {
    /// Build a `RelationMap` from a `Schema`.
    ///
    /// Iterates all tables once, extracting foreign key constraints and indexes.
    pub fn build(schema: &Schema) -> Self {
        let mut incoming_fks: BTreeMap<String, Vec<ForeignKeyInfo>> = BTreeMap::new();
        let mut outgoing_fks: BTreeMap<String, Vec<ForeignKeyInfo>> = BTreeMap::new();
        let mut column_indexes: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();

        for table in schema.tables.values() {
            for constraint in &table.constraints {
                if let Constraint::ForeignKey {
                    name,
                    columns,
                    references,
                    on_delete,
                    on_update,
                } = constraint
                {
                    let info = ForeignKeyInfo {
                        name: name.clone(),
                        table: table.name.clone(),
                        columns: columns.clone(),
                        references: references.clone(),
                        on_delete: on_delete.clone(),
                        on_update: on_update.clone(),
                    };

                    outgoing_fks
                        .entry(table.name.clone())
                        .or_default()
                        .push(info.clone());

                    incoming_fks
                        .entry(references.table.clone())
                        .or_default()
                        .push(info);
                }
            }

            for index in &table.indexes {
                for col in &index.columns {
                    column_indexes
                        .entry((table.name.clone(), col.clone()))
                        .or_default()
                        .push(index.name.clone());
                }
            }
        }

        Self {
            incoming_fks,
            outgoing_fks,
            column_indexes,
        }
    }

    /// Get all foreign keys pointing to the given table.
    pub fn incoming(&self, table: &str) -> &[ForeignKeyInfo] {
        self.incoming_fks
            .get(table)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get all foreign keys defined on the given table.
    pub fn outgoing(&self, table: &str) -> &[ForeignKeyInfo] {
        self.outgoing_fks
            .get(table)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get all index names covering the given column.
    pub fn indexes_for_column(&self, table: &str, column: &str) -> &[String] {
        self.column_indexes
            .get(&(table.to_string(), column.to_string()))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{Expression, PgType};
    use crate::schema::{Column, Index, Table};

    fn users_table() -> Table {
        let mut table = Table::new("users");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("email", PgType::Text));
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });
        table
    }

    fn posts_table() -> Table {
        let mut table = Table::new("posts");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("author_id", PgType::Uuid));
        table.add_column(Column::new("title", PgType::Text));
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("posts_pkey".into()),
            columns: vec!["id".into()],
        });
        table.add_constraint(Constraint::ForeignKey {
            name: Some("posts_author_fk".into()),
            columns: vec!["author_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        table.add_index(Index {
            name: "posts_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: false,
            partial: None,
        });
        table
    }

    fn comments_table() -> Table {
        let mut table = Table::new("comments");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("post_id", PgType::Uuid));
        table.add_column(Column::new("author_id", PgType::Uuid));
        table.add_column(
            Column::new("body", PgType::Text).with_default(Expression::Literal("''".into())),
        );
        table.add_constraint(Constraint::PrimaryKey {
            name: Some("comments_pkey".into()),
            columns: vec!["id".into()],
        });
        table.add_constraint(Constraint::ForeignKey {
            name: Some("comments_post_fk".into()),
            columns: vec!["post_id".into()],
            references: ForeignKeyRef {
                table: "posts".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        table.add_constraint(Constraint::ForeignKey {
            name: Some("comments_author_fk".into()),
            columns: vec!["author_id".into()],
            references: ForeignKeyRef {
                table: "users".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::SetNull),
            on_update: None,
        });
        table.add_index(Index {
            name: "comments_post_idx".into(),
            columns: vec!["post_id".into()],
            unique: false,
            partial: None,
        });
        table.add_index(Index {
            name: "comments_author_idx".into(),
            columns: vec!["author_id".into()],
            unique: false,
            partial: None,
        });
        table
    }

    #[test]
    fn empty_schema_produces_empty_relation_map() {
        let schema = Schema::new();
        let map = RelationMap::build(&schema);
        assert!(map.incoming_fks.is_empty());
        assert!(map.outgoing_fks.is_empty());
        assert!(map.column_indexes.is_empty());
    }

    #[test]
    fn schema_without_fks_has_empty_fk_maps() {
        let mut schema = Schema::new();
        schema.add_table(users_table());
        let map = RelationMap::build(&schema);
        assert!(map.incoming_fks.is_empty());
        assert!(map.outgoing_fks.is_empty());
    }

    #[test]
    fn outgoing_fks_correct() {
        let mut schema = Schema::new();
        schema.add_table(users_table());
        schema.add_table(posts_table());
        let map = RelationMap::build(&schema);

        let outgoing = map.outgoing("posts");
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].name.as_deref(), Some("posts_author_fk"));
        assert_eq!(outgoing[0].table, "posts");
        assert_eq!(outgoing[0].columns, vec!["author_id"]);
        assert_eq!(outgoing[0].references.table, "users");
        assert_eq!(outgoing[0].references.columns, vec!["id"]);
        assert_eq!(outgoing[0].on_delete, Some(ReferentialAction::Cascade));
        assert_eq!(outgoing[0].on_update, None);
    }

    #[test]
    fn incoming_fks_correct() {
        let mut schema = Schema::new();
        schema.add_table(users_table());
        schema.add_table(posts_table());
        let map = RelationMap::build(&schema);

        let incoming = map.incoming("users");
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].name.as_deref(), Some("posts_author_fk"));
        assert_eq!(incoming[0].table, "posts");
    }

    #[test]
    fn no_incoming_for_leaf_table() {
        let mut schema = Schema::new();
        schema.add_table(users_table());
        schema.add_table(posts_table());
        let map = RelationMap::build(&schema);

        assert!(map.incoming("posts").is_empty());
    }

    #[test]
    fn no_outgoing_for_root_table() {
        let mut schema = Schema::new();
        schema.add_table(users_table());
        schema.add_table(posts_table());
        let map = RelationMap::build(&schema);

        assert!(map.outgoing("users").is_empty());
    }

    #[test]
    fn multiple_incoming_fks() {
        let mut schema = Schema::new();
        schema.add_table(users_table());
        schema.add_table(posts_table());
        schema.add_table(comments_table());
        let map = RelationMap::build(&schema);

        // users has incoming from both posts and comments
        let incoming = map.incoming("users");
        assert_eq!(incoming.len(), 2);
        let sources: Vec<&str> = incoming.iter().map(|fk| fk.table.as_str()).collect();
        assert!(sources.contains(&"posts"));
        assert!(sources.contains(&"comments"));
    }

    #[test]
    fn multiple_outgoing_fks() {
        let mut schema = Schema::new();
        schema.add_table(users_table());
        schema.add_table(posts_table());
        schema.add_table(comments_table());
        let map = RelationMap::build(&schema);

        // comments has outgoing to both posts and users
        let outgoing = map.outgoing("comments");
        assert_eq!(outgoing.len(), 2);
        let targets: Vec<&str> = outgoing
            .iter()
            .map(|fk| fk.references.table.as_str())
            .collect();
        assert!(targets.contains(&"posts"));
        assert!(targets.contains(&"users"));
    }

    #[test]
    fn column_indexes_correct() {
        let mut schema = Schema::new();
        schema.add_table(posts_table());
        let map = RelationMap::build(&schema);

        let idx_names = map.indexes_for_column("posts", "author_id");
        assert_eq!(idx_names, &["posts_author_idx"]);
    }

    #[test]
    fn column_without_index_returns_empty() {
        let mut schema = Schema::new();
        schema.add_table(posts_table());
        let map = RelationMap::build(&schema);

        assert!(map.indexes_for_column("posts", "title").is_empty());
    }

    #[test]
    fn nonexistent_table_returns_empty() {
        let schema = Schema::new();
        let map = RelationMap::build(&schema);

        assert!(map.incoming("nonexistent").is_empty());
        assert!(map.outgoing("nonexistent").is_empty());
        assert!(map.indexes_for_column("nonexistent", "id").is_empty());
    }

    #[test]
    fn multiple_indexes_on_same_column() {
        let mut table = Table::new("events");
        table.add_column(Column::new("created_at", PgType::Timestamptz));
        table.add_index(Index {
            name: "events_created_idx".into(),
            columns: vec!["created_at".into()],
            unique: false,
            partial: None,
        });
        table.add_index(Index {
            name: "events_recent_idx".into(),
            columns: vec!["created_at".into()],
            unique: false,
            partial: Some("WHERE created_at > now() - interval '7 days'".into()),
        });

        let mut schema = Schema::new();
        schema.add_table(table);
        let map = RelationMap::build(&schema);

        let idx_names = map.indexes_for_column("events", "created_at");
        assert_eq!(idx_names.len(), 2);
        assert!(idx_names.contains(&"events_created_idx".to_string()));
        assert!(idx_names.contains(&"events_recent_idx".to_string()));
    }

    #[test]
    fn composite_index_registers_all_columns() {
        let mut table = Table::new("events");
        table.add_column(Column::new("tenant_id", PgType::Uuid));
        table.add_column(Column::new("created_at", PgType::Timestamptz));
        table.add_index(Index {
            name: "events_tenant_created_idx".into(),
            columns: vec!["tenant_id".into(), "created_at".into()],
            unique: false,
            partial: None,
        });

        let mut schema = Schema::new();
        schema.add_table(table);
        let map = RelationMap::build(&schema);

        assert_eq!(
            map.indexes_for_column("events", "tenant_id"),
            &["events_tenant_created_idx"]
        );
        assert_eq!(
            map.indexes_for_column("events", "created_at"),
            &["events_tenant_created_idx"]
        );
    }

    #[test]
    fn circular_fk_references_build_without_panic() {
        // Table A references Table B, Table B references Table A
        let mut table_a = Table::new("table_a");
        table_a.add_column(Column::new("id", PgType::Uuid));
        table_a.add_column(Column::new("b_id", PgType::Uuid));
        table_a.add_constraint(Constraint::ForeignKey {
            name: Some("a_to_b_fk".into()),
            columns: vec!["b_id".into()],
            references: ForeignKeyRef {
                table: "table_b".into(),
                columns: vec!["id".into()],
            },
            on_delete: None,
            on_update: None,
        });

        let mut table_b = Table::new("table_b");
        table_b.add_column(Column::new("id", PgType::Uuid));
        table_b.add_column(Column::new("a_id", PgType::Uuid));
        table_b.add_constraint(Constraint::ForeignKey {
            name: Some("b_to_a_fk".into()),
            columns: vec!["a_id".into()],
            references: ForeignKeyRef {
                table: "table_a".into(),
                columns: vec!["id".into()],
            },
            on_delete: None,
            on_update: None,
        });

        let mut schema = Schema::new();
        schema.add_table(table_a);
        schema.add_table(table_b);

        // Should not panic
        let map = RelationMap::build(&schema);

        // table_a has outgoing to table_b, and incoming from table_b
        assert_eq!(map.outgoing("table_a").len(), 1);
        assert_eq!(map.incoming("table_a").len(), 1);
        assert_eq!(map.outgoing("table_a")[0].references.table, "table_b");
        assert_eq!(map.incoming("table_a")[0].table, "table_b");

        // table_b has outgoing to table_a, and incoming from table_a
        assert_eq!(map.outgoing("table_b").len(), 1);
        assert_eq!(map.incoming("table_b").len(), 1);
        assert_eq!(map.outgoing("table_b")[0].references.table, "table_a");
        assert_eq!(map.incoming("table_b")[0].table, "table_a");
    }

    #[test]
    fn self_referencing_fk() {
        let mut table = Table::new("employees");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("manager_id", PgType::Uuid));
        table.add_constraint(Constraint::ForeignKey {
            name: Some("employees_manager_fk".into()),
            columns: vec!["manager_id".into()],
            references: ForeignKeyRef {
                table: "employees".into(),
                columns: vec!["id".into()],
            },
            on_delete: Some(ReferentialAction::SetNull),
            on_update: None,
        });

        let mut schema = Schema::new();
        schema.add_table(table);

        let map = RelationMap::build(&schema);

        // Self-referencing: both incoming and outgoing on the same table
        assert_eq!(map.outgoing("employees").len(), 1);
        assert_eq!(map.incoming("employees").len(), 1);
        assert_eq!(map.outgoing("employees")[0].references.table, "employees");
        assert_eq!(map.incoming("employees")[0].table, "employees");
    }
}
