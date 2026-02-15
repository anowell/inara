pub mod diff;
pub mod relations;
pub mod render;
pub mod types;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use types::{Expression, ForeignKeyRef, PgType, ReferentialAction};

/// Top-level schema model. The single source of truth for all features.
///
/// Uses `BTreeMap` for deterministic iteration order (alphabetical by name).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub tables: BTreeMap<String, Table>,
    pub enums: BTreeMap<String, EnumType>,
    pub types: BTreeMap<String, CustomType>,
}

impl Schema {
    /// Create an empty schema.
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
            enums: BTreeMap::new(),
            types: BTreeMap::new(),
        }
    }

    /// Add a table to the schema, keyed by its name.
    pub fn add_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    /// Add an enum type to the schema.
    pub fn add_enum(&mut self, enum_type: EnumType) {
        self.enums.insert(enum_type.name.clone(), enum_type);
    }

    /// Add a custom type to the schema.
    pub fn add_type(&mut self, custom_type: CustomType) {
        self.types.insert(custom_type.name.clone(), custom_type);
    }

    /// Look up a table by name.
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// Look up an enum by name.
    pub fn enum_type(&self, name: &str) -> Option<&EnumType> {
        self.enums.get(name)
    }

    /// Returns an iterator over table names in deterministic order.
    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(|s| s.as_str())
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

/// A database table with its columns, constraints, and indexes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
}

impl Table {
    /// Create a new table with the given name and no columns, constraints, or indexes.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
        }
    }

    /// Add a column to the table.
    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    /// Add a constraint to the table.
    pub fn add_constraint(&mut self, constraint: Constraint) {
        self.constraints.push(constraint);
    }

    /// Add an index to the table.
    pub fn add_index(&mut self, index: Index) {
        self.indexes.push(index);
    }

    /// Look up a column by name.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Returns the primary key constraint, if any.
    pub fn primary_key(&self) -> Option<&Constraint> {
        self.constraints
            .iter()
            .find(|c| matches!(c, Constraint::PrimaryKey { .. }))
    }

    /// Returns all foreign key constraints.
    pub fn foreign_keys(&self) -> Vec<&Constraint> {
        self.constraints
            .iter()
            .filter(|c| matches!(c, Constraint::ForeignKey { .. }))
            .collect()
    }
}

/// A table column.
///
/// Column order in `Vec<Column>` reflects the physical table definition
/// order as reported by `pg_catalog`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub pg_type: PgType,
    pub nullable: bool,
    pub default: Option<Expression>,
}

impl Column {
    /// Create a new non-nullable column with no default.
    pub fn new(name: impl Into<String>, pg_type: PgType) -> Self {
        Self {
            name: name.into(),
            pg_type,
            nullable: false,
            default: None,
        }
    }

    /// Set the column as nullable.
    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    /// Set a default expression on the column.
    pub fn with_default(mut self, default: Expression) -> Self {
        self.default = Some(default);
        self
    }
}

/// Table constraint.
///
/// All constraint variants are stored in a single list on the table,
/// matching how Postgres reports them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Constraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        references: ForeignKeyRef,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Check {
        name: Option<String>,
        expression: String,
    },
}

/// A table index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub partial: Option<String>,
}

/// A Postgres enum type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumType {
    pub name: String,
    pub variants: Vec<String>,
}

/// A Postgres custom type (domain, composite, or range).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomType {
    pub name: String,
    pub kind: CustomTypeKind,
}

/// The kind of custom type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CustomTypeKind {
    Domain {
        base_type: PgType,
        constraints: Vec<String>,
    },
    Composite {
        fields: Vec<(String, PgType)>,
    },
    Range {
        subtype: PgType,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_users_table() -> Table {
        let mut table = Table::new("users");
        table.add_column(Column::new("id", PgType::Uuid));
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
        table
    }

    fn sample_posts_table() -> Table {
        let mut table = Table::new("posts");
        table.add_column(Column::new("id", PgType::Uuid));
        table.add_column(Column::new("author_id", PgType::Uuid));
        table.add_column(Column::new("title", PgType::Text));
        table.add_column(Column::new("body", PgType::Text).nullable());
        table.add_column(
            Column::new("created_at", PgType::Timestamptz)
                .with_default(Expression::FunctionCall("now()".into())),
        );
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

    #[test]
    fn empty_schema() {
        let schema = Schema::new();
        assert!(schema.tables.is_empty());
        assert!(schema.enums.is_empty());
        assert!(schema.types.is_empty());
    }

    #[test]
    fn schema_default() {
        let schema = Schema::default();
        assert_eq!(schema, Schema::new());
    }

    #[test]
    fn schema_add_and_lookup_table() {
        let mut schema = Schema::new();
        schema.add_table(sample_users_table());

        assert_eq!(schema.tables.len(), 1);
        let table = schema.table("users").expect("users table should exist");
        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 3);
    }

    #[test]
    fn schema_table_not_found() {
        let schema = Schema::new();
        assert!(schema.table("nonexistent").is_none());
    }

    #[test]
    fn schema_deterministic_ordering() {
        let mut schema = Schema::new();
        // Insert in reverse alphabetical order
        schema.add_table(sample_users_table());
        schema.add_table(sample_posts_table());
        schema.add_table(Table::new("comments"));

        // BTreeMap should iterate in alphabetical order
        let names: Vec<&str> = schema.table_names().collect();
        assert_eq!(names, vec!["comments", "posts", "users"]);
    }

    #[test]
    fn schema_add_enum() {
        let mut schema = Schema::new();
        schema.add_enum(EnumType {
            name: "mood".into(),
            variants: vec!["happy".into(), "sad".into(), "neutral".into()],
        });

        let mood = schema.enum_type("mood").expect("mood enum should exist");
        assert_eq!(mood.variants.len(), 3);
        assert_eq!(mood.variants[0], "happy");
    }

    #[test]
    fn schema_add_custom_type() {
        let mut schema = Schema::new();
        schema.add_type(CustomType {
            name: "email_address".into(),
            kind: CustomTypeKind::Domain {
                base_type: PgType::Text,
                constraints: vec!["CHECK (VALUE ~ '^.+@.+$')".into()],
            },
        });

        assert_eq!(schema.types.len(), 1);
        assert!(schema.types.contains_key("email_address"));
    }

    #[test]
    fn table_empty() {
        let table = Table::new("empty");
        assert_eq!(table.name, "empty");
        assert!(table.columns.is_empty());
        assert!(table.constraints.is_empty());
        assert!(table.indexes.is_empty());
    }

    #[test]
    fn table_column_lookup() {
        let table = sample_users_table();
        let email = table.column("email").expect("email column should exist");
        assert_eq!(email.pg_type, PgType::Text);
        assert!(!email.nullable);
        assert!(email.default.is_none());
    }

    #[test]
    fn table_column_not_found() {
        let table = sample_users_table();
        assert!(table.column("nonexistent").is_none());
    }

    #[test]
    fn table_primary_key() {
        let table = sample_users_table();
        let pk = table.primary_key().expect("should have primary key");
        match pk {
            Constraint::PrimaryKey { columns, .. } => {
                assert_eq!(columns, &["id"]);
            }
            _ => panic!("expected PrimaryKey constraint"),
        }
    }

    #[test]
    fn table_no_primary_key() {
        let table = Table::new("no_pk");
        assert!(table.primary_key().is_none());
    }

    #[test]
    fn table_foreign_keys() {
        let table = sample_posts_table();
        let fks = table.foreign_keys();
        assert_eq!(fks.len(), 1);
        match &fks[0] {
            Constraint::ForeignKey {
                columns,
                references,
                on_delete,
                ..
            } => {
                assert_eq!(columns, &["author_id"]);
                assert_eq!(references.table, "users");
                assert_eq!(references.columns, vec!["id"]);
                assert_eq!(*on_delete, Some(ReferentialAction::Cascade));
            }
            _ => panic!("expected ForeignKey constraint"),
        }
    }

    #[test]
    fn table_no_foreign_keys() {
        let table = sample_users_table();
        assert!(table.foreign_keys().is_empty());
    }

    #[test]
    fn column_builder_pattern() {
        let col = Column::new("bio", PgType::Text)
            .nullable()
            .with_default(Expression::Literal("''".into()));

        assert_eq!(col.name, "bio");
        assert_eq!(col.pg_type, PgType::Text);
        assert!(col.nullable);
        assert_eq!(col.default, Some(Expression::Literal("''".into())));
    }

    #[test]
    fn column_default_not_null() {
        let col = Column::new("id", PgType::Uuid);
        assert!(!col.nullable);
        assert!(col.default.is_none());
    }

    #[test]
    fn column_order_preserved() {
        let table = sample_users_table();
        let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "email", "created_at"]);
    }

    #[test]
    fn constraint_check() {
        let check = Constraint::Check {
            name: Some("positive_age".into()),
            expression: "age > 0".into(),
        };
        match &check {
            Constraint::Check { name, expression } => {
                assert_eq!(name.as_deref(), Some("positive_age"));
                assert_eq!(expression, "age > 0");
            }
            _ => panic!("expected Check constraint"),
        }
    }

    #[test]
    fn constraint_unnamed() {
        let pk = Constraint::PrimaryKey {
            name: None,
            columns: vec!["id".into()],
        };
        match &pk {
            Constraint::PrimaryKey { name, columns } => {
                assert!(name.is_none());
                assert_eq!(columns, &["id"]);
            }
            _ => panic!("expected PrimaryKey"),
        }
    }

    #[test]
    fn constraint_composite_primary_key() {
        let pk = Constraint::PrimaryKey {
            name: Some("composite_pk".into()),
            columns: vec!["tenant_id".into(), "user_id".into()],
        };
        match &pk {
            Constraint::PrimaryKey { columns, .. } => {
                assert_eq!(columns.len(), 2);
            }
            _ => panic!("expected PrimaryKey"),
        }
    }

    #[test]
    fn index_basic() {
        let idx = Index {
            name: "idx_email".into(),
            columns: vec!["email".into()],
            unique: true,
            partial: None,
        };
        assert!(idx.unique);
        assert!(idx.partial.is_none());
    }

    #[test]
    fn index_partial() {
        let idx = Index {
            name: "idx_active_users".into(),
            columns: vec!["email".into()],
            unique: false,
            partial: Some("WHERE active = true".into()),
        };
        assert!(!idx.unique);
        assert_eq!(idx.partial.as_deref(), Some("WHERE active = true"));
    }

    #[test]
    fn index_composite() {
        let idx = Index {
            name: "idx_composite".into(),
            columns: vec!["tenant_id".into(), "created_at".into()],
            unique: false,
            partial: None,
        };
        assert_eq!(idx.columns.len(), 2);
    }

    #[test]
    fn enum_type() {
        let e = EnumType {
            name: "status".into(),
            variants: vec!["active".into(), "inactive".into(), "pending".into()],
        };
        assert_eq!(e.name, "status");
        assert_eq!(e.variants.len(), 3);
    }

    #[test]
    fn custom_type_composite() {
        let ct = CustomType {
            name: "address".into(),
            kind: CustomTypeKind::Composite {
                fields: vec![
                    ("street".into(), PgType::Text),
                    ("city".into(), PgType::Text),
                    ("zip".into(), PgType::Varchar(Some(10))),
                ],
            },
        };
        match &ct.kind {
            CustomTypeKind::Composite { fields } => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "street");
                assert_eq!(fields[2].1, PgType::Varchar(Some(10)));
            }
            _ => panic!("expected Composite"),
        }
    }

    #[test]
    fn custom_type_range() {
        let ct = CustomType {
            name: "float_range".into(),
            kind: CustomTypeKind::Range {
                subtype: PgType::DoublePrecision,
            },
        };
        match &ct.kind {
            CustomTypeKind::Range { subtype } => {
                assert_eq!(*subtype, PgType::DoublePrecision);
            }
            _ => panic!("expected Range"),
        }
    }

    #[test]
    fn schema_clone_is_independent() {
        let mut schema = Schema::new();
        schema.add_table(sample_users_table());

        let cloned = schema.clone();
        schema.add_table(Table::new("extra"));

        assert_eq!(cloned.tables.len(), 1);
        assert_eq!(schema.tables.len(), 2);
    }

    #[test]
    fn schema_equality() {
        let mut a = Schema::new();
        a.add_table(sample_users_table());

        let mut b = Schema::new();
        b.add_table(sample_users_table());

        assert_eq!(a, b);
    }

    #[test]
    fn schema_inequality() {
        let mut a = Schema::new();
        a.add_table(sample_users_table());

        let mut b = Schema::new();
        b.add_table(sample_posts_table());

        assert_ne!(a, b);
    }

    #[test]
    fn full_schema_roundtrip() {
        let mut schema = Schema::new();
        schema.add_table(sample_users_table());
        schema.add_table(sample_posts_table());
        schema.add_enum(EnumType {
            name: "mood".into(),
            variants: vec!["happy".into(), "sad".into()],
        });

        assert_eq!(schema.tables.len(), 2);
        assert_eq!(schema.enums.len(), 1);

        let users = schema.table("users").expect("users");
        assert_eq!(users.columns.len(), 3);
        assert!(users.primary_key().is_some());
        assert!(users.foreign_keys().is_empty());

        let posts = schema.table("posts").expect("posts");
        assert_eq!(posts.columns.len(), 5);
        assert!(posts.primary_key().is_some());
        assert_eq!(posts.foreign_keys().len(), 1);
        assert_eq!(posts.indexes.len(), 1);

        let created_at = posts.column("created_at").expect("created_at");
        assert!(!created_at.nullable);
        assert_eq!(
            created_at.default,
            Some(Expression::FunctionCall("now()".into()))
        );

        let body = posts.column("body").expect("body");
        assert!(body.nullable);
    }
}
