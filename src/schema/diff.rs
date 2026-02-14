use super::types::{Expression, PgType};
use super::{Column, Constraint, Index, Table};

/// A structural change between two schema versions.
///
/// Produced by the diff engine when comparing `old: &Schema` vs `new: &Schema`.
/// Column-level modifications (rename, type change, nullability, default) are
/// bundled into a single `AlterColumn` variant. Renames are only produced when
/// explicit rename metadata is provided; the diff engine never guesses renames
/// from add+drop pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    AddTable(Table),
    DropTable(String),
    AddColumn {
        table: String,
        column: Column,
    },
    DropColumn {
        table: String,
        column: String,
    },
    AlterColumn {
        table: String,
        column: String,
        changes: ColumnChanges,
    },
    AddConstraint {
        table: String,
        constraint: Constraint,
    },
    DropConstraint {
        table: String,
        name: String,
    },
    AddIndex {
        table: String,
        index: Index,
    },
    DropIndex(String),
}

/// Bundled column modifications. Only set fields represent actual changes.
/// Enables generating a single `ALTER TABLE` statement per column.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnChanges {
    /// Rename the column. Contains the new name.
    pub rename: Option<String>,
    /// Change the column type. Contains (from, to) for migration context.
    pub data_type: Option<(PgType, PgType)>,
    /// Change nullability. `true` = make nullable, `false` = make not null.
    pub nullable: Option<bool>,
    /// Change or drop the default expression.
    pub default: Option<DefaultChange>,
}

impl ColumnChanges {
    /// Returns true if no changes are set.
    pub fn is_empty(&self) -> bool {
        self.rename.is_none()
            && self.data_type.is_none()
            && self.nullable.is_none()
            && self.default.is_none()
    }
}

/// A change to a column's default expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultChange {
    Set(Expression),
    Drop,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_add_table() {
        let table = Table::new("users");
        let change = Change::AddTable(table.clone());
        match &change {
            Change::AddTable(t) => assert_eq!(t.name, "users"),
            _ => panic!("expected AddTable"),
        }
    }

    #[test]
    fn change_drop_table() {
        let change = Change::DropTable("old_table".into());
        match &change {
            Change::DropTable(name) => assert_eq!(name, "old_table"),
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn change_add_column() {
        let col = Column::new("email", PgType::Text);
        let change = Change::AddColumn {
            table: "users".into(),
            column: col,
        };
        match &change {
            Change::AddColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column.name, "email");
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn change_drop_column() {
        let change = Change::DropColumn {
            table: "users".into(),
            column: "age".into(),
        };
        match &change {
            Change::DropColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "age");
            }
            _ => panic!("expected DropColumn"),
        }
    }

    #[test]
    fn alter_column_rename_only() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "name".into(),
            changes: ColumnChanges {
                rename: Some("full_name".into()),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn {
                table,
                column,
                changes,
            } => {
                assert_eq!(table, "users");
                assert_eq!(column, "name");
                assert_eq!(changes.rename.as_deref(), Some("full_name"));
                assert!(changes.data_type.is_none());
                assert!(changes.nullable.is_none());
                assert!(changes.default.is_none());
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_type_change() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "age".into(),
            changes: ColumnChanges {
                data_type: Some((PgType::Integer, PgType::BigInt)),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                let (from, to) = changes.data_type.as_ref().expect("should have type change");
                assert_eq!(*from, PgType::Integer);
                assert_eq!(*to, PgType::BigInt);
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_set_not_null() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(false),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(changes.nullable, Some(false));
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_drop_not_null() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(true),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(changes.nullable, Some(true));
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_set_default() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "created_at".into(),
            changes: ColumnChanges {
                default: Some(DefaultChange::Set(Expression::FunctionCall("now()".into()))),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(
                    changes.default,
                    Some(DefaultChange::Set(Expression::FunctionCall("now()".into())))
                );
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_drop_default() {
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "created_at".into(),
            changes: ColumnChanges {
                default: Some(DefaultChange::Drop),
                ..Default::default()
            },
        };
        match &change {
            Change::AlterColumn { changes, .. } => {
                assert_eq!(changes.default, Some(DefaultChange::Drop));
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn alter_column_bundled_changes() {
        let changes = ColumnChanges {
            rename: Some("full_name".into()),
            data_type: Some((PgType::Varchar(Some(100)), PgType::Text)),
            nullable: Some(false),
            default: None,
        };
        let change = Change::AlterColumn {
            table: "users".into(),
            column: "name".into(),
            changes,
        };
        match &change {
            Change::AlterColumn {
                column, changes, ..
            } => {
                assert_eq!(column, "name");
                assert_eq!(changes.rename.as_deref(), Some("full_name"));
                assert!(changes.data_type.is_some());
                assert_eq!(changes.nullable, Some(false));
                assert!(changes.default.is_none());
            }
            _ => panic!("expected AlterColumn"),
        }
    }

    #[test]
    fn column_changes_is_empty() {
        let empty = ColumnChanges::default();
        assert!(empty.is_empty());

        let not_empty = ColumnChanges {
            nullable: Some(true),
            ..Default::default()
        };
        assert!(!not_empty.is_empty());
    }

    #[test]
    fn change_equality() {
        let a = Change::DropColumn {
            table: "users".into(),
            column: "age".into(),
        };
        let b = Change::DropColumn {
            table: "users".into(),
            column: "age".into(),
        };
        assert_eq!(a, b);
    }

    #[test]
    fn change_inequality_different_variants() {
        let a = Change::AddTable(Table::new("users"));
        let b = Change::DropTable("users".into());
        assert_ne!(a, b);
    }

    #[test]
    fn alter_column_inequality_different_changes() {
        let a = Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(false),
                ..Default::default()
            },
        };
        let b = Change::AlterColumn {
            table: "users".into(),
            column: "email".into(),
            changes: ColumnChanges {
                nullable: Some(true),
                ..Default::default()
            },
        };
        assert_ne!(a, b);
    }
}
