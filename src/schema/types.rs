use serde::{Deserialize, Serialize};

/// Postgres type representation.
///
/// Covers common types with a `Custom` variant for anything else
/// (enums, domains, composite types).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Real,
    DoublePrecision,
    /// Numeric with optional (precision, scale). `None` = arbitrary precision.
    /// `Some((p, s))` = `NUMERIC(p, s)`. Scale 0 renders as `NUMERIC(p)`.
    Numeric(Option<(u32, u32)>),
    Text,
    Varchar(Option<u32>),
    Char(Option<u32>),
    Bytea,
    Uuid,
    Timestamp,
    Timestamptz,
    Date,
    Time,
    Timetz,
    Interval,
    Json,
    Jsonb,
    Array(Box<PgType>),
    Custom(String),
}

impl PgType {
    /// Returns `true` for text-family types (text, varchar, char).
    ///
    /// Used by the default-prompt auto-quoting logic: unquoted input on a
    /// text-type column that doesn't look like a SQL keyword or function call
    /// is automatically wrapped in single quotes.
    pub fn is_text_type(&self) -> bool {
        matches!(self, PgType::Text | PgType::Varchar(_) | PgType::Char(_))
    }
}

impl std::fmt::Display for PgType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgType::Boolean => write!(f, "boolean"),
            PgType::SmallInt => write!(f, "smallint"),
            PgType::Integer => write!(f, "integer"),
            PgType::BigInt => write!(f, "bigint"),
            PgType::Real => write!(f, "real"),
            PgType::DoublePrecision => write!(f, "double precision"),
            PgType::Numeric(None) => write!(f, "numeric"),
            PgType::Numeric(Some((p, 0))) => write!(f, "numeric({p})"),
            PgType::Numeric(Some((p, s))) => write!(f, "numeric({p},{s})"),
            PgType::Text => write!(f, "text"),
            PgType::Varchar(None) => write!(f, "varchar"),
            PgType::Varchar(Some(n)) => write!(f, "varchar({n})"),
            PgType::Char(None) => write!(f, "char"),
            PgType::Char(Some(n)) => write!(f, "char({n})"),
            PgType::Bytea => write!(f, "bytea"),
            PgType::Uuid => write!(f, "uuid"),
            PgType::Timestamp => write!(f, "timestamp"),
            PgType::Timestamptz => write!(f, "timestamptz"),
            PgType::Date => write!(f, "date"),
            PgType::Time => write!(f, "time"),
            PgType::Timetz => write!(f, "timetz"),
            PgType::Interval => write!(f, "interval"),
            PgType::Json => write!(f, "json"),
            PgType::Jsonb => write!(f, "jsonb"),
            PgType::Array(inner) => write!(f, "{inner}[]"),
            PgType::Custom(name) => write!(f, "{name}"),
        }
    }
}

/// Column default expression.
///
/// Enables structural comparison during diffing. The `Raw` variant
/// is the escape hatch for expressions we can't parse structurally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expression {
    Literal(String),
    FunctionCall(String),
    Raw(String),
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Literal(s) => write!(f, "{s}"),
            Expression::FunctionCall(s) => write!(f, "{s}"),
            Expression::Raw(s) => write!(f, "{s}"),
        }
    }
}

/// Foreign key referential action (ON DELETE / ON UPDATE).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl std::fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReferentialAction::NoAction => write!(f, "NO ACTION"),
            ReferentialAction::Restrict => write!(f, "RESTRICT"),
            ReferentialAction::Cascade => write!(f, "CASCADE"),
            ReferentialAction::SetNull => write!(f, "SET NULL"),
            ReferentialAction::SetDefault => write!(f, "SET DEFAULT"),
        }
    }
}

/// Target of a foreign key constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyRef {
    pub table: String,
    pub columns: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_type_display_simple() {
        assert_eq!(PgType::Boolean.to_string(), "boolean");
        assert_eq!(PgType::Text.to_string(), "text");
        assert_eq!(PgType::Uuid.to_string(), "uuid");
        assert_eq!(PgType::Timestamptz.to_string(), "timestamptz");
        assert_eq!(PgType::Integer.to_string(), "integer");
    }

    #[test]
    fn pg_type_display_parameterized() {
        assert_eq!(PgType::Varchar(None).to_string(), "varchar");
        assert_eq!(PgType::Varchar(Some(255)).to_string(), "varchar(255)");
        assert_eq!(PgType::Char(Some(1)).to_string(), "char(1)");
    }

    #[test]
    fn pg_type_display_numeric() {
        assert_eq!(PgType::Numeric(None).to_string(), "numeric");
        assert_eq!(PgType::Numeric(Some((10, 0))).to_string(), "numeric(10)");
        assert_eq!(PgType::Numeric(Some((10, 2))).to_string(), "numeric(10,2)");
    }

    #[test]
    fn pg_type_numeric_equality() {
        assert_eq!(PgType::Numeric(None), PgType::Numeric(None));
        assert_ne!(PgType::Numeric(None), PgType::Numeric(Some((10, 2))));
        assert_ne!(
            PgType::Numeric(Some((10, 2))),
            PgType::Numeric(Some((10, 4)))
        );
        assert_eq!(
            PgType::Numeric(Some((10, 2))),
            PgType::Numeric(Some((10, 2)))
        );
    }

    #[test]
    fn pg_type_display_array() {
        assert_eq!(PgType::Array(Box::new(PgType::Text)).to_string(), "text[]");
        assert_eq!(
            PgType::Array(Box::new(PgType::Integer)).to_string(),
            "integer[]"
        );
    }

    #[test]
    fn pg_type_display_custom() {
        assert_eq!(PgType::Custom("mood".into()).to_string(), "mood");
    }

    #[test]
    fn pg_type_nested_array() {
        let nested = PgType::Array(Box::new(PgType::Array(Box::new(PgType::Integer))));
        assert_eq!(nested.to_string(), "integer[][]");
    }

    #[test]
    fn expression_display() {
        assert_eq!(Expression::Literal("42".into()).to_string(), "42");
        assert_eq!(
            Expression::FunctionCall("now()".into()).to_string(),
            "now()"
        );
        assert_eq!(
            Expression::Raw("CURRENT_TIMESTAMP".into()).to_string(),
            "CURRENT_TIMESTAMP"
        );
    }

    #[test]
    fn referential_action_display() {
        assert_eq!(ReferentialAction::Cascade.to_string(), "CASCADE");
        assert_eq!(ReferentialAction::SetNull.to_string(), "SET NULL");
        assert_eq!(ReferentialAction::NoAction.to_string(), "NO ACTION");
        assert_eq!(ReferentialAction::Restrict.to_string(), "RESTRICT");
        assert_eq!(ReferentialAction::SetDefault.to_string(), "SET DEFAULT");
    }

    #[test]
    fn pg_type_equality() {
        assert_eq!(PgType::Integer, PgType::Integer);
        assert_ne!(PgType::Integer, PgType::BigInt);
        assert_eq!(PgType::Varchar(Some(50)), PgType::Varchar(Some(50)));
        assert_ne!(PgType::Varchar(Some(50)), PgType::Varchar(Some(100)));
        assert_ne!(PgType::Varchar(Some(50)), PgType::Varchar(None));
    }

    #[test]
    fn pg_type_clone() {
        let arr = PgType::Array(Box::new(PgType::Jsonb));
        let cloned = arr.clone();
        assert_eq!(arr, cloned);
    }

    #[test]
    fn pg_type_is_text_type() {
        assert!(PgType::Text.is_text_type());
        assert!(PgType::Varchar(None).is_text_type());
        assert!(PgType::Varchar(Some(255)).is_text_type());
        assert!(PgType::Char(None).is_text_type());
        assert!(PgType::Char(Some(1)).is_text_type());
        assert!(!PgType::Integer.is_text_type());
        assert!(!PgType::Boolean.is_text_type());
        assert!(!PgType::Uuid.is_text_type());
        assert!(!PgType::Jsonb.is_text_type());
        assert!(!PgType::Array(Box::new(PgType::Text)).is_text_type());
    }

    #[test]
    fn foreign_key_ref_fields() {
        let fk_ref = ForeignKeyRef {
            table: "users".into(),
            columns: vec!["id".into()],
        };
        assert_eq!(fk_ref.table, "users");
        assert_eq!(fk_ref.columns, vec!["id"]);
    }
}
