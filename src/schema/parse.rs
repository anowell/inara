use winnow::combinator::{alt, opt};
use winnow::error::{ContextError, ErrMode, ParseError, StrContext};
use winnow::prelude::*;
use winnow::token::{take_till, take_while};
use winnow::ModalResult;

use super::{
    types::{Expression, ForeignKeyRef, PgType, ReferentialAction},
    Column, Constraint, CustomType, CustomTypeKind, EnumType, Index, Schema, Table,
};

/// Error returned when parsing the declarative schema format fails.
#[derive(Debug, thiserror::Error)]
#[error("parse error at line {line}, column {col}: {message}")]
pub struct SchemaParseError {
    pub line: usize,
    pub col: usize,
    pub message: String,
}

/// Parse the declarative schema format into a `Schema`.
///
/// This is the inverse of `render::render_schema`. For any valid schema,
/// `parse_schema(render_schema(s))` should produce an equivalent schema.
pub fn parse_schema(input: &str) -> Result<Schema, SchemaParseError> {
    schema.parse(input).map_err(|e| format_error(input, e))
}

fn format_error(input: &str, e: ParseError<&str, ContextError>) -> SchemaParseError {
    let offset = e.offset();
    let consumed = &input[..offset];
    let line = consumed.chars().filter(|&c| c == '\n').count() + 1;
    let col = consumed.rfind('\n').map_or(offset, |nl| offset - nl - 1) + 1;
    let message = e.inner().to_string().trim().to_string();
    let message = if message.is_empty() {
        "unexpected input".to_string()
    } else {
        message
    };
    SchemaParseError { line, col, message }
}

// ── Top-level ───────────────────────────────────────────────────

fn schema(input: &mut &str) -> ModalResult<Schema> {
    let mut s = Schema::new();
    ws(input)?;
    while !input.is_empty() {
        let decl = declaration(input)?;
        match decl {
            Declaration::Enum(e) => s.add_enum(e),
            Declaration::CustomType(ct) => s.add_type(ct),
            Declaration::Table(t) => s.add_table(t),
        }
        ws(input)?;
    }
    Ok(s)
}

enum Declaration {
    Enum(EnumType),
    CustomType(CustomType),
    Table(Table),
}

fn declaration(input: &mut &str) -> ModalResult<Declaration> {
    alt((
        enum_decl.map(Declaration::Enum),
        domain_decl.map(Declaration::CustomType),
        composite_decl.map(Declaration::CustomType),
        range_decl.map(Declaration::CustomType),
        table_decl.map(Declaration::Table),
    ))
    .context(StrContext::Label("declaration"))
    .parse_next(input)
}

// ── Enum ────────────────────────────────────────────────────────

fn enum_decl(input: &mut &str) -> ModalResult<EnumType> {
    "enum".parse_next(input)?;
    ws1(input)?;
    let name = identifier(input)?;
    ws(input)?;
    "{".parse_next(input)?;
    ws(input)?;

    // Empty enum: `enum name { }`
    if input.starts_with('}') {
        "}".parse_next(input)?;
        ws(input)?;
        return Ok(EnumType {
            name,
            variants: vec![],
        });
    }

    let mut variants = Vec::new();
    loop {
        if input.starts_with('}') {
            break;
        }
        let variant = identifier(input)?;
        variants.push(variant);
        ws(input)?;
    }
    "}".parse_next(input)?;
    ws(input)?;
    Ok(EnumType { name, variants })
}

// ── Custom types ────────────────────────────────────────────────

fn domain_decl(input: &mut &str) -> ModalResult<CustomType> {
    "domain".parse_next(input)?;
    ws1(input)?;
    let name = identifier(input)?;
    ws1(input)?;
    let base_type = pg_type(input)?;

    // Collect remaining constraints until end of line
    let mut constraints = Vec::new();
    loop {
        // Eat horizontal whitespace
        take_while(0.., |c: char| c == ' ' || c == '\t').parse_next(input)?;
        if input.starts_with('\n') || input.is_empty() {
            break;
        }
        // Collect text until end of line as a constraint
        let constraint: &str = take_till(1.., |c: char| c == '\n').parse_next(input)?;
        constraints.push(constraint.trim_end().to_string());
    }
    ws(input)?;

    Ok(CustomType {
        name,
        kind: CustomTypeKind::Domain {
            base_type,
            constraints,
        },
    })
}

fn composite_decl(input: &mut &str) -> ModalResult<CustomType> {
    "composite".parse_next(input)?;
    ws1(input)?;
    let name = identifier(input)?;
    ws(input)?;
    "{".parse_next(input)?;
    ws(input)?;

    if input.starts_with('}') {
        "}".parse_next(input)?;
        ws(input)?;
        return Ok(CustomType {
            name,
            kind: CustomTypeKind::Composite { fields: vec![] },
        });
    }

    let mut fields = Vec::new();
    loop {
        if input.starts_with('}') {
            break;
        }
        let field_name = identifier(input)?;
        ws1(input)?;
        let field_type = pg_type(input)?;
        fields.push((field_name, field_type));
        ws(input)?;
    }
    "}".parse_next(input)?;
    ws(input)?;

    Ok(CustomType {
        name,
        kind: CustomTypeKind::Composite { fields },
    })
}

fn range_decl(input: &mut &str) -> ModalResult<CustomType> {
    "range".parse_next(input)?;
    ws1(input)?;
    let name = identifier(input)?;
    ws1(input)?;
    let subtype = pg_type(input)?;
    ws(input)?;

    Ok(CustomType {
        name,
        kind: CustomTypeKind::Range { subtype },
    })
}

// ── Table ───────────────────────────────────────────────────────

fn table_decl(input: &mut &str) -> ModalResult<Table> {
    "table".parse_next(input)?;
    ws1(input)?;
    let name = identifier(input)?;
    ws(input)?;
    "{".parse_next(input)?;
    ws(input)?;

    // Empty table
    if input.starts_with('}') {
        "}".parse_next(input)?;
        ws(input)?;
        return Ok(Table::new(name));
    }

    let mut table = Table::new(name);

    // Parse table body: columns first, then constraints/indexes
    // Columns are lines starting with a lowercase identifier
    // Constraints/indexes start with keywords (PRIMARY, UNIQUE, FOREIGN, CHECK, INDEX)

    loop {
        // Skip whitespace (including blank line separators)
        ws(input)?;
        if input.is_empty() || input.starts_with('}') {
            break;
        }

        if input.starts_with("PRIMARY KEY")
            || input.starts_with("UNIQUE (")
            || input.starts_with("UNIQUE INDEX")
            || input.starts_with("FOREIGN KEY")
            || input.starts_with("CHECK")
            || input.starts_with("INDEX")
        {
            parse_table_constraint_or_index(&mut table, input)?;
        } else {
            parse_column_line(&mut table, input)?;
        }
    }

    "}".parse_next(input)?;
    ws(input)?;
    Ok(table)
}

fn parse_column_line(table: &mut Table, input: &mut &str) -> ModalResult<()> {
    let name = identifier(input)?;
    ws1(input)?;
    let pg = pg_type(input)?;

    let mut nullable = true;
    let mut default = None;
    let mut is_pk = false;
    let mut is_unique = false;

    // Parse inline modifiers
    loop {
        // Check for end of line or end of modifiers
        let remaining = input.trim_start_matches([' ', '\t']);
        if remaining.starts_with('\n') || remaining.is_empty() || remaining.starts_with('}') {
            break;
        }

        // Skip horizontal whitespace
        take_while(0.., |c: char| c == ' ' || c == '\t').parse_next(input)?;

        if input.starts_with("NOT NULL") {
            "NOT NULL".parse_next(input)?;
            nullable = false;
        } else if input.starts_with("DEFAULT ") {
            "DEFAULT ".parse_next(input)?;
            default = Some(parse_default_expression(input)?);
        } else if input.starts_with("PRIMARY KEY") {
            "PRIMARY KEY".parse_next(input)?;
            is_pk = true;
        } else if input.starts_with("UNIQUE") {
            "UNIQUE".parse_next(input)?;
            is_unique = true;
        } else {
            break;
        }
    }

    // Consume to end of line
    take_while(0.., |c: char| c == ' ' || c == '\t').parse_next(input)?;
    opt("\n").parse_next(input)?;

    let col = Column {
        name: name.clone(),
        pg_type: pg,
        nullable,
        default,
    };
    table.add_column(col);

    if is_pk {
        table.add_constraint(Constraint::PrimaryKey {
            name: None,
            columns: vec![name.clone()],
        });
    }
    if is_unique {
        table.add_constraint(Constraint::Unique {
            name: None,
            columns: vec![name],
        });
    }

    Ok(())
}

fn parse_default_expression(input: &mut &str) -> ModalResult<Expression> {
    // A default expression continues until we hit a double-space separator
    // before another keyword (NOT NULL, PRIMARY KEY, UNIQUE) or end of line.
    //
    // Expressions can be:
    // - String literals: 'hello', ''
    // - Numeric literals: 42, 3.14
    // - Function calls: now(), gen_random_uuid()
    // - Keywords: CURRENT_TIMESTAMP, true, false
    // - Raw expressions: anything else
    //
    // The tricky part: we need to stop at `  PRIMARY KEY`, `  UNIQUE`, or newline,
    // but NOT stop at spaces within the expression itself.
    //
    // Strategy: collect characters, stopping when we see `  PRIMARY KEY`,
    // `  UNIQUE`, or end-of-line.

    let mut result = String::new();

    loop {
        if input.is_empty() || input.starts_with('\n') {
            break;
        }
        // Check for double-space followed by keyword (end of default expr)
        if input.starts_with("  PRIMARY KEY") || input.starts_with("  UNIQUE") {
            break;
        }
        // Take one character
        let c = input.chars().next().unwrap();
        *input = &input[c.len_utf8()..];
        result.push(c);
    }

    let result = result.trim_end().to_string();

    // Classify the expression
    let expr = if result.contains('(') && result.ends_with(')') {
        Expression::FunctionCall(result)
    } else if result.starts_with('\'')
        || result.starts_with('-')
        || result.chars().next().is_some_and(|c| c.is_ascii_digit())
    {
        Expression::Literal(result)
    } else {
        Expression::Raw(result)
    };

    Ok(expr)
}

fn parse_table_constraint_or_index(table: &mut Table, input: &mut &str) -> ModalResult<()> {
    if input.starts_with("PRIMARY KEY") {
        parse_primary_key_constraint(table, input)?;
    } else if input.starts_with("UNIQUE INDEX") {
        parse_index(table, input, true)?;
    } else if input.starts_with("UNIQUE (") {
        parse_unique_constraint(table, input)?;
    } else if input.starts_with("FOREIGN KEY") {
        parse_foreign_key_constraint(table, input)?;
    } else if input.starts_with("CHECK") {
        parse_check_constraint(table, input)?;
    } else if input.starts_with("INDEX") {
        parse_index(table, input, false)?;
    } else {
        return Err(ErrMode::Cut(ContextError::new()));
    }

    Ok(())
}

fn parse_primary_key_constraint(table: &mut Table, input: &mut &str) -> ModalResult<()> {
    "PRIMARY KEY (".parse_next(input)?;
    let cols = column_list(input)?;
    ")".parse_next(input)?;
    consume_line(input)?;
    table.add_constraint(Constraint::PrimaryKey {
        name: None,
        columns: cols,
    });
    Ok(())
}

fn parse_unique_constraint(table: &mut Table, input: &mut &str) -> ModalResult<()> {
    "UNIQUE (".parse_next(input)?;
    let cols = column_list(input)?;
    ")".parse_next(input)?;
    consume_line(input)?;
    table.add_constraint(Constraint::Unique {
        name: None,
        columns: cols,
    });
    Ok(())
}

fn parse_foreign_key_constraint(table: &mut Table, input: &mut &str) -> ModalResult<()> {
    "FOREIGN KEY (".parse_next(input)?;
    let cols = column_list(input)?;
    ") REFERENCES ".parse_next(input)?;
    let ref_table = identifier(input)?;
    "(".parse_next(input)?;
    let ref_cols = column_list(input)?;
    ")".parse_next(input)?;

    let mut on_delete = None;
    let mut on_update = None;

    // Parse optional ON DELETE / ON UPDATE
    loop {
        take_while(0.., |c: char| c == ' ' || c == '\t').parse_next(input)?;
        if input.starts_with("ON DELETE ") {
            "ON DELETE ".parse_next(input)?;
            on_delete = Some(referential_action(input)?);
        } else if input.starts_with("ON UPDATE ") {
            "ON UPDATE ".parse_next(input)?;
            on_update = Some(referential_action(input)?);
        } else {
            break;
        }
    }

    consume_line(input)?;
    table.add_constraint(Constraint::ForeignKey {
        name: None,
        columns: cols,
        references: ForeignKeyRef {
            table: ref_table,
            columns: ref_cols,
        },
        on_delete,
        on_update,
    });
    Ok(())
}

fn parse_check_constraint(table: &mut Table, input: &mut &str) -> ModalResult<()> {
    "CHECK (".parse_next(input)?;
    // The expression can contain nested parens, so we need balanced paren parsing
    let expr = balanced_parens(input)?;
    ")".parse_next(input)?;
    consume_line(input)?;
    table.add_constraint(Constraint::Check {
        name: None,
        expression: expr,
    });
    Ok(())
}

fn parse_index(table: &mut Table, input: &mut &str, unique_prefix: bool) -> ModalResult<()> {
    if unique_prefix {
        "UNIQUE INDEX ".parse_next(input)?;
    } else {
        "INDEX ".parse_next(input)?;
    }
    let name = identifier(input)?;
    "(".parse_next(input)?;
    let cols = column_list(input)?;
    ")".parse_next(input)?;

    // Optional WHERE clause (partial index) — rest of line
    take_while(0.., |c: char| c == ' ' || c == '\t').parse_next(input)?;
    let partial = if input.starts_with("WHERE ") || input.starts_with("where ") {
        let clause: &str = take_till(0.., |c: char| c == '\n').parse_next(input)?;
        let clause = clause.trim_end();
        if clause.is_empty() {
            None
        } else {
            Some(clause.to_string())
        }
    } else {
        None
    };

    consume_line(input)?;
    table.add_index(Index {
        name,
        columns: cols,
        unique: unique_prefix,
        partial,
    });
    Ok(())
}

// ── PgType parser ───────────────────────────────────────────────

fn pg_type(input: &mut &str) -> ModalResult<PgType> {
    let ty = alt((pg_type_parameterized, pg_type_simple, pg_type_custom)).parse_next(input)?;

    // Check for array suffix(es)
    let mut result = ty;
    while input.starts_with("[]") {
        "[]".parse_next(input)?;
        result = PgType::Array(Box::new(result));
    }

    Ok(result)
}

/// Match a keyword and verify it's followed by a word boundary (not alnum or underscore).
fn keyword<'a>(mut kw: &'static str) -> impl FnMut(&mut &'a str) -> ModalResult<&'a str> {
    move |input: &mut &'a str| {
        let matched: &str = kw.parse_next(input)?;
        // Ensure word boundary: next char must NOT be alphanumeric or underscore
        if input
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(ErrMode::Backtrack(ContextError::new()));
        }
        Ok(matched)
    }
}

fn pg_type_simple(input: &mut &str) -> ModalResult<PgType> {
    // Must check multi-word types before single-word, and longer prefixes first
    alt((
        keyword("double precision").value(PgType::DoublePrecision),
        keyword("boolean").value(PgType::Boolean),
        keyword("smallint").value(PgType::SmallInt),
        keyword("integer").value(PgType::Integer),
        keyword("bigint").value(PgType::BigInt),
        keyword("real").value(PgType::Real),
        keyword("text").value(PgType::Text),
        keyword("bytea").value(PgType::Bytea),
        keyword("uuid").value(PgType::Uuid),
        keyword("timestamptz").value(PgType::Timestamptz),
        keyword("timestamp").value(PgType::Timestamp),
        keyword("timetz").value(PgType::Timetz),
        keyword("time").value(PgType::Time),
        keyword("date").value(PgType::Date),
        keyword("interval").value(PgType::Interval),
        keyword("jsonb").value(PgType::Jsonb),
        keyword("json").value(PgType::Json),
    ))
    .parse_next(input)
}

fn pg_type_parameterized(input: &mut &str) -> ModalResult<PgType> {
    alt((pg_type_numeric, pg_type_varchar, pg_type_char)).parse_next(input)
}

fn pg_type_numeric(input: &mut &str) -> ModalResult<PgType> {
    keyword("numeric").parse_next(input)?;
    if input.starts_with('(') {
        "(".parse_next(input)?;
        let p = parse_u32(input)?;
        let s = if input.starts_with(',') {
            ",".parse_next(input)?;
            parse_u32(input)?
        } else {
            0
        };
        ")".parse_next(input)?;
        Ok(PgType::Numeric(Some((p, s))))
    } else {
        Ok(PgType::Numeric(None))
    }
}

fn pg_type_varchar(input: &mut &str) -> ModalResult<PgType> {
    keyword("varchar").parse_next(input)?;
    if input.starts_with('(') {
        "(".parse_next(input)?;
        let n = parse_u32(input)?;
        ")".parse_next(input)?;
        Ok(PgType::Varchar(Some(n)))
    } else {
        Ok(PgType::Varchar(None))
    }
}

fn pg_type_char(input: &mut &str) -> ModalResult<PgType> {
    keyword("char").parse_next(input)?;
    if input.starts_with('(') {
        "(".parse_next(input)?;
        let n = parse_u32(input)?;
        ")".parse_next(input)?;
        Ok(PgType::Char(Some(n)))
    } else {
        Ok(PgType::Char(None))
    }
}

/// Custom/enum type: any identifier that doesn't match a known type keyword.
fn pg_type_custom(input: &mut &str) -> ModalResult<PgType> {
    let name = identifier(input)?;
    Ok(PgType::Custom(name))
}

// ── Helpers ─────────────────────────────────────────────────────

/// Parse a simple identifier (alphanumeric + underscore, starting with letter or underscore).
fn identifier(input: &mut &str) -> ModalResult<String> {
    let first: char = winnow::token::any
        .verify(|c: &char| c.is_ascii_alphabetic() || *c == '_')
        .parse_next(input)?;
    let rest: &str =
        take_while(0.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)?;
    let mut name = String::with_capacity(1 + rest.len());
    name.push(first);
    name.push_str(rest);
    Ok(name)
}

/// Parse a u32 number.
fn parse_u32(input: &mut &str) -> ModalResult<u32> {
    let digits: &str = take_while(1.., |c: char| c.is_ascii_digit()).parse_next(input)?;
    digits
        .parse::<u32>()
        .map_err(|_| ErrMode::Cut(ContextError::new()))
}

/// Parse a referential action keyword.
fn referential_action(input: &mut &str) -> ModalResult<ReferentialAction> {
    alt((
        "NO ACTION".value(ReferentialAction::NoAction),
        "RESTRICT".value(ReferentialAction::Restrict),
        "CASCADE".value(ReferentialAction::Cascade),
        "SET NULL".value(ReferentialAction::SetNull),
        "SET DEFAULT".value(ReferentialAction::SetDefault),
    ))
    .parse_next(input)
}

/// Parse a comma-separated list of identifiers.
fn column_list(input: &mut &str) -> ModalResult<Vec<String>> {
    let first = identifier(input)?;
    let mut cols = vec![first];
    loop {
        take_while(0.., |c: char| c == ' ').parse_next(input)?;
        if input.starts_with(',') {
            ",".parse_next(input)?;
            take_while(0.., |c: char| c == ' ').parse_next(input)?;
            cols.push(identifier(input)?);
        } else {
            break;
        }
    }
    Ok(cols)
}

/// Parse balanced parentheses content (not including the outer parens).
fn balanced_parens(input: &mut &str) -> ModalResult<String> {
    let mut result = String::new();
    let mut depth = 0;
    loop {
        if input.is_empty() {
            return Err(ErrMode::Cut(ContextError::new()));
        }
        let c = input.chars().next().unwrap();
        if c == '(' {
            depth += 1;
            *input = &input[1..];
            result.push(c);
        } else if c == ')' {
            if depth == 0 {
                break;
            }
            depth -= 1;
            *input = &input[1..];
            result.push(c);
        } else {
            *input = &input[c.len_utf8()..];
            result.push(c);
        }
    }
    Ok(result)
}

/// Consume optional whitespace (spaces, tabs, newlines).
fn ws(input: &mut &str) -> ModalResult<()> {
    take_while(0.., |c: char| c.is_ascii_whitespace()).parse_next(input)?;
    Ok(())
}

/// Consume at least one whitespace character.
fn ws1(input: &mut &str) -> ModalResult<()> {
    take_while(1.., |c: char| c == ' ' || c == '\t').parse_next(input)?;
    Ok(())
}

/// Consume the rest of the current line (optional trailing whitespace + newline).
fn consume_line(input: &mut &str) -> ModalResult<()> {
    take_while(0.., |c: char| c == ' ' || c == '\t').parse_next(input)?;
    opt("\n").parse_next(input)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::render::render_schema;

    // ── Basic parsing tests ─────────────────────────────────────

    #[test]
    fn parse_empty_schema() {
        let schema = parse_schema("").unwrap();
        assert!(schema.tables.is_empty());
        assert!(schema.enums.is_empty());
        assert!(schema.types.is_empty());
    }

    #[test]
    fn parse_empty_table() {
        let schema = parse_schema("table empty { }\n").unwrap();
        let table = schema.table("empty").unwrap();
        assert!(table.columns.is_empty());
        assert!(table.constraints.is_empty());
        assert!(table.indexes.is_empty());
    }

    #[test]
    fn parse_empty_enum() {
        let schema = parse_schema("enum empty { }\n").unwrap();
        let e = schema.enum_type("empty").unwrap();
        assert!(e.variants.is_empty());
    }

    #[test]
    fn parse_enum_with_variants() {
        let input = "enum mood {\n    happy\n    sad\n    neutral\n}\n";
        let schema = parse_schema(input).unwrap();
        let e = schema.enum_type("mood").unwrap();
        assert_eq!(e.variants, vec!["happy", "sad", "neutral"]);
    }

    #[test]
    fn parse_simple_table() {
        let input = "\
table users {
    id          uuid         NOT NULL  DEFAULT gen_random_uuid()  PRIMARY KEY
    email       text         NOT NULL  UNIQUE
    created_at  timestamptz  NOT NULL  DEFAULT now()
}
";
        let schema = parse_schema(input).unwrap();
        let table = schema.table("users").unwrap();
        assert_eq!(table.columns.len(), 3);

        let id = table.column("id").unwrap();
        assert_eq!(id.pg_type, PgType::Uuid);
        assert!(!id.nullable);
        assert_eq!(
            id.default,
            Some(Expression::FunctionCall("gen_random_uuid()".into()))
        );

        let email = table.column("email").unwrap();
        assert!(!email.nullable);

        // Check inline PK
        assert!(table
            .constraints
            .iter()
            .any(|c| matches!(c, Constraint::PrimaryKey { columns, .. } if columns == &["id"])));
        // Check inline UNIQUE
        assert!(table
            .constraints
            .iter()
            .any(|c| matches!(c, Constraint::Unique { columns, .. } if columns == &["email"])));
    }

    #[test]
    fn parse_nullable_columns() {
        let input = "\
table profiles {
    id      uuid    NOT NULL  PRIMARY KEY
    mood    mood
    status  status  NOT NULL
}
";
        let schema = parse_schema(input).unwrap();
        let table = schema.table("profiles").unwrap();

        let mood = table.column("mood").unwrap();
        assert!(mood.nullable);
        assert_eq!(mood.pg_type, PgType::Custom("mood".into()));

        let status = table.column("status").unwrap();
        assert!(!status.nullable);
    }

    #[test]
    fn parse_foreign_key_with_actions() {
        let input = "\
table orders {
    id         bigint  NOT NULL
    user_id    uuid    NOT NULL

    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL ON UPDATE CASCADE
}
";
        let schema = parse_schema(input).unwrap();
        let table = schema.table("orders").unwrap();
        let fks = table.foreign_keys();
        assert_eq!(fks.len(), 1);
        match &fks[0] {
            Constraint::ForeignKey {
                columns,
                references,
                on_delete,
                on_update,
                ..
            } => {
                assert_eq!(columns, &["user_id"]);
                assert_eq!(references.table, "users");
                assert_eq!(references.columns, vec!["id"]);
                assert_eq!(*on_delete, Some(ReferentialAction::SetNull));
                assert_eq!(*on_update, Some(ReferentialAction::Cascade));
            }
            _ => panic!("expected ForeignKey"),
        }
    }

    #[test]
    fn parse_composite_primary_key() {
        let input = "\
table team_members {
    team_id  uuid  NOT NULL
    user_id  uuid  NOT NULL

    PRIMARY KEY (team_id, user_id)
}
";
        let schema = parse_schema(input).unwrap();
        let table = schema.table("team_members").unwrap();
        let pk = table.primary_key().unwrap();
        match pk {
            Constraint::PrimaryKey { columns, .. } => {
                assert_eq!(columns, &["team_id", "user_id"]);
            }
            _ => panic!("expected PrimaryKey"),
        }
    }

    #[test]
    fn parse_check_constraint() {
        let input = "\
table orders {
    amount  numeric(10,2)  NOT NULL

    CHECK (amount > 0)
}
";
        let schema = parse_schema(input).unwrap();
        let table = schema.table("orders").unwrap();
        let checks: Vec<_> = table
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::Check { .. }))
            .collect();
        assert_eq!(checks.len(), 1);
        match &checks[0] {
            Constraint::Check { expression, .. } => {
                assert_eq!(expression, "amount > 0");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn parse_indexes() {
        let input = "\
table orders {
    id      bigint  NOT NULL
    status  text    NOT NULL

    INDEX orders_user_idx(id)
    UNIQUE INDEX orders_status_idx(status) WHERE status != 'completed'
}
";
        let schema = parse_schema(input).unwrap();
        let table = schema.table("orders").unwrap();
        assert_eq!(table.indexes.len(), 2);

        assert_eq!(table.indexes[0].name, "orders_user_idx");
        assert!(!table.indexes[0].unique);
        assert!(table.indexes[0].partial.is_none());

        assert_eq!(table.indexes[1].name, "orders_status_idx");
        assert!(table.indexes[1].unique);
        assert_eq!(
            table.indexes[1].partial.as_deref(),
            Some("WHERE status != 'completed'")
        );
    }

    #[test]
    fn parse_domain_type() {
        let input = "domain positive_int integer CHECK (VALUE > 0)\n";
        let schema = parse_schema(input).unwrap();
        let ct = schema.types.get("positive_int").unwrap();
        match &ct.kind {
            CustomTypeKind::Domain {
                base_type,
                constraints,
            } => {
                assert_eq!(*base_type, PgType::Integer);
                assert_eq!(constraints, &["CHECK (VALUE > 0)"]);
            }
            _ => panic!("expected Domain"),
        }
    }

    #[test]
    fn parse_composite_type() {
        let input = "\
composite address {
    street  text
    city    text
    zip     varchar(10)
}
";
        let schema = parse_schema(input).unwrap();
        let ct = schema.types.get("address").unwrap();
        match &ct.kind {
            CustomTypeKind::Composite { fields } => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "street");
                assert_eq!(fields[0].1, PgType::Text);
                assert_eq!(fields[2].0, "zip");
                assert_eq!(fields[2].1, PgType::Varchar(Some(10)));
            }
            _ => panic!("expected Composite"),
        }
    }

    #[test]
    fn parse_range_type() {
        let input = "range float_range double precision\n";
        let schema = parse_schema(input).unwrap();
        let ct = schema.types.get("float_range").unwrap();
        match &ct.kind {
            CustomTypeKind::Range { subtype } => {
                assert_eq!(*subtype, PgType::DoublePrecision);
            }
            _ => panic!("expected Range"),
        }
    }

    #[test]
    fn parse_array_types() {
        let input = "\
table t {
    tags  text[]
}
";
        let schema = parse_schema(input).unwrap();
        let col = schema.table("t").unwrap().column("tags").unwrap();
        assert_eq!(col.pg_type, PgType::Array(Box::new(PgType::Text)));
    }

    #[test]
    fn parse_all_pg_types() {
        // Verify each PgType variant can round-trip through Display → parse
        let types = vec![
            PgType::Boolean,
            PgType::SmallInt,
            PgType::Integer,
            PgType::BigInt,
            PgType::Real,
            PgType::DoublePrecision,
            PgType::Numeric(None),
            PgType::Numeric(Some((10, 0))),
            PgType::Numeric(Some((10, 2))),
            PgType::Text,
            PgType::Varchar(None),
            PgType::Varchar(Some(255)),
            PgType::Char(None),
            PgType::Char(Some(1)),
            PgType::Bytea,
            PgType::Uuid,
            PgType::Timestamp,
            PgType::Timestamptz,
            PgType::Date,
            PgType::Time,
            PgType::Timetz,
            PgType::Interval,
            PgType::Json,
            PgType::Jsonb,
            PgType::Array(Box::new(PgType::Text)),
            PgType::Array(Box::new(PgType::Array(Box::new(PgType::Integer)))),
            PgType::Custom("my_enum".into()),
        ];

        for ty in types {
            let rendered = ty.to_string();
            // Append a newline so the parser has a clear end
            let input_str = format!("{rendered}\n");
            let mut input = input_str.as_str();
            let parsed = pg_type(&mut input).unwrap_or_else(|e| {
                panic!("failed to parse type '{rendered}': {e:?}");
            });
            assert_eq!(parsed, ty, "type round-trip failed for '{rendered}'");
        }
    }

    #[test]
    fn parse_error_has_location() {
        let input = "table users {\n    id uuid NOT NULL\n    !!!\n}\n";
        let err = parse_schema(input).unwrap_err();
        assert!(err.line > 0);
        assert!(err.col > 0);
    }

    // ── Round-trip tests ────────────────────────────────────────

    #[test]
    fn roundtrip_simple_users() {
        let input = "\
table users {
    id          uuid         NOT NULL  DEFAULT gen_random_uuid()  PRIMARY KEY
    email       text         NOT NULL  UNIQUE
    created_at  timestamptz  NOT NULL  DEFAULT now()
}
";
        let schema = parse_schema(input).unwrap();
        let rendered = render_schema(&schema);
        let reparsed = parse_schema(&rendered).unwrap();
        let rerendered = render_schema(&reparsed);
        assert_eq!(rendered, rerendered);
    }

    #[test]
    fn roundtrip_complex_schema() {
        let input = "\
enum role {
    admin
    member
    guest
}

domain email_address text CHECK (VALUE ~ '^.+@.+$')

table team_members {
    team_id    uuid         NOT NULL
    user_id    uuid         NOT NULL
    joined_at  timestamptz  NOT NULL  DEFAULT now()

    PRIMARY KEY (team_id, user_id)
    FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    INDEX team_members_user_idx(user_id)
}

table teams {
    id          uuid         NOT NULL  PRIMARY KEY
    name        text         NOT NULL
    created_at  timestamptz  NOT NULL  DEFAULT now()
}

table users {
    id          uuid           NOT NULL  DEFAULT gen_random_uuid()  PRIMARY KEY
    email       email_address  NOT NULL  UNIQUE
    name        varchar(255)   NOT NULL
    role        role           NOT NULL
    tags        text[]
    metadata    jsonb
    created_at  timestamptz    NOT NULL  DEFAULT now()

    UNIQUE INDEX users_email_idx(email)
}
";
        let schema = parse_schema(input).unwrap();
        let rendered = render_schema(&schema);
        let reparsed = parse_schema(&rendered).unwrap();
        let rerendered = render_schema(&reparsed);
        assert_eq!(rendered, rerendered);
    }

    #[test]
    fn roundtrip_all_constraint_types() {
        let input = "\
table orders {
    id         bigint         NOT NULL  UNIQUE
    tenant_id  uuid           NOT NULL
    user_id    uuid           NOT NULL
    amount     numeric(10,2)  NOT NULL
    status     order_status   NOT NULL

    PRIMARY KEY (tenant_id, id)
    UNIQUE (tenant_id, user_id)
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL ON UPDATE CASCADE
    CHECK (amount > 0)
    INDEX orders_user_idx(user_id)
    INDEX orders_status_idx(status) WHERE status != 'completed'
}
";
        let schema = parse_schema(input).unwrap();
        let rendered = render_schema(&schema);
        let reparsed = parse_schema(&rendered).unwrap();
        let rerendered = render_schema(&reparsed);
        assert_eq!(rendered, rerendered);
    }

    #[test]
    fn roundtrip_enums_with_table() {
        let input = "\
enum mood {
    happy
    sad
    neutral
}

enum status {
    active
    inactive
    pending
}

table profiles {
    id      uuid    NOT NULL  PRIMARY KEY
    mood    mood
    status  status  NOT NULL
}
";
        let schema = parse_schema(input).unwrap();
        let rendered = render_schema(&schema);
        let reparsed = parse_schema(&rendered).unwrap();
        let rerendered = render_schema(&reparsed);
        assert_eq!(rendered, rerendered);
    }

    #[test]
    fn roundtrip_multi_table_with_fks() {
        let input = "\
table posts {
    id         uuid  NOT NULL  PRIMARY KEY
    author_id  uuid  NOT NULL
    title      text  NOT NULL
    body       text

    FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
    INDEX posts_author_idx(author_id)
}

table users {
    id    uuid  NOT NULL  PRIMARY KEY
    name  text  NOT NULL
}
";
        let schema = parse_schema(input).unwrap();
        let rendered = render_schema(&schema);
        let reparsed = parse_schema(&rendered).unwrap();
        let rerendered = render_schema(&reparsed);
        assert_eq!(rendered, rerendered);
    }

    #[test]
    fn roundtrip_custom_types() {
        let input = "\
composite address {
    street  text
    city    text
    zip     varchar(10)
}
";
        let schema = parse_schema(input).unwrap();
        let rendered = render_schema(&schema);
        let reparsed = parse_schema(&rendered).unwrap();
        let rerendered = render_schema(&reparsed);
        assert_eq!(rendered, rerendered);
    }

    #[test]
    fn parse_extra_whitespace() {
        // Extra whitespace and blank lines should be tolerated
        let input = "\n\n  table t {\n    id  uuid  NOT NULL\n  }\n\n";
        let schema = parse_schema(input).unwrap();
        assert!(schema.table("t").is_some());
    }

    #[test]
    fn parse_single_column_table() {
        let input = "table t {\n    id  uuid  NOT NULL  PRIMARY KEY\n}\n";
        let schema = parse_schema(input).unwrap();
        let table = schema.table("t").unwrap();
        assert_eq!(table.columns.len(), 1);
        assert!(table.primary_key().is_some());
    }

    // ── Proptest round-trip ─────────────────────────────────────

    #[cfg(test)]
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        fn arb_pg_type() -> impl Strategy<Value = PgType> {
            prop_oneof![
                Just(PgType::Boolean),
                Just(PgType::SmallInt),
                Just(PgType::Integer),
                Just(PgType::BigInt),
                Just(PgType::Real),
                Just(PgType::DoublePrecision),
                Just(PgType::Numeric(None)),
                (1u32..100, 0u32..20).prop_map(|(p, s)| PgType::Numeric(Some((p, s)))),
                Just(PgType::Text),
                Just(PgType::Varchar(None)),
                (1u32..1000).prop_map(|n| PgType::Varchar(Some(n))),
                Just(PgType::Char(None)),
                (1u32..255).prop_map(|n| PgType::Char(Some(n))),
                Just(PgType::Bytea),
                Just(PgType::Uuid),
                Just(PgType::Timestamp),
                Just(PgType::Timestamptz),
                Just(PgType::Date),
                Just(PgType::Time),
                Just(PgType::Timetz),
                Just(PgType::Interval),
                Just(PgType::Json),
                Just(PgType::Jsonb),
            ]
        }

        fn arb_identifier() -> impl Strategy<Value = String> {
            "[a-z][a-z0-9_]{1,15}".prop_filter("no reserved words", |s| {
                !matches!(
                    s.as_str(),
                    "table"
                        | "enum"
                        | "domain"
                        | "composite"
                        | "range"
                        | "boolean"
                        | "smallint"
                        | "integer"
                        | "bigint"
                        | "real"
                        | "double"
                        | "numeric"
                        | "text"
                        | "varchar"
                        | "char"
                        | "bytea"
                        | "uuid"
                        | "timestamp"
                        | "timestamptz"
                        | "date"
                        | "time"
                        | "timetz"
                        | "interval"
                        | "json"
                        | "jsonb"
                )
            })
        }

        fn arb_column() -> impl Strategy<Value = Column> {
            (arb_identifier(), arb_pg_type(), any::<bool>()).prop_map(
                |(name, pg_type, nullable)| Column {
                    name,
                    pg_type,
                    nullable,
                    default: None,
                },
            )
        }

        fn arb_table() -> impl Strategy<Value = Table> {
            (
                arb_identifier(),
                proptest::collection::vec(arb_column(), 1..6),
            )
                .prop_map(|(name, columns)| {
                    // Ensure unique column names
                    let mut seen = std::collections::HashSet::new();
                    let columns: Vec<Column> = columns
                        .into_iter()
                        .filter(|c| seen.insert(c.name.clone()))
                        .collect();
                    Table {
                        name,
                        columns,
                        constraints: vec![],
                        indexes: vec![],
                    }
                })
        }

        fn arb_enum() -> impl Strategy<Value = EnumType> {
            (
                arb_identifier(),
                proptest::collection::vec(arb_identifier(), 0..5),
            )
                .prop_map(|(name, variants)| {
                    // Ensure unique variants
                    let mut seen = std::collections::HashSet::new();
                    let variants: Vec<String> = variants
                        .into_iter()
                        .filter(|v| seen.insert(v.clone()))
                        .collect();
                    EnumType { name, variants }
                })
        }

        fn arb_schema() -> impl Strategy<Value = Schema> {
            (
                proptest::collection::vec(arb_table(), 0..4),
                proptest::collection::vec(arb_enum(), 0..3),
            )
                .prop_map(|(tables, enums)| {
                    let mut schema = Schema::new();
                    let mut seen_tables = std::collections::HashSet::new();
                    for t in tables {
                        if seen_tables.insert(t.name.clone()) {
                            schema.add_table(t);
                        }
                    }
                    let mut seen_enums = std::collections::HashSet::new();
                    for e in enums {
                        if seen_enums.insert(e.name.clone()) {
                            schema.add_enum(e);
                        }
                    }
                    schema
                })
        }

        proptest! {
            #[test]
            fn roundtrip_property(schema in arb_schema()) {
                let rendered = render_schema(&schema);
                let reparsed = parse_schema(&rendered).unwrap();
                let rerendered = render_schema(&reparsed);
                prop_assert_eq!(rendered, rerendered);
            }
        }
    }
}
