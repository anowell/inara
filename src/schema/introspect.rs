use std::collections::BTreeMap;

use sqlx::PgPool;

use super::types::{Expression, ForeignKeyRef, PgType, ReferentialAction};
use super::{Column, Constraint, CustomType, CustomTypeKind, EnumType, Index, Schema, Table};

/// Errors that can occur during schema introspection.
#[derive(Debug, thiserror::Error)]
pub enum IntrospectError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Introspect the database schema for the given schema name.
///
/// Queries pg_catalog to build a complete `Schema` model including tables,
/// columns, constraints, indexes, enums, and custom types.
pub async fn introspect(pool: &PgPool, schema_name: &str) -> Result<Schema, IntrospectError> {
    let (tables, enums, types) = tokio::try_join!(
        load_tables(pool, schema_name),
        load_enums(pool, schema_name),
        load_custom_types(pool, schema_name),
    )?;

    Ok(Schema {
        tables,
        enums,
        types,
    })
}

// ── Row types for sqlx queries ────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct TableRow {
    table_name: String,
}

#[derive(sqlx::FromRow)]
struct ColumnRow {
    table_name: String,
    column_name: String,
    udt_name: String,
    data_type: String,
    is_nullable: String,
    column_default: Option<String>,
    character_maximum_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
    domain_name: Option<String>,
}

#[derive(sqlx::FromRow)]
struct ConstraintRow {
    table_name: String,
    constraint_name: String,
    constraint_type: String,
    columns: Vec<String>,
    foreign_table_name: Option<String>,
    foreign_columns: Option<Vec<String>>,
    check_clause: Option<String>,
    delete_rule: Option<String>,
    update_rule: Option<String>,
}

#[derive(sqlx::FromRow)]
struct IndexRow {
    table_name: String,
    index_name: String,
    columns: Vec<String>,
    is_unique: bool,
    filter_condition: Option<String>,
}

#[derive(sqlx::FromRow)]
struct EnumRow {
    enum_name: String,
    enum_value: String,
    #[allow(dead_code)]
    sort_order: f32,
}

#[derive(sqlx::FromRow)]
struct CompositeFieldRow {
    type_name: String,
    attribute_name: String,
    attribute_type: String,
}

#[derive(sqlx::FromRow)]
struct DomainRow {
    domain_name: String,
    data_type: String,
    udt_name: String,
    character_maximum_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
}

#[derive(sqlx::FromRow)]
struct DomainCheckRow {
    domain_name: String,
    check_clause: String,
}

// ── Table loading ────────────────────────────────────────────────────

async fn load_tables(
    pool: &PgPool,
    schema_name: &str,
) -> Result<BTreeMap<String, Table>, IntrospectError> {
    let table_rows = sqlx::query_as::<_, TableRow>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = $1
           AND table_type = 'BASE TABLE'
         ORDER BY table_name",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;

    let (columns, constraints, indexes) = tokio::try_join!(
        load_columns(pool, schema_name),
        load_constraints(pool, schema_name),
        load_indexes(pool, schema_name),
    )?;

    let mut tables = BTreeMap::new();
    for row in table_rows {
        let name = row.table_name;
        let table_columns = columns.get(&name).cloned().unwrap_or_default();
        let table_constraints = constraints.get(&name).cloned().unwrap_or_default();
        let table_indexes = indexes.get(&name).cloned().unwrap_or_default();

        tables.insert(
            name.clone(),
            Table {
                name,
                columns: table_columns,
                constraints: table_constraints,
                indexes: table_indexes,
            },
        );
    }

    Ok(tables)
}

// ── Column loading ──────────────────────────────────────────────────

async fn load_columns(
    pool: &PgPool,
    schema_name: &str,
) -> Result<BTreeMap<String, Vec<Column>>, IntrospectError> {
    let rows = sqlx::query_as::<_, ColumnRow>(
        "SELECT table_name, column_name, udt_name, data_type,
                is_nullable, column_default,
                character_maximum_length,
                numeric_precision, numeric_scale,
                domain_name
         FROM information_schema.columns
         WHERE table_schema = $1
         ORDER BY table_name, ordinal_position",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;

    let mut result: BTreeMap<String, Vec<Column>> = BTreeMap::new();
    for row in rows {
        // Domain columns: information_schema reports the base type, but we
        // want to map them to PgType::Custom(domain_name).
        let pg_type = if let Some(ref domain) = row.domain_name {
            PgType::Custom(domain.clone())
        } else {
            parse_pg_type(
                &row.data_type,
                &row.udt_name,
                row.character_maximum_length,
                row.numeric_precision,
                row.numeric_scale,
            )
        };
        let nullable = row.is_nullable == "YES";
        let default = row.column_default.as_deref().and_then(parse_default);

        result.entry(row.table_name).or_default().push(Column {
            name: row.column_name,
            pg_type,
            nullable,
            default,
        });
    }

    Ok(result)
}

// ── Constraint loading ──────────────────────────────────────────────

async fn load_constraints(
    pool: &PgPool,
    schema_name: &str,
) -> Result<BTreeMap<String, Vec<Constraint>>, IntrospectError> {
    let rows = sqlx::query_as::<_, ConstraintRow>(
        "SELECT
             c_table.relname AS table_name,
             con.conname AS constraint_name,
             CASE con.contype
                 WHEN 'p' THEN 'PRIMARY KEY'
                 WHEN 'u' THEN 'UNIQUE'
                 WHEN 'f' THEN 'FOREIGN KEY'
                 WHEN 'c' THEN 'CHECK'
             END AS constraint_type,
             -- Constrained columns in key-order
             ARRAY(
                 SELECT a.attname
                 FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
                 JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
                 ORDER BY k.ord
             ) AS columns,
             -- FK target table
             fk_table.relname AS foreign_table_name,
             -- FK target columns
             CASE WHEN con.contype = 'f' THEN
                 ARRAY(
                     SELECT a.attname
                     FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord)
                     JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
                     ORDER BY k.ord
                 )
             END AS foreign_columns,
             -- Check clause (from pg_constraint)
             CASE WHEN con.contype = 'c' THEN
                 pg_get_constraintdef(con.oid)
             END AS check_clause,
             -- Referential actions
             CASE con.confdeltype
                 WHEN 'a' THEN 'NO ACTION'
                 WHEN 'r' THEN 'RESTRICT'
                 WHEN 'c' THEN 'CASCADE'
                 WHEN 'n' THEN 'SET NULL'
                 WHEN 'd' THEN 'SET DEFAULT'
             END AS delete_rule,
             CASE con.confupdtype
                 WHEN 'a' THEN 'NO ACTION'
                 WHEN 'r' THEN 'RESTRICT'
                 WHEN 'c' THEN 'CASCADE'
                 WHEN 'n' THEN 'SET NULL'
                 WHEN 'd' THEN 'SET DEFAULT'
             END AS update_rule
         FROM pg_constraint con
         JOIN pg_class c_table ON c_table.oid = con.conrelid
         JOIN pg_namespace n ON n.oid = c_table.relnamespace
         LEFT JOIN pg_class fk_table ON fk_table.oid = con.confrelid
         WHERE n.nspname = $1
           AND con.contype IN ('p', 'u', 'f', 'c')
         ORDER BY c_table.relname, con.contype, con.conname",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;

    let mut result: BTreeMap<String, Vec<Constraint>> = BTreeMap::new();
    for row in rows {
        let constraint = match row.constraint_type.as_str() {
            "PRIMARY KEY" => Constraint::PrimaryKey {
                name: Some(row.constraint_name),
                columns: row.columns,
            },
            "UNIQUE" => Constraint::Unique {
                name: Some(row.constraint_name),
                columns: row.columns,
            },
            "FOREIGN KEY" => Constraint::ForeignKey {
                name: Some(row.constraint_name),
                columns: row.columns,
                references: ForeignKeyRef {
                    table: row.foreign_table_name.unwrap_or_default(),
                    columns: row.foreign_columns.unwrap_or_default(),
                },
                on_delete: row
                    .delete_rule
                    .as_deref()
                    .and_then(parse_referential_action),
                on_update: row
                    .update_rule
                    .as_deref()
                    .and_then(parse_referential_action),
            },
            "CHECK" => {
                let raw = row.check_clause.unwrap_or_default();
                // pg_get_constraintdef returns "CHECK ((expr))" — strip the prefix
                let expr = strip_check_prefix(&raw);
                // Skip internal NOT NULL checks that Postgres generates
                if is_not_null_check(expr) {
                    continue;
                }
                Constraint::Check {
                    name: Some(row.constraint_name),
                    expression: expr.to_string(),
                }
            }
            _ => continue,
        };

        result.entry(row.table_name).or_default().push(constraint);
    }

    Ok(result)
}

/// Strip the `CHECK (...)` prefix from `pg_get_constraintdef` output.
///
/// Returns the inner expression. E.g., `CHECK ((age > 0))` → `(age > 0)`.
fn strip_check_prefix(s: &str) -> &str {
    let trimmed = s.trim();
    if let Some(inner) = trimmed.strip_prefix("CHECK (") {
        if let Some(expr) = inner.strip_suffix(')') {
            return expr;
        }
    }
    trimmed
}

/// Detect Postgres-generated NOT NULL check constraints.
///
/// These have the form `column_name IS NOT NULL` and should be excluded
/// since nullability is tracked on the Column itself.
fn is_not_null_check(expr: &str) -> bool {
    let trimmed = expr.trim();
    trimmed.ends_with("IS NOT NULL")
        && !trimmed.contains("AND")
        && !trimmed.contains("OR")
        && !trimmed.contains(',')
}

// ── Index loading ───────────────────────────────────────────────────

async fn load_indexes(
    pool: &PgPool,
    schema_name: &str,
) -> Result<BTreeMap<String, Vec<Index>>, IntrospectError> {
    let rows = sqlx::query_as::<_, IndexRow>(
        "SELECT
             c_table.relname AS table_name,
             c_index.relname AS index_name,
             ARRAY(
                 SELECT a.attname
                 FROM pg_attribute a
                 WHERE a.attrelid = c_index.oid
                   AND a.attnum > 0
                 ORDER BY a.attnum
             ) AS columns,
             ix.indisunique AS is_unique,
             pg_get_expr(ix.indpred, ix.indrelid) AS filter_condition
         FROM pg_index ix
         JOIN pg_class c_index ON c_index.oid = ix.indexrelid
         JOIN pg_class c_table ON c_table.oid = ix.indrelid
         JOIN pg_namespace n ON n.oid = c_table.relnamespace
         WHERE n.nspname = $1
           AND NOT ix.indisprimary
           AND NOT ix.indisexclusion
           -- Exclude unique indexes that back unique constraints
           AND NOT EXISTS (
               SELECT 1
               FROM pg_constraint con
               WHERE con.conindid = c_index.oid
                 AND con.contype = 'u'
           )
         ORDER BY c_table.relname, c_index.relname",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;

    let mut result: BTreeMap<String, Vec<Index>> = BTreeMap::new();
    for row in rows {
        result.entry(row.table_name).or_default().push(Index {
            name: row.index_name,
            columns: row.columns,
            unique: row.is_unique,
            partial: row.filter_condition,
        });
    }

    Ok(result)
}

// ── Enum loading ────────────────────────────────────────────────────

async fn load_enums(
    pool: &PgPool,
    schema_name: &str,
) -> Result<BTreeMap<String, EnumType>, IntrospectError> {
    let rows = sqlx::query_as::<_, EnumRow>(
        "SELECT
             t.typname AS enum_name,
             e.enumlabel AS enum_value,
             e.enumsortorder AS sort_order
         FROM pg_type t
         JOIN pg_enum e ON e.enumtypid = t.oid
         JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE n.nspname = $1
         ORDER BY t.typname, e.enumsortorder",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;

    let mut result: BTreeMap<String, EnumType> = BTreeMap::new();
    for row in rows {
        result
            .entry(row.enum_name.clone())
            .or_insert_with(|| EnumType {
                name: row.enum_name,
                variants: Vec::new(),
            })
            .variants
            .push(row.enum_value);
    }

    Ok(result)
}

// ── Custom type loading ─────────────────────────────────────────────

async fn load_custom_types(
    pool: &PgPool,
    schema_name: &str,
) -> Result<BTreeMap<String, CustomType>, IntrospectError> {
    let (composites, domains, domain_checks) = tokio::try_join!(
        load_composite_types(pool, schema_name),
        load_domains(pool, schema_name),
        load_domain_checks(pool, schema_name),
    )?;

    let mut result: BTreeMap<String, CustomType> = BTreeMap::new();

    // Composite types
    let mut composite_fields: BTreeMap<String, Vec<(String, PgType)>> = BTreeMap::new();
    for row in composites {
        let pg_type = parse_type_name(&row.attribute_type);
        composite_fields
            .entry(row.type_name.clone())
            .or_default()
            .push((row.attribute_name, pg_type));
    }
    for (name, fields) in composite_fields {
        result.insert(
            name.clone(),
            CustomType {
                name,
                kind: CustomTypeKind::Composite { fields },
            },
        );
    }

    // Domain types
    let mut domain_constraint_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in domain_checks {
        domain_constraint_map
            .entry(row.domain_name)
            .or_default()
            .push(row.check_clause);
    }
    for row in domains {
        let base_type = parse_pg_type(
            &row.data_type,
            &row.udt_name,
            row.character_maximum_length,
            row.numeric_precision,
            row.numeric_scale,
        );
        let constraints = domain_constraint_map
            .remove(&row.domain_name)
            .unwrap_or_default();
        result.insert(
            row.domain_name.clone(),
            CustomType {
                name: row.domain_name,
                kind: CustomTypeKind::Domain {
                    base_type,
                    constraints,
                },
            },
        );
    }

    Ok(result)
}

async fn load_composite_types(
    pool: &PgPool,
    schema_name: &str,
) -> Result<Vec<CompositeFieldRow>, IntrospectError> {
    let rows = sqlx::query_as::<_, CompositeFieldRow>(
        "SELECT
             t.typname AS type_name,
             a.attname AS attribute_name,
             format_type(a.atttypid, a.atttypmod) AS attribute_type
         FROM pg_type t
         JOIN pg_namespace n ON n.oid = t.typnamespace
         JOIN pg_class c ON c.oid = t.typrelid
         JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
         WHERE n.nspname = $1
           AND t.typtype = 'c'
           -- Exclude composite types that are implicit row types for tables
           AND NOT EXISTS (
               SELECT 1
               FROM pg_class tbl
               WHERE tbl.oid = t.typrelid AND tbl.relkind IN ('r', 'v', 'm', 'f', 'p')
           )
         ORDER BY t.typname, a.attnum",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

async fn load_domains(pool: &PgPool, schema_name: &str) -> Result<Vec<DomainRow>, IntrospectError> {
    let rows = sqlx::query_as::<_, DomainRow>(
        "SELECT
             domain_name,
             data_type,
             udt_name,
             character_maximum_length,
             numeric_precision,
             numeric_scale
         FROM information_schema.domains
         WHERE domain_schema = $1
         ORDER BY domain_name",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

async fn load_domain_checks(
    pool: &PgPool,
    schema_name: &str,
) -> Result<Vec<DomainCheckRow>, IntrospectError> {
    let rows = sqlx::query_as::<_, DomainCheckRow>(
        "SELECT
             dc.domain_name,
             cc.check_clause
         FROM information_schema.domain_constraints dc
         JOIN information_schema.check_constraints cc
             ON cc.constraint_name = dc.constraint_name
            AND cc.constraint_schema = dc.constraint_schema
         WHERE dc.domain_schema = $1
         ORDER BY dc.domain_name, dc.constraint_name",
    )
    .bind(schema_name)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

// ── Type parsing helpers ────────────────────────────────────────────

/// Map information_schema data_type + udt_name to our PgType.
fn parse_pg_type(
    data_type: &str,
    udt_name: &str,
    char_max_len: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
) -> PgType {
    match data_type {
        "boolean" => PgType::Boolean,
        "smallint" => PgType::SmallInt,
        "integer" => PgType::Integer,
        "bigint" => PgType::BigInt,
        "real" => PgType::Real,
        "double precision" => PgType::DoublePrecision,
        "numeric" => {
            let params = match (numeric_precision, numeric_scale) {
                (Some(p), Some(s)) => Some((p as u32, s as u32)),
                _ => None,
            };
            PgType::Numeric(params)
        }
        "text" => PgType::Text,
        "character varying" => PgType::Varchar(char_max_len.map(|n| n as u32)),
        "character" => PgType::Char(char_max_len.map(|n| n as u32)),
        "bytea" => PgType::Bytea,
        "uuid" => PgType::Uuid,
        "timestamp without time zone" => PgType::Timestamp,
        "timestamp with time zone" => PgType::Timestamptz,
        "date" => PgType::Date,
        "time without time zone" => PgType::Time,
        "time with time zone" => PgType::Timetz,
        "interval" => PgType::Interval,
        "json" => PgType::Json,
        "jsonb" => PgType::Jsonb,
        "ARRAY" => {
            // udt_name for arrays is _elementtype (e.g., "_text" for text[])
            let element_type = udt_name.strip_prefix('_').unwrap_or(udt_name);
            let inner = parse_type_name(element_type);
            PgType::Array(Box::new(inner))
        }
        "USER-DEFINED" => PgType::Custom(udt_name.to_string()),
        _ => PgType::Custom(udt_name.to_string()),
    }
}

/// Parse a type name string (from pg_catalog format_type or udt_name) into PgType.
fn parse_type_name(name: &str) -> PgType {
    match name {
        "bool" | "boolean" => PgType::Boolean,
        "int2" | "smallint" => PgType::SmallInt,
        "int4" | "integer" | "int" => PgType::Integer,
        "int8" | "bigint" => PgType::BigInt,
        "float4" | "real" => PgType::Real,
        "float8" | "double precision" => PgType::DoublePrecision,
        "numeric" => PgType::Numeric(None),
        "text" => PgType::Text,
        "varchar" | "character varying" => PgType::Varchar(None),
        "bpchar" | "char" | "character" => PgType::Char(None),
        "bytea" => PgType::Bytea,
        "uuid" => PgType::Uuid,
        "timestamp" | "timestamp without time zone" => PgType::Timestamp,
        "timestamptz" | "timestamp with time zone" => PgType::Timestamptz,
        "date" => PgType::Date,
        "time" | "time without time zone" => PgType::Time,
        "timetz" | "time with time zone" => PgType::Timetz,
        "interval" => PgType::Interval,
        "json" => PgType::Json,
        "jsonb" => PgType::Jsonb,
        other => {
            // Handle varchar(N) and character varying(N) from format_type
            if let Some(inner) = other
                .strip_prefix("character varying(")
                .and_then(|s| s.strip_suffix(')'))
            {
                return PgType::Varchar(inner.parse().ok().map(|n: u32| n));
            }
            if let Some(inner) = other
                .strip_prefix("character(")
                .and_then(|s| s.strip_suffix(')'))
            {
                return PgType::Char(inner.parse().ok().map(|n: u32| n));
            }
            if let Some(inner) = other
                .strip_prefix("numeric(")
                .and_then(|s| s.strip_suffix(')'))
            {
                if let Some((p, s)) = inner.split_once(',') {
                    if let (Ok(p), Ok(s)) = (p.parse::<u32>(), s.parse::<u32>()) {
                        return PgType::Numeric(Some((p, s)));
                    }
                } else if let Ok(p) = inner.parse::<u32>() {
                    return PgType::Numeric(Some((p, 0)));
                }
            }
            // Array notation
            if let Some(element) = other.strip_suffix("[]") {
                return PgType::Array(Box::new(parse_type_name(element)));
            }
            PgType::Custom(other.to_string())
        }
    }
}

/// Parse a column default expression string from Postgres into our Expression type.
fn parse_default(raw: &str) -> Option<Expression> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Strip type cast suffixes like ::text, ::character varying, etc.
    // Postgres often appends these to defaults.
    let without_cast = strip_trailing_cast(trimmed);

    // Function calls: contain parens and don't start with a quote
    if without_cast.contains('(') && without_cast.contains(')') && !without_cast.starts_with('\'') {
        return Some(Expression::FunctionCall(without_cast.to_string()));
    }

    // String literals: 'value'
    if without_cast.starts_with('\'') && without_cast.ends_with('\'') {
        return Some(Expression::Literal(without_cast.to_string()));
    }

    // Numeric literals
    if without_cast
        .chars()
        .all(|c| c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Some(Expression::Literal(without_cast.to_string()));
    }

    // Boolean literals
    if without_cast == "true" || without_cast == "false" {
        return Some(Expression::Literal(without_cast.to_string()));
    }

    // Fallback: raw expression
    Some(Expression::Raw(without_cast.to_string()))
}

/// Strip trailing type casts from a Postgres default expression.
///
/// E.g., `'active'::status` → `'active'`, `'{}'::text[]` → `'{}'`
fn strip_trailing_cast(expr: &str) -> &str {
    // Handle casts like 'value'::type or value::type
    // We need to be careful not to strip :: inside function calls
    if let Some(pos) = expr.rfind("::") {
        let before = &expr[..pos];
        // Only strip if the cast is at the end (not inside parens)
        let after = &expr[pos + 2..];
        if !after.contains('(') {
            return before;
        }
    }
    expr
}

/// Parse a referential action string from information_schema.
fn parse_referential_action(action: &str) -> Option<ReferentialAction> {
    match action {
        "CASCADE" => Some(ReferentialAction::Cascade),
        "SET NULL" => Some(ReferentialAction::SetNull),
        "SET DEFAULT" => Some(ReferentialAction::SetDefault),
        "RESTRICT" => Some(ReferentialAction::Restrict),
        "NO ACTION" => Some(ReferentialAction::NoAction),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_type_common_types() {
        assert_eq!(
            parse_pg_type("boolean", "bool", None, None, None),
            PgType::Boolean
        );
        assert_eq!(
            parse_pg_type("integer", "int4", None, None, None),
            PgType::Integer
        );
        assert_eq!(
            parse_pg_type("bigint", "int8", None, None, None),
            PgType::BigInt
        );
        assert_eq!(
            parse_pg_type("text", "text", None, None, None),
            PgType::Text
        );
        assert_eq!(
            parse_pg_type("uuid", "uuid", None, None, None),
            PgType::Uuid
        );
        assert_eq!(
            parse_pg_type("timestamp with time zone", "timestamptz", None, None, None),
            PgType::Timestamptz
        );
        assert_eq!(
            parse_pg_type("jsonb", "jsonb", None, None, None),
            PgType::Jsonb
        );
    }

    #[test]
    fn parse_pg_type_varchar_with_length() {
        assert_eq!(
            parse_pg_type("character varying", "varchar", Some(255), None, None),
            PgType::Varchar(Some(255))
        );
    }

    #[test]
    fn parse_pg_type_varchar_without_length() {
        assert_eq!(
            parse_pg_type("character varying", "varchar", None, None, None),
            PgType::Varchar(None)
        );
    }

    #[test]
    fn parse_pg_type_numeric_with_precision() {
        assert_eq!(
            parse_pg_type("numeric", "numeric", None, Some(5), Some(2)),
            PgType::Numeric(Some((5, 2)))
        );
    }

    #[test]
    fn parse_pg_type_numeric_without_precision() {
        assert_eq!(
            parse_pg_type("numeric", "numeric", None, None, None),
            PgType::Numeric(None)
        );
    }

    #[test]
    fn parse_pg_type_array() {
        assert_eq!(
            parse_pg_type("ARRAY", "_text", None, None, None),
            PgType::Array(Box::new(PgType::Text))
        );
        assert_eq!(
            parse_pg_type("ARRAY", "_int4", None, None, None),
            PgType::Array(Box::new(PgType::Integer))
        );
    }

    #[test]
    fn parse_pg_type_user_defined() {
        assert_eq!(
            parse_pg_type("USER-DEFINED", "status", None, None, None),
            PgType::Custom("status".into())
        );
    }

    #[test]
    fn parse_type_name_common() {
        assert_eq!(parse_type_name("text"), PgType::Text);
        assert_eq!(parse_type_name("int4"), PgType::Integer);
        assert_eq!(parse_type_name("bool"), PgType::Boolean);
        assert_eq!(parse_type_name("timestamptz"), PgType::Timestamptz);
    }

    #[test]
    fn parse_type_name_varchar_with_length() {
        assert_eq!(
            parse_type_name("character varying(10)"),
            PgType::Varchar(Some(10))
        );
    }

    #[test]
    fn parse_type_name_custom() {
        assert_eq!(parse_type_name("mood"), PgType::Custom("mood".into()));
    }

    #[test]
    fn parse_default_function_call() {
        assert_eq!(
            parse_default("now()"),
            Some(Expression::FunctionCall("now()".into()))
        );
        assert_eq!(
            parse_default("gen_random_uuid()"),
            Some(Expression::FunctionCall("gen_random_uuid()".into()))
        );
    }

    #[test]
    fn parse_default_literal_string() {
        assert_eq!(
            parse_default("'hello'::text"),
            Some(Expression::Literal("'hello'".into()))
        );
    }

    #[test]
    fn parse_default_literal_number() {
        assert_eq!(
            parse_default("0.00"),
            Some(Expression::Literal("0.00".into()))
        );
        assert_eq!(parse_default("42"), Some(Expression::Literal("42".into())));
    }

    #[test]
    fn parse_default_boolean() {
        assert_eq!(
            parse_default("true"),
            Some(Expression::Literal("true".into()))
        );
        assert_eq!(
            parse_default("false"),
            Some(Expression::Literal("false".into()))
        );
    }

    #[test]
    fn parse_default_with_cast() {
        assert_eq!(
            parse_default("'active'::status"),
            Some(Expression::Literal("'active'".into()))
        );
    }

    #[test]
    fn parse_default_empty() {
        assert_eq!(parse_default(""), None);
        assert_eq!(parse_default("  "), None);
    }

    #[test]
    fn parse_referential_action_all() {
        assert_eq!(
            parse_referential_action("CASCADE"),
            Some(ReferentialAction::Cascade)
        );
        assert_eq!(
            parse_referential_action("SET NULL"),
            Some(ReferentialAction::SetNull)
        );
        assert_eq!(
            parse_referential_action("SET DEFAULT"),
            Some(ReferentialAction::SetDefault)
        );
        assert_eq!(
            parse_referential_action("RESTRICT"),
            Some(ReferentialAction::Restrict)
        );
        assert_eq!(
            parse_referential_action("NO ACTION"),
            Some(ReferentialAction::NoAction)
        );
        assert_eq!(parse_referential_action("UNKNOWN"), None);
    }

    #[test]
    fn is_not_null_check_positive() {
        assert!(is_not_null_check("name IS NOT NULL"));
        assert!(is_not_null_check("  age IS NOT NULL  "));
    }

    #[test]
    fn is_not_null_check_negative() {
        assert!(!is_not_null_check("age > 0 AND age < 200"));
        assert!(!is_not_null_check("a IS NOT NULL AND b IS NOT NULL"));
    }

    #[test]
    fn strip_trailing_cast_basic() {
        assert_eq!(strip_trailing_cast("'hello'::text"), "'hello'");
        assert_eq!(strip_trailing_cast("42::integer"), "42");
        assert_eq!(strip_trailing_cast("now()"), "now()");
        assert_eq!(strip_trailing_cast("'{}'::text[]"), "'{}'");
    }
}
