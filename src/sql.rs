use std::{collections::HashSet, fmt, ops::ControlFlow};

use anyhow::Result;
use sqlparser::{
    ast::{Expr, ObjectName, Statement, TableFactor, Visit, Visitor},
    dialect::DuckDbDialect,
    parser::Parser,
};

#[derive(Debug)]
pub enum SqlValidationError {
    Empty,
    Parse(String),
    StatementCount(usize),
    NotReadOnly(String),
    SourceFunctionNotAllowed(String),
    SourceRelationNotAllowed(String),
}

impl fmt::Display for SqlValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("SQL must not be empty"),
            Self::Parse(error) => write!(formatter, "SQL could not be parsed as DuckDB SQL: {error}"),
            Self::StatementCount(count) => {
                write!(
                    formatter,
                    "exactly one SQL statement is allowed; received {count}"
                )
            }
            Self::NotReadOnly(statement) => {
                write!(
                    formatter,
                    "only read-only query statements are allowed; got {statement}"
                )
            }
            Self::SourceFunctionNotAllowed(function) => write!(
                formatter,
                "source function '{function}' is not allowed; query registered backend tables or logical views instead"
            ),
            Self::SourceRelationNotAllowed(relation) => write!(
                formatter,
                "path-like source relation {relation} is not allowed; query registered backend tables or logical views instead"
            ),
        }
    }
}

impl std::error::Error for SqlValidationError {}

#[cfg(test)]
fn validate_read_only(sql: &str) -> Result<()> {
    validate_read_only_with_relations(sql, &HashSet::new())
}

pub fn validate_read_only_with_relations(sql: &str, allowed_relations: &HashSet<String>) -> Result<()> {
    if sql.trim().is_empty() {
        return Err(SqlValidationError::Empty.into());
    }
    let statements = Parser::parse_sql(&DuckDbDialect {}, sql)
        .map_err(|error| SqlValidationError::Parse(error.to_string()))?;
    if statements.len() != 1 {
        return Err(SqlValidationError::StatementCount(statements.len()).into());
    }
    match &statements[0] {
        statement @ (Statement::Query(_) | Statement::Explain { .. } | Statement::ExplainTable { .. }) => {
            validate_external_sources(statement, allowed_relations)
        }
        other => Err(SqlValidationError::NotReadOnly(other.to_string()).into()),
    }
}

fn validate_external_sources(statement: &Statement, allowed_relations: &HashSet<String>) -> Result<()> {
    let mut visitor = ExternalSourceVisitor { allowed_relations };
    match statement.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error.into()),
    }
}

struct ExternalSourceVisitor<'a> {
    allowed_relations: &'a HashSet<String>,
}

impl Visitor for ExternalSourceVisitor<'_> {
    type Break = SqlValidationError;

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        let function = match table_factor {
            TableFactor::Table {
                name, args: Some(_), ..
            }
            | TableFactor::Function { name, .. } => Some(name),
            _ => None,
        };
        if let Some(function) = function.and_then(blocked_source_function) {
            return ControlFlow::Break(SqlValidationError::SourceFunctionNotAllowed(function));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        blocked_source_relation(relation, self.allowed_relations)
            .map_or(ControlFlow::Continue(()), |relation| {
                ControlFlow::Break(SqlValidationError::SourceRelationNotAllowed(relation))
            })
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        blocked_source_function(&function.name).map_or(ControlFlow::Continue(()), |function| {
            ControlFlow::Break(SqlValidationError::SourceFunctionNotAllowed(function))
        })
    }
}

fn blocked_source_relation(name: &ObjectName, allowed_relations: &HashSet<String>) -> Option<String> {
    let Some(identifiers) = name
        .0
        .iter()
        .map(|part| part.as_ident())
        .collect::<Option<Vec<_>>>()
    else {
        return Some(name.to_string());
    };
    if identifiers.is_empty() {
        return None;
    }
    if identifiers.iter().any(|identifier| {
        identifier.quote_style == Some('\'')
            || identifier
                .value
                .bytes()
                .any(|byte| matches!(byte, b'/' | b'\\' | b'*' | b'?' | b'[' | b']'))
    }) {
        return Some(name.to_string());
    }
    let relation = identifiers
        .iter()
        .map(|identifier| identifier.value.as_str())
        .collect::<Vec<_>>()
        .join(".");
    if !looks_like_external_file(&relation) {
        return None;
    }
    (!allowed_relations.contains(&relation.to_ascii_lowercase())).then(|| name.to_string())
}

fn looks_like_external_file(relation: &str) -> bool {
    let lower = relation.to_ascii_lowercase();
    let without_compression = [".gz", ".zst", ".bz2", ".xz"]
        .iter()
        .find_map(|suffix| lower.strip_suffix(suffix))
        .unwrap_or(&lower);
    [
        ".parquet", ".csv", ".tsv", ".json", ".jsonl", ".ndjson", ".arrow", ".ipc", ".feather", ".avro",
        ".orc", ".xlsx", ".sqlite", ".sqlite3", ".duckdb", ".db",
    ]
    .iter()
    .any(|suffix| without_compression.ends_with(suffix))
}

fn blocked_source_function(name: &ObjectName) -> Option<String> {
    let function = name.0.last()?.as_ident()?.value.as_str();
    is_blocked_source_function(function).then(|| name.to_string())
}

fn is_blocked_source_function(function: &str) -> bool {
    let function = function.to_ascii_lowercase();
    function.starts_with("read_")
        || function.ends_with("_scan")
        || function.starts_with("parquet_")
        || matches!(
            function.as_str(),
            "glob" | "query" | "query_table" | "sniff_csv" | "sqlite_attach" | "sqlite_query"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_queries() {
        for sql in [
            "SELECT 42",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "VALUES (1), (2)",
            "EXPLAIN SELECT 42",
            "SELECT * FROM range(3)",
            "SELECT * FROM duckdb_tables()",
            "SELECT 'sqlite_query(''app'', ''SELECT 1'')' AS example",
        ] {
            assert!(validate_read_only(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn rejects_writes_and_multiple_statements() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE t(i int)",
            "COPY (SELECT 1) TO '/tmp/x'",
            "SELECT 1; DROP TABLE t",
            "ATTACH '/tmp/x.db' AS x",
            "SHOW '/tmp/data.parquet'",
            "SUMMARIZE '/tmp/data.parquet'",
        ] {
            assert!(validate_read_only(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn rejects_direct_and_nested_source_functions() {
        for sql in [
            "SELECT * FROM sqlite_query('app', 'SELECT * FROM events')",
            "SELECT * FROM SQLITE_SCAN('/tmp/app.sqlite', 'events')",
            "SELECT * FROM read_parquet('/tmp/data.parquet')",
            "SELECT * FROM parquet_metadata('/tmp/data.parquet')",
            "SELECT * FROM query('SELECT * FROM sqlite_query(''app'', ''SELECT 1'')')",
            "WITH x AS (SELECT * FROM glob('/tmp/*')) SELECT * FROM x",
            "EXPLAIN SELECT * FROM main.sqlite_query('app', 'SELECT 1')",
        ] {
            let error = validate_read_only(sql).unwrap_err();
            assert!(
                error
                    .downcast_ref::<SqlValidationError>()
                    .is_some_and(|error| matches!(error, SqlValidationError::SourceFunctionNotAllowed(_))),
                "{sql}: {error:#}"
            );
        }
    }

    #[test]
    fn rejects_path_like_source_relations() {
        for sql in [
            "SELECT * FROM '/tmp/data.parquet'",
            "SELECT * FROM \"/tmp/data.parquet\"",
            "SELECT * FROM 'relative-file'",
            "SELECT * FROM 'relative.parquet'",
            "SELECT * FROM \"relative.parquet\"",
            "SELECT * FROM relative.parquet",
            "SELECT * FROM relative.csv.gz",
            "SELECT * FROM 'https://example.test/data.parquet'",
            "SELECT * FROM 's3://bucket/data.parquet'",
            "WITH x AS (SELECT * FROM '../data/*.parquet') SELECT * FROM x",
            "EXPLAIN SELECT * FROM './data.json'",
            "DESCRIBE '/tmp/data.parquet'",
            "SELECT * FROM safe_table JOIN 'other.parquet' ON true",
        ] {
            let error = validate_read_only(sql).unwrap_err();
            assert!(
                error
                    .downcast_ref::<SqlValidationError>()
                    .is_some_and(|error| matches!(error, SqlValidationError::SourceRelationNotAllowed(_))),
                "{sql}: {error:#}"
            );
        }
    }

    #[test]
    fn permits_only_exact_file_like_names_of_registered_relations() {
        let relations = HashSet::from([
            "archive.parquet".to_owned(),
            "unified.csv".to_owned(),
            "warehouse.main.parquet".to_owned(),
        ]);
        for sql in [
            "SELECT * FROM archive.parquet",
            "SELECT * FROM \"archive\".\"parquet\"",
            "SELECT * FROM warehouse.main.parquet",
            "SELECT * FROM unified.csv",
            "DESCRIBE archive.parquet",
        ] {
            assert!(
                validate_read_only_with_relations(sql, &relations).is_ok(),
                "{sql}"
            );
        }
        for sql in [
            "SELECT * FROM archive.other.parquet",
            "SELECT * FROM warehouse.parquet",
        ] {
            let error = validate_read_only_with_relations(sql, &relations).unwrap_err();
            assert!(matches!(
                error.downcast_ref::<SqlValidationError>(),
                Some(SqlValidationError::SourceRelationNotAllowed(_))
            ));
        }
    }
}
