use std::{fmt, ops::ControlFlow};

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
        }
    }
}

impl std::error::Error for SqlValidationError {}

pub fn validate_read_only(sql: &str) -> Result<()> {
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
            validate_source_functions(statement)
        }
        other => Err(SqlValidationError::NotReadOnly(other.to_string()).into()),
    }
}

fn validate_source_functions(statement: &Statement) -> Result<()> {
    let mut visitor = SourceFunctionVisitor;
    match statement.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(function) => Err(SqlValidationError::SourceFunctionNotAllowed(function).into()),
    }
}

struct SourceFunctionVisitor;

impl Visitor for SourceFunctionVisitor {
    type Break = String;

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        let name = match table_factor {
            TableFactor::Table {
                name, args: Some(_), ..
            }
            | TableFactor::Function { name, .. } => Some(name),
            _ => None,
        };
        name.and_then(blocked_source_function)
            .map_or(ControlFlow::Continue(()), ControlFlow::Break)
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        blocked_source_function(&function.name).map_or(ControlFlow::Continue(()), ControlFlow::Break)
    }
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
}
