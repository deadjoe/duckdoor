use std::fmt;

use anyhow::Result;
use sqlparser::{ast::Statement, dialect::DuckDbDialect, parser::Parser};

#[derive(Debug)]
pub enum SqlValidationError {
    Empty,
    Parse(String),
    StatementCount(usize),
    NotReadOnly(String),
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
        Statement::Query(_) | Statement::Explain { .. } | Statement::ExplainTable { .. } => Ok(()),
        other => Err(SqlValidationError::NotReadOnly(other.to_string()).into()),
    }
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
}
