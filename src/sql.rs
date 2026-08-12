use anyhow::{Context, Result, bail};
use sqlparser::{ast::Statement, dialect::DuckDbDialect, parser::Parser};

pub fn validate_read_only(sql: &str) -> Result<()> {
    if sql.trim().is_empty() {
        bail!("SQL must not be empty");
    }
    let statements =
        Parser::parse_sql(&DuckDbDialect {}, sql).with_context(|| "SQL could not be parsed as DuckDB SQL")?;
    if statements.len() != 1 {
        bail!("exactly one SQL statement is allowed");
    }
    match &statements[0] {
        Statement::Query(_) | Statement::Explain { .. } | Statement::ExplainTable { .. } => Ok(()),
        other => bail!("only read-only query statements are allowed; got {other}"),
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
