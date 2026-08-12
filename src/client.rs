use std::{
    fs,
    io::{self, Read},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    config::{Paths, load_config},
    engine::QueryResult,
    output,
    server::QueryRequest,
};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum OutputFormat {
    Json,
    Jsonl,
    Csv,
    Table,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
    #[serde(default)]
    details: Value,
}

pub fn read_sql(argument: Option<String>, file: Option<std::path::PathBuf>) -> Result<String> {
    match (argument, file) {
        (Some(_), Some(_)) => {
            bail!("provide SQL as an argument, with --file, or on stdin; not both")
        }
        (Some(sql), None) => Ok(sql),
        (None, Some(path)) => {
            fs::read_to_string(&path).with_context(|| format!("could not read {}", path.display()))
        }
        (None, None) => {
            let mut sql = String::new();
            io::stdin().read_to_string(&mut sql)?;
            Ok(sql)
        }
    }
}

pub fn query(paths: &Paths, sql: String) -> Result<QueryResult> {
    let config = load_config(paths)?;
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_seconds + 5))
        .build()?
        .post(format!("http://{}/v1/query", config.listen))
        .json(&QueryRequest { sql })
        .send()
        .map_err(|error| {
            output::CommandError::new(
                "daemon_unreachable",
                "could not reach the duckdoor daemon",
                json!({
                    "listen": config.listen,
                    "cause": error.to_string(),
                    "resolution": "run `duckdoor status`; start the daemon if it is stopped",
                }),
            )
        })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().unwrap_or_default();
        if let Some(error) = parse_structured_error(&body) {
            let mut details = error.details;
            if !details.is_object() {
                details = json!({});
            }
            details["http_status"] = json!(status.as_u16());
            return Err(output::CommandError::new(error.code, error.message, details).into());
        }
        return Err(output::CommandError::new(
            "invalid_daemon_response",
            "daemon returned an unstructured error response",
            json!({ "http_status": status.as_u16(), "body": body }),
        )
        .into());
    }
    response.json().map_err(|error| {
        output::CommandError::new(
            "invalid_daemon_response",
            "daemon returned an invalid success response",
            json!({ "cause": error.to_string() }),
        )
        .into()
    })
}

fn parse_structured_error(body: &str) -> Option<ErrorBody> {
    serde_json::from_str::<ErrorEnvelope>(body)
        .ok()
        .map(|envelope| envelope.error)
}

pub fn print_result(result: &QueryResult, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => output::success("query", result)?,
        OutputFormat::Jsonl => {
            for row in &result.rows {
                let object = result
                    .columns
                    .iter()
                    .zip(row)
                    .map(|(column, value)| (column.name.clone(), value.clone()));
                println!(
                    "{}",
                    serde_json::to_string(&object.collect::<serde_json::Map<_, _>>())?
                );
            }
        }
        OutputFormat::Csv => {
            let mut writer = csv::Writer::from_writer(io::stdout());
            writer.write_record(result.columns.iter().map(|column| &column.name))?;
            for row in &result.rows {
                writer.write_record(row.iter().map(display_value))?;
            }
            writer.flush()?;
        }
        OutputFormat::Table => print_table(result),
    }
    if result.truncated && !matches!(format, OutputFormat::Json) {
        output::warning(
            "result_truncated",
            "query result reached the configured row limit",
            serde_json::json!({ "row_count": result.row_count }),
        );
    }
    Ok(())
}

fn print_table(result: &QueryResult) {
    if result.columns.is_empty() {
        println!("(no columns)");
        return;
    }
    let mut widths = result
        .columns
        .iter()
        .map(|column| column.name.len())
        .collect::<Vec<_>>();
    for row in &result.rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(display_value(value).chars().count()).min(80);
        }
    }
    print_row(
        &result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>(),
        &widths,
    );
    for row in &result.rows {
        print_row(&row.iter().map(display_value).collect::<Vec<_>>(), &widths);
    }
    println!("rows={} elapsed_ms={:.3}", result.row_count, result.elapsed_ms);
}

fn print_row(values: &[String], widths: &[usize]) {
    let cells = values.iter().zip(widths).map(|(value, width)| {
        let mut value = value.replace(['\n', '\r'], " ");
        if value.chars().count() > *width {
            value = value.chars().take(width.saturating_sub(1)).collect::<String>() + "…";
        }
        format!(" {value:width$} ")
    });
    println!("{}", cells.collect::<Vec<_>>().join("|"));
}

fn display_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_owned(),
        serde_json::Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_structured_http_errors() {
        let error = parse_structured_error(
            r#"{"ok":false,"error":{"code":"query_not_read_only","message":"write rejected"}}"#,
        )
        .unwrap();
        assert_eq!(error.code, "query_not_read_only");
        assert_eq!(error.message, "write rejected");
        assert!(parse_structured_error(r#"{"error":"legacy message"}"#).is_none());
    }
}
