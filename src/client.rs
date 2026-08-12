use std::{
    fs,
    io::{self, Read},
    time::Duration,
};

use anyhow::{Context, Result, bail};

use crate::{
    config::{Paths, load_config},
    engine::QueryResult,
    server::QueryRequest,
};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Jsonl,
    Csv,
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
        .context("could not reach duckdoor; run `duckdoor start`")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().unwrap_or_default();
        bail!("query failed ({status}): {body}");
    }
    response
        .json()
        .context("daemon returned an invalid query response")
}

pub fn print_result(result: &QueryResult, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(result)?),
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
    if result.truncated {
        eprintln!("warning: result truncated at {} rows", result.row_count);
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
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width + 2))
            .collect::<Vec<_>>()
            .join("+")
    );
    for row in &result.rows {
        print_row(&row.iter().map(display_value).collect::<Vec<_>>(), &widths);
    }
    println!("({} rows, {:.3} ms)", result.row_count, result.elapsed_ms);
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
