use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

use anyhow::{Context, Result, anyhow, bail};
use arc_swap::ArcSwap;
use base64::Engine as _;
use duckdb::{Connection, types::Value};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value as JsonValue, json};
use sqlparser::{ast::Statement, dialect::DuckDbDialect, parser::Parser};
use tokio::sync::oneshot;

use crate::{
    config::{Backend, Config, Paths, load_config},
    sql::validate_read_only,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<JsonValue>>,
    pub row_count: usize,
    pub truncated: bool,
    pub elapsed_ms: f64,
}

enum WorkerMessage {
    Query {
        sql: String,
        response: oneshot::Sender<Result<QueryResult, String>>,
    },
    Stop,
}

pub struct QueryPool {
    senders: Vec<mpsc::SyncSender<WorkerMessage>>,
    next: AtomicUsize,
    workers: usize,
    enabled_backends: usize,
}

impl QueryPool {
    pub fn new(config: &Config, init_sql: &str) -> Result<Self> {
        if config.backends.iter().any(|backend| backend.enabled) {
            let installer = Connection::open_in_memory()?;
            load_sqlite_extension(&installer, true)?;
        }
        let mut senders = Vec::with_capacity(config.workers);
        let mut started = Vec::with_capacity(config.workers);
        for worker_id in 0..config.workers {
            let (sender, receiver) = mpsc::sync_channel(32);
            let (ready_tx, ready_rx) = mpsc::sync_channel(1);
            let backends = config
                .backends
                .iter()
                .filter(|backend| backend.enabled)
                .cloned()
                .collect::<Vec<_>>();
            let init_sql = init_sql.to_owned();
            let max_rows = config.max_rows;
            let threads_per_worker = config.threads_per_worker;
            thread::Builder::new()
                .name(format!("duckdoor-query-{worker_id}"))
                .spawn(move || {
                    worker_main(
                        &receiver,
                        &ready_tx,
                        &backends,
                        &init_sql,
                        max_rows,
                        threads_per_worker,
                    );
                })
                .context("could not start query worker")?;
            senders.push(sender);
            started.push(ready_rx);
        }
        for ready in started {
            match ready.recv().context("query worker exited during startup")? {
                Ok(()) => {}
                Err(error) => {
                    for sender in &senders {
                        let _ = sender.send(WorkerMessage::Stop);
                    }
                    bail!(error);
                }
            }
        }
        Ok(Self {
            senders,
            next: AtomicUsize::new(0),
            workers: config.workers,
            enabled_backends: config.backends.iter().filter(|backend| backend.enabled).count(),
        })
    }

    pub async fn query(&self, sql: String) -> Result<QueryResult> {
        validate_read_only(&sql)?;
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        let (response_tx, response_rx) = oneshot::channel();
        self.senders[index]
            .try_send(WorkerMessage::Query {
                sql,
                response: response_tx,
            })
            .map_err(|error| anyhow!("query queue is full or unavailable: {error}"))?;
        response_rx
            .await
            .context("query worker stopped unexpectedly")?
            .map_err(anyhow::Error::msg)
    }

    pub fn workers(&self) -> usize {
        self.workers
    }
    pub fn enabled_backends(&self) -> usize {
        self.enabled_backends
    }
}

impl Drop for QueryPool {
    fn drop(&mut self) {
        for sender in &self.senders {
            let _ = sender.try_send(WorkerMessage::Stop);
        }
    }
}

pub struct Engine {
    pool: ArcSwap<QueryPool>,
    paths: Paths,
}

impl Engine {
    pub fn load(paths: Paths) -> Result<Self> {
        let config = load_config(&paths)?;
        let init_sql = fs::read_to_string(&paths.init_sql)?;
        let pool = QueryPool::new(&config, &init_sql)?;
        Ok(Self {
            pool: ArcSwap::from_pointee(pool),
            paths,
        })
    }

    pub async fn query(&self, sql: String) -> Result<QueryResult> {
        let pool = self.pool.load_full();
        pool.query(sql).await
    }

    pub fn reload(&self) -> Result<(usize, usize)> {
        let config = load_config(&self.paths)?;
        let init_sql = fs::read_to_string(&self.paths.init_sql)?;
        let pool = Arc::new(QueryPool::new(&config, &init_sql)?);
        let stats = (pool.workers(), pool.enabled_backends());
        self.pool.store(pool);
        Ok(stats)
    }

    pub fn stats(&self) -> (usize, usize) {
        let pool = self.pool.load();
        (pool.workers(), pool.enabled_backends())
    }
}

pub fn test_backend(backend: &Backend) -> Result<()> {
    if !backend.path.is_file() {
        bail!("SQLite file does not exist: {}", backend.path.display());
    }
    let connection = Connection::open_in_memory()?;
    load_sqlite_extension(&connection, true)?;
    attach(&connection, backend)?;
    connection.query_row(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = ?",
        [&backend.name],
        |_row| Ok(()),
    )?;
    Ok(())
}

fn worker_main(
    receiver: &mpsc::Receiver<WorkerMessage>,
    ready: &mpsc::SyncSender<Result<(), String>>,
    backends: &[Backend],
    init_sql: &str,
    max_rows: usize,
    threads_per_worker: usize,
) {
    let connection = initialize_connection(backends, init_sql, threads_per_worker);
    let connection = match connection {
        Ok(connection) => {
            let _ = ready.send(Ok(()));
            connection
        }
        Err(error) => {
            let _ = ready.send(Err(format!("{error:#}")));
            return;
        }
    };
    while let Ok(message) = receiver.recv() {
        match message {
            WorkerMessage::Query { sql, response } => {
                let result = run_query(&connection, &sql, max_rows).map_err(|error| format!("{error:#}"));
                let _ = response.send(result);
            }
            WorkerMessage::Stop => break,
        }
    }
}

fn initialize_connection(
    backends: &[Backend],
    init_sql: &str,
    threads_per_worker: usize,
) -> Result<Connection> {
    let connection = Connection::open_in_memory()?;
    if !backends.is_empty() {
        load_sqlite_extension(&connection, false)?;
        for backend in backends {
            attach(&connection, backend)?;
        }
    }
    if !init_sql.trim().is_empty() && !only_comments(init_sql) {
        validate_init_sql(init_sql)?;
        connection.execute_batch(init_sql).context("init.sql failed")?;
    }
    harden_connection(&connection, backends, threads_per_worker)?;
    Ok(connection)
}

fn load_sqlite_extension(connection: &Connection, install: bool) -> Result<()> {
    connection
        .execute_batch(if install {
            "INSTALL sqlite; LOAD sqlite;"
        } else {
            "LOAD sqlite;"
        })
        .context("could not install/load DuckDB's sqlite extension")
}

fn attach(connection: &Connection, backend: &Backend) -> Result<()> {
    let path = backend.path.to_string_lossy().replace('\'', "''");
    let sql = format!(
        "ATTACH '{path}' AS {} (TYPE SQLITE, READ_ONLY)",
        quote_ident(&backend.name)
    );
    connection
        .execute_batch(&sql)
        .with_context(|| format!("could not attach {} ({})", backend.name, backend.path.display()))
}

fn harden_connection(connection: &Connection, backends: &[Backend], threads_per_worker: usize) -> Result<()> {
    let allowed = backends
        .iter()
        .map(|backend| format!("'{}'", backend.path.to_string_lossy().replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SET threads = {threads_per_worker};\n\
         SET allowed_paths = [{allowed}];\n\
         SET allow_community_extensions = false;\n\
         SET autoinstall_known_extensions = false;\n\
         SET autoload_known_extensions = false;\n\
         SET enable_external_access = false;\n\
         SET lock_configuration = true;"
    );
    connection
        .execute_batch(&sql)
        .context("could not harden DuckDB connection")
}

fn validate_init_sql(sql: &str) -> Result<()> {
    let statements = Parser::parse_sql(&DuckDbDialect {}, sql).context("init.sql could not be parsed")?;
    for statement in statements {
        match statement {
            Statement::CreateView(_) | Statement::CreateMacro { .. } => {}
            other => bail!("init.sql only allows CREATE VIEW and CREATE MACRO; got {other}"),
        }
    }
    Ok(())
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn only_comments(sql: &str) -> bool {
    sql.lines().all(|line| {
        let line = line.trim();
        line.is_empty() || line.starts_with("--")
    })
}

fn run_query(connection: &Connection, sql: &str, max_rows: usize) -> Result<QueryResult> {
    let started = Instant::now();
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query([])?;
    let columns = rows
        .as_ref()
        .map(|statement| {
            (0..statement.column_count())
                .map(|index| Column {
                    name: statement
                        .column_name(index)
                        .map_or_else(|_| format!("column_{index}"), Clone::clone),
                    data_type: format!("{:?}", statement.column_type(index)),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut output = Vec::new();
    let mut truncated = false;
    while let Some(row) = rows.next()? {
        if output.len() == max_rows {
            truncated = true;
            break;
        }
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            values.push(value_to_json(row.get::<_, Value>(index)?));
        }
        output.push(values);
    }
    Ok(QueryResult {
        row_count: output.len(),
        rows: output,
        columns,
        truncated,
        elapsed_ms: started.elapsed().as_secs_f64() * 1_000.0,
    })
}

fn value_to_json(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(value) => JsonValue::Bool(value),
        Value::TinyInt(value) => json!(value),
        Value::SmallInt(value) => json!(value),
        Value::Int(value) => json!(value),
        Value::BigInt(value) => json!(value),
        Value::UTinyInt(value) => json!(value),
        Value::USmallInt(value) => json!(value),
        Value::UInt(value) => json!(value),
        Value::UBigInt(value) => json!(value),
        Value::Float(value) => Number::from_f64(f64::from(value)).map_or(JsonValue::Null, JsonValue::Number),
        Value::Double(value) => Number::from_f64(value).map_or(JsonValue::Null, JsonValue::Number),
        Value::HugeInt(value) => JsonValue::String(value.to_string()),
        Value::UHugeInt(value) => JsonValue::String(value.to_string()),
        Value::Decimal(value) => JsonValue::String(value.to_string()),
        Value::Text(value) | Value::Enum(value) => JsonValue::String(value),
        Value::Blob(value) | Value::Geometry(value) => {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(value))
        }
        Value::Timestamp(unit, value) | Value::Time64(unit, value) => {
            JsonValue::String(format!("{value} {unit:?}"))
        }
        Value::Date32(value) => JsonValue::String(format!("date32:{value}")),
        Value::Interval { months, days, nanos } => {
            JsonValue::String(format!("{months} months {days} days {nanos} nanos"))
        }
        Value::List(values) | Value::Array(values) => {
            JsonValue::Array(values.into_iter().map(value_to_json).collect())
        }
        Value::Struct(values) => JsonValue::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), value_to_json(value.clone())))
                .collect::<Map<_, _>>(),
        ),
        Value::Map(values) => JsonValue::Array(
            values
                .iter()
                .map(|(key, value)| json!({ "key": value_to_json(key.clone()), "value": value_to_json(value.clone()) }))
                .collect(),
        ),
        Value::Union(value) => value_to_json(*value),
        _ => JsonValue::String(format!("{value:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_queries_sqlite_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_path = dir.path().join("app.sqlite");
        let seed = Connection::open_in_memory().unwrap();
        load_sqlite_extension(&seed, true).unwrap();
        seed.execute_batch(&format!(
            "ATTACH '{}' AS seed (TYPE SQLITE); \
             CREATE TABLE seed.events(id INTEGER, name TEXT); \
             INSERT INTO seed.events VALUES (1, 'open'); \
             DETACH seed;",
            sqlite_path.to_string_lossy().replace('\'', "''")
        ))
        .unwrap();

        let config = Config {
            workers: 1,
            backends: vec![Backend {
                name: "app".into(),
                path: sqlite_path,
                enabled: true,
            }],
            ..Config::default()
        };
        let pool = QueryPool::new(&config, "").unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime
            .block_on(pool.query("SELECT * FROM app.events".into()))
            .unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0][1], "open");
    }
}
