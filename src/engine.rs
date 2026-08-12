use std::{
    fmt, fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use base64::Engine as _;
use duckdb::{Connection, InterruptHandle, types::Value};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value as JsonValue, json};
use sqlparser::{ast::Statement, dialect::DuckDbDialect, parser::Parser};
use tokio::sync::oneshot;

use crate::{
    config::{Backend, BackendType, Config, LogicalView, MissingSourcePolicy, Paths, ViewMode, load_config},
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

#[derive(Debug, Clone, Serialize)]
pub struct ViewStatus {
    pub name: String,
    pub enabled: bool,
    pub status: &'static str,
    pub resolved_sources: Vec<String>,
    pub skipped_sources: Vec<SkippedSource>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SkippedSource {
    pub name: String,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct PoolStats {
    pub workers: usize,
    pub enabled_backends: usize,
    pub active_views: usize,
    pub unavailable_views: usize,
}

#[derive(Debug, Clone)]
struct ResolvedBackend {
    backend: Backend,
    files: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
struct CompiledView {
    status: ViewStatus,
    create_sql: Option<String>,
}

#[derive(Debug)]
pub enum QueryError {
    WorkersBusy,
    WorkerStopped,
    Timeout(u64),
    Execution(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkersBusy => formatter.write_str("all query workers are busy or unavailable"),
            Self::WorkerStopped => formatter.write_str("query worker stopped unexpectedly"),
            Self::Timeout(seconds) => write!(formatter, "query exceeded the {seconds} second timeout"),
            Self::Execution(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for QueryError {}

enum WorkerMessage {
    Query {
        sql: String,
        response: oneshot::Sender<Result<QueryResult, String>>,
    },
    Stop,
}

pub struct QueryPool {
    senders: Vec<mpsc::SyncSender<WorkerMessage>>,
    interrupts: Vec<Arc<InterruptHandle>>,
    next: AtomicUsize,
    workers: usize,
    enabled_backends: usize,
    view_statuses: Vec<ViewStatus>,
    timeout: std::time::Duration,
}

impl QueryPool {
    pub fn new(config: &Config, init_sql: &str) -> Result<Self> {
        let backends = config
            .backends
            .iter()
            .filter(|backend| backend.enabled)
            .map(resolve_backend)
            .collect::<Result<Vec<_>>>()?;
        let views = compile_views(config)?;
        if backends
            .iter()
            .any(|backend| backend.backend.kind == BackendType::Sqlite)
        {
            let installer = Connection::open_in_memory()?;
            load_sqlite_extension(&installer, true)?;
        }
        let mut senders = Vec::with_capacity(config.workers);
        let mut started = Vec::with_capacity(config.workers);
        for worker_id in 0..config.workers {
            let (sender, receiver) = mpsc::sync_channel(0);
            let (ready_tx, ready_rx) = mpsc::sync_channel(1);
            let backends = backends.clone();
            let views = views.clone();
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
                        &views,
                        &init_sql,
                        max_rows,
                        threads_per_worker,
                    );
                })
                .context("could not start query worker")?;
            senders.push(sender);
            started.push(ready_rx);
        }
        let mut interrupts = Vec::with_capacity(config.workers);
        for ready in started {
            match ready.recv().context("query worker exited during startup")? {
                Ok(interrupt) => interrupts.push(interrupt),
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
            interrupts,
            next: AtomicUsize::new(0),
            workers: config.workers,
            enabled_backends: config.backends.iter().filter(|backend| backend.enabled).count(),
            view_statuses: views.into_iter().map(|view| view.status).collect(),
            timeout: std::time::Duration::from_secs(config.request_timeout_seconds),
        })
    }

    pub async fn query(&self, sql: String) -> Result<QueryResult> {
        validate_read_only(&sql)?;
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        let (response_tx, response_rx) = oneshot::channel();
        let mut message = WorkerMessage::Query {
            sql,
            response: response_tx,
        };
        let mut selected = None;
        for offset in 0..self.senders.len() {
            let index = (start + offset) % self.senders.len();
            match self.senders[index].try_send(message) {
                Ok(()) => {
                    selected = Some(index);
                    break;
                }
                Err(mpsc::TrySendError::Full(returned) | mpsc::TrySendError::Disconnected(returned)) => {
                    message = returned;
                }
            }
        }
        let index = selected.ok_or(QueryError::WorkersBusy)?;
        let mut interrupt_guard = InterruptOnDrop::new(Arc::clone(&self.interrupts[index]));
        match tokio::time::timeout(self.timeout, response_rx).await {
            Ok(response) => {
                interrupt_guard.disarm();
                response
                    .map_err(|_| QueryError::WorkerStopped)?
                    .map_err(|error| QueryError::Execution(error).into())
            }
            Err(_) => Err(QueryError::Timeout(self.timeout.as_secs()).into()),
        }
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            workers: self.workers,
            enabled_backends: self.enabled_backends,
            active_views: self
                .view_statuses
                .iter()
                .filter(|view| view.status == "ready")
                .count(),
            unavailable_views: self
                .view_statuses
                .iter()
                .filter(|view| view.status == "unavailable")
                .count(),
        }
    }

    pub fn view_statuses(&self) -> &[ViewStatus] {
        &self.view_statuses
    }
}

struct InterruptOnDrop {
    handle: Arc<InterruptHandle>,
    armed: bool,
}

impl InterruptOnDrop {
    fn new(handle: Arc<InterruptHandle>) -> Self {
        Self { handle, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InterruptOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.handle.interrupt();
        }
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

    pub fn reload(&self) -> Result<PoolStats> {
        let config = load_config(&self.paths)?;
        let init_sql = fs::read_to_string(&self.paths.init_sql)?;
        let pool = Arc::new(QueryPool::new(&config, &init_sql)?);
        let stats = pool.stats();
        self.pool.store(pool);
        Ok(stats)
    }

    pub fn stats(&self) -> PoolStats {
        let pool = self.pool.load();
        pool.stats()
    }
}

pub fn test_backend(backend: &Backend) -> Result<()> {
    let backend = resolve_backend(backend)?;
    let connection = Connection::open_in_memory()?;
    if backend.backend.kind == BackendType::Sqlite {
        load_sqlite_extension(&connection, true)?;
    }
    register_backend(&connection, &backend)?;
    let probe = match backend.backend.kind {
        BackendType::Sqlite | BackendType::Duckdb => format!(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = '{}'",
            escape_literal(&backend.backend.name)
        ),
        BackendType::Parquet => format!(
            "SELECT * FROM {}.{} LIMIT 0",
            quote_ident(&backend.backend.name),
            quote_ident(backend.backend.relation())
        ),
    };
    let mut statement = connection.prepare(&probe)?;
    let mut rows = statement.query([])?;
    let _ = rows.next()?;
    Ok(())
}

pub fn resolved_file_count(backend: &Backend) -> Result<usize> {
    Ok(resolve_backend(backend)?.files.len())
}

pub fn view_statuses(config: &Config) -> Result<Vec<ViewStatus>> {
    Ok(compile_views(config)?
        .into_iter()
        .map(|view| view.status)
        .collect())
}

pub fn inspect_view(config: &Config, view: &LogicalView) -> Result<(ViewStatus, Option<String>)> {
    let compiled = compile_view(config, view)?;
    Ok((compiled.status, compiled.create_sql))
}

fn worker_main(
    receiver: &mpsc::Receiver<WorkerMessage>,
    ready: &mpsc::SyncSender<Result<Arc<InterruptHandle>, String>>,
    backends: &[ResolvedBackend],
    views: &[CompiledView],
    init_sql: &str,
    max_rows: usize,
    threads_per_worker: usize,
) {
    let connection = initialize_connection(backends, views, init_sql, threads_per_worker);
    let connection = match connection {
        Ok(connection) => {
            let _ = ready.send(Ok(connection.interrupt_handle()));
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
    backends: &[ResolvedBackend],
    views: &[CompiledView],
    init_sql: &str,
    threads_per_worker: usize,
) -> Result<Connection> {
    let connection = Connection::open_in_memory()?;
    if backends
        .iter()
        .any(|backend| backend.backend.kind == BackendType::Sqlite)
    {
        load_sqlite_extension(&connection, false)?;
    }
    for backend in backends {
        register_backend(&connection, backend)?;
    }
    for view in views {
        if let Some(sql) = &view.create_sql {
            connection
                .execute_batch(sql)
                .with_context(|| format!("could not create logical view {}", view.status.name))?;
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

fn register_backend(connection: &Connection, resolved: &ResolvedBackend) -> Result<()> {
    let backend = &resolved.backend;
    let sql = match backend.kind {
        BackendType::Sqlite | BackendType::Duckdb => {
            let kind = match backend.kind {
                BackendType::Sqlite => "SQLITE",
                BackendType::Duckdb => "DUCKDB",
                BackendType::Parquet => unreachable!(),
            };
            format!(
                "ATTACH '{}' AS {} (TYPE {kind}, READ_ONLY)",
                escape_literal(&backend.path.to_string_lossy()),
                quote_ident(&backend.name)
            )
        }
        BackendType::Parquet => {
            let files = sql_path_list(&resolved.files);
            format!(
                "CREATE SCHEMA IF NOT EXISTS {};\nCREATE VIEW {}.{} AS SELECT * FROM read_parquet({files}, union_by_name = true);",
                quote_ident(&backend.name),
                quote_ident(&backend.name),
                quote_ident(backend.relation())
            )
        }
    };
    connection.execute_batch(&sql).with_context(|| {
        format!(
            "could not register {} backend {} ({})",
            format_backend_type(backend.kind),
            backend.name,
            backend.path.display()
        )
    })
}

fn harden_connection(
    connection: &Connection,
    backends: &[ResolvedBackend],
    threads_per_worker: usize,
) -> Result<()> {
    let allowed = backends
        .iter()
        .flat_map(|resolved| {
            if resolved.backend.kind == BackendType::Parquet {
                resolved.files.clone()
            } else {
                vec![resolved.backend.path.clone()]
            }
        })
        .map(|path| format!("'{}'", escape_literal(&path.to_string_lossy())))
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

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn sql_path_list(paths: &[PathBuf]) -> String {
    format!(
        "[{}]",
        paths
            .iter()
            .map(|path| format!("'{}'", escape_literal(&path.to_string_lossy())))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn format_backend_type(kind: BackendType) -> &'static str {
    match kind {
        BackendType::Sqlite => "sqlite",
        BackendType::Duckdb => "duckdb",
        BackendType::Parquet => "parquet",
    }
}

fn resolve_backend(backend: &Backend) -> Result<ResolvedBackend> {
    let files = match backend.kind {
        BackendType::Sqlite | BackendType::Duckdb => {
            if !backend.path.is_file() {
                bail!(
                    "{} backend path is not a regular file: {}",
                    format_backend_type(backend.kind),
                    backend.path.display()
                );
            }
            vec![backend.path.clone()]
        }
        BackendType::Parquet => resolve_parquet_files(&backend.path)?,
    };
    Ok(ResolvedBackend {
        backend: backend.clone(),
        files,
    })
}

fn resolve_parquet_files(path: &Path) -> Result<Vec<PathBuf>> {
    let pattern = if path.is_dir() {
        format!("{}/**/*.parquet", glob::Pattern::escape(&path.to_string_lossy()))
    } else {
        path.to_string_lossy().into_owned()
    };
    let has_glob = pattern.bytes().any(|byte| matches!(byte, b'*' | b'?' | b'['));
    let mut files = if has_glob {
        glob::glob(&pattern)
            .with_context(|| format!("invalid Parquet path pattern: {pattern}"))?
            .map(|entry| entry.with_context(|| format!("could not read Parquet path matching {pattern}")))
            .collect::<Result<Vec<_>>>()?
    } else if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        bail!(
            "Parquet path does not exist or is not a file/directory: {}",
            path.display()
        );
    };
    files.retain(|file| file.is_file());
    files = files
        .into_iter()
        .map(|file| {
            fs::canonicalize(&file)
                .with_context(|| format!("could not resolve Parquet file {}", file.display()))
        })
        .collect::<Result<Vec<_>>>()?;
    files.sort();
    files.dedup();
    if files.is_empty() {
        bail!("Parquet path matched no files: {}", path.display());
    }
    Ok(files)
}

fn compile_views(config: &Config) -> Result<Vec<CompiledView>> {
    config
        .views
        .iter()
        .map(|view| compile_view(config, view))
        .collect()
}

#[allow(clippy::too_many_lines)]
fn compile_view(config: &Config, view: &LogicalView) -> Result<CompiledView> {
    let qualified_name = view.qualified_name();
    if !view.enabled {
        return Ok(CompiledView {
            status: ViewStatus {
                name: qualified_name,
                enabled: false,
                status: "disabled",
                resolved_sources: Vec::new(),
                skipped_sources: Vec::new(),
            },
            create_sql: None,
        });
    }
    if config
        .backends
        .iter()
        .any(|backend| backend.enabled && backend.name == view.schema)
    {
        bail!(
            "logical view schema '{}' conflicts with attached backend '{}'; choose another schema",
            view.schema,
            view.schema
        );
    }
    let mut selects = Vec::new();
    let mut resolved_sources = Vec::new();
    let mut skipped_sources = Vec::new();
    for input in &view.inputs {
        let Some(backend) = config
            .backends
            .iter()
            .find(|backend| backend.name == input.backend)
        else {
            if view.missing_source_policy == MissingSourcePolicy::Error {
                bail!(
                    "view {qualified_name} requires unregistered backend {}",
                    input.backend
                );
            }
            skipped_sources.push(SkippedSource {
                name: input.backend.clone(),
                reason: "not_registered",
            });
            continue;
        };
        if !backend.enabled {
            if view.missing_source_policy == MissingSourcePolicy::Error {
                bail!(
                    "view {qualified_name} requires disabled backend {}",
                    input.backend
                );
            }
            skipped_sources.push(SkippedSource {
                name: input.backend.clone(),
                reason: "disabled",
            });
            continue;
        }
        if backend.kind == BackendType::Parquet && input.relation != backend.relation() {
            bail!(
                "view {qualified_name} input {} names relation '{}', but Parquet backend exposes '{}'",
                input.backend,
                input.relation,
                backend.relation()
            );
        }
        let mut projection = Vec::new();
        if let Some(source_column) = &view.source_column {
            projection.push(format!(
                "'{}' AS {}",
                escape_literal(&input.backend),
                quote_ident(source_column)
            ));
        }
        if view.columns.is_empty() {
            projection.push("*".to_owned());
        } else {
            projection.extend(view.columns.iter().map(|(logical, data_type)| {
                let physical = input.columns.get(logical).unwrap_or(logical);
                format!(
                    "CAST({} AS {data_type}) AS {}",
                    quote_ident(physical),
                    quote_ident(logical)
                )
            }));
        }
        selects.push(format!(
            "SELECT {} FROM {}",
            projection.join(", "),
            source_relation(&input.backend, &input.relation)
        ));
        resolved_sources.push(input.backend.clone());
    }
    let status = if selects.is_empty() {
        "unavailable"
    } else {
        "ready"
    };
    let create_sql = if selects.is_empty() {
        None
    } else {
        let operator = match view.mode {
            ViewMode::UnionAll => " UNION ALL ",
            ViewMode::UnionAllByName => " UNION ALL BY NAME ",
        };
        Some(format!(
            "CREATE SCHEMA IF NOT EXISTS {};\nCREATE VIEW {}.{} AS {};",
            quote_ident(&view.schema),
            quote_ident(&view.schema),
            quote_ident(&view.name),
            selects.join(operator)
        ))
    };
    Ok(CompiledView {
        status: ViewStatus {
            name: qualified_name,
            enabled: true,
            status,
            resolved_sources,
            skipped_sources,
        },
        create_sql,
    })
}

fn source_relation(backend: &str, relation: &str) -> String {
    let mut parts = vec![quote_ident(backend)];
    parts.extend(relation.split('.').map(quote_ident));
    parts.join(".")
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::ViewInput;

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
                kind: BackendType::Sqlite,
                path: sqlite_path,
                relation: None,
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

    #[test]
    fn timeout_interrupts_query_worker() {
        let config = Config {
            workers: 1,
            ..Config::default()
        };
        let mut pool = QueryPool::new(&config, "").unwrap();
        pool.timeout = std::time::Duration::from_millis(10);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let error = runtime
            .block_on(pool.query("SELECT sum(i) FROM range(100000000000) t(i)".into()))
            .unwrap_err();
        assert!(error.to_string().contains("timeout"));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn logical_view_unifies_sqlite_duckdb_and_parquet_with_mappings() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_path = dir.path().join("old.sqlite");
        let seed = Connection::open_in_memory().unwrap();
        load_sqlite_extension(&seed, true).unwrap();
        seed.execute_batch(&format!(
            "ATTACH '{}' AS seed (TYPE SQLITE); \
             CREATE TABLE seed.old_tracks(code TEXT, title TEXT, year INTEGER); \
             INSERT INTO seed.old_tracks VALUES ('A', 'old', 2020); \
             DETACH seed;",
            escape_literal(&sqlite_path.to_string_lossy())
        ))
        .unwrap();

        let duckdb_path = dir.path().join("new.duckdb");
        let duckdb_seed = Connection::open(&duckdb_path).unwrap();
        duckdb_seed
            .execute_batch(
                "CREATE TABLE new_tracks(isrc VARCHAR, track_name VARCHAR, release_year INTEGER); \
                 INSERT INTO new_tracks VALUES ('B', 'new', 2021);",
            )
            .unwrap();
        drop(duckdb_seed);

        let parquet_path = dir.path().join("archive.parquet");
        let parquet_seed = Connection::open_in_memory().unwrap();
        parquet_seed
            .execute_batch(&format!(
                "COPY (SELECT 'C'::VARCHAR AS recording_id, 'archive'::VARCHAR AS name, 2019::INTEGER AS released) \
                 TO '{}' (FORMAT PARQUET)",
                escape_literal(&parquet_path.to_string_lossy())
            ))
            .unwrap();

        let columns = BTreeMap::from([
            ("isrc".to_owned(), "VARCHAR".to_owned()),
            ("release_year".to_owned(), "INTEGER".to_owned()),
            ("track_name".to_owned(), "VARCHAR".to_owned()),
        ]);
        let config = Config {
            workers: 1,
            backends: vec![
                Backend {
                    name: "old".into(),
                    kind: BackendType::Sqlite,
                    path: sqlite_path,
                    relation: None,
                    enabled: true,
                },
                Backend {
                    name: "new".into(),
                    kind: BackendType::Duckdb,
                    path: duckdb_path,
                    relation: None,
                    enabled: true,
                },
                Backend {
                    name: "archive".into(),
                    kind: BackendType::Parquet,
                    path: parquet_path,
                    relation: Some("archive_tracks".into()),
                    enabled: true,
                },
            ],
            views: vec![LogicalView {
                name: "tracks".into(),
                schema: "unified".into(),
                enabled: true,
                mode: ViewMode::UnionAllByName,
                missing_source_policy: MissingSourcePolicy::Skip,
                source_column: Some("_source_backend".into()),
                columns,
                inputs: vec![
                    ViewInput {
                        backend: "old".into(),
                        relation: "old_tracks".into(),
                        columns: BTreeMap::from([
                            ("isrc".into(), "code".into()),
                            ("track_name".into(), "title".into()),
                            ("release_year".into(), "year".into()),
                        ]),
                    },
                    ViewInput {
                        backend: "new".into(),
                        relation: "new_tracks".into(),
                        columns: BTreeMap::new(),
                    },
                    ViewInput {
                        backend: "archive".into(),
                        relation: "archive_tracks".into(),
                        columns: BTreeMap::from([
                            ("isrc".into(), "recording_id".into()),
                            ("track_name".into(), "name".into()),
                            ("release_year".into(), "released".into()),
                        ]),
                    },
                ],
            }],
            ..Config::default()
        };
        let pool = QueryPool::new(&config, "").unwrap();
        assert_eq!(pool.stats().active_views, 1);
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(pool.query(
                "SELECT _source_backend, isrc, track_name, release_year FROM unified.tracks ORDER BY isrc"
                    .into(),
            ))
            .unwrap();
        assert_eq!(result.row_count, 3);
        assert_eq!(
            result.rows[0],
            vec![json!("old"), json!("A"), json!("old"), json!(2020)]
        );
        assert_eq!(
            result.rows[1],
            vec![json!("new"), json!("B"), json!("new"), json!(2021)]
        );
        assert_eq!(
            result.rows[2],
            vec![json!("archive"), json!("C"), json!("archive"), json!(2019)]
        );
    }

    #[test]
    fn skip_policy_marks_view_unavailable_when_all_inputs_are_disabled() {
        let config = Config {
            backends: vec![Backend {
                name: "app".into(),
                kind: BackendType::Sqlite,
                path: PathBuf::from("/tmp/not-opened.sqlite"),
                relation: None,
                enabled: false,
            }],
            views: vec![LogicalView {
                name: "events".into(),
                schema: "unified".into(),
                enabled: true,
                mode: ViewMode::UnionAllByName,
                missing_source_policy: MissingSourcePolicy::Skip,
                source_column: Some("_source_backend".into()),
                columns: BTreeMap::new(),
                inputs: vec![ViewInput {
                    backend: "app".into(),
                    relation: "events".into(),
                    columns: BTreeMap::new(),
                }],
            }],
            workers: 1,
            ..Config::default()
        };
        let pool = QueryPool::new(&config, "").unwrap();
        assert_eq!(pool.stats().unavailable_views, 1);
        assert_eq!(pool.view_statuses()[0].skipped_sources[0].reason, "disabled");
    }
}
