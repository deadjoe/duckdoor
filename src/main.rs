mod client;
mod config;
mod daemon;
mod engine;
mod output;
mod server;
mod sql;

use std::{
    collections::BTreeMap,
    fs,
    fs::OpenOptions,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Context, Result};
use clap::{ArgAction, CommandFactory, Parser, Subcommand, error::ErrorKind};
use config::{
    Backend, BackendType, Config, LogicalView, MissingSourcePolicy, Paths, ViewInput, ViewMode, load_config,
    parse_qualified_view_name, save_config, validate_name, validate_relation,
};
use fs2::FileExt;
use serde_json::json;

#[derive(Debug, Parser)]
#[command(
    name = "duckdoor",
    version,
    about = "Read-only DuckDB gateway for local SQLite, DuckDB, and Parquet data",
    arg_required_else_help = true,
    after_help = "OUTPUT:\n  Commands emit one compact JSON document by default.\n  `logs` emits JSON Lines. `query --output table|csv|jsonl` is opt-in.\n\nEXAMPLES:\n  duckdoor add app /absolute/path/app.sqlite\n  duckdoor add archive '/data/archive/*.parquet' --type parquet --relation events\n  duckdoor view add unified.events --input app=events --input archive=events\n  duckdoor start\n  duckdoor query 'SELECT count(*) FROM unified.events'"
)]
struct Cli {
    /// Configuration and state directory.
    #[arg(long, global = true, env = "DUCKDOOR_HOME")]
    home: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the gateway in the background.
    #[command(after_help = "OUTPUT:\n  One JSON document describing the running daemon.")]
    Start,
    /// Stop the background gateway.
    #[command(after_help = "OUTPUT:\n  One JSON document describing the stopped daemon.")]
    Stop,
    /// Stop and start the gateway.
    #[command(after_help = "OUTPUT:\n  One JSON document describing the restarted daemon.")]
    Restart,
    /// Show daemon and health state.
    #[command(after_help = "OUTPUT:\n  One JSON document. `data.state` is running, unhealthy, or stopped.")]
    Status,
    /// Validate configuration, sources, logical views, and the query engine.
    #[command(
        after_help = "OUTPUT:\n  One JSON document containing config, backend, engine, daemon, and warning checks."
    )]
    Doctor,
    /// Stream structured gateway logs.
    #[command(
        after_help = "OUTPUT:\n  JSON Lines: one complete log event per line. Use --no-follow for a finite response."
    )]
    Logs {
        #[arg(short = 'n', long, default_value_t = 100)]
        lines: usize,
        #[arg(long = "no-follow", action = ArgAction::SetFalse, default_value_t = true)]
        follow: bool,
    },
    /// List registered data backends.
    #[command(after_help = "OUTPUT:\n  One JSON document with counts and the registered backend array.")]
    List,
    /// Register a `SQLite`, `DuckDB`, or Parquet backend.
    #[command(
        after_help = "TYPE INFERENCE:\n  Existing .sqlite/.sqlite3/.db, .duckdb, and .parquet files are detected.\n  Directories and glob patterns require --type parquet. Quote globs in the shell.\n\nEXAMPLES:\n  duckdoor add app ./app.sqlite\n  duckdoor add warehouse ./warehouse.duckdb --type duckdb\n  duckdoor add archive './data/**/*.parquet' --type parquet --relation events\n\nOUTPUT:\n  One JSON document with the stored absolute path, backend type, and reload state."
    )]
    Add {
        /// Stable source name used in queries and logical views.
        name: String,
        /// Existing database file, Parquet file/directory, or quoted Parquet glob.
        path: PathBuf,
        /// Source type; inferred only for existing files with a known extension.
        #[arg(long = "type", value_enum)]
        kind: Option<BackendType>,
        /// Table-like name exposed by a Parquet backend (default: data).
        #[arg(long)]
        relation: Option<String>,
        /// Register the backend without attaching it to query workers.
        #[arg(long)]
        disabled: bool,
    },
    /// Remove one or all backend registrations; source files are never deleted.
    #[command(
        after_help = "EXAMPLES:\n  duckdoor remove app\n  duckdoor remove --all\n\nOUTPUT:\n  One JSON document listing exactly what was unregistered. Source files are never deleted."
    )]
    Remove {
        /// Name of one registered backend to remove.
        #[arg(value_name = "NAME", required_unless_present = "all", conflicts_with = "all")]
        name: Option<String>,
        /// Atomically remove every backend registration.
        #[arg(long)]
        all: bool,
    },
    /// Enable and hot-activate a backend.
    #[command(after_help = "OUTPUT:\n  One JSON document. `changed` is false when already enabled.")]
    Enable { name: String },
    /// Disable and hot-deactivate a backend.
    #[command(after_help = "OUTPUT:\n  One JSON document. `changed` is false when already disabled.")]
    Disable { name: String },
    /// Test a registered backend without changing it.
    #[command(after_help = "OUTPUT:\n  One JSON document confirming the source can be registered and read.")]
    Test { name: String },
    /// Manage persistent non-materialized logical views.
    #[command(subcommand)]
    View(ViewCommand),
    /// Hot-reload configuration and init.sql.
    #[command(after_help = "OUTPUT:\n  One JSON document containing the active worker and backend counts.")]
    Reload,
    /// Run one read-only SQL query through the daemon.
    #[command(
        after_help = "INPUT:\n  Provide SQL as one argument, with --file, or on stdin.\n\nOUTPUT:\n  json (default): one JSON document with columns, rows, counts, and timing.\n  jsonl: one JSON object per result row.\n  csv: header plus result rows.\n  table: compact human-readable columns without separator art."
    )]
    Query {
        /// One read-only SQL statement. Reads stdin when omitted.
        sql: Option<String>,
        /// Read the SQL statement from a UTF-8 file.
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Output format. JSON is the stable default for humans and agents.
        #[arg(
            short = 'o',
            long = "output",
            visible_alias = "format",
            value_enum,
            default_value_t = client::OutputFormat::Json
        )]
        format: client::OutputFormat,
    },
    /// Run the HTTP service in the foreground.
    #[command(hide = true)]
    Serve,
}

#[derive(Debug, Subcommand)]
enum ViewCommand {
    /// Create a logical view from one or more backend relations.
    #[command(
        after_help = "INPUT FORMAT:\n  --input BACKEND=TABLE (or BACKEND=SCHEMA.TABLE), repeated once per source.\n  --column NAME=DUCKDB_TYPE defines a stable logical schema.\n  --map BACKEND:LOGICAL=PHYSICAL maps a source column.\n\nEXAMPLES:\n  duckdoor view add unified.events --input app=events --input archive=events\n  duckdoor view add unified.tracks --input old=tracks --input new=songs --column isrc=VARCHAR --column title=VARCHAR --map new:title=name\n\nOUTPUT:\n  One JSON document containing the persisted definition, resolution status, and reload state."
    )]
    Add {
        /// Logical name as NAME or SCHEMA.NAME (default schema: unified).
        name: String,
        /// Source relation as BACKEND=TABLE; repeat for each backend.
        #[arg(long, required = true)]
        input: Vec<String>,
        /// Logical column contract as `NAME=DUCKDB_TYPE`; repeat as needed.
        #[arg(long)]
        column: Vec<String>,
        /// Per-source rename as BACKEND:LOGICAL=PHYSICAL; requires --column.
        #[arg(long = "map")]
        mappings: Vec<String>,
        /// Set operation used to combine inputs.
        #[arg(long, value_enum, default_value_t = ViewMode::UnionAllByName)]
        mode: ViewMode,
        /// Behavior when a configured backend is absent or disabled.
        #[arg(long, value_enum, default_value_t = MissingSourcePolicy::Skip)]
        missing_source_policy: MissingSourcePolicy,
        /// Provenance column added to every row.
        #[arg(long, default_value = "_source_backend")]
        source_column: String,
        /// Do not add a source provenance column.
        #[arg(long, conflicts_with = "source_column")]
        no_source_column: bool,
        /// Persist the view without making it queryable.
        #[arg(long)]
        disabled: bool,
    },
    /// List logical views and their currently resolved sources.
    List,
    /// Show one view definition, resolution status, and compiled SQL.
    Show { name: String },
    /// Validate one view against current backend schemas without changing it.
    Test { name: String },
    /// Enable and hot-load a logical view.
    Enable { name: String },
    /// Disable and hot-remove a logical view.
    Disable { name: String },
    /// Remove a logical view definition; source files are never changed.
    Remove { name: String },
}

fn main() -> ExitCode {
    if std::env::args_os().len() == 1 {
        let mut command = Cli::command();
        let _ = command.print_help();
        println!();
        return ExitCode::SUCCESS;
    }
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp
                    | ErrorKind::DisplayVersion
                    | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) =>
        {
            let _ = error.print();
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            let rendered = error.to_string();
            let message = concise_clap_error(&rendered);
            let usage = clap_usage(&rendered);
            output::error_with_details(
                "invalid_arguments",
                message,
                &json!({
                    "usage": usage,
                    "help": "run the command with --help to see its arguments and examples",
                }),
            );
            return ExitCode::from(2);
        }
    };
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if let Some(error) = error.downcast_ref::<output::CommandError>() {
                error.write();
            } else {
                output::error("command_failed", &format!("{error:#}"));
            }
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    let paths = Paths::resolve(cli.home)?;
    paths.ensure()?;
    match cli.command {
        Command::Start => output::success("start", daemon::start(&paths)?),
        Command::Stop => output::success("stop", daemon::stop(&paths)?),
        Command::Restart => output::success("restart", daemon::restart(&paths)?),
        Command::Status => output::success("status", daemon::status(&paths)?),
        Command::Doctor => doctor(&paths),
        Command::Logs { lines, follow } => daemon::logs(&paths, lines, follow),
        Command::List => list(&paths),
        Command::Add {
            name,
            path,
            kind,
            relation,
            disabled,
        } => add(&paths, &name, &path, kind, relation, !disabled),
        Command::Remove { name, all } => match (name, all) {
            (Some(name), false) => remove(&paths, &name),
            (None, true) => remove_all(&paths),
            _ => unreachable!("clap validates remove arguments"),
        },
        Command::Enable { name } => set_enabled(&paths, &name, true),
        Command::Disable { name } => set_enabled(&paths, &name, false),
        Command::Test { name } => test(&paths, &name),
        Command::View(command) => run_view(&paths, command),
        Command::Reload => output::success("reload", daemon::reload(&paths)?),
        Command::Query { sql, file, format } => {
            let sql = client::read_sql(sql, file)?;
            let result = client::query(&paths, sql)?;
            client::print_result(&result, format)
        }
        Command::Serve => tokio::runtime::Runtime::new()?.block_on(serve(paths)),
    }
}

fn concise_clap_error(rendered: &str) -> &str {
    let body = rendered.trim().strip_prefix("error: ").unwrap_or(rendered.trim());
    body.split("\n\nUsage:").next().unwrap_or(body).trim()
}

fn clap_usage(rendered: &str) -> Option<&str> {
    rendered
        .split("\n\nUsage:")
        .nth(1)
        .and_then(|usage| usage.lines().next())
        .map(str::trim)
        .filter(|usage| !usage.is_empty())
}

fn load_cli_config(paths: &Paths) -> Result<Config> {
    load_config(paths).map_err(|error| {
        output::CommandError::new(
            "invalid_configuration",
            "duckdoor configuration could not be loaded",
            json!({
                "path": paths.config,
                "cause": format!("{error:#}"),
                "resolution": "correct config.toml or restore a known-good copy, then retry",
            }),
        )
        .into()
    })
}

async fn serve(paths: Paths) -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("duckdoor=info".parse()?),
        )
        .with_current_span(false)
        .with_span_list(false)
        .init();
    let _pid_guard = daemon::PidGuard::acquire(&paths)?;
    server::run(paths).await
}

fn list(paths: &Paths) -> Result<()> {
    let config = load_cli_config(paths)?;
    let enabled = config.backends.iter().filter(|backend| backend.enabled).count();
    output::success(
        "list",
        json!({
            "count": config.backends.len(),
            "enabled": enabled,
            "disabled": config.backends.len() - enabled,
            "backends": config.backends,
        }),
    )
}

fn add(
    paths: &Paths,
    name: &str,
    path: &Path,
    kind: Option<BackendType>,
    relation: Option<String>,
    enabled: bool,
) -> Result<()> {
    validate_name(name).map_err(|error| {
        output::CommandError::new(
            "invalid_backend_name",
            error.to_string(),
            json!({ "name": name, "expected": "[A-Za-z_][A-Za-z0-9_]{0,62}" }),
        )
    })?;
    let current = load_cli_config(paths)?;
    if let Some(existing) = current.backends.iter().find(|item| item.name == name) {
        return Err(output::CommandError::new(
            "backend_already_exists",
            format!("backend name '{name}' is already registered"),
            json!({
                "existing": existing,
                "attempted_path": path,
                "resolution": "choose a unique name or remove the existing registration first",
            }),
        )
        .into());
    }
    let Some(kind) = kind.or_else(|| infer_backend_type(path)) else {
        return Err(output::CommandError::new(
            "backend_type_required",
            "backend type could not be inferred from the path",
            json!({
                "path": path,
                "accepted_types": ["sqlite", "duckdb", "parquet"],
                "resolution": "add --type sqlite, --type duckdb, or --type parquet; directories and globs require --type parquet",
            }),
        )
        .into());
    };
    if kind != BackendType::Parquet && relation.is_some() {
        return Err(output::CommandError::new(
            "invalid_backend_options",
            "--relation is only valid for Parquet backends",
            json!({ "type": kind, "resolution": "remove --relation or use --type parquet" }),
        )
        .into());
    }
    let relation = if kind == BackendType::Parquet {
        Some(relation.unwrap_or_else(|| "data".to_owned()))
    } else {
        None
    };
    if let Some(relation) = &relation {
        validate_name(relation).map_err(|error| {
            output::CommandError::new(
                "invalid_relation_name",
                error.to_string(),
                json!({ "relation": relation, "expected": "[A-Za-z_][A-Za-z0-9_]{0,62}" }),
            )
        })?;
    }
    let path = prepare_backend_path(path, kind)?;
    let backend = Backend {
        name: name.to_owned(),
        kind,
        path,
        relation,
        enabled,
    };
    engine::test_backend(&backend).map_err(|error| {
        output::CommandError::new(
            "backend_validation_failed",
            format!(
                "{} backend '{}' could not be opened read-only",
                backend_type_name(kind),
                name
            ),
            json!({
                "name": name,
                "type": kind,
                "path": backend.path,
                "cause": format!("{error:#}"),
                "configuration_changed": false,
            }),
        )
    })?;
    mutate_config(paths, |config| {
        if config.backends.iter().any(|item| item.name == name) {
            return Err(output::CommandError::new(
                "backend_already_exists",
                format!("backend name '{name}' was registered concurrently"),
                json!({ "attempted_path": backend.path }),
            )
            .into());
        }
        config.backends.push(backend.clone());
        Ok(())
    })?;
    output::success(
        "add",
        json!({
            "backend": backend,
            "configuration_reloaded": daemon::running_pid(paths)?.is_some(),
        }),
    )
}

fn remove(paths: &Paths, name: &str) -> Result<()> {
    let current = load_cli_config(paths)?;
    let removed = current
        .backends
        .iter()
        .find(|item| item.name == name)
        .cloned()
        .ok_or_else(|| backend_not_found(name, &current))?;
    mutate_config(paths, |config| {
        config.backends.retain(|item| item.name != name);
        Ok(())
    })?;
    output::success(
        "remove",
        json!({
            "name": name,
            "type": removed.kind,
            "registration_removed": true,
            "source_files_deleted": 0,
            "configuration_reloaded": daemon::running_pid(paths)?.is_some(),
        }),
    )
}

fn remove_all(paths: &Paths) -> Result<()> {
    let _lock = lock_config(paths)?;
    let previous = load_cli_config(paths)?;
    if previous.backends.is_empty() {
        return output::success(
            "remove",
            json!({
                "scope": "all",
                "changed": false,
                "removed_count": 0,
                "removed": [],
                "source_files_deleted": 0,
                "configuration_reloaded": false,
            }),
        );
    }
    let mut updated = previous.clone();
    updated.backends.clear();
    let daemon_running = daemon::running_pid(paths)?.is_some();
    save_and_reload(paths, &previous, &updated)?;
    output::success(
        "remove",
        json!({
            "scope": "all",
            "changed": true,
            "removed_count": previous.backends.len(),
            "removed": previous.backends,
            "source_files_deleted": 0,
            "configuration_reloaded": daemon_running,
        }),
    )
}

fn set_enabled(paths: &Paths, name: &str, enabled: bool) -> Result<()> {
    let _lock = lock_config(paths)?;
    let mut config = load_cli_config(paths)?;
    let previous = config.clone();
    let backend = config
        .backends
        .iter_mut()
        .find(|item| item.name == name)
        .ok_or_else(|| backend_not_found(name, &previous))?;
    if enabled {
        engine::test_backend(backend).map_err(|error| {
            output::CommandError::new(
                "backend_validation_failed",
                format!("backend '{name}' cannot be enabled because validation failed"),
                json!({
                    "backend": backend,
                    "cause": format!("{error:#}"),
                    "configuration_changed": false,
                }),
            )
        })?;
    }
    if backend.enabled == enabled {
        return output::success(
            if enabled { "enable" } else { "disable" },
            json!({ "name": name, "enabled": enabled, "changed": false }),
        );
    }
    backend.enabled = enabled;
    save_and_reload(paths, &previous, &config)?;
    output::success(
        if enabled { "enable" } else { "disable" },
        json!({
            "name": name,
            "enabled": enabled,
            "changed": true,
            "configuration_reloaded": daemon::running_pid(paths)?.is_some(),
        }),
    )
}

fn test(paths: &Paths, name: &str) -> Result<()> {
    let config = load_cli_config(paths)?;
    let backend = config
        .backends
        .iter()
        .find(|item| item.name == name)
        .ok_or_else(|| backend_not_found(name, &config))?;
    engine::test_backend(backend).map_err(|error| {
        output::CommandError::new(
            "backend_validation_failed",
            format!("backend '{name}' could not be opened read-only"),
            json!({ "backend": backend, "cause": format!("{error:#}") }),
        )
    })?;
    output::success(
        "test",
        json!({
            "backend": backend,
            "validation": "ok",
            "read_only": true,
            "resolved_files": engine::resolved_file_count(backend)?,
        }),
    )
}

fn run_view(paths: &Paths, command: ViewCommand) -> Result<()> {
    match command {
        ViewCommand::Add {
            name,
            input,
            column,
            mappings,
            mode,
            missing_source_policy,
            source_column,
            no_source_column,
            disabled,
        } => add_view(
            paths,
            &name,
            &input,
            &column,
            &mappings,
            mode,
            missing_source_policy,
            (!no_source_column).then_some(source_column),
            !disabled,
        ),
        ViewCommand::List => list_views(paths),
        ViewCommand::Show { name } => show_view(paths, &name),
        ViewCommand::Test { name } => test_view(paths, &name),
        ViewCommand::Enable { name } => set_view_enabled(paths, &name, true),
        ViewCommand::Disable { name } => set_view_enabled(paths, &name, false),
        ViewCommand::Remove { name } => remove_view(paths, &name),
    }
}

#[allow(clippy::too_many_arguments)]
fn add_view(
    paths: &Paths,
    qualified_name: &str,
    input_specs: &[String],
    column_specs: &[String],
    mapping_specs: &[String],
    mode: ViewMode,
    missing_source_policy: MissingSourcePolicy,
    source_column: Option<String>,
    enabled: bool,
) -> Result<()> {
    let (schema, name) = parse_qualified_view_name(qualified_name).map_err(|error| {
        output::CommandError::new(
            "invalid_view_name",
            error.to_string(),
            json!({ "name": qualified_name, "expected": "NAME or SCHEMA.NAME" }),
        )
    })?;
    let mut inputs = input_specs
        .iter()
        .map(|spec| parse_view_input(spec))
        .collect::<Result<Vec<_>>>()?;
    for (index, input) in inputs.iter().enumerate() {
        if inputs[..index]
            .iter()
            .any(|candidate| candidate.backend == input.backend)
        {
            return Err(output::CommandError::new(
                "duplicate_view_input",
                format!("backend '{}' was specified more than once", input.backend),
                json!({
                    "backend": input.backend,
                    "resolution": "use one input relation per backend in a logical view",
                }),
            )
            .into());
        }
    }
    let columns = parse_columns(column_specs)?;
    if !mapping_specs.is_empty() && columns.is_empty() {
        return Err(output::CommandError::new(
            "view_schema_required",
            "--map requires at least one --column logical schema declaration",
            json!({ "resolution": "add --column LOGICAL_NAME=DUCKDB_TYPE for every output column" }),
        )
        .into());
    }
    apply_mappings(&mut inputs, mapping_specs, &columns)?;
    if let Some(column) = &source_column {
        validate_name(column).map_err(|error| {
            output::CommandError::new(
                "invalid_source_column",
                error.to_string(),
                json!({ "source_column": column, "expected": "[A-Za-z_][A-Za-z0-9_]{0,62}" }),
            )
        })?;
    }
    let view = LogicalView {
        name,
        schema,
        enabled,
        mode,
        missing_source_policy,
        source_column,
        columns,
        inputs,
    };
    let current = load_cli_config(paths)?;
    if let Some(existing) = current
        .views
        .iter()
        .find(|candidate| candidate.qualified_name() == view.qualified_name())
    {
        return Err(output::CommandError::new(
            "view_already_exists",
            format!("logical view '{}' is already registered", view.qualified_name()),
            json!({
                "existing": existing,
                "resolution": "choose another name or remove the existing view first",
            }),
        )
        .into());
    }
    mutate_config(paths, |config| {
        config.views.push(view.clone());
        Ok(())
    })?;
    let config = load_cli_config(paths)?;
    let stored = find_view(&config, &view.qualified_name())?;
    let (status, _) = engine::inspect_view(&config, stored)?;
    output::success(
        "view.add",
        json!({
            "view": stored,
            "resolution": status,
            "configuration_reloaded": daemon::running_pid(paths)?.is_some(),
        }),
    )
}

fn list_views(paths: &Paths) -> Result<()> {
    let config = load_cli_config(paths)?;
    let statuses = engine::view_statuses(&config)?;
    let ready = statuses.iter().filter(|status| status.status == "ready").count();
    let unavailable = statuses
        .iter()
        .filter(|status| status.status == "unavailable")
        .count();
    output::success(
        "view.list",
        json!({
            "count": config.views.len(),
            "ready": ready,
            "unavailable": unavailable,
            "disabled": statuses.iter().filter(|status| status.status == "disabled").count(),
            "views": statuses,
        }),
    )
}

fn show_view(paths: &Paths, qualified_name: &str) -> Result<()> {
    let config = load_cli_config(paths)?;
    let view = find_view(&config, qualified_name)?;
    let (status, compiled_sql) = engine::inspect_view(&config, view)?;
    output::success(
        "view.show",
        json!({ "view": view, "resolution": status, "compiled_sql": compiled_sql }),
    )
}

fn test_view(paths: &Paths, qualified_name: &str) -> Result<()> {
    let config = load_cli_config(paths)?;
    let view = find_view(&config, qualified_name)?.clone();
    let mut probe = config.clone();
    probe.workers = 1;
    probe.views = vec![view.clone()];
    let pool = engine::QueryPool::new(&probe, "").map_err(|error| {
        output::CommandError::new(
            "view_validation_failed",
            format!("logical view '{}' could not be created", view.qualified_name()),
            json!({ "cause": format!("{error:#}"), "configuration_changed": false }),
        )
    })?;
    let status = pool
        .view_statuses()
        .first()
        .cloned()
        .with_context(|| format!("view {} produced no validation status", view.qualified_name()))?;
    if status.status == "ready" {
        let query = format!(
            "SELECT * FROM {}.{} LIMIT 0",
            quote_identifier(&view.schema),
            quote_identifier(&view.name)
        );
        tokio::runtime::Runtime::new()?
            .block_on(pool.query(query))
            .map_err(|error| {
                output::CommandError::new(
                    "view_validation_failed",
                    format!("logical view '{}' could not be queried", view.qualified_name()),
                    json!({ "cause": format!("{error:#}"), "configuration_changed": false }),
                )
            })?;
    }
    output::success(
        "view.test",
        json!({ "name": view.qualified_name(), "validation": "ok", "resolution": status }),
    )
}

fn set_view_enabled(paths: &Paths, qualified_name: &str, enabled: bool) -> Result<()> {
    let _lock = lock_config(paths)?;
    let mut config = load_cli_config(paths)?;
    let previous = config.clone();
    let (schema, name) = parse_qualified_view_name(qualified_name)?;
    let Some(view_index) = config
        .views
        .iter()
        .position(|view| view.schema == schema && view.name == name)
    else {
        return Err(view_not_found(qualified_name, &config).into());
    };
    let view = &mut config.views[view_index];
    if view.enabled == enabled {
        return output::success(
            if enabled { "view.enable" } else { "view.disable" },
            json!({ "name": format!("{}.{}", schema, name), "enabled": enabled, "changed": false }),
        );
    }
    view.enabled = enabled;
    save_and_reload(paths, &previous, &config)?;
    output::success(
        if enabled { "view.enable" } else { "view.disable" },
        json!({
            "name": format!("{}.{}", schema, name),
            "enabled": enabled,
            "changed": true,
            "configuration_reloaded": daemon::running_pid(paths)?.is_some(),
        }),
    )
}

fn remove_view(paths: &Paths, qualified_name: &str) -> Result<()> {
    let (schema, name) = parse_qualified_view_name(qualified_name)?;
    let current = load_cli_config(paths)?;
    if !current
        .views
        .iter()
        .any(|view| view.schema == schema && view.name == name)
    {
        return Err(view_not_found(qualified_name, &current).into());
    }
    mutate_config(paths, |config| {
        config
            .views
            .retain(|view| view.schema != schema || view.name != name);
        Ok(())
    })?;
    output::success(
        "view.remove",
        json!({
            "name": format!("{}.{}", schema, name),
            "registration_removed": true,
            "source_files_deleted": 0,
            "configuration_reloaded": daemon::running_pid(paths)?.is_some(),
        }),
    )
}

fn infer_backend_type(path: &Path) -> Option<BackendType> {
    if !path.is_file() {
        return None;
    }
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("sqlite" | "sqlite3" | "db") => Some(BackendType::Sqlite),
        Some("duckdb") => Some(BackendType::Duckdb),
        Some("parquet") => Some(BackendType::Parquet),
        _ => None,
    }
}

fn prepare_backend_path(path: &Path, kind: BackendType) -> Result<PathBuf> {
    let has_glob = path
        .to_string_lossy()
        .bytes()
        .any(|byte| matches!(byte, b'*' | b'?' | b'['));
    if has_glob && kind != BackendType::Parquet {
        return Err(output::CommandError::new(
            "invalid_backend_path",
            "glob patterns are only supported for Parquet backends",
            json!({ "path": path, "type": kind }),
        )
        .into());
    }
    let resolved = if has_glob {
        std::path::absolute(path).with_context(|| format!("could not make {} absolute", path.display()))?
    } else {
        fs::canonicalize(path).map_err(|error| {
            output::CommandError::new(
                "backend_path_not_found",
                format!("backend path does not exist: {}", path.display()),
                json!({ "path": path, "cause": error.to_string(), "configuration_changed": false }),
            )
        })?
    };
    if !has_glob {
        let valid_kind = match kind {
            BackendType::Sqlite | BackendType::Duckdb => resolved.is_file(),
            BackendType::Parquet => resolved.is_file() || resolved.is_dir(),
        };
        if !valid_kind {
            return Err(output::CommandError::new(
                "invalid_backend_path",
                format!(
                    "{} backend requires {}",
                    backend_type_name(kind),
                    if kind == BackendType::Parquet {
                        "a file, directory, or glob"
                    } else {
                        "a regular file"
                    }
                ),
                json!({ "path": resolved, "type": kind, "configuration_changed": false }),
            )
            .into());
        }
    }
    Ok(resolved)
}

fn backend_type_name(kind: BackendType) -> &'static str {
    match kind {
        BackendType::Sqlite => "sqlite",
        BackendType::Duckdb => "duckdb",
        BackendType::Parquet => "parquet",
    }
}

fn parse_view_input(spec: &str) -> Result<ViewInput> {
    let Some((backend, relation)) = spec.split_once('=') else {
        return Err(output::CommandError::new(
            "invalid_view_input",
            "view input must use BACKEND=TABLE or BACKEND=SCHEMA.TABLE",
            json!({ "input": spec, "example": "archive=events" }),
        )
        .into());
    };
    validate_name(backend).map_err(|error| {
        output::CommandError::new(
            "invalid_view_input",
            error.to_string(),
            json!({ "input": spec, "field": "backend" }),
        )
    })?;
    validate_relation(relation).map_err(|error| {
        output::CommandError::new(
            "invalid_view_input",
            error.to_string(),
            json!({ "input": spec, "field": "relation" }),
        )
    })?;
    Ok(ViewInput {
        backend: backend.to_owned(),
        relation: relation.to_owned(),
        columns: BTreeMap::new(),
    })
}

fn parse_columns(specs: &[String]) -> Result<BTreeMap<String, String>> {
    let mut columns = BTreeMap::new();
    for spec in specs {
        let Some((name, data_type)) = spec.split_once('=') else {
            return Err(output::CommandError::new(
                "invalid_view_column",
                "logical column must use NAME=DUCKDB_TYPE",
                json!({ "column": spec, "example": "release_year=INTEGER" }),
            )
            .into());
        };
        validate_name(name).map_err(|error| {
            output::CommandError::new(
                "invalid_view_column",
                error.to_string(),
                json!({ "column": spec }),
            )
        })?;
        if columns.insert(name.to_owned(), data_type.to_owned()).is_some() {
            return Err(output::CommandError::new(
                "duplicate_view_column",
                format!("logical column '{name}' was specified more than once"),
                json!({ "column": name }),
            )
            .into());
        }
    }
    Ok(columns)
}

fn apply_mappings(
    inputs: &mut [ViewInput],
    specs: &[String],
    columns: &BTreeMap<String, String>,
) -> Result<()> {
    for spec in specs {
        let Some((backend, mapping)) = spec.split_once(':') else {
            return Err(invalid_mapping(spec).into());
        };
        let Some((logical, physical)) = mapping.split_once('=') else {
            return Err(invalid_mapping(spec).into());
        };
        if !columns.contains_key(logical) {
            return Err(output::CommandError::new(
                "unknown_logical_column",
                format!("mapping refers to undeclared logical column '{logical}'"),
                json!({ "mapping": spec, "declared_columns": columns.keys().collect::<Vec<_>>() }),
            )
            .into());
        }
        validate_name(physical).map_err(|error| {
            output::CommandError::new(
                "invalid_view_mapping",
                error.to_string(),
                json!({ "mapping": spec, "field": "physical_column" }),
            )
        })?;
        let input_backends = inputs
            .iter()
            .map(|input| input.backend.clone())
            .collect::<Vec<_>>();
        let input = inputs
            .iter_mut()
            .find(|input| input.backend == backend)
            .ok_or_else(|| {
                output::CommandError::new(
                    "mapping_backend_not_found",
                    format!("mapping refers to backend '{backend}' that is not an input"),
                    json!({
                        "mapping": spec,
                        "input_backends": input_backends,
                    }),
                )
            })?;
        if input
            .columns
            .insert(logical.to_owned(), physical.to_owned())
            .is_some()
        {
            return Err(output::CommandError::new(
                "duplicate_view_mapping",
                format!("mapping for '{backend}:{logical}' was specified more than once"),
                json!({ "mapping": spec, "backend": backend, "logical_column": logical }),
            )
            .into());
        }
    }
    Ok(())
}

fn invalid_mapping(spec: &str) -> output::CommandError {
    output::CommandError::new(
        "invalid_view_mapping",
        "view mapping must use BACKEND:LOGICAL=PHYSICAL",
        json!({ "mapping": spec, "example": "legacy:track_name=title" }),
    )
}

fn find_view<'a>(config: &'a Config, qualified_name: &str) -> Result<&'a LogicalView> {
    let (schema, name) = parse_qualified_view_name(qualified_name).map_err(|error| {
        output::CommandError::new(
            "invalid_view_name",
            error.to_string(),
            json!({ "name": qualified_name, "expected": "NAME or SCHEMA.NAME" }),
        )
    })?;
    config
        .views
        .iter()
        .find(|view| view.schema == schema && view.name == name)
        .ok_or_else(|| view_not_found(qualified_name, config).into())
}

fn view_not_found(name: &str, config: &Config) -> output::CommandError {
    output::CommandError::new(
        "view_not_found",
        format!("logical view '{name}' is not registered"),
        json!({
            "requested_name": name,
            "registered_names": config.views.iter().map(LogicalView::qualified_name).collect::<Vec<_>>(),
        }),
    )
}

fn backend_not_found(name: &str, config: &Config) -> output::CommandError {
    output::CommandError::new(
        "backend_not_found",
        format!("backend '{name}' is not registered"),
        json!({
            "requested_name": name,
            "registered_names": config.backends.iter().map(|backend| &backend.name).collect::<Vec<_>>(),
        }),
    )
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn mutate_config(function_paths: &Paths, mutate: impl FnOnce(&mut Config) -> Result<()>) -> Result<()> {
    let _lock = lock_config(function_paths)?;
    let previous = load_config(function_paths)?;
    let mut updated = previous.clone();
    mutate(&mut updated)?;
    updated.backends.sort_by(|a, b| a.name.cmp(&b.name));
    save_and_reload(function_paths, &previous, &updated)
}

fn lock_config(paths: &Paths) -> Result<std::fs::File> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&paths.config_lock)?;
    file.lock_exclusive().context("could not lock configuration")?;
    Ok(file)
}

fn save_and_reload(paths: &Paths, previous: &Config, updated: &Config) -> Result<()> {
    let init_sql = fs::read_to_string(&paths.init_sql)?;
    let probe_config = Config {
        workers: 1,
        ..updated.clone()
    };
    engine::QueryPool::new(&probe_config, &init_sql).map_err(|error| {
        output::CommandError::new(
            "configuration_validation_failed",
            "configuration could not be activated; no changes were saved",
            json!({
                "cause": format!("{error:#}"),
                "configuration_changed": false,
                "resolution": "correct the backend or logical view definition and retry",
            }),
        )
    })?;
    save_config(paths, updated)?;
    if daemon::running_pid(paths)?.is_none() {
        return Ok(());
    }
    if let Err(error) = daemon::reload(paths) {
        save_config(paths, previous).context("reload failed and configuration rollback also failed")?;
        let _ = daemon::reload(paths);
        return Err(output::CommandError::new(
            "configuration_reload_failed",
            "daemon rejected the change; the previous configuration was restored",
            json!({
                "cause": format!("{error:#}"),
                "configuration_changed": false,
                "rollback": "completed",
            }),
        )
        .into());
    }
    Ok(())
}

fn doctor(paths: &Paths) -> Result<()> {
    let config = load_cli_config(paths)?;
    let address: SocketAddr = config
        .listen
        .parse()
        .context("listen must be an IP socket address")?;
    let mut warnings = Vec::new();
    if !address.ip().is_loopback() {
        warnings.push(format!(
            "{address} is not loopback; put authentication and TLS in front of duckdoor"
        ));
    }
    let mut backends = Vec::with_capacity(config.backends.len());
    for backend in &config.backends {
        engine::test_backend(backend).map_err(|error| {
            output::CommandError::new(
                "doctor_backend_failed",
                format!("backend '{}' failed validation", backend.name),
                json!({ "backend": backend, "cause": format!("{error:#}") }),
            )
        })?;
        let mut status = serde_json::to_value(backend)?;
        let object = status
            .as_object_mut()
            .context("serialized backend was not a JSON object")?;
        object.insert("validation".to_owned(), json!("ok"));
        object.insert("read_only".to_owned(), json!(true));
        object.insert(
            "resolved_files".to_owned(),
            json!(engine::resolved_file_count(backend)?),
        );
        backends.push(status);
    }
    let views = engine::view_statuses(&config)?;
    for view in &views {
        if view.status == "unavailable" {
            warnings.push(format!(
                "logical view {} has no currently enabled and registered inputs",
                view.name
            ));
        }
    }
    let init = fs::read_to_string(&paths.init_sql)?;
    let pool = engine::QueryPool::new(
        &Config {
            workers: 1,
            ..config.clone()
        },
        &init,
    )
    .map_err(|error| {
        output::CommandError::new(
            "doctor_engine_failed",
            "query engine or logical view validation failed",
            json!({ "cause": format!("{error:#}"), "probe_workers": 1 }),
        )
    })?;
    tokio::runtime::Runtime::new()?
        .block_on(pool.query("SELECT 42 AS answer".to_owned()))
        .map_err(|error| {
            output::CommandError::new(
                "doctor_query_failed",
                "query engine probe failed",
                json!({ "cause": format!("{error:#}"), "sql": "SELECT 42 AS answer" }),
            )
        })?;
    if let Some(pid) = daemon::running_pid(paths)? {
        daemon::health(paths).with_context(|| format!("daemon pid {pid} is unhealthy"))?;
    }
    output::success(
        "doctor",
        json!({
            "status": "ok",
            "home": paths.home,
            "config": { "status": "ok", "path": paths.config },
            "listen": config.listen,
            "backends": backends,
            "views": views,
            "engine": {
                "status": "ok",
                "duckdb_version": duckdb_version(),
                "probe_workers": 1,
            },
            "daemon": daemon::status(paths)?,
            "warnings": warnings,
        }),
    )
}

fn duckdb_version() -> &'static str {
    // duckdb-rs and DuckDB use aligned release numbering; the exact engine
    // version is also visible through SELECT version().
    "1.5.5"
}
