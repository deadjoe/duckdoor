mod client;
mod config;
mod daemon;
mod engine;
mod output;
mod server;
mod sql;

use std::{fs, fs::OpenOptions, net::SocketAddr, path::PathBuf, process::ExitCode};

use anyhow::{Context, Result, bail};
use clap::{ArgAction, CommandFactory, Parser, Subcommand, error::ErrorKind};
use config::{Backend, Config, Paths, load_config, save_config, validate_name};
use fs2::FileExt;
use serde_json::json;

#[derive(Debug, Parser)]
#[command(
    name = "duckdoor",
    version,
    about = "Read-only DuckDB gateway for SQLite fleets",
    arg_required_else_help = true,
    after_help = "OUTPUT:\n  Commands emit one compact JSON document by default.\n  `logs` emits JSON Lines. `query --output table|csv|jsonl` is opt-in.\n\nEXAMPLES:\n  duckdoor add app /absolute/path/app.sqlite\n  duckdoor start\n  duckdoor query 'SELECT count(*) FROM app.events'\n  duckdoor status"
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
    /// Validate configuration, backends, and the query engine.
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
    /// List registered `SQLite` backends.
    #[command(after_help = "OUTPUT:\n  One JSON document with counts and the registered backend array.")]
    List,
    /// Register a `SQLite` backend.
    #[command(after_help = "OUTPUT:\n  One JSON document with the canonical backend path and reload state.")]
    Add {
        /// Catalog name used as `name.table` in queries.
        name: String,
        /// Absolute or relative path to an existing `SQLite` file.
        path: PathBuf,
        /// Register the backend without attaching it to query workers.
        #[arg(long)]
        disabled: bool,
    },
    /// Remove one or all backend registrations (`SQLite` files are never deleted).
    #[command(
        after_help = "EXAMPLES:\n  duckdoor remove app\n  duckdoor remove --all\n\nOUTPUT:\n  One JSON document listing exactly what was unregistered. Source SQLite files are never deleted."
    )]
    Remove {
        /// Name of one registered backend to remove.
        #[arg(value_name = "NAME", required_unless_present = "all", conflicts_with = "all")]
        name: Option<String>,
        /// Atomically remove every backend registration.
        #[arg(long)]
        all: bool,
    },
    /// Enable and hot-attach a backend.
    #[command(after_help = "OUTPUT:\n  One JSON document. `changed` is false when already enabled.")]
    Enable { name: String },
    /// Disable and hot-detach a backend.
    #[command(after_help = "OUTPUT:\n  One JSON document. `changed` is false when already disabled.")]
    Disable { name: String },
    /// Test a registered backend without changing it.
    #[command(after_help = "OUTPUT:\n  One JSON document confirming read-only attach compatibility.")]
    Test { name: String },
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
        Command::Add { name, path, disabled } => add(&paths, &name, &path, !disabled),
        Command::Remove { name, all } => match (name, all) {
            (Some(name), false) => remove(&paths, &name),
            (None, true) => remove_all(&paths),
            _ => unreachable!("clap validates remove arguments"),
        },
        Command::Enable { name } => set_enabled(&paths, &name, true),
        Command::Disable { name } => set_enabled(&paths, &name, false),
        Command::Test { name } => test(&paths, &name),
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
    let config = load_config(paths)?;
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

fn add(paths: &Paths, name: &str, path: &std::path::Path, enabled: bool) -> Result<()> {
    validate_name(name)?;
    let current = load_config(paths)?;
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
    let path = daemon::ensure_sqlite_path(path)?;
    let backend = Backend {
        name: name.to_owned(),
        path,
        enabled,
    };
    engine::test_backend(&backend).context("backend test failed")?;
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
    mutate_config(paths, |config| {
        let old_len = config.backends.len();
        config.backends.retain(|item| item.name != name);
        if config.backends.len() == old_len {
            return Err(output::CommandError::new(
                "backend_not_found",
                format!("backend '{name}' is not registered"),
                json!({
                    "requested_name": name,
                    "registered_names": config.backends.iter().map(|item| &item.name).collect::<Vec<_>>(),
                }),
            )
            .into());
        }
        Ok(())
    })?;
    output::success(
        "remove",
        json!({
            "name": name,
            "registration_removed": true,
            "sqlite_file_deleted": false,
            "configuration_reloaded": daemon::running_pid(paths)?.is_some(),
        }),
    )
}

fn remove_all(paths: &Paths) -> Result<()> {
    let _lock = lock_config(paths)?;
    let previous = load_config(paths)?;
    if previous.backends.is_empty() {
        return output::success(
            "remove",
            json!({
                "scope": "all",
                "changed": false,
                "removed_count": 0,
                "removed": [],
                "sqlite_files_deleted": 0,
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
            "sqlite_files_deleted": 0,
            "configuration_reloaded": daemon_running,
        }),
    )
}

fn set_enabled(paths: &Paths, name: &str, enabled: bool) -> Result<()> {
    let _lock = lock_config(paths)?;
    let mut config = load_config(paths)?;
    let previous = config.clone();
    let backend = config
        .backends
        .iter_mut()
        .find(|item| item.name == name)
        .with_context(|| format!("unknown backend: {name}"))?;
    if enabled {
        engine::test_backend(backend).context("backend test failed")?;
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
    let config = load_config(paths)?;
    let backend = config
        .backends
        .iter()
        .find(|item| item.name == name)
        .with_context(|| format!("unknown backend: {name}"))?;
    engine::test_backend(backend)?;
    output::success(
        "test",
        json!({
            "backend": backend,
            "attach": "ok",
            "read_only": true,
        }),
    )
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
    save_config(paths, updated)?;
    if daemon::running_pid(paths)?.is_none() {
        return Ok(());
    }
    if let Err(error) = daemon::reload(paths) {
        save_config(paths, previous).context("reload failed and configuration rollback also failed")?;
        let _ = daemon::reload(paths);
        bail!("daemon rejected the change; configuration was rolled back: {error:#}");
    }
    Ok(())
}

fn doctor(paths: &Paths) -> Result<()> {
    let config = load_config(paths)?;
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
        engine::test_backend(backend).with_context(|| format!("backend {} failed", backend.name))?;
        backends.push(json!({
            "name": backend.name,
            "path": backend.path,
            "enabled": backend.enabled,
            "attach": "ok",
            "read_only": true,
        }));
    }
    let init = fs::read_to_string(&paths.init_sql)?;
    let pool = engine::QueryPool::new(
        &Config {
            workers: 1,
            ..config.clone()
        },
        &init,
    )?;
    tokio::runtime::Runtime::new()?.block_on(pool.query("SELECT 42 AS answer".to_owned()))?;
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
