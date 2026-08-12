mod client;
mod config;
mod daemon;
mod engine;
mod server;
mod sql;

use std::{fs, fs::OpenOptions, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::{ArgAction, Parser, Subcommand};
use config::{Backend, Config, Paths, load_config, save_config, validate_name};
use fs2::FileExt;

#[derive(Debug, Parser)]
#[command(
    name = "duckdoor",
    version,
    about = "Read-only DuckDB gateway for SQLite fleets"
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
    Start,
    /// Stop the background gateway.
    Stop,
    /// Stop and start the gateway.
    Restart,
    /// Show daemon and health state.
    Status,
    /// Validate configuration, backends, and the query engine.
    Doctor,
    /// Stream structured gateway logs.
    Logs {
        #[arg(short = 'n', long, default_value_t = 100)]
        lines: usize,
        #[arg(long = "no-follow", action = ArgAction::SetFalse, default_value_t = true)]
        follow: bool,
    },
    /// List registered `SQLite` backends.
    List,
    /// Register a `SQLite` backend.
    Add {
        name: String,
        path: PathBuf,
        #[arg(long)]
        disabled: bool,
    },
    /// Remove a backend registration (the `SQLite` file is never deleted).
    Remove { name: String },
    /// Enable and hot-attach a backend.
    Enable { name: String },
    /// Disable and hot-detach a backend.
    Disable { name: String },
    /// Test a registered backend without changing it.
    Test { name: String },
    /// Hot-reload configuration and init.sql.
    Reload,
    /// Run one read-only SQL query through the daemon.
    Query {
        sql: Option<String>,
        #[arg(short, long)]
        file: Option<PathBuf>,
        #[arg(short = 'o', long, value_enum, default_value_t = client::OutputFormat::Table)]
        format: client::OutputFormat,
    },
    /// Run the HTTP service in the foreground.
    #[command(hide = true)]
    Serve,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let paths = Paths::resolve(cli.home)?;
    paths.ensure()?;
    match cli.command {
        Command::Start => daemon::start(&paths),
        Command::Stop => daemon::stop(&paths),
        Command::Restart => daemon::restart(&paths),
        Command::Status => daemon::status(&paths),
        Command::Doctor => doctor(&paths),
        Command::Logs { lines, follow } => daemon::logs(&paths, lines, follow),
        Command::List => list(&paths),
        Command::Add { name, path, disabled } => add(&paths, &name, &path, !disabled),
        Command::Remove { name } => remove(&paths, &name),
        Command::Enable { name } => set_enabled(&paths, &name, true),
        Command::Disable { name } => set_enabled(&paths, &name, false),
        Command::Test { name } => test(&paths, &name),
        Command::Reload => {
            println!("{}", serde_json::to_string_pretty(&daemon::reload(&paths)?)?);
            Ok(())
        }
        Command::Query { sql, file, format } => {
            let sql = client::read_sql(sql, file)?;
            let result = client::query(&paths, sql)?;
            client::print_result(&result, format)
        }
        Command::Serve => tokio::runtime::Runtime::new()?.block_on(serve(paths)),
    }
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
    if config.backends.is_empty() {
        println!("No backends registered.");
        return Ok(());
    }
    println!("NAME\tSTATE\tPATH");
    for backend in config.backends {
        println!(
            "{}\t{}\t{}",
            backend.name,
            if backend.enabled { "enabled" } else { "disabled" },
            backend.path.display()
        );
    }
    Ok(())
}

fn add(paths: &Paths, name: &str, path: &std::path::Path, enabled: bool) -> Result<()> {
    validate_name(name)?;
    let path = daemon::ensure_sqlite_path(path)?;
    let backend = Backend {
        name: name.to_owned(),
        path,
        enabled,
    };
    engine::test_backend(&backend).context("backend test failed")?;
    mutate_config(paths, |config| {
        if config.backends.iter().any(|item| item.name == name) {
            bail!("backend already exists: {name}");
        }
        config.backends.push(backend);
        Ok(())
    })?;
    println!("added {name} ({})", if enabled { "enabled" } else { "disabled" });
    Ok(())
}

fn remove(paths: &Paths, name: &str) -> Result<()> {
    mutate_config(paths, |config| {
        let old_len = config.backends.len();
        config.backends.retain(|item| item.name != name);
        if config.backends.len() == old_len {
            bail!("unknown backend: {name}");
        }
        Ok(())
    })?;
    println!("removed registration {name}; the SQLite file was not touched");
    Ok(())
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
        println!(
            "{name} is already {}",
            if enabled { "enabled" } else { "disabled" }
        );
        return Ok(());
    }
    backend.enabled = enabled;
    save_and_reload(paths, &previous, &config)?;
    println!("{name} {}", if enabled { "enabled" } else { "disabled" });
    Ok(())
}

fn test(paths: &Paths, name: &str) -> Result<()> {
    let config = load_config(paths)?;
    let backend = config
        .backends
        .iter()
        .find(|item| item.name == name)
        .with_context(|| format!("unknown backend: {name}"))?;
    engine::test_backend(backend)?;
    println!("ok: {} ({})", backend.name, backend.path.display());
    Ok(())
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
    println!("home: {}", paths.home.display());
    let config = load_config(paths)?;
    println!("config: ok ({})", paths.config.display());
    let address: SocketAddr = config
        .listen
        .parse()
        .context("listen must be an IP socket address")?;
    if !address.ip().is_loopback() {
        println!("warning: {address} is not loopback; put authentication and TLS in front of duckdoor");
    }
    for backend in &config.backends {
        engine::test_backend(backend).with_context(|| format!("backend {} failed", backend.name))?;
        println!(
            "backend {}: ok{}",
            backend.name,
            if backend.enabled { "" } else { " (disabled)" }
        );
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
    println!("engine: ok (DuckDB {}, 1 probe worker)", duckdb_version());
    match daemon::running_pid(paths)? {
        Some(pid) => println!("daemon: healthy (pid {pid}, {})", daemon::health(paths)?),
        None => println!("daemon: stopped (configuration is ready)"),
    }
    println!("doctor: all checks passed");
    Ok(())
}

fn duckdb_version() -> &'static str {
    // duckdb-rs and DuckDB use aligned release numbering; the exact engine
    // version is also visible through SELECT version().
    "1.5.5"
}
