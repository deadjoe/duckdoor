use std::{
    fmt::Write as _,
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
};

use duckdb::Connection;
use serde_json::Value;

fn duckdoor() -> Command {
    Command::new(env!("CARGO_BIN_EXE_duckdoor"))
}

fn initialize_home(home: &Path) {
    let output = duckdoor()
        .args(["--home", home.to_str().unwrap(), "status"])
        .output()
        .unwrap();
    assert!(output.status.success());
}

fn seed_backends(home: &Path, port: u16, backends: &[(&str, &Path, bool)]) {
    let mut config = format!(
        "version = 1\nlisten = \"127.0.0.1:{port}\"\nworkers = 1\nthreads_per_worker = 1\nmax_rows = 10000\nrequest_timeout_seconds = 30\n",
    );
    for (name, path, enabled) in backends {
        writeln!(
            config,
            "\n[[backends]]\nname = \"{name}\"\npath = \"{}\"\nenabled = {enabled}\n",
            path.display()
        )
        .unwrap();
    }
    fs::write(home.join("config.toml"), config).unwrap();
}

fn assert_query_error(home: &Path, sql: &str, expected_code: &str) -> Value {
    let output = duckdoor()
        .args(["--home", home.to_str().unwrap(), "query", sql])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let value: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(value["error"]["code"], expected_code);
    assert_eq!(value["error"]["details"]["http_status"], 400);
    value
}

struct DaemonGuard {
    home: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = duckdoor()
            .args(["--home", self.home.to_str().unwrap(), "stop"])
            .output();
    }
}

#[test]
fn empty_command_prints_help_and_succeeds() {
    let output = duckdoor().output().unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Usage: duckdoor [OPTIONS] <COMMAND>"));
    assert!(stdout.contains("Commands emit one compact JSON document by default"));
}

#[test]
fn finite_commands_emit_one_json_document() {
    let home = tempfile::tempdir().unwrap();
    for (command, assertion) in [
        (
            "status",
            Box::new(|value: &Value| value["data"]["state"] == "stopped") as Box<dyn Fn(&Value) -> bool>,
        ),
        (
            "list",
            Box::new(|value: &Value| value["data"]["backends"] == serde_json::json!([])),
        ),
    ] {
        let output = duckdoor()
            .args(["--home", home.path().to_str().unwrap(), command])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["ok"], true);
        assert_eq!(value["command"], command);
        assert!(assertion(&value));
    }
}

#[test]
fn add_infers_and_validates_duckdb_and_parquet_sources() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    let sources = tempfile::tempdir().unwrap();
    let duckdb_path = sources.path().join("warehouse.duckdb");
    let connection = Connection::open(&duckdb_path).unwrap();
    connection
        .execute_batch("CREATE TABLE events(id INTEGER); INSERT INTO events VALUES (1)")
        .unwrap();
    drop(connection);
    let parquet_path = sources.path().join("archive.parquet");
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "COPY (SELECT 2::INTEGER AS id) TO '{}' (FORMAT PARQUET)",
            parquet_path.to_string_lossy().replace('\'', "''")
        ))
        .unwrap();
    drop(connection);

    for arguments in [
        vec!["add", "warehouse", duckdb_path.to_str().unwrap()],
        vec![
            "add",
            "archive",
            parquet_path.to_str().unwrap(),
            "--relation",
            "events",
        ],
    ] {
        let output = duckdoor()
            .arg("--home")
            .arg(home.path())
            .args(arguments)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let listed = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "list"])
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(value["data"]["backends"][0]["name"], "archive");
    assert_eq!(value["data"]["backends"][0]["type"], "parquet");
    assert_eq!(value["data"]["backends"][0]["relation"], "events");
    assert_eq!(value["data"]["backends"][1]["name"], "warehouse");
    assert_eq!(value["data"]["backends"][1]["type"], "duckdb");

    let tested = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "test", "archive"])
        .output()
        .unwrap();
    assert!(tested.status.success());
    let value: Value = serde_json::from_slice(&tested.stdout).unwrap();
    assert_eq!(value["data"]["resolved_files"], 1);
    assert_eq!(value["data"]["read_only"], true);
}

#[test]
fn logical_view_hot_reloads_with_backend_enable_state() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    seed_backends(home.path(), port, &[]);
    let source = tempfile::tempdir().unwrap();
    let path = source.path().join("warehouse.duckdb");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE events(id INTEGER); INSERT INTO events VALUES (7)")
        .unwrap();
    drop(connection);

    let started = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "start"])
        .output()
        .unwrap();
    assert!(started.status.success());
    let _guard = DaemonGuard {
        home: home.path().to_path_buf(),
    };
    let added = duckdoor()
        .arg("--home")
        .arg(home.path())
        .args(["add", "warehouse"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(added.status.success());
    let value: Value = serde_json::from_slice(&added.stdout).unwrap();
    assert_eq!(value["data"]["configuration_reloaded"], true);

    let view = duckdoor()
        .args([
            "--home",
            home.path().to_str().unwrap(),
            "view",
            "add",
            "unified.events",
            "--input",
            "warehouse=events",
        ])
        .output()
        .unwrap();
    assert!(view.status.success());
    let queried = duckdoor()
        .args([
            "--home",
            home.path().to_str().unwrap(),
            "query",
            "SELECT id, _source_backend FROM unified.events",
        ])
        .output()
        .unwrap();
    assert!(queried.status.success());
    let value: Value = serde_json::from_slice(&queried.stdout).unwrap();
    assert_eq!(value["data"]["rows"], serde_json::json!([[7, "warehouse"]]));

    let disabled = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "disable", "warehouse"])
        .output()
        .unwrap();
    assert!(disabled.status.success());
    let status = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "status"])
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(value["data"]["health"]["enabled_backends"], 0);
    assert_eq!(value["data"]["health"]["active_views"], 0);
    assert_eq!(value["data"]["health"]["unavailable_views"], 1);

    let enabled = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "enable", "warehouse"])
        .output()
        .unwrap();
    assert!(enabled.status.success());
    let queried = duckdoor()
        .args([
            "--home",
            home.path().to_str().unwrap(),
            "query",
            "SELECT count(*) AS n FROM unified.events",
        ])
        .output()
        .unwrap();
    assert!(queried.status.success());
    let value: Value = serde_json::from_slice(&queried.stdout).unwrap();
    assert_eq!(value["data"]["rows"], serde_json::json!([[1]]));
}

#[test]
fn invalid_arguments_emit_json_to_stderr() {
    let output = duckdoor().arg("unknown-command").output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let value: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(value["ok"], false);
    assert_eq!(value["error"]["code"], "invalid_arguments");
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unrecognized subcommand")
    );
    assert_eq!(value["error"]["details"]["usage"], "duckdoor [OPTIONS] <COMMAND>");
}

#[test]
fn invalid_configuration_has_a_specific_actionable_error() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    fs::write(home.path().join("config.toml"), "workers = 0\n").unwrap();
    let output = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "list"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let value: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(value["error"]["code"], "invalid_configuration");
    assert!(
        value["error"]["details"]["cause"]
            .as_str()
            .unwrap()
            .contains("workers must be between 1 and 64")
    );
    assert!(value["error"]["details"]["resolution"].is_string());
}

#[test]
fn add_missing_path_identifies_the_field_and_exact_usage() {
    let output = duckdoor().args(["add", "music"]).output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let value: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(value["error"]["code"], "invalid_arguments");
    assert!(value["error"]["message"].as_str().unwrap().contains("<PATH>"));
    assert_eq!(value["error"]["details"]["usage"], "duckdoor add <NAME> <PATH>");
}

#[test]
fn duplicate_backend_name_is_reported_before_path_validation() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    let existing = home.path().join("existing.sqlite");
    seed_backends(home.path(), 9494, &[("music", &existing, true)]);

    let attempted = home.path().join("does-not-exist.sqlite");
    let output = duckdoor()
        .args([
            "--home",
            home.path().to_str().unwrap(),
            "add",
            "music",
            attempted.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let value: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(value["error"]["code"], "backend_already_exists");
    assert_eq!(value["error"]["details"]["existing"]["name"], "music");
    assert_eq!(
        value["error"]["details"]["attempted_path"],
        attempted.to_str().unwrap()
    );
}

#[test]
fn remove_all_is_atomic_idempotent_and_preserves_source_files() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    let first = home.path().join("first.sqlite");
    let second = home.path().join("second.sqlite");
    fs::write(&first, b"first").unwrap();
    fs::write(&second, b"second").unwrap();
    seed_backends(
        home.path(),
        9494,
        &[("first", &first, true), ("second", &second, false)],
    );

    let output = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "remove", "--all"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["data"]["scope"], "all");
    assert_eq!(value["data"]["changed"], true);
    assert_eq!(value["data"]["removed_count"], 2);
    assert_eq!(value["data"]["source_files_deleted"], 0);
    assert_eq!(value["data"]["configuration_reloaded"], false);
    assert!(first.is_file());
    assert!(second.is_file());

    let repeated = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "remove", "--all"])
        .output()
        .unwrap();
    assert!(repeated.status.success());
    let value: Value = serde_json::from_slice(&repeated.stdout).unwrap();
    assert_eq!(value["data"]["changed"], false);
    assert_eq!(value["data"]["removed_count"], 0);
    assert_eq!(value["data"]["removed"], serde_json::json!([]));
}

#[test]
fn remove_all_hot_reloads_a_running_empty_gateway_once() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    seed_backends(home.path(), port, &[]);
    let started = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "start"])
        .output()
        .unwrap();
    assert!(started.status.success());
    let _guard = DaemonGuard {
        home: home.path().to_path_buf(),
    };

    let first = home.path().join("first.sqlite");
    let second = home.path().join("second.sqlite");
    fs::write(&first, b"first").unwrap();
    fs::write(&second, b"second").unwrap();
    seed_backends(
        home.path(),
        port,
        &[("first", &first, true), ("second", &second, true)],
    );
    let output = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "remove", "--all"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["data"]["removed_count"], 2);
    assert_eq!(value["data"]["configuration_reloaded"], true);

    let status = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "status"])
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(value["data"]["state"], "running");
    assert_eq!(value["data"]["health"]["enabled_backends"], 0);
    assert!(first.is_file());
    assert!(second.is_file());
}

#[test]
fn remove_requires_exactly_one_name_or_all() {
    for arguments in [vec!["remove"], vec!["remove", "app", "--all"]] {
        let output = duckdoor().args(arguments).output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let value: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(value["error"]["code"], "invalid_arguments");
        assert!(value["error"]["details"]["help"].is_string());
        assert!(value["error"]["details"]["usage"].is_string());
    }
}

#[test]
fn logical_view_commands_are_structured_and_persistent() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    let added = duckdoor()
        .args([
            "--home",
            home.path().to_str().unwrap(),
            "view",
            "add",
            "unified.events",
            "--input",
            "missing=events",
            "--no-source-column",
        ])
        .output()
        .unwrap();
    assert!(added.status.success());
    assert!(added.stderr.is_empty());
    let value: Value = serde_json::from_slice(&added.stdout).unwrap();
    assert_eq!(value["command"], "view.add");
    assert_eq!(value["data"]["resolution"]["status"], "unavailable");
    assert_eq!(
        value["data"]["resolution"]["skipped_sources"][0]["reason"],
        "not_registered"
    );

    let shown = duckdoor()
        .args([
            "--home",
            home.path().to_str().unwrap(),
            "view",
            "show",
            "unified.events",
        ])
        .output()
        .unwrap();
    assert!(shown.status.success());
    let value: Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(value["command"], "view.show");
    assert!(value["data"]["compiled_sql"].is_null());
    assert_eq!(value["data"]["view"]["inputs"][0]["relation"], "events");
}

#[test]
fn strict_missing_view_source_fails_without_saving_configuration() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    let output = duckdoor()
        .args([
            "--home",
            home.path().to_str().unwrap(),
            "view",
            "add",
            "unified.events",
            "--input",
            "missing=events",
            "--missing-source-policy",
            "error",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let value: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(value["error"]["code"], "configuration_validation_failed");
    assert_eq!(value["error"]["details"]["configuration_changed"], false);
    assert!(
        value["error"]["details"]["cause"]
            .as_str()
            .unwrap()
            .contains("unregistered backend missing")
    );

    let listed = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "view", "list"])
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(value["data"]["count"], 0);
}

#[test]
fn query_cli_preserves_specific_http_error_codes() {
    let home = tempfile::tempdir().unwrap();
    initialize_home(home.path());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    seed_backends(home.path(), port, &[]);
    let started = duckdoor()
        .args(["--home", home.path().to_str().unwrap(), "start"])
        .output()
        .unwrap();
    assert!(started.status.success());
    let _guard = DaemonGuard {
        home: home.path().to_path_buf(),
    };

    assert_query_error(
        home.path(),
        "DELETE FROM anything WHERE 1 = 0",
        "query_not_read_only",
    );
    assert_query_error(
        home.path(),
        "SELECT * FROM sqlite_query('app', 'SELECT 1')",
        "query_source_function_not_allowed",
    );
    assert_query_error(
        home.path(),
        "SELECT * FROM '/tmp/data.parquet'",
        "query_source_relation_not_allowed",
    );

    let healthy = duckdoor()
        .args([
            "--home",
            home.path().to_str().unwrap(),
            "query",
            "SELECT 42 AS answer",
        ])
        .output()
        .unwrap();
    assert!(healthy.status.success());
    let value: Value = serde_json::from_slice(&healthy.stdout).unwrap();
    assert_eq!(value["data"]["rows"][0][0], 42);

    for (sql, expected_code) in [
        ("SELEC 1", "invalid_sql"),
        (
            "SELECT * FROM table_that_does_not_exist",
            "query_execution_failed",
        ),
    ] {
        let value = assert_query_error(home.path(), sql, expected_code);
        assert!(
            !value["error"]["message"]
                .as_str()
                .unwrap()
                .contains("Unknown error code")
        );
    }

    let client = reqwest::blocking::Client::new();
    let invalid_json = client
        .post(format!("http://127.0.0.1:{port}/v1/query"))
        .header("content-type", "application/json")
        .body("{")
        .send()
        .unwrap();
    assert_eq!(invalid_json.status(), reqwest::StatusCode::BAD_REQUEST);
    let value: Value = invalid_json.json().unwrap();
    assert_eq!(value["error"]["code"], "invalid_request_json");

    let unknown = client
        .get(format!("http://127.0.0.1:{port}/missing"))
        .send()
        .unwrap();
    assert_eq!(unknown.status(), reqwest::StatusCode::NOT_FOUND);
    let value: Value = unknown.json().unwrap();
    assert_eq!(value["error"]["code"], "route_not_found");

    let wrong_method = client
        .get(format!("http://127.0.0.1:{port}/v1/query"))
        .send()
        .unwrap();
    assert_eq!(wrong_method.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    let value: Value = wrong_method.json().unwrap();
    assert_eq!(value["error"]["code"], "method_not_allowed");
}
