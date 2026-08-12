use std::{
    fmt::Write as _,
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
};

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
    assert_eq!(value["data"]["sqlite_files_deleted"], 0);
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
