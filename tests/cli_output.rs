use std::process::Command;

use serde_json::Value;

fn duckdoor() -> Command {
    Command::new(env!("CARGO_BIN_EXE_duckdoor"))
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
}
