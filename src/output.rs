use std::io::{self, Write};

use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};

pub fn success(command: &str, data: impl Serialize) -> Result<()> {
    write_json(
        io::stdout().lock(),
        &json!({
            "ok": true,
            "command": command,
            "data": data,
        }),
    )
}

pub fn error(code: &str, message: &str) {
    let _ = write_json(
        io::stderr().lock(),
        &json!({
            "ok": false,
            "error": {
                "code": code,
                "message": message,
            },
        }),
    );
}

pub fn warning(code: &str, message: &str, data: impl Serialize) {
    let _ = write_json(
        io::stderr().lock(),
        &json!({
            "level": "warning",
            "code": code,
            "message": message,
            "data": data,
        }),
    );
}

fn write_json(mut writer: impl Write, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut writer, value)?;
    writer.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_writer_emits_one_compact_document() {
        let mut bytes = Vec::new();
        write_json(&mut bytes, &json!({ "ok": true, "value": "a\nb" })).unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "{\"ok\":true,\"value\":\"a\\nb\"}\n"
        );
    }
}
