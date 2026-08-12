# duckdoor

`duckdoor` is a small, read-only DuckDB gateway for a fleet of SQLite files. It keeps a bounded pool of DuckDB connections warm, attaches enabled SQLite databases to every worker, and exposes one low-latency HTTP endpoint plus a matching CLI.

```text
app-a.sqlite ─┐
app-b.sqlite ─┼─> duckdoor :9494 ─> CLI / HTTP / applications
logs.sqlite  ─┤      DuckDB
metrics.sqlite┘   direct reads at query time
```

DuckDB reads the SQLite tables directly when a query runs. `duckdoor` never copies source data and always attaches backends with `READ_ONLY`.

## Why not Quack?

[Quack](https://github.com/duckdb/duckdb-quack) is DuckDB's promising native client/server protocol, but it is currently published as an experimental pre-release. The stable path in `duckdoor` is therefore its Rust HTTP service. Quack compatibility can be added later without making registry, lifecycle, or HTTP availability depend on it.

## Install

You need Rust 1.85.1 or newer and a C++ toolchain.

```sh
cargo install --path . --locked
```

The executable contains DuckDB itself. On the first SQLite registration, DuckDB downloads its signed official `sqlite` extension into the user's DuckDB extension cache. Later starts load the cached extension; production images should warm that cache during provisioning if runtime network access is unavailable.

`duckdoor` currently targets macOS and Linux. By default its files live in the platform configuration directory. Set `DUCKDOOR_HOME` or pass `--home` to use another directory.

## Quick start

```sh
duckdoor add app_a /absolute/path/app-a.sqlite
duckdoor add logs /absolute/path/logs.sqlite --disabled
duckdoor start

duckdoor query 'SELECT count(*) FROM app_a.events'
duckdoor query 'SELECT * FROM app_a.events LIMIT 10'
duckdoor query -o table 'SELECT * FROM app_a.events LIMIT 10'
printf 'SELECT 42 AS answer' | duckdoor query -o jsonl

duckdoor enable logs
duckdoor list
duckdoor remove --all
duckdoor status
duckdoor logs
```

Backend names become DuckDB catalog names and must match `[A-Za-z_][A-Za-z0-9_]{0,62}`. Qualify SQLite tables as `backend_name.table_name`.

The management surface is deliberately compact:

```text
daemon:   start stop restart status doctor logs
backends: list add remove enable disable test
query:    query reload
```

`remove NAME` removes one registration. `remove --all` atomically clears every registration and is idempotent when the registry is already empty. Neither form deletes or changes a SQLite file. Changes made while the daemon is running are validated and hot-reloaded; a failed reload rolls the configuration back.

## CLI output contract

`duckdoor` is designed for scripts and coding agents as well as interactive use:

- Every finite command writes exactly one compact JSON document to stdout by default.
- Successful documents use `{ "ok": true, "command": "...", "data": ... }`.
- Runtime and argument errors write `{ "ok": false, "error": { "code": "...", "message": "..." } }` to stderr and exit non-zero. When available, `error.details` supplies machine-readable context such as exact usage, the conflicting backend, or registered names.
- `duckdoor logs` emits JSON Lines because it is a stream.
- `duckdoor query` defaults to JSON. `--output jsonl`, `csv`, and the compact human-only `table` format are explicit alternatives.
- Running `duckdoor` with no arguments prints help and exits successfully.

This makes outputs unambiguous and directly consumable with `jq`:

```sh
duckdoor status | jq -r '.data.state'
duckdoor list | jq '.data.backends[] | {name, enabled, path}'
duckdoor query 'SELECT count(*) AS n FROM app_a.events' | jq '.data.rows[0][0]'
```

## HTTP API

The server listens on `127.0.0.1:9494` by default.

```sh
curl -sS http://127.0.0.1:9494/healthz

curl -sS http://127.0.0.1:9494/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT count(*) AS n FROM app_a.events"}'
```

Successful HTTP query responses contain `columns`, row arrays, `row_count`, `truncated`, and `elapsed_ms`. CLI JSON wraps that response in its standard `ok`/`command`/`data` envelope. Health responses include the daemon PID so lifecycle checks cannot mistake another process on the configured port for the newly started instance. Values that JSON cannot represent losslessly—such as 128-bit integers and decimals—are strings. BLOB values are base64.

Only one read-only `SELECT`, `VALUES`, `WITH`, or `EXPLAIN` statement is accepted per request. Results are capped by `max_rows` (10,000 by default), request bodies are capped at 1 MiB, work queues are bounded, and request time is limited. The daemon installs/loads SQLite before hardening each worker, then disables external access and extension loading and locks DuckDB configuration.

### Security boundary

Treat SQL as code. `duckdoor` is a local trusted-user gateway, not a multi-tenant SQL sandbox. Keep it on loopback. If you deliberately bind it to another interface, put authentication, authorization, TLS, network isolation, and OS-level resource limits in front of it. The HTTP query route does not provide authentication by itself.

## Configuration

Run `duckdoor doctor` to print the active home and validate every backend. The generated `config.toml` resembles:

```toml
version = 1
listen = "127.0.0.1:9494"
workers = 8
threads_per_worker = 1
max_rows = 10000
request_timeout_seconds = 300

[[backends]]
name = "app_a"
path = "/absolute/path/app-a.sqlite"
enabled = true
```

The default pool has at most eight workers. Each worker owns a long-lived DuckDB connection and one DuckDB execution thread, preventing concurrent requests from multiplying CPU parallelism unpredictably. Tune both values for the workload and machine.

`init.sql` in the same directory is loaded into every new worker after backends are attached. For safety it only accepts `CREATE VIEW` and `CREATE MACRO` statements:

```sql
CREATE VIEW recent_events AS
SELECT * FROM app_a.events WHERE created_at >= current_date - INTERVAL 7 DAY;
```

Run `duckdoor reload` after editing configuration or `init.sql` manually.

Logs are newline-delimited JSON written to `duckdoor.log`; `duckdoor logs` follows the file. Query text and result data are not logged—only byte count, row count, timing, and errors.

## DuckDB CLI and Python

Use `duckdoor query` for shell scripts, or call `/v1/query` from Python or any HTTP client. The stock DuckDB CLI cannot directly speak this HTTP protocol. Native DuckDB CLI connectivity is intentionally deferred until Quack is production-ready; this avoids presenting an experimental transport as the stable gateway path.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo build --release --locked
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidance and [SECURITY.md](SECURITY.md) for private vulnerability reporting.

## License

Apache License 2.0. See [LICENSE](LICENSE).
