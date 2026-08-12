# duckdoor

`duckdoor` is a small, read-only DuckDB gateway for local SQLite, DuckDB, and Parquet data. It keeps a bounded pool of DuckDB connections warm, registers enabled sources in every worker, and exposes one low-latency HTTP endpoint plus a matching CLI. Persistent non-materialized views can present different physical tables and columns as one logical dataset.

```text
SQLite files ─┐
DuckDB files ─┼─> logical views ─> duckdoor :9494 ─> CLI / HTTP / applications
Parquet files ┘                    embedded DuckDB
```

DuckDB reads source data directly when a query runs. `duckdoor` does not copy or materialize it. SQLite and DuckDB files are attached with `READ_ONLY`; Parquet is exposed through `read_parquet` with only registered files allowed.

For the process model, query path, worker-pool design, reload semantics, data-virtualization implementation, and security boundaries, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Why not Quack?

[Quack](https://github.com/duckdb/duckdb-quack) is DuckDB's promising native client/server protocol, but it is currently published as an experimental pre-release. The stable path in `duckdoor` is therefore its Rust HTTP service. Quack compatibility can be added later without making registry, lifecycle, or HTTP availability depend on it.

## Install

You need Rust 1.85.1 or newer and a C++ toolchain.

```sh
cargo install --path . --locked
```

The executable contains DuckDB itself. On the first SQLite registration, DuckDB downloads its signed official `sqlite` extension into the user's DuckDB extension cache. Later starts load the cached extension; production images should warm that cache during provisioning if runtime network access is unavailable. Native DuckDB and Parquet sources do not need that SQLite extension.

`duckdoor` currently targets macOS and Linux. By default its files live in the platform configuration directory. Set `DUCKDOOR_HOME` or pass `--home` to use another directory.

### Safely update a deployed binary

Stop the daemon before replacing its executable. Copy an update to a new file in the same directory, verify that candidate, and then rename it over the installed path. The rename is atomic; do not use `scp` to overwrite the executable in place while duckdoor is running. On macOS, modifying a running Mach-O file in place can make the kernel reject it with a code-signing error and `SIGKILL`.

For example, from the build machine, replacing `~/.local/bin/duckdoor` on `HOST`:

```sh
ssh HOST '~/.local/bin/duckdoor stop'
scp target/release/duckdoor HOST:~/.local/bin/duckdoor.new

ssh HOST '
  chmod 755 ~/.local/bin/duckdoor.new &&
  ~/.local/bin/duckdoor.new --version &&
  mv ~/.local/bin/duckdoor.new ~/.local/bin/duckdoor &&
  ~/.local/bin/duckdoor start &&
  ~/.local/bin/duckdoor doctor
'
```

Compare the candidate's SHA-256 checksum with the build artifact before the rename. On macOS, `codesign --verify --verbose=4 ~/.local/bin/duckdoor.new` provides an additional check. Replacing the executable does not change the configuration, registered backend paths, or source files.

## Quick start

```sh
duckdoor add app_a /absolute/path/app-a.sqlite
duckdoor add warehouse /absolute/path/warehouse.duckdb
duckdoor add archive '/data/archive/**/*.parquet' --type parquet --relation events
duckdoor add logs /absolute/path/logs.sqlite --disabled
duckdoor start

duckdoor query 'SELECT count(*) FROM app_a.events'
duckdoor query 'SELECT * FROM app_a.events LIMIT 10'
duckdoor query -o table 'SELECT * FROM app_a.events LIMIT 10'
printf 'SELECT 42 AS answer' | duckdoor query -o jsonl

duckdoor enable logs
duckdoor list
duckdoor view add unified.events \
  --input app_a=events \
  --input archive=events
duckdoor view test unified.events
duckdoor query 'SELECT * FROM unified.events LIMIT 10'
duckdoor remove --all
duckdoor status
duckdoor logs
```

Backend names must match `[A-Za-z_][A-Za-z0-9_]{0,62}`. SQLite and DuckDB tables are queried as `backend_name.table_name`. A Parquet backend exposes `backend_name.relation_name`, where the relation defaults to `data`.

The type is inferred for existing `.sqlite`, `.sqlite3`, `.db`, `.duckdb`, and `.parquet` files. Directories and glob patterns require `--type parquet`; quote globs so the shell does not expand them. Duckdoor stores absolute paths. Parquet globs are resolved to a deterministic file list during add, start, or reload, so newly added files require `duckdoor reload`.

The management surface is deliberately compact:

```text
daemon:   start stop restart status doctor logs
backends: list add remove enable disable test
views:    view add/list/show/test/enable/disable/remove
query:    query reload
```

`remove NAME` removes one registration. `remove --all` atomically clears every backend registration and is idempotent when the registry is already empty. Neither form deletes or changes source files. Every configuration change is validated by building a one-worker probe before it is saved, including while the daemon is stopped. A running daemon then builds a complete replacement pool and atomically swaps it in; failed activation restores the previous configuration and pool.

## Logical views and federated queries

A logical view persists a query relationship, not data. Duckdoor recreates its lightweight definition in every worker at start or reload; the underlying SQLite, DuckDB, or Parquet data is read only when a query references the view.

Physical table names may differ:

```sh
duckdoor view add unified.tracks \
  --input sonic_tier1=sonic_tracks \
  --input sonic_tier2=tracks \
  --input sonic_archive=archive_tracks
```

By default inputs are combined with `UNION ALL BY NAME` and each row receives `_source_backend`. Use `--mode union-all` for positional union or `--no-source-column` to omit provenance. Duckdoor never performs implicit deduplication.

When physical column names or SQLite types differ, declare a stable logical schema and source mappings:

```sh
duckdoor view add unified.tracks \
  --input legacy=recordings \
  --input current=tracks \
  --column isrc=VARCHAR \
  --column track_name=VARCHAR \
  --column release_year=INTEGER \
  --map legacy:isrc=recording_code \
  --map legacy:track_name=title \
  --map legacy:release_year=year
```

Unmapped logical columns use the same physical name. Duckdoor emits explicit casts for the declared types and validates table binding in a real probe worker before saving the definition.

The default `--missing-source-policy skip` omits unregistered or disabled inputs. A view with no resolved inputs is persisted as `unavailable`; the daemon remains healthy, while `view list`, `view show`, and `doctor` explain why it is not queryable. Use `--missing-source-policy error` when every input must be present and enabled.

```sh
duckdoor view list
duckdoor view show unified.tracks
duckdoor view test unified.tracks
duckdoor view disable unified.tracks
duckdoor view enable unified.tracks
duckdoor view remove unified.tracks
```

`view show` includes the persisted definition, resolved/skipped sources, and generated SQL. The query API remains strictly read-only; it does not accept `CREATE VIEW` because a request is handled by only one worker and could not safely update the whole pool.

## CLI output contract

`duckdoor` is designed for scripts and coding agents as well as interactive use:

- Every finite command writes exactly one compact JSON document to stdout by default.
- Successful documents use `{ "ok": true, "command": "...", "data": ... }`.
- Runtime and argument errors write `{ "ok": false, "error": { "code": "...", "message": "..." } }` to stderr and exit non-zero. Stable codes distinguish argument, path, backend validation, view validation, reload, query syntax, read-only-policy, execution, timeout, and worker-capacity failures. When available, `error.details` supplies machine-readable context, a cause, whether configuration changed, and a suggested resolution.
- `duckdoor logs` emits JSON Lines because it is a stream.
- `duckdoor query` defaults to JSON. `--output jsonl`, `csv`, and the compact human-only `table` format are explicit alternatives.
- Running `duckdoor` with no arguments prints help and exits successfully.

This makes outputs unambiguous and directly consumable with `jq`:

```sh
duckdoor status | jq -r '.data.state'
duckdoor list | jq '.data.backends[] | {name, enabled, path}'
duckdoor view list | jq '.data.views[] | {name, status, skipped_sources}'
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

HTTP failures use the same structured envelope. For example, rejected DDL is `query_not_read_only`, a direct external/pass-through source function is `query_source_function_not_allowed`, a path-like table reference is `query_source_relation_not_allowed`, malformed SQL is `invalid_sql`, a DuckDB binder/runtime error is `query_execution_failed`, saturation is `query_workers_busy`, and an elapsed query is `query_timeout`. Invalid JSON, unknown routes, and wrong HTTP methods also return specific JSON errors.

Only one read-only `SELECT`, `VALUES`, `WITH`, or `EXPLAIN` statement is accepted per request. Source-opening and SQL pass-through functions such as `sqlite_query`, `sqlite_scan`, `read_parquet`, and `query` are rejected. Path-like table references such as `FROM '/data/events.parquet'` are also rejected so DuckDB replacement scans cannot bypass backend relation names; clients query only registered backend relations and managed logical views. Results are capped by `max_rows` (10,000 by default), request bodies are capped at 1 MiB, work queues are bounded, and request time is limited. When needed, the daemon loads SQLite support before hardening each worker; it then limits file access to registered source files, disables extension loading, and locks DuckDB configuration.

### Security boundary

Treat SQL as code. `duckdoor` is a local trusted-user gateway, not a multi-tenant SQL sandbox. Keep it on loopback. If you deliberately bind it to another interface, put authentication, authorization, TLS, network isolation, and OS-level resource limits in front of it. The HTTP query route does not provide authentication by itself.

## Configuration

Run `duckdoor doctor` to print the active home and validate every backend, logical view, and probe worker. The generated `config.toml` resembles:

```toml
version = 1
listen = "127.0.0.1:9494"
workers = 8
threads_per_worker = 1
max_rows = 10000
request_timeout_seconds = 300

[[backends]]
name = "app_a"
type = "sqlite"
path = "/absolute/path/app-a.sqlite"
enabled = true

[[backends]]
name = "archive"
type = "parquet"
path = "/absolute/path/archive/**/*.parquet"
relation = "tracks"
enabled = true

[[views]]
name = "tracks"
schema = "unified"
enabled = true
mode = "union_all_by_name"
missing_source_policy = "skip"
source_column = "_source_backend"

[[views.inputs]]
backend = "app_a"
relation = "sonic_tracks"

[[views.inputs]]
backend = "archive"
relation = "tracks"
```

Existing version-1 configurations without a backend `type` remain compatible and default to `sqlite`. CLI management is preferred over hand-editing because it validates and atomically activates changes.

The default pool has at most eight workers. Each worker owns a long-lived DuckDB connection and one DuckDB execution thread, preventing concurrent requests from multiplying CPU parallelism unpredictably. Tune both values for the workload and machine.

`init.sql` in the same directory remains an advanced escape hatch. It is loaded into every new worker after registered logical views and accepts only `CREATE VIEW` and `CREATE MACRO` statements:

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
cargo test --all-features --locked
cargo build --release --locked
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidance and [SECURITY.md](SECURITY.md) for private vulnerability reporting.

## License

Apache License 2.0. See [LICENSE](LICENSE).
