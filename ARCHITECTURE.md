# Duckdoor Architecture

This document describes the architecture implemented in duckdoor 0.2.x. It
focuses on runtime behavior, performance and safety properties, source
registration, data virtualization, lifecycle management, and the boundaries of
the current system.

## 1. Purpose and scope

Duckdoor is a single-host, read-only analytics gateway. One daemon embeds
DuckDB, keeps a bounded set of query connections warm, and makes local SQLite,
DuckDB, and Parquet data available through a small HTTP API and the same
`duckdoor` binary used as a CLI.

Its main design goals are:

- low and predictable request overhead;
- bounded concurrency instead of an unbounded latency queue;
- direct, read-only access to source data;
- persistent, non-materialized data virtualization across heterogeneous local
  sources;
- safe configuration changes without interrupting the active query pool;
- concise, structured interfaces for both people and software agents.

Duckdoor is not a database storage engine. It does not ingest, copy, replicate,
or own backend data.

## 2. System overview

```text
                          one duckdoor process

  CLI / scripts / apps       Axum HTTP server        management CLI
          |                         |                       |
          | POST /v1/query          |                       | config lock
          +------------------------>|                       | validate/save
                                    v                       |
                           +------------------+              |
                           | Engine / ArcSwap |<-------------+ reload
                           +---------+--------+
                                     |
                         atomic current-pool snapshot
                                     |
                 +-------------------+-------------------+
                 |                   |                   |
          query worker 0      query worker 1      query worker N
          DuckDB connection   DuckDB connection   DuckDB connection
                 |                   |                   |
                 +-------------------+-------------------+
                                     |
                 read-only attached or scanned sources
                     SQLite | DuckDB | Parquet
```

There is no separate DuckDB server process. DuckDB is linked into the Rust
binary through `duckdb-rs` with the bundled DuckDB build. The daemon process
owns the HTTP runtime and a fixed set of operating-system query threads. Each
query thread owns one in-memory DuckDB connection for its entire lifetime.

The in-memory connections contain catalogs, logical view definitions, macros,
and execution state. They do not contain imported copies of backend rows.

The embedded engine is the full DuckDB library rather than a reduced query
implementation. Duckdoor deliberately exposes only a restricted read-only
subset of SQL through its public query API and disables external access after
worker initialization. Engine capability and gateway policy are therefore
separate concerns.

## 3. Process and component model

The code is divided along operational boundaries:

- `main.rs` defines the CLI, management commands, configuration transactions,
  file locking, diagnostics, and command-level error responses.
- `config.rs` defines the persisted schema, path layout, defaults, validation,
  and atomic configuration writes.
- `daemon.rs` implements background process lifecycle, PID ownership, health
  checks, log following, and the authenticated reload client.
- `server.rs` implements the Axum HTTP service and maps engine failures to
  stable HTTP error codes.
- `engine.rs` implements source resolution, worker initialization, logical-view
  compilation, query admission, execution, interruption, and result conversion.
- `sql.rs` enforces the read-only statement boundary before dispatch.
- `client.rs` implements the CLI HTTP query client and JSON, JSON Lines, CSV,
  and compact table rendering.
- `output.rs` defines the stable CLI success, error, and warning envelopes.

The public executable also has a hidden `serve` subcommand. `duckdoor start`
launches the same executable in that mode, redirects stdout and stderr to the
structured log file, and waits until `/healthz` reports the expected child PID.
A locked PID file prevents two daemons from owning the same state directory.

The resolved state directory contains `config.toml`, the configuration lock,
`init.sql`, the PID file, the JSON log, and a local admin token. The directory
is created automatically on first use. `DUCKDOOR_HOME` or `--home` can select an
explicit location; otherwise duckdoor uses the platform application-data path.

### Rust execution and ownership model

The HTTP control plane is asynchronous and runs on Tokio/Axum. DuckDB's Rust
API is synchronous, so database calls are not run on Tokio executor threads.
They are isolated on named, long-lived operating-system threads instead. This
keeps blocking native execution out of the async scheduler and gives every
DuckDB connection one clear owning thread.

Rust channels define the ownership transfer for a request and its response.
`Arc` keeps a pool generation alive for in-flight work, `ArcSwap` publishes a
new generation atomically, and an atomic counter distributes admission attempts.
Configuration and API shapes are strongly typed and serialized with Serde;
Clap defines the CLI contract. Project code denies `unsafe`, while native
DuckDB remains behind its Rust binding.

## 4. Query execution path

A query follows this sequence:

1. A client sends `{ "sql": "..." }` to `POST /v1/query`. The CLI `query`
   command is an HTTP client for this endpoint.
2. Duckdoor parses the SQL with the DuckDB dialect and requires exactly one
   read-only `Query`, `EXPLAIN`, or `EXPLAIN TABLE` statement.
3. The engine loads an `Arc` to the current pool generation.
4. A round-robin starting point is selected atomically. Duckdoor tries each
   worker once and hands the request to the first idle worker.
5. The selected worker prepares and executes the SQL on its long-lived embedded
   DuckDB connection.
6. Rows are converted to JSON-compatible values until `max_rows`. One
   additional row is observed to determine whether the result was truncated.
7. The result returns through a Tokio one-shot channel and then through HTTP.

Each worker uses a zero-capacity synchronous channel. A request can be handed
off only while a worker is ready to receive it; requests are never accumulated
in an internal work queue. If every worker is occupied or unavailable, the
gateway immediately returns `query_workers_busy` with HTTP 503. This is
intentional admission control: overload is explicit and does not silently turn
into an arbitrarily long queue.

The configured query timeout is enforced around the worker response. If the
deadline expires, or if the async request is otherwise dropped, duckdoor calls
DuckDB's interrupt handle for that connection. The HTTP stack also has an outer
request deadline as an additional safeguard.

Results are currently buffered in memory and returned as one response; they are
not streamed. One query occupies one worker connection, while independent
queries can run concurrently on different workers.

## 5. Why the request path is fast and bounded

The current performance design relies on several complementary choices:

- **Embedded execution.** There is no internal database-server process or
  second network hop between duckdoor and DuckDB.
- **Warm workers.** Connections, extensions, attachments, Parquet relations,
  logical views, and macros are prepared before a worker accepts traffic.
- **Fixed concurrency.** The number of workers and DuckDB threads per worker is
  explicit. The defaults use up to eight workers and one DuckDB execution thread
  per worker, limiting CPU oversubscription and latency jitter.
- **No unbounded queue.** Zero-capacity handoff gives callers immediate,
  machine-readable backpressure when all workers are busy.
- **Low-contention selection.** An atomic counter chooses the first worker to
  try, and an idle worker can accept the request without a shared task queue.
- **Atomic pool snapshots.** The active pool is held in `ArcSwap`. Ordinary
  queries load the current `Arc` without taking the configuration lock.
- **Direct source reads.** DuckDB scans attached SQLite/DuckDB files or resolved
  Parquet files at query time. Duckdoor adds no ingestion or cache layer.
- **Optimizer-visible views.** Logical views are ordinary non-materialized
  DuckDB views. DuckDB can optimize the resulting query plan, including useful
  projection and union-branch pruning where the source query permits it.
- **Release build settings.** Release binaries use thin LTO, one code-generation
  unit, and symbol stripping.

These choices make gateway overhead small and predictable, but total latency is
still governed by the query plan, source format, storage, and data volume.
SQLite is row-oriented and may be substantially slower for broad analytical
scans than columnar Parquet or native DuckDB storage. Duckdoor does not hide
that cost with materialization.

`workers` controls concurrent connections. `threads_per_worker` controls the
DuckDB execution threads available to one connection. Increasing both can
oversubscribe the machine, so they should be tuned together against real
workloads rather than maximized independently.

## 6. Physical source registration

Backend registrations are persistent configuration records with a stable name,
type, absolute path, enabled state, and—only for Parquet—an exposed relation
name.

### SQLite

An enabled SQLite source is registered in every worker with:

```sql
ATTACH '/absolute/path/source.sqlite' AS "backend" (TYPE SQLITE, READ_ONLY);
```

Duckdoor ensures that the official DuckDB SQLite extension is installed in the
user's DuckDB extension cache, then loads it in each worker before hardening.
Tables are addressed as
`backend.table` or `backend.schema.table` where applicable.

### DuckDB

An enabled DuckDB file uses the same catalog model:

```sql
ATTACH '/absolute/path/source.duckdb' AS "backend" (TYPE DUCKDB, READ_ONLY);
```

### Parquet

A Parquet registration can point to one file, a directory, or a quoted glob. A
directory is expanded recursively. During pool construction, matching files are
canonicalized, sorted, deduplicated, and frozen as the exact file list for that
pool generation. A schema and non-materialized view expose the files:

```sql
CREATE SCHEMA IF NOT EXISTS "backend";
CREATE VIEW "backend"."relation" AS
SELECT * FROM read_parquet([/* resolved absolute files */], union_by_name = true);
```

New files matching a directory or glob do not appear inside an already active
pool. `duckdoor reload` builds a new generation and resolves the pattern again.
This keeps the active generation deterministic.

Disabled backends remain registered in configuration but are not attached or
exposed to queries. An empty backend registry is valid, so the daemon can start
first and accept hot additions later. Removing a backend only removes its
registration; duckdoor never deletes the source file. `remove --all` clears all
backend registrations in one transaction but retains logical-view definitions,
which then follow their configured missing-source policy.

## 7. Data virtualization and federated queries

Duckdoor's virtualization layer is a persistent definition of a logical view
over one or more physical relations. It is deliberately non-materialized.

For example, this command:

```sh
duckdoor view add unified.tracks \
  --input tier1=sonic_tracks \
  --input tier2=tracks \
  --input tier3=archive_tracks
```

is compiled conceptually as:

```sql
CREATE SCHEMA IF NOT EXISTS "unified";
CREATE VIEW "unified"."tracks" AS
SELECT 'tier1' AS "_source_backend", * FROM "tier1"."sonic_tracks"
UNION ALL BY NAME
SELECT 'tier2' AS "_source_backend", * FROM "tier2"."tracks"
UNION ALL BY NAME
SELECT 'tier3' AS "_source_backend", * FROM "tier3"."archive_tracks";
```

The view definition is stored in `config.toml`. It is recompiled into every new
worker at daemon start and reload. Rebuilding the definition does not scan or
copy its data; source rows are read only when a query references the view.

The default combination is `UNION ALL BY NAME`, so table names may differ and
columns align by name. `UNION ALL` is available for positional alignment.
Duckdoor does not implicitly deduplicate rows.

When physical column names or types differ, a view can declare a logical column
contract and per-input mappings. Each branch then projects the chosen physical
column, casts it to the declared DuckDB type, and aliases it to the logical
name. Unmapped logical columns use the same physical name. This gives clients a
stable schema without modifying the source databases.

The optional `_source_backend` provenance column identifies the source branch
for every row. Its name can be changed or the column can be disabled.

Each view has an explicit missing-source policy:

- `skip` omits absent or disabled inputs. If no inputs resolve, the definition
  remains persisted with status `unavailable`, but no SQL view is created and
  the daemon remains healthy.
- `error` makes pool construction fail unless every configured input is
  registered and enabled.

Enable, disable, add, or remove operations rebuild the pool, so view membership
changes as one generation. `view list`, `view show`, `view test`, and `doctor`
report resolved sources, skipped sources, and reasons such as `disabled` or
`not_registered`.

This is local federation: DuckDB plans one query across multiple local source
interfaces inside one process. It is not distributed query execution and does
not coordinate transactions across backend engines.

## 8. Startup, reload, and configuration transactions

At startup, duckdoor performs all expensive or failure-prone setup before
serving queries:

1. load and validate `config.toml` and `init.sql`;
2. resolve every enabled backend;
3. compile logical-view definitions and resolution states;
4. install SQLite support if any enabled SQLite backend requires it;
5. create every worker connection;
6. load required extensions and register all enabled sources;
7. create managed logical views;
8. validate and apply `init.sql` views and macros;
9. apply and lock DuckDB hardening settings;
10. wait until every worker reports readiness.

The daemon does not report healthy if any required worker fails initialization.

Management mutations use a configuration-file lock so concurrent CLI commands
cannot overwrite each other. Before a change is saved, duckdoor builds a
one-worker probe pool from the proposed configuration. This validates actual
source registration, view binding, and initialization SQL even while the daemon
is stopped.

The configuration file is written through a temporary file and atomically
renamed into place. If a daemon is running, the CLI then calls the authenticated
local reload endpoint. Reload builds an entire replacement pool before changing
the active pointer:

```text
active generation A serves queries
            |
            +---- build and validate generation B
                         |
                         +---- failure: A remains active
                         |
                         +---- success: atomic pointer swap to B
```

Requests that already hold generation A can finish on it. New requests load
generation B after the swap. If activation fails after the configuration write,
the CLI restores the previous file and asks the daemon to reload the previous
generation. The error response identifies the failed phase and rollback state.

The persisted configuration schema is versioned. Version-1 files created before
typed backends and logical views remain valid because omitted backend types
default to SQLite and omitted view lists default to empty.

## 9. Read-only and security model

Read-only behavior is enforced in layers:

1. SQL is parsed before dispatch, and only one query or explain statement is
   accepted. DDL, DML, `COPY`, `ATTACH`, and multi-statement requests are
   rejected.
2. The parsed AST rejects direct source-opening and SQL pass-through functions,
   including `sqlite_query`, `sqlite_scan`, `read_*`, `*_scan`, and dynamic
   `query`. Clients use only registered relations and managed logical views.
3. SQLite and DuckDB files are attached with `READ_ONLY`.
4. Parquet files are exposed only through read operations.
5. Each DuckDB connection restricts allowed paths to the exact resolved source
   files.
6. Community extensions, extension auto-installation, extension auto-loading,
   and general external access are disabled after initialization.
7. DuckDB configuration is locked after hardening.
8. `init.sql` accepts only `CREATE VIEW` and `CREATE MACRO` statements.

The reload endpoint requires a random admin token stored in the state directory
with owner-only permissions on Unix. The query endpoint itself is intentionally
unauthenticated, and the default listener is `127.0.0.1:9494`.

These controls provide defense in depth for a local trusted-user tool. They do
not make duckdoor a hostile multi-tenant SQL sandbox. A non-loopback deployment
must add authentication, authorization, TLS, network isolation, and operating-
system resource limits in front of duckdoor.

## 10. HTTP and CLI contracts

The stable HTTP surface is intentionally small:

- `GET /healthz` reports version, PID, worker count, enabled backend count, and
  active/unavailable view counts.
- `POST /v1/query` accepts one JSON object containing only `sql`.
- `POST /v1/admin/reload` rebuilds and atomically activates configuration after
  validating the admin token.

The server limits request bodies to 1 MiB. Application-level failures use
structured errors for invalid JSON, unknown routes, unsupported methods, SQL
policy failures, engine errors, engine timeouts, saturation, and reload
rejection. Query execution errors are separate from syntax/policy errors so
callers can react without parsing prose. Outer timeout and panic middleware are
last-resort safeguards and can return an HTTP status before application-level
JSON error mapping runs.

Finite CLI commands emit one compact JSON document by default:

```json
{"ok":true,"command":"status","data":{"state":"running"}}
```

Failures go to stderr, exit non-zero, and use a stable code plus a human-readable
message and optional structured details:

```json
{"ok":false,"error":{"code":"backend_not_found","message":"...","details":{}}}
```

Logs use JSON Lines because they are streams. Query output defaults to the same
JSON envelope, with JSON Lines, CSV, and a compact human table as explicit
alternatives. Query metadata contains column names and DuckDB types, returned
rows, row count, truncation state, and engine elapsed time. Values that cannot
be represented losslessly as JSON numbers, including decimals and 128-bit
integers, are strings; BLOB values are base64.

## 11. Observability and diagnostics

Daemon logs are newline-delimited JSON. Normal query completion records SQL byte
length, returned row count, and elapsed milliseconds. SQL text and result rows
are not logged. Startup, shutdown, configuration reloads, and failures are also
structured events.

`status` combines PID-file state with the live health endpoint. `doctor`
validates paths, configuration, every backend, logical-view resolution, a probe
engine, and daemon health. These commands use the same machine-readable output
contract as other finite commands.

## 12. Resource and failure boundaries

The important bounded resources are:

- `workers`: maximum simultaneously occupied query connections;
- `threads_per_worker`: DuckDB execution threads available to each connection;
- `max_rows`: maximum result rows materialized and returned;
- `request_timeout_seconds`: engine and HTTP request deadline;
- 1 MiB: maximum HTTP request body.

These bounds do not impose a memory limit on DuckDB's intermediate execution or
an operating-system CPU limit. Production use should pair realistic SQL,
source-aware tuning, and OS-level controls where stronger isolation is needed.

A backend or strict logical view that cannot be initialized prevents a new pool
from becoming active. It does not partially update some workers. During normal
reload failure the previous pool continues serving. At initial daemon startup,
there is no previous generation, so startup fails and the log contains the
cause.

## 13. Current limitations and deliberate non-goals

The current architecture deliberately does not provide:

- writes to backend data;
- materialized views, ingestion, replication, or a query-result cache;
- automatic deduplication across logical-view inputs;
- remote or distributed query execution;
- automatic discovery of new Parquet files without a reload;
- streaming HTTP result sets;
- arbitrary user DDL through the query endpoint;
- a standard DuckDB wire protocol for the stock DuckDB CLI;
- authentication on the read-only query endpoint;
- hostile multi-tenant sandboxing or per-user authorization;
- arbitrary managed joins or transformations in the logical-view CLI.

For advanced local definitions, `init.sql` can create views and macros in every
worker, subject to its restricted statement policy. Native DuckDB CLI transport
can be considered when Quack becomes an appropriate stable dependency; the
current production interface is HTTP.

## 14. Verification strategy

The implementation is checked at several levels:

- unit tests cover SQL policy, timeout/interruption behavior, source
  registration, typed result conversion, logical-view compilation, column
  mappings, missing-source policies, and backward-compatible configuration;
- CLI integration tests cover structured output and errors, typed backend
  management, logical-view persistence, hot reload, validation-before-save,
  and idempotent bulk removal without deleting source files;
- CI runs formatting, Clippy with warnings denied, all-feature tests, and a
  locked release build on Linux and macOS;
- dependency policy is checked with `cargo-deny`, and pull requests run GitHub's
  dependency review.

The repository denies unsafe Rust in project code. DuckDB itself is native code
embedded through its Rust binding, so process-level crashes in native execution
remain part of the underlying engine's trust boundary.
