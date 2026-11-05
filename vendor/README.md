# duckdb-rs compatibility patch

`duckdb/` contains the crates.io source of `duckdb` 1.20000.1, licensed under MIT.
The original archive checksum is recorded below. The crate's `.cargo_vcs_info.json`
also retains its upstream commit identity.

Source: https://crates.io/api/v1/crates/duckdb/1.20000.1/download

SHA-256: `d807ca47d4fa206f102ff568046e018fb84518eb0a0b5e65a165a2eb7bd4a7c2`

The only code change is in `src/row.rs`: `Row::value_ref_internal` explicitly
returns `ValueRef::Null` for Arrow `DataType::Null`. Arrow NullArray has no physical
validity buffer, so `is_null(row)` alone does not detect its logical nulls. The
unpatched binding panics for `SELECT NULL`, and for null-only children of lists,
structs, and maps; this can terminate a gateway query worker.

The application's `upgraded_engine_preserves_query_values_and_row_limits` test
covers scalar and nested nulls together with precision-preserving output. Keep
this patch until a tested upstream release includes the fix, then remove the
`[patch.crates-io]` entry and this directory and regenerate `Cargo.lock`.

The upstream files are otherwise unchanged; `.cargo-ok` (Cargo's local extraction
marker) is omitted and the upstream MIT license is included explicitly.
