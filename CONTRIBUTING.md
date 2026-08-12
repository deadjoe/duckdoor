# Contributing

Thank you for improving duckdoor.

1. Open an issue for behavior or protocol changes so the compatibility and security impact can be discussed first.
2. Keep the gateway read-only. Any path that can mutate a registered SQLite, DuckDB, or Parquet source is out of scope.
3. Add focused tests for behavior changes.
4. Run the formatting, Clippy, test, and release-build commands listed in the README.
5. Keep commits small and explain operational tradeoffs in the pull request.

Please do not include real databases, credentials, admin tokens, or logs containing sensitive paths in issues or test fixtures.
