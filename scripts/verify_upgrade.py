"""Verify a candidate binary against synthetic DuckDB 1.5.6 data in a temporary home.

Requires the Python package duckdb==1.5.6. Never reads the user's registered data.
Usage: python scripts/verify_upgrade.py target/release/duckdoor
"""

import hashlib
import json
import pathlib
import socket
import sqlite3
import subprocess
import sys
import tempfile
import urllib.request

import duckdb

binary = str(pathlib.Path(sys.argv[1]).resolve())
assert duckdb.__version__ == '1.5.6', duckdb.__version__
with tempfile.TemporaryDirectory(prefix='duckdoor-upgrade-smoke-') as root:
    root = pathlib.Path(root)
    home = root / 'home'
    home.mkdir()
    sqlite_path = root / 'legacy.sqlite'
    native_path = root / 'legacy.duckdb'
    parquet_path = root / 'legacy.parquet'
    with sqlite3.connect(sqlite_path) as db:
        db.executescript("CREATE TABLE events(id INTEGER, label TEXT); INSERT INTO events VALUES (1, 'sqlite');")
    db = duckdb.connect(str(native_path))
    db.execute("CREATE TABLE events(id INTEGER, label VARCHAR); INSERT INTO events VALUES (2, 'duckdb-1.5.6')")
    db.execute("COPY (SELECT 3 AS id, 'parquet-1.5.6' AS label) TO ? (FORMAT PARQUET)", [str(parquet_path)])
    db.close()
    sources = [sqlite_path, native_path, parquet_path]
    hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    # Deliberately retain the legacy v1 SQLite registration without a type field.
    (home / 'config.toml').write_text(f'''version = 1
listen = "127.0.0.1:{port}"
workers = 2
threads_per_worker = 1
max_rows = 100
request_timeout_seconds = 30
[[backends]]
name = "legacy"
path = "{sqlite_path}"
enabled = true
''')
    def command(*args, success=True, parse=True):
        result = subprocess.run([binary, '--home', str(home), *map(str, args)], capture_output=True, text=True, timeout=90)
        assert (result.returncode == 0) == success, (args, result.returncode, result.stderr, result.stdout)
        if not parse:
            return result.stdout
        value = json.loads(result.stdout if success else result.stderr)
        assert value['ok'] == success, (args, value)
        return value
    started = False
    try:
        command('doctor')
        command('add', 'warehouse', native_path)
        command('add', 'archive', parquet_path, '--relation', 'events')
        command('view', 'add', 'unified.events', '--input', 'legacy=events', '--input', 'warehouse=events', '--input', 'archive=events')
        command('start')
        started = True
        expected = [[1, 'sqlite', 'legacy'], [2, 'duckdb-1.5.6', 'warehouse'], [3, 'parquet-1.5.6', 'archive']]
        sql = 'SELECT id, label, _source_backend FROM unified.events ORDER BY id'
        assert command('query', sql)['data']['rows'] == expected
        assert command('query', 'SELECT version()')['data']['rows'] == [['v2.0.0']]
        request = urllib.request.Request(f'http://127.0.0.1:{port}/v1/query', data=json.dumps({'sql': sql}).encode(), headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=30) as response:
            assert json.load(response)['rows'] == expected
        for mode in ['csv', 'jsonl', 'table']:
            output = command('query', '-o', mode, sql, parse=False)
            assert 'duckdb-1.5.6' in output, (mode, output)
        error = command('query', 'DELETE FROM legacy.events', success=False)
        assert error['error']['code'] == 'query_not_read_only'
        command('reload')
        assert command('query', sql)['data']['rows'] == expected
        command('restart')
        assert command('query', sql)['data']['rows'] == expected
        command('doctor')
        command('stop')
        started = False
        assert command('status')['data']['state'] == 'stopped'
        for p in sources:
            assert hashlib.sha256(p.read_bytes()).hexdigest() == hashes[p], f'Source changed: {p.name}'
        old_reader = duckdb.connect(str(native_path), read_only=True)
        assert old_reader.execute('SELECT * FROM events').fetchall() == [(2, 'duckdb-1.5.6')]
        old_reader.close()
        print(json.dumps({'ok': True, 'seed_engine': duckdb.__version__, 'new_engine': '2.0.0', 'checks': ['legacy-v1-config', 'sqlite', 'duckdb-1.5.6-file', 'parquet-1.5.6-file', 'federated-view', 'cli-json-csv-jsonl-table', 'http-query', 'write-rejection', 'reload', 'restart', 'doctor', 'source-sha256-unchanged', 'old-engine-can-still-read']}))
    finally:
        if started:
            command('stop')
