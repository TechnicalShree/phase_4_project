"""SQLite operational store shared by the gateway, judge and monitor."""
import os
import sqlite3
from contextlib import closing
from pathlib import Path

SCHEMA = '''
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY, trace_id TEXT, span_id TEXT, question TEXT NOT NULL, answer TEXT, category TEXT,
  severity TEXT, tool_results TEXT, status TEXT NOT NULL, error TEXT, latency_ms REAL, created_at REAL DEFAULT (unixepoch('subsec')));
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, question TEXT NOT NULL, status TEXT NOT NULL, result TEXT,
  created_at REAL DEFAULT (unixepoch('subsec')), updated_at REAL);
'''


def connect():
    path = Path(os.getenv('DATA_DIR', 'data'))
    path.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path / 'ops.sqlite', timeout=10)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA journal_mode=WAL')
    con.executescript(SCHEMA)
    return con


def execute(sql, *args):
    with closing(connect()) as con, con:
        con.execute(sql, args)


def query(sql, *args):
    with closing(connect()) as con:
        return [dict(row) for row in con.execute(sql, args)]
