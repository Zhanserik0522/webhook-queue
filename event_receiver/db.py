import sqlite3
from contextlib import contextmanager
from pathlib import Path


def connect(path):
    connection = sqlite3.connect(str(path), timeout=10, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 10000")
    return connection


@contextmanager
def transaction(path):
    connection = connect(path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def init_db(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(path)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id),
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'success', 'failed', 'skipped')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
                available_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                lease_until REAL,
                lease_token TEXT,
                last_error TEXT,
                finished_at REAL,
                CHECK ((lease_until IS NULL) = (lease_token IS NULL))
            );
            CREATE INDEX IF NOT EXISTS tasks_queue
                ON tasks(status, available_at);
        """)
    finally:
        connection.close()
