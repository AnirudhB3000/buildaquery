import sqlite3
from pathlib import Path
import uuid

import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.errors import IntegrityConstraintError, LockTimeoutError
from buildaquery.execution.retry import RetryPolicy
from buildaquery.execution.sqlite import SqliteExecutor


@pytest.fixture()
def sqlite_retry_db() -> Path:
    db_path = Path("static/test-sqlite") / f"retry_integration_{uuid.uuid4().hex}.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, value TEXT UNIQUE)")
        conn.commit()
    finally:
        conn.close()
    try:
        yield db_path
    finally:
        if db_path.exists():
            db_path.unlink()


def test_sqlite_execute_with_retry_raises_normalized_lock_error(sqlite_retry_db: Path) -> None:
    lock_conn = sqlite3.connect(str(sqlite_retry_db), timeout=0.1)
    runner_conn = sqlite3.connect(str(sqlite_retry_db), timeout=0.1)
    executor = SqliteExecutor(connection=runner_conn)

    try:
        lock_conn.execute("BEGIN EXCLUSIVE")
        lock_conn.execute("INSERT INTO t (id, value) VALUES (?, ?)", [1, "locked"])

        with pytest.raises(LockTimeoutError):
            executor.execute_with_retry(
                CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[2, "retry"]),
                retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0.0),
            )
    finally:
        lock_conn.rollback()
        lock_conn.close()
        runner_conn.close()


def test_sqlite_execute_with_retry_does_not_retry_integrity_error(sqlite_retry_db: Path) -> None:
    conn = sqlite3.connect(str(sqlite_retry_db), timeout=0.1)
    executor = SqliteExecutor(connection=conn)

    try:
        executor.execute(CompiledQuery(sql="INSERT INTO u (id, value) VALUES (?, ?)", params=[1, "dup"]))
        with pytest.raises(IntegrityConstraintError):
            executor.execute_with_retry(
                CompiledQuery(sql="INSERT INTO u (id, value) VALUES (?, ?)", params=[2, "dup"]),
                retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.0),
            )
    finally:
        conn.close()
