import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.execution.mysql import MySqlExecutor
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor


def _test_db_path(prefix: str) -> Path:
    base = Path("static") / "test-sqlite"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{prefix}_{uuid4().hex}.sqlite"


def test_sqlite_context_manager_closes_executor() -> None:
    db_path = _test_db_path("ctx")

    try:
        with SqliteExecutor(connection_info=str(db_path)) as executor:
            executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "x"]))

        with pytest.raises(RuntimeError):
            executor.fetch_all(CompiledQuery(sql="SELECT id FROM t", params=[]))
    finally:
        if db_path.exists():
            db_path.unlink()


def test_sqlite_close_rolls_back_active_transaction() -> None:
    db_path = _test_db_path("rollback")

    try:
        executor = SqliteExecutor(connection_info=str(db_path))
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
        executor.begin()
        executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "x"]))
        executor.close()

        verify = sqlite3.connect(str(db_path))
        try:
            rows = verify.execute("SELECT id, value FROM t").fetchall()
        finally:
            verify.close()
        assert rows == []
    finally:
        if db_path.exists():
            db_path.unlink()


def test_sqlite_pool_hooks_release_connection() -> None:
    db_path = _test_db_path("pool")
    keeper = sqlite3.connect(str(db_path))
    keeper.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
    keeper.commit()

    acquired: list[sqlite3.Connection] = []
    released: list[sqlite3.Connection] = []

    def acquire() -> sqlite3.Connection:
        conn = sqlite3.connect(str(db_path))
        acquired.append(conn)
        return conn

    def release(conn: sqlite3.Connection) -> None:
        released.append(conn)
        conn.close()

    try:
        executor = SqliteExecutor(acquire_connection=acquire, release_connection=release)
        executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "pool"]))
        rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t", params=[]))

        assert rows == [(1, "pool")]
        assert len(acquired) == 2
        assert len(released) == 2
    finally:
        keeper.close()
        if db_path.exists():
            db_path.unlink()


def test_postgres_connect_timeout_is_forwarded() -> None:
    with patch("buildaquery.execution.postgres.PostgresExecutor._get_psycopg") as mock_get_psycopg:
        module = MagicMock()
        conn = MagicMock()
        cursor_cm = conn.cursor.return_value
        cursor = cursor_cm.__enter__.return_value
        cursor.fetchall.return_value = []
        module.connect.return_value = conn
        mock_get_psycopg.return_value = module

        executor = PostgresExecutor(connection_info="dsn", connect_timeout_seconds=3)
        executor.fetch_all(CompiledQuery(sql="SELECT 1", params=[]))

        module.connect.assert_called_once_with("dsn", connect_timeout=3)


def test_mysql_connect_timeout_is_forwarded() -> None:
    with patch("buildaquery.execution.mysql.MySqlExecutor._get_mysql_connector") as mock_get_connector:
        module = MagicMock()
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.fetchall.return_value = []
        module.connect.return_value = conn
        mock_get_connector.return_value = module

        executor = MySqlExecutor(
            connection_info="mysql://user:pass@localhost:3306/db",
            connect_timeout_seconds=4,
        )
        executor.fetch_all(CompiledQuery(sql="SELECT %s", params=[1]))

        assert module.connect.call_args.kwargs["connection_timeout"] == 4


def test_mssql_connect_timeout_is_forwarded() -> None:
    with patch("buildaquery.execution.mssql.MsSqlExecutor._get_pyodbc") as mock_get_pyodbc:
        module = MagicMock()
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.fetchall.return_value = []
        module.connect.return_value = conn
        mock_get_pyodbc.return_value = module

        executor = MsSqlExecutor(connection_info="mssql://user:pass@localhost:1433/db", connect_timeout_seconds=5)
        executor.fetch_all(CompiledQuery(sql="SELECT ?", params=[1]))

        assert module.connect.call_args.kwargs["timeout"] == 5
