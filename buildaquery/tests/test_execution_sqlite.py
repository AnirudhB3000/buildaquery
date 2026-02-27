import pytest
import sqlite3

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor


def test_sqlite_executor_execute_and_fetch():
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
    executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "a"]))

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t", params=[]))
    assert rows == [(1, "a")]
    conn.close()


def test_sqlite_transaction_lifecycle_with_owned_connection():
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")

    executor.begin("IMMEDIATE")
    executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "x"]))
    executor.savepoint("sp1")
    executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[2, "y"]))
    executor.rollback_to_savepoint("sp1")
    executor.release_savepoint("sp1")
    executor.commit()

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t ORDER BY id", params=[]))
    assert rows == [(1, "x")]
    conn.close()


def test_sqlite_transaction_rollback_with_existing_connection():
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t_rb (id INTEGER PRIMARY KEY, value TEXT)")

    executor.begin()
    executor.execute(CompiledQuery(sql="INSERT INTO t_rb (id, value) VALUES (?, ?)", params=[1, "z"]))
    executor.rollback()

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t_rb", params=[]))
    assert rows == []
    conn.close()


def test_sqlite_begin_invalid_isolation_level():
    executor = SqliteExecutor(connection_info=":memory:")
    with pytest.raises(ValueError):
        executor.begin("READ COMMITTED")


def test_sqlite_transaction_errors():
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    with pytest.raises(RuntimeError):
        executor.commit()
    with pytest.raises(RuntimeError):
        executor.rollback()
    with pytest.raises(RuntimeError):
        executor.savepoint("sp1")

    executor.begin()
    with pytest.raises(RuntimeError):
        executor.begin()
    executor.rollback()
    conn.close()
