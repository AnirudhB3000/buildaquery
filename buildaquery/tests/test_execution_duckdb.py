import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.duckdb import DuckDbExecutor


duckdb = pytest.importorskip("duckdb")


def test_duckdb_executor_execute_and_fetch() -> None:
    conn = duckdb.connect(database=":memory:")
    executor = DuckDbExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t (id INTEGER, value VARCHAR)")
    executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "a"]))

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t ORDER BY id", params=[]))
    assert rows == [(1, "a")]
    conn.close()


def test_duckdb_executor_execute_many() -> None:
    conn = duckdb.connect(database=":memory:")
    executor = DuckDbExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t_many (id INTEGER, value VARCHAR)")
    executor.execute_many("INSERT INTO t_many (id, value) VALUES (?, ?)", [[1, "a"], [2, "b"]])

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t_many ORDER BY id", params=[]))
    assert rows == [(1, "a"), (2, "b")]
    conn.close()


def test_duckdb_transaction_lifecycle() -> None:
    conn = duckdb.connect(database=":memory:")
    executor = DuckDbExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t_txn (id INTEGER, value VARCHAR)")

    executor.begin()
    executor.execute(CompiledQuery(sql="INSERT INTO t_txn (id, value) VALUES (?, ?)", params=[1, "x"]))
    try:
        executor.savepoint("sp1")
        executor.execute(CompiledQuery(sql="INSERT INTO t_txn (id, value) VALUES (?, ?)", params=[2, "y"]))
        executor.rollback_to_savepoint("sp1")
        executor.release_savepoint("sp1")
        executor.commit()
        rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t_txn ORDER BY id", params=[]))
        assert rows == [(1, "x")]
    except RuntimeError as exc:
        assert "savepoints are not supported" in str(exc).lower()
        executor.rollback()
        rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t_txn ORDER BY id", params=[]))
        assert rows == []
    conn.close()


def test_duckdb_transaction_errors() -> None:
    conn = duckdb.connect(database=":memory:")
    executor = DuckDbExecutor(connection=conn)
    with pytest.raises(RuntimeError):
        executor.commit()
    with pytest.raises(RuntimeError):
        executor.rollback()
    with pytest.raises(RuntimeError):
        executor.savepoint("sp1")
    conn.close()


def test_duckdb_savepoint_runtime_error_when_probe_marks_unsupported() -> None:
    conn = duckdb.connect(database=":memory:")
    executor = DuckDbExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t_probe (id INTEGER)")
    executor.begin()
    executor._savepoint_supported = False
    with pytest.raises(RuntimeError, match="savepoints are not supported"):
        executor.savepoint("sp1")
    executor.rollback()
    conn.close()
