import pytest
import sqlite3
from dataclasses import dataclass

from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnNode,
    LiteralNode,
    SelectStatementNode,
    TableNode,
    WhereClauseNode,
)
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor


@dataclass
class _UserRow:
    id: int
    value: str


def test_sqlite_executor_execute_and_fetch():
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
    executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "a"]))

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t", params=[]))
    assert rows == [(1, "a")]
    conn.close()


def test_sqlite_executor_execute_many():
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE t_many (id INTEGER PRIMARY KEY, value TEXT)")

    executor.execute_many(
        "INSERT INTO t_many (id, value) VALUES (?, ?)",
        [[1, "a"], [2, "b"]],
    )

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t_many ORDER BY id", params=[]))
    assert rows == [(1, "a"), (2, "b")]
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


def test_sqlite_executor_to_sql_compiles_without_execution():
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    query = SelectStatementNode(
        select_list=[ColumnNode(name="id")],
        from_table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="email"),
                operator="=",
                right=LiteralNode(value="alice@example.com"),
            )
        ),
    )

    compiled = executor.to_sql(query)

    assert compiled.sql == "SELECT id FROM users WHERE (email = ?)"
    assert compiled.params == ["alice@example.com"]
    conn.close()


def test_sqlite_executor_fetch_all_dict_rows() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn, row_output="dict")
    executor.execute_raw("CREATE TABLE t_dict (id INTEGER PRIMARY KEY, value TEXT)")
    executor.execute(CompiledQuery(sql="INSERT INTO t_dict (id, value) VALUES (?, ?)", params=[1, "a"]))

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t_dict", params=[]))

    assert rows == [{"id": 1, "value": "a"}]
    conn.close()


def test_sqlite_executor_fetch_one_model_row() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn, row_output="model", row_model=_UserRow)
    executor.execute_raw("CREATE TABLE t_model (id INTEGER PRIMARY KEY, value TEXT)")
    executor.execute(CompiledQuery(sql="INSERT INTO t_model (id, value) VALUES (?, ?)", params=[1, "a"]))

    row = executor.fetch_one(CompiledQuery(sql="SELECT id, value FROM t_model", params=[]))

    assert row == _UserRow(id=1, value="a")
    conn.close()


def test_sqlite_executor_row_output_model_requires_row_model() -> None:
    with pytest.raises(ValueError, match="row_model is required"):
        SqliteExecutor(connection=sqlite3.connect(":memory:"), row_output="model")
