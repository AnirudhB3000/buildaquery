import sqlite3

import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.oracle import OracleExecutor
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor


def test_sqlite_executor_named_params_execute_and_fetch() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")

    executor.execute(
        CompiledQuery(
            sql="INSERT INTO users (id, email) VALUES (:id, :email)",
            params={"id": 1, "email": "alice@example.com"},
        )
    )

    row = executor.fetch_one(
        CompiledQuery(
            sql="SELECT id, email FROM users WHERE email = :email",
            params={"email": "alice@example.com"},
        )
    )

    assert row == (1, "alice@example.com")
    conn.close()


def test_executor_to_sql_rewrites_named_params() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)

    compiled = executor.to_sql(
        CompiledQuery(
            sql="SELECT id FROM users WHERE email = :email AND tenant_id = :tenant_id",
            params={"email": "alice@example.com", "tenant_id": 42},
        )
    )

    assert compiled.sql == "SELECT id FROM users WHERE email = ? AND tenant_id = ?"
    assert compiled.params == ["alice@example.com", 42]
    conn.close()


def test_execute_raw_named_params_sqlite() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
    executor.execute_raw(
        "INSERT INTO users (id, email) VALUES (:id, :email)",
        params={"id": 2, "email": "bob@example.com"},
    )

    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, email FROM users", params=[]))

    assert rows == [(2, "bob@example.com")]
    conn.close()


def test_named_param_rewriter_ignores_literals_comments_and_casts() -> None:
    executor = PostgresExecutor(connection_info="postgresql://user:password@localhost:5432/db")
    sql, params = executor._normalize_sql_params(
        "SELECT ':ignored' AS literal, :email AS bound -- :comment\n, now()::text, /* :block */ :id",
        {"email": "alice@example.com", "id": 7},
    )

    assert sql == "SELECT ':ignored' AS literal, %s AS bound -- :comment\n, now()::text, /* :block */ %s"
    assert params == ["alice@example.com", 7]


def test_oracle_named_params_rewrite_to_numbered_placeholders() -> None:
    executor = OracleExecutor(connection_info="oracle://user:password@localhost:1521/XEPDB1")

    sql, params = executor._normalize_sql_params(
        "SELECT :name, :name, :account_id FROM dual",
        {"name": "Alice", "account_id": 5},
    )

    assert sql == "SELECT :1, :2, :3 FROM dual"
    assert params == ["Alice", "Alice", 5]


def test_named_params_missing_key_raises_explicit_error() -> None:
    executor = SqliteExecutor(connection=sqlite3.connect(":memory:"))

    with pytest.raises(ValueError, match="Missing named SQL parameter: email"):
        executor._normalize_sql_params(
            "SELECT :email, :id",
            {"id": 1},
        )


def test_named_params_hostile_value_stays_out_of_sql() -> None:
    executor = SqliteExecutor(connection=sqlite3.connect(":memory:"))
    hostile = "x'; DROP TABLE users; --"

    sql, params = executor._normalize_sql_params(
        "SELECT :payload AS payload",
        {"payload": hostile},
    )

    assert "DROP TABLE users" not in sql
    assert params == [hostile]
