import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from buildaquery.abstract_syntax_tree.models import (
    ColumnDefinitionNode,
    ColumnNode,
    CreateStatementNode,
    FunctionCallNode,
    InsertStatementNode,
    LiteralNode,
    SelectStatementNode,
    StarNode,
    TableNode,
)
from buildaquery.execution.cockroachdb import CockroachExecutor
from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.execution.mysql import MySqlExecutor
from buildaquery.execution.oracle import OracleExecutor
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")
MYSQL_DATABASE_URL = os.getenv("MYSQL_DATABASE_URL", "mysql://root:password@127.0.0.1:3307/buildaquery_test")
MARIADB_DATABASE_URL = os.getenv("MARIADB_DATABASE_URL", "mariadb://root:password@127.0.0.1:3308/buildaquery_test")
ORACLE_DATABASE_URL = os.getenv("ORACLE_DATABASE_URL", "oracle://buildaquery:password@127.0.0.1:1522/XEPDB1")
MSSQL_DATABASE_URL = os.getenv(
    "MSSQL_DATABASE_URL",
    "mssql://sa:Password%21@127.0.0.1:1434/buildaquery_test?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes",
)
COCKROACH_DATABASE_URL = os.getenv(
    "COCKROACH_DATABASE_URL",
    "postgresql://root@127.0.0.1:26258/buildaquery_test?sslmode=disable",
)

ALL_DIALECTS = ["postgres", "sqlite", "mysql", "mariadb", "cockroach", "oracle", "mssql"]


def _build_executor(dialect: str) -> Any:
    if dialect == "postgres":
        return PostgresExecutor(connection_info=DATABASE_URL)
    if dialect == "sqlite":
        db_dir = Path("static") / "test-sqlite"
        db_dir.mkdir(parents=True, exist_ok=True)
        db_path = db_dir / f"batch_{uuid4().hex}.sqlite"
        return SqliteExecutor(connection_info=str(db_path))
    if dialect == "mysql":
        return MySqlExecutor(connection_info=MYSQL_DATABASE_URL)
    if dialect == "mariadb":
        return MariaDbExecutor(connection_info=MARIADB_DATABASE_URL)
    if dialect == "cockroach":
        return CockroachExecutor(connection_info=COCKROACH_DATABASE_URL)
    if dialect == "oracle":
        return OracleExecutor(connection_info=ORACLE_DATABASE_URL)
    if dialect == "mssql":
        return MsSqlExecutor(connection_info=MSSQL_DATABASE_URL)
    raise ValueError(f"Unsupported dialect: {dialect}")


def _prepare_executor_or_skip(dialect: str) -> Any:
    try:
        return _build_executor(dialect)
    except Exception as exc:
        pytest.skip(f"{dialect} executor unavailable: {exc}")


def _is_backend_unavailable_error(exc: Exception) -> bool:
    text = str(exc).lower()
    unavailable_signals = [
        "connection refused",
        "could not connect",
        "can't connect",
        "network-related",
        "server is not found",
        "server is not accessible",
        "timed out",
        "timeout",
        "connection attempt failed",
        "ssl provider",
        "encryption not supported on the client",
        "login timeout expired",
        "tns:",
    ]
    return any(signal in text for signal in unavailable_signals)


def _column_type_for_dialect(dialect: str) -> str:
    if dialect == "cockroach":
        return "STRING"
    if dialect == "oracle":
        return "VARCHAR2(255)"
    if dialect == "mssql":
        return "NVARCHAR(255)"
    if dialect in {"mysql", "mariadb"}:
        return "VARCHAR(255)"
    return "TEXT"


def _drop_table(executor: Any, table_name: str, dialect: str) -> None:
    try:
        if dialect == "oracle":
            drop_block = (
                "BEGIN "
                f"EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; "
                "EXCEPTION WHEN OTHERS THEN "
                "IF SQLCODE != -942 THEN RAISE; END IF; "
                "END;"
            )
            executor.execute_raw(drop_block)
            return
        executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass


def _create_table(executor: Any, table: TableNode, dialect: str) -> None:
    executor.execute(
        CreateStatementNode(
            table=table,
            columns=[
                ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                ColumnDefinitionNode(name="value", data_type=_column_type_for_dialect(dialect)),
            ],
        )
    )


def _count_rows(executor: Any, table: TableNode) -> int:
    stmt = SelectStatementNode(
        select_list=[FunctionCallNode(name="COUNT", args=[StarNode()])],
        from_table=table,
    )
    rows = executor.execute(stmt)
    assert rows is not None
    return int(rows[0][0])


def _insert_template_for_dialect(dialect: str, table_name: str) -> str:
    if dialect in {"postgres", "mysql", "cockroach"}:
        return f"INSERT INTO {table_name} (id, value) VALUES (%s, %s)"
    if dialect in {"sqlite", "mariadb", "mssql"}:
        return f"INSERT INTO {table_name} (id, value) VALUES (?, ?)"
    if dialect == "oracle":
        return f"INSERT INTO {table_name} (id, value) VALUES (:1, :2)"
    raise ValueError(f"Unsupported dialect: {dialect}")


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_batch_insert_with_ast_rows(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"batch_ast_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        _create_table(executor, table, dialect)
        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                rows=[
                    [LiteralNode(value=1), LiteralNode(value="a")],
                    [LiteralNode(value=2), LiteralNode(value="b")],
                    [LiteralNode(value=3), LiteralNode(value="c")],
                ],
            )
        )
        assert _count_rows(executor, table) == 3
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name, dialect)


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_batch_insert_with_executor_execute_many(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"batch_execmany_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        _create_table(executor, table, dialect)
        sql = _insert_template_for_dialect(dialect, table_name)
        executor.execute_many(
            sql,
            [[1, "a"], [2, "b"], [3, "c"]],
        )
        assert _count_rows(executor, table) == 3
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name, dialect)
