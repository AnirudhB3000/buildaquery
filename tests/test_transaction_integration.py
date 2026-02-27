import os
from pathlib import Path
from typing import Any
from uuid import uuid4
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnDefinitionNode,
    ColumnNode,
    CreateStatementNode,
    FunctionCallNode,
    InsertStatementNode,
    LiteralNode,
    SelectStatementNode,
    StarNode,
    TableNode,
    WhereClauseNode,
)
from buildaquery.execution.cockroachdb import CockroachExecutor
from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.execution.mysql import MySqlExecutor
from buildaquery.execution.oracle import OracleExecutor
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor


# ==================================================
# Integration Configuration
# ==================================================

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")
MYSQL_DATABASE_URL = os.getenv("MYSQL_DATABASE_URL", "mysql://root:password@127.0.0.1:3307/buildaquery_test")
MARIADB_DATABASE_URL = os.getenv("MARIADB_DATABASE_URL", "mariadb://root:password@127.0.0.1:3308/buildaquery_test")
COCKROACH_DATABASE_URL = os.getenv(
    "COCKROACH_DATABASE_URL",
    "postgresql://root@127.0.0.1:26258/buildaquery_test?sslmode=disable",
)
ORACLE_DATABASE_URL = os.getenv("ORACLE_DATABASE_URL", "oracle://buildaquery:password@127.0.0.1:1522/XEPDB1")
MSSQL_DATABASE_URL = os.getenv(
    "MSSQL_DATABASE_URL",
    "mssql://sa:Password%21@127.0.0.1:1434/buildaquery_test?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes",
)

DIALECTS = ["postgres", "sqlite", "mysql", "mariadb", "cockroach", "oracle", "mssql"]


def _parse_mssql_url(url: str) -> dict[str, str | int | None]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    return {
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 1433,
        "database": parsed.path.lstrip("/") if parsed.path else None,
        "driver": query.get("driver", ["ODBC Driver 18 for SQL Server"])[0],
        "encrypt": query.get("encrypt", ["no"])[0],
        "trust_server_certificate": query.get("trust_server_certificate", ["yes"])[0],
    }


def _build_executor(dialect: str) -> Any:
    if dialect == "postgres":
        return PostgresExecutor(connection_info=DATABASE_URL)
    if dialect == "mysql":
        return MySqlExecutor(connection_info=MYSQL_DATABASE_URL)
    if dialect == "mariadb":
        return MariaDbExecutor(connection_info=MARIADB_DATABASE_URL)
    if dialect == "cockroach":
        return CockroachExecutor(connection_info=COCKROACH_DATABASE_URL)
    if dialect == "oracle":
        return OracleExecutor(connection_info=ORACLE_DATABASE_URL)
    if dialect == "mssql":
        return MsSqlExecutor(connection_info=_parse_mssql_url(MSSQL_DATABASE_URL))
    if dialect == "sqlite":
        db_dir = Path("static") / "test-sqlite"
        db_dir.mkdir(parents=True, exist_ok=True)
        db_path = db_dir / f"txn_{uuid4().hex}.sqlite"
        return SqliteExecutor(connection_info=str(db_path))
    raise ValueError(f"Unsupported dialect: {dialect}")


def _column_type_for_dialect(dialect: str) -> str:
    if dialect == "oracle":
        return "VARCHAR2(255)"
    if dialect == "mssql":
        return "NVARCHAR(255)"
    if dialect == "cockroach":
        return "STRING"
    return "TEXT"


def _drop_table(executor: Any, dialect: str, table_name: str) -> None:
    try:
        if dialect == "oracle":
            block = (
                "BEGIN "
                f"EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; "
                "EXCEPTION WHEN OTHERS THEN "
                "IF SQLCODE != -942 THEN RAISE; END IF; "
                "END;"
            )
            executor.execute_raw(block)
            return
        executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        # Best-effort cleanup for integration tests.
        pass


def _count_rows(executor: Any, table: TableNode) -> int:
    stmt = SelectStatementNode(
        select_list=[FunctionCallNode(name="COUNT", args=[StarNode()])],
        from_table=table,
    )
    result = executor.execute(stmt)
    return int(result[0][0])


def _insert_row_stmt(table: TableNode, row_id: int, value: str) -> InsertStatementNode:
    return InsertStatementNode(
        table=table,
        columns=[ColumnNode(name="id"), ColumnNode(name="value")],
        values=[LiteralNode(value=row_id), LiteralNode(value=value)],
    )


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
    ]
    return any(signal in text for signal in unavailable_signals)


@pytest.mark.parametrize("dialect", DIALECTS)
def test_transaction_commit_persists_rows(dialect: str):
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"txn_commit_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        executor.execute(
            CreateStatementNode(
                table=table,
                columns=[
                    ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                    ColumnDefinitionNode(name="value", data_type=_column_type_for_dialect(dialect)),
                ],
            )
        )
        executor.begin()
        executor.execute(_insert_row_stmt(table, 1, "persisted"))
        executor.commit()
        assert _count_rows(executor, table) == 1
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, dialect, table_name)


@pytest.mark.parametrize("dialect", DIALECTS)
def test_transaction_rollback_discards_rows(dialect: str):
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"txn_rollback_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        executor.execute(
            CreateStatementNode(
                table=table,
                columns=[
                    ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                    ColumnDefinitionNode(name="value", data_type=_column_type_for_dialect(dialect)),
                ],
            )
        )
        executor.begin()
        executor.execute(_insert_row_stmt(table, 1, "rollback"))
        executor.rollback()
        assert _count_rows(executor, table) == 0
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, dialect, table_name)


@pytest.mark.parametrize("dialect", DIALECTS)
def test_savepoint_rollback_keeps_prior_work(dialect: str):
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"txn_savepoint_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        executor.execute(
            CreateStatementNode(
                table=table,
                columns=[
                    ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                    ColumnDefinitionNode(name="value", data_type=_column_type_for_dialect(dialect)),
                ],
            )
        )
        executor.begin()
        executor.execute(_insert_row_stmt(table, 1, "keep"))
        executor.savepoint("sp1")
        executor.execute(_insert_row_stmt(table, 2, "discard"))
        executor.rollback_to_savepoint("sp1")
        executor.release_savepoint("sp1")
        executor.commit()

        assert _count_rows(executor, table) == 1
        remaining_stmt = SelectStatementNode(
            select_list=[ColumnNode(name="value")],
            from_table=table,
            where_clause=WhereClauseNode(
                condition=BinaryOperationNode(
                    left=ColumnNode(name="id"),
                    operator="=",
                    right=LiteralNode(value=1),
                )
            ),
        )
        remaining = executor.execute(remaining_stmt)
        assert remaining[0][0] == "keep"
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, dialect, table_name)
