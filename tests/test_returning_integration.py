import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnDefinitionNode,
    ColumnNode,
    CreateStatementNode,
    DeleteStatementNode,
    InsertStatementNode,
    LiteralNode,
    ReturningClauseNode,
    TableNode,
    UpdateStatementNode,
    WhereClauseNode,
)
from buildaquery.execution.cockroachdb import CockroachExecutor
from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor


# ==================================================
# Integration Configuration
# ==================================================

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")
MARIADB_DATABASE_URL = os.getenv("MARIADB_DATABASE_URL", "mariadb://root:password@127.0.0.1:3308/buildaquery_test")
MSSQL_DATABASE_URL = os.getenv(
    "MSSQL_DATABASE_URL",
    "mssql://sa:Password%21@127.0.0.1:1434/buildaquery_test?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes",
)
COCKROACH_DATABASE_URL = os.getenv(
    "COCKROACH_DATABASE_URL",
    "postgresql://root@127.0.0.1:26258/buildaquery_test?sslmode=disable",
)

INSERT_RETURNING_DIALECTS = ["postgres", "sqlite", "cockroach", "mariadb"]
UPDATE_RETURNING_DIALECTS = ["postgres", "sqlite", "cockroach"]
DELETE_RETURNING_DIALECTS = ["postgres", "sqlite", "cockroach", "mariadb"]
OUTPUT_DIALECTS = ["mssql"]

INSERT_SUPPORTED_DIALECTS = INSERT_RETURNING_DIALECTS + OUTPUT_DIALECTS
UPDATE_SUPPORTED_DIALECTS = UPDATE_RETURNING_DIALECTS + OUTPUT_DIALECTS
DELETE_SUPPORTED_DIALECTS = DELETE_RETURNING_DIALECTS + OUTPUT_DIALECTS


def _build_executor(dialect: str) -> Any:
    if dialect == "postgres":
        return PostgresExecutor(connection_info=DATABASE_URL)
    if dialect == "mariadb":
        return MariaDbExecutor(connection_info=MARIADB_DATABASE_URL)
    if dialect == "mssql":
        return MsSqlExecutor(connection_info=MSSQL_DATABASE_URL)
    if dialect == "cockroach":
        return CockroachExecutor(connection_info=COCKROACH_DATABASE_URL)
    if dialect == "sqlite":
        db_dir = Path("static") / "test-sqlite"
        db_dir.mkdir(parents=True, exist_ok=True)
        db_path = db_dir / f"returning_{uuid4().hex}.sqlite"
        return SqliteExecutor(connection_info=str(db_path))
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
    ]
    return any(signal in text for signal in unavailable_signals)


def _column_type_for_dialect(dialect: str) -> str:
    if dialect == "cockroach":
        return "STRING"
    if dialect == "mssql":
        return "NVARCHAR(255)"
    if dialect == "mariadb":
        return "VARCHAR(255)"
    return "TEXT"


def _drop_table(executor: Any, table_name: str) -> None:
    try:
        executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass


def _create_returning_table(executor: Any, dialect: str, table: TableNode) -> None:
    executor.execute(
        CreateStatementNode(
            table=table,
            columns=[
                ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                ColumnDefinitionNode(name="value", data_type=_column_type_for_dialect(dialect)),
            ],
        )
    )


@pytest.mark.parametrize("dialect", INSERT_SUPPORTED_DIALECTS)
def test_insert_returning_payload(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"returning_insert_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        _create_returning_table(executor, dialect, table)
        rows = executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value="created")],
                returning_clause=ReturningClauseNode(
                    expressions=[ColumnNode(name="id"), ColumnNode(name="value")]
                ),
            )
        )
        assert rows is not None
        assert int(rows[0][0]) == 1
        assert str(rows[0][1]) == "created"
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name)


@pytest.mark.parametrize("dialect", UPDATE_SUPPORTED_DIALECTS)
def test_update_returning_payload(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"returning_update_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        _create_returning_table(executor, dialect, table)
        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value="old")],
            )
        )
        rows = executor.execute(
            UpdateStatementNode(
                table=table,
                set_clauses={"value": LiteralNode(value="new")},
                where_clause=WhereClauseNode(
                    condition=BinaryOperationNode(
                        left=ColumnNode(name="id"),
                        operator="=",
                        right=LiteralNode(value=1),
                    )
                ),
                returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="value")]),
            )
        )
        assert rows is not None
        assert str(rows[0][0]) == "new"
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name)


@pytest.mark.parametrize("dialect", DELETE_SUPPORTED_DIALECTS)
def test_delete_returning_payload(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"returning_delete_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        _create_returning_table(executor, dialect, table)
        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value="gone")],
            )
        )
        rows = executor.execute(
            DeleteStatementNode(
                table=table,
                where_clause=WhereClauseNode(
                    condition=BinaryOperationNode(
                        left=ColumnNode(name="id"),
                        operator="=",
                        right=LiteralNode(value=1),
                    )
                ),
                returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id"), ColumnNode(name="value")]),
            )
        )
        assert rows is not None
        assert int(rows[0][0]) == 1
        assert str(rows[0][1]) == "gone"
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name)
