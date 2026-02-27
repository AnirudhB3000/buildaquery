import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from buildaquery.abstract_syntax_tree.models import (
    ColumnDefinitionNode,
    ColumnNode,
    ConflictTargetNode,
    CreateStatementNode,
    InsertStatementNode,
    LiteralNode,
    SelectStatementNode,
    TableNode,
    UpsertClauseNode,
    WhereClauseNode,
    BinaryOperationNode,
)
from buildaquery.execution.cockroachdb import CockroachExecutor
from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.execution.mysql import MySqlExecutor
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

ON_CONFLICT_DIALECTS = ["postgres", "sqlite", "cockroach"]
ON_DUPLICATE_KEY_DIALECTS = ["mysql", "mariadb"]
ALL_DIALECTS = ON_CONFLICT_DIALECTS + ON_DUPLICATE_KEY_DIALECTS


def _build_executor(dialect: str) -> Any:
    if dialect == "postgres":
        return PostgresExecutor(connection_info=DATABASE_URL)
    if dialect == "mysql":
        return MySqlExecutor(connection_info=MYSQL_DATABASE_URL)
    if dialect == "mariadb":
        return MariaDbExecutor(connection_info=MARIADB_DATABASE_URL)
    if dialect == "cockroach":
        return CockroachExecutor(connection_info=COCKROACH_DATABASE_URL)
    if dialect == "sqlite":
        db_dir = Path("static") / "test-sqlite"
        db_dir.mkdir(parents=True, exist_ok=True)
        db_path = db_dir / f"upsert_{uuid4().hex}.sqlite"
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
    return "TEXT"


def _drop_table(executor: Any, table_name: str) -> None:
    try:
        executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        # Best-effort cleanup for integration tests.
        pass


def _create_upsert_table(executor: Any, dialect: str, table: TableNode) -> None:
    executor.execute(
        CreateStatementNode(
            table=table,
            columns=[
                ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                ColumnDefinitionNode(name="value", data_type=_column_type_for_dialect(dialect)),
            ],
        )
    )


def _select_value_for_id(executor: Any, table: TableNode, row_id: int) -> str:
    stmt = SelectStatementNode(
        select_list=[ColumnNode(name="value")],
        from_table=table,
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=row_id),
            )
        ),
    )
    rows = executor.execute(stmt)
    return str(rows[0][0])


@pytest.mark.parametrize("dialect", ON_CONFLICT_DIALECTS)
def test_upsert_do_nothing_preserves_existing_row(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"upsert_ignore_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        _create_upsert_table(executor, dialect, table)

        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value="original")],
            )
        )
        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value="updated")],
                upsert_clause=UpsertClauseNode(
                    conflict_target=ConflictTargetNode(columns=[ColumnNode(name="id")]),
                    do_nothing=True,
                ),
            )
        )

        assert _select_value_for_id(executor, table, 1) == "original"
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name)


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_upsert_update_changes_existing_row(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"upsert_update_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        _create_upsert_table(executor, dialect, table)

        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value="original")],
            )
        )

        if dialect in ON_CONFLICT_DIALECTS:
            upsert_clause = UpsertClauseNode(
                conflict_target=ConflictTargetNode(columns=[ColumnNode(name="id")]),
                update_columns=["value"],
            )
        else:
            upsert_clause = UpsertClauseNode(update_columns=["value"])

        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value="updated")],
                upsert_clause=upsert_clause,
            )
        )

        assert _select_value_for_id(executor, table, 1) == "updated"
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name)
