import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from buildaquery.abstract_syntax_tree.models import (
    AddColumnActionNode,
    AddConstraintActionNode,
    AlterTableStatementNode,
    BinaryOperationNode,
    CheckConstraintNode,
    ColumnDefinitionNode,
    ColumnNode,
    CreateIndexStatementNode,
    CreateStatementNode,
    DropColumnActionNode,
    DropIndexStatementNode,
    ForeignKeyConstraintNode,
    InsertStatementNode,
    LiteralNode,
    PrimaryKeyConstraintNode,
    SelectStatementNode,
    StarNode,
    TableNode,
    UniqueConstraintNode,
    WhereClauseNode,
    FunctionCallNode,
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
ADD_CONSTRAINT_DIALECTS = ["postgres", "mysql", "mariadb", "cockroach", "oracle", "mssql"]


def _build_executor(dialect: str) -> Any:
    if dialect == "postgres":
        return PostgresExecutor(connection_info=DATABASE_URL)
    if dialect == "sqlite":
        db_dir = Path("static") / "test-sqlite"
        db_dir.mkdir(parents=True, exist_ok=True)
        db_path = db_dir / f"ddl_{uuid4().hex}.sqlite"
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


def _text_type_for_dialect(dialect: str) -> str:
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


def _count_rows(executor: Any, table: TableNode) -> int:
    rows = executor.execute(
        SelectStatementNode(
            select_list=[FunctionCallNode(name="COUNT", args=[StarNode()])],
            from_table=table,
        )
    )
    return int(rows[0][0])


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_create_table_constraints_enforced_for_check_constraint(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    parent_name = f"ddl_parent_{dialect}_{uuid4().hex[:8]}"
    child_name = f"ddl_child_{dialect}_{uuid4().hex[:8]}"
    parent = TableNode(name=parent_name)
    child = TableNode(name=child_name)

    try:
        executor.execute(
            CreateStatementNode(
                table=parent,
                columns=[ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True)],
            )
        )
        executor.execute(
            CreateStatementNode(
                table=child,
                columns=[
                    ColumnDefinitionNode(name="id", data_type="INTEGER", not_null=True),
                    ColumnDefinitionNode(name="parent_id", data_type="INTEGER", not_null=True),
                    ColumnDefinitionNode(name="qty", data_type="INTEGER", not_null=True),
                    ColumnDefinitionNode(name="value", data_type=_text_type_for_dialect(dialect)),
                ],
                constraints=[
                    PrimaryKeyConstraintNode(columns=[ColumnNode(name="id"), ColumnNode(name="parent_id")]),
                    UniqueConstraintNode(columns=[ColumnNode(name="value"), ColumnNode(name="parent_id")]),
                    ForeignKeyConstraintNode(
                        columns=[ColumnNode(name="parent_id")],
                        reference_table=parent,
                        reference_columns=[ColumnNode(name="id")],
                        on_delete="CASCADE",
                    ),
                    CheckConstraintNode(
                        condition=BinaryOperationNode(
                            left=ColumnNode(name="qty"),
                            operator=">",
                            right=ColumnNode(name="parent_id"),
                        )
                    ),
                ],
            )
        )

        executor.execute(
            InsertStatementNode(
                table=parent,
                columns=[ColumnNode(name="id")],
                values=[LiteralNode(value=1)],
            )
        )

        executor.execute(
            InsertStatementNode(
                table=child,
                columns=[ColumnNode(name="id"), ColumnNode(name="parent_id"), ColumnNode(name="qty"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value=1), LiteralNode(value=5), LiteralNode(value="ok")],
            )
        )

        with pytest.raises(Exception):
            executor.execute(
                InsertStatementNode(
                    table=child,
                    columns=[ColumnNode(name="id"), ColumnNode(name="parent_id"), ColumnNode(name="qty"), ColumnNode(name="value")],
                    values=[LiteralNode(value=2), LiteralNode(value=3), LiteralNode(value=1), LiteralNode(value="bad")],
                )
            )

        assert _count_rows(executor, child) == 1
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, child_name, dialect)
        _drop_table(executor, parent_name, dialect)


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_create_and_drop_index(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"ddl_idx_{dialect}_{uuid4().hex[:8]}"
    index_name = f"idx_{table_name}_value"
    table = TableNode(name=table_name)

    try:
        executor.execute(
            CreateStatementNode(
                table=table,
                columns=[
                    ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                    ColumnDefinitionNode(name="value", data_type=_text_type_for_dialect(dialect)),
                ],
            )
        )

        executor.execute(
            CreateIndexStatementNode(
                name=index_name,
                table=table,
                columns=[ColumnNode(name="value")],
            )
        )

        drop_node = DropIndexStatementNode(name=index_name)
        if dialect in {"mysql", "mariadb", "mssql"}:
            drop_node.table = table

        executor.execute(drop_node)
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name, dialect)


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_alter_table_add_and_drop_column(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"ddl_alter_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        executor.execute(
            CreateStatementNode(
                table=table,
                columns=[ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True)],
            )
        )

        executor.execute(
            AlterTableStatementNode(
                table=table,
                actions=[AddColumnActionNode(column=ColumnDefinitionNode(name="note", data_type=_text_type_for_dialect(dialect)))],
            )
        )

        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="note")],
                values=[LiteralNode(value=1), LiteralNode(value="has-note")],
            )
        )

        executor.execute(
            AlterTableStatementNode(
                table=table,
                actions=[DropColumnActionNode(column_name="note")],
            )
        )

        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id")],
                values=[LiteralNode(value=2)],
            )
        )

        assert _count_rows(executor, table) == 2
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name, dialect)


@pytest.mark.parametrize("dialect", ADD_CONSTRAINT_DIALECTS)
def test_alter_table_add_unique_constraint_enforces_uniqueness(dialect: str) -> None:
    executor = _prepare_executor_or_skip(dialect)
    table_name = f"ddl_add_constraint_{dialect}_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        executor.execute(
            CreateStatementNode(
                table=table,
                columns=[ColumnDefinitionNode(name="id", data_type="INTEGER")],
            )
        )
        executor.execute(
            AlterTableStatementNode(
                table=table,
                actions=[
                    AddConstraintActionNode(
                        constraint=UniqueConstraintNode(name=f"uq_{table_name}_id", columns=[ColumnNode(name="id")])
                    )
                ],
            )
        )

        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id")],
                values=[LiteralNode(value=7)],
            )
        )
        with pytest.raises(Exception):
            executor.execute(
                InsertStatementNode(
                    table=table,
                    columns=[ColumnNode(name="id")],
                    values=[LiteralNode(value=7)],
                )
            )
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"{dialect} integration unavailable: {exc}")
        raise
    finally:
        _drop_table(executor, table_name, dialect)
