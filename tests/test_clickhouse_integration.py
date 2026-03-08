import os
from typing import Any
from uuid import uuid4

import pytest

from buildaquery.abstract_syntax_tree.models import (
    ColumnNode,
    InsertStatementNode,
    LiteralNode,
    OrderByClauseNode,
    SelectStatementNode,
    TableNode,
)
from buildaquery.execution.clickhouse import ClickHouseExecutor


CLICKHOUSE_DATABASE_URL = os.getenv(
    "CLICKHOUSE_DATABASE_URL",
    "clickhouse://buildaquery:password@127.0.0.1:9001/buildaquery_test",
)


def _new_executor() -> ClickHouseExecutor:
    return ClickHouseExecutor(connection_info=CLICKHOUSE_DATABASE_URL)


def _is_backend_unavailable_error(exc: Exception) -> bool:
    text = str(exc).lower()
    unavailable_signals = [
        "connection refused",
        "actively refused",
        "no connection could be made",
        "code: 210",
        "timed out",
        "timeout",
        "network is unreachable",
        "name or service not known",
        "failed to connect",
        "clickhouse-driver",
    ]
    return any(signal in text for signal in unavailable_signals)


def test_clickhouse_basic_insert_select() -> None:
    pytest.importorskip("clickhouse_driver")
    executor = _new_executor()
    table_name = f"click_events_{uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        executor.execute_raw(
            f"CREATE TABLE {table_name} (id UInt32, value String) ENGINE = Memory"
        )
        executor.execute(
            InsertStatementNode(
                table=table,
                columns=[ColumnNode(name="id"), ColumnNode(name="value")],
                values=[LiteralNode(value=1), LiteralNode(value="a")],
            )
        )
        executor.execute_many(
            f"INSERT INTO {table_name} (id, value) VALUES (%s, %s)",
            [[2, "b"], [3, "c"]],
        )
        rows = executor.execute(
            SelectStatementNode(
                select_list=[ColumnNode(name="id"), ColumnNode(name="value")],
                from_table=table,
                order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="id"), direction="ASC")],
            )
        )
        assert rows == [(1, "a"), (2, "b"), (3, "c")]
    except Exception as exc:
        if _is_backend_unavailable_error(exc):
            pytest.skip(f"clickhouse integration unavailable: {exc}")
        raise
    finally:
        try:
            executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass
        executor.close()


def test_clickhouse_transactions_unsupported() -> None:
    executor = _new_executor()
    with pytest.raises(RuntimeError, match="does not support explicit transaction control APIs"):
        executor.begin()
    executor.close()
