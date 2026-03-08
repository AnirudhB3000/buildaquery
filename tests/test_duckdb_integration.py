from pathlib import Path
from uuid import uuid4

import pytest

from buildaquery.abstract_syntax_tree.models import (
    AliasNode,
    BinaryOperationNode,
    ColumnDefinitionNode,
    ColumnNode,
    CreateStatementNode,
    CTENode,
    FunctionCallNode,
    GroupByClauseNode,
    InsertStatementNode,
    LiteralNode,
    OrderByClauseNode,
    OverClauseNode,
    SelectStatementNode,
    StarNode,
    TableNode,
    WhereClauseNode,
)
from buildaquery.execution.duckdb import DuckDbExecutor


pytest.importorskip("duckdb")


def _new_executor() -> DuckDbExecutor:
    db_dir = Path("static") / "test-duckdb"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / f"integration_{uuid4().hex}.duckdb"
    return DuckDbExecutor(connection_info=str(db_path))


def test_duckdb_basic_crud_lifecycle() -> None:
    executor = _new_executor()
    users_table = TableNode(name=f"duck_users_{uuid4().hex[:8]}")
    try:
        executor.execute(
            CreateStatementNode(
                table=users_table,
                columns=[
                    ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                    ColumnDefinitionNode(name="name", data_type="VARCHAR"),
                    ColumnDefinitionNode(name="age", data_type="INTEGER"),
                ],
            )
        )

        executor.execute(
            InsertStatementNode(
                table=users_table,
                columns=[ColumnNode(name="id"), ColumnNode(name="name"), ColumnNode(name="age")],
                values=[LiteralNode(value=1), LiteralNode(value="Alice"), LiteralNode(value=30)],
            )
        )
        executor.execute(
            InsertStatementNode(
                table=users_table,
                columns=[ColumnNode(name="id"), ColumnNode(name="name"), ColumnNode(name="age")],
                values=[LiteralNode(value=2), LiteralNode(value="Bob"), LiteralNode(value=25)],
            )
        )

        rows = executor.execute(
            SelectStatementNode(
                select_list=[ColumnNode(name="name"), ColumnNode(name="age")],
                from_table=users_table,
                order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="age"))],
            )
        )
        assert rows == [("Bob", 25), ("Alice", 30)]
    finally:
        executor.execute_raw(f"DROP TABLE IF EXISTS {users_table.name}")
        executor.close()


def test_duckdb_cte_and_window() -> None:
    executor = _new_executor()
    sales_table = TableNode(name=f"duck_sales_{uuid4().hex[:8]}")
    try:
        executor.execute(
            CreateStatementNode(
                table=sales_table,
                columns=[
                    ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                    ColumnDefinitionNode(name="agent", data_type="VARCHAR"),
                    ColumnDefinitionNode(name="amount", data_type="INTEGER"),
                ],
            )
        )
        for idx, (agent, amount) in enumerate([("A", 100), ("A", 200), ("B", 300)], start=1):
            executor.execute(
                InsertStatementNode(
                    table=sales_table,
                    columns=[ColumnNode(name="id"), ColumnNode(name="agent"), ColumnNode(name="amount")],
                    values=[LiteralNode(value=idx), LiteralNode(value=agent), LiteralNode(value=amount)],
                )
            )

        totals_cte = CTENode(
            name="agent_totals",
            subquery=SelectStatementNode(
                select_list=[
                    ColumnNode(name="agent"),
                    AliasNode(
                        expression=FunctionCallNode(name="SUM", args=[ColumnNode(name="amount")]),
                        name="total_amount",
                    ),
                ],
                from_table=sales_table,
                group_by=GroupByClauseNode(expressions=[ColumnNode(name="agent")]),
            ),
        )

        cte_rows = executor.execute(
            SelectStatementNode(
                select_list=[StarNode()],
                from_table=TableNode(name="agent_totals"),
                ctes=[totals_cte],
                where_clause=WhereClauseNode(
                    condition=BinaryOperationNode(
                        left=ColumnNode(name="total_amount"),
                        operator=">",
                        right=LiteralNode(value=250),
                    )
                ),
            )
        )
        assert len(cte_rows) == 2

        window_rows = executor.execute(
            SelectStatementNode(
                select_list=[
                    ColumnNode(name="agent"),
                    ColumnNode(name="amount"),
                    FunctionCallNode(
                        name="RANK",
                        args=[],
                        over=OverClauseNode(
                            partition_by=[ColumnNode(name="agent")],
                            order_by=[OrderByClauseNode(expression=ColumnNode(name="amount"), direction="DESC")],
                        ),
                    ),
                ],
                from_table=sales_table,
                order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="amount"), direction="DESC")],
            )
        )
        assert window_rows[0][2] == 1
    finally:
        executor.execute_raw(f"DROP TABLE IF EXISTS {sales_table.name}")
        executor.close()
