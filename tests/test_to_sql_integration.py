import sqlite3

from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnNode,
    InsertStatementNode,
    LiteralNode,
    SelectStatementNode,
    TableNode,
    WhereClauseNode,
)
from buildaquery.execution.sqlite import SqliteExecutor


def test_sqlite_to_sql_preview_remains_parameterized_for_hostile_input() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    users = TableNode(name="users")

    executor.execute_raw(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
        trusted=True,
    )
    executor.execute(
        InsertStatementNode(
            table=users,
            columns=[ColumnNode(name="id"), ColumnNode(name="email")],
            values=[LiteralNode(value=1), LiteralNode(value="safe@example.com")],
        )
    )

    hostile_email = "safe@example.com' OR 1=1 --"
    compiled = executor.to_sql(
        SelectStatementNode(
            select_list=[ColumnNode(name="id")],
            from_table=users,
            where_clause=WhereClauseNode(
                condition=BinaryOperationNode(
                    left=ColumnNode(name="email"),
                    operator="=",
                    right=LiteralNode(value=hostile_email),
                )
            ),
        )
    )

    assert compiled.sql == "SELECT id FROM users WHERE (email = ?)"
    assert hostile_email not in compiled.sql
    assert compiled.params == [hostile_email]
    assert executor.fetch_all(compiled) == []
    assert executor.fetch_all(
        executor.to_sql(
            SelectStatementNode(
                select_list=[ColumnNode(name="id")],
                from_table=users,
                where_clause=WhereClauseNode(
                    condition=BinaryOperationNode(
                        left=ColumnNode(name="email"),
                        operator="=",
                        right=LiteralNode(value="safe@example.com"),
                    )
                ),
            )
        )
    ) == [(1,)]
    conn.close()
