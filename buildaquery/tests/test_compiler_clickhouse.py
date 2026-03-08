import pytest

from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnNode,
    DeleteStatementNode,
    InsertStatementNode,
    LiteralNode,
    LockClauseNode,
    ReturningClauseNode,
    SelectStatementNode,
    StarNode,
    TableNode,
    UpdateStatementNode,
    UpsertClauseNode,
    WhereClauseNode,
)
from buildaquery.compiler.clickhouse.clickhouse_compiler import ClickHouseCompiler


@pytest.fixture
def compiler() -> ClickHouseCompiler:
    return ClickHouseCompiler()


def test_compile_select_uses_percent_s_placeholders(compiler: ClickHouseCompiler) -> None:
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="events"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=1),
            )
        ),
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM events WHERE (id = %s)"
    assert compiled.params == [1]


def test_compile_multi_row_insert(compiler: ClickHouseCompiler) -> None:
    query = InsertStatementNode(
        table=TableNode(name="events"),
        columns=[ColumnNode(name="id"), ColumnNode(name="value")],
        rows=[
            [LiteralNode(value=1), LiteralNode(value="a")],
            [LiteralNode(value=2), LiteralNode(value="b")],
        ],
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "INSERT INTO events (id, value) VALUES (%s, %s), (%s, %s)"
    assert compiled.params == [1, "a", 2, "b"]


def test_compile_upsert_rejected(compiler: ClickHouseCompiler) -> None:
    query = InsertStatementNode(
        table=TableNode(name="events"),
        columns=[ColumnNode(name="id")],
        values=[LiteralNode(value=1)],
        upsert_clause=UpsertClauseNode(do_nothing=True),
    )
    with pytest.raises(ValueError, match="does not support upsert_clause"):
        compiler.compile(query)


def test_compile_returning_rejected(compiler: ClickHouseCompiler) -> None:
    insert_query = InsertStatementNode(
        table=TableNode(name="events"),
        columns=[ColumnNode(name="id")],
        values=[LiteralNode(value=1)],
        returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id")]),
    )
    with pytest.raises(ValueError, match="does not support RETURNING on INSERT"):
        compiler.compile(insert_query)

    update_query = UpdateStatementNode(
        table=TableNode(name="events"),
        set_clauses={"id": LiteralNode(value=2)},
        returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id")]),
    )
    with pytest.raises(ValueError, match="does not support RETURNING on UPDATE"):
        compiler.compile(update_query)

    delete_query = DeleteStatementNode(
        table=TableNode(name="events"),
        returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id")]),
    )
    with pytest.raises(ValueError, match="does not support RETURNING on DELETE"):
        compiler.compile(delete_query)


def test_compile_lock_clause_rejected(compiler: ClickHouseCompiler) -> None:
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="events"),
        lock_clause=LockClauseNode(mode="UPDATE"),
    )
    with pytest.raises(ValueError, match="does not support FOR UPDATE/FOR SHARE"):
        compiler.compile(query)
