import pytest

from buildaquery.abstract_syntax_tree.models import (
    ColumnNode,
    ConflictTargetNode,
    DeleteStatementNode,
    InsertStatementNode,
    LiteralNode,
    LockClauseNode,
    ReturningClauseNode,
    SelectStatementNode,
    StarNode,
    TableNode,
    UpsertClauseNode,
    WhereClauseNode,
    BinaryOperationNode,
)
from buildaquery.compiler.duckdb.duckdb_compiler import DuckDbCompiler


@pytest.fixture
def compiler() -> DuckDbCompiler:
    return DuckDbCompiler()


def test_compile_select_uses_qmark_placeholders(compiler: DuckDbCompiler) -> None:
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=1),
            )
        ),
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM users WHERE (id = ?)"
    assert compiled.params == [1]


def test_compile_insert_upsert(compiler: DuckDbCompiler) -> None:
    query = InsertStatementNode(
        table=TableNode(name="users"),
        columns=[ColumnNode(name="id"), ColumnNode(name="name")],
        values=[LiteralNode(value=1), LiteralNode(value="alice")],
        upsert_clause=UpsertClauseNode(
            conflict_target=ConflictTargetNode(columns=[ColumnNode(name="id")]),
            update_columns=["name"],
        ),
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "INSERT INTO users (id, name) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET name = excluded.name"
    assert compiled.params == [1, "alice"]


def test_compile_insert_returning(compiler: DuckDbCompiler) -> None:
    query = InsertStatementNode(
        table=TableNode(name="users"),
        columns=[ColumnNode(name="name")],
        values=[LiteralNode(value="alice")],
        returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="name")]),
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "INSERT INTO users (name) VALUES (?) RETURNING name"
    assert compiled.params == ["alice"]


def test_compile_delete_returning(compiler: DuckDbCompiler) -> None:
    query = DeleteStatementNode(
        table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=2),
            )
        ),
        returning_clause=ReturningClauseNode(expressions=[StarNode()]),
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "DELETE FROM users WHERE (id = ?) RETURNING *"
    assert compiled.params == [2]


def test_compile_lock_clause_rejected(compiler: DuckDbCompiler) -> None:
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        lock_clause=LockClauseNode(mode="UPDATE"),
    )
    with pytest.raises(ValueError, match="DuckDB does not support FOR UPDATE/FOR SHARE row-lock clauses"):
        compiler.compile(query)
