from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnNode,
    LiteralNode,
    SelectStatementNode,
    TableNode,
    WhereClauseNode,
)
from buildaquery.compiler import PostgresCompiler
from buildaquery.compiler.compiled_query import CompiledQuery


def _build_hostile_query() -> SelectStatementNode:
    return SelectStatementNode(
        select_list=[ColumnNode(name="id")],
        from_table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="email"),
                operator="=",
                right=LiteralNode(value="x'; DROP TABLE users; --"),
            )
        ),
    )


def test_compiled_query_to_sql_returns_sql_text() -> None:
    compiled = CompiledQuery(sql="SELECT 1", params=[123])

    assert compiled.to_sql() == "SELECT 1"


def test_compiler_to_sql_returns_compiled_query() -> None:
    compiler = PostgresCompiler()

    compiled = compiler.to_sql(_build_hostile_query())

    assert isinstance(compiled, CompiledQuery)
    assert compiled.sql == "SELECT id FROM users WHERE (email = %s)"
    assert compiled.params == ["x'; DROP TABLE users; --"]


def test_compiler_to_sql_keeps_hostile_literal_out_of_sql_text() -> None:
    compiler = PostgresCompiler()
    compiled = compiler.to_sql(_build_hostile_query())

    assert "DROP TABLE users" not in compiled.to_sql()
    assert compiled.params == ["x'; DROP TABLE users; --"]
