import pytest
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, LiteralNode, 
    BinaryOperationNode, WhereClauseNode, StarNode, TopClauseNode,
    OrderByClauseNode, GroupByClauseNode, HavingClauseNode
)

@pytest.fixture
def compiler():
    return PostgresCompiler()

def test_compile_simple_select(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users")
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM users"
    assert compiled.params == []

def test_compile_where_with_params(compiler):
    query = SelectStatementNode(
        select_list=[ColumnNode(name="name")],
        from_table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="age"),
                operator=">",
                right=LiteralNode(value=25)
            )
        )
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT name FROM users WHERE (age > %s)"
    assert compiled.params == [25]

def test_compile_multiple_params(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="products"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=BinaryOperationNode(
                    left=ColumnNode(name="price"),
                    operator=">",
                    right=LiteralNode(value=100)
                ),
                operator="AND",
                right=BinaryOperationNode(
                    left=ColumnNode(name="category"),
                    operator="=",
                    right=LiteralNode(value="electronics")
                )
            )
        )
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM products WHERE ((price > %s) AND (category = %s))"
    assert compiled.params == [100, "electronics"]

def test_compile_order_by(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="id"), direction="DESC")]
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM users ORDER BY id DESC"

def test_compile_top_translation(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        top_clause=TopClauseNode(count=10, on_expression=ColumnNode(name="score"), direction="DESC")
    )
    compiled = compiler.compile(query)
    # Note: Our implementation adds ORDER BY if not present and maps TOP to LIMIT
    assert compiled.sql == "SELECT * FROM users ORDER BY score DESC LIMIT 10"

def test_compile_top_vs_limit_error(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        top_clause=TopClauseNode(count=10),
        limit=5
    )
    with pytest.raises(ValueError, match="TOP clause is mutually exclusive with LIMIT and OFFSET"):
        compiler.compile(query)

def test_compile_group_by_having(compiler):
    query = SelectStatementNode(
        select_list=[ColumnNode(name="dept"), ColumnNode(name="COUNT(*)")],
        from_table=TableNode(name="employees"),
        group_by=GroupByClauseNode(expressions=[ColumnNode(name="dept")]),
        having_clause=HavingClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="COUNT(*)"),
                operator=">",
                right=LiteralNode(value=5)
            )
        )
    )
    compiled = compiler.compile(query)
    assert "GROUP BY dept" in compiled.sql
    assert "HAVING (COUNT(*) > %s)" in compiled.sql
    assert compiled.params == [5]
