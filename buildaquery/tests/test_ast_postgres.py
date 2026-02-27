from buildaquery.abstract_syntax_tree.models import (
    ColumnNode, TableNode, LiteralNode, BinaryOperationNode, 
    SelectStatementNode, TopClauseNode, StarNode, InsertStatementNode
)

def test_column_node_init():
    node = ColumnNode(name="id")
    assert node.name == "id"

def test_table_node_init():
    node = TableNode(name="users")
    assert node.name == "users"

def test_literal_node_init():
    node = LiteralNode(value=10)
    assert node.value == 10
    node_str = LiteralNode(value="test")
    assert node_str.value == "test"

def test_binary_operation_node_init():
    left = ColumnNode(name="age")
    right = LiteralNode(value=18)
    node = BinaryOperationNode(left=left, operator=">=", right=right)
    assert node.left == left
    assert node.operator == ">="
    assert node.right == right

def test_top_clause_node_init():
    node = TopClauseNode(count=5, direction="ASC")
    assert node.count == 5
    assert node.direction == "ASC"
    assert node.on_expression is None

def test_select_statement_node_init():
    select_list = [StarNode()]
    from_table = TableNode(name="users")
    node = SelectStatementNode(select_list=select_list, from_table=from_table)
    assert node.select_list == select_list
    assert node.from_table == from_table
    assert node.where_clause is None


def test_insert_statement_node_rows_init():
    node = InsertStatementNode(
        table=TableNode(name="users"),
        columns=[ColumnNode(name="id"), ColumnNode(name="name")],
        rows=[[LiteralNode(value=1), LiteralNode(value="alice")]],
    )
    assert node.values is None
    assert node.rows is not None
    assert len(node.rows) == 1
