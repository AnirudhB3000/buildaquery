import pytest
from buildaquery.traversal.visitor_pattern import Visitor, Transformer
from buildaquery.abstract_syntax_tree.models import ColumnNode, TableNode, ASTNode

class MockVisitor(Visitor):
    def visit_ColumnNode(self, node: ColumnNode) -> str:
        return f"Col:{node.name}"
    
    def visit_TableNode(self, node: TableNode) -> str:
        return f"Tab:{node.name}"

def test_visitor_dispatch():
    visitor = MockVisitor()
    col = ColumnNode(name="id")
    tab = TableNode(name="users")
    
    assert visitor.visit(col) == "Col:id"
    assert visitor.visit(tab) == "Tab:users"

def test_visitor_generic_visit_raises():
    visitor = MockVisitor()
    class UnknownNode(ASTNode):
        pass
    
    with pytest.raises(NotImplementedError) as excinfo:
        visitor.visit(UnknownNode())
    assert "No visit_UnknownNode method defined" in str(excinfo.value)

def test_transformer_default_behavior():
    transformer = Transformer()
    col = ColumnNode(name="id")
    # Transformer should return the same node by default
    assert transformer.visit(col) is col

class MockTransformer(Transformer):
    def visit_ColumnNode(self, node: ColumnNode) -> ColumnNode:
        return ColumnNode(name=node.name.upper())

def test_transformer_override():
    transformer = MockTransformer()
    col = ColumnNode(name="id")
    transformed = transformer.visit(col)
    assert transformed.name == "ID"
    assert transformed is not col
