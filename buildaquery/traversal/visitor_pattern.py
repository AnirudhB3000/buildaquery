from typing import Any
from buildaquery.abstract_syntax_tree.models import ASTNode

class Visitor:
    """
    A base class for traversing the Abstract Syntax Tree.
    """
    def visit(self, node: ASTNode) -> Any:
        """
        The entry point for visiting a node. Dispatches to the correct visit method.
        """
        method_name = f'visit_{node.__class__.__name__}'
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node: ASTNode) -> Any:
        """
        Called if no explicit visit method exists for a node type.
        """
        raise NotImplementedError(f"No visit_{node.__class__.__name__} method defined in {self.__class__.__name__}")

class Transformer(Visitor):
    """
    A specialized Visitor for transforming an AST into a new AST.
    """
    def visit(self, node: ASTNode) -> ASTNode:
        """
        Overrides the base visit to ensure the return type is an ASTNode.
        """
        return super().visit(node)

    def generic_visit(self, node: ASTNode) -> ASTNode:
        """
        By default, a transformer returns the node as-is if no specific transformation is defined.
        """
        return node
