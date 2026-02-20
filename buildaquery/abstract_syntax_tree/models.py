from abc import ABC
from dataclasses import dataclass
from typing import Any

# ==================================================
# Base classes
# ==================================================
@dataclass
class ASTNode(ABC):
    """
    A generic AST node. All specific AST node types will inherit from this base class.
    """
    pass

@dataclass
class ExpressionNode(ASTNode):
    """
    A base class for all expression nodes in the AST.
    """
    pass

# ==================================================
# Specific expression nodes
# ==================================================

@dataclass
class LiteralNode(ExpressionNode):
    """
    Represents a literal value in the AST, such as a number or string.
    """
    value: Any

@dataclass
class ColumnNode(ExpressionNode):
    """
    Represents a column reference in the AST.
    """
    name: str

@dataclass
class BinaryOperationNode(ExpressionNode):
    """
    Represents a binary operation in the AST, such as addition, subtraction, etc.
    """
    left: ExpressionNode
    operator: str
    right: ExpressionNode

# ==================================================
# Statement nodes
# ==================================================

@dataclass
class StatementNode(ASTNode):
    """
    A base class for all statement nodes in the AST.
    """
    pass

@dataclass
class FromClauseNode(ASTNode):
    """
    Represents a FROM clause in the AST, anything can appears here, including subqueries, joins, etc.
    """
    pass

@dataclass
class JoinClauseNode(FromClauseNode):
    """
    Represents a JOIN clause in the AST.
    """
    left: FromClauseNode
    right: FromClauseNode
    on_condition: ExpressionNode
    join_type: str

@dataclass
class OrderByClauseNode(ASTNode):
    """
    Represents a single item in the ORDER BY clause in the AST.
    """
    expression: ExpressionNode
    direction: str = "ASC" # default to ascending order

@dataclass
class TopClauseNode(ASTNode):
    """
    Represents a TOP clause in the AST.
    """
    count: int
    # Optional: Column to order by for TOP clause, if not already specified in ORDER BY
    on_expression: ExpressionNode | None = None
    direction: str = "DESC" # default to descending order for TOP

@dataclass
class TableNode(FromClauseNode):
    """
    Represents a table reference in the AST.
    """
    name: str

@dataclass
class AliasNode(ExpressionNode):
    """Represents an aliased expression (e.g., 'column AS new_name')."""
    expression: ExpressionNode
    name: str

@dataclass
class FunctionCallNode(ExpressionNode):
    """Represents a function call (e.g., COUNT(*), MAX(price))."""
    name: str
    args: list[ExpressionNode]

@dataclass
class UnaryOperationNode(ExpressionNode):
    """Represents a unary operation (e.g., NOT, -)."""
    operator: str
    operand: ExpressionNode

@dataclass
class StarNode(ExpressionNode):
    """Represents the '*' in 'SELECT *'."""
    pass

@dataclass
class WhereClauseNode(ASTNode):
    """A wrapper for the expression in a WHERE clause."""
    condition: ExpressionNode

@dataclass
class GroupByClauseNode(ASTNode):
    """A wrapper for the list of expressions in a GROUP BY clause."""
    expressions: list[ExpressionNode]

@dataclass
class HavingClauseNode(ASTNode):
    """A wrapper for the expression in a HAVING clause."""
    condition: ExpressionNode

@dataclass
class SelectStatementNode(StatementNode):
    """
    Represents a SELECT statement in the AST.
    """
    select_list: list[ExpressionNode] # list of expressions to select (eg: columns, functions, etc.)
    from_table: FromClauseNode | None = None # the table to select from, optional for edge cases
    where_clause: WhereClauseNode | None = None # optional where clause for filtering results
    group_by: GroupByClauseNode | None = None # optional group by clause for aggregation
    having_clause: HavingClauseNode | None = None # optional having clause for filtering groups in aggregation
    order_by_clause: list[OrderByClauseNode] | None = None # optional list of order by clauses for sorting results
    top_clause: TopClauseNode | None = None # optional top clause, mutually exclusive with limit and offset
    limit: int | None = None # optional limit for number of results to return
    offset: int | None = None # optional offset for skipping results