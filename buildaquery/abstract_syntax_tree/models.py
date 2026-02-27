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
    table: str | None = None

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
class SubqueryNode(ExpressionNode, FromClauseNode):
    """
    Represents a subquery that can be used in an expression or a FROM clause.
    """
    statement: 'SelectStatementNode'
    alias: str | None = None

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
class LockClauseNode(ASTNode):
    """
    Represents row-level locking modifiers for SELECT statements.
    """
    mode: str = "UPDATE"  # UPDATE or SHARE
    nowait: bool = False
    skip_locked: bool = False

@dataclass
class TableNode(FromClauseNode):
    """
    Represents a table reference in the AST.
    """
    name: str
    schema: str | None = None
    alias: str | None = None

@dataclass
class AliasNode(ExpressionNode):
    """Represents an aliased expression (e.g., 'column AS new_name')."""
    expression: ExpressionNode
    name: str

@dataclass
class OverClauseNode(ASTNode):
    """Represents an OVER clause for window functions."""
    partition_by: list[ExpressionNode] | None = None
    order_by: list[OrderByClauseNode] | None = None

@dataclass
class CastNode(ExpressionNode):
    """Represents a type cast (e.g., 'CAST(column AS type)' or 'column::type')."""
    expression: ExpressionNode
    data_type: str

@dataclass
class FunctionCallNode(ExpressionNode):
    """Represents a function call (e.g., COUNT(*), MAX(price))."""
    name: str
    args: list[ExpressionNode]
    over: OverClauseNode | None = None # optional OVER clause for window functions

@dataclass
class UnaryOperationNode(ExpressionNode):
    """Represents a unary operation (e.g., NOT, -)."""
    operator: str
    operand: ExpressionNode

@dataclass
class InNode(ExpressionNode):
    """Represents an IN expression (e.g., 'column IN (1, 2, 3)')."""
    expression: ExpressionNode
    values: list[ExpressionNode]
    negated: bool = False

@dataclass
class WhenThenNode(ASTNode):
    """Represents a WHEN ... THEN ... clause in a CASE expression."""
    condition: ExpressionNode
    result: ExpressionNode

@dataclass
class CaseExpressionNode(ExpressionNode):
    """Represents a CASE expression (e.g., 'CASE WHEN cond THEN res ELSE default END')."""
    cases: list[WhenThenNode]
    else_result: ExpressionNode | None = None

@dataclass
class BetweenNode(ExpressionNode):
    """Represents a BETWEEN expression (e.g., 'column BETWEEN 1 AND 10')."""
    expression: ExpressionNode
    low: ExpressionNode
    high: ExpressionNode
    negated: bool = False

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
class CTENode(ASTNode):
    """Represents a Common Table Expression (WITH clause)."""
    name: str
    subquery: 'SelectStatementNode'

@dataclass
class SelectStatementNode(StatementNode):
    """
    Represents a SELECT statement in the AST.
    """
    select_list: list[ExpressionNode] # list of expressions to select (eg: columns, functions, etc.)
    distinct: bool = False # toggle between SELECT ALL and SELECT DISTINCT
    ctes: list[CTENode] | None = None # optional list of Common Table Expressions
    from_table: FromClauseNode | None = None # the table to select from, optional for edge cases
    where_clause: WhereClauseNode | None = None # optional where clause for filtering results
    group_by: GroupByClauseNode | None = None # optional group by clause for aggregation
    having_clause: HavingClauseNode | None = None # optional having clause for filtering groups in aggregation
    order_by_clause: list[OrderByClauseNode] | None = None # optional list of order by clauses for sorting results
    top_clause: TopClauseNode | None = None # optional top clause, mutually exclusive with limit and offset
    limit: int | None = None # optional limit for number of results to return
    offset: int | None = None # optional offset for skipping results
    lock_clause: LockClauseNode | None = None # optional row-locking clause (e.g., FOR UPDATE)

@dataclass
class DeleteStatementNode(StatementNode):
    """
    Represents a DELETE statement in the AST.
    """
    table: TableNode
    where_clause: WhereClauseNode | None = None

@dataclass
class ColumnDefinitionNode(ASTNode):
    """Represents a column definition in a CREATE TABLE statement."""
    name: str
    data_type: str
    primary_key: bool = False
    not_null: bool = False
    default: ExpressionNode | None = None

@dataclass
class CreateStatementNode(StatementNode):
    """Represents a CREATE TABLE statement."""
    table: TableNode
    columns: list[ColumnDefinitionNode]
    if_not_exists: bool = False

@dataclass
class DropStatementNode(StatementNode):
    """Represents a DROP TABLE statement."""
    table: TableNode
    if_exists: bool = False
    cascade: bool = False

@dataclass
class InsertStatementNode(StatementNode):
    """
    Represents an INSERT statement in the AST.
    """
    table: TableNode
    values: list[ExpressionNode]
    columns: list[ColumnNode] | None = None

@dataclass
class UpdateStatementNode(StatementNode):
    """
    Represents an UPDATE statement in the AST.
    """
    table: TableNode
    set_clauses: dict[str, ExpressionNode] # Map column names to new values/expressions
    where_clause: WhereClauseNode | None = None

@dataclass
class SetOperationNode(StatementNode):
    """
    Base class for set operations like UNION, INTERSECT, EXCEPT.
    """
    left: StatementNode
    right: StatementNode
    all: bool = False

@dataclass
class UnionNode(SetOperationNode):
    """Represents a UNION operation."""
    pass

@dataclass
class IntersectNode(SetOperationNode):
    """Represents an INTERSECT operation."""
    pass

@dataclass
class ExceptNode(SetOperationNode):
    """Represents an EXCEPT operation."""
    pass
