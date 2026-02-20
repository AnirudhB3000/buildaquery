from dataclasses import dataclass, field
from typing import Any

from buildaquery.abstract_syntax_tree.models import (
    ASTNode,
    SelectStatementNode,
    ColumnNode,
    TableNode,
    LiteralNode,
    BinaryOperationNode,
    TopClauseNode,
    StarNode,
    JoinClauseNode,
    OrderByClauseNode,
    WhereClauseNode,
    GroupByClauseNode,
    HavingClauseNode,
    AliasNode,
    FunctionCallNode,
    UnaryOperationNode,
)
from buildaquery.traversal.visitor_pattern import Visitor

# ==================================================
# Compiled Output
# ==================================================

@dataclass
class CompiledQuery:
    """
    Represents the result of the compilation process.
    """
    sql: str
    params: list[Any] = field(default_factory=list)

# ==================================================
# PostgreSQL Compiler
# ==================================================

class PostgresCompiler(Visitor):
    """
    A visitor that compiles an AST into a PostgreSQL query string and a list of parameters.
    """

    def __init__(self) -> None:
        self._params: list[Any] = []

    def compile(self, node: ASTNode) -> CompiledQuery:
        """
        The main entry point for compiling an AST node.
        """
        self._params = [] # Reset params for each compilation
        sql = self.visit(node)
        return CompiledQuery(sql=sql, params=self._params)

    # --------------------------------------------------
    # Statement Nodes
    # --------------------------------------------------

    def visit_SelectStatementNode(self, node: SelectStatementNode) -> str:
        """
        Compiles a SELECT statement, ensuring clauses are in the correct order.
        """
        # Mutual Exclusivity Check: TOP vs LIMIT/OFFSET
        if node.top_clause and (node.limit is not None or node.offset is not None):
            raise ValueError("TOP clause is mutually exclusive with LIMIT and OFFSET.")

        parts: list[str] = ["SELECT"]

        # 1. Select List
        select_list_sql = ", ".join([self.visit(item) for item in node.select_list])
        parts.append(select_list_sql)

        # 2. FROM Clause
        if node.from_table:
            parts.append("FROM")
            parts.append(self.visit(node.from_table))

        # 3. WHERE Clause
        if node.where_clause:
            parts.append(self.visit(node.where_clause))

        # 4. GROUP BY Clause
        if node.group_by:
            parts.append(self.visit(node.group_by))

        # 5. HAVING Clause
        if node.having_clause:
            parts.append(self.visit(node.having_clause))

        # 6. ORDER BY Clause
        order_by_sql = ""
        if node.order_by_clause:
            order_by_items = [self.visit(item) for item in node.order_by_clause]
            order_by_sql = f"ORDER BY {', '.join(order_by_items)}"
        
        # Apply implicit TOP ordering if no explicit ORDER BY is present
        if node.top_clause and node.top_clause.on_expression and not order_by_sql:
            top_order = f"{self.visit(node.top_clause.on_expression)} {node.top_clause.direction}"
            order_by_sql = f"ORDER BY {top_order}"

        if order_by_sql:
            parts.append(order_by_sql)

        # 7. LIMIT / OFFSET (Standard or from TOP translation)
        if node.limit is not None:
            parts.append(f"LIMIT {node.limit}")
        if node.offset is not None:
            parts.append(f"OFFSET {node.offset}")
        
        if node.top_clause:
            # PostgreSQL translates TOP to LIMIT
            parts.append(f"LIMIT {node.top_clause.count}")

        return " ".join(parts)

    # --------------------------------------------------
    # Expression Nodes
    # --------------------------------------------------

    def visit_ColumnNode(self, node: ColumnNode) -> str:
        return node.name

    def visit_LiteralNode(self, node: LiteralNode) -> str:
        """
        Parametrizes the literal value to prevent SQL injection.
        """
        self._params.append(node.value)
        return "%s"

    def visit_BinaryOperationNode(self, node: BinaryOperationNode) -> str:
        left = self.visit(node.left)
        right = self.visit(node.right)
        return f"({left} {node.operator} {right})"

    def visit_StarNode(self, node: StarNode) -> str:
        return "*"

    def visit_AliasNode(self, node: AliasNode) -> str:
        return f"{self.visit(node.expression)} AS {node.name}"

    def visit_FunctionCallNode(self, node: FunctionCallNode) -> str:
        args = ", ".join([self.visit(arg) for arg in node.args])
        return f"{node.name}({args})"

    def visit_UnaryOperationNode(self, node: UnaryOperationNode) -> str:
        return f"({node.operator} {self.visit(node.operand)})"

    # --------------------------------------------------
    # Clause Nodes
    # --------------------------------------------------

    def visit_TableNode(self, node: TableNode) -> str:
        return node.name

    def visit_JoinClauseNode(self, node: JoinClauseNode) -> str:
        left = self.visit(node.left)
        right = self.visit(node.right)
        on = self.visit(node.on_condition)
        return f"{left} {node.join_type} JOIN {right} ON {on}"

    def visit_WhereClauseNode(self, node: WhereClauseNode) -> str:
        return f"WHERE {self.visit(node.condition)}"

    def visit_GroupByClauseNode(self, node: GroupByClauseNode) -> str:
        exprs = ", ".join([self.visit(expr) for expr in node.expressions])
        return f"GROUP BY {exprs}"

    def visit_HavingClauseNode(self, node: HavingClauseNode) -> str:
        return f"HAVING {self.visit(node.condition)}"

    def visit_OrderByClauseNode(self, node: OrderByClauseNode) -> str:
        return f"{self.visit(node.expression)} {node.direction}"
