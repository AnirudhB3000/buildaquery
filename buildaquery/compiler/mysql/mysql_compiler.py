from typing import Any

from buildaquery.compiler.compiled_query import CompiledQuery
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
    CastNode,
    FunctionCallNode,
    UnaryOperationNode,
    DeleteStatementNode,
    InsertStatementNode,
    UpdateStatementNode,
    ColumnDefinitionNode,
    CreateStatementNode,
    DropStatementNode,
    UnionNode,
    IntersectNode,
    ExceptNode,
    InNode,
    BetweenNode,
    CaseExpressionNode,
    WhenThenNode,
    SubqueryNode,
    CTENode,
    OverClauseNode,
    LockClauseNode
)
from buildaquery.traversal.visitor_pattern import Visitor

# ==================================================
# MySQL Compiler
# ==================================================

class MySqlCompiler(Visitor):
    """
    A visitor that compiles an AST into a MySQL query string and a list of parameters.
    """

    def __init__(self) -> None:
        self._params: list[Any] = []

    def compile(self, node: ASTNode) -> CompiledQuery:
        """
        The main entry point for compiling an AST node.
        """
        self._params = []
        sql = self.visit(node)
        return CompiledQuery(sql=sql, params=self._params)

    # --------------------------------------------------
    # Statement Nodes
    # --------------------------------------------------

    def visit_SelectStatementNode(self, node: SelectStatementNode) -> str:
        """
        Compiles a SELECT statement, ensuring clauses are in the correct order.
        """
        if node.top_clause and (node.limit is not None or node.offset is not None):
            raise ValueError("TOP clause is mutually exclusive with LIMIT and OFFSET.")

        parts: list[str] = []

        if node.ctes:
            cte_sqls = [self.visit(cte) for cte in node.ctes]
            parts.append(f"WITH {', '.join(cte_sqls)}")

        parts.append("SELECT")

        if node.distinct:
            parts.append("DISTINCT")

        select_list_sql = ", ".join([self.visit(item) for item in node.select_list])
        parts.append(select_list_sql)

        if node.from_table:
            parts.append("FROM")
            parts.append(self.visit(node.from_table))

        if node.where_clause:
            parts.append(self.visit(node.where_clause))

        if node.group_by:
            parts.append(self.visit(node.group_by))

        if node.having_clause:
            parts.append(self.visit(node.having_clause))

        order_by_sql = ""
        if node.order_by_clause:
            order_by_items = [self.visit(item) for item in node.order_by_clause]
            order_by_sql = f"ORDER BY {', '.join(order_by_items)}"

        if node.top_clause and node.top_clause.on_expression and not order_by_sql:
            top_order = f"{self.visit(node.top_clause.on_expression)} {node.top_clause.direction}"
            order_by_sql = f"ORDER BY {top_order}"

        if order_by_sql:
            parts.append(order_by_sql)

        if node.limit is not None:
            parts.append(f"LIMIT {node.limit}")
        if node.offset is not None:
            parts.append(f"OFFSET {node.offset}")

        if node.top_clause:
            parts.append(f"LIMIT {node.top_clause.count}")

        if node.lock_clause:
            parts.append(self.visit(node.lock_clause))

        return " ".join(parts)

    def visit_CTENode(self, node: CTENode) -> str:
        """
        Compiles a CTE (name AS (subquery)).
        """
        return f"{node.name} AS ({self.visit(node.subquery)})"

    def visit_DeleteStatementNode(self, node: DeleteStatementNode) -> str:
        """
        Compiles a DELETE statement.
        """
        parts = ["DELETE FROM", self.visit(node.table)]
        if node.where_clause:
            parts.append(self.visit(node.where_clause))
        return " ".join(parts)

    def visit_InsertStatementNode(self, node: InsertStatementNode) -> str:
        """
        Compiles an INSERT statement.
        """
        table = self.visit(node.table)
        cols = ""
        if node.columns:
            cols = f" ({', '.join([c.name for c in node.columns])})"

        vals = ", ".join([self.visit(v) for v in node.values])
        return f"INSERT INTO {table}{cols} VALUES ({vals})"

    def visit_UpdateStatementNode(self, node: UpdateStatementNode) -> str:
        """
        Compiles an UPDATE statement.
        """
        table = self.visit(node.table)
        sets = ", ".join([f"{col} = {self.visit(expr)}" for col, expr in node.set_clauses.items()])

        parts = [f"UPDATE {table} SET {sets}"]
        if node.where_clause:
            parts.append(self.visit(node.where_clause))

        return " ".join(parts)

    def visit_CreateStatementNode(self, node: CreateStatementNode) -> str:
        """
        Compiles a CREATE TABLE statement.
        """
        if_not_exists = " IF NOT EXISTS" if node.if_not_exists else ""
        table = self.visit(node.table)
        cols = ", ".join([self.visit(c) for c in node.columns])
        return f"CREATE TABLE{if_not_exists} {table} ({cols})"

    def visit_ColumnDefinitionNode(self, node: ColumnDefinitionNode) -> str:
        """
        Compiles a column definition.
        """
        parts = [node.name, node.data_type]
        if node.primary_key:
            parts.append("PRIMARY KEY")
        if node.not_null:
            parts.append("NOT NULL")
        if node.default:
            parts.append(f"DEFAULT {self.visit(node.default)}")
        return " ".join(parts)

    def visit_DropStatementNode(self, node: DropStatementNode) -> str:
        """
        Compiles a DROP TABLE statement.
        """
        if node.cascade:
            raise ValueError("MySQL does not support CASCADE in DROP TABLE.")
        if_exists = " IF EXISTS" if node.if_exists else ""
        table = self.visit(node.table)
        return f"DROP TABLE{if_exists} {table}"

    def visit_UnionNode(self, node: UnionNode) -> str:
        """
        Compiles a UNION operation.
        """
        op = "UNION ALL" if node.all else "UNION"
        return f"{self.visit(node.left)} {op} {self.visit(node.right)}"

    def visit_IntersectNode(self, node: IntersectNode) -> str:
        """
        MySQL does not support INTERSECT.
        """
        raise ValueError("MySQL does not support INTERSECT.")

    def visit_ExceptNode(self, node: ExceptNode) -> str:
        """
        MySQL does not support EXCEPT.
        """
        raise ValueError("MySQL does not support EXCEPT.")

    # --------------------------------------------------
    # Expression Nodes
    # --------------------------------------------------

    def visit_ColumnNode(self, node: ColumnNode) -> str:
        if node.table:
            return f"{node.table}.{node.name}"
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

    def visit_CastNode(self, node: CastNode) -> str:
        return f"CAST({self.visit(node.expression)} AS {node.data_type})"

    def visit_FunctionCallNode(self, node: FunctionCallNode) -> str:
        args = ", ".join([self.visit(arg) for arg in node.args])
        sql = f"{node.name}({args})"
        if node.over:
            sql += f" OVER {self.visit(node.over)}"
        return sql

    def visit_OverClauseNode(self, node: OverClauseNode) -> str:
        """
        Compiles an OVER clause.
        """
        parts = []
        if node.partition_by:
            exprs = ", ".join([self.visit(expr) for expr in node.partition_by])
            parts.append(f"PARTITION BY {exprs}")

        if node.order_by:
            exprs = ", ".join([self.visit(item) for item in node.order_by])
            parts.append(f"ORDER BY {exprs}")

        return f"({' '.join(parts)})"

    def visit_UnaryOperationNode(self, node: UnaryOperationNode) -> str:
        return f"({node.operator} {self.visit(node.operand)})"

    def visit_InNode(self, node: InNode) -> str:
        """
        Compiles an IN expression.
        """
        expr = self.visit(node.expression)
        vals = ", ".join([self.visit(v) for v in node.values])
        op = "NOT IN" if node.negated else "IN"
        return f"({expr} {op} ({vals}))"

    def visit_BetweenNode(self, node: BetweenNode) -> str:
        """
        Compiles a BETWEEN expression.
        """
        expr = self.visit(node.expression)
        low = self.visit(node.low)
        high = self.visit(node.high)
        op = "NOT BETWEEN" if node.negated else "BETWEEN"
        return f"({expr} {op} {low} AND {high})"

    def visit_CaseExpressionNode(self, node: CaseExpressionNode) -> str:
        """
        Compiles a CASE expression.
        """
        parts = ["CASE"]
        for case in node.cases:
            parts.append(self.visit(case))
        if node.else_result:
            parts.append(f"ELSE {self.visit(node.else_result)}")
        parts.append("END")
        return " ".join(parts)

    def visit_WhenThenNode(self, node: WhenThenNode) -> str:
        """
        Compiles a WHEN ... THEN ... clause.
        """
        return f"WHEN {self.visit(node.condition)} THEN {self.visit(node.result)}"

    def visit_SubqueryNode(self, node: SubqueryNode) -> str:
        """
        Compiles a subquery.
        """
        sql = f"({self.visit(node.statement)})"
        if node.alias:
            sql += f" AS {node.alias}"
        return sql

    # --------------------------------------------------
    # Clause Nodes
    # --------------------------------------------------

    def visit_TableNode(self, node: TableNode) -> str:
        name = node.name
        if node.schema:
            name = f"{node.schema}.{name}"
        if node.alias:
            name = f"{name} AS {node.alias}"
        return name

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

    def visit_LockClauseNode(self, node: LockClauseNode) -> str:
        mode = node.mode.strip().upper()
        if mode not in {"UPDATE", "SHARE"}:
            raise ValueError("MySQL lock mode must be 'UPDATE' or 'SHARE'.")
        if node.nowait and node.skip_locked:
            raise ValueError("NOWAIT and SKIP LOCKED are mutually exclusive.")

        parts = [f"FOR {mode}"]
        if node.nowait:
            parts.append("NOWAIT")
        if node.skip_locked:
            parts.append("SKIP LOCKED")
        return " ".join(parts)
