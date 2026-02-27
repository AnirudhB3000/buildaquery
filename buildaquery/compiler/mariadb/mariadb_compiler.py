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
    PrimaryKeyConstraintNode,
    UniqueConstraintNode,
    ForeignKeyConstraintNode,
    CheckConstraintNode,
    CreateStatementNode,
    DropStatementNode,
    CreateIndexStatementNode,
    DropIndexStatementNode,
    AlterTableStatementNode,
    AddColumnActionNode,
    DropColumnActionNode,
    AddConstraintActionNode,
    DropConstraintActionNode,
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
    LockClauseNode,
    UpsertClauseNode,
    ReturningClauseNode,
)
from buildaquery.traversal.visitor_pattern import Visitor

# ==================================================
# MariaDB Compiler
# ==================================================

class MariaDbCompiler(Visitor):
    """
    A visitor that compiles an AST into a MariaDB query string and a list of parameters.
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
        if node.returning_clause:
            parts.append(self._compile_returning_clause(node.returning_clause))
        return " ".join(parts)

    def visit_InsertStatementNode(self, node: InsertStatementNode) -> str:
        """
        Compiles an INSERT statement.
        """
        table = self.visit(node.table)
        cols = ""
        if node.columns:
            cols = f" ({', '.join([c.name for c in node.columns])})"

        values_sql = self._compile_insert_values(node)
        sql = f"INSERT INTO {table}{cols} {values_sql}"
        if node.upsert_clause:
            sql += f" {self._compile_upsert_clause(node.upsert_clause)}"
        if node.returning_clause:
            sql += f" {self._compile_returning_clause(node.returning_clause)}"
        return sql

    def _compile_insert_values(self, node: InsertStatementNode) -> str:
        has_values = node.values is not None
        has_rows = node.rows is not None
        if has_values == has_rows:
            raise ValueError("Insert must provide exactly one of values or rows.")

        if node.values is not None:
            if node.columns and len(node.columns) != len(node.values):
                raise ValueError("Insert columns and values must have the same length.")
            vals = ", ".join([self.visit(v) for v in node.values])
            return f"VALUES ({vals})"

        assert node.rows is not None
        if not node.rows:
            raise ValueError("Insert rows must include at least one row.")
        expected = len(node.rows[0])
        if expected == 0:
            raise ValueError("Insert rows cannot be empty.")
        if node.columns and len(node.columns) != expected:
            raise ValueError("Insert columns and row values must have the same length.")
        row_sql: list[str] = []
        for row in node.rows:
            if len(row) != expected:
                raise ValueError("All insert rows must have the same number of values.")
            row_sql.append(f"({', '.join([self.visit(v) for v in row])})")
        return f"VALUES {', '.join(row_sql)}"

    def _compile_upsert_clause(self, clause: UpsertClauseNode) -> str:
        if clause.conflict_target is not None:
            raise ValueError("MariaDB upsert does not accept conflict_target.")
        if clause.do_nothing:
            raise ValueError("MariaDB upsert does not support do_nothing.")
        if not clause.update_columns:
            raise ValueError("MariaDB upsert requires update_columns.")

        updates = ", ".join([f"{col} = VALUES({col})" for col in clause.update_columns])
        return f"ON DUPLICATE KEY UPDATE {updates}"

    def visit_UpdateStatementNode(self, node: UpdateStatementNode) -> str:
        """
        Compiles an UPDATE statement.
        """
        table = self.visit(node.table)
        sets = ", ".join([f"{col} = {self.visit(expr)}" for col, expr in node.set_clauses.items()])

        parts = [f"UPDATE {table} SET {sets}"]
        if node.where_clause:
            parts.append(self.visit(node.where_clause))
        if node.returning_clause:
            raise ValueError("MariaDB does not support UPDATE ... RETURNING in this compiler.")

        return " ".join(parts)

    def _compile_returning_clause(self, clause: ReturningClauseNode) -> str:
        if not clause.expressions:
            raise ValueError("RETURNING requires at least one expression.")
        exprs = ", ".join([self.visit(expr) for expr in clause.expressions])
        return f"RETURNING {exprs}"

    def visit_CreateStatementNode(self, node: CreateStatementNode) -> str:
        """
        Compiles a CREATE TABLE statement.
        """
        if_not_exists = " IF NOT EXISTS" if node.if_not_exists else ""
        table = self.visit(node.table)
        parts = [self.visit(c) for c in node.columns]
        if node.constraints:
            parts.extend([self.visit(constraint) for constraint in node.constraints])
        cols = ", ".join(parts)
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
        if_exists = " IF EXISTS" if node.if_exists else ""
        table = self.visit(node.table)
        cascade = " CASCADE" if node.cascade else ""
        return f"DROP TABLE{if_exists} {table}{cascade}"

    def visit_CreateIndexStatementNode(self, node: CreateIndexStatementNode) -> str:
        if node.if_not_exists:
            raise ValueError("MariaDB does not support IF NOT EXISTS in CREATE INDEX.")
        if not node.columns:
            raise ValueError("CREATE INDEX requires at least one column.")
        unique = "UNIQUE " if node.unique else ""
        cols = ", ".join([self.visit(column) for column in node.columns])
        return f"CREATE {unique}INDEX {node.name} ON {self.visit(node.table)} ({cols})"

    def visit_DropIndexStatementNode(self, node: DropIndexStatementNode) -> str:
        if node.if_exists:
            raise ValueError("MariaDB does not support IF EXISTS in DROP INDEX.")
        if node.cascade:
            raise ValueError("MariaDB does not support CASCADE in DROP INDEX.")
        if node.table is None:
            raise ValueError("MariaDB DROP INDEX requires a table.")
        return f"DROP INDEX {node.name} ON {self.visit(node.table)}"

    def visit_AlterTableStatementNode(self, node: AlterTableStatementNode) -> str:
        if not node.actions:
            raise ValueError("ALTER TABLE requires at least one action.")
        actions = ", ".join([self.visit(action) for action in node.actions])
        return f"ALTER TABLE {self.visit(node.table)} {actions}"

    def visit_UnionNode(self, node: UnionNode) -> str:
        """
        Compiles a UNION operation.
        """
        op = "UNION ALL" if node.all else "UNION"
        return f"{self.visit(node.left)} {op} {self.visit(node.right)}"

    def visit_IntersectNode(self, node: IntersectNode) -> str:
        """
        Compiles an INTERSECT operation.
        """
        op = "INTERSECT ALL" if node.all else "INTERSECT"
        return f"{self.visit(node.left)} {op} {self.visit(node.right)}"

    def visit_ExceptNode(self, node: ExceptNode) -> str:
        """
        Compiles an EXCEPT operation.
        """
        op = "EXCEPT ALL" if node.all else "EXCEPT"
        return f"{self.visit(node.left)} {op} {self.visit(node.right)}"

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
        return "?"

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
            raise ValueError("MariaDB lock mode must be 'UPDATE' or 'SHARE'.")
        if node.nowait and node.skip_locked:
            raise ValueError("NOWAIT and SKIP LOCKED are mutually exclusive.")

        parts = [f"FOR {mode}"]
        if node.nowait:
            parts.append("NOWAIT")
        if node.skip_locked:
            parts.append("SKIP LOCKED")
        return " ".join(parts)

    def visit_PrimaryKeyConstraintNode(self, node: PrimaryKeyConstraintNode) -> str:
        columns = node.columns or []
        if not columns:
            raise ValueError("PRIMARY KEY constraint requires at least one column.")
        cols = ", ".join([column.name for column in columns])
        prefix = f"CONSTRAINT {node.name} " if node.name else ""
        return f"{prefix}PRIMARY KEY ({cols})"

    def visit_UniqueConstraintNode(self, node: UniqueConstraintNode) -> str:
        columns = node.columns or []
        if not columns:
            raise ValueError("UNIQUE constraint requires at least one column.")
        cols = ", ".join([column.name for column in columns])
        prefix = f"CONSTRAINT {node.name} " if node.name else ""
        return f"{prefix}UNIQUE ({cols})"

    def visit_ForeignKeyConstraintNode(self, node: ForeignKeyConstraintNode) -> str:
        columns = node.columns or []
        reference_columns = node.reference_columns or []
        if not columns or not reference_columns or node.reference_table is None:
            raise ValueError("FOREIGN KEY constraint requires columns, reference_table, and reference_columns.")
        if len(columns) != len(reference_columns):
            raise ValueError("FOREIGN KEY columns and reference_columns must have the same length.")
        cols = ", ".join([column.name for column in columns])
        ref_cols = ", ".join([column.name for column in reference_columns])
        parts = []
        if node.name:
            parts.append(f"CONSTRAINT {node.name}")
        parts.append(f"FOREIGN KEY ({cols}) REFERENCES {self.visit(node.reference_table)} ({ref_cols})")
        if node.on_delete:
            parts.append(f"ON DELETE {node.on_delete}")
        if node.on_update:
            parts.append(f"ON UPDATE {node.on_update}")
        return " ".join(parts)

    def visit_CheckConstraintNode(self, node: CheckConstraintNode) -> str:
        if node.condition is None:
            raise ValueError("CHECK constraint requires a condition.")
        prefix = f"CONSTRAINT {node.name} " if node.name else ""
        return f"{prefix}CHECK ({self.visit(node.condition)})"

    def visit_AddColumnActionNode(self, node: AddColumnActionNode) -> str:
        return f"ADD COLUMN {self.visit(node.column)}"

    def visit_DropColumnActionNode(self, node: DropColumnActionNode) -> str:
        if node.if_exists:
            raise ValueError("MariaDB does not support IF EXISTS for DROP COLUMN in this compiler.")
        return f"DROP COLUMN {node.column_name}"

    def visit_AddConstraintActionNode(self, node: AddConstraintActionNode) -> str:
        return f"ADD {self.visit(node.constraint)}"

    def visit_DropConstraintActionNode(self, node: DropConstraintActionNode) -> str:
        _ = node
        raise ValueError("MariaDB DROP CONSTRAINT is not supported in this compiler.")
