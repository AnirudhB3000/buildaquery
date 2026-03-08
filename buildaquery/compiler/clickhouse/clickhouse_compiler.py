from buildaquery.abstract_syntax_tree.models import (
    DeleteStatementNode,
    DropStatementNode,
    InsertStatementNode,
    LockClauseNode,
    UpdateStatementNode,
)
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler

# ==================================================
# ClickHouse Compiler
# ==================================================


class ClickHouseCompiler(PostgresCompiler):
    """
    ClickHouse compiler implementation.

    ClickHouse uses `%s` placeholders with `clickhouse-driver` DB-API usage.
    This compiler reuses PostgreSQL SQL generation for supported paths and
    raises explicit errors for unsupported OLTP-only clauses.
    """

    def visit_InsertStatementNode(self, node: InsertStatementNode) -> str:
        if node.upsert_clause is not None:
            raise ValueError("ClickHouse does not support upsert_clause in INSERT.")
        if node.returning_clause is not None:
            raise ValueError("ClickHouse does not support RETURNING on INSERT.")
        return super().visit_InsertStatementNode(node)

    def visit_UpdateStatementNode(self, node: UpdateStatementNode) -> str:
        if node.returning_clause is not None:
            raise ValueError("ClickHouse does not support RETURNING on UPDATE.")
        return super().visit_UpdateStatementNode(node)

    def visit_DeleteStatementNode(self, node: DeleteStatementNode) -> str:
        if node.returning_clause is not None:
            raise ValueError("ClickHouse does not support RETURNING on DELETE.")
        return super().visit_DeleteStatementNode(node)

    def visit_DropStatementNode(self, node: DropStatementNode) -> str:
        if node.cascade:
            raise ValueError("ClickHouse does not support CASCADE in DROP TABLE.")
        return super().visit_DropStatementNode(node)

    def visit_LockClauseNode(self, node: LockClauseNode) -> str:
        _ = node
        raise ValueError("ClickHouse does not support FOR UPDATE/FOR SHARE row-lock clauses.")
