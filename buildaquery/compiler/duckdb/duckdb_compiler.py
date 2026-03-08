from buildaquery.abstract_syntax_tree.models import LockClauseNode
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler

# ==================================================
# DuckDB Compiler
# ==================================================


class DuckDbCompiler(SqliteCompiler):
    """
    DuckDB compiler implementation.

    DuckDB uses qmark placeholders and is largely compatible with the SQLite
    SQL generation strategy in this project.
    """

    def visit_LockClauseNode(self, node: LockClauseNode) -> str:
        _ = node
        raise ValueError("DuckDB does not support FOR UPDATE/FOR SHARE row-lock clauses.")
