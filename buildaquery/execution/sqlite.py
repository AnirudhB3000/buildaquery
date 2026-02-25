from typing import Any, Sequence, cast

from buildaquery.abstract_syntax_tree.models import ASTNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler
from buildaquery.execution.base import Executor

# ==================================================
# SQLite Executor
# ==================================================

class SqliteExecutor(Executor):
    """
    An executor for SQLite using the standard library 'sqlite3' module.
    """

    def __init__(
        self,
        connection_info: str | None = None,
        connection: Any | None = None,
        compiler: Any | None = None
    ) -> None:
        """
        Initializes the executor with connection information or an existing connection.

        Args:
            connection_info: A SQLite database file path (or ":memory:").
            connection: An existing sqlite3 connection object.
            compiler: An optional compiler instance to compile AST nodes automatically.
        """
        if connection_info is None and connection is None:
            raise ValueError("Either connection_info or connection must be provided.")

        self.connection_info = connection_info
        self.connection = connection
        self.compiler = compiler or SqliteCompiler()
        self._sqlite3 = None

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        """
        Compiles the query if it is an AST node.
        """
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_sqlite3(self) -> Any:
        """
        Lazily imports sqlite3 and returns the module.
        """
        if self._sqlite3 is None:
            import sqlite3
            self._sqlite3 = sqlite3
        return self._sqlite3

    def _execute_with_connection(self, connection: Any, compiled_query: CompiledQuery) -> Any:
        with connection:
            cur = connection.execute(compiled_query.sql, compiled_query.params)
            if cur.description:
                return cur.fetchall()
        return None

    def execute(self, query: CompiledQuery | ASTNode) -> Any:
        """
        Executes a query. Returns rows for SELECT statements, otherwise None.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            return self._execute_with_connection(self.connection, compiled_query)

        sqlite3 = self._get_sqlite3()
        with sqlite3.connect(self.connection_info) as conn:
            return self._execute_with_connection(conn, compiled_query)

    def fetch_all(self, query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]:
        """
        Executes a query and returns all resulting rows.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            cur = self.connection.execute(compiled_query.sql, compiled_query.params)
            return cast(Sequence[Sequence[Any]], cur.fetchall())

        sqlite3 = self._get_sqlite3()
        with sqlite3.connect(self.connection_info) as conn:
            cur = conn.execute(compiled_query.sql, compiled_query.params)
            return cast(Sequence[Sequence[Any]], cur.fetchall())

    def fetch_one(self, query: CompiledQuery | ASTNode) -> Sequence[Any] | None:
        """
        Executes a query and returns a single resulting row.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            cur = self.connection.execute(compiled_query.sql, compiled_query.params)
            return cast(Sequence[Any] | None, cur.fetchone())

        sqlite3 = self._get_sqlite3()
        with sqlite3.connect(self.connection_info) as conn:
            cur = conn.execute(compiled_query.sql, compiled_query.params)
            return cast(Sequence[Any] | None, cur.fetchone())

    def execute_raw(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """
        Executes a raw SQL string.
        """
        if self.connection:
            with self.connection:
                self.connection.execute(sql, params or [])
            return

        sqlite3 = self._get_sqlite3()
        with sqlite3.connect(self.connection_info) as conn:
            conn.execute(sql, params or [])
