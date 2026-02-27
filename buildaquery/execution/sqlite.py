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
        self._transaction_connection: Any | None = None
        self._owns_transaction_connection = False

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
        cur = connection.execute(compiled_query.sql, compiled_query.params)
        if cur.description:
            return cur.fetchall()
        return None

    def _has_active_transaction(self) -> bool:
        return self._transaction_connection is not None

    def _get_connection_for_query(self) -> tuple[Any, bool]:
        if self._transaction_connection is not None:
            return self._transaction_connection, False
        if self.connection is not None:
            return self.connection, False

        sqlite3 = self._get_sqlite3()
        conn = sqlite3.connect(self.connection_info)
        return conn, True

    def _require_active_transaction_connection(self) -> Any:
        if self._transaction_connection is None:
            raise RuntimeError("No active transaction. Call begin() first.")
        return self._transaction_connection

    def execute(self, query: CompiledQuery | ASTNode) -> Any:
        """
        Executes a query. Returns rows for SELECT statements, otherwise None.
        """
        compiled_query = self._compile_if_needed(query)
        conn, should_close = self._get_connection_for_query()
        try:
            result = self._execute_with_connection(conn, compiled_query)
            if should_close:
                conn.commit()
            return result
        finally:
            if should_close:
                conn.close()

    def fetch_all(self, query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]:
        """
        Executes a query and returns all resulting rows.
        """
        compiled_query = self._compile_if_needed(query)
        conn, should_close = self._get_connection_for_query()
        try:
            cur = conn.execute(compiled_query.sql, compiled_query.params)
            return cast(Sequence[Sequence[Any]], cur.fetchall())
        finally:
            if should_close:
                conn.close()

    def fetch_one(self, query: CompiledQuery | ASTNode) -> Sequence[Any] | None:
        """
        Executes a query and returns a single resulting row.
        """
        compiled_query = self._compile_if_needed(query)
        conn, should_close = self._get_connection_for_query()
        try:
            cur = conn.execute(compiled_query.sql, compiled_query.params)
            return cast(Sequence[Any] | None, cur.fetchone())
        finally:
            if should_close:
                conn.close()

    def execute_raw(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """
        Executes a raw SQL string.
        """
        conn, should_close = self._get_connection_for_query()
        try:
            conn.execute(sql, params or [])
            if should_close:
                conn.commit()
        finally:
            if should_close:
                conn.close()

    def begin(self, isolation_level: str | None = None) -> None:
        if self._has_active_transaction():
            raise RuntimeError("Transaction already active.")

        normalized: str | None = None
        if isolation_level:
            normalized = isolation_level.strip().upper()
            if normalized not in {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}:
                raise ValueError(
                    "SQLite isolation_level must be one of: DEFERRED, IMMEDIATE, EXCLUSIVE."
                )

        if self.connection is not None:
            self._transaction_connection = self.connection
            self._owns_transaction_connection = False
        else:
            sqlite3 = self._get_sqlite3()
            self._transaction_connection = sqlite3.connect(self.connection_info)
            self._owns_transaction_connection = True

        if normalized:
            self._transaction_connection.execute(f"BEGIN {normalized}")
        else:
            self._transaction_connection.execute("BEGIN")

    def commit(self) -> None:
        conn = self._require_active_transaction_connection()
        try:
            conn.commit()
        finally:
            if self._owns_transaction_connection:
                conn.close()
            self._transaction_connection = None
            self._owns_transaction_connection = False

    def rollback(self) -> None:
        conn = self._require_active_transaction_connection()
        try:
            conn.rollback()
        finally:
            if self._owns_transaction_connection:
                conn.close()
            self._transaction_connection = None
            self._owns_transaction_connection = False

    def savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        conn.execute(f"SAVEPOINT {name}")

    def rollback_to_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        conn.execute(f"ROLLBACK TO SAVEPOINT {name}")

    def release_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        conn.execute(f"RELEASE SAVEPOINT {name}")
