from typing import Any, Sequence, cast
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler
from buildaquery.execution.base import Executor
from buildaquery.abstract_syntax_tree.models import ASTNode

# ==================================================
# PostgreSQL Executor
# ==================================================

class PostgresExecutor(Executor):
    """
    An executor for PostgreSQL using the 'psycopg' library.
    """

    def __init__(self, connection_info: str | dict[str, Any] | None = None, connection: Any | None = None, compiler: Any | None = None) -> None:
        """
        Initializes the executor with connection information or an existing connection.
        
        Args:
            connection_info: A connection string or a dictionary of parameters.
            connection: An existing psycopg connection object.
            compiler: An optional compiler instance to compile AST nodes automatically.
        """
        if connection_info is None and connection is None:
            raise ValueError("Either connection_info or connection must be provided.")
            
        self.connection_info = connection_info
        self.connection = connection
        self.compiler = compiler or PostgresCompiler()
        self._psycopg = None
        self._transaction_connection: Any | None = None
        self._owns_transaction_connection = False
        self._transaction_previous_autocommit: bool | None = None

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        """
        Compiles the query if it is an AST node.
        """
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_psycopg(self) -> Any:
        """
        Lazily imports psycopg and returns the module.
        """
        if self._psycopg is None:
            try:
                import psycopg
                self._psycopg = psycopg
            except ImportError:
                raise ImportError(
                    "The 'psycopg' library is required for PostgresExecutor. "
                    "Install it with 'pip install psycopg[binary]'."
                )
        return self._psycopg

    def _has_active_transaction(self) -> bool:
        return self._transaction_connection is not None

    def _get_connection_for_query(self) -> tuple[Any, bool]:
        if self._transaction_connection is not None:
            return self._transaction_connection, False
        if self.connection is not None:
            return self.connection, False

        psycopg = self._get_psycopg()
        conn = psycopg.connect(self.connection_info)
        return conn, True

    def _require_active_transaction_connection(self) -> Any:
        if self._transaction_connection is None:
            raise RuntimeError("No active transaction. Call begin() first.")
        return self._transaction_connection

    def execute(self, query: CompiledQuery | ASTNode) -> Any:
        """
        Executes a query that does not return results (e.g., INSERT, UPDATE).
        Returns the cursor for inspection if needed, or None.
        """
        compiled_query = self._compile_if_needed(query)
        conn, should_close = self._get_connection_for_query()
        try:
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
                # Return result for SELECT queries executed via .execute() (common in tests)
                if cur.description:
                    return cur.fetchall()
        finally:
            if should_close:
                if getattr(conn, "autocommit", False) is False:
                    conn.commit()
                conn.close()

    def fetch_all(self, query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]:
        """
        Executes a query and returns all resulting rows.
        """
        compiled_query = self._compile_if_needed(query)
        conn, should_close = self._get_connection_for_query()
        try:
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
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
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
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
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            if should_close:
                if getattr(conn, "autocommit", False) is False:
                    conn.commit()
                conn.close()

    def begin(self, isolation_level: str | None = None) -> None:
        if self._has_active_transaction():
            raise RuntimeError("Transaction already active.")

        if self.connection is not None:
            self._transaction_connection = self.connection
            self._owns_transaction_connection = False
        else:
            psycopg = self._get_psycopg()
            self._transaction_connection = psycopg.connect(self.connection_info)
            self._owns_transaction_connection = True

        if hasattr(self._transaction_connection, "autocommit"):
            self._transaction_previous_autocommit = self._transaction_connection.autocommit
            self._transaction_connection.autocommit = False
        else:
            self._transaction_previous_autocommit = None

        with self._transaction_connection.cursor() as cur:
            if isolation_level:
                cur.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")

    def commit(self) -> None:
        conn = self._require_active_transaction_connection()
        try:
            conn.commit()
            if (
                self._transaction_previous_autocommit is not None
                and hasattr(conn, "autocommit")
            ):
                conn.autocommit = self._transaction_previous_autocommit
        finally:
            if self._owns_transaction_connection:
                conn.close()
            self._transaction_connection = None
            self._owns_transaction_connection = False
            self._transaction_previous_autocommit = None

    def rollback(self) -> None:
        conn = self._require_active_transaction_connection()
        try:
            conn.rollback()
            if (
                self._transaction_previous_autocommit is not None
                and hasattr(conn, "autocommit")
            ):
                conn.autocommit = self._transaction_previous_autocommit
        finally:
            if self._owns_transaction_connection:
                conn.close()
            self._transaction_connection = None
            self._owns_transaction_connection = False
            self._transaction_previous_autocommit = None

    def savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        with conn.cursor() as cur:
            cur.execute(f"SAVEPOINT {name}")

    def rollback_to_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        with conn.cursor() as cur:
            cur.execute(f"ROLLBACK TO SAVEPOINT {name}")

    def release_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        with conn.cursor() as cur:
            cur.execute(f"RELEASE SAVEPOINT {name}")
