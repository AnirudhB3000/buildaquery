from typing import Any, Sequence, cast
from urllib.parse import urlparse, unquote

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
from buildaquery.execution.base import Executor
from buildaquery.abstract_syntax_tree.models import ASTNode

# ==================================================
# MySQL Executor
# ==================================================

class MySqlExecutor(Executor):
    """
    An executor for MySQL using the 'mysql-connector-python' library.
    """

    def __init__(
        self,
        connection_info: str | dict[str, Any] | None = None,
        connection: Any | None = None,
        compiler: Any | None = None
    ) -> None:
        """
        Initializes the executor with connection information or an existing connection.

        Args:
            connection_info: A MySQL connection URL (mysql://user:pass@host:port/db) or config dict.
            connection: An existing mysql.connector connection object.
            compiler: An optional compiler instance to compile AST nodes automatically.
        """
        if connection_info is None and connection is None:
            raise ValueError("Either connection_info or connection must be provided.")

        self.connection_info = connection_info
        self.connection = connection
        self.compiler = compiler or MySqlCompiler()
        self._mysql_connector = None
        self._transaction_connection: Any | None = None
        self._owns_transaction_connection = False

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        """
        Compiles the query if it is an AST node.
        """
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_mysql_connector(self) -> Any:
        """
        Lazily imports mysql.connector and returns the module.
        """
        if self._mysql_connector is None:
            try:
                import importlib
                self._mysql_connector = importlib.import_module("mysql.connector")
            except ImportError:
                raise ImportError(
                    "The 'mysql-connector-python' library is required for MySqlExecutor. "
                    "Install it with 'pip install mysql-connector-python'."
                )
        return self._mysql_connector

    def _parse_connection_info(self) -> dict[str, Any]:
        """
        Parses a mysql:// connection string into mysql.connector kwargs.
        """
        if isinstance(self.connection_info, dict):
            return self.connection_info

        if not isinstance(self.connection_info, str):
            raise ValueError("connection_info must be a connection string or a dict.")

        parsed = urlparse(self.connection_info)
        if parsed.scheme not in {"mysql"}:
            raise ValueError("MySQL connection string must start with mysql://")

        config = {
            "user": unquote(parsed.username) if parsed.username else None,
            "password": unquote(parsed.password) if parsed.password else None,
            "host": parsed.hostname or "127.0.0.1",
            "port": parsed.port or 3306,
            "database": parsed.path.lstrip("/") if parsed.path else None
        }
        return {key: value for key, value in config.items() if value is not None}

    def _execute_with_connection(self, connection: Any, compiled_query: CompiledQuery) -> Any:
        cursor = connection.cursor()
        try:
            cursor.execute(compiled_query.sql, compiled_query.params)
            if cursor.description:
                return cursor.fetchall()
            return None
        finally:
            cursor.close()

    def _has_active_transaction(self) -> bool:
        return self._transaction_connection is not None

    def _get_connection_for_query(self) -> tuple[Any, bool]:
        if self._transaction_connection is not None:
            return self._transaction_connection, False
        if self.connection is not None:
            return self.connection, False

        mysql_connector = self._get_mysql_connector()
        conn = mysql_connector.connect(**self._parse_connection_info())
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
            cursor = conn.cursor()
            try:
                cursor.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Sequence[Any]], cursor.fetchall())
            finally:
                cursor.close()
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
            cursor = conn.cursor()
            try:
                cursor.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Any] | None, cursor.fetchone())
            finally:
                cursor.close()
        finally:
            if should_close:
                conn.close()

    def execute_many(self, sql: str, param_sets: Sequence[Sequence[Any]]) -> None:
        """
        Executes a parameterized SQL statement for multiple parameter sets.
        """
        if not param_sets:
            return
        conn, should_close = self._get_connection_for_query()
        try:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, param_sets)
                if should_close:
                    conn.commit()
            finally:
                cursor.close()
        finally:
            if should_close:
                conn.close()

    def execute_raw(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """
        Executes a raw SQL string.
        """
        conn, should_close = self._get_connection_for_query()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or [])
                if should_close:
                    conn.commit()
            finally:
                cursor.close()
        finally:
            if should_close:
                conn.close()

    def begin(self, isolation_level: str | None = None) -> None:
        if self._has_active_transaction():
            raise RuntimeError("Transaction already active.")

        if self.connection is not None:
            self._transaction_connection = self.connection
            self._owns_transaction_connection = False
        else:
            mysql_connector = self._get_mysql_connector()
            self._transaction_connection = mysql_connector.connect(**self._parse_connection_info())
            self._owns_transaction_connection = True

        if isolation_level:
            cursor = self._transaction_connection.cursor()
            try:
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
            finally:
                cursor.close()

        if hasattr(self._transaction_connection, "start_transaction"):
            self._transaction_connection.start_transaction()
        else:
            cursor = self._transaction_connection.cursor()
            try:
                cursor.execute("START TRANSACTION")
            finally:
                cursor.close()

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
        cursor = conn.cursor()
        try:
            cursor.execute(f"SAVEPOINT {name}")
        finally:
            cursor.close()

    def rollback_to_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")
        finally:
            cursor.close()

    def release_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"RELEASE SAVEPOINT {name}")
        finally:
            cursor.close()
