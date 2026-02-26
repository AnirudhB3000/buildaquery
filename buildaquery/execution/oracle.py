import importlib
from typing import Any, Sequence, cast
from urllib.parse import urlparse, unquote

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.compiler.oracle.oracle_compiler import OracleCompiler
from buildaquery.execution.base import Executor
from buildaquery.abstract_syntax_tree.models import ASTNode

# ==================================================
# Oracle Executor
# ==================================================

class OracleExecutor(Executor):
    """
    An executor for Oracle using the 'oracledb' library.
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
            connection_info: An Oracle connection URL (oracle://user:pass@host:port/service) or config dict.
            connection: An existing oracledb connection object.
            compiler: An optional compiler instance to compile AST nodes automatically.
        """
        if connection_info is None and connection is None:
            raise ValueError("Either connection_info or connection must be provided.")

        self.connection_info = connection_info
        self.connection = connection
        self.compiler = compiler or OracleCompiler()
        self._oracledb = None

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        """
        Compiles the query if it is an AST node.
        """
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_oracledb(self) -> Any:
        """
        Lazily imports oracledb and returns the module.
        """
        if self._oracledb is None:
            try:
                self._oracledb = importlib.import_module("oracledb")
            except ImportError:
                raise ImportError(
                    "The 'oracledb' library is required for OracleExecutor. "
                    "Install it with 'pip install oracledb'."
                )
        return self._oracledb

    def _parse_connection_info(self) -> dict[str, Any]:
        """
        Parses an oracle:// connection string into oracledb kwargs.
        """
        if isinstance(self.connection_info, dict):
            return self.connection_info

        if not isinstance(self.connection_info, str):
            raise ValueError("connection_info must be a connection string or a dict.")

        parsed = urlparse(self.connection_info)
        if parsed.scheme != "oracle":
            raise ValueError("Oracle connection string must start with oracle://")

        service_name = parsed.path.lstrip("/") if parsed.path else None
        if not service_name:
            raise ValueError("Oracle connection string must include a service name (e.g., /XEPDB1).")

        config = {
            "user": unquote(parsed.username) if parsed.username else None,
            "password": unquote(parsed.password) if parsed.password else None,
            "host": parsed.hostname or "127.0.0.1",
            "port": parsed.port or 1521,
            "service_name": service_name
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

    def execute(self, query: CompiledQuery | ASTNode) -> Any:
        """
        Executes a query. Returns rows for SELECT statements, otherwise None.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            return self._execute_with_connection(self.connection, compiled_query)

        oracledb = self._get_oracledb()
        conn = oracledb.connect(**self._parse_connection_info())
        try:
            result = self._execute_with_connection(conn, compiled_query)
            conn.commit()
            return result
        finally:
            conn.close()

    def fetch_all(self, query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]:
        """
        Executes a query and returns all resulting rows.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            cursor = self.connection.cursor()
            try:
                cursor.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Sequence[Any]], cursor.fetchall())
            finally:
                cursor.close()

        oracledb = self._get_oracledb()
        conn = oracledb.connect(**self._parse_connection_info())
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Sequence[Any]], cursor.fetchall())
            finally:
                cursor.close()
        finally:
            conn.close()

    def fetch_one(self, query: CompiledQuery | ASTNode) -> Sequence[Any] | None:
        """
        Executes a query and returns a single resulting row.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            cursor = self.connection.cursor()
            try:
                cursor.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Any] | None, cursor.fetchone())
            finally:
                cursor.close()

        oracledb = self._get_oracledb()
        conn = oracledb.connect(**self._parse_connection_info())
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Any] | None, cursor.fetchone())
            finally:
                cursor.close()
        finally:
            conn.close()

    def execute_raw(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """
        Executes a raw SQL string.
        """
        if self.connection:
            cursor = self.connection.cursor()
            try:
                cursor.execute(sql, params or [])
            finally:
                cursor.close()
            return

        oracledb = self._get_oracledb()
        conn = oracledb.connect(**self._parse_connection_info())
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or [])
                conn.commit()
            finally:
                cursor.close()
        finally:
            conn.close()
