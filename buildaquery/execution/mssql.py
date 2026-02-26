import importlib
from typing import Any, Sequence, cast
from urllib.parse import urlparse, unquote, parse_qs

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.compiler.mssql.mssql_compiler import MsSqlCompiler
from buildaquery.execution.base import Executor
from buildaquery.abstract_syntax_tree.models import ASTNode

# ==================================================
# SQL Server Executor
# ==================================================

class MsSqlExecutor(Executor):
    """
    An executor for SQL Server using the 'pyodbc' library.
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
            connection_info: A SQL Server connection URL (mssql://user:pass@host:port/db?driver=...).
            connection: An existing pyodbc connection object.
            compiler: An optional compiler instance to compile AST nodes automatically.
        """
        if connection_info is None and connection is None:
            raise ValueError("Either connection_info or connection must be provided.")

        self.connection_info = connection_info
        self.connection = connection
        self.compiler = compiler or MsSqlCompiler()
        self._pyodbc = None

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        """
        Compiles the query if it is an AST node.
        """
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_pyodbc(self) -> Any:
        """
        Lazily imports pyodbc and returns the module.
        """
        if self._pyodbc is None:
            try:
                self._pyodbc = importlib.import_module("pyodbc")
            except ImportError:
                raise ImportError(
                    "The 'pyodbc' library is required for MsSqlExecutor. "
                    "Install it with 'pip install pyodbc'."
                )
        return self._pyodbc

    def _build_connection_string(self, config: dict[str, Any]) -> str:
        driver = config.get("driver", "ODBC Driver 18 for SQL Server")
        server = config.get("server", config.get("host", "127.0.0.1"))
        port = config.get("port")
        database = config.get("database")
        user = config.get("user")
        password = config.get("password")
        encrypt = config.get("encrypt", "no")
        trust_cert = config.get("trust_server_certificate", "yes")

        server_part = f"{server},{port}" if port else server
        parts = [f"DRIVER={{{driver}}}", f"SERVER={server_part}"]
        if database:
            parts.append(f"DATABASE={database}")
        if user:
            parts.append(f"UID={user}")
        if password:
            parts.append(f"PWD={password}")
        if encrypt is not None:
            parts.append(f"Encrypt={encrypt}")
        if trust_cert is not None:
            parts.append(f"TrustServerCertificate={trust_cert}")
        return ";".join(parts)

    def _parse_connection_info(self) -> str:
        """
        Parses a mssql:// connection string into a pyodbc connection string.
        """
        if isinstance(self.connection_info, dict):
            if "connection_string" in self.connection_info:
                return self.connection_info["connection_string"]
            return self._build_connection_string(self.connection_info)

        if not isinstance(self.connection_info, str):
            raise ValueError("connection_info must be a connection string or a dict.")

        parsed = urlparse(self.connection_info)
        if parsed.scheme != "mssql":
            raise ValueError("SQL Server connection string must start with mssql://")

        query = parse_qs(parsed.query)
        driver = query.get("driver", ["ODBC Driver 18 for SQL Server"])[0]
        encrypt = query.get("encrypt", ["no"])[0]
        trust_cert = query.get("trust_server_certificate", ["yes"])[0]

        config = {
            "user": unquote(parsed.username) if parsed.username else None,
            "password": unquote(parsed.password) if parsed.password else None,
            "host": parsed.hostname or "127.0.0.1",
            "port": parsed.port,
            "database": parsed.path.lstrip("/") if parsed.path else None,
            "driver": driver,
            "encrypt": encrypt,
            "trust_server_certificate": trust_cert
        }
        return self._build_connection_string({key: value for key, value in config.items() if value is not None})

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

        pyodbc = self._get_pyodbc()
        conn = pyodbc.connect(self._parse_connection_info())
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

        pyodbc = self._get_pyodbc()
        conn = pyodbc.connect(self._parse_connection_info())
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

        pyodbc = self._get_pyodbc()
        conn = pyodbc.connect(self._parse_connection_info())
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

        pyodbc = self._get_pyodbc()
        conn = pyodbc.connect(self._parse_connection_info())
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or [])
                conn.commit()
            finally:
                cursor.close()
        finally:
            conn.close()
