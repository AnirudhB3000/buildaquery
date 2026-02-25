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

    def execute(self, query: CompiledQuery | ASTNode) -> Any:
        """
        Executes a query that does not return results (e.g., INSERT, UPDATE).
        Returns the cursor for inspection if needed, or None.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            with self.connection.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
                # Return result for SELECT queries executed via .execute() (common in tests)
                if cur.description:
                    return cur.fetchall()
        else:
            psycopg = self._get_psycopg()
            with psycopg.connect(self.connection_info) as conn:
                with conn.cursor() as cur:
                    cur.execute(compiled_query.sql, compiled_query.params)
                    if cur.description:
                        return cur.fetchall()

    def fetch_all(self, query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]:
        """
        Executes a query and returns all resulting rows.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            with self.connection.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Sequence[Any]], cur.fetchall())
        else:
            psycopg = self._get_psycopg()
            with psycopg.connect(self.connection_info) as conn:
                with conn.cursor() as cur:
                    cur.execute(compiled_query.sql, compiled_query.params)
                    return cast(Sequence[Sequence[Any]], cur.fetchall())

    def fetch_one(self, query: CompiledQuery | ASTNode) -> Sequence[Any] | None:
        """
        Executes a query and returns a single resulting row.
        """
        compiled_query = self._compile_if_needed(query)
        if self.connection:
            with self.connection.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Any] | None, cur.fetchone())
        else:
            psycopg = self._get_psycopg()
            with psycopg.connect(self.connection_info) as conn:
                with conn.cursor() as cur:
                    cur.execute(compiled_query.sql, compiled_query.params)
                    return cast(Sequence[Any] | None, cur.fetchone())

    def execute_raw(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """
        Executes a raw SQL string.
        """
        if self.connection:
            with self.connection.cursor() as cur:
                cur.execute(sql, params)
        else:
            psycopg = self._get_psycopg()
            with psycopg.connect(self.connection_info) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
