from typing import Any, Sequence, cast
from buildaquery.compiler.postgres.postgres_compiler import CompiledQuery
from buildaquery.execution.base import Executor

# ==================================================
# PostgreSQL Executor
# ==================================================

class PostgresExecutor(Executor):
    """
    An executor for PostgreSQL using the 'psycopg' library.
    """

    def __init__(self, connection_info: str | dict[str, Any]) -> None:
        """
        Initializes the executor with connection information.
        
        Args:
            connection_info: A connection string or a dictionary of parameters 
                             compatible with psycopg.connect().
        """
        self.connection_info = connection_info
        self._psycopg = None

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

    def execute(self, compiled_query: CompiledQuery) -> None:
        """
        Executes a query that does not return results (e.g., INSERT, UPDATE).
        """
        psycopg = self._get_psycopg()
        # Use 'with' to ensure the connection and cursor are closed
        with psycopg.connect(self.connection_info) as conn:
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)

    def fetch_all(self, compiled_query: CompiledQuery) -> Sequence[Sequence[Any]]:
        """
        Executes a query and returns all resulting rows.
        """
        psycopg = self._get_psycopg()
        with psycopg.connect(self.connection_info) as conn:
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Sequence[Any]], cur.fetchall())

    def fetch_one(self, compiled_query: CompiledQuery) -> Sequence[Any] | None:
        """
        Executes a query and returns a single resulting row.
        """
        psycopg = self._get_psycopg()
        with psycopg.connect(self.connection_info) as conn:
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
                return cast(Sequence[Any] | None, cur.fetchone())
