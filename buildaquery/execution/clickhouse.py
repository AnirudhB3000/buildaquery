from typing import Any, Sequence, cast
from urllib.parse import unquote, urlparse
import re

from buildaquery.abstract_syntax_tree.models import ASTNode
from buildaquery.compiler.clickhouse.clickhouse_compiler import ClickHouseCompiler
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.base import Executor
from buildaquery.execution.connection import ConnectionAcquireHook, ConnectionReleaseHook, ConnectionSettings
from buildaquery.execution.observability import ObservabilitySettings

# ==================================================
# ClickHouse Executor
# ==================================================


class ClickHouseExecutor(Executor):
    """
    An executor for ClickHouse using the `clickhouse-driver` DB-API module.
    """

    def __init__(
        self,
        connection_info: str | dict[str, Any] | None = None,
        connection: Any | None = None,
        compiler: Any | None = None,
        connect_timeout_seconds: float | None = None,
        acquire_connection: ConnectionAcquireHook | None = None,
        release_connection: ConnectionReleaseHook | None = None,
        observability_settings: ObservabilitySettings | None = None,
    ) -> None:
        if connection_info is None and connection is None and acquire_connection is None:
            raise ValueError("Provide connection_info, connection, or acquire_connection.")

        self.connection_info = connection_info
        self.connection = connection
        self.compiler = compiler or ClickHouseCompiler()
        self.connection_settings = ConnectionSettings(
            connect_timeout_seconds=connect_timeout_seconds,
            acquire_connection=acquire_connection,
            release_connection=release_connection,
        )
        self.observability_settings = observability_settings or ObservabilitySettings()
        self._clickhouse_dbapi = None
        self._closed = False

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_clickhouse_dbapi(self) -> Any:
        if self._clickhouse_dbapi is None:
            try:
                import importlib

                self._clickhouse_dbapi = importlib.import_module("clickhouse_driver.dbapi")
            except ImportError:
                raise ImportError(
                    "The 'clickhouse-driver' library is required for ClickHouseExecutor. "
                    "Install it with 'pip install clickhouse-driver'."
                )
        return self._clickhouse_dbapi

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Executor is closed.")

    def _parse_connection_info(self) -> dict[str, Any]:
        if isinstance(self.connection_info, dict):
            return dict(self.connection_info)
        if not isinstance(self.connection_info, str):
            raise ValueError("connection_info must be a connection string or a dict.")

        parsed = urlparse(self.connection_info)
        if parsed.scheme != "clickhouse":
            raise ValueError("ClickHouse connection string must start with clickhouse://")

        config = {
            "host": parsed.hostname or "127.0.0.1",
            "port": parsed.port or 9000,
            "database": parsed.path.lstrip("/") if parsed.path else "default",
            "user": unquote(parsed.username) if parsed.username else "default",
            "password": unquote(parsed.password) if parsed.password else "",
        }
        return config

    def _connect(self) -> Any:
        dbapi = self._get_clickhouse_dbapi()
        kwargs = self._parse_connection_info()
        timeout = self.connection_settings.connect_timeout_seconds
        if timeout is not None:
            kwargs["connect_timeout"] = timeout
            kwargs["send_receive_timeout"] = timeout
        return dbapi.connect(**kwargs)

    def _execute_with_connection(self, connection: Any, compiled_query: CompiledQuery) -> Any:
        cursor = connection.cursor()
        try:
            sql, rows = self._prepare_insert_sql_and_rows(
                compiled_query.sql, compiled_query.params
            )
            if rows is not None:
                cursor.executemany(sql, rows)
            else:
                self._cursor_execute(cursor, compiled_query.sql, compiled_query.params)
            if cursor.description:
                return cursor.fetchall()
            return None
        finally:
            cursor.close()

    def _cursor_execute(self, cursor: Any, sql: str, params: Sequence[Any] | None) -> None:
        if params is None or len(params) == 0:
            cursor.execute(sql)
            return
        cursor.execute(sql, params)

    def _prepare_insert_sql_and_rows(
        self,
        sql: str,
        params: Sequence[Any] | None,
    ) -> tuple[str, Sequence[Sequence[Any]] | None]:
        """
        clickhouse-driver expects INSERT data as rows payload, not `%s` value placeholders.
        Convert `INSERT ... VALUES (%s, ...)` statements into `INSERT ... VALUES` + rows.
        """
        upper_sql = sql.upper()
        if not upper_sql.startswith("INSERT INTO") or "VALUES" not in upper_sql:
            return sql, None
        if params is None:
            return sql, None

        values_index = upper_sql.index("VALUES")
        insert_sql = sql[: values_index + len("VALUES")]
        values_fragment = sql[values_index + len("VALUES") :]
        total_placeholders = values_fragment.count("%s")
        if total_placeholders == 0:
            return sql, None

        first_group = re.search(r"\(([^)]*)\)", values_fragment)
        if first_group is None:
            return sql, None
        row_width = first_group.group(1).count("%s")
        if row_width <= 0:
            return sql, None

        if len(params) != total_placeholders:
            return sql, None
        if total_placeholders % row_width != 0:
            return sql, None

        rows: list[tuple[Any, ...]] = []
        for index in range(0, len(params), row_width):
            rows.append(tuple(params[index : index + row_width]))
        return insert_sql, rows

    def _has_active_transaction(self) -> bool:
        return False

    def _get_connection_for_query(self) -> tuple[Any, str | None]:
        self._ensure_open()
        if self.connection is not None:
            return self.connection, None
        if self.connection_settings.acquire_connection is not None:
            self._emit_event("connection.acquire.start", success=True)
            conn = self.connection_settings.acquire_connection()
            self._emit_event("connection.acquire.end", success=True, connection_id=str(id(conn)))
            return conn, "release"
        self._emit_event("connection.acquire.start", success=True)
        conn = self._connect()
        self._emit_event("connection.acquire.end", success=True, connection_id=str(id(conn)))
        return conn, "close"

    def _release_connection(self, conn: Any, mode: str | None) -> None:
        if mode is None:
            return
        if mode == "release":
            self._emit_event("connection.release", success=True, connection_id=str(id(conn)))
            if self.connection_settings.release_connection is not None:
                self.connection_settings.release_connection(conn)
                return
            conn.close()
            return
        if mode == "close":
            self._emit_event("connection.close", success=True, connection_id=str(id(conn)))
            conn.close()

    def execute(self, query: CompiledQuery | ASTNode) -> Any:
        compiled_query = self._compile_if_needed(query)
        return self._observe_query(
            operation="execute",
            sql=compiled_query.sql,
            params=compiled_query.params,
            run=lambda: self._execute_observed(compiled_query),
        )

    def _execute_observed(self, compiled_query: CompiledQuery) -> Any:
        conn, release_mode = self._get_connection_for_query()
        try:
            return self._execute_with_connection(conn, compiled_query)
        finally:
            self._release_connection(conn, release_mode)

    def fetch_all(self, query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]:
        compiled_query = self._compile_if_needed(query)
        return self._observe_query(
            operation="fetch_all",
            sql=compiled_query.sql,
            params=compiled_query.params,
            run=lambda: self._fetch_all_observed(compiled_query),
        )

    def _fetch_all_observed(self, compiled_query: CompiledQuery) -> Sequence[Sequence[Any]]:
        conn, release_mode = self._get_connection_for_query()
        try:
            cursor = conn.cursor()
            try:
                self._cursor_execute(cursor, compiled_query.sql, compiled_query.params)
                return cast(Sequence[Sequence[Any]], cursor.fetchall())
            finally:
                cursor.close()
        finally:
            self._release_connection(conn, release_mode)

    def fetch_one(self, query: CompiledQuery | ASTNode) -> Sequence[Any] | None:
        compiled_query = self._compile_if_needed(query)
        return self._observe_query(
            operation="fetch_one",
            sql=compiled_query.sql,
            params=compiled_query.params,
            run=lambda: self._fetch_one_observed(compiled_query),
        )

    def _fetch_one_observed(self, compiled_query: CompiledQuery) -> Sequence[Any] | None:
        conn, release_mode = self._get_connection_for_query()
        try:
            cursor = conn.cursor()
            try:
                self._cursor_execute(cursor, compiled_query.sql, compiled_query.params)
                return cast(Sequence[Any] | None, cursor.fetchone())
            finally:
                cursor.close()
        finally:
            self._release_connection(conn, release_mode)

    def execute_many(self, sql: str, param_sets: Sequence[Sequence[Any]]) -> None:
        if not param_sets:
            return
        self._observe_query(
            operation="execute_many",
            sql=sql,
            params=param_sets[0],
            run=lambda: self._execute_many_observed(sql, param_sets),
        )

    def _execute_many_observed(self, sql: str, param_sets: Sequence[Sequence[Any]]) -> None:
        conn, release_mode = self._get_connection_for_query()
        try:
            cursor = conn.cursor()
            try:
                insert_sql, _ = self._prepare_insert_sql_and_rows(sql, param_sets[0] if param_sets else None)
                if insert_sql != sql and "VALUES" in sql.upper():
                    cursor.executemany(insert_sql, param_sets)
                else:
                    cursor.executemany(sql, param_sets)
            finally:
                cursor.close()
        finally:
            self._release_connection(conn, release_mode)

    def execute_raw(self, sql: str, params: Sequence[Any] | None = None) -> None:
        self._observe_query(
            operation="execute_raw",
            sql=sql,
            params=params,
            run=lambda: self._execute_raw_observed(sql, params),
        )

    def _execute_raw_observed(self, sql: str, params: Sequence[Any] | None = None) -> None:
        conn, release_mode = self._get_connection_for_query()
        try:
            cursor = conn.cursor()
            try:
                if params is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, params)
            finally:
                cursor.close()
        finally:
            self._release_connection(conn, release_mode)

    def begin(self, isolation_level: str | None = None) -> None:
        _ = isolation_level
        raise RuntimeError("ClickHouse does not support explicit transaction control APIs.")

    def commit(self) -> None:
        raise RuntimeError("ClickHouse does not support explicit transaction control APIs.")

    def rollback(self) -> None:
        raise RuntimeError("ClickHouse does not support explicit transaction control APIs.")

    def savepoint(self, name: str) -> None:
        _ = name
        raise RuntimeError("ClickHouse does not support savepoint APIs.")

    def rollback_to_savepoint(self, name: str) -> None:
        _ = name
        raise RuntimeError("ClickHouse does not support savepoint APIs.")

    def release_savepoint(self, name: str) -> None:
        _ = name
        raise RuntimeError("ClickHouse does not support savepoint APIs.")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
