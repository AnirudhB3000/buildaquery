from typing import Any, Sequence, cast
from urllib.parse import unquote, urlparse
import time
from uuid import uuid4

from buildaquery.abstract_syntax_tree.models import ASTNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
from buildaquery.execution.base import Executor
from buildaquery.execution.connection import ConnectionAcquireHook, ConnectionReleaseHook, ConnectionSettings
from buildaquery.execution.observability import ObservabilitySettings

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
        self.compiler = compiler or MySqlCompiler()
        self.connection_settings = ConnectionSettings(
            connect_timeout_seconds=connect_timeout_seconds,
            acquire_connection=acquire_connection,
            release_connection=release_connection,
        )
        self.observability_settings = observability_settings or ObservabilitySettings()
        self._mysql_connector = None
        self._closed = False
        self._transaction_connection: Any | None = None
        self._transaction_release_mode: str | None = None
        self._transaction_id: str | None = None
        self._transaction_started_at: float | None = None

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_mysql_connector(self) -> Any:
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

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Executor is closed.")

    def _parse_connection_info(self) -> dict[str, Any]:
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
            "database": parsed.path.lstrip("/") if parsed.path else None,
        }
        return {key: value for key, value in config.items() if value is not None}

    def _connect(self) -> Any:
        mysql_connector = self._get_mysql_connector()
        kwargs = self._parse_connection_info()
        timeout = self.connection_settings.connect_timeout_seconds
        if timeout is not None:
            kwargs["connection_timeout"] = timeout
        return mysql_connector.connect(**kwargs)

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

    def _get_connection_for_query(self) -> tuple[Any, str | None]:
        self._ensure_open()
        if self._transaction_connection is not None:
            return self._transaction_connection, None
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

    def _require_active_transaction_connection(self) -> Any:
        self._ensure_open()
        if self._transaction_connection is None:
            raise RuntimeError("No active transaction. Call begin() first.")
        return self._transaction_connection

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
            result = self._execute_with_connection(conn, compiled_query)
            if release_mode is not None:
                conn.commit()
            return result
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
                cursor.execute(compiled_query.sql, compiled_query.params)
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
                cursor.execute(compiled_query.sql, compiled_query.params)
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
                cursor.executemany(sql, param_sets)
                if release_mode is not None:
                    conn.commit()
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
                cursor.execute(sql, params or [])
                if release_mode is not None:
                    conn.commit()
            finally:
                cursor.close()
        finally:
            self._release_connection(conn, release_mode)

    def begin(self, isolation_level: str | None = None) -> None:
        self._ensure_open()
        if self._has_active_transaction():
            raise RuntimeError("Transaction already active.")

        if self.connection is not None:
            self._transaction_connection = self.connection
            self._transaction_release_mode = None
        elif self.connection_settings.acquire_connection is not None:
            self._emit_event("connection.acquire.start", success=True)
            self._transaction_connection = self.connection_settings.acquire_connection()
            self._transaction_release_mode = "release"
            self._emit_event(
                "connection.acquire.end",
                success=True,
                connection_id=str(id(self._transaction_connection)),
            )
        else:
            self._emit_event("connection.acquire.start", success=True)
            self._transaction_connection = self._connect()
            self._transaction_release_mode = "close"
            self._emit_event(
                "connection.acquire.end",
                success=True,
                connection_id=str(id(self._transaction_connection)),
            )

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
        self._transaction_id = uuid4().hex
        self._transaction_started_at = time.perf_counter()
        self._emit_event("txn.begin", success=True, transaction_id=self._transaction_id)

    def _finalize_transaction(self) -> None:
        conn = self._transaction_connection
        mode = self._transaction_release_mode
        if conn is None:
            return
        self._transaction_connection = None
        self._transaction_release_mode = None
        self._transaction_id = None
        self._transaction_started_at = None
        self._release_connection(conn, mode)

    def commit(self) -> None:
        conn = self._require_active_transaction_connection()
        tx_id = self._transaction_id
        started_at = self._transaction_started_at
        try:
            conn.commit()
            duration_ms = None if started_at is None else (time.perf_counter() - started_at) * 1000
            self._emit_event("txn.commit", success=True, transaction_id=tx_id, duration_ms=duration_ms)
        finally:
            self._finalize_transaction()

    def rollback(self) -> None:
        conn = self._require_active_transaction_connection()
        tx_id = self._transaction_id
        started_at = self._transaction_started_at
        try:
            conn.rollback()
            duration_ms = None if started_at is None else (time.perf_counter() - started_at) * 1000
            self._emit_event("txn.rollback", success=True, transaction_id=tx_id, duration_ms=duration_ms)
        finally:
            self._finalize_transaction()

    def savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"SAVEPOINT {name}")
        finally:
            cursor.close()
        self._emit_event("txn.savepoint.create", success=True, transaction_id=self._transaction_id, savepoint_name=name)

    def rollback_to_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")
        finally:
            cursor.close()
        self._emit_event("txn.savepoint.rollback", success=True, transaction_id=self._transaction_id, savepoint_name=name)

    def release_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"RELEASE SAVEPOINT {name}")
        finally:
            cursor.close()
        self._emit_event("txn.savepoint.release", success=True, transaction_id=self._transaction_id, savepoint_name=name)

    def close(self) -> None:
        if self._closed:
            return
        if self._transaction_connection is not None:
            conn = self._transaction_connection
            tx_id = self._transaction_id
            started_at = self._transaction_started_at
            try:
                conn.rollback()
                duration_ms = None if started_at is None else (time.perf_counter() - started_at) * 1000
                self._emit_event("txn.rollback", success=True, transaction_id=tx_id, duration_ms=duration_ms)
            except Exception:
                pass
            self._finalize_transaction()
        self._closed = True
