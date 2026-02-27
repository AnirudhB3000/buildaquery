from typing import Any, Sequence, cast

from buildaquery.abstract_syntax_tree.models import ASTNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler
from buildaquery.execution.base import Executor
from buildaquery.execution.connection import ConnectionAcquireHook, ConnectionReleaseHook, ConnectionSettings
from buildaquery.execution.observability import ObservabilitySettings

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
        self.compiler = compiler or SqliteCompiler()
        self.connection_settings = ConnectionSettings(
            connect_timeout_seconds=connect_timeout_seconds,
            acquire_connection=acquire_connection,
            release_connection=release_connection,
        )
        self.observability_settings = observability_settings or ObservabilitySettings()
        self._sqlite3 = None
        self._closed = False
        self._transaction_connection: Any | None = None
        self._transaction_release_mode: str | None = None

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_sqlite3(self) -> Any:
        if self._sqlite3 is None:
            import sqlite3

            self._sqlite3 = sqlite3
        return self._sqlite3

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Executor is closed.")

    def _connect(self) -> Any:
        sqlite3 = self._get_sqlite3()
        timeout = self.connection_settings.connect_timeout_seconds
        if timeout is None:
            return sqlite3.connect(self.connection_info)
        return sqlite3.connect(self.connection_info, timeout=timeout)

    def _execute_with_connection(self, connection: Any, compiled_query: CompiledQuery) -> Any:
        cur = connection.execute(compiled_query.sql, compiled_query.params)
        if cur.description:
            return cur.fetchall()
        return None

    def _has_active_transaction(self) -> bool:
        return self._transaction_connection is not None

    def _get_connection_for_query(self) -> tuple[Any, str | None]:
        self._ensure_open()
        if self._transaction_connection is not None:
            return self._transaction_connection, None
        if self.connection is not None:
            return self.connection, None
        if self.connection_settings.acquire_connection is not None:
            return self.connection_settings.acquire_connection(), "release"
        return self._connect(), "close"

    def _release_connection(self, conn: Any, mode: str | None) -> None:
        if mode is None:
            return
        if mode == "release":
            if self.connection_settings.release_connection is not None:
                self.connection_settings.release_connection(conn)
                return
            conn.close()
            return
        if mode == "close":
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
            cur = conn.execute(compiled_query.sql, compiled_query.params)
            return cast(Sequence[Sequence[Any]], cur.fetchall())
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
            cur = conn.execute(compiled_query.sql, compiled_query.params)
            return cast(Sequence[Any] | None, cur.fetchone())
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
            conn.executemany(sql, param_sets)
            if release_mode is not None:
                conn.commit()
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
            conn.execute(sql, params or [])
            if release_mode is not None:
                conn.commit()
        finally:
            self._release_connection(conn, release_mode)

    def begin(self, isolation_level: str | None = None) -> None:
        self._ensure_open()
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
            self._transaction_release_mode = None
        elif self.connection_settings.acquire_connection is not None:
            self._transaction_connection = self.connection_settings.acquire_connection()
            self._transaction_release_mode = "release"
        else:
            self._transaction_connection = self._connect()
            self._transaction_release_mode = "close"

        if normalized:
            self._transaction_connection.execute(f"BEGIN {normalized}")
        else:
            self._transaction_connection.execute("BEGIN")

    def _finalize_transaction(self) -> None:
        conn = self._transaction_connection
        mode = self._transaction_release_mode
        if conn is None:
            return
        self._transaction_connection = None
        self._transaction_release_mode = None
        self._release_connection(conn, mode)

    def commit(self) -> None:
        conn = self._require_active_transaction_connection()
        try:
            conn.commit()
        finally:
            self._finalize_transaction()

    def rollback(self) -> None:
        conn = self._require_active_transaction_connection()
        try:
            conn.rollback()
        finally:
            self._finalize_transaction()

    def savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        conn.execute(f"SAVEPOINT {name}")

    def rollback_to_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        conn.execute(f"ROLLBACK TO SAVEPOINT {name}")

    def release_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        conn.execute(f"RELEASE SAVEPOINT {name}")

    def close(self) -> None:
        if self._closed:
            return
        if self._transaction_connection is not None:
            conn = self._transaction_connection
            try:
                conn.rollback()
            except Exception:
                pass
            self._finalize_transaction()
        self._closed = True
