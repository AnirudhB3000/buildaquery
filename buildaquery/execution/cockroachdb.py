from typing import Any, Sequence, cast
import time
from uuid import uuid4

from buildaquery.abstract_syntax_tree.models import ASTNode
from buildaquery.compiler.cockroachdb.cockroachdb_compiler import CockroachDbCompiler
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.base import Executor
from buildaquery.execution.connection import ConnectionAcquireHook, ConnectionReleaseHook, ConnectionSettings
from buildaquery.execution.observability import ObservabilitySettings

# ==================================================
# CockroachDB Executor
# ==================================================


class CockroachExecutor(Executor):
    """
    An executor for CockroachDB using the 'psycopg' library.
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
        self.compiler = compiler or CockroachDbCompiler()
        self.connection_settings = ConnectionSettings(
            connect_timeout_seconds=connect_timeout_seconds,
            acquire_connection=acquire_connection,
            release_connection=release_connection,
        )
        self.observability_settings = observability_settings or ObservabilitySettings()
        self._psycopg = None
        self._closed = False
        self._transaction_connection: Any | None = None
        self._transaction_release_mode: str | None = None
        self._transaction_previous_autocommit: bool | None = None
        self._transaction_id: str | None = None
        self._transaction_started_at: float | None = None

    def _compile_if_needed(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        if isinstance(query, ASTNode):
            return self.compiler.compile(query)
        return query

    def _get_psycopg(self) -> Any:
        if self._psycopg is None:
            try:
                import psycopg

                self._psycopg = psycopg
            except ImportError:
                raise ImportError(
                    "The 'psycopg' library is required for CockroachExecutor. "
                    "Install it with 'pip install psycopg[binary]'."
                )
        return self._psycopg

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Executor is closed.")

    def _connect(self) -> Any:
        psycopg = self._get_psycopg()
        timeout = self.connection_settings.connect_timeout_seconds
        if timeout is None:
            return psycopg.connect(self.connection_info)
        try:
            return psycopg.connect(self.connection_info, connect_timeout=timeout)
        except TypeError:
            return psycopg.connect(self.connection_info)

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
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
                if cur.description:
                    return cur.fetchall()
        finally:
            if release_mode is not None:
                if getattr(conn, "autocommit", False) is False:
                    conn.commit()
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
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
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
            with conn.cursor() as cur:
                cur.execute(compiled_query.sql, compiled_query.params)
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
            with conn.cursor() as cur:
                cur.executemany(sql, param_sets)
        finally:
            if release_mode is not None:
                if getattr(conn, "autocommit", False) is False:
                    conn.commit()
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
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            if release_mode is not None:
                if getattr(conn, "autocommit", False) is False:
                    conn.commit()
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

        if hasattr(self._transaction_connection, "autocommit"):
            self._transaction_previous_autocommit = self._transaction_connection.autocommit
            self._transaction_connection.autocommit = False
        else:
            self._transaction_previous_autocommit = None

        with self._transaction_connection.cursor() as cur:
            if isolation_level:
                cur.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
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
        self._transaction_previous_autocommit = None
        self._transaction_id = None
        self._transaction_started_at = None
        self._release_connection(conn, mode)

    def commit(self) -> None:
        conn = self._require_active_transaction_connection()
        tx_id = self._transaction_id
        started_at = self._transaction_started_at
        try:
            conn.commit()
            if (
                self._transaction_previous_autocommit is not None
                and hasattr(conn, "autocommit")
            ):
                conn.autocommit = self._transaction_previous_autocommit
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
            if (
                self._transaction_previous_autocommit is not None
                and hasattr(conn, "autocommit")
            ):
                conn.autocommit = self._transaction_previous_autocommit
            duration_ms = None if started_at is None else (time.perf_counter() - started_at) * 1000
            self._emit_event("txn.rollback", success=True, transaction_id=tx_id, duration_ms=duration_ms)
        finally:
            self._finalize_transaction()

    def savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        with conn.cursor() as cur:
            cur.execute(f"SAVEPOINT {name}")
        self._emit_event("txn.savepoint.create", success=True, transaction_id=self._transaction_id, savepoint_name=name)

    def rollback_to_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        with conn.cursor() as cur:
            cur.execute(f"ROLLBACK TO SAVEPOINT {name}")
        self._emit_event("txn.savepoint.rollback", success=True, transaction_id=self._transaction_id, savepoint_name=name)

    def release_savepoint(self, name: str) -> None:
        conn = self._require_active_transaction_connection()
        with conn.cursor() as cur:
            cur.execute(f"RELEASE SAVEPOINT {name}")
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
