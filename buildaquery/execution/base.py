from abc import ABC, abstractmethod
from datetime import datetime, timezone
import time
from typing import Any, Mapping, Sequence
from uuid import uuid4
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.errors import ExecutionError, TransientExecutionError, normalize_execution_error
from buildaquery.execution.observability import ExecutionEvent, ObservabilitySettings, QueryObservation
from buildaquery.execution.retry import RetryPolicy, run_with_retry

# ==================================================
# Base Executor
# ==================================================

class Executor(ABC):
    """
    Abstract base class for executing compiled queries against a database.
    """

    @abstractmethod
    def execute(self, compiled_query: CompiledQuery) -> Any:
        """
        Executes a single compiled query.
        """
        pass

    @abstractmethod
    def fetch_all(self, compiled_query: CompiledQuery) -> Sequence[Sequence[Any]]:
        """
        Executes a query and returns all resulting rows.
        """
        pass

    @abstractmethod
    def fetch_one(self, compiled_query: CompiledQuery) -> Sequence[Any] | None:
        """
        Executes a query and returns a single resulting row.
        """
        pass

    @abstractmethod
    def execute_many(self, sql: str, param_sets: Sequence[Sequence[Any]]) -> None:
        """
        Executes a SQL statement against multiple parameter sets.
        """
        pass

    @abstractmethod
    def begin(self, isolation_level: str | None = None) -> None:
        """
        Begins an explicit transaction.
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """
        Commits the active explicit transaction.
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """
        Rolls back the active explicit transaction.
        """
        pass

    @abstractmethod
    def savepoint(self, name: str) -> None:
        """
        Creates a savepoint in the active explicit transaction.
        """
        pass

    @abstractmethod
    def rollback_to_savepoint(self, name: str) -> None:
        """
        Rolls back to a savepoint in the active explicit transaction.
        """
        pass

    @abstractmethod
    def release_savepoint(self, name: str) -> None:
        """
        Releases a savepoint in the active explicit transaction.
        """
        pass

    # ==================================================
    # Normalized Error + Retry Helpers
    # ==================================================

    def _next_query_id(self) -> str:
        return uuid4().hex

    def _metadata(self, override: Mapping[str, Any] | None = None) -> dict[str, Any]:
        settings = getattr(self, "observability_settings", None)
        base = dict(settings.metadata) if isinstance(settings, ObservabilitySettings) else {}
        if override:
            base.update(override)
        return base

    def _emit_event(self, event: str, *, success: bool, **kwargs: Any) -> None:
        settings = getattr(self, "observability_settings", None)
        if not isinstance(settings, ObservabilitySettings) or settings.event_observer is None:
            return

        payload = ExecutionEvent(
            timestamp=datetime.now(timezone.utc).isoformat(),
            event=event,
            dialect=self._dialect_name(),
            executor=self.__class__.__name__,
            success=success,
            metadata=self._metadata(),
            **kwargs,
        )
        settings.event_observer(payload)

    def _dialect_name(self) -> str:
        name = self.__class__.__name__.lower()
        name = name.replace("executor", "")
        return name or "unknown"

    def _normalize_execution_error(self, *, operation: str, exc: Exception) -> ExecutionError:
        return normalize_execution_error(
            dialect=self._dialect_name(),
            operation=operation,
            exc=exc,
        )

    def _observe_query(
        self,
        *,
        operation: str,
        sql: str,
        params: Sequence[Any] | None,
        run: Any,
    ) -> Any:
        settings = getattr(self, "observability_settings", None)
        if not isinstance(settings, ObservabilitySettings):
            return run()

        query_id = self._next_query_id()
        self._emit_event(
            "query.start",
            success=True,
            operation=operation,
            query_id=query_id,
        )

        started = time.perf_counter()
        error: Exception | None = None
        try:
            result = run()
            return result
        except Exception as exc:
            error = exc
            raise
        finally:
            duration_ms = (time.perf_counter() - started) * 1000
            in_transaction = False
            has_active = getattr(self, "_has_active_transaction", None)
            if callable(has_active):
                try:
                    in_transaction = bool(has_active())
                except Exception:
                    in_transaction = False

            if settings.query_observer is not None:
                settings.query_observer(
                    QueryObservation(
                        dialect=self._dialect_name(),
                        operation=operation,
                        sql=sql,
                        param_count=len(params) if params is not None else 0,
                        duration_ms=duration_ms,
                        succeeded=error is None,
                        in_transaction=in_transaction,
                        metadata=self._metadata(),
                        error_type=type(error).__name__ if error is not None else None,
                        error_message=str(error) if error is not None else None,
                    )
                )
            self._emit_event(
                "query.end",
                success=error is None,
                operation=operation,
                query_id=query_id,
                duration_ms=duration_ms,
                error_type=type(error).__name__ if error is not None else None,
                error_message=str(error) if error is not None else None,
            )

    def execute_with_retry(
        self,
        compiled_query: CompiledQuery,
        retry_policy: RetryPolicy | None = None,
    ) -> Any:
        policy = retry_policy or RetryPolicy()
        return run_with_retry(
            operation=lambda: self.execute(compiled_query),
            normalize_error=lambda exc: self._normalize_execution_error(operation="execute", exc=exc),
            policy=policy,
            on_retry=lambda normalized, attempt, delay: self._emit_event(
                "retry.scheduled",
                success=False,
                operation="execute",
                retry_attempt=attempt,
                max_attempts=policy.max_attempts,
                backoff_ms=delay * 1000,
                error_type=type(normalized).__name__,
                retryable=isinstance(normalized, TransientExecutionError),
            ),
            on_giveup=lambda normalized, attempt: self._emit_event(
                "retry.giveup",
                success=False,
                operation="execute",
                retry_attempt=attempt,
                max_attempts=policy.max_attempts,
                error_type=type(normalized).__name__,
                retryable=isinstance(normalized, TransientExecutionError),
            ),
        )

    def fetch_all_with_retry(
        self,
        compiled_query: CompiledQuery,
        retry_policy: RetryPolicy | None = None,
    ) -> Sequence[Sequence[Any]]:
        policy = retry_policy or RetryPolicy()
        return run_with_retry(
            operation=lambda: self.fetch_all(compiled_query),
            normalize_error=lambda exc: self._normalize_execution_error(operation="fetch_all", exc=exc),
            policy=policy,
            on_retry=lambda normalized, attempt, delay: self._emit_event(
                "retry.scheduled",
                success=False,
                operation="fetch_all",
                retry_attempt=attempt,
                max_attempts=policy.max_attempts,
                backoff_ms=delay * 1000,
                error_type=type(normalized).__name__,
                retryable=isinstance(normalized, TransientExecutionError),
            ),
            on_giveup=lambda normalized, attempt: self._emit_event(
                "retry.giveup",
                success=False,
                operation="fetch_all",
                retry_attempt=attempt,
                max_attempts=policy.max_attempts,
                error_type=type(normalized).__name__,
                retryable=isinstance(normalized, TransientExecutionError),
            ),
        )

    def fetch_one_with_retry(
        self,
        compiled_query: CompiledQuery,
        retry_policy: RetryPolicy | None = None,
    ) -> Sequence[Any] | None:
        policy = retry_policy or RetryPolicy()
        return run_with_retry(
            operation=lambda: self.fetch_one(compiled_query),
            normalize_error=lambda exc: self._normalize_execution_error(operation="fetch_one", exc=exc),
            policy=policy,
            on_retry=lambda normalized, attempt, delay: self._emit_event(
                "retry.scheduled",
                success=False,
                operation="fetch_one",
                retry_attempt=attempt,
                max_attempts=policy.max_attempts,
                backoff_ms=delay * 1000,
                error_type=type(normalized).__name__,
                retryable=isinstance(normalized, TransientExecutionError),
            ),
            on_giveup=lambda normalized, attempt: self._emit_event(
                "retry.giveup",
                success=False,
                operation="fetch_one",
                retry_attempt=attempt,
                max_attempts=policy.max_attempts,
                error_type=type(normalized).__name__,
                retryable=isinstance(normalized, TransientExecutionError),
            ),
        )

    def execute_many_with_retry(
        self,
        sql: str,
        param_sets: Sequence[Sequence[Any]],
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        policy = retry_policy or RetryPolicy()
        run_with_retry(
            operation=lambda: self.execute_many(sql, param_sets),
            normalize_error=lambda exc: self._normalize_execution_error(operation="execute_many", exc=exc),
            policy=policy,
            on_retry=lambda normalized, attempt, delay: self._emit_event(
                "retry.scheduled",
                success=False,
                operation="execute_many",
                retry_attempt=attempt,
                max_attempts=policy.max_attempts,
                backoff_ms=delay * 1000,
                error_type=type(normalized).__name__,
                retryable=isinstance(normalized, TransientExecutionError),
            ),
            on_giveup=lambda normalized, attempt: self._emit_event(
                "retry.giveup",
                success=False,
                operation="execute_many",
                retry_attempt=attempt,
                max_attempts=policy.max_attempts,
                error_type=type(normalized).__name__,
                retryable=isinstance(normalized, TransientExecutionError),
            ),
        )

    # ==================================================
    # Lifecycle Controls
    # ==================================================

    def close(self) -> None:
        """
        Releases executor-owned resources.
        Subclasses should override when they hold lifecycle state.
        """
        return None

    def __enter__(self) -> "Executor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        _ = exc_type
        _ = exc
        _ = tb
        self.close()
