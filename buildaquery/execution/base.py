from abc import ABC, abstractmethod
from datetime import datetime, timezone
import time
from typing import Any, Literal, Mapping, Sequence
from uuid import uuid4
from buildaquery.abstract_syntax_tree.models import ASTNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.capabilities import ExecutorCapabilities
from buildaquery.execution.errors import (
    ExecutionError,
    ExecutionErrorDetails,
    ProgrammingExecutionError,
    TransientExecutionError,
    normalize_execution_error,
)
from buildaquery.execution.observability import ExecutionEvent, ObservabilitySettings, QueryObservation
from buildaquery.execution.retry import RetryPolicy, run_with_retry

RawSqlPolicy = Literal["allow", "deny_untrusted", "deny_all"]
RowOutput = Literal["tuple", "dict", "model"]


class _TransactionContext:
    """
    Context manager for explicit executor-managed transactions.
    """

    def __init__(self, executor: "Executor", isolation_level: str | None = None) -> None:
        self._executor = executor
        self._isolation_level = isolation_level

    def __enter__(self) -> "Executor":
        self._executor.begin(self._isolation_level)
        return self._executor

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        _ = exc_type
        _ = exc
        _ = tb
        if exc_type is None:
            self._executor.commit()
            return False
        self._executor.rollback()
        return False

# ==================================================
# Base Executor
# ==================================================

class Executor(ABC):
    """
    Abstract base class for executing compiled queries against a database.
    """

    CAPABILITIES = ExecutorCapabilities(
        transactions=True,
        savepoints=True,
        upsert=False,
        insert_returning=False,
        update_returning=False,
        delete_returning=False,
        select_for_update=False,
        select_for_share=False,
        lock_nowait=False,
        lock_skip_locked=False,
    )

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

    def to_sql(self, query: CompiledQuery | ASTNode) -> CompiledQuery:
        """
        Returns the compiled placeholder SQL and params without executing it.
        """
        if isinstance(query, CompiledQuery):
            return self._normalize_compiled_query(query)

        compiler = getattr(self, "compiler", None)
        if compiler is None or not hasattr(compiler, "compile"):
            raise RuntimeError("Executor does not expose a compiler for to_sql().")
        return compiler.compile(query)

    def capabilities(self) -> ExecutorCapabilities:
        """
        Returns the executor's explicit dialect capability contract.
        """
        return self.CAPABILITIES

    def _dialect_placeholder(self, index: int, name: str) -> str:
        _ = name
        dialect = self._dialect_name()
        if dialect == "oracle":
            return f":{index}"
        if dialect in {"postgres", "mysql", "cockroachdb"}:
            return "%s"
        return "?"

    def _rewrite_named_params(
        self,
        sql: str,
        params: Mapping[str, Any],
    ) -> tuple[str, list[Any]]:
        rewritten: list[str] = []
        ordered_params: list[Any] = []
        length = len(sql)
        index = 0
        placeholder_index = 1

        while index < length:
            char = sql[index]

            if char == "'" or char == '"':
                quote = char
                rewritten.append(char)
                index += 1
                while index < length:
                    current = sql[index]
                    rewritten.append(current)
                    index += 1
                    if current == quote:
                        if quote == "'" and index < length and sql[index] == "'":
                            rewritten.append(sql[index])
                            index += 1
                            continue
                        break
                continue

            if char == "-" and index + 1 < length and sql[index + 1] == "-":
                newline = sql.find("\n", index + 2)
                if newline == -1:
                    rewritten.append(sql[index:])
                    break
                rewritten.append(sql[index:newline])
                index = newline
                continue

            if char == "/" and index + 1 < length and sql[index + 1] == "*":
                comment_end = sql.find("*/", index + 2)
                if comment_end == -1:
                    rewritten.append(sql[index:])
                    break
                comment_end += 2
                rewritten.append(sql[index:comment_end])
                index = comment_end
                continue

            if char == ":":
                next_char = sql[index + 1] if index + 1 < length else ""
                if next_char == ":" or next_char == "=":
                    rewritten.append(char)
                    rewritten.append(next_char)
                    index += 2
                    continue
                if next_char.isdigit():
                    rewritten.append(char)
                    index += 1
                    continue
                if next_char.isalpha() or next_char == "_":
                    end = index + 2
                    while end < length and (sql[end].isalnum() or sql[end] == "_"):
                        end += 1
                    name = sql[index + 1 : end]
                    if name not in params:
                        raise ValueError(f"Missing named SQL parameter: {name}")
                    rewritten.append(self._dialect_placeholder(placeholder_index, name))
                    ordered_params.append(params[name])
                    placeholder_index += 1
                    index = end
                    continue

            rewritten.append(char)
            index += 1

        return "".join(rewritten), ordered_params

    def _normalize_sql_params(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None,
    ) -> tuple[str, Sequence[Any] | None]:
        if params is None:
            return sql, None
        if isinstance(params, Mapping):
            return self._rewrite_named_params(sql, params)
        return sql, params

    def _normalize_compiled_query(self, query: CompiledQuery) -> CompiledQuery:
        sql, params = self._normalize_sql_params(query.sql, query.params)
        return CompiledQuery(sql=sql, params=[] if params is None else params)

    def _validate_row_output(self, row_output: str, row_model: type[Any] | None) -> RowOutput:
        allowed: tuple[str, ...] = ("tuple", "dict", "model")
        if row_output not in allowed:
            raise ValueError(f"Invalid row_output {row_output!r}. Expected one of: {', '.join(allowed)}")
        if row_output == "model" and row_model is None:
            raise ValueError("row_model is required when row_output='model'.")
        return row_output

    def _column_names_from_description(self, description: Any) -> list[str]:
        if not description:
            return []
        names: list[str] = []
        for column in description:
            if isinstance(column, (list, tuple)) and column:
                names.append(str(column[0]))
            else:
                names.append(str(getattr(column, "name", column)))
        return names

    def _shape_row(self, row: Sequence[Any], column_names: Sequence[str]) -> Any:
        row_output = getattr(self, "row_output", "tuple")
        if row_output == "tuple":
            return tuple(row)

        payload = dict(zip(column_names, row))
        if row_output == "dict":
            return payload

        row_model = getattr(self, "row_model", None)
        if row_model is None:
            raise RuntimeError("row_model must be configured when row_output='model'.")
        return row_model(**payload)

    def _shape_rows(self, rows: Sequence[Sequence[Any]], description: Any) -> list[Any]:
        column_names = self._column_names_from_description(description)
        if not column_names or getattr(self, "row_output", "tuple") == "tuple":
            return [tuple(row) for row in rows]
        return [self._shape_row(row, column_names) for row in rows]

    def _shape_single_row(self, row: Sequence[Any] | None, description: Any) -> Any:
        if row is None:
            return None
        column_names = self._column_names_from_description(description)
        if not column_names or getattr(self, "row_output", "tuple") == "tuple":
            return tuple(row)
        return self._shape_row(row, column_names)

    def transaction(self, isolation_level: str | None = None) -> _TransactionContext:
        """
        Returns a context manager that commits on success and rolls back on error.
        """
        return _TransactionContext(self, isolation_level)

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

    def _validate_raw_sql_policy(self, raw_sql_policy: str) -> RawSqlPolicy:
        allowed: tuple[str, ...] = ("allow", "deny_untrusted", "deny_all")
        if raw_sql_policy not in allowed:
            raise ValueError(f"Invalid raw_sql_policy {raw_sql_policy!r}. Expected one of: {', '.join(allowed)}")
        return raw_sql_policy

    def _enforce_execute_raw_policy(self, *, sql: str, trusted: bool) -> None:
        _ = sql
        policy = self._validate_raw_sql_policy(getattr(self, "raw_sql_policy", "allow"))
        if policy == "allow":
            return

        blocked = policy == "deny_all" or (policy == "deny_untrusted" and not trusted)
        if not blocked:
            return

        reason = (
            "execute_raw is disabled by raw_sql_policy='deny_all'"
            if policy == "deny_all"
            else "execute_raw requires trusted=True when raw_sql_policy='deny_untrusted'"
        )
        self._emit_event(
            "security.execute_raw.blocked",
            success=False,
            operation="execute_raw",
            error_type="ProgrammingExecutionError",
            error_message=reason,
        )
        details = ExecutionErrorDetails(
            dialect=self._dialect_name(),
            operation="execute_raw",
            sqlstate=None,
            sql=sql,
            original_message=reason,
        )
        raise ProgrammingExecutionError(details, ValueError(reason))

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

    def _normalize_execution_error(
        self,
        *,
        operation: str,
        exc: Exception,
        sql: str | None = None,
    ) -> ExecutionError:
        return normalize_execution_error(
            dialect=self._dialect_name(),
            operation=operation,
            exc=exc,
            sql=sql,
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
            normalize_error=lambda exc: self._normalize_execution_error(
                operation="execute",
                exc=exc,
                sql=compiled_query.sql,
            ),
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
            normalize_error=lambda exc: self._normalize_execution_error(
                operation="fetch_all",
                exc=exc,
                sql=compiled_query.sql,
            ),
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
            normalize_error=lambda exc: self._normalize_execution_error(
                operation="fetch_one",
                exc=exc,
                sql=compiled_query.sql,
            ),
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
            normalize_error=lambda exc: self._normalize_execution_error(
                operation="execute_many",
                exc=exc,
                sql=sql,
            ),
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

