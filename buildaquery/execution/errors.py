from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ==================================================
# Normalized Execution Errors
# ==================================================


@dataclass(slots=True)
class ExecutionErrorDetails:
    """
    Structured metadata for normalized execution errors.
    """

    dialect: str
    operation: str
    sqlstate: str | None
    original_message: str


class ExecutionError(Exception):
    """
    Base normalized execution error type.
    """

    def __init__(self, details: ExecutionErrorDetails, original_exception: Exception) -> None:
        self.details = details
        self.original_exception = original_exception
        super().__init__(
            f"[{details.dialect}:{details.operation}] {self.__class__.__name__}: {details.original_message}"
        )


class TransientExecutionError(ExecutionError):
    """
    Base type for errors that are typically retryable.
    """


class DeadlockError(TransientExecutionError):
    pass


class SerializationError(TransientExecutionError):
    pass


class LockTimeoutError(TransientExecutionError):
    pass


class ConnectionTimeoutError(TransientExecutionError):
    pass


class IntegrityConstraintError(ExecutionError):
    pass


class ProgrammingExecutionError(ExecutionError):
    pass


def _extract_sqlstate(exc: Exception) -> str | None:
    sqlstate = getattr(exc, "sqlstate", None)
    if isinstance(sqlstate, str) and sqlstate:
        return sqlstate.upper()

    pgcode = getattr(exc, "pgcode", None)
    if isinstance(pgcode, str) and pgcode:
        return pgcode.upper()

    code = getattr(exc, "code", None)
    if isinstance(code, str) and len(code) == 5:
        return code.upper()

    return None


def normalize_execution_error(
    *,
    dialect: str,
    operation: str,
    exc: Exception,
) -> ExecutionError:
    """
    Maps driver exceptions to a normalized execution error taxonomy.
    """
    sqlstate = _extract_sqlstate(exc)
    message = str(exc).lower()
    details = ExecutionErrorDetails(
        dialect=dialect,
        operation=operation,
        sqlstate=sqlstate,
        original_message=str(exc),
    )

    deadlock_states = {"40P01", "1213"}
    serialization_states = {"40001"}
    lock_timeout_states = {"55P03", "57014", "1205"}
    integrity_prefixes = {"23"}
    programming_prefixes = {"42"}

    if sqlstate in deadlock_states or "deadlock" in message:
        return DeadlockError(details, exc)

    if sqlstate in serialization_states or "serialization failure" in message or "could not serialize" in message:
        return SerializationError(details, exc)

    if (
        sqlstate in lock_timeout_states
        or "lock wait timeout" in message
        or "database is locked" in message
        or "lock timeout" in message
    ):
        return LockTimeoutError(details, exc)

    if (
        "connection timed out" in message
        or "timed out" in message
        or "login timeout" in message
        or "could not connect" in message
        or "connection refused" in message
    ):
        return ConnectionTimeoutError(details, exc)

    if sqlstate and sqlstate[:2] in integrity_prefixes:
        return IntegrityConstraintError(details, exc)
    if "unique constraint" in message or "foreign key constraint" in message or "duplicate key" in message:
        return IntegrityConstraintError(details, exc)

    if sqlstate and sqlstate[:2] in programming_prefixes:
        return ProgrammingExecutionError(details, exc)
    if "syntax error" in message or "invalid identifier" in message or "unknown column" in message:
        return ProgrammingExecutionError(details, exc)

    return ExecutionError(details, exc)
