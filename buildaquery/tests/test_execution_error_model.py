from buildaquery.execution.errors import (
    ConnectionTimeoutError,
    DeadlockError,
    ExecutionError,
    IntegrityConstraintError,
    LockTimeoutError,
    ProgrammingExecutionError,
    SerializationError,
    normalize_execution_error,
)


class _FakeDriverError(Exception):
    def __init__(self, message: str, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


def test_normalize_deadlock_by_sqlstate() -> None:
    err = normalize_execution_error(
        dialect="postgres",
        operation="execute",
        exc=_FakeDriverError("deadlock detected", "40P01"),
    )
    assert isinstance(err, DeadlockError)
    assert err.details.dialect == "postgres"
    assert err.details.operation == "execute"
    assert err.details.sql is None


def test_normalize_serialization_by_sqlstate() -> None:
    err = normalize_execution_error(
        dialect="cockroach",
        operation="execute",
        exc=_FakeDriverError("retry transaction", "40001"),
    )
    assert isinstance(err, SerializationError)


def test_normalize_lock_timeout_by_message() -> None:
    err = normalize_execution_error(
        dialect="sqlite",
        operation="execute",
        exc=_FakeDriverError("database is locked"),
    )
    assert isinstance(err, LockTimeoutError)


def test_normalize_connection_timeout_by_message() -> None:
    err = normalize_execution_error(
        dialect="mssql",
        operation="execute",
        exc=_FakeDriverError("login timeout expired"),
    )
    assert isinstance(err, ConnectionTimeoutError)


def test_normalize_integrity_error_by_sqlstate_class() -> None:
    err = normalize_execution_error(
        dialect="mysql",
        operation="execute",
        exc=_FakeDriverError("duplicate key", "23000"),
    )
    assert isinstance(err, IntegrityConstraintError)


def test_normalize_programming_error_by_sqlstate_class() -> None:
    err = normalize_execution_error(
        dialect="oracle",
        operation="execute",
        exc=_FakeDriverError("syntax error", "42000"),
    )
    assert isinstance(err, ProgrammingExecutionError)


def test_normalize_generic_execution_error_fallback() -> None:
    err = normalize_execution_error(
        dialect="postgres",
        operation="execute",
        exc=_FakeDriverError("unknown failure"),
    )
    assert isinstance(err, ExecutionError)
    assert type(err) is ExecutionError


def test_normalized_error_message_includes_sqlstate_and_redacted_sql() -> None:
    err = normalize_execution_error(
        dialect="postgres",
        operation="fetch_all",
        exc=_FakeDriverError("syntax error at or near SELECT", "42601"),
        sql="SELECT id, email FROM users WHERE active = %s ORDER BY id",
    )

    assert "sqlstate=42601" in str(err)
    assert "sql='SELECT id, email FROM users WHERE active = %s ORDER BY id'" in str(err)
    assert err.details.sql == "SELECT id, email FROM users WHERE active = %s ORDER BY id"


def test_normalized_error_redacts_by_keeping_placeholders_not_hostile_values() -> None:
    hostile_value = "x'; DROP TABLE users; --"
    err = normalize_execution_error(
        dialect="sqlite",
        operation="execute",
        exc=_FakeDriverError("database is locked"),
        sql="SELECT id FROM users WHERE email = ?",
    )

    assert hostile_value not in str(err)
    assert "sql='SELECT id FROM users WHERE email = ?'" in str(err)


def test_normalized_error_truncates_long_sql_context() -> None:
    sql = "SELECT " + ", ".join([f"col_{idx}" for idx in range(40)]) + " FROM very_large_table WHERE active = %s"
    err = normalize_execution_error(
        dialect="postgres",
        operation="fetch_all",
        exc=_FakeDriverError("unknown failure"),
        sql=sql,
    )

    assert err.details.sql is not None
    assert len(err.details.sql) <= 163
    assert err.details.sql.endswith("...")
