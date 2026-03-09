import sqlite3

import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.errors import ProgrammingExecutionError
from buildaquery.execution.observability import ExecutionEvent, ObservabilitySettings
from buildaquery.execution.sqlite import SqliteExecutor


def test_raw_sql_policy_invalid_value_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid raw_sql_policy"):
        SqliteExecutor(connection=sqlite3.connect(":memory:"), raw_sql_policy="bad-policy")  # type: ignore[arg-type]


def test_raw_sql_policy_deny_untrusted_blocks_by_default() -> None:
    conn = sqlite3.connect(":memory:")
    events: list[ExecutionEvent] = []
    executor = SqliteExecutor(
        connection=conn,
        raw_sql_policy="deny_untrusted",
        observability_settings=ObservabilitySettings(event_observer=events.append),
    )

    with pytest.raises(ProgrammingExecutionError, match="requires trusted=True"):
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")

    assert any(event.event == "security.execute_raw.blocked" for event in events)
    conn.close()


def test_raw_sql_policy_deny_untrusted_allows_trusted_execute_raw() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn, raw_sql_policy="deny_untrusted")
    executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)", trusted=True)
    rows = executor.fetch_all(
        CompiledQuery(sql="SELECT name FROM sqlite_master WHERE type='table' AND name='t'", params=[])
    )
    assert rows == [("t",)]
    conn.close()


def test_raw_sql_policy_deny_all_blocks_even_trusted() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn, raw_sql_policy="deny_all")
    with pytest.raises(ProgrammingExecutionError, match="disabled by raw_sql_policy='deny_all'"):
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)", trusted=True)
    conn.close()
