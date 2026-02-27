import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.errors import IntegrityConstraintError
from buildaquery.execution.observability import ObservabilitySettings, QueryObservation
from buildaquery.execution.retry import RetryPolicy
from buildaquery.execution.sqlite import SqliteExecutor


def _test_db_path(prefix: str) -> Path:
    base = Path("static") / "test-sqlite"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{prefix}_{uuid4().hex}.sqlite"


def test_sqlite_observability_emits_success_event() -> None:
    db_path = _test_db_path("observe_success")
    events: list[QueryObservation] = []

    def observer(event: QueryObservation) -> None:
        events.append(event)

    try:
        executor = SqliteExecutor(
            connection_info=str(db_path),
            observability_settings=ObservabilitySettings(
                query_observer=observer,
                metadata={"service": "unit-test"},
            ),
        )
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
        executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "x"]))

        assert len(events) == 2
        insert_event = events[1]
        assert insert_event.operation == "execute"
        assert insert_event.sql == "INSERT INTO t (id, value) VALUES (?, ?)"
        assert insert_event.param_count == 2
        assert insert_event.succeeded is True
        assert insert_event.duration_ms >= 0
        assert insert_event.metadata["service"] == "unit-test"
        assert insert_event.error_type is None
    finally:
        if db_path.exists():
            db_path.unlink()


def test_sqlite_observability_emits_failure_event() -> None:
    db_path = _test_db_path("observe_failure")
    events: list[QueryObservation] = []

    try:
        executor = SqliteExecutor(
            connection_info=str(db_path),
            observability_settings=ObservabilitySettings(query_observer=events.append),
        )
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")

        with pytest.raises(sqlite3.OperationalError):
            executor.execute(CompiledQuery(sql="INSERT INTO missing (id) VALUES (?)", params=[1]))

        assert len(events) == 2
        failed = events[1]
        assert failed.operation == "execute"
        assert failed.succeeded is False
        assert failed.error_type == "OperationalError"
        assert failed.error_message is not None
    finally:
        if db_path.exists():
            db_path.unlink()


def test_retry_path_emits_event_per_attempt() -> None:
    db_path = _test_db_path("observe_retry")
    events: list[QueryObservation] = []
    conn: sqlite3.Connection | None = None

    try:
        conn = sqlite3.connect(str(db_path), timeout=0.1)
        executor = SqliteExecutor(
            connection=conn,
            observability_settings=ObservabilitySettings(query_observer=events.append),
        )
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
        executor.execute_raw("CREATE TABLE u (id INTEGER PRIMARY KEY, value TEXT UNIQUE)")
        executor.execute(CompiledQuery(sql="INSERT INTO u (id, value) VALUES (?, ?)", params=[1, "dup"]))

        with pytest.raises(IntegrityConstraintError):
            executor.execute_with_retry(
                CompiledQuery(sql="INSERT INTO u (id, value) VALUES (?, ?)", params=[2, "dup"]),
                retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0.0),
            )

        failure_events = [event for event in events if event.operation == "execute" and event.succeeded is False]
        assert len(failure_events) >= 1
        assert failure_events[-1].error_type == "IntegrityError"
    finally:
        if conn is not None:
            conn.close()
        if db_path.exists():
            db_path.unlink()
