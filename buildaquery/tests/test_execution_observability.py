import sqlite3
import json
import logging
from pathlib import Path
from uuid import uuid4

import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.errors import IntegrityConstraintError
from buildaquery.execution.observability import (
    ExecutionEvent,
    InMemoryMetricsAdapter,
    InMemoryTracingAdapter,
    ObservabilitySettings,
    QueryObservation,
    compose_event_observers,
    execution_event_to_dict,
    make_json_event_logger,
)
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


def test_event_observer_emits_query_start_and_end() -> None:
    db_path = _test_db_path("observe_query_events")
    lifecycle_events: list[ExecutionEvent] = []

    try:
        executor = SqliteExecutor(
            connection_info=str(db_path),
            observability_settings=ObservabilitySettings(event_observer=lifecycle_events.append),
        )
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
        executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "x"]))

        names = [event.event for event in lifecycle_events]
        assert "query.start" in names
        assert "query.end" in names

        query_end = [event for event in lifecycle_events if event.event == "query.end"][-1]
        assert query_end.operation == "execute"
        assert query_end.duration_ms is not None
        assert query_end.success is True
        assert query_end.query_id is not None
    finally:
        if db_path.exists():
            db_path.unlink()


def test_event_observer_emits_transaction_and_savepoint_events() -> None:
    db_path = _test_db_path("observe_txn_events")
    lifecycle_events: list[ExecutionEvent] = []

    try:
        executor = SqliteExecutor(
            connection_info=str(db_path),
            observability_settings=ObservabilitySettings(event_observer=lifecycle_events.append),
        )
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
        executor.begin()
        executor.savepoint("sp1")
        executor.rollback_to_savepoint("sp1")
        executor.release_savepoint("sp1")
        executor.commit()

        names = [event.event for event in lifecycle_events]
        assert "txn.begin" in names
        assert "txn.savepoint.create" in names
        assert "txn.savepoint.rollback" in names
        assert "txn.savepoint.release" in names
        assert "txn.commit" in names

        begin_event = [event for event in lifecycle_events if event.event == "txn.begin"][-1]
        commit_event = [event for event in lifecycle_events if event.event == "txn.commit"][-1]
        assert begin_event.transaction_id is not None
        assert commit_event.transaction_id == begin_event.transaction_id
        assert commit_event.duration_ms is not None
    finally:
        if db_path.exists():
            db_path.unlink()


def test_event_observer_emits_retry_scheduled_and_giveup() -> None:
    db_path = _test_db_path("observe_retry_events")
    lifecycle_events: list[ExecutionEvent] = []

    try:
        executor = SqliteExecutor(
            connection_info=str(db_path),
            observability_settings=ObservabilitySettings(event_observer=lifecycle_events.append),
        )
        executor.execute_raw("CREATE TABLE u (id INTEGER PRIMARY KEY, value TEXT UNIQUE)")
        executor.execute(CompiledQuery(sql="INSERT INTO u (id, value) VALUES (?, ?)", params=[1, "dup"]))

        with pytest.raises(IntegrityConstraintError):
            executor.execute_with_retry(
                CompiledQuery(sql="INSERT INTO u (id, value) VALUES (?, ?)", params=[2, "dup"]),
                retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0.0),
            )

        names = [event.event for event in lifecycle_events]
        assert "retry.giveup" in names
        assert "retry.scheduled" not in names

        giveup = [event for event in lifecycle_events if event.event == "retry.giveup"][-1]
        assert giveup.operation == "execute"
        assert giveup.retry_attempt == 1
        assert giveup.retryable is False
    finally:
        if db_path.exists():
            db_path.unlink()


def test_execution_event_to_dict_shapes_payload() -> None:
    event = ExecutionEvent(
        timestamp="2026-02-28T00:00:00+00:00",
        event="query.end",
        dialect="sqlite",
        executor="SqliteExecutor",
        success=True,
        metadata={"service": "unit"},
        operation="execute",
        query_id="q1",
        duration_ms=1.2,
    )

    payload = execution_event_to_dict(event)
    assert payload["event"] == "query.end"
    assert payload["dialect"] == "sqlite"
    assert payload["metadata"]["service"] == "unit"
    assert payload["duration_ms"] == 1.2


def test_make_json_event_logger_emits_json_line(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("buildaquery.observability.test")
    event_logger = make_json_event_logger(logger=logger, level=logging.INFO)
    event = ExecutionEvent(
        timestamp="2026-02-28T00:00:00+00:00",
        event="txn.commit",
        dialect="sqlite",
        executor="SqliteExecutor",
        success=True,
        metadata={"service": "unit"},
        transaction_id="tx1",
        duration_ms=3.4,
    )

    with caplog.at_level(logging.INFO, logger=logger.name):
        event_logger(event)

    assert len(caplog.records) == 1
    message = caplog.records[0].message
    decoded = json.loads(message)
    assert decoded["event"] == "txn.commit"
    assert decoded["transaction_id"] == "tx1"
    assert decoded["metadata"]["service"] == "unit"


def test_in_memory_metrics_adapter_records_counters_and_histograms() -> None:
    metrics = InMemoryMetricsAdapter()
    labels = {
        "dialect": "sqlite",
        "executor": "SqliteExecutor",
        "operation": "execute",
        "event": "query.end",
        "error_type": "none",
    }
    metrics(
        ExecutionEvent(
            timestamp="2026-02-28T00:00:00+00:00",
            event="query.end",
            dialect="sqlite",
            executor="SqliteExecutor",
            success=True,
            metadata={},
            operation="execute",
            query_id="q1",
            duration_ms=5.0,
        )
    )
    metrics(
        ExecutionEvent(
            timestamp="2026-02-28T00:00:01+00:00",
            event="retry.giveup",
            dialect="sqlite",
            executor="SqliteExecutor",
            success=False,
            metadata={},
            operation="execute",
            error_type="IntegrityConstraintError",
            retryable=False,
            retry_attempt=1,
            max_attempts=2,
        )
    )

    assert metrics.counter_value("buildaquery_queries_total", labels) == 1
    assert metrics.histogram_values("buildaquery_query_duration_ms", labels) == [5.0]
    retry_labels = {
        "dialect": "sqlite",
        "executor": "SqliteExecutor",
        "operation": "execute",
        "event": "retry.giveup",
        "error_type": "IntegrityConstraintError",
    }
    assert metrics.counter_value("buildaquery_retry_giveups_total", retry_labels) == 1


def test_in_memory_tracing_adapter_builds_query_and_transaction_spans() -> None:
    tracing = InMemoryTracingAdapter()
    tracing(
        ExecutionEvent(
            timestamp="2026-02-28T00:00:00+00:00",
            event="query.start",
            dialect="sqlite",
            executor="SqliteExecutor",
            success=True,
            metadata={},
            operation="fetch_all",
            query_id="q1",
        )
    )
    tracing(
        ExecutionEvent(
            timestamp="2026-02-28T00:00:01+00:00",
            event="query.end",
            dialect="sqlite",
            executor="SqliteExecutor",
            success=True,
            metadata={},
            operation="fetch_all",
            query_id="q1",
            duration_ms=1.5,
        )
    )
    tracing(
        ExecutionEvent(
            timestamp="2026-02-28T00:00:02+00:00",
            event="txn.begin",
            dialect="sqlite",
            executor="SqliteExecutor",
            success=True,
            metadata={},
            transaction_id="tx1",
        )
    )
    tracing(
        ExecutionEvent(
            timestamp="2026-02-28T00:00:03+00:00",
            event="txn.commit",
            dialect="sqlite",
            executor="SqliteExecutor",
            success=True,
            metadata={},
            transaction_id="tx1",
            duration_ms=2.0,
        )
    )

    assert len(tracing.completed_spans) == 2
    query_span = [span for span in tracing.completed_spans if span.name == "db.query"][0]
    tx_span = [span for span in tracing.completed_spans if span.name == "db.transaction"][0]
    assert query_span.attributes["buildaquery.query_id"] == "q1"
    assert query_span.attributes["buildaquery.duration_ms"] == 1.5
    assert tx_span.attributes["buildaquery.transaction_id"] == "tx1"
    assert tx_span.attributes["buildaquery.outcome"] == "txn.commit"


def test_compose_event_observers_dispatches_to_all() -> None:
    sink1: list[str] = []
    sink2: list[str] = []
    observer = compose_event_observers(
        lambda event: sink1.append(event.event),
        lambda event: sink2.append(event.event),
    )
    observer(
        ExecutionEvent(
            timestamp="2026-02-28T00:00:00+00:00",
            event="query.start",
            dialect="sqlite",
            executor="SqliteExecutor",
            success=True,
            metadata={},
            operation="execute",
            query_id="q1",
        )
    )
    assert sink1 == ["query.start"]
    assert sink2 == ["query.start"]
