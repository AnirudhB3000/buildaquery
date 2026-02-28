from pathlib import Path
import uuid
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
)
from buildaquery.execution.retry import RetryPolicy
from buildaquery.execution.sqlite import SqliteExecutor


def test_sqlite_observability_end_to_end() -> None:
    db_path = Path("static/test-sqlite") / f"observability_integration_{uuid.uuid4().hex}.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    events: list[QueryObservation] = []

    try:
        with SqliteExecutor(
            connection_info=str(db_path),
            observability_settings=ObservabilitySettings(
                query_observer=events.append,
                metadata={"env": "integration"},
            ),
        ) as executor:
            executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            executor.execute_many(
                "INSERT INTO t (id, value) VALUES (?, ?)",
                [[1, "a"], [2, "b"]],
            )
            rows = executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t ORDER BY id", params=[]))

        assert rows == [(1, "a"), (2, "b")]
        assert [event.operation for event in events] == ["execute_raw", "execute_many", "fetch_all"]
        assert all(event.duration_ms >= 0 for event in events)
        assert all(event.metadata.get("env") == "integration" for event in events)
    finally:
        if db_path.exists():
            db_path.unlink()


def test_sqlite_lifecycle_events_end_to_end() -> None:
    db_path = Path("static/test-sqlite") / f"observability_lifecycle_{uuid.uuid4().hex}.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    events: list[ExecutionEvent] = []

    try:
        with SqliteExecutor(
            connection_info=str(db_path),
            observability_settings=ObservabilitySettings(
                event_observer=events.append,
                metadata={"env": "integration"},
            ),
        ) as executor:
            executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            executor.begin()
            executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "x"]))
            executor.savepoint("sp1")
            executor.rollback_to_savepoint("sp1")
            executor.release_savepoint("sp1")
            executor.commit()

        event_names = [event.event for event in events]
        assert "query.start" in event_names
        assert "query.end" in event_names
        assert "txn.begin" in event_names
        assert "txn.commit" in event_names
        assert "txn.savepoint.create" in event_names
        assert "txn.savepoint.rollback" in event_names
        assert "txn.savepoint.release" in event_names
        assert "connection.close" in event_names

        tx_begin = [event for event in events if event.event == "txn.begin"][-1]
        tx_commit = [event for event in events if event.event == "txn.commit"][-1]
        assert tx_begin.transaction_id is not None
        assert tx_commit.transaction_id == tx_begin.transaction_id
        assert tx_commit.duration_ms is not None
        assert all(event.metadata.get("env") == "integration" for event in events)
    finally:
        if db_path.exists():
            db_path.unlink()


def test_sqlite_metrics_and_tracing_adapters_end_to_end() -> None:
    db_path = Path("static/test-sqlite") / f"observability_metrics_tracing_{uuid.uuid4().hex}.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    metrics = InMemoryMetricsAdapter()
    tracing = InMemoryTracingAdapter()

    try:
        with SqliteExecutor(
            connection_info=str(db_path),
            observability_settings=ObservabilitySettings(
                event_observer=compose_event_observers(metrics, tracing),
                metadata={"env": "integration"},
            ),
        ) as executor:
            executor.execute_raw("CREATE TABLE u (id INTEGER PRIMARY KEY, value TEXT UNIQUE)")
            executor.begin()
            executor.execute(CompiledQuery(sql="INSERT INTO u (id, value) VALUES (?, ?)", params=[1, "dup"]))
            executor.savepoint("sp1")
            executor.rollback_to_savepoint("sp1")
            executor.release_savepoint("sp1")
            executor.commit()

            with pytest.raises(IntegrityConstraintError):
                executor.execute_with_retry(
                    CompiledQuery(sql="INSERT INTO u (id, value) VALUES (?, ?)", params=[2, "dup"]),
                    retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0.0),
                )

        query_labels = {
            "dialect": "sqlite",
            "executor": "SqliteExecutor",
            "operation": "execute",
            "event": "query.end",
            "error_type": "none",
        }
        assert metrics.counter_value("buildaquery_queries_total", query_labels) >= 1
        assert len(metrics.histogram_values("buildaquery_query_duration_ms", query_labels)) >= 1

        retry_labels = {
            "dialect": "sqlite",
            "executor": "SqliteExecutor",
            "operation": "execute",
            "event": "retry.giveup",
            "error_type": "IntegrityConstraintError",
        }
        assert metrics.counter_value("buildaquery_retry_giveups_total", retry_labels) == 1

        span_names = [span.name for span in tracing.completed_spans]
        assert "db.query" in span_names
        assert "db.transaction" in span_names
        assert any(span.attributes.get("buildaquery.outcome") == "txn.commit" for span in tracing.completed_spans)
    finally:
        if db_path.exists():
            db_path.unlink()
