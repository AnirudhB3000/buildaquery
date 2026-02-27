from pathlib import Path
import uuid

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.observability import ObservabilitySettings, QueryObservation
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
