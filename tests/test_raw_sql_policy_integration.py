from pathlib import Path
import uuid

import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.errors import ProgrammingExecutionError
from buildaquery.execution.observability import ExecutionEvent, ObservabilitySettings
from buildaquery.execution.sqlite import SqliteExecutor


def test_sqlite_execute_raw_policy_blocks_untrusted_and_emits_event() -> None:
    db_path = Path("static/test-sqlite") / f"raw_sql_policy_{uuid.uuid4().hex}.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    events: list[ExecutionEvent] = []

    try:
        with SqliteExecutor(
            connection_info=str(db_path),
            raw_sql_policy="deny_untrusted",
            observability_settings=ObservabilitySettings(event_observer=events.append),
        ) as executor:
            with pytest.raises(ProgrammingExecutionError, match="requires trusted=True"):
                executor.execute_raw("CREATE TABLE guarded (id INTEGER PRIMARY KEY)")

            executor.execute_raw("CREATE TABLE guarded (id INTEGER PRIMARY KEY)", trusted=True)
            rows = executor.fetch_all(
                CompiledQuery(
                    sql="SELECT name FROM sqlite_master WHERE type='table' AND name='guarded'",
                    params=[],
                )
            )

        assert rows == [("guarded",)]
        blocked_events = [event for event in events if event.event == "security.execute_raw.blocked"]
        assert len(blocked_events) == 1
        assert blocked_events[0].operation == "execute_raw"
        assert blocked_events[0].success is False
    finally:
        if db_path.exists():
            db_path.unlink()
