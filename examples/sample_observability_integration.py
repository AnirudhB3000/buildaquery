import logging

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.observability import (
    InMemoryMetricsAdapter,
    InMemoryTracingAdapter,
    ObservabilitySettings,
    compose_event_observers,
    make_json_event_logger,
)
from buildaquery.execution.sqlite import SqliteExecutor

# ==================================================
# App-Level Observability Wiring
# ==================================================
# This example shows consumer-side setup only.
# No changes to buildaquery source code are required.

events_logger = logging.getLogger("buildaquery.events")
events_logger.setLevel(logging.INFO)
events_logger.addHandler(logging.StreamHandler())

metrics = InMemoryMetricsAdapter()
tracing = InMemoryTracingAdapter()

with SqliteExecutor(
    connection_info="static/test-sqlite/db.sqlite",
    observability_settings=ObservabilitySettings(
        event_observer=compose_event_observers(
            make_json_event_logger(logger=events_logger),
            metrics,
            tracing,
        ),
        metadata={"service": "example-app", "env": "local"},
    ),
) as executor:
    executor.execute_raw("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
    executor.execute(CompiledQuery(sql="INSERT INTO users (id, name) VALUES (?, ?)", params=[1, "Alice"]))
    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, name FROM users ORDER BY id", params=[]))
    print("rows:", rows)

labels = {
    "dialect": "sqlite",
    "executor": "SqliteExecutor",
    "operation": "fetch_all",
    "event": "query.end",
    "error_type": "none",
}

print("query_total(fetch_all):", metrics.counter_value("buildaquery_queries_total", labels))
print("spans:", [span.name for span in tracing.completed_spans])
