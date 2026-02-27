from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.observability import ObservabilitySettings, QueryObservation
from buildaquery.execution.sqlite import SqliteExecutor


def log_query(event: QueryObservation) -> None:
    print(
        f"[{event.dialect}] op={event.operation} success={event.succeeded} "
        f"duration_ms={event.duration_ms:.2f} params={event.param_count} metadata={dict(event.metadata)}"
    )


with SqliteExecutor(
    connection_info="static/test-sqlite/db.sqlite",
    observability_settings=ObservabilitySettings(
        query_observer=log_query,
        metadata={"service": "buildaquery-sample"},
    ),
) as executor:
    executor.execute_raw("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
    executor.execute(CompiledQuery(sql="INSERT INTO users (id, name) VALUES (?, ?)", params=[1, "Alice"]))
    rows = executor.fetch_all(CompiledQuery(sql="SELECT id, name FROM users ORDER BY id", params=[]))
    print(rows)
