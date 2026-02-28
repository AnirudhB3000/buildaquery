# Execution

The `execution` module is responsible for taking compiled queries and executing them against a real database. It provides an abstraction layer over database drivers, allowing the rest of the application to remain agnostic of the specific library used for database communication.

## Core Concepts

### `Executor` Base Class
An abstract base class that defines the interface for all database executors. 

#### Methods:
- `execute(compiled_query)`: Runs a query without expecting a return value.
- `fetch_all(compiled_query)`: Returns all rows matching the query.
- `fetch_one(compiled_query)`: Returns the first row matching the query.
- `execute_many(sql, param_sets)`: Runs one SQL statement against multiple parameter sets (bulk write path).
- `execute_with_retry(compiled_query, retry_policy=None)`: Executes with transient-failure retries and normalized errors.
- `fetch_all_with_retry(compiled_query, retry_policy=None)`: Fetches all rows with transient-failure retries and normalized errors.
- `fetch_one_with_retry(compiled_query, retry_policy=None)`: Fetches one row with transient-failure retries and normalized errors.
- `execute_many_with_retry(sql, param_sets, retry_policy=None)`: Bulk execution with transient-failure retries and normalized errors.
- `close()`: Releases executor resources and rolls back any still-open explicit transaction.
- `begin(isolation_level=None)`: Starts an explicit transaction.
- `commit()`: Commits the active explicit transaction.
- `rollback()`: Rolls back the active explicit transaction.
- `savepoint(name)`: Creates a savepoint in the active transaction.
- `rollback_to_savepoint(name)`: Rolls back to a savepoint.
- `release_savepoint(name)`: Releases a savepoint when supported by the dialect.

Executors also support context manager lifecycle control:
- `with PostgresExecutor(...) as executor: ...` (calls `close()` on exit).

### Connection Management

All executors support production-oriented connection controls:
- `connect_timeout_seconds`: dialect-aware connect timeout.
- `acquire_connection`: hook for external connection pool checkout.
- `release_connection`: hook for external connection pool checkin.

If `acquire_connection` is provided, executor operations use pooled connections and return them with `release_connection` (or `close()` when no release hook is provided).

### Observability Hooks

All executors support observability through `ObservabilitySettings`:
- `query_observer`: callback receiving a structured `QueryObservation` event (query timing payload).
- `event_observer`: callback receiving a structured `ExecutionEvent` payload (lifecycle logging events).
- `metadata`: static, tracing-safe key/value metadata attached to each emitted payload.

Built-in event adapters:
- `make_json_event_logger(logger=...)`: emits one JSON log line per event.
- `InMemoryMetricsAdapter()`: aggregates counters/histograms from lifecycle events.
- `InMemoryTracingAdapter()`: builds in-memory query/transaction spans.
- `compose_event_observers(...)`: fans out each event to multiple adapters.

Lifecycle event names:
- `query.start`, `query.end`
- `retry.scheduled`, `retry.giveup`
- `txn.begin`, `txn.commit`, `txn.rollback`
- `txn.savepoint.create`, `txn.savepoint.rollback`, `txn.savepoint.release`
- `connection.acquire.start`, `connection.acquire.end`, `connection.release`, `connection.close`

### Normalized Error Types

Retry-enabled APIs normalize backend-specific exceptions into common types:
- `DeadlockError`
- `SerializationError`
- `LockTimeoutError`
- `ConnectionTimeoutError`
- `IntegrityConstraintError`
- `ProgrammingExecutionError`

### `PostgresExecutor`
A concrete implementation for PostgreSQL using the `psycopg` library. It handles connection management and query parametrization automatically.

### `SqliteExecutor`
A concrete implementation for SQLite using Python's standard library `sqlite3` module.

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

### `MySqlExecutor`
A concrete implementation for MySQL using `mysql-connector-python`.

### `MariaDbExecutor`
A concrete implementation for MariaDB using `mariadb`.

### `CockroachExecutor`
A concrete implementation for CockroachDB using `psycopg`.

### `OracleExecutor`
A concrete implementation for Oracle using `oracledb`.

## Usage Example

```python
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.retry import RetryPolicy
from buildaquery.execution.errors import TransientExecutionError
from buildaquery.execution.observability import (
    InMemoryMetricsAdapter,
    InMemoryTracingAdapter,
    ObservabilitySettings,
    QueryObservation,
    compose_event_observers,
    make_json_event_logger,
)
import logging

# 1. Prepare the query (usually from a compiler)
compiled = CompiledQuery(
    sql="SELECT * FROM users WHERE age > %s",
    params=[25]
)

# 2. Initialize the executor
# Can take a connection string or a dictionary of params
executor = PostgresExecutor("dbname=test user=postgres password=secret")

# 3. Execute and fetch results
rows = executor.fetch_all(compiled)
for row in rows:
    print(row)

# 4. Explicit transaction control
executor.begin()
executor.execute(CompiledQuery(sql="INSERT INTO users(name) VALUES (%s)", params=["Alice"]))
executor.savepoint("after_alice")
executor.execute(CompiledQuery(sql="INSERT INTO users(name) VALUES (%s)", params=["Bob"]))
executor.rollback_to_savepoint("after_alice")
executor.commit()

# 5. Driver-level bulk writes
executor.execute_many(
    "INSERT INTO users(name) VALUES (%s)",
    [["Carol"], ["Dave"]],
)

# 6. Retry transient failures with normalized errors
retry_policy = RetryPolicy(max_attempts=3, base_delay_seconds=0.05)
try:
    rows = executor.fetch_all_with_retry(compiled, retry_policy=retry_policy)
except TransientExecutionError:
    # handle exhausted transient retries
    raise

# 7. Connection lifecycle + pool hooks
with PostgresExecutor(
    connection_info="dbname=test user=postgres password=secret",
    connect_timeout_seconds=5,
) as managed_executor:
    managed_executor.execute(compiled)


def on_query(event: QueryObservation) -> None:
    print(event.operation, event.duration_ms, event.succeeded, dict(event.metadata))


# 8. Observability hook wiring
events_logger = logging.getLogger("buildaquery.events")
events_logger.setLevel(logging.INFO)
events_logger.addHandler(logging.StreamHandler())

observed_executor = PostgresExecutor(
    connection_info="dbname=test user=postgres password=secret",
    observability_settings=ObservabilitySettings(
        query_observer=on_query,
        event_observer=compose_event_observers(
            make_json_event_logger(logger=events_logger),
            InMemoryMetricsAdapter(),
            InMemoryTracingAdapter(),
        ),
        metadata={"service": "api"},
    ),
)
observed_executor.fetch_all(compiled)
```

### Oracle Example

```python
from buildaquery.execution.oracle import OracleExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

compiled = CompiledQuery(
    sql="SELECT * FROM users WHERE age > :1",
    params=[25]
)

executor = OracleExecutor(connection_info="oracle://user:password@localhost:1521/XEPDB1")
rows = executor.fetch_all(compiled)
```

### SQL Server Example

```python
from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

compiled = CompiledQuery(
    sql="SELECT * FROM users WHERE age > ?",
    params=[25]
)

executor = MsSqlExecutor(connection_info="mssql://user:password@localhost:1433/dbname?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes")
rows = executor.fetch_all(compiled)
```

### MariaDB Example

```python
from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

compiled = CompiledQuery(
    sql="SELECT * FROM users WHERE age > ?",
    params=[25]
)

executor = MariaDbExecutor(connection_info="mariadb://user:password@localhost:3306/dbname")
rows = executor.fetch_all(compiled)
```

### CockroachDB Example

```python
from buildaquery.execution.cockroachdb import CockroachExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

compiled = CompiledQuery(
    sql="SELECT * FROM users WHERE age > %s",
    params=[25]
)

executor = CockroachExecutor(connection_info="postgresql://root@localhost:26257/dbname?sslmode=disable")
rows = executor.fetch_all(compiled)
```

## Dependencies
Different executors require specific database drivers:
- `PostgresExecutor` requires `psycopg` (`pip install psycopg[binary]`).
- `SqliteExecutor` uses the standard library `sqlite3` module (no external dependency).
- `MySqlExecutor` requires `mysql-connector-python` (`pip install mysql-connector-python`).
- `OracleExecutor` requires `oracledb` (`pip install oracledb`).
- `MsSqlExecutor` requires `pyodbc` (`pip install pyodbc`).
- `MariaDbExecutor` requires `mariadb` (`pip install mariadb`).
- `CockroachExecutor` requires `psycopg` (`pip install psycopg[binary]`).
