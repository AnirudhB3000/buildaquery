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
- `begin(isolation_level=None)`: Starts an explicit transaction.
- `commit()`: Commits the active explicit transaction.
- `rollback()`: Rolls back the active explicit transaction.
- `savepoint(name)`: Creates a savepoint in the active transaction.
- `rollback_to_savepoint(name)`: Rolls back to a savepoint.
- `release_savepoint(name)`: Releases a savepoint when supported by the dialect.

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
