# Execution

The `execution` module is responsible for taking compiled queries and executing them against a real database. It provides an abstraction layer over database drivers, allowing the rest of the application to remain agnostic of the specific library used for database communication.

## Core Concepts

### `Executor` Base Class
An abstract base class that defines the interface for all database executors. 

#### Methods:
- `execute(compiled_query)`: Runs a query without expecting a return value.
- `fetch_all(compiled_query)`: Returns all rows matching the query.
- `fetch_one(compiled_query)`: Returns the first row matching the query.

### `PostgresExecutor`
A concrete implementation for PostgreSQL using the `psycopg` library. It handles connection management and query parametrization automatically.

### `SqliteExecutor`
A concrete implementation for SQLite using Python's standard library `sqlite3` module.

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

### `MySqlExecutor`
A concrete implementation for MySQL using `mysql-connector-python`.

### `OracleExecutor`
A concrete implementation for Oracle using `oracledb`.

## Usage Example

```python
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

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

## Dependencies
Different executors require specific database drivers:
- `PostgresExecutor` requires `psycopg` (`pip install psycopg[binary]`).
- `SqliteExecutor` uses the standard library `sqlite3` module (no external dependency).
- `MySqlExecutor` requires `mysql-connector-python` (`pip install mysql-connector-python`).
- `OracleExecutor` requires `oracledb` (`pip install oracledb`).
