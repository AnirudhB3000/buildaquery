# Build-a-Query User Guide

This guide is a concise, user-focused reference for installing Build-a-Query, building queries, and running tests. For implementation details and developer internals, use the nested package `README.md` files in `buildaquery/`.

## What It Is

Build-a-Query is a Python query builder that uses an Abstract Syntax Tree (AST) to generate parameterized SQL safely across multiple databases, with compiler-side identifier validation for table/schema/column/alias names.

Supported dialects:
- PostgreSQL
- SQLite
- DuckDB
- ClickHouse
- MySQL
- MariaDB
- CockroachDB
- Oracle
- SQL Server

## Install

```bash
pip install buildaquery
```

Install optional database extras only for the backends you use:

```bash
pip install "buildaquery[postgres]"
pip install "buildaquery[cockroach]"
pip install "buildaquery[mysql]"
pip install "buildaquery[mariadb]"
pip install "buildaquery[oracle]"
pip install "buildaquery[mssql]"
pip install "buildaquery[duckdb]"
pip install "buildaquery[clickhouse]"
pip install "buildaquery[validation]"
pip install "buildaquery[all-databases]"
```

If you are working in this repository, use Poetry:

```bash
poetry install
```

## Core Workflow

1. Build an AST query object.
2. Execute it with the executor for your database.
3. Let Build-a-Query compile and parameterize automatically.
4. Use `to_sql()` on a compiler or executor when you want a safe SQL preview before execution.

Optional resilience path:
4. Use `RetryPolicy` + `execute_with_retry(...)` (or `fetch_all_with_retry(...)` / `fetch_one_with_retry(...)` / `execute_many_with_retry(...)`) to retry transient failures using normalized execution errors.
5. Use executor lifecycle and connection controls (`with ...`, `close()`, `connect_timeout_seconds`, pool hooks) for production deployments.
6. Use `ObservabilitySettings(query_observer=..., event_observer=..., metadata=...)` to capture query timing and lifecycle execution events.
7. For immediate log visibility, wire `event_observer` with `make_json_event_logger(logger=...)`.
8. For built-in telemetry in-process, use `InMemoryMetricsAdapter`, `InMemoryTracingAdapter`, and `compose_event_observers(...)`.
9. If input comes from external sources, validate it first with the optional `buildaquery.validation` models/translators.
10. If your app must block ad-hoc raw SQL, set `raw_sql_policy="deny_untrusted"` or `raw_sql_policy="deny_all"` on executors.

Normalized execution errors include:
- dialect
- operation
- SQLSTATE when available
- redacted placeholder SQL context

## Optional Boundary Validation

Use minimal Pydantic validation at the app boundary for external payloads (API/CLI/job/env input).

```python
from buildaquery.validation import (
    ExecutorInputConfigModel,
    RawExecutionRequestModel,
    to_connection_settings_kwargs,
    to_raw_execution_payload,
)

validated_config = ExecutorInputConfigModel(
    connection_info="postgresql://user:password@localhost:5432/mydb",
    connect_timeout_seconds=3,
)
validated_query = RawExecutionRequestModel(
    sql="SELECT * FROM users WHERE id = %s",
    params=[1],
)

connection_kwargs = to_connection_settings_kwargs(validated_config)
sql, params = to_raw_execution_payload(validated_query)
```

## Raw SQL Guardrails

`execute_raw(...)` is an explicit escape hatch. By default it is allowed, but you can enforce safer runtime boundaries with executor policy:

- `raw_sql_policy="allow"`: current/default behavior.
- `raw_sql_policy="deny_untrusted"`: blocks `execute_raw(...)` unless `trusted=True` is passed.
- `raw_sql_policy="deny_all"`: blocks `execute_raw(...)` always.

Example:

```python
from buildaquery.execution.sqlite import SqliteExecutor

executor = SqliteExecutor(
    connection_info="static/test-sqlite/db.sqlite",
    raw_sql_policy="deny_untrusted",
)

# blocked (raises ProgrammingExecutionError)
# executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")

# allowed explicit trusted call
executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)", trusted=True)
```

## SQL Preview

Use `to_sql()` to inspect generated SQL without executing it:

- `compiler.to_sql(ast)` returns a `CompiledQuery`
- `executor.to_sql(ast_or_compiled)` returns a `CompiledQuery`
- `CompiledQuery.to_sql()` returns the placeholder SQL string

Parameter values remain in `CompiledQuery.params`; they are not interpolated into the SQL text.

```python
from buildaquery.abstract_syntax_tree.models import ColumnNode, SelectStatementNode, TableNode
from buildaquery.compiler import SqliteCompiler
from buildaquery.execution.sqlite import SqliteExecutor

query = SelectStatementNode(
    select_list=[ColumnNode(name="id")],
    from_table=TableNode(name="users"),
)

compiled = SqliteCompiler().to_sql(query)
print(compiled.to_sql())
print(compiled.params)

executor = SqliteExecutor(connection_info="static/test-sqlite/db.sqlite")
preview = executor.to_sql(query)
print(preview.to_sql())
print(preview.params)
```
## OLTP Features

For transactional workloads, Build-a-Query provides:

- Transaction control APIs: `begin()`, `commit()`, `rollback()`, `savepoint(name)`, `rollback_to_savepoint(name)`, and `release_savepoint(name)`.
  - DuckDB note: savepoint APIs are runtime-version dependent; unsupported runtimes raise a clear executor `RuntimeError`.
  - ClickHouse note: explicit transaction/savepoint APIs are not supported by `ClickHouseExecutor`.
- Concurrency lock clauses on `SELECT` (dialect-aware), including `NOWAIT` and `SKIP LOCKED` on supported backends.
- Dialect-aware upsert support with conflict handling.
- Write-return payloads through `returning_clause` (`RETURNING`/`OUTPUT` equivalents by dialect).
- Batch write support via `InsertStatementNode.rows` and `execute_many(...)`.
- Retry helpers with normalized transient errors:
  - `DeadlockError`
  - `SerializationError`
  - `LockTimeoutError`
  - `ConnectionTimeoutError`
- Connection lifecycle controls (`close()`, context manager), connect timeout (`connect_timeout_seconds`), and optional pool hooks (`acquire_connection`, `release_connection`).
- Observability hooks through `ObservabilitySettings`:
  - `query_observer` for query timing payloads.
  - `event_observer` for lifecycle events (`query.*`, `retry.*`, `txn.*`, `connection.*`).
  - Built-in adapters are available for JSON logs, in-memory metrics, and in-memory tracing spans.

### Minimal Transaction + Retry Example

```python
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.retry import RetryPolicy

executor = PostgresExecutor(connection_info="postgresql://user:password@localhost:5432/mydb")
policy = RetryPolicy(max_attempts=3)

executor.begin()
try:
    executor.execute_with_retry("UPDATE accounts SET balance = balance - %s WHERE id = %s", [50, 1], policy=policy)
    executor.execute_with_retry("UPDATE accounts SET balance = balance + %s WHERE id = %s", [50, 2], policy=policy)
    executor.commit()
except Exception:
    executor.rollback()
    raise
finally:
    executor.close()
```

For first-time local validation with the fewest setup steps, use [Quick Start (SQLite)](#quick-start-sqlite-recommended).

## Quick Start (PostgreSQL)

```python
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode,
    TableNode,
    StarNode,
)

executor = PostgresExecutor(connection_info="postgresql://user:password@localhost:5432/mydb")

query = SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="users"),
)

rows = executor.execute(query)
print(rows)

# Explicit lifecycle control
executor.close()
```

## Quick Start (SQLite, Recommended)

```python
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode,
    TableNode,
    StarNode,
)

executor = SqliteExecutor(connection_info="static/test-sqlite/db.sqlite")

query = SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="users"),
)

rows = executor.execute(query)
print(rows)
```

SQLite version is provided by Python's `sqlite3` build. Check with `sqlite3.sqlite_version`.

## Quick Start (DuckDB)

```python
from buildaquery.execution.duckdb import DuckDbExecutor
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode,
    TableNode,
    StarNode,
)

executor = DuckDbExecutor(connection_info="static/test-duckdb/sample.duckdb")

query = SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="users"),
)

rows = executor.execute(query)
print(rows)
```

## Quick Start (ClickHouse)

```python
from buildaquery.execution.clickhouse import ClickHouseExecutor
from buildaquery.abstract_syntax_tree.models import (
    ColumnNode,
    InsertStatementNode,
    LiteralNode,
    SelectStatementNode,
    TableNode,
)

executor = ClickHouseExecutor(connection_info="clickhouse://buildaquery:password@127.0.0.1:9001/buildaquery_test")
events = TableNode(name="events")

# ClickHouse CREATE TABLE typically requires an engine clause, so use raw SQL.
executor.execute_raw("CREATE TABLE IF NOT EXISTS events (id UInt32, value String) ENGINE = Memory")
executor.execute(
    InsertStatementNode(
        table=events,
        columns=[ColumnNode(name="id"), ColumnNode(name="value")],
        values=[LiteralNode(value=1), LiteralNode(value="hello")],
    )
)
rows = executor.execute(
    SelectStatementNode(
        select_list=[ColumnNode(name="id"), ColumnNode(name="value")],
        from_table=events,
    )
)
print(rows)
```

## Basic Query Examples

### Filtered SELECT

```python
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode,
    TableNode,
    ColumnNode,
    WhereClauseNode,
    BinaryOperationNode,
    LiteralNode,
)

query = SelectStatementNode(
    select_list=[ColumnNode(name="id"), ColumnNode(name="name")],
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="age"),
            operator=">=",
            right=LiteralNode(value=18),
        )
    ),
)
```

### INSERT

```python
from buildaquery.abstract_syntax_tree.models import (
    InsertStatementNode,
    TableNode,
    ColumnNode,
    LiteralNode,
)

query = InsertStatementNode(
    table=TableNode(name="users"),
    columns=[ColumnNode(name="name"), ColumnNode(name="email")],
    values=[LiteralNode(value="Alice"), LiteralNode(value="alice@example.com")],
)
```

### BATCH INSERT

```python
from buildaquery.abstract_syntax_tree.models import (
    InsertStatementNode,
    TableNode,
    ColumnNode,
    LiteralNode,
)

query = InsertStatementNode(
    table=TableNode(name="users"),
    columns=[ColumnNode(name="id"), ColumnNode(name="email")],
    rows=[
        [LiteralNode(value=1), LiteralNode(value="a@example.com")],
        [LiteralNode(value=2), LiteralNode(value="b@example.com")],
    ],
)
```

### UPSERT / CONFLICT HANDLING

```python
from buildaquery.abstract_syntax_tree.models import (
    InsertStatementNode,
    TableNode,
    ColumnNode,
    LiteralNode,
    ConflictTargetNode,
    UpsertClauseNode,
)

query = InsertStatementNode(
    table=TableNode(name="users"),
    columns=[ColumnNode(name="id"), ColumnNode(name="email")],
    values=[LiteralNode(value=1), LiteralNode(value="alice@new.example")],
    upsert_clause=UpsertClauseNode(
        conflict_target=ConflictTargetNode(columns=[ColumnNode(name="id")]),
        update_columns=["email"],
    ),
)
```

### WRITE-RETURN PAYLOADS (`RETURNING` / `OUTPUT`)

```python
from buildaquery.abstract_syntax_tree.models import (
    InsertStatementNode,
    TableNode,
    ColumnNode,
    LiteralNode,
    ReturningClauseNode,
)

query = InsertStatementNode(
    table=TableNode(name="users"),
    columns=[ColumnNode(name="name"), ColumnNode(name="email")],
    values=[LiteralNode(value="Alice"), LiteralNode(value="alice@example.com")],
    returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id"), ColumnNode(name="email")]),
)
```

### DDL CONSTRAINTS / INDEXES / ALTER TABLE

```python
from buildaquery.abstract_syntax_tree.models import (
    AddColumnActionNode,
    AlterTableStatementNode,
    BinaryOperationNode,
    CheckConstraintNode,
    ColumnDefinitionNode,
    ColumnNode,
    CreateIndexStatementNode,
    CreateStatementNode,
    ForeignKeyConstraintNode,
    PrimaryKeyConstraintNode,
    TableNode,
    UniqueConstraintNode,
)

users = TableNode(name="users")
accounts = TableNode(name="accounts")

create_accounts = CreateStatementNode(
    table=accounts,
    columns=[
        ColumnDefinitionNode(name="account_id", data_type="INTEGER", not_null=True),
        ColumnDefinitionNode(name="user_id", data_type="INTEGER", not_null=True),
        ColumnDefinitionNode(name="balance", data_type="INTEGER", not_null=True),
    ],
    constraints=[
        PrimaryKeyConstraintNode(columns=[ColumnNode(name="account_id"), ColumnNode(name="user_id")]),
        UniqueConstraintNode(columns=[ColumnNode(name="user_id"), ColumnNode(name="account_id")]),
        ForeignKeyConstraintNode(
            columns=[ColumnNode(name="user_id")],
            reference_table=users,
            reference_columns=[ColumnNode(name="id")],
            on_delete="CASCADE",
        ),
        CheckConstraintNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="balance"),
                operator=">",
                right=ColumnNode(name="user_id"),
            )
        ),
    ],
)

create_idx = CreateIndexStatementNode(
    name="idx_accounts_user_id",
    table=accounts,
    columns=[ColumnNode(name="user_id")],
)

alter_add_status = AlterTableStatementNode(
    table=accounts,
    actions=[AddColumnActionNode(column=ColumnDefinitionNode(name="status", data_type="TEXT"))],
)
```

### UPDATE

```python
from buildaquery.abstract_syntax_tree.models import (
    UpdateStatementNode,
    TableNode,
    LiteralNode,
    WhereClauseNode,
    BinaryOperationNode,
    ColumnNode,
)

query = UpdateStatementNode(
    table=TableNode(name="users"),
    set_clauses={"email": LiteralNode(value="new@example.com")},
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="id"),
            operator="=",
            right=LiteralNode(value=1),
        )
    ),
)
```

### DELETE

```python
from buildaquery.abstract_syntax_tree.models import (
    DeleteStatementNode,
    TableNode,
    WhereClauseNode,
    BinaryOperationNode,
    ColumnNode,
    LiteralNode,
)

query = DeleteStatementNode(
    table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="id"),
            operator="=",
            right=LiteralNode(value=1),
        )
    ),
)
```

## Dialect Notes (Important)

- MySQL: `INTERSECT`, `EXCEPT`, and `DROP TABLE ... CASCADE` are not supported.
- SQLite: `DROP TABLE ... CASCADE` is not supported.
- DuckDB: `DROP TABLE ... CASCADE` and trailing row-lock clauses (`FOR UPDATE` / `FOR SHARE`) are not supported in this compiler.
- ClickHouse: `DROP TABLE ... CASCADE`, row-lock clauses, upsert clauses, and generic `RETURNING` payloads are not supported in this compiler. `ClickHouseExecutor` does not expose explicit transaction/savepoint APIs.
- Oracle: `IF EXISTS` / `IF NOT EXISTS` are not supported for `DROP` / `CREATE`; `EXCEPT` compiles to `MINUS`.
- SQL Server: `EXCEPT ALL`, `INTERSECT ALL`, and `DROP TABLE ... CASCADE` are not supported.
- MariaDB: `DROP TABLE ... CASCADE` is accepted as a no-op.
- Upsert behavior:
  - PostgreSQL, SQLite, CockroachDB use `ON CONFLICT`.
  - MySQL, MariaDB use `ON DUPLICATE KEY UPDATE`.
  - Oracle, SQL Server use `MERGE`-based upsert generation.
- Write-return behavior:
  - PostgreSQL, SQLite, CockroachDB compile `returning_clause` as `RETURNING`.
  - MariaDB compiles `returning_clause` as `RETURNING` for `INSERT` and `DELETE` (not `UPDATE`).
  - SQL Server compiles `returning_clause` as `OUTPUT INSERTED...` / `OUTPUT DELETED...`.
  - MySQL generic `RETURNING` payloads are not supported.
- Oracle `RETURNING ... INTO` is not yet supported.
- Batch write behavior:
  - Multi-row insert payloads are represented with `InsertStatementNode.rows`.
  - Executors also expose `execute_many(sql, param_sets)` for driver-level bulk writes.
  - Oracle uses `INSERT ALL` for multi-row inserts.
  - SQL Server and Oracle `MERGE` upsert paths currently support single-row `values` only.
- DDL behavior:
  - `CreateStatementNode.constraints` supports table-level `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, and `CHECK`.
  - `CreateIndexStatementNode` and `DropIndexStatementNode` are supported with dialect-specific `IF EXISTS`/`IF NOT EXISTS` and table-target rules.
  - `AlterTableStatementNode` supports add/drop column and add/drop constraint actions with dialect-specific restrictions.
- Normalized execution errors:
  - Retry helpers classify transient failures into `DeadlockError`, `SerializationError`, `LockTimeoutError`, and `ConnectionTimeoutError`.
  - Non-transient examples include `IntegrityConstraintError` and `ProgrammingExecutionError`.
- Connection management:
  - All executors support `connect_timeout_seconds`.
  - All executors support pool hooks via `acquire_connection` and `release_connection`.
  - Executors support context manager lifecycle control and `close()` for resource cleanup.
- Observability:
  - All executors support `query_observer` (structured query timing payloads) and `event_observer` (structured lifecycle logging events).
  - Lifecycle event names: `query.start`, `query.end`, `retry.scheduled`, `retry.giveup`, `txn.begin`, `txn.commit`, `txn.rollback`, `txn.savepoint.create`, `txn.savepoint.rollback`, `txn.savepoint.release`, `connection.acquire.start`, `connection.acquire.end`, `connection.release`, `connection.close`.
- OLTP integration coverage:
  - Integration tests include contention/retry success, deadlock normalization, lost-update prevention, transaction visibility isolation checks, and lock semantics (`NOWAIT`, `SKIP LOCKED`).
  - Dedicated scenarios live in `tests/test_oltp_integration.py`.

## Testing Commands (Repo)

```bash
poetry run unit-tests
poetry run setup-tests
poetry run integration-tests
poetry run all-tests
poetry run clean
poetry run package-check
```

## Where To Go Next

- Project overview: `README.md`
- End-to-end examples: `examples/`
- Syntax-only canonical quickstart example (no DB interaction): `examples/sample_syntax_quickstart.py`
- Copy-paste starter templates (CRUD/upsert/transaction/retry/observability): `examples/sample_starter_templates.py`
- DuckDB example: `examples/sample_duckdb.py`
- ClickHouse syntax example: `examples/sample_clickhouse.py`
- Observability wiring example: `examples/sample_observability_integration.py`
- Boundary validation example: `examples/sample_validation.py`
- Integration test setup details: `tests/README.md`
- Developer internals (AST nodes, traversal, compilers, executors): nested `README.md` files in:
  - `buildaquery/abstract_syntax_tree/`
  - `buildaquery/traversal/`
  - `buildaquery/compiler/`
  - `buildaquery/execution/`



