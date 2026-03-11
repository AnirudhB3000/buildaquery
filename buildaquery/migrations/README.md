# Migrations

This package contains a minimal migration runner for ordered schema evolution on top of the existing executor APIs.

## Public Surface

- `MigrationStep(version, name, up, down=None)`: one ordered migration definition.
- `MigrationRunner(tracking_table="buildaquery_migrations", transactional=True)`: applies pending migrations and tracks applied versions.
- `AppliedMigration`: one row from the tracking table.
- `MigrationApplySummary`: reports what an apply run did.
- `MigrationRollbackSummary`: reports the outcome of `rollback_last(...)`.

## Supported Action Types

Each `MigrationStep.up` or `MigrationStep.down` can be:

- an AST statement node
- a `CompiledQuery`
- a callable that receives the executor

Callable actions still go through the existing executor surface. If a callable uses `execute_raw(...)`, the executor's `raw_sql_policy` still applies.

## Example

```python
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.migrations import MigrationRunner, MigrationStep

executor = SqliteExecutor(connection_info="static/test-sqlite/db.sqlite")
runner = MigrationRunner()

migrations = [
    MigrationStep(
        version=1,
        name="create-users",
        up=CompiledQuery(
            sql="CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)",
        ),
        down=CompiledQuery(sql="DROP TABLE users"),
    ),
]

summary = runner.apply(executor, migrations)
print(summary.applied_versions)
```
