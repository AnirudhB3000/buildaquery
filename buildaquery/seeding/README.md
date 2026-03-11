# Seeding

This package contains a minimal deterministic seeding utility built on top of the existing executor APIs.

## Public Surface

- `SeedStep(name, action)`: one ordered seed operation.
- `SeedRunner(transactional=True)`: executes steps in order, optionally wrapping the run in a transaction when the executor supports transactions.
- `SeedRunSummary`: reports the completed outcome of a seed run.
- `SeedRunError`: identifies the failed step and how many steps completed before the error.

## Supported Step Types

Each `SeedStep.action` can be:

- an AST statement node
- a `CompiledQuery`
- a callable that receives the executor

Callable steps are useful for small imperative hooks, but they still go through the existing executor surface. If a callable uses `execute_raw(...)`, the executor's `raw_sql_policy` still applies.

## Example

```python
from buildaquery.abstract_syntax_tree.models import (
    ColumnNode,
    InsertStatementNode,
    LiteralNode,
    TableNode,
)
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.seeding import SeedRunner, SeedStep

executor = SqliteExecutor(connection_info="static/test-sqlite/db.sqlite")

steps = [
    SeedStep(
        name="insert-admin",
        action=InsertStatementNode(
            table=TableNode(name="users"),
            columns=[ColumnNode(name="id"), ColumnNode(name="email")],
            values=[LiteralNode(1), LiteralNode("admin@example.com")],
        ),
    ),
]

summary = SeedRunner(transactional=True).run(executor, steps)
print(summary.completed_steps)
```
