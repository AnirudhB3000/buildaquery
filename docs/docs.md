# Build-a-Query User Guide

This guide is a concise, user-focused reference for installing Build-a-Query, building queries, and running tests. For implementation details and developer internals, use the nested package `README.md` files in `buildaquery/`.

## What It Is

Build-a-Query is a Python query builder that uses an Abstract Syntax Tree (AST) to generate parameterized SQL safely across multiple databases.

Supported dialects:
- PostgreSQL
- SQLite
- MySQL
- MariaDB
- CockroachDB
- Oracle
- SQL Server

## Install

```bash
pip install buildaquery
```

If you are working in this repository, use Poetry:

```bash
poetry install
```

## Core Workflow

1. Build an AST query object.
2. Execute it with the executor for your database.
3. Let Build-a-Query compile and parameterize automatically.

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
```

## Quick Start (SQLite)

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
- Oracle: `IF EXISTS` / `IF NOT EXISTS` are not supported for `DROP` / `CREATE`; `EXCEPT` compiles to `MINUS`.
- SQL Server: `EXCEPT ALL`, `INTERSECT ALL`, and `DROP TABLE ... CASCADE` are not supported.
- MariaDB: `DROP TABLE ... CASCADE` is accepted as a no-op.

## Testing Commands (Repo)

```bash
poetry run unit-tests
poetry run setup-tests
poetry run integration-tests
poetry run all-tests
poetry run clean
```

## Where To Go Next

- Project overview: `README.md`
- End-to-end examples: `examples/`
- Integration test setup details: `tests/README.md`
- Developer internals (AST nodes, traversal, compilers, executors): nested `README.md` files in:
  - `buildaquery/abstract_syntax_tree/`
  - `buildaquery/traversal/`
  - `buildaquery/compiler/`
  - `buildaquery/execution/`
