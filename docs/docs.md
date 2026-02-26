# Build-a-Query: Comprehensive Documentation

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Core Concepts](#core-concepts)
4. [Getting Started](#getting-started)
5. [AST Model Reference](#ast-model-reference)
6. [PostgreSQL Compiler](#postgresql-compiler)
7. [SQLite Compiler](#sqlite-compiler)
8. [MySQL Compiler](#mysql-compiler)
9. [Oracle Compiler](#oracle-compiler)
10. [SQL Server Compiler](#sql-server-compiler)
11. [Execution Layer](#execution-layer)
12. [Traversal Patterns](#traversal-patterns)
13. [Usage Examples](#usage-examples)
14. [Advanced Topics](#advanced-topics)
15. [Testing](#testing)
16. [Troubleshooting](#troubleshooting)

---

## Introduction

**Build-a-Query** is a Python-based query builder library designed to programmatically construct, compile, and execute SQL queries using a dialect-agnostic Abstract Syntax Tree (AST). The library supports PostgreSQL, SQLite, MySQL, Oracle, and SQL Server.

### Design Philosophy

- **Type-Safe AST Construction**: Uses Python dataclasses to represent SQL queries as structured objects.
- **Separation of Concerns**: Clean separation between AST modeling, compilation, and execution.
- **Extensibility**: Leverages the Visitor pattern to allow custom transformations and analyses.
- **Security**: Automatic parameterization to prevent SQL injection attacks.

### Dialect Notes

- **MySQL**: `INTERSECT`, `EXCEPT`, and `DROP TABLE ... CASCADE` are not supported by the compiler (raises `ValueError`).
- **SQLite**: `DROP TABLE ... CASCADE` is not supported by the compiler (raises `ValueError`).
- **Oracle**: `IF EXISTS` / `IF NOT EXISTS` are not supported in `DROP TABLE` / `CREATE TABLE` (raises `ValueError`). `EXCEPT` is compiled as `MINUS`.
- **SQL Server**: `EXCEPT ALL` / `INTERSECT ALL` and `DROP TABLE ... CASCADE` are not supported by the compiler (raises `ValueError`).

### Key Use Cases

- Build dynamic SQL queries programmatically without string concatenation.
- Analyze or transform SQL queries before execution.
- Create an abstraction layer over multiple database systems.
- Generate reports, migrations, or data transformation scripts.

---

## Architecture Overview

Build-a-Query follows a three-layer architecture:

```
User Code
    ↓
Abstract Syntax Tree (AST) Layer
    ↓
Compiler Layer (PostgreSQL / SQLite / MySQL / Oracle / SQL Server)
    ↓
Execution Layer (psycopg / mysql-connector-python / oracledb / pyodbc / sqlite3)
    ↓
PostgreSQL / MySQL / Oracle / SQL Server / SQLite Databases
```

### Layer Breakdown

#### 1. **Abstract Syntax Tree (AST) Layer** (`buildaquery/abstract_syntax_tree/`)

Defines the data structures representing SQL queries. Each SQL construct (SELECT, INSERT, WHERE, etc.) is represented as a Python dataclass.

- **Nodes**: Inherit from `ASTNode` base class
- **Types**: Expression nodes, statement nodes, clause nodes
- **Dataclasses**: Nodes are plain dataclasses, optimized for clarity and type safety

#### 2. **Compiler Layer** (`buildaquery/compiler/`)

Converts AST representations into executable SQL strings with parameterized values.

- **PostgresCompiler**: Implements PostgreSQL-specific SQL generation
- **SqliteCompiler**: Implements SQLite-specific SQL generation
- **MySqlCompiler**: Implements MySQL-specific SQL generation
- **OracleCompiler**: Implements Oracle-specific SQL generation
- **MsSqlCompiler**: Implements SQL Server-specific SQL generation
- **Visitor Pattern**: Uses the visitor pattern to traverse the AST
- **Parameterization**: Extracts values into a params list for safe execution

#### 3. **Execution Layer** (`buildaquery/execution/`)

Handles database connections and query execution.

- **PostgresExecutor**: Manages PostgreSQL connections via psycopg
- **SqliteExecutor**: Manages SQLite database files via the standard library `sqlite3` module
- **MySqlExecutor**: Manages MySQL connections via mysql-connector-python
- **OracleExecutor**: Manages Oracle connections via oracledb
- **MsSqlExecutor**: Manages SQL Server connections via pyodbc
- **Connection Management**: Supports both connection strings and existing connections
- **Methods**: `execute()`, `fetch_all()`, `fetch_one()`, `execute_raw()`

#### 4. **Traversal Module** (`buildaquery/traversal/`)

Provides base classes for AST traversal:

- **Visitor**: For read-only tree traversal and analysis
- **Transformer**: For creating modified AST copies

---

## Core Concepts

### 1. Abstract Syntax Tree (AST)

An AST is a tree representation of the structure of source code. In Build-a-Query, each SQL query is represented as a tree of interconnected node objects.

**Example:**
```python
SelectStatementNode(
    select_list=[ColumnNode(name="id"), ColumnNode(name="name")],
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="age"),
            operator=">",
            right=LiteralNode(value=18)
        )
    )
)
```

This represents:
```sql
SELECT id, name FROM users WHERE age > 18;
```

### 2. Nodes

All elements in the AST inherit from the base `ASTNode` class:

- **Expression Nodes**: Represent values, operations, function calls (inherit from `ExpressionNode`)
- **Statement Nodes**: Represent top-level SQL statements (inherit from `StatementNode`)
- **Clause Nodes**: Represent SQL clauses (FROM, WHERE, JOIN, etc., inherit from `FromClauseNode`, `WhereClauseNode`, etc.)

### 3. Parameterization

All user-provided values are extracted into a separate `params` list, preventing SQL injection:

```python
# Instead of:
"SELECT * FROM users WHERE name = 'Robert'; DROP TABLE users;--"

# Build-a-Query produces:
sql = "SELECT * FROM users WHERE name = %s"  # PostgreSQL
sql = "SELECT * FROM users WHERE name = ?"   # SQLite
params = ["Robert'; DROP TABLE users;--"]
```

The database driver safely handles the values.

### 4. Visitor Pattern

The library uses the Visitor design pattern for tree traversal:

- **Visitor**: Read-only traversal and code generation
- **Transformer**: Tree traversal with node modification

This allows extending the library without modifying core AST classes.

---

## Getting Started

### Installation

```bash
pip install buildaquery
```

### Prerequisites

For PostgreSQL, ensure you have a running database and the required environment variables set:

```bash
# .env file
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydatabase
DB_USER=postgres
DB_PASSWORD=yourpassword
```

For SQLite, no external database server is required. You only need a writable file path (e.g., `static/test-sqlite/db.sqlite`).

For MySQL, ensure you have a running database and a connection URL (for example: `mysql://user:password@host:3306/dbname`).

For Oracle, ensure you have a running database and a connection URL (for example: `oracle://user:password@host:1521/service_name`).

### First Query

```python
from dotenv import load_dotenv
import os
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, StarNode
)

# Load environment variables
load_dotenv()

# Build connection string
connection_string = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Create executor
executor = PostgresExecutor(connection_info=connection_string)

# Build a simple SELECT query
query = SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="users")
)

# Execute and print results
results = executor.execute(query)
print(results)
```

### SQLite Quick Start

```python
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, StarNode
)

executor = SqliteExecutor(connection_info="static/test-sqlite/db.sqlite")

query = SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="users")
)

results = executor.execute(query)
print(results)
```

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

---

## AST Model Reference

This section documents all available AST node types.

### Base Classes

#### `ASTNode`

```python
@dataclass
class ASTNode(ABC):
    """Base class for all AST nodes."""
    pass
```

All nodes inherit from this abstract base class.

#### `ExpressionNode`

```python
@dataclass
class ExpressionNode(ASTNode):
    """Base class for all expression nodes."""
    pass
```

Used for nodes representing values or operations.

#### `StatementNode`

```python
@dataclass
class StatementNode(ASTNode):
    """Base class for all statement nodes."""
    pass
```

Used for top-level SQL statements.

### Expression Nodes

#### `LiteralNode`

Represents a literal value (number, string, boolean).

```python
from buildaquery.abstract_syntax_tree.models import LiteralNode

# Integer literal
LiteralNode(value=42)

# String literal
LiteralNode(value="Alice")

# Boolean literal
LiteralNode(value=True)
```

#### `ColumnNode`

Represents a column reference.

```python
# Simple column reference
ColumnNode(name="email")

# Column with table qualifier
ColumnNode(name="email", table="users")
```

#### `StarNode`

Represents the `*` wildcard.

```python
# SELECT *
SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="users")
)
```

#### `BinaryOperationNode`

Represents binary operations (e.g., `+`, `-`, `=`, `>`).

```python
# age > 18
BinaryOperationNode(
    left=ColumnNode(name="age"),
    operator=">",
    right=LiteralNode(value=18)
)

# Supported operators: =, !=, <>, <, >, <=, >=, +, -, *, /, %, AND, OR, etc.
```

#### `UnaryOperationNode`

Represents unary operations (e.g., `NOT`, negation).

```python
# NOT active
UnaryOperationNode(
    operator="NOT",
    operand=ColumnNode(name="active")
)
```

#### `FunctionCallNode`

Represents function calls.

```python
# COUNT(*)
FunctionCallNode(
    name="COUNT",
    args=[StarNode()]
)

# MAX(salary)
FunctionCallNode(
    name="MAX",
    args=[ColumnNode(name="salary")]
)

# SUM(amount) OVER (PARTITION BY customer_id)
FunctionCallNode(
    name="SUM",
    args=[ColumnNode(name="amount")],
    over=OverClauseNode(
        partition_by=[ColumnNode(name="customer_id")]
    )
)
```

#### `OverClauseNode`

Represents the `OVER (...)` clause for window functions.

```python
OverClauseNode(
    partition_by=[ColumnNode(name="dept")],
    order_by=[OrderByClauseNode(expression=ColumnNode(name="salary"), direction="DESC")]
)
```

#### `CastNode`

Represents type casting.

```python
# CAST(age AS TEXT)
CastNode(
    expression=ColumnNode(name="age"),
    data_type="TEXT"
)
```

#### `AliasNode`

Represents aliased expressions.

```python
# name AS full_name
AliasNode(
    expression=ColumnNode(name="name"),
    name="full_name"
)
```

#### `InNode`

Represents `IN` and `NOT IN` expressions.

```python
# status IN ('active', 'pending')
InNode(
    expression=ColumnNode(name="status"),
    values=[LiteralNode(value="active"), LiteralNode(value="pending")],
    negated=False
)

# NOT IN variant
InNode(
    expression=ColumnNode(name="status"),
    values=[LiteralNode(value="inactive")],
    negated=True
)
```

#### `BetweenNode`

Represents `BETWEEN` expressions.

```python
# age BETWEEN 18 AND 65
BetweenNode(
    expression=ColumnNode(name="age"),
    low=LiteralNode(value=18),
    high=LiteralNode(value=65),
    negated=False
)
```

#### `CaseExpressionNode`

Represents `CASE` expressions.

```python
# CASE WHEN age < 18 THEN 'minor' WHEN age >= 18 THEN 'adult' END
CaseExpressionNode(
    cases=[
        WhenThenNode(
            condition=BinaryOperationNode(...),
            result=LiteralNode(value="minor")
        ),
        WhenThenNode(
            condition=BinaryOperationNode(...),
            result=LiteralNode(value="adult")
        )
    ],
    else_result=LiteralNode(value="unknown")
)
```

#### `SubqueryNode`

Represents subqueries in expressions or FROM clauses.

```python
# (SELECT * FROM active_users) AS au
SubqueryNode(
    statement=SelectStatementNode(...),
    alias="au"
)
```

### Statement Nodes

#### `SelectStatementNode`

Represents a SELECT statement.

```python
SelectStatementNode(
    select_list=[ColumnNode(name="id"), ColumnNode(name="name")],
    distinct=False,
    ctes=None,  # Common Table Expressions
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(...),
    group_by=GroupByClauseNode(...),
    having_clause=HavingClauseNode(...),
    order_by_clause=[OrderByClauseNode(...)],
    top_clause=None,
    limit=None,
    offset=None
)
```

#### `TopClauseNode`

Represents a `TOP` clause for dialects that support it. It is mutually exclusive with `LIMIT` and `OFFSET`.

```python
TopClauseNode(
    count=10,
    on_expression=ColumnNode(name="score"),
    direction="DESC"
)
```

Note: In the PostgreSQL, SQLite, and MySQL compilers, `TOP` is translated into `LIMIT` (and may inject an `ORDER BY` if needed). In the Oracle compiler, `TOP` is translated into `FETCH FIRST ... ROWS ONLY`.

#### `CTENode`

Represents a Common Table Expression used in a `WITH` clause.

```python
CTENode(
    name="recent_users",
    subquery=SelectStatementNode(...)
)
```

#### `InsertStatementNode`

Represents an INSERT statement.

```python
InsertStatementNode(
    table=TableNode(name="users"),
    columns=[ColumnNode(name="name"), ColumnNode(name="email")],
    values=[LiteralNode(value="Alice"), LiteralNode(value="alice@example.com")]
)
```

#### `UpdateStatementNode`

Represents an UPDATE statement.

```python
UpdateStatementNode(
    table=TableNode(name="users"),
    set_clauses={
        "email": LiteralNode(value="newemail@example.com"),
        "updated_at": FunctionCallNode(name="NOW", args=[])
    },
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(...)
    )
)
```

#### `DeleteStatementNode`

Represents a DELETE statement.

```python
DeleteStatementNode(
    table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="id"),
            operator="=",
            right=LiteralNode(value=1)
        )
    )
)
```

#### `CreateStatementNode`

Represents a CREATE TABLE statement.

```python
CreateStatementNode(
    table=TableNode(name="users"),
    columns=[
        ColumnDefinitionNode(
            name="id",
            data_type="SERIAL",
            primary_key=True
        ),
        ColumnDefinitionNode(
            name="email",
            data_type="TEXT",
            not_null=True
        ),
        ColumnDefinitionNode(
            name="created_at",
            data_type="TIMESTAMP",
            default=FunctionCallNode(name="NOW", args=[])
        )
    ],
    if_not_exists=True
)
```

#### `ColumnDefinitionNode`

Represents a column definition used in `CREATE TABLE`.

```python
ColumnDefinitionNode(
    name="id",
    data_type="INTEGER",
    primary_key=True,
    not_null=True,
    default=LiteralNode(value=1)
)
```

#### `DropStatementNode`

Represents a DROP TABLE statement.

```python
DropStatementNode(
    table=TableNode(name="users"),
    if_exists=True,
    cascade=True  # DROP TABLE ... CASCADE
)
```

Note: SQLite does not support `DROP TABLE ... CASCADE`. The SQLite compiler raises `ValueError` when `cascade=True`.

### Clause Nodes

#### `WhereClauseNode`

Represents a WHERE clause.

```python
WhereClauseNode(
    condition=BinaryOperationNode(...)
)
```

#### `JoinClauseNode`

Represents a JOIN clause.

```python
JoinClauseNode(
    left=TableNode(name="users"),
    right=TableNode(name="orders"),
    on_condition=BinaryOperationNode(
        left=ColumnNode(name="id", table="users"),
        operator="=",
        right=ColumnNode(name="user_id", table="orders")
    ),
    join_type="INNER"  # INNER, LEFT, RIGHT, FULL OUTER, CROSS
)
```

Note: SQLite does not support `RIGHT` or `FULL OUTER` joins.

#### `OrderByClauseNode`

Represents an item in an ORDER BY clause.

```python
OrderByClauseNode(
    expression=ColumnNode(name="created_at"),
    direction="DESC"  # ASC or DESC
)
```

#### `GroupByClauseNode`

Represents a GROUP BY clause.

```python
GroupByClauseNode(
    expressions=[ColumnNode(name="department")]
)
```

#### `TableNode`

Represents a table reference.

```python
# Simple table
TableNode(name="users")

# With schema
TableNode(name="users", schema="public")

# With alias
TableNode(name="users", alias="u")
```

---

## PostgreSQL Compiler

The PostgreSQL compiler converts AST nodes into executable PostgreSQL query strings.

### How It Works

The compiler uses the Visitor pattern to traverse the AST and generate SQL:

1. **Initialization**: Create a `PostgresCompiler` instance
2. **Compilation**: Call `compile(ast_node)` to process the entire tree
3. **Output**: Receive a `CompiledQuery` with SQL string and parameters

### CompiledQuery

```python
@dataclass
class CompiledQuery:
    sql: str                    # The PostgreSQL query string with %s placeholders
    params: list[Any]          # List of values to be substituted
```

**Note**: `CompiledQuery` is shared across dialects and lives in `buildaquery/compiler/compiled_query.py`.

### Example

```python
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, WhereClauseNode,
    BinaryOperationNode, LiteralNode
)

compiler = PostgresCompiler()

query = SelectStatementNode(
    select_list=[ColumnNode(name="id"), ColumnNode(name="name")],
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="age"),
            operator=">",
            right=LiteralNode(value=18)
        )
    )
)

compiled = compiler.compile(query)
print(compiled.sql)    # SELECT id, name FROM users WHERE age > %s
print(compiled.params) # [18]
```

### Supported SQL Features

The compiler supports:

- **Clauses**: SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
- **Set Operations**: UNION, INTERSECT, EXCEPT
- **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS
- **Subqueries**: In FROM and WHERE clauses
- **Window Functions**: OVER clause with PARTITION BY and ORDER BY
- **CTEs (Common Table Expressions)**: WITH clause
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX, etc.
- **Type Casting**: CAST and :: operator
- **CASE Expressions**: Conditional logic
- **IN/BETWEEN**: Membership and range testing

### Extending the Compiler

To add support for new SQL features, subclass `PostgresCompiler` and override specific visit methods:

```python
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler

class CustomCompiler(PostgresCompiler):
    def visit_MyCustomNode(self, node):
        # Custom compilation logic
        return "CUSTOM SQL"
```

---

## SQLite Compiler

The SQLite compiler converts AST nodes into executable SQLite query strings.

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

### How It Works

The compiler uses the Visitor pattern to traverse the AST and generate SQL:

1. **Initialization**: Create a `SqliteCompiler` instance
2. **Compilation**: Call `compile(ast_node)` to process the entire tree
3. **Output**: Receive a `CompiledQuery` with SQL string and parameters

### CompiledQuery

```python
@dataclass
class CompiledQuery:
    sql: str                    # The SQLite query string with ? placeholders
    params: list[Any]          # List of values to be substituted
```

### Example

```python
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, WhereClauseNode,
    BinaryOperationNode, LiteralNode
)

compiler = SqliteCompiler()

query = SelectStatementNode(
    select_list=[ColumnNode(name="id"), ColumnNode(name="name")],
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="age"),
            operator=">",
            right=LiteralNode(value=18)
        )
    )
)

compiled = compiler.compile(query)
print(compiled.sql)    # SELECT id, name FROM users WHERE age > ?
print(compiled.params) # [18]
```

### SQLite Notes

- Uses `?` placeholders.
- `TOP` is translated to `LIMIT`.
- `DROP TABLE ... CASCADE` is not supported and raises `ValueError`.
- Window functions require SQLite 3.25+.

---

## MySQL Compiler

The MySQL compiler converts AST nodes into executable MySQL query strings.

### How It Works

1. **Initialization**: Create a `MySqlCompiler` instance
2. **Compilation**: Call `compile(ast_node)` to process the entire tree
3. **Output**: Receive a `CompiledQuery` with SQL string and parameters

### CompiledQuery

```python
@dataclass
class CompiledQuery:
    sql: str                    # The MySQL query string with %s placeholders
    params: list[Any]          # List of values to be substituted
```

### Example

```python
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, WhereClauseNode,
    BinaryOperationNode, LiteralNode
)

compiler = MySqlCompiler()

query = SelectStatementNode(
    select_list=[ColumnNode(name="id"), ColumnNode(name="name")],
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="age"),
            operator=">",
            right=LiteralNode(value=18)
        )
    )
)

compiled = compiler.compile(query)
print(compiled.sql)    # SELECT id, name FROM users WHERE age > %s
print(compiled.params) # [18]
```

### MySQL Notes

- Uses `%s` placeholders.
- `INTERSECT` and `EXCEPT` are not supported and raise `ValueError`.
- `DROP TABLE ... CASCADE` is not supported and raises `ValueError`.

---

## Oracle Compiler

The Oracle compiler converts AST nodes into executable Oracle query strings.

### How It Works

1. **Initialization**: Create an `OracleCompiler` instance
2. **Compilation**: Call `compile(ast_node)` to process the entire tree
3. **Output**: Receive a `CompiledQuery` with SQL string and parameters

### CompiledQuery

```python
@dataclass
class CompiledQuery:
    sql: str                    # The Oracle query string with :1, :2 placeholders
    params: list[Any]          # List of values to be substituted
```

### Example

```python
from buildaquery.compiler.oracle.oracle_compiler import OracleCompiler
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, WhereClauseNode,
    BinaryOperationNode, LiteralNode
)

compiler = OracleCompiler()

query = SelectStatementNode(
    select_list=[ColumnNode(name="id"), ColumnNode(name="name")],
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="age"),
            operator=">",
            right=LiteralNode(value=18)
        )
    )
)

compiled = compiler.compile(query)
print(compiled.sql)    # SELECT id, name FROM users WHERE age > :1
print(compiled.params) # [18]
```

### Oracle Notes

- Uses `:1`, `:2`, ... positional bind variables.
- `LIMIT`/`OFFSET` compile to `OFFSET ... ROWS` and `FETCH FIRST ... ROWS ONLY`.
- `EXCEPT` compiles to `MINUS`.
- `INTERSECT ALL` and `MINUS ALL` raise `ValueError`.
- `IF EXISTS` / `IF NOT EXISTS` in `DROP TABLE` / `CREATE TABLE` raise `ValueError`.
- Table aliases are emitted without `AS`.

---

## SQL Server Compiler

The SQL Server compiler converts AST nodes into executable SQL Server query strings.

### How It Works

1. **Initialization**: Create an `MsSqlCompiler` instance
2. **Compilation**: Call `compile(ast_node)` to process the entire tree
3. **Output**: Receive a `CompiledQuery` with SQL string and parameters

### CompiledQuery

```python
@dataclass
class CompiledQuery:
    sql: str                    # The SQL Server query string with ? placeholders
    params: list[Any]          # List of values to be substituted
```

### Example

```python
from buildaquery.compiler.mssql.mssql_compiler import MsSqlCompiler
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, WhereClauseNode,
    BinaryOperationNode, LiteralNode
)

compiler = MsSqlCompiler()

query = SelectStatementNode(
    select_list=[ColumnNode(name="id"), ColumnNode(name="name")],
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="age"),
            operator=">",
            right=LiteralNode(value=18)
        )
    )
)

compiled = compiler.compile(query)
print(compiled.sql)    # SELECT id, name FROM users WHERE age > ?
print(compiled.params) # [18]
```

### SQL Server Notes

- Uses `?` positional bind variables.
- `TOP` is supported and compiled to `TOP n`.
- `LIMIT`/`OFFSET` compile to `OFFSET ... ROWS` and `FETCH NEXT ... ROWS ONLY` (requires `ORDER BY`).
- `EXCEPT ALL` and `INTERSECT ALL` raise `ValueError`.
- `DROP TABLE ... CASCADE` is not supported and raises `ValueError`.

## Execution Layer

The execution layer handles database connections and query execution via psycopg (PostgreSQL), sqlite3 (SQLite), mysql-connector-python (MySQL), oracledb (Oracle), and pyodbc (SQL Server).

### PostgresExecutor

#### Initialization

```python
from buildaquery.execution.postgres import PostgresExecutor

# Method 1: Connection string
executor = PostgresExecutor(
    connection_info="postgresql://user:password@localhost:5432/dbname"
)

# Method 2: Existing psycopg connection
import psycopg
conn = psycopg.connect("postgresql://...")
executor = PostgresExecutor(connection=conn)

# With custom compiler
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler
executor = PostgresExecutor(
    connection_info="...",
    compiler=PostgresCompiler()
)
```

#### Methods

##### `execute(query: CompiledQuery | ASTNode) -> Any`

Executes a query. For SELECT queries, returns rows. For INSERT/UPDATE/DELETE, returns cursor or None.

```python
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, StarNode, TableNode
)

query = SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="users")
)

results = executor.execute(query)
# Returns: [(1, 'Alice', 30), (2, 'Bob', 25), ...]
```

##### `fetch_all(query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]`

Executes a query and returns all rows.

```python
rows = executor.fetch_all(query)
for row in rows:
    print(row)
```

##### `fetch_one(query: CompiledQuery | ASTNode) -> Sequence[Any] | None`

Executes a query and returns the first row.

```python
first_row = executor.fetch_one(query)
if first_row:
    print(first_row)
```

##### `execute_raw(sql: str, params: Sequence[Any] | None = None) -> None`

Executes raw SQL directly (bypass AST compilation).

```python
executor.execute_raw(
    "INSERT INTO users (name, age) VALUES (%s, %s)",
    ["Charlie", 35]
)
```

For SQLite, use `?` placeholders:
```python
executor.execute_raw(
    "INSERT INTO users (name, age) VALUES (?, ?)",
    ["Charlie", 35]
)
```

#### Connection Management

The executor handles connections automatically:

- **Connection String Mode**: Creates a new connection for each query
- **Existing Connection Mode**: Reuses the provided connection

For long-running applications, managing connections externally is recommended:

```python
import psycopg

with psycopg.connect("postgresql://...") as conn:
    executor = PostgresExecutor(connection=conn)
    
    # Perform multiple operations
    results1 = executor.fetch_all(query1)
    results2 = executor.fetch_all(query2)
    
# Connection automatically closed
```

### SqliteExecutor

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

#### Initialization

```python
from buildaquery.execution.sqlite import SqliteExecutor

# Method 1: Database file path
executor = SqliteExecutor(connection_info="static/test-sqlite/db.sqlite")

# Method 2: Existing sqlite3 connection
import sqlite3
conn = sqlite3.connect("static/test-sqlite/db.sqlite")
executor = SqliteExecutor(connection=conn)

# With custom compiler
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler
executor = SqliteExecutor(
    connection_info="static/test-sqlite/db.sqlite",
    compiler=SqliteCompiler()
)
```

#### Methods

The `SqliteExecutor` interface mirrors `PostgresExecutor`:

- `execute(query: CompiledQuery | ASTNode) -> Any`
- `fetch_all(query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]`
- `fetch_one(query: CompiledQuery | ASTNode) -> Sequence[Any] | None`
- `execute_raw(sql: str, params: Sequence[Any] | None = None) -> None`

#### Connection Management

- **Connection String Mode**: Uses the provided database file path to open a connection per call.
- **Existing Connection Mode**: Reuses the provided connection.

### MySqlExecutor

#### Initialization

```python
from buildaquery.execution.mysql import MySqlExecutor

# Method 1: Connection URL
executor = MySqlExecutor(connection_info="mysql://user:password@localhost:3306/dbname")

# Method 2: Existing mysql-connector connection
import mysql.connector
conn = mysql.connector.connect(user="user", password="password", host="localhost", database="dbname")
executor = MySqlExecutor(connection=conn)

# With custom compiler
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
executor = MySqlExecutor(
    connection_info="mysql://user:password@localhost:3306/dbname",
    compiler=MySqlCompiler()
)
```

#### Methods

The `MySqlExecutor` interface mirrors `PostgresExecutor`:

- `execute(query: CompiledQuery | ASTNode) -> Any`
- `fetch_all(query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]`
- `fetch_one(query: CompiledQuery | ASTNode) -> Sequence[Any] | None`
- `execute_raw(sql: str, params: Sequence[Any] | None = None) -> None`

#### Connection Management

- **Connection String Mode**: Uses a MySQL URL to open a connection per call.
- **Existing Connection Mode**: Reuses the provided connection.

### OracleExecutor

#### Initialization

```python
from buildaquery.execution.oracle import OracleExecutor

# Method 1: Connection URL
executor = OracleExecutor(connection_info="oracle://user:password@localhost:1521/XEPDB1")

# Method 2: Existing oracledb connection
import oracledb
conn = oracledb.connect(user="user", password="password", host="localhost", port=1521, service_name="XEPDB1")
executor = OracleExecutor(connection=conn)

# With custom compiler
from buildaquery.compiler.oracle.oracle_compiler import OracleCompiler
executor = OracleExecutor(
    connection_info="oracle://user:password@localhost:1521/XEPDB1",
    compiler=OracleCompiler()
)
```

#### Methods

The `OracleExecutor` interface mirrors `PostgresExecutor`:

- `execute(query: CompiledQuery | ASTNode) -> Any`
- `fetch_all(query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]`
- `fetch_one(query: CompiledQuery | ASTNode) -> Sequence[Any] | None`
- `execute_raw(sql: str, params: Sequence[Any] | None = None) -> None`

#### Connection Management

- **Connection String Mode**: Uses an Oracle URL to open a connection per call.
- **Existing Connection Mode**: Reuses the provided connection.

### MsSqlExecutor

#### Initialization

```python
from buildaquery.execution.mssql import MsSqlExecutor

# Method 1: Connection URL
executor = MsSqlExecutor(connection_info="mssql://user:password@localhost:1433/dbname?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes")

# Method 2: Existing pyodbc connection
import pyodbc
conn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=dbname;UID=user;PWD=password;Encrypt=no;TrustServerCertificate=yes")
executor = MsSqlExecutor(connection=conn)

# With custom compiler
from buildaquery.compiler.mssql.mssql_compiler import MsSqlCompiler
executor = MsSqlExecutor(
    connection_info="mssql://user:password@localhost:1433/dbname?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes",
    compiler=MsSqlCompiler()
)
```

#### Methods

The `MsSqlExecutor` interface mirrors `PostgresExecutor`:

- `execute(query: CompiledQuery | ASTNode) -> Any`
- `fetch_all(query: CompiledQuery | ASTNode) -> Sequence[Sequence[Any]]`
- `fetch_one(query: CompiledQuery | ASTNode) -> Sequence[Any] | None`
- `execute_raw(sql: str, params: Sequence[Any] | None = None) -> None`

#### Connection Management

- **Connection String Mode**: Uses a SQL Server URL to open a connection per call.
- **Existing Connection Mode**: Reuses the provided connection.

---

## Traversal Patterns

The library provides extensible traversal mechanisms for custom analysis and transformations.

### Visitor Pattern

The `Visitor` class enables read-only tree traversal:

```python
from buildaquery.traversal.visitor_pattern import Visitor
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode
)

class ColumnCollector(Visitor):
    """Collects all column names from a query."""
    
    def __init__(self):
        self.columns = set()
    
    def visit_ColumnNode(self, node: ColumnNode) -> None:
        self.columns.add(node.name)
        self.generic_visit(node)
    
    def generic_visit(self, node) -> None:
        """Default behavior: visit children."""
        if hasattr(node, '__dict__'):
            for value in node.__dict__.values():
                if isinstance(value, list):
                    for item in value:
                        if hasattr(item, '__class__'):
                            self.visit(item)
                elif hasattr(value, '__class__'):
                    self.visit(value)

# Usage
query = SelectStatementNode(...)
collector = ColumnCollector()
collector.visit(query)
print(collector.columns)
```

### Transformer Pattern

The `Transformer` class enables tree traversal with node modifications:

```python
from buildaquery.traversal.visitor_pattern import Transformer
from buildaquery.abstract_syntax_tree.models import (
    ColumnNode, LiteralNode
)

class ColumnRenamer(Transformer):
    """Renames columns in a query."""
    
    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping
    
    def visit_ColumnNode(self, node: ColumnNode) -> ColumnNode:
        new_name = self.mapping.get(node.name, node.name)
        return ColumnNode(
            name=new_name,
            table=node.table
        )

# Usage
query = SelectStatementNode(...)
renamer = ColumnRenamer({"email": "user_email"})
new_query = renamer.visit(query)
```

---

## Usage Examples

All examples use `PostgresExecutor`, but you can swap in `SqliteExecutor` for SQLite, `MySqlExecutor` for MySQL, `OracleExecutor` for Oracle, or `MsSqlExecutor` for SQL Server by changing the executor import and using the appropriate connection info. Be aware that some functions (e.g., `NOW()`) are dialect-specific and may not exist in SQLite.

### Example 1: Simple SELECT

```python
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, StarNode
)

executor = PostgresExecutor(connection_info="postgresql://...")

query = SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="users")
)

results = executor.execute(query)
for row in results:
    print(row)
```

### Example 2: SELECT with WHERE

```python
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, WhereClauseNode,
    BinaryOperationNode, LiteralNode
)

query = SelectStatementNode(
    select_list=[ColumnNode(name="name"), ColumnNode(name="email")],
    from_table=TableNode(name="users"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="age"),
            operator=">",
            right=LiteralNode(value=18)
        )
    )
)

results = executor.execute(query)
```

### Example 3: Aggregation with GROUP BY

```python
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, FunctionCallNode, ColumnNode, TableNode,
    GroupByClauseNode, OrderByClauseNode
)

query = SelectStatementNode(
    select_list=[
        ColumnNode(name="department"),
        FunctionCallNode(name="COUNT", args=[StarNode()]),
    ],
    from_table=TableNode(name="employees"),
    group_by=GroupByClauseNode(
        expressions=[ColumnNode(name="department")]
    ),
    order_by_clause=[
        OrderByClauseNode(
            expression=FunctionCallNode(name="COUNT", args=[StarNode()]),
            direction="DESC"
        )
    ]
)

results = executor.execute(query)
for dept, count in results:
    print(f"{dept}: {count} employees")
```

### Example 4: JOIN

```python
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, StarNode, TableNode, JoinClauseNode,
    ColumnNode, BinaryOperationNode
)

users_table = TableNode(name="users", alias="u")
orders_table = TableNode(name="orders", alias="o")

join = JoinClauseNode(
    left=users_table,
    right=orders_table,
    on_condition=BinaryOperationNode(
        left=ColumnNode(name="id", table="u"),
        operator="=",
        right=ColumnNode(name="user_id", table="o")
    ),
    join_type="INNER"
)

query = SelectStatementNode(
    select_list=[
        ColumnNode(name="name", table="u"),
        ColumnNode(name="order_id", table="o")
    ],
    from_table=join
)

results = executor.execute(query)
```

### Example 5: INSERT

```python
from buildaquery.abstract_syntax_tree.models import (
    InsertStatementNode, TableNode, ColumnNode, LiteralNode
)

insert_query = InsertStatementNode(
    table=TableNode(name="users"),
    columns=[
        ColumnNode(name="name"),
        ColumnNode(name="email"),
        ColumnNode(name="age")
    ],
    values=[
        LiteralNode(value="Alice"),
        LiteralNode(value="alice@example.com"),
        LiteralNode(value=30)
    ]
)

executor.execute(insert_query)
```

### Example 6: UPDATE

```python
from buildaquery.abstract_syntax_tree.models import (
    UpdateStatementNode, TableNode, LiteralNode, WhereClauseNode,
    BinaryOperationNode, ColumnNode
)

update_query = UpdateStatementNode(
    table=TableNode(name="users"),
    set_clauses={
        "email": LiteralNode(value="newemail@example.com"),
        "updated_at": FunctionCallNode(name="NOW", args=[])
    },
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="id"),
            operator="=",
            right=LiteralNode(value=1)
        )
    )
)

executor.execute(update_query)
```

### Example 7: CREATE TABLE

```python
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode
)

create_query = CreateStatementNode(
    table=TableNode(name="users"),
    columns=[
        ColumnDefinitionNode(
            name="id",
            data_type="SERIAL",
            primary_key=True
        ),
        ColumnDefinitionNode(
            name="name",
            data_type="TEXT",
            not_null=True
        ),
        ColumnDefinitionNode(
            name="email",
            data_type="TEXT",
            not_null=True
        ),
        ColumnDefinitionNode(
            name="age",
            data_type="INTEGER"
        )
    ],
    if_not_exists=True
)

executor.execute(create_query)
```

### Example 8: CASE Expression

```python
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, CaseExpressionNode,
    WhenThenNode, LiteralNode, BinaryOperationNode, TableNode
)

query = SelectStatementNode(
    select_list=[
        ColumnNode(name="name"),
        CaseExpressionNode(
            cases=[
                WhenThenNode(
                    condition=BinaryOperationNode(
                        left=ColumnNode(name="age"),
                        operator="<",
                        right=LiteralNode(value=18)
                    ),
                    result=LiteralNode(value="minor")
                ),
                WhenThenNode(
                    condition=BinaryOperationNode(
                        left=ColumnNode(name="age"),
                        operator=">=",
                        right=LiteralNode(value=18)
                    ),
                    result=LiteralNode(value="adult")
                )
            ],
            else_result=LiteralNode(value="unknown")
        )
    ],
    from_table=TableNode(name="users")
)

results = executor.execute(query)
```

### Example 9: Window Function

```python
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, FunctionCallNode, TableNode,
    OverClauseNode, OrderByClauseNode
)

query = SelectStatementNode(
    select_list=[
        ColumnNode(name="name"),
        ColumnNode(name="salary"),
        FunctionCallNode(
            name="ROW_NUMBER",
            args=[],
            over=OverClauseNode(
                order_by=[
                    OrderByClauseNode(
                        expression=ColumnNode(name="salary"),
                        direction="DESC"
                    )
                ]
            )
        )
    ],
    from_table=TableNode(name="employees")
)

results = executor.execute(query)
```

### Example 10: Subquery

```python
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, StarNode, SubqueryNode, TableNode,
    WhereClauseNode, BinaryOperationNode, ColumnNode, LiteralNode,
    FunctionCallNode
)

# Subquery: SELECT MAX(salary) FROM employees
inner_query = SelectStatementNode(
    select_list=[
        FunctionCallNode(name="MAX", args=[ColumnNode(name="salary")])
    ],
    from_table=TableNode(name="employees")
)

# Outer query: SELECT * FROM employees WHERE salary = (subquery)
query = SelectStatementNode(
    select_list=[StarNode()],
    from_table=TableNode(name="employees"),
    where_clause=WhereClauseNode(
        condition=BinaryOperationNode(
            left=ColumnNode(name="salary"),
            operator="=",
            right=SubqueryNode(statement=inner_query)
        )
    )
)

results = executor.execute(query)
```

---

## Advanced Topics

### Custom Compiler Implementation

Extend the PostgreSQL or SQLite compiler to support custom SQL dialects or add new nodes:

```python
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler
from buildaquery.abstract_syntax_tree.models import ASTNode

class CustomSQLCompiler(PostgresCompiler):
    def __init__(self):
        super().__init__()
    
    def visit_CustomNode(self, node: CustomNode) -> str:
        # Custom compilation logic
        return self.compile_custom_node(node)
    
    def compile_custom_node(self, node) -> str:
        # Implementation
        pass
```

For SQLite, subclass `SqliteCompiler` instead.

### Dynamic Query Building

Build queries programmatically based on user input:

```python
def build_filter_query(table_name: str, filters: dict) -> SelectStatementNode:
    """Build a SELECT query with dynamic filters."""
    conditions = []
    
    for column, value in filters.items():
        conditions.append(
            BinaryOperationNode(
                left=ColumnNode(name=column),
                operator="=",
                right=LiteralNode(value=value)
            )
        )
    
    # Combine conditions with AND
    where_condition = conditions[0]
    for condition in conditions[1:]:
        where_condition = BinaryOperationNode(
            left=where_condition,
            operator="AND",
            right=condition
        )
    
    return SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name=table_name),
        where_clause=WhereClauseNode(condition=where_condition)
    )

# Usage
query = build_filter_query("users", {"age": 30, "status": "active"})
results = executor.execute(query)
```

### Query Analysis

Analyze queries to extract metadata:

```python
class QueryAnalyzer(Visitor):
    """Analyzes a query to extract metadata."""
    
    def __init__(self):
        self.tables = set()
        self.columns = set()
        self.functions = set()
        
    def visit_TableNode(self, node: TableNode) -> None:
        self.tables.add(node.name)
        self.generic_visit(node)
    
    def visit_ColumnNode(self, node: ColumnNode) -> None:
        self.columns.add(node.name)
        self.generic_visit(node)
    
    def visit_FunctionCallNode(self, node: FunctionCallNode) -> None:
        self.functions.add(node.name)
        self.generic_visit(node)

# Usage
query = SelectStatementNode(...)
analyzer = QueryAnalyzer()
analyzer.visit(query)
print(f"Tables: {analyzer.tables}")
print(f"Columns: {analyzer.columns}")
print(f"Functions: {analyzer.functions}")
```

---

## Testing

The project includes comprehensive unit and integration tests.

### Running Tests

#### Unit Tests
```bash
poetry run pytest buildaquery/tests
```

#### Integration Tests
```bash
# Start the test database
docker-compose up -d

# Run integration tests
poetry run pytest tests
```

SQLite integration tests run against the file-based database at `static/test-sqlite/db.sqlite`.
Oracle integration tests run against the Dockerized Oracle XE container (startup may take a couple of minutes).

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

SQLite integration tests do not require Docker.

#### All Tests
```bash
poetry run all-tests
```

### Writing Tests

Create test files in `tests/` or `buildaquery/tests/`:

```python
import pytest
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, StarNode, TableNode
)

def test_simple_select(executor):
    """Test a simple SELECT query."""
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users")
    )
    
    results = executor.execute(query)
    assert len(results) > 0
```

For SQLite integration tests, use the `sqlite_executor` and `sqlite_create_table` fixtures from `tests/conftest.py`.

---

## Troubleshooting

### Common Issues

#### 1. Module Not Found: `buildaquery`

**Problem**: `ModuleNotFoundError: No module named 'buildaquery'`

**Solution**: Ensure you're running from the project root or have the library installed:
```bash
pip install buildaquery
# OR (from project root)
export PYTHONPATH=.
```

#### 2. Connection Refused

**Problem**: Cannot connect to PostgreSQL database.

**Solution**: Verify database is running:
```bash
# Check if PostgreSQL is running
psql -U postgres -h localhost

# Or start via Docker
docker-compose up -d
```

#### 3. Invalid Environment Variables

**Problem**: Connection string fails with authentication errors.

**Solution**: Check `.env` file:
```bash
# Verify .env exists
ls -la .env

# Verify content
cat .env

# Test connection manually
psql postgresql://user:password@host:port/dbname
```

#### 4. Compilation Errors

**Problem**: AST nodes produce invalid SQL.

**Solution**: Debug the compilation:
```python
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler

compiler = PostgresCompiler()
compiled = compiler.compile(query)
print(compiled.sql)      # Review SQL
print(compiled.params)   # Review parameters
```

For SQLite:
```python
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler

compiler = SqliteCompiler()
compiled = compiler.compile(query)
print(compiled.sql)
print(compiled.params)
```

#### 5. Test Database Setup

**Problem**: Integration tests fail to connect.

**Solution**: Ensure Docker is running and database initialized:
```bash
# Start database
docker-compose up -d

# Verify connection
docker-compose exec postgres psql -U postgres -c "SELECT 1"
```

#### 6. SQLite Database File Issues

**Problem**: SQLite integration tests fail due to missing or unwritable database file.

**Solution**: Ensure the path is writable and create the file if needed:
```bash
./scripts/create_sqlite_db.sh
# or on Windows
.\scripts\create_sqlite_db.ps1
```

#### 7. MySQL Driver Missing

**Problem**: `No module named 'mysql'` when running MySQL integration tests.

**Solution**: Install the driver:
```bash
poetry add mysql-connector-python
# or
pip install mysql-connector-python
```

#### 8. Oracle Driver Missing

**Problem**: `No module named 'oracledb'` when running Oracle integration tests.

**Solution**: Install the driver:
```bash
poetry add oracledb
# or
pip install oracledb
```

#### 9. SQL Server Driver Missing

**Problem**: `No module named 'pyodbc'` when running SQL Server integration tests.

**Solution**: Install the driver:
```bash
poetry add pyodbc
# or
pip install pyodbc
```

### Getting Help

- Check the [README.md](../README.md) for quick start guide
- Review examples in `examples/`
- Examine tests in `tests/` and `buildaquery/tests/`
- Check project issues on GitHub

---

## Project Structure

```
buildaquery/
├── abstract_syntax_tree/     # AST node definitions
│   ├── models.py
│   └── README.md
├── compiler/                 # SQL compilation logic
│   ├── compiled_query.py
│   ├── postgres/
│   │   ├── postgres_compiler.py
│   │   └── README.md
│   ├── mysql/
│   │   ├── mysql_compiler.py
│   │   └── README.md
│   ├── oracle/
│   │   ├── oracle_compiler.py
│   │   └── README.md
│   ├── mssql/
│   │   ├── mssql_compiler.py
│   │   └── README.md
│   ├── sqlite/
│   │   ├── sqlite_compiler.py
│   │   └── README.md
│   └── README.md
├── execution/                # Database execution layer
│   ├── base.py
│   ├── mysql.py
│   ├── mssql.py
│   ├── oracle.py
│   ├── postgres.py
│   ├── sqlite.py
│   └── README.md
├── traversal/                # AST traversal patterns
│   ├── visitor_pattern.py
│   └── README.md
├── tests/                    # Unit tests
│   ├── test_ast.py
│   ├── test_compiler_postgres.py
│   ├── test_compiler_mysql.py
│   ├── test_compiler_oracle.py
│   ├── test_compiler_mssql.py
│   ├── test_compiler_sqlite.py
│   ├── test_execution.py
│   ├── test_execution_mysql.py
│   ├── test_execution_oracle.py
│   ├── test_execution_mssql.py
│   └── test_traversal.py
├── __init__.py
└── __pycache__/

docs/                         # Documentation
├── docs.md                   # This file
└── ...

examples/                     # Example scripts
├── sample_mysql.py
├── sample_mssql.py
├── sample_oracle.py
├── sample_postgres.py
├── sample_sqlite.py
└── .env

tests/                        # Integration tests
├── conftest.py
├── test_mssql_integration.py
├── test_oracle_integration.py
├── test_postgres_integration.py
├── test_sqlite_integration.py
└── README.md

scripts/                      # Utility scripts
├── __init__.py
├── create_sqlite_db.ps1
├── create_sqlite_db.sh
└── ...

pyproject.toml               # Project metadata and dependencies
README.md                    # Project overview
LICENSE.txt                  # License information
docker-compose.yml           # Docker test environment
```

---

## Conclusion

Build-a-Query provides a powerful, extensible foundation for programmatic SQL query construction. By leveraging Python's type system and design patterns, it enables safe, maintainable, and composable query building.

For more information, visit the [README](../README.md) or explore the `examples/` directory.
