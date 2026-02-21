# Build-a-Query

A Python-based query builder designed to represent, compile, and execute SQL queries using a dialect-agnostic Abstract Syntax Tree (AST). Initial support is focused on PostgreSQL.

## Core Features

- **Dialect-Agnostic AST**: Build queries using high-level Python objects.
- **Full DML Support**: Create `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements.
- **Advanced Querying**: Support for CTEs (`WITH`), Subqueries, Set Operations (`UNION`, `INTERSECT`, `EXCEPT`), and Window Functions (`OVER`).
- **Rich Expression Logic**: Includes `CASE` expressions, `IN`, `BETWEEN`, and type casting.
- **DDL Support**: Basic schema management with `CREATE TABLE` and `DROP TABLE`.
- **Visitor Pattern Traversal**: Extensible architecture for analysis and compilation.
- **Secure Compilation**: Automatic parameterization to prevent SQL injection.
- **Execution Layer**: Built-in support for executing compiled queries via `psycopg`.

## Getting Started

### Prerequisites

- Python 3.12+
- Virtual environment (provided in `venv/`)

### Running Tests

To execute the unit test suite, run the following command from the root directory:

```powershell
$env:PYTHONPATH='.'; .\venv\Scripts\python.exe -m pytest buildaquery/tests
```

To run integration tests, you must first start the test database:

```powershell
docker-compose up -d
```

Then run the integration suite:

```powershell
$env:PYTHONPATH='.'; .\venv\Scripts\python.exe -m pytest tests
```

### Running the Example

A demonstration of the query builder and compiler can be found in the `examples/` directory:

```powershell
$env:PYTHONPATH='.'; .\venv\Scripts\python.exe examples/postgres_demo.py
```

## Project Structure

- `buildaquery/abstract_syntax_tree/`: Defines query nodes.
- `buildaquery/traversal/`: Base classes for AST traversal (Visitor/Transformer).
- `buildaquery/compiler/`: Dialect-specific SQL generation.
- `buildaquery/execution/`: Database connection and execution logic.
- `tests/`: Exhaustive unit tests for all modules.
- `examples/`: Practical demonstrations of the library.
