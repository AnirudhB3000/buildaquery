# Build-a-Query

A Python-based query builder designed to represent, compile, and execute SQL queries using a dialect-agnostic Abstract Syntax Tree (AST). Initial support is focused on PostgreSQL.

## Core Features

- **Dialect-Agnostic AST**: Build queries using high-level Python objects.
- **Visitor Pattern Traversal**: Extensible architecture for analysis and compilation.
- **Secure Compilation**: Automatic parameterization to prevent SQL injection.
- **Execution Layer**: Built-in support for executing compiled queries via `psycopg`.

## Getting Started

### Prerequisites

- Python 3.12+
- Virtual environment (provided in `venv/`)

### Running Tests

To execute the exhaustive test suite, run the following command from the root directory (using PowerShell on Windows):

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
