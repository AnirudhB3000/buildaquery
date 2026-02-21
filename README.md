# Build-a-Query

A Python-based query builder designed to represent, compile, and execute SQL queries using a dialect-agnostic Abstract Syntax Tree (AST). Initial support is focused on PostgreSQL.

## Features

- **Dialect-Agnostic AST**: Build queries using high-level Python objects.
- **Full DML Support**: Create `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements.
- **Advanced Querying**: Support for CTEs (`WITH`), Subqueries, Set Operations (`UNION`, `INTERSECT`, `EXCEPT`), and Window Functions (`OVER`).
- **Rich Expression Logic**: Includes `CASE` expressions, `IN`, `BETWEEN`, and type casting.
- **DDL Support**: Basic schema management with `CREATE TABLE` and `DROP TABLE`.
- **Visitor Pattern Traversal**: Extensible architecture for analysis and compilation.
- **Secure Compilation**: Automatic parameterization to prevent SQL injection.
- **Execution Layer**: Built-in support for executing compiled queries via `psycopg`.

## Installation

### For Users

Install Build-a-Query via pip:

```bash
pip install buildaquery
```

**Requirements:**
- Python 3.12+
- **PostgreSQL database**: A running PostgreSQL instance (version 12+ recommended). You can set this up locally, via Docker, or use a cloud service.
  - Example with Docker: `docker run --name postgres -e POSTGRES_PASSWORD=yourpassword -d -p 5432:5432 postgres:15`
- `psycopg` (automatically installed as a dependency) - the PostgreSQL adapter for Python.
- `python-dotenv` (automatically installed as a dependency) - for loading environment variables from a `.env` file.

### Environment Variables

To connect to your PostgreSQL database, set the following environment variables (or use a `.env` file with `python-dotenv`):

- `DB_HOST`: PostgreSQL host (e.g., `localhost`)
- `DB_PORT`: PostgreSQL port (e.g., `5432`)
- `DB_NAME`: Database name (e.g., `mydatabase`)
- `DB_USER`: Database username (e.g., `postgres`)
- `DB_PASSWORD`: Database password (e.g., `yourpassword`)

Example `.env` file:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=buildaquery
DB_USER=postgres
DB_PASSWORD=yourpassword
```

### For Developers

Clone the repository and set up the development environment:

```bash
git clone https://github.com/yourusername/buildaquery.git
cd buildaquery
```

Install dependencies using Poetry:

```bash
poetry install
```

Activate the virtual environment:

```bash
poetry shell
```

## Quick Start

Here's a simple example of creating a table, inserting data, querying it, and dropping the table. This example uses environment variables for database connection (see Environment Variables section above).

```python
from dotenv import load_dotenv
import os
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

# Load environment variables
load_dotenv()

# Build connection string from environment variables
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Set up executor with your PostgreSQL connection
executor = PostgresExecutor(connection_info=connection_string)

# Define table
users_table = TableNode(name="users")

# Create table
create_stmt = CreateStatementNode(
    table=users_table,
    columns=[
        ColumnDefinitionNode(name="id", data_type="SERIAL", primary_key=True),
        ColumnDefinitionNode(name="name", data_type="TEXT", not_null=True),
        ColumnDefinitionNode(name="age", data_type="INTEGER")
    ]
)
executor.execute(create_stmt)

# Insert data
insert_stmt = InsertStatementNode(
    table=users_table,
    columns=[ColumnNode(name="name"), ColumnNode(name="age")],
    values=[LiteralNode(value="Alice"), LiteralNode(value=30)]
)
executor.execute(insert_stmt)

# Query data
select_stmt = SelectStatementNode(
    select_list=[StarNode()],  # SELECT *
    from_table=users_table
)
results = executor.execute(select_stmt)
print(results)  # [(1, 'Alice', 30)]

# Drop table
drop_stmt = DropStatementNode(table=users_table, if_exists=True)
executor.execute(drop_stmt)
```

For more examples, see the `examples/` directory.

## Development Setup

### Prerequisites

- Python 3.12+
- Poetry (for dependency management)
- Docker (for running integration tests)

### Setting Up the Environment

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/buildaquery.git
   cd buildaquery
   ```

2. Install dependencies:
   ```bash
   poetry install
   ```

3. Activate the virtual environment:
   ```bash
   poetry shell
   ```

### Running Tests

#### Unit Tests

Run unit tests for all modules:

```bash
poetry run pytest buildaquery/tests
```

#### Integration Tests

Integration tests require a PostgreSQL database. Start the test database using Docker:

```bash
docker-compose up -d
```

Then run integration tests:

```bash
poetry run pytest tests
```

#### All Tests

Run all tests (unit and integration):

```bash
poetry run all-tests
```

### Running Examples

Execute the sample script:

```bash
poetry run python examples/sample_query.py
```

## Project Structure

- `buildaquery/abstract_syntax_tree/`: Defines query nodes and AST models.
- `buildaquery/traversal/`: Base classes for AST traversal (Visitor/Transformer pattern).
- `buildaquery/compiler/`: Dialect-specific SQL generation (currently PostgreSQL).
- `buildaquery/execution/`: Database connection and execution logic.
- `tests/`: Exhaustive unit and integration tests.
- `examples/`: Practical demonstrations of the library.
- `scripts/`: Utility scripts for testing and maintenance.

## Contributing

Contributions are welcome! Please see the contributing guidelines for more information.

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.
