# Build-a-Query

A Python-based query builder designed to represent, compile, and execute SQL queries using a dialect-agnostic Abstract Syntax Tree (AST). Supports PostgreSQL, SQLite, MySQL, MariaDB, Oracle, and SQL Server.

## Features

- **Dialect-Agnostic AST**: Build queries using high-level Python objects.
- **Full DML Support**: Create `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements.
- **Advanced Querying**: Support for CTEs (`WITH`), Subqueries, Set Operations (`UNION`, `INTERSECT`, `EXCEPT`), and Window Functions (`OVER`).
- **Rich Expression Logic**: Includes `CASE` expressions, `IN`, `BETWEEN`, and type casting.
- **DDL Support**: Basic schema management with `CREATE TABLE` and `DROP TABLE`.
- **Visitor Pattern Traversal**: Extensible architecture for analysis and compilation.
- **Secure Compilation**: Automatic parameterization to prevent SQL injection.
- **Execution Layer**: Built-in support for executing compiled queries via `psycopg` (PostgreSQL), `mysql-connector-python` (MySQL), `mariadb` (MariaDB), `oracledb` (Oracle), `pyodbc` (SQL Server), and the standard library `sqlite3` (SQLite).

## Dialect Notes
- MySQL does not support `INTERSECT` / `EXCEPT` or `DROP TABLE ... CASCADE` in this implementation (the compiler raises `ValueError`).
- SQLite does not support `DROP TABLE ... CASCADE` (the compiler raises `ValueError`).
- Oracle does not support `IF EXISTS` / `IF NOT EXISTS` in `DROP TABLE`/`CREATE TABLE` (the compiler raises `ValueError`), and `EXCEPT` is compiled as `MINUS`.
- SQL Server does not support `EXCEPT ALL` / `INTERSECT ALL` or `DROP TABLE ... CASCADE` in this implementation (the compiler raises `ValueError`).
- MariaDB supports `INTERSECT` / `EXCEPT` (including `ALL`), and accepts `DROP TABLE ... CASCADE` (treated as a no-op).

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
- **MySQL database**: A running MySQL instance (version 8.0+ recommended).
  - Example with Docker: `docker run --name mysql -e MYSQL_ROOT_PASSWORD=yourpassword -e MYSQL_DATABASE=buildaquery -d -p 3306:3306 mysql:8.0`
- `mysql-connector-python` (automatically installed as a dependency) - the MySQL adapter for Python.
- **MariaDB database**: A running MariaDB instance (MariaDB 10.3+ recommended).
  - Example with Docker (MariaDB): `docker run --name mariadb -e MARIADB_ROOT_PASSWORD=yourpassword -e MARIADB_DATABASE=buildaquery -d -p 3306:3306 mariadb:11.4`
- `mariadb` (automatically installed as a dependency) - the MariaDB adapter for Python.
- **Oracle database**: A running Oracle instance (Oracle XE is suitable for development).
  - Example with Docker (Oracle XE): `docker run --name oracle-xe -e ORACLE_PASSWORD=yourpassword -e APP_USER=buildaquery -e APP_USER_PASSWORD=yourpassword -d -p 1521:1521 gvenzl/oracle-xe:21-slim`
- `oracledb` (automatically installed as a dependency) - the Oracle adapter for Python.
- **SQL Server database**: A running SQL Server instance (Express is suitable for development).
  - Example with Docker (SQL Server Express): `docker run --name sqlserver -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=yourpassword -e MSSQL_PID=Express -d -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest`
- `pyodbc` (automatically installed as a dependency) - the SQL Server adapter for Python.
- `python-dotenv` (automatically installed as a dependency) - for loading environment variables from a `.env` file.
- **SQLite**: Uses Python's standard library `sqlite3` module.
  - **SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

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

For MySQL, you can use a connection URL directly in code (e.g., `mysql://user:password@host:3306/dbname`) or set your own environment variables and construct the URL similarly.

For Oracle, use a connection URL in the format `oracle://user:password@host:port/service_name` (for example: `oracle://buildaquery:password@127.0.0.1:1521/XEPDB1`).

For SQL Server, use a connection URL in the format `mssql://user:password@host:port/dbname?driver=...` (for example: `mssql://sa:password@127.0.0.1:1433/buildaquery?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes`).

For MariaDB, use a connection URL in the format `mariadb://user:password@host:port/dbname` (for example: `mariadb://root:password@127.0.0.1:3306/buildaquery`).

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

### SQLite Quick Start

```python
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

executor = SqliteExecutor(connection_info="static/test-sqlite/db.sqlite")

users_table = TableNode(name="users")
create_stmt = CreateStatementNode(
    table=users_table,
    columns=[
        ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
        ColumnDefinitionNode(name="name", data_type="TEXT", not_null=True),
        ColumnDefinitionNode(name="age", data_type="INTEGER")
    ]
)
executor.execute(create_stmt)

insert_stmt = InsertStatementNode(
    table=users_table,
    columns=[ColumnNode(name="name"), ColumnNode(name="age")],
    values=[LiteralNode(value="Alice"), LiteralNode(value=30)]
)
executor.execute(insert_stmt)

select_stmt = SelectStatementNode(
    select_list=[StarNode()],
    from_table=users_table
)
print(executor.execute(select_stmt))

drop_stmt = DropStatementNode(table=users_table, if_exists=True)
executor.execute(drop_stmt)
```

### MySQL Quick Start

```python
from buildaquery.execution.mysql import MySqlExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

executor = MySqlExecutor(connection_info="mysql://root:password@127.0.0.1:3306/buildaquery")

users_table = TableNode(name="users")
create_stmt = CreateStatementNode(
    table=users_table,
    columns=[
        ColumnDefinitionNode(name="id", data_type="INT AUTO_INCREMENT", primary_key=True),
        ColumnDefinitionNode(name="name", data_type="VARCHAR(255)", not_null=True),
        ColumnDefinitionNode(name="age", data_type="INT")
    ]
)
executor.execute(create_stmt)

insert_stmt = InsertStatementNode(
    table=users_table,
    columns=[ColumnNode(name="name"), ColumnNode(name="age")],
    values=[LiteralNode(value="Alice"), LiteralNode(value=30)]
)
executor.execute(insert_stmt)

select_stmt = SelectStatementNode(
    select_list=[StarNode()],
    from_table=users_table
)
print(executor.execute(select_stmt))

drop_stmt = DropStatementNode(table=users_table, if_exists=True)
executor.execute(drop_stmt)
```

### Oracle Quick Start

```python
from buildaquery.execution.oracle import OracleExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

executor = OracleExecutor(connection_info="oracle://buildaquery:password@127.0.0.1:1521/XEPDB1")

users_table = TableNode(name="users")
create_stmt = CreateStatementNode(
    table=users_table,
    columns=[
        ColumnDefinitionNode(name="id", data_type="NUMBER", primary_key=True),
        ColumnDefinitionNode(name="name", data_type="VARCHAR2(255)", not_null=True),
        ColumnDefinitionNode(name="age", data_type="NUMBER")
    ]
)
executor.execute(create_stmt)

insert_stmt = InsertStatementNode(
    table=users_table,
    columns=[ColumnNode(name="id"), ColumnNode(name="name"), ColumnNode(name="age")],
    values=[LiteralNode(value=1), LiteralNode(value="Alice"), LiteralNode(value=30)]
)
executor.execute(insert_stmt)

select_stmt = SelectStatementNode(
    select_list=[StarNode()],
    from_table=users_table
)
print(executor.execute(select_stmt))

drop_stmt = DropStatementNode(table=users_table, cascade=True)
executor.execute(drop_stmt)
```

### SQL Server Quick Start

```python
from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

executor = MsSqlExecutor(connection_info="mssql://sa:password@127.0.0.1:1433/buildaquery?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes")

users_table = TableNode(name="users")
create_stmt = CreateStatementNode(
    table=users_table,
    columns=[
        ColumnDefinitionNode(name="id", data_type="INT", primary_key=True),
        ColumnDefinitionNode(name="name", data_type="NVARCHAR(255)", not_null=True),
        ColumnDefinitionNode(name="age", data_type="INT")
    ]
)
executor.execute(create_stmt)

insert_stmt = InsertStatementNode(
    table=users_table,
    columns=[ColumnNode(name="id"), ColumnNode(name="name"), ColumnNode(name="age")],
    values=[LiteralNode(value=1), LiteralNode(value="Alice"), LiteralNode(value=30)]
)
executor.execute(insert_stmt)

select_stmt = SelectStatementNode(
    select_list=[StarNode()],
    from_table=users_table
)
print(executor.execute(select_stmt))

drop_stmt = DropStatementNode(table=users_table, if_exists=True)
executor.execute(drop_stmt)
```

### MariaDB Quick Start

```python
from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

executor = MariaDbExecutor(connection_info="mariadb://root:password@127.0.0.1:3306/buildaquery")

users_table = TableNode(name="users")
create_stmt = CreateStatementNode(
    table=users_table,
    columns=[
        ColumnDefinitionNode(name="id", data_type="INT AUTO_INCREMENT", primary_key=True),
        ColumnDefinitionNode(name="name", data_type="VARCHAR(255)", not_null=True),
        ColumnDefinitionNode(name="age", data_type="INT")
    ]
)
executor.execute(create_stmt)

insert_stmt = InsertStatementNode(
    table=users_table,
    columns=[ColumnNode(name="name"), ColumnNode(name="age")],
    values=[LiteralNode(value="Alice"), LiteralNode(value=30)]
)
executor.execute(insert_stmt)

select_stmt = SelectStatementNode(
    select_list=[StarNode()],
    from_table=users_table
)
print(executor.execute(select_stmt))

drop_stmt = DropStatementNode(table=users_table, if_exists=True, cascade=True)
executor.execute(drop_stmt)
```

For more examples, see the `examples/` directory (including `examples/sample_mysql.py`, `examples/sample_oracle.py`, `examples/sample_mssql.py`, and `examples/sample_mariadb.py`).

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

Integration tests require PostgreSQL, MySQL, MariaDB, Oracle, and SQL Server databases (and the respective drivers). Start the test databases using Docker:

```bash
docker-compose up -d
```

Then run integration tests:

```bash
poetry run pytest tests
```

SQLite integration tests use the file-based database at `static/test-sqlite/db.sqlite`.

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
- `buildaquery/compiler/`: Dialect-specific SQL generation (PostgreSQL, SQLite, MySQL, MariaDB, Oracle, SQL Server).
- `buildaquery/execution/`: Database connection and execution logic.
- `tests/`: Exhaustive unit and integration tests.
- `examples/`: Practical demonstrations of the library.
- `scripts/`: Utility scripts for testing and maintenance.

## Contributing

Contributions are welcome! Please see the contributing guidelines for more information.

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.
