# Integration Tests

This directory contains integration tests that verify the full query lifecycle—from AST construction and compilation to execution against live PostgreSQL and MySQL databases and a SQLite database file.

## Strategy

We use local Docker-based PostgreSQL and MySQL instances and a file-based SQLite database for integration testing. Each test suite is responsible for its own schema management (creating and dropping tables) to ensure consistency.

### 1. Database Infrastructure
We use Docker to provide consistent PostgreSQL and MySQL environments without requiring local installations.

- **Image**: `postgres:15`
- **Port**: `5433` (Mapped to 5432 in container to avoid local conflicts)
- **Host**: `127.0.0.1` (Explicitly used to avoid IPv6 issues)
- **Credentials**: Configured via `docker-compose.yml` and `conftest.py`.

#### MySQL
- **Image**: `mysql:8.0`
- **Port**: `3307` (Mapped to 3306 in container to avoid local conflicts)
- **Host**: `127.0.0.1` (Explicitly used to avoid IPv6 issues)
- **Credentials**: Configured via `docker-compose.yml` and `conftest.py`.
- **Default URL**: `mysql://root:password@127.0.0.1:3307/buildaquery_test` (override with `MYSQL_DATABASE_URL`).

### 2. Schema Lifecycle
The tests themselves handle table creation and cleanup using `pytest` fixtures.

- **Setup**: A fixture runs `CREATE TABLE` before the test(s) in a module.
- **Teardown**: The same fixture runs `DROP TABLE` after the test(s) to leave the database clean.

## TODOs

- [x] **Docker Setup**: Create a `docker-compose.yml` in the root to easily start the test databases.
- [x] **Environment**: Define default `DATABASE_URL` and `MYSQL_DATABASE_URL` values for local testing.
- [x] **Fixtures**: Implement `tests/conftest.py` with:
    - `db_connection`: Provides a connected `psycopg` session.
    - `table_manager`: A fixture that takes DDL as input, creates the table, yields to the test, and drops the table on completion.
- [x] **Initial Integration Test**: Create `tests/test_postgres_integration.py` to test the full "Create -> Insert -> Select -> Update -> Delete -> Drop" lifecycle.
- [x] **MySQL Integration Test**: Create `tests/test_mysql_integration.py` with the same lifecycle coverage.
- [x] **SQLite Integration Test**: Create `tests/test_sqlite_integration.py` using the file-based SQLite database at `static/test-sqlite/db.sqlite`.

## How to Run

1. **Start the Database**:
   ```bash
   docker-compose up -d
   ```

2. **Run the Tests**:
   ```bash
   $env:PYTHONPATH='.'; .\venv\Scripts\python.exe -m pytest tests
   ```

## SQLite Details

- **SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).
- **Database File**: `static/test-sqlite/db.sqlite` (created and reset automatically by the test fixtures).

## MySQL Details

- **Driver**: `mysql-connector-python` (required for MySQL integration tests).
- **Default URL**: `mysql://root:password@127.0.0.1:3307/buildaquery_test` (override with `MYSQL_DATABASE_URL`).
