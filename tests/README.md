# Integration Tests

This directory contains integration tests that verify the full query lifecycle—from AST construction and compilation to execution against live PostgreSQL, MySQL, MariaDB, CockroachDB, Oracle, and SQL Server databases and a SQLite database file.
It also includes cross-dialect transaction and upsert behavior validation suites.

## Strategy

We use local Docker-based PostgreSQL, MySQL, MariaDB, CockroachDB, Oracle, and SQL Server instances and a file-based SQLite database for integration testing. Each test suite is responsible for its own schema management (creating and dropping tables) to ensure consistency.

### 1. Database Infrastructure
We use Docker to provide consistent PostgreSQL, MySQL, MariaDB, CockroachDB, Oracle, and SQL Server environments without requiring local installations.

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

#### MariaDB
- **Image**: `mariadb:11.4`
- **Port**: `3308` (Mapped to 3306 in container to avoid local conflicts)
- **Host**: `127.0.0.1`
- **Credentials**: Configured via `docker-compose.yml` and `conftest.py`.
- **Default URL**: `mariadb://root:password@127.0.0.1:3308/buildaquery_test` (override with `MARIADB_DATABASE_URL`).

#### CockroachDB
- **Image**: `cockroachdb/cockroach:v24.3.1`
- **Port**: `26258` (SQL listens on `26258` inside the container to avoid local conflicts)
- **Host**: `127.0.0.1`
- **Credentials**: `root` user, insecure mode (for local testing only)
- **Default URL**: `postgresql://root@127.0.0.1:26258/buildaquery_test?sslmode=disable` (override with `COCKROACH_DATABASE_URL`).

#### Oracle
- **Image**: `gvenzl/oracle-xe:21-slim`
- **Port**: `1522` (Mapped to 1521 in container to avoid local conflicts)
- **Host**: `127.0.0.1`
- **Credentials**: Configured via `docker-compose.yml` and `conftest.py`.
- **Default URL**: `oracle://buildaquery:password@127.0.0.1:1522/XEPDB1` (override with `ORACLE_DATABASE_URL`).

#### SQL Server
- **Image**: `mcr.microsoft.com/mssql/server:2022-latest` (Express)
- **Port**: `1434` (Mapped to 1433 in container to avoid local conflicts)
- **Host**: `127.0.0.1`
- **Credentials**: Configured via `docker-compose.yml` and `conftest.py`.
- **Default URL**: `mssql://sa:Password%21@127.0.0.1:1434/buildaquery_test?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes` (override with `MSSQL_DATABASE_URL`).

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
- [x] **Oracle Integration Test**: Create `tests/test_oracle_integration.py` using the Dockerized Oracle XE database.
- [x] **SQL Server Integration Test**: Create `tests/test_mssql_integration.py` using the Dockerized SQL Server Express database.
- [x] **MariaDB Integration Test**: Create `tests/test_mariadb_integration.py` using the Dockerized MariaDB database.
- [x] **CockroachDB Integration Test**: Create `tests/test_cockroach_integration.py` using the Dockerized CockroachDB database.
- [x] **Transaction Integration Test**: Create `tests/test_transaction_integration.py` for cross-dialect transaction APIs.
- [x] **Upsert Integration Test**: Create `tests/test_upsert_integration.py` for `ON CONFLICT`/`ON DUPLICATE KEY UPDATE` behavior.

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

## Oracle Details

- **Driver**: `oracledb` (required for Oracle integration tests).
- **Default URL**: `oracle://buildaquery:password@127.0.0.1:1522/XEPDB1` (override with `ORACLE_DATABASE_URL`).

## SQL Server Details

- **Driver**: `pyodbc` (required for SQL Server integration tests).
- **Default URL**: `mssql://sa:Password%21@127.0.0.1:1434/buildaquery_test?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes` (override with `MSSQL_DATABASE_URL`).

## MariaDB Details

- **Driver**: `mariadb` (required for MariaDB integration tests).
- **Default URL**: `mariadb://root:password@127.0.0.1:3308/buildaquery_test` (override with `MARIADB_DATABASE_URL`).

## CockroachDB Details

- **Driver**: `psycopg` (required for CockroachDB integration tests).
- **Default URL**: `postgresql://root@127.0.0.1:26258/buildaquery_test?sslmode=disable` (override with `COCKROACH_DATABASE_URL`).
