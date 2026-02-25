import os
import time
import pytest
import psycopg
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor

# Use Docker credentials by default, but allow override via environment variable
# Use 127.0.0.1 to avoid potential IPv6 resolution issues on some systems
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", os.path.join("static", "test-sqlite", "db.sqlite"))

@pytest.fixture(scope="session")
def db_connection():
    """
    Yields a connection to the test database.
    This fixture ensures the connection is established once per test session.
    It retries for up to 10 seconds to allow the database container to start up.
    """
    retries = 10
    while retries > 0:
        try:
            conn = psycopg.connect(DATABASE_URL)
            yield conn
            conn.close()
            return
        except psycopg.OperationalError:
            retries -= 1
            time.sleep(1)
    
    pytest.fail(f"Could not connect to test database at {DATABASE_URL} after 10 seconds. Is Docker running?")

@pytest.fixture(scope="function")
def executor(db_connection):
    """
    Yields a PostgresExecutor instance for running queries.
    This fixture ensures a fresh transaction for each test function.
    """
    executor = PostgresExecutor(connection=db_connection)
    yield executor
    # No explicit rollback needed here as psycopg handles transaction management,
    # but for full isolation, we could add db_connection.rollback() here if needed.

@pytest.fixture(scope="function")
def create_table(executor):
    """
    A fixture that creates a table and ensures it is dropped after the test.
    This fixture expects a tuple of (create_statement_node, table_name) or just the node.
    """
    created_tables = []

    def _create(create_node):
        executor.execute(create_node)
        created_tables.append(create_node.table.name)
        return create_node.table.name

    yield _create

    # Teardown: Drop all tables created by this test
    for table_name in reversed(created_tables):
        # We construct a raw SQL string for simplicity in cleanup, 
        # or we could construct a DropStatementNode if we wanted to be pure AST.
        executor.execute_raw(f"DROP TABLE IF EXISTS {table_name} CASCADE")

@pytest.fixture(scope="session")
def sqlite_connection():
    """
    Yields a sqlite3 connection to the test database file.
    The database file is created if missing and reset at the start of the session.
    """
    db_dir = os.path.dirname(SQLITE_DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    if os.path.exists(SQLITE_DB_PATH):
        os.remove(SQLITE_DB_PATH)

    import sqlite3
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()

@pytest.fixture(scope="function")
def sqlite_executor(sqlite_connection):
    """
    Yields a SqliteExecutor instance for running queries.
    """
    executor = SqliteExecutor(connection=sqlite_connection)
    yield executor

@pytest.fixture(scope="function")
def sqlite_create_table(sqlite_executor):
    """
    A fixture that creates a table and ensures it is dropped after the test.
    This fixture expects a tuple of (create_statement_node, table_name) or just the node.
    """
    created_tables = []

    def _create(create_node):
        sqlite_executor.execute(create_node)
        created_tables.append(create_node.table.name)
        return create_node.table.name

    yield _create

    for table_name in reversed(created_tables):
        sqlite_executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")
