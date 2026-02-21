import os
import time
import pytest
import psycopg
from buildaquery.execution.postgres import PostgresExecutor

# Use Docker credentials by default, but allow override via environment variable
# Use 127.0.0.1 to avoid potential IPv6 resolution issues on some systems
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")

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
