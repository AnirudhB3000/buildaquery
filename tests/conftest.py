import os
import time
import warnings
import pytest
import psycopg
from urllib.parse import urlparse, unquote, parse_qs

warnings.filterwarnings(
    "ignore",
    message=r"invalid escape sequence '\\\*'",
    category=SyntaxWarning,
    module=r"mariadb\.connectionpool"
)
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.execution.mysql import MySqlExecutor
from buildaquery.execution.oracle import OracleExecutor
from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.execution.cockroachdb import CockroachExecutor

# Use Docker credentials by default, but allow override via environment variable
# Use 127.0.0.1 to avoid potential IPv6 resolution issues on some systems
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", os.path.join("static", "test-sqlite", "db.sqlite"))
MYSQL_DATABASE_URL = os.getenv("MYSQL_DATABASE_URL", "mysql://root:password@127.0.0.1:3307/buildaquery_test")
ORACLE_DATABASE_URL = os.getenv("ORACLE_DATABASE_URL", "oracle://buildaquery:password@127.0.0.1:1522/XEPDB1")
MSSQL_DATABASE_URL = os.getenv("MSSQL_DATABASE_URL", "mssql://sa:Password%21@127.0.0.1:1434/buildaquery_test?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no&trust_server_certificate=yes")
MARIADB_DATABASE_URL = os.getenv("MARIADB_DATABASE_URL", "mariadb://root:password@127.0.0.1:3308/buildaquery_test")
COCKROACH_DATABASE_URL = os.getenv(
    "COCKROACH_DATABASE_URL",
    "postgresql://root@127.0.0.1:26258/buildaquery_test?sslmode=disable"
)

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

def _parse_mysql_url(url: str) -> dict[str, str | int | None]:
    parsed = urlparse(url)
    if parsed.scheme != "mysql":
        raise ValueError("MySQL connection string must start with mysql://")

    return {
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 3306,
        "database": parsed.path.lstrip("/") if parsed.path else None
    }

def _parse_oracle_url(url: str) -> dict[str, str | int | None]:
    parsed = urlparse(url)
    if parsed.scheme != "oracle":
        raise ValueError("Oracle connection string must start with oracle://")

    service_name = parsed.path.lstrip("/") if parsed.path else None
    if not service_name:
        raise ValueError("Oracle connection string must include a service name (e.g., /XEPDB1).")

    config = {
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 1521,
        "service_name": service_name
    }
    return {key: value for key, value in config.items() if value is not None}

def _parse_mssql_url(url: str) -> dict[str, str | int | None]:
    parsed = urlparse(url)
    if parsed.scheme != "mssql":
        raise ValueError("SQL Server connection string must start with mssql://")

    query = dict((key, values[0]) for key, values in parse_qs(parsed.query).items())
    return {
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 1433,
        "database": parsed.path.lstrip("/") if parsed.path else None,
        "driver": query.get("driver", "ODBC Driver 18 for SQL Server"),
        "encrypt": query.get("encrypt", "no"),
        "trust_server_certificate": query.get("trust_server_certificate", "yes")
    }

def _parse_mariadb_url(url: str) -> dict[str, str | int | None]:
    parsed = urlparse(url)
    if parsed.scheme != "mariadb":
        raise ValueError("MariaDB connection string must start with mariadb://")

    return {
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 3306,
        "database": parsed.path.lstrip("/") if parsed.path else None
    }

@pytest.fixture(scope="session")
def cockroach_connection():
    """
    Yields a connection to the CockroachDB test database.
    Retries for up to 60 seconds to allow the container to start.
    """
    retries = 60
    last_error: Exception | None = None
    parsed = urlparse(COCKROACH_DATABASE_URL)
    database = parsed.path.lstrip("/") if parsed.path else "buildaquery_test"
    sslmode = parse_qs(parsed.query).get("sslmode", ["disable"])[0]
    admin_host = parsed.hostname or "127.0.0.1"
    admin_port = parsed.port or 26257
    admin_url = f"postgresql://root@{admin_host}:{admin_port}/defaultdb?sslmode={sslmode}"

    while retries > 0:
        try:
            import psycopg
            admin_conn = psycopg.connect(admin_url)
            admin_conn.autocommit = True
            with admin_conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            admin_conn.close()
            break
        except Exception as exc:
            last_error = exc
            retries -= 1
            time.sleep(1)
    else:
        error_details = f" Last error: {last_error}" if last_error else ""
        pytest.fail(
            f"Could not connect to CockroachDB test database at {COCKROACH_DATABASE_URL} after 60 seconds. Is Docker running?{error_details}"
        )

    retries = 60
    while retries > 0:
        try:
            import psycopg
            conn = psycopg.connect(COCKROACH_DATABASE_URL)
            conn.autocommit = True
            yield conn
            conn.close()
            return
        except Exception as exc:
            last_error = exc
            retries -= 1
            time.sleep(1)

    error_details = f" Last error: {last_error}" if last_error else ""
    pytest.fail(
        f"Could not connect to CockroachDB test database at {COCKROACH_DATABASE_URL} after 60 seconds. Is Docker running?{error_details}"
    )

@pytest.fixture(scope="function")
def cockroach_executor(cockroach_connection):
    """
    Yields a CockroachExecutor instance for running queries.
    """
    executor = CockroachExecutor(connection=cockroach_connection)
    yield executor

@pytest.fixture(scope="function")
def cockroach_create_table(cockroach_executor):
    """
    A fixture that creates a table and ensures it is dropped after the test.
    """
    created_tables = []

    def _create(create_node):
        cockroach_executor.execute(create_node)
        created_tables.append(create_node.table.name)
        return create_node.table.name

    yield _create

    for table_name in reversed(created_tables):
        cockroach_executor.execute_raw(f"DROP TABLE IF EXISTS {table_name} CASCADE")

@pytest.fixture(scope="session")
def mysql_connection():
    """
    Yields a connection to the MySQL test database.
    Retries for up to 10 seconds to allow the container to start.
    """
    retries = 120
    last_error: Exception | None = None
    while retries > 0:
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                **_parse_mysql_url(MYSQL_DATABASE_URL),
                auth_plugin="mysql_native_password",
                connection_timeout=3
            )
            conn.autocommit = True
            conn.ping(reconnect=True, attempts=3, delay=1)
            yield conn
            conn.close()
            return
        except Exception as exc:
            last_error = exc
            retries -= 1
            time.sleep(1)

    error_details = f" Last error: {last_error}" if last_error else ""
    pytest.fail(
        f"Could not connect to MySQL test database at {MYSQL_DATABASE_URL} after 30 seconds. Is Docker running?{error_details}"
    )

@pytest.fixture(scope="function")
def mysql_executor(mysql_connection):
    """
    Yields a MySqlExecutor instance for running queries.
    """
    executor = MySqlExecutor(connection=mysql_connection)
    yield executor

@pytest.fixture(scope="function")
def mysql_create_table(mysql_executor):
    """
    A fixture that creates a table and ensures it is dropped after the test.
    """
    created_tables = []

    def _create(create_node):
        mysql_executor.execute(create_node)
        created_tables.append(create_node.table.name)
        return create_node.table.name

    yield _create

    for table_name in reversed(created_tables):
        mysql_executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")

@pytest.fixture(scope="session")
def oracle_connection():
    """
    Yields a connection to the Oracle test database.
    Retries for up to 180 seconds to allow the container to start.
    """
    retries = 180
    last_error: Exception | None = None
    while retries > 0:
        try:
            import oracledb
            conn = oracledb.connect(**_parse_oracle_url(ORACLE_DATABASE_URL))
            if hasattr(conn, "autocommit"):
                conn.autocommit = True
            yield conn
            conn.close()
            return
        except Exception as exc:
            last_error = exc
            retries -= 1
            time.sleep(1)

    error_details = f" Last error: {last_error}" if last_error else ""
    pytest.fail(
        f"Could not connect to Oracle test database at {ORACLE_DATABASE_URL} after 180 seconds. Is Docker running?{error_details}"
    )

@pytest.fixture(scope="function")
def oracle_executor(oracle_connection):
    """
    Yields an OracleExecutor instance for running queries.
    """
    executor = OracleExecutor(connection=oracle_connection)
    yield executor

@pytest.fixture(scope="function")
def oracle_create_table(oracle_executor):
    """
    A fixture that creates a table and ensures it is dropped after the test.
    """
    created_tables = []

    def _create(create_node):
        oracle_executor.execute(create_node)
        created_tables.append(create_node.table.name)
        return create_node.table.name

    yield _create

    for table_name in reversed(created_tables):
        drop_block = (
            "BEGIN "
            f"EXECUTE IMMEDIATE 'DROP TABLE {table_name}'; "
            "EXCEPTION WHEN OTHERS THEN "
            "IF SQLCODE != -942 THEN RAISE; END IF; "
            "END;"
        )
        oracle_executor.execute_raw(drop_block)

@pytest.fixture(scope="session")
def mssql_connection():
    """
    Yields a connection to the SQL Server test database.
    Retries for up to 30 seconds to allow the container to start.
    """
    retries = 30
    last_error: Exception | None = None
    config = _parse_mssql_url(MSSQL_DATABASE_URL)
    database = config.get("database") or "master"
    config["database"] = "master"
    conn_str = MsSqlExecutor(connection_info=config)._parse_connection_info()

    while retries > 0:
        try:
            import pyodbc
            conn = pyodbc.connect(conn_str, autocommit=True, timeout=5)
            cur = conn.cursor()
            cur.execute("SELECT DB_ID(?)", database)
            exists = cur.fetchone()[0] is not None
            if not exists:
                cur.execute(f"CREATE DATABASE {database}")
            cur.close()
            conn.close()
            break
        except Exception as exc:
            last_error = exc
            retries -= 1
            time.sleep(1)
    else:
        error_details = f" Last error: {last_error}" if last_error else ""
        pytest.fail(
            f"Could not connect to SQL Server test database at {MSSQL_DATABASE_URL} after 120 seconds. Is Docker running?{error_details}"
        )

    config["database"] = database
    conn_str = MsSqlExecutor(connection_info=config)._parse_connection_info()
    retries = 120
    while retries > 0:
        try:
            import pyodbc
            conn = pyodbc.connect(conn_str, autocommit=True, timeout=5)
            yield conn
            conn.close()
            return
        except Exception as exc:
            last_error = exc
            retries -= 1
            time.sleep(1)

    error_details = f" Last error: {last_error}" if last_error else ""
    pytest.fail(
        f"Could not connect to SQL Server test database at {MSSQL_DATABASE_URL} after 120 seconds. Is Docker running?{error_details}"
    )

@pytest.fixture(scope="function")
def mssql_executor(mssql_connection):
    """
    Yields a MsSqlExecutor instance for running queries.
    """
    executor = MsSqlExecutor(connection=mssql_connection)
    yield executor

@pytest.fixture(scope="function")
def mssql_create_table(mssql_executor):
    """
    A fixture that creates a table and ensures it is dropped after the test.
    """
    created_tables = []

    def _create(create_node):
        mssql_executor.execute(create_node)
        created_tables.append(create_node.table.name)
        return create_node.table.name

    yield _create

    for table_name in reversed(created_tables):
        mssql_executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")

@pytest.fixture(scope="session")
def mariadb_connection():
    """
    Yields a connection to the MariaDB test database.
    Retries for up to 30 seconds to allow the container to start.
    """
    retries = 30
    last_error: Exception | None = None
    while retries > 0:
        try:
            import mariadb
            config = _parse_mariadb_url(MARIADB_DATABASE_URL)
            database = config.get("database")
            admin_config = {key: value for key, value in config.items() if key != "database"}
            admin_config["database"] = "mysql"
            admin_conn = mariadb.connect(**admin_config)
            admin_cur = admin_conn.cursor()
            if database:
                admin_cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            admin_cur.close()
            admin_conn.close()

            conn = mariadb.connect(**config)
            if hasattr(conn, "autocommit"):
                conn.autocommit = True
            yield conn
            conn.close()
            return
        except Exception as exc:
            last_error = exc
            retries -= 1
            time.sleep(1)

    error_details = f" Last error: {last_error}" if last_error else ""
    pytest.fail(
        f"Could not connect to MariaDB test database at {MARIADB_DATABASE_URL} after 30 seconds. Is Docker running?{error_details}"
    )

@pytest.fixture(scope="function")
def mariadb_executor(mariadb_connection):
    """
    Yields a MariaDbExecutor instance for running queries.
    """
    executor = MariaDbExecutor(connection=mariadb_connection)
    yield executor

@pytest.fixture(scope="function")
def mariadb_create_table(mariadb_executor):
    """
    A fixture that creates a table and ensures it is dropped after the test.
    """
    created_tables = []

    def _create(create_node):
        mariadb_executor.execute(create_node)
        created_tables.append(create_node.table.name)
        return create_node.table.name

    yield _create

    for table_name in reversed(created_tables):
        mariadb_executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}")

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
