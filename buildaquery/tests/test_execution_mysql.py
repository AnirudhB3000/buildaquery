import pytest
from unittest.mock import MagicMock, patch

from buildaquery.execution.mysql import MySqlExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

@pytest.fixture
def mock_mysql_connector():
    with patch("buildaquery.execution.mysql.MySqlExecutor._get_mysql_connector") as mock:
        mock_module = MagicMock()
        mock.return_value = mock_module
        yield mock_module

def test_mysql_executor_fetch_all(mock_mysql_connector):
    executor = MySqlExecutor(connection_info="mysql://user:pass@localhost:3306/db")
    query = CompiledQuery(sql="SELECT %s", params=[1])

    mock_conn = mock_mysql_connector.connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1,)]

    results = executor.fetch_all(query)

    assert results == [(1,)]
    mock_mysql_connector.connect.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT %s", [1])
    mock_cursor.fetchall.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mysql_executor_execute(mock_mysql_connector):
    executor = MySqlExecutor(connection_info="mysql://user:pass@localhost:3306/db")
    query = CompiledQuery(sql="INSERT INTO t VALUES (%s)", params=[10])

    mock_conn = mock_mysql_connector.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.execute(query)

    mock_cursor.execute.assert_called_once_with("INSERT INTO t VALUES (%s)", [10])
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mysql_executor_import_error():
    executor = MySqlExecutor(connection_info="mysql://user:pass@localhost:3306/db")

    with patch("importlib.import_module", side_effect=ImportError("mysql.connector not found")):
        with pytest.raises(ImportError) as excinfo:
            executor._get_mysql_connector()
        assert "mysql-connector-python" in str(excinfo.value)

def test_mysql_transaction_lifecycle_connection_info(mock_mysql_connector):
    executor = MySqlExecutor(connection_info="mysql://user:pass@localhost:3306/db")
    mock_conn = mock_mysql_connector.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.begin("READ COMMITTED")
    executor.savepoint("sp1")
    executor.rollback_to_savepoint("sp1")
    executor.release_savepoint("sp1")
    executor.commit()

    mock_cursor.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    mock_conn.start_transaction.assert_called_once()
    mock_cursor.execute.assert_any_call("SAVEPOINT sp1")
    mock_cursor.execute.assert_any_call("ROLLBACK TO SAVEPOINT sp1")
    mock_cursor.execute.assert_any_call("RELEASE SAVEPOINT sp1")
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mysql_transaction_uses_existing_connection():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value
    executor = MySqlExecutor(connection=mock_conn)

    executor.begin()
    executor.execute(CompiledQuery(sql="SELECT %s", params=[1]))
    executor.rollback()

    mock_conn.start_transaction.assert_called_once()
    mock_cursor.execute.assert_any_call("SELECT %s", [1])
    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_not_called()

def test_mysql_transaction_errors():
    executor = MySqlExecutor(connection=MagicMock())

    with pytest.raises(RuntimeError):
        executor.commit()
    with pytest.raises(RuntimeError):
        executor.rollback()
    with pytest.raises(RuntimeError):
        executor.savepoint("sp1")

    executor.begin()
    with pytest.raises(RuntimeError):
        executor.begin()
