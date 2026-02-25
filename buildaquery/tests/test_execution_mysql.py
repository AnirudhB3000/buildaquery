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
