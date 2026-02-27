import pytest
from unittest.mock import MagicMock, patch

from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

@pytest.fixture
def mock_pyodbc():
    with patch("buildaquery.execution.mssql.MsSqlExecutor._get_pyodbc") as mock:
        mock_module = MagicMock()
        mock.return_value = mock_module
        yield mock_module

def test_mssql_executor_fetch_all(mock_pyodbc):
    executor = MsSqlExecutor(connection_info="mssql://user:pass@localhost:1433/db")
    query = CompiledQuery(sql="SELECT ?", params=[1])

    mock_conn = mock_pyodbc.connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1,)]

    results = executor.fetch_all(query)

    assert results == [(1,)]
    mock_pyodbc.connect.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT ?", [1])
    mock_cursor.fetchall.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mssql_executor_execute(mock_pyodbc):
    executor = MsSqlExecutor(connection_info="mssql://user:pass@localhost:1433/db")
    query = CompiledQuery(sql="INSERT INTO t VALUES (?)", params=[10])

    mock_conn = mock_pyodbc.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.execute(query)

    mock_cursor.execute.assert_called_once_with("INSERT INTO t VALUES (?)", [10])
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mssql_executor_import_error():
    executor = MsSqlExecutor(connection_info="mssql://user:pass@localhost:1433/db")

    with patch("buildaquery.execution.mssql.importlib.import_module", side_effect=ImportError("pyodbc not found")):
        with pytest.raises(ImportError) as excinfo:
            executor._get_pyodbc()
        assert "pyodbc" in str(excinfo.value)

def test_mssql_transaction_lifecycle_connection_info(mock_pyodbc):
    executor = MsSqlExecutor(connection_info="mssql://user:pass@localhost:1433/db")
    mock_conn = mock_pyodbc.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.begin("READ COMMITTED")
    executor.savepoint("sp1")
    executor.rollback_to_savepoint("sp1")
    executor.release_savepoint("sp1")
    executor.commit()

    mock_cursor.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    mock_cursor.execute.assert_any_call("SAVE TRANSACTION sp1")
    mock_cursor.execute.assert_any_call("ROLLBACK TRANSACTION sp1")
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mssql_transaction_with_existing_connection():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value
    executor = MsSqlExecutor(connection=mock_conn)

    executor.begin()
    executor.execute(CompiledQuery(sql="SELECT ?", params=[1]))
    executor.rollback()

    mock_cursor.execute.assert_any_call("SELECT ?", [1])
    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_not_called()

def test_mssql_transaction_errors():
    executor = MsSqlExecutor(connection=MagicMock())

    with pytest.raises(RuntimeError):
        executor.commit()
    with pytest.raises(RuntimeError):
        executor.rollback()
    with pytest.raises(RuntimeError):
        executor.savepoint("sp1")

    executor.begin()
    with pytest.raises(RuntimeError):
        executor.begin()
