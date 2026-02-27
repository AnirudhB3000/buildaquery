import pytest
from unittest.mock import MagicMock, patch

from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

@pytest.fixture
def mock_mariadb():
    with patch("buildaquery.execution.mariadb.MariaDbExecutor._get_mariadb") as mock:
        mock_module = MagicMock()
        mock.return_value = mock_module
        yield mock_module

def test_mariadb_executor_fetch_all(mock_mariadb):
    executor = MariaDbExecutor(connection_info="mariadb://user:pass@localhost:3306/db")
    query = CompiledQuery(sql="SELECT ?", params=[1])

    mock_conn = mock_mariadb.connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1,)]

    results = executor.fetch_all(query)

    assert results == [(1,)]
    mock_mariadb.connect.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT ?", [1])
    mock_cursor.fetchall.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mariadb_executor_execute(mock_mariadb):
    executor = MariaDbExecutor(connection_info="mariadb://user:pass@localhost:3306/db")
    query = CompiledQuery(sql="INSERT INTO t VALUES (?)", params=[10])

    mock_conn = mock_mariadb.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.execute(query)

    mock_cursor.execute.assert_called_once_with("INSERT INTO t VALUES (?)", [10])
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mariadb_executor_execute_many(mock_mariadb):
    executor = MariaDbExecutor(connection_info="mariadb://user:pass@localhost:3306/db")
    mock_conn = mock_mariadb.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.execute_many(
        "INSERT INTO t(id, value) VALUES (?, ?)",
        [[1, "a"], [2, "b"]],
    )

    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO t(id, value) VALUES (?, ?)",
        [[1, "a"], [2, "b"]],
    )
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mariadb_executor_import_error():
    executor = MariaDbExecutor(connection_info="mariadb://user:pass@localhost:3306/db")

    with patch("buildaquery.execution.mariadb.importlib.import_module", side_effect=ImportError("mariadb not found")):
        with pytest.raises(ImportError) as excinfo:
            executor._get_mariadb()
        assert "mariadb" in str(excinfo.value)

def test_mariadb_transaction_lifecycle_connection_info(mock_mariadb):
    executor = MariaDbExecutor(connection_info="mariadb://user:pass@localhost:3306/db")
    mock_conn = mock_mariadb.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.begin("READ COMMITTED")
    executor.savepoint("sp1")
    executor.rollback_to_savepoint("sp1")
    executor.release_savepoint("sp1")
    executor.commit()

    mock_cursor.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    mock_cursor.execute.assert_any_call("START TRANSACTION")
    mock_cursor.execute.assert_any_call("SAVEPOINT sp1")
    mock_cursor.execute.assert_any_call("ROLLBACK TO SAVEPOINT sp1")
    mock_cursor.execute.assert_any_call("RELEASE SAVEPOINT sp1")
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

def test_mariadb_transaction_with_existing_connection():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value
    executor = MariaDbExecutor(connection=mock_conn)

    executor.begin()
    executor.execute(CompiledQuery(sql="SELECT ?", params=[1]))
    executor.rollback()

    mock_cursor.execute.assert_any_call("START TRANSACTION")
    mock_cursor.execute.assert_any_call("SELECT ?", [1])
    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_not_called()

def test_mariadb_transaction_errors():
    executor = MariaDbExecutor(connection=MagicMock())

    with pytest.raises(RuntimeError):
        executor.commit()
    with pytest.raises(RuntimeError):
        executor.rollback()
    with pytest.raises(RuntimeError):
        executor.savepoint("sp1")

    executor.begin()
    with pytest.raises(RuntimeError):
        executor.begin()
