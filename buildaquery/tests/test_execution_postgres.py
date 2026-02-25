import pytest
from unittest.mock import MagicMock, patch
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.compiler.compiled_query import CompiledQuery

@pytest.fixture
def mock_psycopg():
    with patch("buildaquery.execution.postgres.PostgresExecutor._get_psycopg") as mock:
        mock_module = MagicMock()
        mock.return_value = mock_module
        yield mock_module

def test_postgres_executor_fetch_all(mock_psycopg):
    executor = PostgresExecutor(connection_info="dsn")
    query = CompiledQuery(sql="SELECT %s", params=[1])
    
    # Setup mock chain: connect -> cursor -> execute -> fetchall
    mock_conn = mock_psycopg.connect.return_value.__enter__.return_value
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    mock_cur.fetchall.return_value = [(1,)]
    
    results = executor.fetch_all(query)
    
    assert results == [(1,)]
    mock_psycopg.connect.assert_called_once_with("dsn")
    mock_cur.execute.assert_called_once_with("SELECT %s", [1])
    mock_cur.fetchall.assert_called_once()

def test_postgres_executor_execute(mock_psycopg):
    executor = PostgresExecutor(connection_info="dsn")
    query = CompiledQuery(sql="INSERT INTO t VALUES (%s)", params=[10])
    
    mock_conn = mock_psycopg.connect.return_value.__enter__.return_value
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    
    executor.execute(query)
    
    mock_cur.execute.assert_called_once_with("INSERT INTO t VALUES (%s)", [10])

def test_postgres_executor_import_error():
    # Test that it raises ImportError if psycopg is missing
    executor = PostgresExecutor(connection_info="dsn")
    
    # We patch 'builtins.__import__' but only for when 'psycopg' is requested
    # Actually, a cleaner way is to use 'side_effect' on a patch that we know will be called
    with patch('builtins.__import__') as mock_import:
        def side_effect(name, *args, **kwargs):
            if name == 'psycopg':
                raise ImportError("psycopg not found")
            return MagicMock() # Return a mock for other imports
        
        mock_import.side_effect = side_effect
        
        with pytest.raises(ImportError) as excinfo:
            executor._get_psycopg()
        assert "The 'psycopg' library is required" in str(excinfo.value)
