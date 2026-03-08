import pytest
from unittest.mock import MagicMock, patch

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.clickhouse import ClickHouseExecutor


@pytest.fixture
def mock_clickhouse_dbapi() -> MagicMock:
    with patch("buildaquery.execution.clickhouse.ClickHouseExecutor._get_clickhouse_dbapi") as mock:
        mock_module = MagicMock()
        mock.return_value = mock_module
        yield mock_module


def test_clickhouse_executor_fetch_all(mock_clickhouse_dbapi: MagicMock) -> None:
    executor = ClickHouseExecutor(connection_info="clickhouse://default@127.0.0.1:9000/default")
    query = CompiledQuery(sql="SELECT %s", params=[1])

    mock_conn = mock_clickhouse_dbapi.connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1,)]

    results = executor.fetch_all(query)

    assert results == [(1,)]
    mock_clickhouse_dbapi.connect.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT %s", [1])
    mock_cursor.fetchall.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_clickhouse_executor_execute(mock_clickhouse_dbapi: MagicMock) -> None:
    executor = ClickHouseExecutor(connection_info="clickhouse://default@127.0.0.1:9000/default")
    query = CompiledQuery(sql="INSERT INTO t VALUES (%s)", params=[10])

    mock_conn = mock_clickhouse_dbapi.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.execute(query)

    mock_cursor.executemany.assert_called_once_with("INSERT INTO t VALUES", [(10,)])
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_clickhouse_executor_execute_many(mock_clickhouse_dbapi: MagicMock) -> None:
    executor = ClickHouseExecutor(connection_info="clickhouse://default@127.0.0.1:9000/default")
    mock_conn = mock_clickhouse_dbapi.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.execute_many(
        "INSERT INTO t(id, value) VALUES (%s, %s)",
        [[1, "a"], [2, "b"]],
    )

    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO t(id, value) VALUES",
        [[1, "a"], [2, "b"]],
    )
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_clickhouse_executor_execute_select_uses_standard_execute(mock_clickhouse_dbapi: MagicMock) -> None:
    executor = ClickHouseExecutor(connection_info="clickhouse://default@127.0.0.1:9000/default")
    query = CompiledQuery(sql="SELECT %s", params=[1])
    mock_conn = mock_clickhouse_dbapi.connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1,)]

    rows = executor.execute(query)

    assert rows == [(1,)]
    mock_cursor.execute.assert_called_once_with("SELECT %s", [1])
    mock_cursor.executemany.assert_not_called()


def test_clickhouse_executor_execute_select_without_params_omits_params_arg(
    mock_clickhouse_dbapi: MagicMock,
) -> None:
    executor = ClickHouseExecutor(connection_info="clickhouse://default@127.0.0.1:9000/default")
    query = CompiledQuery(sql="SELECT 1", params=[])
    mock_conn = mock_clickhouse_dbapi.connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1,)]

    rows = executor.execute(query)

    assert rows == [(1,)]
    mock_cursor.execute.assert_called_once_with("SELECT 1")


def test_clickhouse_execute_raw_without_params_does_not_pass_empty_list(mock_clickhouse_dbapi: MagicMock) -> None:
    executor = ClickHouseExecutor(connection_info="clickhouse://default@127.0.0.1:9000/default")
    mock_conn = mock_clickhouse_dbapi.connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    executor.execute_raw("CREATE TABLE t (id UInt32) ENGINE = Memory")

    mock_cursor.execute.assert_called_once_with("CREATE TABLE t (id UInt32) ENGINE = Memory")
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_clickhouse_executor_import_error() -> None:
    executor = ClickHouseExecutor(connection_info="clickhouse://default@127.0.0.1:9000/default")

    with patch("importlib.import_module", side_effect=ImportError("clickhouse_driver not found")):
        with pytest.raises(ImportError) as excinfo:
            executor._get_clickhouse_dbapi()
        assert "clickhouse-driver" in str(excinfo.value)


def test_clickhouse_transaction_apis_rejected() -> None:
    executor = ClickHouseExecutor(connection=MagicMock())

    with pytest.raises(RuntimeError, match="does not support explicit transaction control APIs"):
        executor.begin()
    with pytest.raises(RuntimeError, match="does not support explicit transaction control APIs"):
        executor.commit()
    with pytest.raises(RuntimeError, match="does not support explicit transaction control APIs"):
        executor.rollback()
    with pytest.raises(RuntimeError, match="does not support savepoint APIs"):
        executor.savepoint("sp1")
