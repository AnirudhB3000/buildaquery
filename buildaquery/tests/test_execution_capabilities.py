from buildaquery.execution import ExecutorCapabilities
from buildaquery.execution.clickhouse import ClickHouseExecutor
from buildaquery.execution.cockroachdb import CockroachExecutor
from buildaquery.execution.duckdb import DuckDbExecutor
from buildaquery.execution.mariadb import MariaDbExecutor
from buildaquery.execution.mssql import MsSqlExecutor
from buildaquery.execution.oracle import OracleExecutor
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor


def test_postgres_capabilities_expose_full_oltp_surface() -> None:
    executor = PostgresExecutor(connection=object())

    assert executor.capabilities() == ExecutorCapabilities(
        transactions=True,
        savepoints=True,
        upsert=True,
        insert_returning=True,
        update_returning=True,
        delete_returning=True,
        select_for_update=True,
        select_for_share=True,
        lock_nowait=True,
        lock_skip_locked=True,
    )


def test_sqlite_capabilities_reflect_no_lock_clause_support() -> None:
    capabilities = SqliteExecutor(connection=object()).capabilities()

    assert capabilities.transactions is True
    assert capabilities.savepoints is True
    assert capabilities.upsert is True
    assert capabilities.insert_returning is True
    assert capabilities.update_returning is True
    assert capabilities.delete_returning is True
    assert capabilities.select_for_update is False
    assert capabilities.select_for_share is False
    assert capabilities.lock_nowait is False
    assert capabilities.lock_skip_locked is False


def test_duckdb_capabilities_report_conservative_savepoint_support() -> None:
    capabilities = DuckDbExecutor(connection=object()).capabilities()

    assert capabilities.transactions is True
    assert capabilities.savepoints is False
    assert capabilities.upsert is True
    assert capabilities.insert_returning is True
    assert capabilities.update_returning is True
    assert capabilities.delete_returning is True


def test_clickhouse_capabilities_report_no_transactional_surface() -> None:
    capabilities = ClickHouseExecutor(connection=object()).capabilities()

    assert capabilities.transactions is False
    assert capabilities.savepoints is False
    assert capabilities.upsert is False
    assert capabilities.insert_returning is False
    assert capabilities.update_returning is False
    assert capabilities.delete_returning is False
    assert capabilities.execute_many is True
    assert capabilities.execute_raw is True


def test_oracle_capabilities_reflect_lock_and_returning_limits() -> None:
    capabilities = OracleExecutor(connection=object()).capabilities()

    assert capabilities.transactions is True
    assert capabilities.savepoints is True
    assert capabilities.upsert is True
    assert capabilities.insert_returning is False
    assert capabilities.update_returning is False
    assert capabilities.delete_returning is False
    assert capabilities.select_for_update is True
    assert capabilities.select_for_share is False
    assert capabilities.lock_nowait is True
    assert capabilities.lock_skip_locked is True


def test_mssql_capabilities_reflect_output_without_trailing_lock_clauses() -> None:
    capabilities = MsSqlExecutor(connection=object()).capabilities()

    assert capabilities.upsert is True
    assert capabilities.insert_returning is True
    assert capabilities.update_returning is True
    assert capabilities.delete_returning is True
    assert capabilities.select_for_update is False
    assert capabilities.select_for_share is False


def test_mariadb_capabilities_reflect_partial_returning_support() -> None:
    capabilities = MariaDbExecutor(connection=object()).capabilities()

    assert capabilities.upsert is True
    assert capabilities.insert_returning is True
    assert capabilities.update_returning is False
    assert capabilities.delete_returning is True
    assert capabilities.select_for_update is True
    assert capabilities.select_for_share is True


def test_cockroach_capabilities_match_postgres_style_surface() -> None:
    executor = CockroachExecutor(connection=object())

    assert executor.capabilities() == ExecutorCapabilities(
        transactions=True,
        savepoints=True,
        upsert=True,
        insert_returning=True,
        update_returning=True,
        delete_returning=True,
        select_for_update=True,
        select_for_share=True,
        lock_nowait=True,
        lock_skip_locked=True,
    )


def test_capabilities_to_dict_is_logging_friendly() -> None:
    capabilities = PostgresExecutor(connection=object()).capabilities().to_dict()

    assert capabilities["transactions"] is True
    assert capabilities["select_for_update"] is True
    assert capabilities["execute_raw"] is True
