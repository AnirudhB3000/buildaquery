from buildaquery import (
    __version__,
    ClickHouseCompiler,
    ClickHouseExecutor,
    CompiledQuery,
    ConnectionSettings,
    ExecutorCapabilities,
    DuckDbCompiler,
    DuckDbExecutor,
    AppliedMigration,
    InMemoryMetricsAdapter,
    MigrationApplySummary,
    MigrationRunner,
    MigrationStep,
    MySqlCompiler,
    ObservabilitySettings,
    PostgresExecutor,
    RetryPolicy,
    SeedRunSummary,
    SeedRunner,
    SeedStep,
    SqliteCompiler,
)
from buildaquery.compiler import CompiledQuery as CompiledQueryFromCompiler
from buildaquery.execution import ExecutorCapabilities as ExecutorCapabilitiesFromExecution
from buildaquery.execution import MetricPoint


def test_root_public_api_exports_are_importable() -> None:
    assert __version__
    assert PostgresExecutor is not None
    assert SqliteCompiler is not None
    assert MySqlCompiler is not None
    assert RetryPolicy is not None
    assert ConnectionSettings is not None
    assert ExecutorCapabilities is not None
    assert ObservabilitySettings is not None
    assert InMemoryMetricsAdapter is not None
    assert CompiledQuery is not None
    assert DuckDbCompiler is not None
    assert DuckDbExecutor is not None
    assert ClickHouseCompiler is not None
    assert ClickHouseExecutor is not None
    assert MigrationStep is not None
    assert AppliedMigration is not None
    assert MigrationRunner is not None
    assert MigrationApplySummary is not None
    assert SeedStep is not None
    assert SeedRunner is not None
    assert SeedRunSummary is not None


def test_compiler_exports_include_compiled_query() -> None:
    assert CompiledQueryFromCompiler is CompiledQuery


def test_execution_exports_include_metric_point() -> None:
    metric = MetricPoint(name="count", labels={"dialect": "sqlite"}, value=1)
    assert metric.name == "count"


def test_execution_exports_include_executor_capabilities() -> None:
    capabilities = ExecutorCapabilitiesFromExecution(
        transactions=True,
        savepoints=True,
        upsert=False,
        insert_returning=False,
        update_returning=False,
        delete_returning=False,
        select_for_update=False,
        select_for_share=False,
        lock_nowait=False,
        lock_skip_locked=False,
    )
    assert capabilities.transactions is True
