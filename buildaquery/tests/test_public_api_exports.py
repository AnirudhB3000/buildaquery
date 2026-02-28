from buildaquery import (
    __version__,
    CompiledQuery,
    ConnectionSettings,
    InMemoryMetricsAdapter,
    MySqlCompiler,
    ObservabilitySettings,
    PostgresExecutor,
    RetryPolicy,
    SqliteCompiler,
)
from buildaquery.compiler import CompiledQuery as CompiledQueryFromCompiler
from buildaquery.execution import MetricPoint


def test_root_public_api_exports_are_importable() -> None:
    assert __version__
    assert PostgresExecutor is not None
    assert SqliteCompiler is not None
    assert MySqlCompiler is not None
    assert RetryPolicy is not None
    assert ConnectionSettings is not None
    assert ObservabilitySettings is not None
    assert InMemoryMetricsAdapter is not None
    assert CompiledQuery is not None


def test_compiler_exports_include_compiled_query() -> None:
    assert CompiledQueryFromCompiler is CompiledQuery


def test_execution_exports_include_metric_point() -> None:
    metric = MetricPoint(name="count", labels={"dialect": "sqlite"}, value=1)
    assert metric.name == "count"
