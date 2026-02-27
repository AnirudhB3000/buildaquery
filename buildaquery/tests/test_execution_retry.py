from typing import Any, Sequence

import pytest

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.base import Executor
from buildaquery.execution.errors import DeadlockError, IntegrityConstraintError
from buildaquery.execution.retry import RetryPolicy


class _FakeExecutor(Executor):
    def __init__(self) -> None:
        self.execute_calls = 0
        self.fetch_all_calls = 0
        self.fetch_one_calls = 0
        self.execute_many_calls = 0
        self.execute_failures: list[Exception] = []
        self.fetch_all_failures: list[Exception] = []
        self.fetch_one_failures: list[Exception] = []
        self.execute_many_failures: list[Exception] = []

    def execute(self, compiled_query: CompiledQuery) -> Any:
        self.execute_calls += 1
        if self.execute_failures:
            raise self.execute_failures.pop(0)
        return [("ok", compiled_query.sql)]

    def fetch_all(self, compiled_query: CompiledQuery) -> Sequence[Sequence[Any]]:
        self.fetch_all_calls += 1
        if self.fetch_all_failures:
            raise self.fetch_all_failures.pop(0)
        return [("all", compiled_query.sql)]

    def fetch_one(self, compiled_query: CompiledQuery) -> Sequence[Any] | None:
        self.fetch_one_calls += 1
        if self.fetch_one_failures:
            raise self.fetch_one_failures.pop(0)
        return ("one", compiled_query.sql)

    def execute_many(self, sql: str, param_sets: Sequence[Sequence[Any]]) -> None:
        self.execute_many_calls += 1
        if self.execute_many_failures:
            raise self.execute_many_failures.pop(0)

    def begin(self, isolation_level: str | None = None) -> None:
        _ = isolation_level

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def savepoint(self, name: str) -> None:
        _ = name

    def rollback_to_savepoint(self, name: str) -> None:
        _ = name

    def release_savepoint(self, name: str) -> None:
        _ = name


class _SqlStateError(Exception):
    def __init__(self, message: str, sqlstate: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


def test_execute_with_retry_retries_transient_and_succeeds() -> None:
    executor = _FakeExecutor()
    executor.execute_failures = [_SqlStateError("database is locked", "55P03")]
    query = CompiledQuery(sql="SELECT 1", params=[])
    policy = RetryPolicy(max_attempts=2, base_delay_seconds=0.0)

    result = executor.execute_with_retry(query, retry_policy=policy)

    assert result == [("ok", "SELECT 1")]
    assert executor.execute_calls == 2


def test_execute_with_retry_does_not_retry_nontransient() -> None:
    executor = _FakeExecutor()
    executor.execute_failures = [_SqlStateError("duplicate key", "23000")]
    query = CompiledQuery(sql="INSERT", params=[])
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=0.0)

    with pytest.raises(IntegrityConstraintError):
        executor.execute_with_retry(query, retry_policy=policy)

    assert executor.execute_calls == 1


def test_fetch_all_with_retry_exhausts_and_raises_transient() -> None:
    executor = _FakeExecutor()
    executor.fetch_all_failures = [
        _SqlStateError("deadlock detected", "40P01"),
        _SqlStateError("deadlock detected", "40P01"),
    ]
    query = CompiledQuery(sql="SELECT 1", params=[])
    policy = RetryPolicy(max_attempts=2, base_delay_seconds=0.0)

    with pytest.raises(DeadlockError):
        executor.fetch_all_with_retry(query, retry_policy=policy)

    assert executor.fetch_all_calls == 2


def test_fetch_one_with_retry_success_after_retry() -> None:
    executor = _FakeExecutor()
    executor.fetch_one_failures = [_SqlStateError("lock timeout", "1205")]
    query = CompiledQuery(sql="SELECT 1", params=[])
    policy = RetryPolicy(max_attempts=2, base_delay_seconds=0.0)

    row = executor.fetch_one_with_retry(query, retry_policy=policy)

    assert row == ("one", "SELECT 1")
    assert executor.fetch_one_calls == 2


def test_execute_many_with_retry_success_after_retry() -> None:
    executor = _FakeExecutor()
    executor.execute_many_failures = [_SqlStateError("serialization failure", "40001")]
    policy = RetryPolicy(max_attempts=2, base_delay_seconds=0.0)

    executor.execute_many_with_retry(
        "INSERT INTO t(a) VALUES (?)",
        [[1], [2]],
        retry_policy=policy,
    )

    assert executor.execute_many_calls == 2
