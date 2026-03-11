import sqlite3

import pytest

from buildaquery.abstract_syntax_tree.models import ColumnNode, InsertStatementNode, LiteralNode, TableNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.capabilities import ExecutorCapabilities
from buildaquery.execution.errors import ProgrammingExecutionError
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.seeding import SeedRunError, SeedRunner, SeedStep


def test_seed_runner_executes_ast_and_compiled_steps_in_order() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")

    steps = [
        SeedStep(
            name="insert-ast",
            action=InsertStatementNode(
                table=TableNode(name="users"),
                columns=[ColumnNode(name="id"), ColumnNode(name="email")],
                values=[LiteralNode(1), LiteralNode("a@example.com")],
            ),
        ),
        SeedStep(
            name="insert-compiled",
            action=CompiledQuery(
                sql="INSERT INTO users (id, email) VALUES (?, ?)",
                params=[2, "b@example.com"],
            ),
        ),
    ]

    summary = SeedRunner().run(executor, steps)
    rows = executor.fetch_all(CompiledQuery("SELECT id, email FROM users ORDER BY id"))

    assert summary.completed_steps == 2
    assert summary.total_steps == 2
    assert summary.wrapped_in_transaction is True
    assert summary.step_names == ["insert-ast", "insert-compiled"]
    assert rows == [(1, "a@example.com"), (2, "b@example.com")]


def test_seed_runner_uses_callable_steps() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")

    def _insert(seed_executor: SqliteExecutor) -> None:
        seed_executor.execute_raw(
            "INSERT INTO users (id, email) VALUES (?, ?)",
            [1, "callable@example.com"],
            trusted=True,
        )

    summary = SeedRunner().run(executor, [SeedStep(name="callable", action=_insert)])
    row = executor.fetch_one(CompiledQuery("SELECT id, email FROM users"))

    assert summary.completed_steps == 1
    assert row == (1, "callable@example.com")


def test_seed_runner_wraps_failed_step_with_context() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")

    steps = [
        SeedStep(
            name="first",
            action=CompiledQuery(
                sql="INSERT INTO users (id, email) VALUES (?, ?)",
                params=[1, "dup@example.com"],
            ),
        ),
        SeedStep(
            name="duplicate",
            action=CompiledQuery(
                sql="INSERT INTO users (id, email) VALUES (?, ?)",
                params=[2, "dup@example.com"],
            ),
        ),
    ]

    with pytest.raises(SeedRunError) as exc_info:
        SeedRunner().run(executor, steps)

    error = exc_info.value
    rows = executor.fetch_all(CompiledQuery("SELECT id, email FROM users"))

    assert error.step_name == "duplicate"
    assert error.completed_steps == 1
    assert error.total_steps == 2
    assert rows == []


def test_seed_runner_can_disable_transaction_wrapping() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")

    summary = SeedRunner(transactional=False).run(
        executor,
        [
            SeedStep(
                name="first",
                action=CompiledQuery(
                    sql="INSERT INTO users (id, email) VALUES (?, ?)",
                    params=[1, "a@example.com"],
                ),
            )
        ],
    )

    assert summary.transactional is False
    assert summary.wrapped_in_transaction is False


def test_seed_runner_skips_transaction_when_executor_does_not_support_it() -> None:
    class NoTxnSqliteExecutor(SqliteExecutor):
        CAPABILITIES = ExecutorCapabilities(
            transactions=False,
            savepoints=False,
            upsert=True,
            insert_returning=True,
            update_returning=True,
            delete_returning=True,
            select_for_update=False,
            select_for_share=False,
            lock_nowait=False,
            lock_skip_locked=False,
        )

    connection = sqlite3.connect(":memory:")
    executor = NoTxnSqliteExecutor(connection=connection)
    executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")

    summary = SeedRunner(transactional=True).run(
        executor,
        [
            SeedStep(
                name="single",
                action=CompiledQuery(
                    sql="INSERT INTO users (id, email) VALUES (?, ?)",
                    params=[1, "a@example.com"],
                ),
            )
        ],
    )

    assert summary.transactional is True
    assert summary.wrapped_in_transaction is False


def test_seed_runner_respects_execute_raw_policy_for_callable_steps() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection, raw_sql_policy="deny_untrusted")
    executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)", trusted=True)

    def _unsafe(seed_executor: SqliteExecutor) -> None:
        seed_executor.execute_raw(
            "INSERT INTO users (id, email) VALUES (:id, :email)",
            {"id": 1, "email": "x@example.com'); DROP TABLE users; --"},
        )

    with pytest.raises(SeedRunError) as exc_info:
        SeedRunner().run(executor, [SeedStep(name="unsafe-raw", action=_unsafe)])

    assert exc_info.value.step_name == "unsafe-raw"
    assert isinstance(exc_info.value.__cause__, ProgrammingExecutionError)
    rows = executor.fetch_all(CompiledQuery("SELECT COUNT(*) FROM users"))
    assert rows == [(0,)]
