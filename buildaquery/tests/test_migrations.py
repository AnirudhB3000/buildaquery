import sqlite3

import pytest

from buildaquery.abstract_syntax_tree.models import ColumnNode, InsertStatementNode, LiteralNode, TableNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.capabilities import ExecutorCapabilities
from buildaquery.execution.errors import ProgrammingExecutionError
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.migrations import (
    MigrationApplyError,
    MigrationPlanError,
    MigrationRollbackError,
    MigrationRunner,
    MigrationStep,
)


def test_migration_runner_applies_pending_steps_and_tracks_versions() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    runner = MigrationRunner()
    migrations = [
        MigrationStep(
            version=1,
            name="create-users",
            up=CompiledQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"),
            down=CompiledQuery("DROP TABLE users"),
        ),
        MigrationStep(
            version=2,
            name="seed-user",
            up=InsertStatementNode(
                table=TableNode(name="users"),
                columns=[ColumnNode(name="id"), ColumnNode(name="email")],
                values=[LiteralNode(1), LiteralNode("admin@example.com")],
            ),
            down=CompiledQuery("DELETE FROM users WHERE id = :id", {"id": 1}),
        ),
    ]

    summary = runner.apply(executor, migrations)
    rows = executor.fetch_all(CompiledQuery("SELECT id, email FROM users"))
    applied = runner.applied_migrations(executor)

    assert summary.applied_count == 2
    assert summary.applied_versions == [1, 2]
    assert rows == [(1, "admin@example.com")]
    assert [migration.version for migration in applied] == [1, 2]


def test_migration_runner_skips_already_applied_versions() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    runner = MigrationRunner()
    migrations = [
        MigrationStep(
            version=1,
            name="create-users",
            up=CompiledQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"),
        ),
    ]

    first = runner.apply(executor, migrations)
    second = runner.apply(executor, migrations)

    assert first.applied_count == 1
    assert second.applied_count == 0
    assert second.skipped_existing == 1


def test_migration_runner_rejects_duplicate_or_out_of_order_versions() -> None:
    runner = MigrationRunner()
    duplicate = [
        MigrationStep(version=1, name="a", up=CompiledQuery("SELECT 1")),
        MigrationStep(version=1, name="b", up=CompiledQuery("SELECT 2")),
    ]
    out_of_order = [
        MigrationStep(version=2, name="b", up=CompiledQuery("SELECT 2")),
        MigrationStep(version=1, name="a", up=CompiledQuery("SELECT 1")),
    ]

    with pytest.raises(MigrationPlanError):
        runner.apply(SqliteExecutor(connection=sqlite3.connect(":memory:")), duplicate)

    with pytest.raises(MigrationPlanError):
        runner.apply(SqliteExecutor(connection=sqlite3.connect(":memory:")), out_of_order)


def test_migration_runner_rolls_back_last_applied_migration() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    runner = MigrationRunner()
    migrations = [
        MigrationStep(
            version=1,
            name="create-users",
            up=CompiledQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"),
            down=CompiledQuery("DROP TABLE users"),
        ),
        MigrationStep(
            version=2,
            name="seed-user",
            up=CompiledQuery(
                "INSERT INTO users (id, email) VALUES (:id, :email)",
                {"id": 1, "email": "admin@example.com"},
            ),
            down=CompiledQuery("DELETE FROM users WHERE id = :id", {"id": 1}),
        ),
    ]

    runner.apply(executor, migrations)
    rollback = runner.rollback_last(executor, migrations)
    rows = executor.fetch_all(CompiledQuery("SELECT COUNT(*) FROM users"))
    applied = runner.applied_migrations(executor)

    assert rollback.rolled_back is not None
    assert rollback.rolled_back.version == 2
    assert rows == [(0,)]
    assert [migration.version for migration in applied] == [1]


def test_migration_runner_requires_down_action_for_rollback() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    runner = MigrationRunner()
    migrations = [
        MigrationStep(
            version=1,
            name="create-users",
            up=CompiledQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"),
        ),
    ]

    runner.apply(executor, migrations)

    with pytest.raises(MigrationRollbackError):
        runner.rollback_last(executor, migrations)


def test_migration_runner_rolls_back_failed_apply_step() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    runner = MigrationRunner()
    migrations = [
        MigrationStep(
            version=1,
            name="create-users",
            up=CompiledQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)"),
        ),
        MigrationStep(
            version=2,
            name="duplicate-seed",
            up=lambda migration_executor: (
                migration_executor.execute_raw(
                    "INSERT INTO users (id, email) VALUES (?, ?)",
                    [1, "dup@example.com"],
                    trusted=True,
                ),
                migration_executor.execute_raw(
                    "INSERT INTO users (id, email) VALUES (?, ?)",
                    [2, "dup@example.com"],
                    trusted=True,
                ),
            ),
        ),
    ]

    with pytest.raises(MigrationApplyError) as exc_info:
        runner.apply(executor, migrations)

    rows = executor.fetch_all(CompiledQuery("SELECT COUNT(*) FROM users"))
    applied = runner.applied_migrations(executor)

    assert exc_info.value.step.version == 2
    assert rows == [(0,)]
    assert [migration.version for migration in applied] == [1]


def test_migration_runner_respects_execute_raw_policy_for_callable_actions() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection, raw_sql_policy="deny_untrusted")
    runner = MigrationRunner()
    migrations = [
        MigrationStep(
            version=1,
            name="unsafe-raw",
            up=lambda migration_executor: migration_executor.execute_raw(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
            ),
        ),
    ]

    with pytest.raises(MigrationApplyError) as exc_info:
        runner.apply(executor, migrations)

    assert isinstance(exc_info.value.__cause__, ProgrammingExecutionError)
    assert runner.applied_migrations(executor) == []


def test_migration_runner_keeps_hostile_payloads_parameterized() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    runner = MigrationRunner()
    hostile_email = "x@example.com'); DROP TABLE users; --"
    migrations = [
        MigrationStep(
            version=1,
            name="create-users",
            up=CompiledQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"),
        ),
        MigrationStep(
            version=2,
            name="seed-hostile",
            up=CompiledQuery(
                "INSERT INTO users (id, email) VALUES (:id, :email)",
                {"id": 1, "email": hostile_email},
            ),
        ),
    ]

    preview = executor.to_sql(migrations[1].up)
    summary = runner.apply(executor, migrations)
    rows = executor.fetch_all(CompiledQuery("SELECT id, email FROM users"))

    assert hostile_email not in preview.to_sql()
    assert summary.applied_versions == [1, 2]
    assert rows == [(1, hostile_email)]


def test_migration_runner_can_skip_transaction_wrapping() -> None:
    connection = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=connection)
    runner = MigrationRunner(transactional=False)
    summary = runner.apply(
        executor,
        [
            MigrationStep(
                version=1,
                name="create-users",
                up=CompiledQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"),
            )
        ],
    )

    assert summary.transactional is False
    assert summary.wrapped_each_in_transaction is False


def test_migration_runner_skips_transaction_when_executor_does_not_support_it() -> None:
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
    summary = MigrationRunner(transactional=True).apply(
        executor,
        [
            MigrationStep(
                version=1,
                name="create-users",
                up=CompiledQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"),
            )
        ],
    )

    assert summary.transactional is True
    assert summary.wrapped_each_in_transaction is False
