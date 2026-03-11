from uuid import uuid4

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.migrations import MigrationRunner, MigrationStep


def test_migration_runner_apply_and_rollback_last_on_sqlite(sqlite_executor) -> None:
    table_name = f"migration_users_{uuid4().hex[:8]}"
    tracking_table = f"migration_history_{uuid4().hex[:8]}"
    runner = MigrationRunner(tracking_table=tracking_table)
    migrations = [
        MigrationStep(
            version=1,
            name="create-users",
            up=CompiledQuery(
                f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"
            ),
            down=CompiledQuery(f"DROP TABLE {table_name}"),
        ),
        MigrationStep(
            version=2,
            name="seed-user",
            up=CompiledQuery(
                f"INSERT INTO {table_name} (id, email) VALUES (:id, :email)",
                {"id": 1, "email": "admin@example.com"},
            ),
            down=CompiledQuery(
                f"DELETE FROM {table_name} WHERE id = :id",
                {"id": 1},
            ),
        ),
    ]

    try:
        apply_summary = runner.apply(sqlite_executor, migrations)
        rows = sqlite_executor.fetch_all(
            CompiledQuery(f"SELECT id, email FROM {table_name} ORDER BY id")
        )
        applied = runner.applied_migrations(sqlite_executor)
        rollback_summary = runner.rollback_last(sqlite_executor, migrations)
        rows_after_rollback = sqlite_executor.fetch_all(
            CompiledQuery(f"SELECT COUNT(*) FROM {table_name}")
        )
    finally:
        sqlite_executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}", trusted=True)
        sqlite_executor.execute_raw(f"DROP TABLE IF EXISTS {tracking_table}", trusted=True)

    assert apply_summary.applied_versions == [1, 2]
    assert rows == [(1, "admin@example.com")]
    assert [migration.version for migration in applied] == [1, 2]
    assert rollback_summary.rolled_back is not None
    assert rollback_summary.rolled_back.version == 2
    assert rows_after_rollback == [(0,)]


def test_migration_runner_rolls_back_failed_sqlite_migration(sqlite_executor) -> None:
    table_name = f"migration_fail_{uuid4().hex[:8]}"
    tracking_table = f"migration_history_{uuid4().hex[:8]}"
    runner = MigrationRunner(tracking_table=tracking_table)
    migrations = [
        MigrationStep(
            version=1,
            name="create-users",
            up=CompiledQuery(
                f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)"
            ),
            down=CompiledQuery(f"DROP TABLE {table_name}"),
        ),
        MigrationStep(
            version=2,
            name="duplicate-seed",
            up=lambda migration_executor: (
                migration_executor.execute_raw(
                    f"INSERT INTO {table_name} (id, email) VALUES (?, ?)",
                    [1, "dup@example.com"],
                    trusted=True,
                ),
                migration_executor.execute_raw(
                    f"INSERT INTO {table_name} (id, email) VALUES (?, ?)",
                    [2, "dup@example.com"],
                    trusted=True,
                ),
            ),
            down=CompiledQuery(f"DELETE FROM {table_name}"),
        ),
    ]

    try:
        from buildaquery.migrations import MigrationApplyError
        import pytest
        with pytest.raises(MigrationApplyError):
            runner.apply(sqlite_executor, migrations)
        applied = runner.applied_migrations(sqlite_executor)
        rows = sqlite_executor.fetch_all(CompiledQuery(f"SELECT COUNT(*) FROM {table_name}"))
    finally:
        sqlite_executor.execute_raw(f"DROP TABLE IF EXISTS {table_name}", trusted=True)
        sqlite_executor.execute_raw(f"DROP TABLE IF EXISTS {tracking_table}", trusted=True)

    assert [migration.version for migration in applied] == [1]
    assert rows == [(0,)]
