from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Callable, Mapping

from buildaquery.abstract_syntax_tree.models import ASTNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.base import Executor

MigrationAction = ASTNode | CompiledQuery | Callable[[Executor], Any]
_TRACKING_TABLE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ==================================================
# Migration Models
# ==================================================


@dataclass(frozen=True)
class MigrationStep:
    """
    Represents one ordered schema migration definition.
    """

    version: int
    name: str
    up: MigrationAction
    down: MigrationAction | None = None


@dataclass(frozen=True)
class AppliedMigration:
    """
    Represents one applied migration record from the tracking table.
    """

    version: int
    name: str
    applied_at: str


@dataclass(frozen=True)
class MigrationApplySummary:
    """
    Represents the completed outcome of an apply run.
    """

    total_defined: int
    applied_count: int
    skipped_existing: int
    transactional: bool
    wrapped_each_in_transaction: bool
    applied_versions: list[int]


@dataclass(frozen=True)
class MigrationRollbackSummary:
    """
    Represents the outcome of a rollback_last run.
    """

    rolled_back: AppliedMigration | None
    transactional: bool
    wrapped_in_transaction: bool


class MigrationError(RuntimeError):
    """
    Base class for migration runner failures.
    """


class MigrationPlanError(MigrationError):
    """
    Raised when the migration definitions are invalid.
    """


class MigrationApplyError(MigrationError):
    """
    Raised when a migration fails during apply.
    """

    def __init__(self, step: MigrationStep, cause: Exception) -> None:
        self.step = step
        self.cause = cause
        super().__init__(
            f"Migration apply failed for version {step.version} ({step.name!r})."
        )


class MigrationRollbackError(MigrationError):
    """
    Raised when a rollback cannot be completed safely.
    """


# ==================================================
# Migration Runner
# ==================================================


class MigrationRunner:
    """
    Applies ordered migrations and tracks them in a dedicated table.
    """

    def __init__(
        self,
        tracking_table: str = "buildaquery_migrations",
        transactional: bool = True,
    ) -> None:
        if not _TRACKING_TABLE_IDENTIFIER.fullmatch(tracking_table):
            raise ValueError(
                "tracking_table must be a simple SQL identifier containing only letters, digits, and underscores."
            )
        self.tracking_table = tracking_table
        self.transactional = transactional

    def apply(self, executor: Executor, migrations: list[MigrationStep]) -> MigrationApplySummary:
        self._validate_steps(migrations)
        self._ensure_tracking_table(executor)
        existing_versions = {migration.version for migration in self.applied_migrations(executor)}
        wrapped = self.transactional and executor.capabilities().transactions
        applied_versions: list[int] = []

        for step in migrations:
            if step.version in existing_versions:
                continue

            try:
                if wrapped:
                    with executor.transaction():
                        self._run_action(executor, step.up)
                        self._record_applied(executor, step)
                else:
                    self._run_action(executor, step.up)
                    self._record_applied(executor, step)
            except Exception as exc:
                raise MigrationApplyError(step, exc) from exc

            existing_versions.add(step.version)
            applied_versions.append(step.version)

        return MigrationApplySummary(
            total_defined=len(migrations),
            applied_count=len(applied_versions),
            skipped_existing=len(migrations) - len(applied_versions),
            transactional=self.transactional,
            wrapped_each_in_transaction=wrapped,
            applied_versions=applied_versions,
        )

    def applied_migrations(self, executor: Executor) -> list[AppliedMigration]:
        self._ensure_tracking_table(executor)
        rows = executor.fetch_all(
            CompiledQuery(
                sql=(
                    f"SELECT version, name, applied_at "
                    f"FROM {self.tracking_table} ORDER BY version"
                ),
            )
        )
        return [
            AppliedMigration(
                version=int(self._row_value(row, "version", 0)),
                name=str(self._row_value(row, "name", 1)),
                applied_at=str(self._row_value(row, "applied_at", 2)),
            )
            for row in rows
        ]

    def rollback_last(
        self,
        executor: Executor,
        migrations: list[MigrationStep],
    ) -> MigrationRollbackSummary:
        self._validate_steps(migrations)
        applied = self.applied_migrations(executor)
        if not applied:
            return MigrationRollbackSummary(
                rolled_back=None,
                transactional=self.transactional,
                wrapped_in_transaction=False,
            )

        latest = applied[-1]
        definitions = {step.version: step for step in migrations}
        step = definitions.get(latest.version)
        if step is None:
            raise MigrationRollbackError(
                f"No migration definition was provided for applied version {latest.version}."
            )
        if step.down is None:
            raise MigrationRollbackError(
                f"Migration version {latest.version} ({latest.name!r}) does not define a down action."
            )

        wrapped = self.transactional and executor.capabilities().transactions
        try:
            if wrapped:
                with executor.transaction():
                    self._run_action(executor, step.down)
                    self._delete_applied_record(executor, latest.version)
            else:
                self._run_action(executor, step.down)
                self._delete_applied_record(executor, latest.version)
        except Exception as exc:
            raise MigrationRollbackError(
                f"Rollback failed for migration version {latest.version} ({latest.name!r})."
            ) from exc

        return MigrationRollbackSummary(
            rolled_back=latest,
            transactional=self.transactional,
            wrapped_in_transaction=wrapped,
        )

    def _validate_steps(self, migrations: list[MigrationStep]) -> None:
        previous_version = -1
        seen_versions: set[int] = set()

        for step in migrations:
            if step.version <= 0:
                raise MigrationPlanError("Migration versions must be positive integers.")
            if not step.name.strip():
                raise MigrationPlanError("Migration names must be non-empty.")
            if step.version in seen_versions:
                raise MigrationPlanError(
                    f"Duplicate migration version detected: {step.version}."
                )
            if step.version <= previous_version:
                raise MigrationPlanError(
                    "Migration definitions must be provided in strictly ascending version order."
                )
            seen_versions.add(step.version)
            previous_version = step.version

    def _ensure_tracking_table(self, executor: Executor) -> None:
        try:
            executor.fetch_all(
                CompiledQuery(
                    sql=(
                        f"SELECT version, name, applied_at "
                        f"FROM {self.tracking_table} WHERE 1 = 0"
                    )
                )
            )
        except Exception:
            executor.execute_raw(self._create_tracking_table_sql(), trusted=True)

    def _create_tracking_table_sql(self) -> str:
        return (
            f"CREATE TABLE {self.tracking_table} ("
            "version BIGINT PRIMARY KEY, "
            "name VARCHAR(255) NOT NULL, "
            "applied_at VARCHAR(64) NOT NULL"
            ")"
        )

    def _record_applied(self, executor: Executor, step: MigrationStep) -> None:
        executor.execute(
            CompiledQuery(
                sql=(
                    f"INSERT INTO {self.tracking_table} "
                    "(version, name, applied_at) VALUES (:version, :name, :applied_at)"
                ),
                params={
                    "version": step.version,
                    "name": step.name,
                    "applied_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        )

    def _delete_applied_record(self, executor: Executor, version: int) -> None:
        executor.execute(
            CompiledQuery(
                sql=f"DELETE FROM {self.tracking_table} WHERE version = :version",
                params={"version": version},
            )
        )

    def _run_action(self, executor: Executor, action: MigrationAction) -> None:
        if callable(action):
            action(executor)
            return
        executor.execute(action)

    def _row_value(self, row: Any, key: str, index: int) -> Any:
        if isinstance(row, Mapping):
            return row[key]
        if hasattr(row, key):
            return getattr(row, key)
        return row[index]
