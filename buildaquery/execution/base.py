from abc import ABC, abstractmethod
from typing import Any, Sequence
from buildaquery.compiler.compiled_query import CompiledQuery

# ==================================================
# Base Executor
# ==================================================

class Executor(ABC):
    """
    Abstract base class for executing compiled queries against a database.
    """

    @abstractmethod
    def execute(self, compiled_query: CompiledQuery) -> Any:
        """
        Executes a single compiled query.
        """
        pass

    @abstractmethod
    def fetch_all(self, compiled_query: CompiledQuery) -> Sequence[Sequence[Any]]:
        """
        Executes a query and returns all resulting rows.
        """
        pass

    @abstractmethod
    def fetch_one(self, compiled_query: CompiledQuery) -> Sequence[Any] | None:
        """
        Executes a query and returns a single resulting row.
        """
        pass

    @abstractmethod
    def execute_many(self, sql: str, param_sets: Sequence[Sequence[Any]]) -> None:
        """
        Executes a SQL statement against multiple parameter sets.
        """
        pass

    @abstractmethod
    def begin(self, isolation_level: str | None = None) -> None:
        """
        Begins an explicit transaction.
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """
        Commits the active explicit transaction.
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """
        Rolls back the active explicit transaction.
        """
        pass

    @abstractmethod
    def savepoint(self, name: str) -> None:
        """
        Creates a savepoint in the active explicit transaction.
        """
        pass

    @abstractmethod
    def rollback_to_savepoint(self, name: str) -> None:
        """
        Rolls back to a savepoint in the active explicit transaction.
        """
        pass

    @abstractmethod
    def release_savepoint(self, name: str) -> None:
        """
        Releases a savepoint in the active explicit transaction.
        """
        pass
