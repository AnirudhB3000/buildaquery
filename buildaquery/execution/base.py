from abc import ABC, abstractmethod
from typing import Any, Sequence
from buildaquery.compiler.postgres.postgres_compiler import CompiledQuery

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
