from dataclasses import dataclass
from typing import Any, Callable

from buildaquery.abstract_syntax_tree.models import ASTNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.base import Executor

SeedAction = ASTNode | CompiledQuery | Callable[[Executor], Any]


# ==================================================
# Seed Models
# ==================================================


@dataclass(frozen=True)
class SeedStep:
    """
    Represents one ordered seed operation.
    """

    name: str
    action: SeedAction


@dataclass(frozen=True)
class SeedRunSummary:
    """
    Represents the completed outcome of a seed run.
    """

    total_steps: int
    completed_steps: int
    transactional: bool
    wrapped_in_transaction: bool
    step_names: list[str]


class SeedRunError(RuntimeError):
    """
    Raised when a seed run fails on a specific step.
    """

    def __init__(
        self,
        step_name: str,
        completed_steps: int,
        total_steps: int,
        cause: Exception,
    ) -> None:
        self.step_name = step_name
        self.completed_steps = completed_steps
        self.total_steps = total_steps
        self.cause = cause
        message = (
            f"Seed run failed at step {step_name!r} "
            f"after {completed_steps} of {total_steps} completed step(s)."
        )
        super().__init__(message)


# ==================================================
# Seed Runner
# ==================================================


class SeedRunner:
    """
    Executes deterministic seed steps through an existing executor.
    """

    def __init__(self, transactional: bool = True) -> None:
        self.transactional = transactional

    def run(self, executor: Executor, steps: list[SeedStep]) -> SeedRunSummary:
        wrapped_in_transaction = self.transactional and executor.capabilities().transactions

        if wrapped_in_transaction:
            with executor.transaction():
                return self._run_steps(executor, steps, wrapped_in_transaction=True)

        return self._run_steps(executor, steps, wrapped_in_transaction=False)

    def _run_steps(
        self,
        executor: Executor,
        steps: list[SeedStep],
        *,
        wrapped_in_transaction: bool,
    ) -> SeedRunSummary:
        completed_steps = 0

        for step in steps:
            try:
                self._run_step(executor, step)
                completed_steps += 1
            except Exception as exc:
                raise SeedRunError(
                    step_name=step.name,
                    completed_steps=completed_steps,
                    total_steps=len(steps),
                    cause=exc,
                ) from exc

        return SeedRunSummary(
            total_steps=len(steps),
            completed_steps=completed_steps,
            transactional=self.transactional,
            wrapped_in_transaction=wrapped_in_transaction,
            step_names=[step.name for step in steps],
        )

    def _run_step(self, executor: Executor, step: SeedStep) -> None:
        if callable(step.action):
            step.action(executor)
            return
        executor.execute(step.action)
