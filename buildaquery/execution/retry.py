from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable, TypeVar

from buildaquery.execution.errors import ExecutionError, TransientExecutionError

T = TypeVar("T")


# ==================================================
# Retry Policy
# ==================================================


@dataclass(slots=True)
class RetryPolicy:
    """
    Retry policy for transient execution failures.
    """

    max_attempts: int = 3
    base_delay_seconds: float = 0.05
    max_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must be >= 0")
        if self.max_delay_seconds < 0:
            raise ValueError("max_delay_seconds must be >= 0")
        if self.backoff_multiplier < 1:
            raise ValueError("backoff_multiplier must be >= 1")


def _compute_delay(policy: RetryPolicy, attempt: int) -> float:
    delay = policy.base_delay_seconds * (policy.backoff_multiplier ** (attempt - 1))
    return min(delay, policy.max_delay_seconds)


def run_with_retry(
    *,
    operation: Callable[[], T],
    normalize_error: Callable[[Exception], ExecutionError],
    policy: RetryPolicy,
    sleep_fn: Callable[[float], Any] = time.sleep,
    on_retry: Callable[[ExecutionError, int, float], Any] | None = None,
    on_giveup: Callable[[ExecutionError, int], Any] | None = None,
) -> T:
    """
    Runs an operation with transient-failure retry handling.
    """
    attempt = 1
    while True:
        try:
            return operation()
        except Exception as exc:
            normalized = exc if isinstance(exc, ExecutionError) else normalize_error(exc)
            if attempt >= policy.max_attempts or not isinstance(normalized, TransientExecutionError):
                if on_giveup is not None:
                    on_giveup(normalized, attempt)
                raise normalized from exc
            delay = _compute_delay(policy, attempt)
            if on_retry is not None:
                on_retry(normalized, attempt, delay)
            if delay > 0:
                sleep_fn(delay)
            attempt += 1
