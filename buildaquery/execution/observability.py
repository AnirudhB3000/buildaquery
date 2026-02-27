from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

# ==================================================
# Observability Types
# ==================================================

QueryObserveHook = Callable[["QueryObservation"], None]


@dataclass(frozen=True)
class ObservabilitySettings:
    """
    Cross-dialect execution observability settings.
    """

    query_observer: QueryObserveHook | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryObservation:
    """
    Structured query execution observation payload.
    """

    dialect: str
    operation: str
    sql: str
    param_count: int
    duration_ms: float
    succeeded: bool
    in_transaction: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)
    error_type: str | None = None
    error_message: str | None = None
