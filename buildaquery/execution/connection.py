from dataclasses import dataclass
from typing import Any, Callable

# ==================================================
# Connection Management Types
# ==================================================

ConnectionAcquireHook = Callable[[], Any]
ConnectionReleaseHook = Callable[[Any], None]


@dataclass(frozen=True)
class ConnectionSettings:
    """
    Cross-dialect execution connection settings.
    """

    connect_timeout_seconds: float | None = None
    acquire_connection: ConnectionAcquireHook | None = None
    release_connection: ConnectionReleaseHook | None = None
