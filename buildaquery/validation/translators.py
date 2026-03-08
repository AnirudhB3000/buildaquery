from __future__ import annotations

from typing import Any

from buildaquery.execution.retry import RetryPolicy
from buildaquery.validation.models import ExecutorInputConfigModel, RawExecutionRequestModel


# ==================================================
# Translation Helpers
# ==================================================


def to_connection_settings_kwargs(config: ExecutorInputConfigModel) -> dict[str, Any]:
    """
    Converts validated external config into executor connection kwargs.
    """
    return {"connect_timeout_seconds": config.connect_timeout_seconds}


def to_retry_policy(config: ExecutorInputConfigModel) -> RetryPolicy | None:
    """
    Builds a RetryPolicy only when external retry fields are provided.
    """
    has_retry_fields = any(
        value is not None
        for value in (
            config.retry_max_attempts,
            config.retry_base_delay_seconds,
            config.retry_max_delay_seconds,
            config.retry_backoff_multiplier,
        )
    )
    if not has_retry_fields:
        return None
    return RetryPolicy(
        max_attempts=config.retry_max_attempts or 3,
        base_delay_seconds=config.retry_base_delay_seconds or 0.05,
        max_delay_seconds=config.retry_max_delay_seconds or 1.0,
        backoff_multiplier=config.retry_backoff_multiplier or 2.0,
    )


def to_raw_execution_payload(request: RawExecutionRequestModel) -> tuple[str, list[Any] | tuple[Any, ...] | dict[str, Any] | None]:
    """
    Returns normalized raw execution payload from a validated request model.
    """
    return request.sql, request.params
