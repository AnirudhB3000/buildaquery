from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ==================================================
# External Input Models
# ==================================================


class ExecutorInputConfigModel(BaseModel):
    """
    Validates external executor configuration before constructing executors.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    connection_info: str = Field(min_length=1)
    connect_timeout_seconds: float | None = Field(default=None, gt=0)
    retry_max_attempts: int | None = Field(default=None, ge=1)
    retry_base_delay_seconds: float | None = Field(default=None, ge=0)
    retry_max_delay_seconds: float | None = Field(default=None, ge=0)
    retry_backoff_multiplier: float | None = Field(default=None, ge=1)


class RawExecutionRequestModel(BaseModel):
    """
    Validates externally supplied raw SQL execution payloads.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    sql: str = Field(min_length=1)
    params: list[Any] | tuple[Any, ...] | dict[str, Any] | None = None
