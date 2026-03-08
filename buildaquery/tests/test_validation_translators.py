from __future__ import annotations

import pytest

pytest.importorskip("pydantic")

from buildaquery.validation.models import ExecutorInputConfigModel, RawExecutionRequestModel
from buildaquery.validation.translators import (
    to_connection_settings_kwargs,
    to_raw_execution_payload,
    to_retry_policy,
)


def test_to_connection_settings_kwargs_maps_timeout() -> None:
    config = ExecutorInputConfigModel(connection_info="dsn", connect_timeout_seconds=3.5)
    kwargs = to_connection_settings_kwargs(config)
    assert kwargs == {"connect_timeout_seconds": 3.5}


def test_to_retry_policy_returns_none_when_no_retry_fields() -> None:
    config = ExecutorInputConfigModel(connection_info="dsn")
    assert to_retry_policy(config) is None


def test_to_retry_policy_uses_validated_values() -> None:
    config = ExecutorInputConfigModel(
        connection_info="dsn",
        retry_max_attempts=5,
        retry_base_delay_seconds=0.2,
        retry_max_delay_seconds=2.0,
        retry_backoff_multiplier=3.0,
    )
    policy = to_retry_policy(config)
    assert policy is not None
    assert policy.max_attempts == 5
    assert policy.base_delay_seconds == 0.2
    assert policy.max_delay_seconds == 2.0
    assert policy.backoff_multiplier == 3.0


def test_to_raw_execution_payload_returns_sql_and_params() -> None:
    payload = RawExecutionRequestModel(sql="SELECT ?", params=[1])
    sql, params = to_raw_execution_payload(payload)
    assert sql == "SELECT ?"
    assert params == [1]
