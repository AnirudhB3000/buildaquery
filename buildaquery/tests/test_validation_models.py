from __future__ import annotations

import pytest

pydantic = pytest.importorskip("pydantic")

from pydantic import ValidationError

from buildaquery.validation.models import ExecutorInputConfigModel, RawExecutionRequestModel


def test_executor_input_config_accepts_minimal_valid_payload() -> None:
    config = ExecutorInputConfigModel(connection_info="sqlite:///tmp/test.db")
    assert config.connection_info == "sqlite:///tmp/test.db"
    assert config.connect_timeout_seconds is None


def test_executor_input_config_rejects_empty_connection_info() -> None:
    with pytest.raises(ValidationError):
        ExecutorInputConfigModel(connection_info="")


def test_executor_input_config_rejects_invalid_retry_bounds() -> None:
    with pytest.raises(ValidationError):
        ExecutorInputConfigModel(connection_info="dsn", retry_backoff_multiplier=0.5)


def test_raw_execution_request_rejects_blank_sql() -> None:
    with pytest.raises(ValidationError):
        RawExecutionRequestModel(sql="")


def test_raw_execution_request_accepts_param_shapes() -> None:
    list_params = RawExecutionRequestModel(sql="SELECT 1", params=[1, "x"])
    tuple_params = RawExecutionRequestModel(sql="SELECT 1", params=(1, "x"))
    dict_params = RawExecutionRequestModel(sql="SELECT 1", params={"id": 1})
    assert list_params.params == [1, "x"]
    assert tuple_params.params == (1, "x")
    assert dict_params.params == {"id": 1}
