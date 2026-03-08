from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

pytest.importorskip("pydantic")

from pydantic import ValidationError

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.validation.models import ExecutorInputConfigModel, RawExecutionRequestModel
from buildaquery.validation.translators import to_connection_settings_kwargs, to_raw_execution_payload


def _make_repo_local_db_path(prefix: str) -> Path:
    base_dir = Path(".tmp") / "validation-tests"
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{prefix}_{uuid4().hex[:8]}.sqlite"


def test_validated_external_payload_executes_against_sqlite() -> None:
    db_path = _make_repo_local_db_path("validated_boundary")
    config = ExecutorInputConfigModel(
        connection_info=str(db_path),
        connect_timeout_seconds=1.0,
    )
    exec_kwargs = to_connection_settings_kwargs(config)

    with SqliteExecutor(connection_info=config.connection_info, **exec_kwargs) as executor:
        executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        payload = RawExecutionRequestModel(
            sql="INSERT INTO users (id, name) VALUES (?, ?)",
            params=[1, "alice"],
        )
        sql, params = to_raw_execution_payload(payload)
        executor.execute_raw(sql, params)
        rows = executor.fetch_all(CompiledQuery(sql="SELECT id, name FROM users ORDER BY id", params=[]))
        assert rows == [(1, "alice")]


def test_invalid_external_payload_is_rejected_before_execution() -> None:
    db_path = _make_repo_local_db_path("validated_boundary_fail")
    with SqliteExecutor(connection_info=str(db_path)) as executor:
        executor.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        with pytest.raises(ValidationError):
            RawExecutionRequestModel(sql="   ", params=[1, "alice"])
        rows = executor.fetch_all(CompiledQuery(sql="SELECT id, name FROM users ORDER BY id", params=[]))
        assert rows == []
