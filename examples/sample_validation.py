"""
Syntax-only example: validate external input with Pydantic before executor usage.

Install optional dependency:
    pip install "buildaquery[validation]"
"""

from buildaquery.validation import (
    ExecutorInputConfigModel,
    RawExecutionRequestModel,
    to_connection_settings_kwargs,
    to_raw_execution_payload,
    to_retry_policy,
)


external_config_payload = {
    "connection_info": "postgresql://user:password@localhost:5432/mydb",
    "connect_timeout_seconds": 3,
    "retry_max_attempts": 4,
}

external_query_payload = {
    "sql": "SELECT * FROM users WHERE id = %s",
    "params": [1],
}

# Validate external inputs once at the boundary.
validated_config = ExecutorInputConfigModel(**external_config_payload)
validated_query = RawExecutionRequestModel(**external_query_payload)

# Translate validated values into executor-ready inputs.
connection_kwargs = to_connection_settings_kwargs(validated_config)
retry_policy = to_retry_policy(validated_config)
sql, params = to_raw_execution_payload(validated_query)

# You would pass these values into your executor in app code:
# executor = PostgresExecutor(connection_info=validated_config.connection_info, **connection_kwargs)
# rows = executor.fetch_all_with_retry(sql, params, retry_policy=retry_policy)
print(connection_kwargs, retry_policy, sql, params)
