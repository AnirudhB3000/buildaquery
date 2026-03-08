# Validation

The `validation` module provides optional, boundary-focused input validation utilities backed by Pydantic.

Use this module to validate external payloads (API/CLI/job/env input) before passing values into executors.

## Models

- `ExecutorInputConfigModel`: validates external executor config values.
- `RawExecutionRequestModel`: validates externally supplied SQL + params payloads.

## Translators

- `to_connection_settings_kwargs(...)`: maps validated config to executor kwargs.
- `to_retry_policy(...)`: maps validated retry fields to `RetryPolicy`.
- `to_raw_execution_payload(...)`: returns normalized `(sql, params)` tuple.

## Install

```bash
pip install "buildaquery[validation]"
```
