# Build-a-Query Project Goals

This project aims to create a query builder for Python with support for PostgreSQL, SQLite, DuckDB, ClickHouse, MySQL, MariaDB, CockroachDB, Oracle, and SQL Server. The development process is broken down into the following stages: 

1.  **Abstract Syntax Tree (AST):** Define and build the necessary data structures to represent queries as an AST.
2.  **AST Traversal:** Implement mechanisms for traversing the AST to analyze, modify, and process the query structure.
3.  **SQL Compiler:** Develop compilers that translate the AST into valid SQL queries for PostgreSQL, SQLite, DuckDB, ClickHouse, MySQL, MariaDB, CockroachDB, Oracle, and SQL Server.
4.  **Execution Layer:** Create layers responsible for executing the compiled SQL queries against the target databases.

---

## Documentation Scope

- `docs/docs.md` is a concise, user-facing guide focused on installation, usage, and essential commands.
- Nested package `README.md` files under `buildaquery/` are the primary developer-facing references for internals and implementation details.

---

## Changelog

*   **Project Initialization:**
    *   Set up the basic project structure with `buildaquery` as the main package.
    *   Created directories for `abstract_syntax_tree`, `traversal`, `compiler`, and `execution`, each with a `README.md` and `__init__.py`.
*   **AST Modifications (`models.py`):**
    *   Added `TopClauseNode` to represent `TOP` clauses in SQL.
    *   Updated `SelectStatementNode` to include an optional `top_clause: TopClauseNode | None`.
*   **Demonstration:**
    *   Created `examples/postgres_demo.py` to show an example of building and compiling a `SelectStatementNode`.
*   **Compiler Implementation:**
    *   Implemented `PostgresCompiler` with automatic parametrization and `TOP` clause translation.
    *   Thoroughly documented the `compiler` module.
*   **Execution Layer Implementation:**
    *   Implemented `Executor` base class and `PostgresExecutor` using `psycopg`.
    *   Thoroughly documented the `execution` module.
*   **Testing Suite:**
    *   Set up `pytest` and `pytest-mock` in the virtual environment.
    *   Implemented exhaustive tests for AST models, traversal logic, compilers, and the execution layer.
    *   Verified all 20 tests pass.
*   **Documentation Finalization:**
    *   Updated all sub-package `README.md` files within `buildaquery` for both human and AI readability.
    *   Created a comprehensive root `README.md` with installation, testing, and usage instructions.
*   **Expanded AST and Compiler Capabilities**:
    *   **DML Support**: Implemented `InsertStatementNode`, `UpdateStatementNode`, and `DeleteStatementNode` to provide full data modification capabilities.
    *   **Advanced Query Features**:
        *   **CTEs**: Added `CTENode` and integrated `WITH` clause support into `SelectStatementNode`.
        *   **Subqueries**: Implemented `SubqueryNode` which acts as both an `ExpressionNode` and `FromClauseNode`, allowing nested queries in `FROM` and `WHERE` clauses.
        *   **Set Operations**: Added `UnionNode`, `IntersectNode`, and `ExceptNode` with support for `ALL` variants.
    *   **Rich Expression Logic**:
        *   **Conditional Logic**: Added `CaseExpressionNode` and `WhenThenNode` for SQL `CASE` statements.
        *   **Range & Membership**: Implemented `BetweenNode` and `InNode`.
        *   **Window Functions**: Introduced `OverClauseNode` and updated `FunctionCallNode` to support `OVER (PARTITION BY ... ORDER BY ...)`.
        *   **Type Casting**: Added `CastNode` for standard SQL type casting.
    *   **Qualified Names**: Updated `TableNode` and `ColumnNode` to support optional `schema`, `table`, and `alias` prefixes, ensuring robust naming for complex joins.
    *   **DDL Support**: Implemented `CreateStatementNode` and `DropStatementNode` (with `IF EXISTS`/`IF NOT EXISTS` and `CASCADE` support).
    *   **Visitor Enhancement**: Updated `PostgresCompiler` (Visitor) to handle all new AST nodes with dialect-correct ordering and syntax.
*   **Integration Testing Framework**:
    *   **Dockerized Environment**: Established a `docker-compose.yml` configuration for a PostgreSQL 15 test database, isolated on port `5433` to prevent conflicts with local instances.
    *   **Lifecycle Management**: Implemented `pytest` fixtures in `tests/conftest.py` that handle session-scoped connections and function-scoped schema management (automatic `CREATE` and `DROP` of tables).
    *   **Exhaustive Suite**: Created `tests/test_postgres_integration.py` which verifies the entire lifecycle (DDL -> DML -> Query -> Cleanup) against a live database.
*   **Enhanced Execution Layer**:
    *   **Persistent Connections**: Refactored `PostgresExecutor` to support an existing `psycopg` connection, facilitating transaction isolation in tests.
    *   **Internal Compilation**: Added an automatic compilation pipeline to `PostgresExecutor`. Methods now accept raw `ASTNode` objects and compile them on-the-fly if needed.
    *   **Raw Execution**: Added `execute_raw` for direct SQL utility tasks.
*   **Poetry Migration**:
    *   **Dependency Management**: Migrated the project to Poetry (`pyproject.toml`), replacing `requirements.txt` with a modern, deterministic dependency solver.
    *   **Custom CLI Scripts**: Implemented a suite of isolated commands via `poetry run`:
        *   `unit-tests`: Runs AST and Compiler unit tests.
        *   `integration-tests`: Runs the Docker-based database tests.
        *   `all-tests`: Executes the complete test suite.
        *   `clean`: Project-wide cleanup of `__pycache__` and test artifacts.
*   **SQLite Support**:
    *   Implemented `SqliteCompiler` with SQLite-compatible placeholders (`?`) and dialect notes.
    *   Implemented `SqliteExecutor` using Python's standard library `sqlite3`.
    *   Added SQLite unit tests and integration tests using the file-based DB at `static/test-sqlite/db.sqlite`.
    *   Added helper scripts to create the SQLite DB: `scripts/create_sqlite_db.ps1` and `scripts/create_sqlite_db.sh`.
    *   Added `setup-tests` Poetry script to bootstrap Postgres (Docker) and SQLite DB setup.
    *   Introduced shared `CompiledQuery` in `buildaquery/compiler/compiled_query.py`.
*   **MySQL Support**:
    *   Implemented `MySqlCompiler` with MySQL-compatible placeholders (`%s`) and dialect notes.
    *   Implemented `MySqlExecutor` using `mysql-connector-python`.
    *   Added MySQL unit tests and integration tests using Docker (`mysql:8.0`) on port `3307`.
    *   Extended integration test fixtures to manage MySQL connections and schema lifecycle.
    *   Added MySQL example script (`examples/sample_mysql.py`) and documentation updates across READMEs and `docs/docs.md`.
*   **Oracle Support**:
    *   Implemented `OracleCompiler` with Oracle-compatible placeholders (`:1`, `:2`, ...) and dialect notes.
    *   Implemented `OracleExecutor` using `oracledb`.
    *   Added Oracle unit tests and integration tests using Docker (`gvenzl/oracle-xe:21-slim`) on port `1522`.
    *   Extended integration test fixtures to manage Oracle connections and schema lifecycle.
    *   Added Oracle example script (`examples/sample_oracle.py`) and documentation updates across READMEs and `docs/docs.md`.
*   **SQL Server Support**:
    *   Implemented `MsSqlCompiler` with SQL Server-compatible placeholders (`?`) and dialect notes.
    *   Implemented `MsSqlExecutor` using `pyodbc`.
    *   Added SQL Server unit tests and integration tests using Docker (`mcr.microsoft.com/mssql/server:2022-latest`) with `MSSQL_PID=Express` on port `1434`.
    *   Extended integration test fixtures to manage SQL Server connections and database lifecycle.
    *   Added SQL Server example script (`examples/sample_mssql.py`) and documentation updates across READMEs and `docs/docs.md`.
    *   Normalized SQL Server integration test result rows to tuples for consistent assertions.
*   **MariaDB Support**:
    *   Implemented `MariaDbCompiler` with MariaDB-compatible placeholders (`?`) and dialect notes.
    *   Implemented `MariaDbExecutor` using `mariadb`.
    *   Added MariaDB unit tests and integration tests using Docker (`mariadb:11.4`) on port `3308`.
    *   Extended integration test fixtures to manage MariaDB connections and database lifecycle.
    *   Added MariaDB example script (`examples/sample_mariadb.py`) and documentation updates across READMEs and `docs/docs.md`.
*   **CockroachDB Support**:
    *   Implemented `CockroachDbCompiler` with CockroachDB-compatible placeholders (`%s`) and dialect notes.
    *   Implemented `CockroachExecutor` using `psycopg`.
    *   Added CockroachDB unit tests and integration tests using Docker (`cockroachdb/cockroach:v24.3.1`) on port `26258`.
    *   Extended integration test fixtures to manage CockroachDB connections and database lifecycle.
    *   Added CockroachDB example script (`examples/sample_cockroachdb.py`) and documentation updates across READMEs and `docs/docs.md`.
    *   Adjusted CockroachDB Docker SQL listener to use port `26258` and aligned test defaults with the new port.
    *   Cast CockroachDB string literals to `STRING` to avoid indeterminate parameter typing in `CASE` expressions.
*   **Dialect-Aware Upsert Support**:
    *   Added `ConflictTargetNode` and `UpsertClauseNode`, and extended `InsertStatementNode` with optional `upsert_clause`.
    *   Implemented compiler paths for:
        *   PostgreSQL, SQLite, CockroachDB: `ON CONFLICT (...) DO NOTHING/DO UPDATE`.
        *   MySQL, MariaDB: `ON DUPLICATE KEY UPDATE`.
        *   Oracle, SQL Server: `MERGE`-based upsert generation.
    *   Added comprehensive compiler unit tests and integration tests for upsert behavior (`tests/test_upsert_integration.py`).
    *   Updated project documentation and examples for upsert usage.
*   **Write-Return Payload Support**:
    *   Added `ReturningClauseNode` and extended `InsertStatementNode`, `UpdateStatementNode`, and `DeleteStatementNode` with optional `returning_clause`.
    *   Implemented compiler paths for:
        *   PostgreSQL, SQLite, CockroachDB: `RETURNING ...` for `INSERT`/`UPDATE`/`DELETE`.
        *   MariaDB: `RETURNING ...` for `INSERT`/`DELETE` (compiler rejects `UPDATE ... RETURNING`).
        *   SQL Server: `OUTPUT INSERTED...` / `OUTPUT DELETED...` for direct `INSERT`/`UPDATE`/`DELETE`.
        *   MySQL: explicit `ValueError` for generic `RETURNING` payloads.
        *   Oracle: explicit `ValueError` until `RETURNING ... INTO` out-bind support is added.
    *   Added comprehensive compiler unit tests and integration tests for write-return behavior (`tests/test_returning_integration.py`).
    *   Added a usage example (`examples/sample_returning.py`) and updated user/developer docs.
*   **Batch Write Support**:
    *   Extended `InsertStatementNode` with optional multi-row payload support via `rows`, while preserving single-row `values`.
    *   Updated all dialect compilers to compile multi-row insert payloads with dialect-aware behavior.
    *   Added `execute_many(sql, param_sets)` to the executor contract and implemented it across all executors.
    *   Added compiler and executor unit tests and a cross-dialect integration suite (`tests/test_batch_write_integration.py`).
    *   Added a usage example (`examples/sample_batch_write.py`) and updated user/developer docs.
*   **Expanded OLTP DDL Support**:
    *   Added table-level constraint AST nodes: `PrimaryKeyConstraintNode`, `UniqueConstraintNode`, `ForeignKeyConstraintNode`, and `CheckConstraintNode`.
    *   Added DDL statement/action nodes for index and schema evolution: `CreateIndexStatementNode`, `DropIndexStatementNode`, and `AlterTableStatementNode` with add/drop column/constraint actions.
    *   Extended all dialect compilers to handle expanded DDL paths with explicit dialect guards for unsupported forms.
    *   Added comprehensive unit coverage (`buildaquery/tests/test_ddl_oltp_compilers.py`) and cross-dialect integration coverage (`tests/test_ddl_constraints_integration.py`).
    *   Added DDL usage example (`examples/sample_ddl_constraints.py`) and updated project documentation.
*   **Normalized Execution Error + Retry Support**:
    *   Added normalized execution error taxonomy (`ExecutionError`, `TransientExecutionError`, `DeadlockError`, `SerializationError`, `LockTimeoutError`, `ConnectionTimeoutError`, `IntegrityConstraintError`, `ProgrammingExecutionError`).
    *   Added retry policy and execution retry runner (`RetryPolicy`, `run_with_retry`) for transient failures.
    *   Extended `Executor` with retry-enabled APIs: `execute_with_retry`, `fetch_all_with_retry`, `fetch_one_with_retry`, and `execute_many_with_retry`.
    *   Added unit coverage for error normalization and retry behavior (`buildaquery/tests/test_execution_error_model.py`, `buildaquery/tests/test_execution_retry.py`).
    *   Added SQLite integration coverage for normalized retry behavior under lock contention and integrity failures (`tests/test_execution_retry_integration.py`).
*   **Production-Grade Connection Management**:
    *   Added executor-level connection controls for pooling hooks (`acquire_connection`, `release_connection`) and dialected connect timeout configuration (`connect_timeout_seconds`).
    *   Added lifecycle controls across executors with `close()` and context-manager support, including rollback-safe cleanup for open explicit transactions.
    *   Added connection management unit coverage (`buildaquery/tests/test_execution_connection_management.py`) and SQLite integration coverage (`tests/test_connection_management_integration.py`).
*   **Execution Observability Hooks**:
    *   Added structured query observation types (`ObservabilitySettings`, `QueryObservation`) for per-query timing and structured logging/tracing metadata.
    *   Extended all executors to emit query events across `execute`, `fetch_all`, `fetch_one`, `execute_many`, and `execute_raw`.
    *   Added observability unit coverage (`buildaquery/tests/test_execution_observability.py`) and SQLite integration coverage (`tests/test_observability_integration.py`).
*   **Execution Lifecycle Logging Events**:
    *   Extended observability with `ExecutionEvent` and `ObservabilitySettings.event_observer` for structured lifecycle event emission.
    *   Added lifecycle event emission for query (`query.start`/`query.end`), retry (`retry.scheduled`/`retry.giveup`), transaction (`txn.begin`/`txn.commit`/`txn.rollback`), savepoint (`txn.savepoint.*`), and connection (`connection.acquire.*`, `connection.release`, `connection.close`) paths.
    *   Added built-in JSON logger helpers (`execution_event_to_dict`, `make_json_event_logger`) for immediate event log output via Python `logging`.
    *   Updated retry runner callbacks and executor instrumentation across all dialect executors.
    *   Expanded observability unit/integration coverage for lifecycle event payloads and ordering.
*   **Built-in Metrics + Tracing Adapters**:
    *   Added in-process observability adapters: `InMemoryMetricsAdapter` and `InMemoryTracingAdapter`.
    *   Added event fan-out helper `compose_event_observers(...)` to combine logger/metrics/tracing observers.
    *   Added unit and SQLite integration coverage for adapter behavior and end-to-end event-to-telemetry mapping.
    *   Updated examples and documentation to show combined JSON logging + metrics + tracing wiring.
*   **OLTP-Focused Integration Coverage**:
    *   Added `tests/test_oltp_integration.py` to validate contention with eventual retry success, deadlock normalization behavior, optimistic lost-update prevention patterns, isolation visibility semantics, and row-locking behavior (`FOR UPDATE NOWAIT` / `SKIP LOCKED`).
    *   Updated user/developer docs to include the OLTP integration coverage scope and test references.
*   **DuckDB Backend Support (Initial OLAP Entry Point)**:
    *   Added `DuckDbCompiler` and `DuckDbExecutor` with first-class AST compile + execution support.
    *   Added DuckDB unit coverage (`buildaquery/tests/test_compiler_duckdb.py`, `buildaquery/tests/test_execution_duckdb.py`) and integration coverage (`tests/test_duckdb_integration.py` plus cross-dialect matrix extensions).
    *   Added DuckDB example (`examples/sample_duckdb.py`) and updated user/developer documentation and install extras.
    *   **Important downstream maintenance**: keep DuckDB included in cross-dialect capability/test matrices and extras/docs when OLTP/OLAP features are expanded.
*   **ClickHouse Backend Support (OLAP Expansion)**:
    *   Added `ClickHouseCompiler` and `ClickHouseExecutor` with first-class AST compile + execution support.
    *   Added ClickHouse unit coverage (`buildaquery/tests/test_compiler_clickhouse.py`, `buildaquery/tests/test_execution_clickhouse.py`) and integration coverage (`tests/test_clickhouse_integration.py`).
    *   Added ClickHouse syntax example (`examples/sample_clickhouse.py`) and updated user/developer documentation and install extras.
    *   **Important downstream maintenance**: keep ClickHouse included in exports, dependency extras, and integration/docs matrices when OLTP/OLAP features are expanded.
*   **Public API Export Stabilization (PyPI Polish)**:
    *   Added explicit, tested public exports at package root (`buildaquery/__init__.py`) for core compiler/executor/retry/observability/error types and `__version__`.
    *   Extended subpackage exports to include `CompiledQuery` in `buildaquery.compiler` and `MetricPoint` in `buildaquery.execution`.
    *   Added unit coverage (`buildaquery/tests/test_public_api_exports.py`) to lock import stability for published consumers.
    *   **Important downstream maintenance**: whenever new public symbols are introduced, update `__all__` exports in the relevant `__init__.py` modules and extend `test_public_api_exports.py` so PyPI import contracts remain stable.
*   **GitHub Actions CI Guardrails (PyPI Polish)**:
    *   Added `.github/workflows/ci.yml` with lightweight CI checks on `push`/`pull_request` to `main`.
    *   CI now validates `poetry install`, `poetry run unit-tests`, and `poetry run package-check`.
    *   **Important downstream maintenance**: keep CI thin and script-driven; if local script commands change, preserve `unit-tests` and `package-check` compatibility or update workflow steps in lockstep.
*   **Optional Dialect Driver Extras (PyPI/CI Stability)**:
    *   Refactored DB driver dependencies (`psycopg`, `mysql-connector-python`, `mariadb`, `oracledb`, `pyodbc`, `clickhouse-driver`) to optional dependencies and mapped them under Poetry extras (`postgres`, `cockroach`, `mysql`, `mariadb`, `oracle`, `mssql`, `clickhouse`, `all-databases`).
    *   Updated installation documentation to instruct users to install extras for the specific backends they use.
    *   **Important downstream maintenance**: when adding a new dialect executor, update the extras mapping in `pyproject.toml`, the install docs, and CI/install profiles so default installs remain lightweight and deterministic.
*   **Optional Pydantic Boundary Validation (External Inputs)**:
    *   Added opt-in `buildaquery.validation` package with minimal Pydantic models for validating external executor config and raw SQL payloads (`ExecutorInputConfigModel`, `RawExecutionRequestModel`).
    *   Added translator helpers (`to_connection_settings_kwargs`, `to_retry_policy`, `to_raw_execution_payload`) to map validated inputs into existing executor/retry APIs without changing executor internals.
    *   Added unit and SQLite boundary integration tests for validation success/failure behavior.
    *   Updated user/developer docs and examples with optional install/use path (`buildaquery[validation]`, `examples/sample_validation.py`).
    *   **Important downstream maintenance**: keep validation logic at external boundaries; when adding new external input shapes, add corresponding model, translator, tests, and docs without coupling Pydantic into AST/compiler/executor cores.
*   **Raw SQL Policy Guardrails (`execute_raw`)**:
    *   Added executor-level `raw_sql_policy` controls across all dialect executors: `allow` (default), `deny_untrusted`, and `deny_all`.
    *   Extended `execute_raw` signature with `trusted: bool = False`; when policy requires trust, calls must pass `trusted=True`.
    *   Added centralized enforcement in the base executor, emitting `security.execute_raw.blocked` lifecycle events and raising `ProgrammingExecutionError` on blocked calls.
    *   Added unit and SQLite integration coverage for policy behavior and blocked-event emission.
    *   **Important downstream maintenance**: for any new executor, wire `raw_sql_policy` initialization and `execute_raw(..., trusted=...)` enforcement consistently, and extend security tests/docs together.
*   **Cross-Dialect Identifier Injection Hardening**:
    *   Added shared compiler identifier validation and wired it across PostgreSQL, SQLite, MySQL, MariaDB, CockroachDB, Oracle, SQL Server, DuckDB, and ClickHouse compiler paths.
    *   Hardened table/schema/column/alias handling in select/cte/subquery/table/ddl constraint/index/drop-column compilation paths to reject unsafe identifiers.
    *   Added cross-dialect hostile-input tests to verify unsafe identifiers are rejected and valid expressions like `COUNT(*)` continue to compile.
    *   **Important downstream maintenance**: when adding new compiler-emitted identifier fields, route them through identifier validation and extend `test_compiler_identifier_security.py`.
*   **Starter Templates for Common Flows**:
    *   Added `examples/sample_starter_templates.py` with syntax-first, copy-paste templates for CRUD, upsert, transaction, retry, and observability wiring.
    *   Updated user/developer docs to point to the starter template entrypoint.
    *   **Important downstream maintenance**: when the public AST/executor workflow changes for CRUD/upsert/transaction/retry/observability paths, keep `sample_starter_templates.py` and linked docs aligned.
*   **Normalized Error Message Context**:
    *   Extended normalized execution errors to include dialect, operation, SQLSTATE when available, and redacted placeholder SQL context.
    *   Added unit coverage to verify SQL context truncation and that hostile values do not leak into error messages.
    *   **Important downstream maintenance**: when adding new normalization paths, pass placeholder SQL context only and keep params out of exception strings and tests.
*   **Transaction Context Helper**:
    *   Added `executor.transaction(isolation_level=None)` as a shared context manager for automatic `begin()`/`commit()`/`rollback()` handling.
    *   Added unit and SQLite integration coverage for commit-on-success, rollback-on-error, and isolation-level forwarding behavior.
    *   Updated transaction examples and docs to show `with executor.transaction(): ...` as the preferred ergonomic path.
    *   **Important downstream maintenance**: when transaction lifecycle behavior changes, keep the explicit APIs and the context helper semantics aligned across executors and tests.
*   **Opt-In Row Shaping**:
    *   Added executor-level row shaping with `row_output="tuple" | "dict" | "model"` and optional `row_model=...`.
    *   Applied row shaping across row-returning `execute(...)`, `fetch_all(...)`, and `fetch_one(...)` paths.
    *   Added unit and SQLite integration coverage plus a syntax-oriented example (`examples/sample_row_shaping.py`).
    *   **Important downstream maintenance**: keep tuple output as the default for compatibility, and ensure new executors route cursor metadata through the shared shaping helpers.
*   **Named-Parameter Convenience For Manual SQL**:
    *   Added shared executor-side rewriting for dict-style params on manual `CompiledQuery(...)` and `execute_raw(...)` inputs using `:name` markers.
    *   Rewrites named markers into dialect-native placeholders (`%s`, `?`, or Oracle `:1`/`:2`/...) without interpolating values into SQL text.
    *   Added unit and SQLite integration coverage for quoted-literal/comment safety, missing-key errors, duplicate named markers, and hostile payload preservation.
    *   **Important downstream maintenance**: keep named-param rewriting scoped to manual SQL/executor inputs only, and extend the scanner/tests whenever a new placeholder dialect or SQL edge case is introduced.

---

## Instructions & Conventions

### Coding Style & Formatting
*   **Comprehensive Type Hinting:** All function signatures and class attributes must be explicitly type-hinted.
*   **Logical Code Grouping:** Use descriptive, prominent comment blocks (e.g., `# ==================`) to separate sections.
*   **Naming Conventions:** `CapWords` for classes, `snake_case` for variables and functions.

### Architecture & Logic
*   **AST Traversal:** Strictly adhere to the **Visitor Pattern**.
*   **Compilation**: Favor **post-order traversal** to ensure sub-expressions resolve before parent nodes.
*   **Execution**: Use `PostgresExecutor` for PostgreSQL, `SqliteExecutor` for SQLite, `DuckDbExecutor` for DuckDB, `ClickHouseExecutor` for ClickHouse, `MySqlExecutor` for MySQL, `MariaDbExecutor` for MariaDB, `CockroachExecutor` for CockroachDB, `OracleExecutor` for Oracle, and `MsSqlExecutor` for SQL Server to leverage automatic parametrization and compilation logic.
*   **SQLite Version Note**: SQLite is provided via Python's `sqlite3` module (version depends on Python build; check `sqlite3.sqlite_version`).

### Testing Workflow
1.  **Unit Tests**: Run `poetry run unit-tests` for rapid validation of AST/Compiler logic.
2.  **Integration Tests**:
    *   Ensure the database is up and SQLite DB exists: `poetry run setup-tests`.
    *   Run `poetry run integration-tests` (covers PostgreSQL, MySQL, MariaDB, CockroachDB, Oracle, SQL Server, ClickHouse, SQLite, and DuckDB).
3.  **Cleanup**: Periodically run `poetry run clean` to keep the workspace tidy.

### Edit Approval Protocol
*   Before writing any file edit, provide the following for review:
    *   **The change**: what will be modified.
    *   **Impact**: what the change will affect.
    *   **Downstream changes**: any additional updates needed because of this change.
    *   **Goal alignment**: how the change achieves the conversation goal.
*   Only write edits to files after explicit user approval.
*   Every new change written to a file must also include any necessary corresponding update to `AGENTS.md`.

### Security-First Update Requirement
*   For **all future updates**, perform a security analysis as part of the change.
*   If a security issue is found, prioritize catching and fixing it quickly in the same update scope whenever feasible.
*   Add appropriate security-focused tests (unit and/or integration) that verify both:
    *   vulnerable input is rejected/neutralized, and
    *   safe/expected behavior remains intact.
