# Build-a-Query Project Goals

This project aims to create a query builder for Python with support for PostgreSQL, SQLite, MySQL, MariaDB, CockroachDB, Oracle, and SQL Server. The development process is broken down into the following stages: 

1.  **Abstract Syntax Tree (AST):** Define and build the necessary data structures to represent queries as an AST.
2.  **AST Traversal:** Implement mechanisms for traversing the AST to analyze, modify, and process the query structure.
3.  **SQL Compiler:** Develop compilers that translate the AST into valid SQL queries for PostgreSQL, SQLite, MySQL, MariaDB, CockroachDB, Oracle, and SQL Server.
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

---

## Instructions & Conventions

### Coding Style & Formatting
*   **Comprehensive Type Hinting:** All function signatures and class attributes must be explicitly type-hinted.
*   **Logical Code Grouping:** Use descriptive, prominent comment blocks (e.g., `# ==================`) to separate sections.
*   **Naming Conventions:** `CapWords` for classes, `snake_case` for variables and functions.

### Architecture & Logic
*   **AST Traversal:** Strictly adhere to the **Visitor Pattern**.
*   **Compilation**: Favor **post-order traversal** to ensure sub-expressions resolve before parent nodes.
*   **Execution**: Use `PostgresExecutor` for PostgreSQL, `SqliteExecutor` for SQLite, `MySqlExecutor` for MySQL, `MariaDbExecutor` for MariaDB, `CockroachExecutor` for CockroachDB, `OracleExecutor` for Oracle, and `MsSqlExecutor` for SQL Server to leverage automatic parametrization and compilation logic.
*   **SQLite Version Note**: SQLite is provided via Python's `sqlite3` module (version depends on Python build; check `sqlite3.sqlite_version`).

### Testing Workflow
1.  **Unit Tests**: Run `poetry run unit-tests` for rapid validation of AST/Compiler logic.
2.  **Integration Tests**:
    *   Ensure the database is up and SQLite DB exists: `poetry run setup-tests`.
    *   Run `poetry run integration-tests` (covers PostgreSQL, MySQL, MariaDB, CockroachDB, Oracle, SQL Server, and SQLite).
3.  **Cleanup**: Periodically run `poetry run clean` to keep the workspace tidy.

### Edit Approval Protocol
*   Before writing any file edit, provide the following for review:
    *   **The change**: what will be modified.
    *   **Impact**: what the change will affect.
    *   **Downstream changes**: any additional updates needed because of this change.
    *   **Goal alignment**: how the change achieves the conversation goal.
*   Only write edits to files after explicit user approval.
