# Build-a-Query Project Goals

This project aims to create a query builder for Python with support for PostgreSQL, SQLite, and MySQL. The development process is broken down into the following stages: 

1.  **Abstract Syntax Tree (AST):** Define and build the necessary data structures to represent queries as an AST.
2.  **AST Traversal:** Implement mechanisms for traversing the AST to analyze, modify, and process the query structure.
3.  **SQL Compiler:** Develop compilers that translate the AST into valid SQL queries for PostgreSQL, SQLite, and MySQL.
4.  **Execution Layer:** Create layers responsible for executing the compiled SQL queries against the target databases.

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

---

## Instructions & Conventions

### Coding Style & Formatting
*   **Comprehensive Type Hinting:** All function signatures and class attributes must be explicitly type-hinted.
*   **Logical Code Grouping:** Use descriptive, prominent comment blocks (e.g., `# ==================`) to separate sections.
*   **Naming Conventions:** `CapWords` for classes, `snake_case` for variables and functions.

### Architecture & Logic
*   **AST Traversal:** Strictly adhere to the **Visitor Pattern**.
*   **Compilation**: Favor **post-order traversal** to ensure sub-expressions resolve before parent nodes.
*   **Execution**: Use `PostgresExecutor` for PostgreSQL, `SqliteExecutor` for SQLite, and `MySqlExecutor` for MySQL to leverage automatic parametrization and compilation logic.
*   **SQLite Version Note**: SQLite is provided via Python's `sqlite3` module (version depends on Python build; check `sqlite3.sqlite_version`).

### Testing Workflow
1.  **Unit Tests**: Run `poetry run unit-tests` for rapid validation of AST/Compiler logic.
2.  **Integration Tests**:
    *   Ensure the database is up and SQLite DB exists: `poetry run setup-tests`.
    *   Run `poetry run integration-tests` (covers PostgreSQL and SQLite).
3.  **Cleanup**: Periodically run `poetry run clean` to keep the workspace tidy.
