# Compiler

The `compiler` module is responsible for translating the dialect-agnostic Abstract Syntax Tree (AST) into specific SQL dialects. 

## Core Concepts

### `CompiledQuery` Dataclass
Every compiler returns a `CompiledQuery` object instead of a raw string. This ensures that queries are handled safely and consistently.

- **`sql` (str)**: The SQL query string containing placeholders (e.g., `%s` for PostgreSQL).
- **`params` (list[Any])**: A list of values corresponding to the placeholders in the SQL string.

### Parametrization
To prevent SQL injection, the compilers are designed to automatically parametrize all literal values. When a `LiteralNode` is encountered, the compiler:
1. Appends a placeholder to the SQL string.
2. Appends the literal value to the `params` list.

## Implementations

### PostgreSQL (`PostgresCompiler`)
The initial implementation supports PostgreSQL.

#### Key Features:
- **DISTINCT Support**: Handles `SELECT DISTINCT` queries.
- **DML Support**: Supports `INSERT`, `UPDATE`, and `DELETE` operations.
- **Set Operations**: Support for `UNION`, `INTERSECT`, and `EXCEPT` (including `ALL`).
- **CTEs**: Support for `WITH` clauses.
- **Specialized Expressions**: Support for `IN`, `BETWEEN`, and `CASE`.
- **Subqueries**: Support for subqueries in `FROM` and `WHERE` clauses.
- **Window Functions**: Support for `OVER` clauses, `PARTITION BY`, and `ORDER BY`.
- **DDL Support**: Support for `CREATE TABLE` and `DROP TABLE`.
- **Qualified Names**: Supports `schema.table` and `table.column` naming conventions.
- **Type Casting**: Supports `CAST(expression AS type)`.
- **Clause Ordering**: Ensures `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT` are placed in the correct sequence.
- **TOP Translation**: Automatically translates `TopClauseNode` into PostgreSQL-compliant `LIMIT` and `ORDER BY` logic.
- **Mutual Exclusivity Enforcement**: Raises a `ValueError` if a query attempts to use both `TOP` and standard `LIMIT/OFFSET`.

### SQLite (`SqliteCompiler`)

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

#### Key Features:
- **`?` Placeholders**: Uses SQLite's parameter style.
- **Core AST Coverage**: Supports the same AST nodes as PostgreSQL where SQLite syntax allows.
- **TOP Translation**: Maps `TopClauseNode` to `LIMIT`, with optional implicit `ORDER BY`.
- **CASCADE Handling**: Raises a `ValueError` when `DropStatementNode.cascade=True`, because SQLite does not support `DROP TABLE ... CASCADE`.

### MySQL (`MySqlCompiler`)

#### Key Features:
- **`%s` Placeholders**: Uses MySQL-compatible parameter style.
- **Core AST Coverage**: Supports the same AST nodes as PostgreSQL where MySQL syntax allows.
- **TOP Translation**: Maps `TopClauseNode` to `LIMIT`, with optional implicit `ORDER BY`.
- **Set Operation Limits**: Raises a `ValueError` for `INTERSECT` and `EXCEPT` (unsupported in MySQL).
- **CASCADE Handling**: Raises a `ValueError` when `DropStatementNode.cascade=True`, because MySQL does not support `DROP TABLE ... CASCADE`.

### Oracle (`OracleCompiler`)

#### Key Features:
- **`:1` Placeholders**: Uses Oracle-style positional bind parameters.
- **LIMIT/OFFSET Translation**: Uses `OFFSET ... ROWS` and `FETCH FIRST ... ROWS ONLY`.
- **TOP Translation**: Maps `TopClauseNode` to `FETCH FIRST`, with optional implicit `ORDER BY`.
- **Set Operation Notes**:
  - `EXCEPT` is translated to `MINUS`.
  - `INTERSECT ALL` and `MINUS ALL` raise `ValueError`.
- **Table Aliases**: Emits `table alias` (Oracle does not allow `AS` for table aliases).
- **IF EXISTS/IF NOT EXISTS**: Raises `ValueError` for `DropStatementNode.if_exists=True` and `CreateStatementNode.if_not_exists=True`.

## Usage Example

```python
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
from buildaquery.compiler.oracle.oracle_compiler import OracleCompiler

compiler = PostgresCompiler()
compiled = compiler.compile(ast_root)

print(compiled.sql)    # "SELECT * FROM users WHERE id = %s"
print(compiled.params) # [123]

sqlite_compiler = SqliteCompiler()
compiled = sqlite_compiler.compile(ast_root)
print(compiled.sql)    # "SELECT * FROM users WHERE id = ?"
print(compiled.params) # [123]

mysql_compiler = MySqlCompiler()
compiled = mysql_compiler.compile(ast_root)
print(compiled.sql)    # "SELECT * FROM users WHERE id = %s"
print(compiled.params) # [123]

oracle_compiler = OracleCompiler()
compiled = oracle_compiler.compile(ast_root)
print(compiled.sql)    # "SELECT * FROM users WHERE id = :1"
print(compiled.params) # [123]
```
