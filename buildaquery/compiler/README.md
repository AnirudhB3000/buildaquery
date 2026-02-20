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
- **Clause Ordering**: Ensures `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT` are placed in the correct sequence.
- **TOP Translation**: Automatically translates `TopClauseNode` into PostgreSQL-compliant `LIMIT` and `ORDER BY` logic.
- **Mutual Exclusivity Enforcement**: Raises a `ValueError` if a query attempts to use both `TOP` and standard `LIMIT/OFFSET`.

## Usage Example

```python
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler

compiler = PostgresCompiler()
compiled = compiler.compile(ast_root)

print(compiled.sql)    # "SELECT * FROM users WHERE id = %s"
print(compiled.params) # [123]
```
