# PostgreSQL Compiler

This sub-module provides the concrete implementation of the SQL compiler for PostgreSQL.

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).


## Features

- **Standard DML Support**: Compiles `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements.
- **Set Operations**: Supports `UNION`, `INTERSECT`, and `EXCEPT` (and their `ALL` variants).
- **CTEs**: Supports Common Table Expressions (`WITH` clause).
- **Specialized Expressions**: Supports `IN`, `BETWEEN`, and `CASE` operators.
- **Subqueries**: Handles subqueries in `FROM` and `WHERE` clauses.
- **Window Functions**: Supports window functions with `OVER`, `PARTITION BY`, and `ORDER BY`.
- **DDL Support**: Handles `CREATE TABLE` and `DROP TABLE` statements.
- **DISTINCT**: Supports `SELECT DISTINCT`.
- **Qualified Names**: Handles `schema.table` and `table.column` identifiers.
- **Type Casting**: Supports `CAST(expression AS type)`.
- **Filtering**: Translates `WhereClauseNode` and complex binary/unary operations.
- **Parametrization**: Automatically uses `%s` placeholders for all literal values to ensure security.
- **Dialect Specifics**: 
    - Translates `TopClauseNode` into a combination of `LIMIT` and `ORDER BY`.
    - Handles PostgreSQL-specific operator precedence through grouping.

## Implementation Details

The compiler is implemented as a `Visitor` that recursively traverses the AST. For expression nodes, it generally follows a post-order traversal to build the SQL string from the bottom up.
