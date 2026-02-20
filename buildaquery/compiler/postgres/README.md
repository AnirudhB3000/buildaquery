# PostgreSQL Compiler

This sub-module provides the concrete implementation of the SQL compiler for PostgreSQL.

## Features

- **Standard SELECT Support**: Compiles columns, tables, and joins.
- **Filtering**: Translates `WhereClauseNode` and complex binary/unary operations.
- **Parametrization**: Automatically uses `%s` placeholders for all literal values to ensure security.
- **Dialect Specifics**: 
    - Translates `TopClauseNode` into a combination of `LIMIT` and `ORDER BY`.
    - Handles PostgreSQL-specific operator precedence through grouping.

## Implementation Details

The compiler is implemented as a `Visitor` that recursively traverses the AST. For expression nodes, it generally follows a post-order traversal to build the SQL string from the bottom up.
