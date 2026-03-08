# DuckDB Compiler

`DuckDbCompiler` compiles AST nodes to DuckDB SQL using `?` placeholders.

## Notes

- Placeholder style: `?`
- Core behavior follows the SQLite compiler strategy in this project.
- Row-lock clauses (`FOR UPDATE` / `FOR SHARE`) are rejected.
