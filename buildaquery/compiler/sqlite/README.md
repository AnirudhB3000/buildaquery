# SQLite Compiler

The SQLite compiler translates the AST into SQLite-compatible SQL.

**SQLite Version**: SQLite 3.x via Python's `sqlite3` module (the exact SQLite version depends on your Python build; check `sqlite3.sqlite_version` at runtime).

**Dialect Notes**: This compiler targets SQLite. See the PostgreSQL and MySQL compiler READMEs for their dialect-specific notes.

## Notes

- Uses `?` placeholders for parametrized values.
- Supports `WITH`, set operations, and window functions (requires SQLite 3.25+ for window functions).
- `TOP` is translated to `LIMIT`.
- `DROP TABLE ... CASCADE` is not supported by SQLite; the compiler raises a `ValueError` if `cascade=True`.
- `lock_clause` is not supported in the SQLite compiler; it raises `ValueError` for row-lock clauses.
- Upsert support is available via `InsertStatementNode.upsert_clause` and compiles to `ON CONFLICT (...) DO NOTHING/DO UPDATE`.
