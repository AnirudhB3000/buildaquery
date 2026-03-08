# ClickHouse Compiler

`ClickHouseCompiler` compiles Build-a-Query AST nodes into ClickHouse SQL.

## Notes

- Placeholder style: `%s`
- Unsupported in this compiler:
  - `lock_clause` (`FOR UPDATE` / `FOR SHARE`)
  - `upsert_clause`
  - `RETURNING` payloads
  - `DROP TABLE ... CASCADE`
