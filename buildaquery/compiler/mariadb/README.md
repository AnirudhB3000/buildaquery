# MariaDB Compiler

The `MariaDbCompiler` translates the AST into MariaDB-compatible SQL with `?` placeholders.

## Notes

- **Placeholders**: Uses `?` for parameters (compatible with the `mariadb` Python connector).
- **TOP Translation**: `TopClauseNode` is translated into `LIMIT`, with optional implicit `ORDER BY`.
- **Set Operations**: `UNION`, `INTERSECT`, and `EXCEPT` are supported (including `ALL` variants).
- **DROP TABLE ... CASCADE**: Accepted and passed through (MariaDB treats `CASCADE` as a no-op).
- **Row Locking**: Supports `lock_clause` with `FOR UPDATE` / `FOR SHARE` and optional `NOWAIT` / `SKIP LOCKED`.
- **Upsert**: Supports `InsertStatementNode.upsert_clause` as `ON DUPLICATE KEY UPDATE` via `update_columns`.
- **Upsert Limitation**: `do_nothing` and explicit `conflict_target` are rejected for MariaDB.
- **Write-Return Payloads**: Supports `returning_clause` and compiles to `RETURNING ...` on `INSERT`/`DELETE`; `UPDATE ... RETURNING` is rejected.

## Example

```python
from buildaquery.compiler.mariadb.mariadb_compiler import MariaDbCompiler
from buildaquery.abstract_syntax_tree.models import SelectStatementNode, StarNode, TableNode

compiler = MariaDbCompiler()
query = SelectStatementNode(select_list=[StarNode()], from_table=TableNode(name="users"))
compiled = compiler.compile(query)

print(compiled.sql)    # SELECT * FROM users
print(compiled.params) # []
```
