# CockroachDB Compiler

The `CockroachDbCompiler` translates the AST into CockroachDB-compatible SQL with `%s` placeholders.

## Notes

- **Placeholders**: Uses `%s` for parameters (compatible with `psycopg`).
- **TOP Translation**: `TopClauseNode` is translated into `LIMIT`, with optional implicit `ORDER BY`.
- **Set Operations**: `UNION`, `INTERSECT`, and `EXCEPT` are supported (including `ALL` variants).
- **DROP TABLE ... CASCADE**: Supported and emitted when requested.
- **Row Locking**: Supports `lock_clause` with `FOR UPDATE` / `FOR SHARE` and optional `NOWAIT` / `SKIP LOCKED`.

## Example

```python
from buildaquery.compiler.cockroachdb.cockroachdb_compiler import CockroachDbCompiler
from buildaquery.abstract_syntax_tree.models import SelectStatementNode, StarNode, TableNode

compiler = CockroachDbCompiler()
query = SelectStatementNode(select_list=[StarNode()], from_table=TableNode(name="users"))
compiled = compiler.compile(query)

print(compiled.sql)    # SELECT * FROM users
print(compiled.params) # []
```
