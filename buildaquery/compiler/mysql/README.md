# MySQL Compiler

The `MySqlCompiler` translates the AST into MySQL-compatible SQL with `%s` placeholders.

## Notes

- **Placeholders**: Uses `%s` for parameters (compatible with `mysql-connector-python`).
- **TOP Translation**: `TopClauseNode` is translated into `LIMIT`, with optional implicit `ORDER BY`.
- **Unsupported Operations**:
  - `INTERSECT` and `EXCEPT` raise `ValueError` (MySQL does not support them).
  - `DROP TABLE ... CASCADE` raises `ValueError`.
- **Row Locking**: Supports `lock_clause` with `FOR UPDATE` / `FOR SHARE` and optional `NOWAIT` / `SKIP LOCKED`.

## Example

```python
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
from buildaquery.abstract_syntax_tree.models import SelectStatementNode, StarNode, TableNode

compiler = MySqlCompiler()
query = SelectStatementNode(select_list=[StarNode()], from_table=TableNode(name="users"))
compiled = compiler.compile(query)

print(compiled.sql)    # SELECT * FROM users
print(compiled.params) # []
```
