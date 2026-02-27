# SQL Server Compiler

The `MsSqlCompiler` translates the AST into SQL Server-compatible SQL with `?` placeholders.

## Notes

- **Placeholders**: Uses `?` for parameters (compatible with `pyodbc`).
- **TOP Support**: `TopClauseNode` compiles to `TOP n` and can inject an `ORDER BY` if needed.
- **LIMIT/OFFSET**: Uses `OFFSET ... ROWS` and `FETCH NEXT ... ROWS ONLY` (requires `ORDER BY`).
- **Set Operations**:
  - `UNION` and `INTERSECT` are supported.
  - `EXCEPT` is supported.
  - `INTERSECT ALL` and `EXCEPT ALL` raise `ValueError`.
- **DROP TABLE ... CASCADE**: Not supported; raises `ValueError`.
- **Row Locking**: `lock_clause` currently raises `ValueError` in this compiler because SQL Server locking typically uses table hints (`WITH (...)`) instead of trailing `FOR UPDATE` syntax.
- **Upsert**: `InsertStatementNode.upsert_clause` compiles through SQL Server `MERGE` generation.

## Example

```python
from buildaquery.compiler.mssql.mssql_compiler import MsSqlCompiler
from buildaquery.abstract_syntax_tree.models import SelectStatementNode, StarNode, TableNode

compiler = MsSqlCompiler()
query = SelectStatementNode(select_list=[StarNode()], from_table=TableNode(name="users"))
compiled = compiler.compile(query)

print(compiled.sql)    # SELECT * FROM users
print(compiled.params) # []
```
