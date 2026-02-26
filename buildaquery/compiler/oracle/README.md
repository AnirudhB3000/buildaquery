# Oracle Compiler

The `OracleCompiler` translates the AST into Oracle-compatible SQL with positional `:1`, `:2`, ... placeholders.

## Notes

- **Placeholders**: Uses Oracle-style positional binds (`:1`, `:2`, ...), compatible with `oracledb`.
- **TOP Translation**: `TopClauseNode` is translated into `FETCH FIRST ... ROWS ONLY`, with optional implicit `ORDER BY`.
- **LIMIT/OFFSET**: Uses `OFFSET ... ROWS` and `FETCH FIRST ... ROWS ONLY`.
- **Set Operations**:
  - `UNION` and `INTERSECT` are supported.
  - `EXCEPT` is mapped to Oracle `MINUS`.
  - `INTERSECT ALL` and `MINUS ALL` raise `ValueError`.
- **CREATE/DROP Limitations**:
  - `IF NOT EXISTS` and `IF EXISTS` raise `ValueError` (not supported in Oracle SQL).
  - `DROP TABLE ... CASCADE` is translated to `DROP TABLE ... CASCADE CONSTRAINTS`.
- **Table Aliases**: Oracle does not allow `AS` for table aliases; the compiler emits `table alias`.

## Example

```python
from buildaquery.compiler.oracle.oracle_compiler import OracleCompiler
from buildaquery.abstract_syntax_tree.models import SelectStatementNode, StarNode, TableNode

compiler = OracleCompiler()
query = SelectStatementNode(select_list=[StarNode()], from_table=TableNode(name="users"))
compiled = compiler.compile(query)

print(compiled.sql)    # SELECT * FROM users
print(compiled.params) # []
```
