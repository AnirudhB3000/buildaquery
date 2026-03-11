from dataclasses import dataclass
import sqlite3

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor


@dataclass
class _RowModel:
    id: int
    value: str


def test_sqlite_row_shaping_modes_integration() -> None:
    conn = sqlite3.connect(":memory:")
    tuple_executor = SqliteExecutor(connection=conn)
    tuple_executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
    tuple_executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "x"]))

    dict_executor = SqliteExecutor(connection=conn, row_output="dict")
    model_executor = SqliteExecutor(connection=conn, row_output="model", row_model=_RowModel)

    tuple_rows = tuple_executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t", params=[]))
    dict_rows = dict_executor.fetch_all(CompiledQuery(sql="SELECT id, value FROM t", params=[]))
    model_row = model_executor.fetch_one(CompiledQuery(sql="SELECT id, value FROM t", params=[]))

    assert tuple_rows == [(1, "x")]
    assert dict_rows == [{"id": 1, "value": "x"}]
    assert model_row == _RowModel(id=1, value="x")
    conn.close()
