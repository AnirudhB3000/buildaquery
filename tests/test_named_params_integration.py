import sqlite3

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor


def test_named_params_integration_sqlite_keeps_payload_parameterized() -> None:
    conn = sqlite3.connect(":memory:")
    executor = SqliteExecutor(connection=conn)
    hostile = "x'); DROP TABLE notes; --"

    executor.execute_raw("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
    executor.execute(
        CompiledQuery(
            sql="INSERT INTO notes (id, body) VALUES (:id, :body)",
            params={"id": 1, "body": hostile},
        )
    )

    row = executor.fetch_one(
        CompiledQuery(
            sql="SELECT id, body FROM notes WHERE id = :id",
            params={"id": 1},
        )
    )
    tables = executor.fetch_all(
        CompiledQuery(
            sql="SELECT name FROM sqlite_master WHERE type = :kind AND name = :name",
            params={"kind": "table", "name": "notes"},
        )
    )

    assert row == (1, hostile)
    assert tables == [("notes",)]
    conn.close()
