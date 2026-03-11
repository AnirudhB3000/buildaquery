from pathlib import Path
from uuid import uuid4

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor


def test_sqlite_context_manager_rolls_back_open_transaction() -> None:
    db_dir = Path("static") / "test-sqlite"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / f"conn_mgmt_{uuid4().hex}.sqlite"

    try:
        with SqliteExecutor(connection_info=str(db_path)) as executor:
            executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
            executor.begin()
            executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "x"]))

        verify = SqliteExecutor(connection_info=str(db_path))
        try:
            rows = verify.fetch_all(CompiledQuery(sql="SELECT id, value FROM t", params=[]))
        finally:
            verify.close()

        assert rows == []
    finally:
        if db_path.exists():
            db_path.unlink()


def test_sqlite_transaction_context_helper_commits_and_rolls_back() -> None:
    db_dir = Path("static") / "test-sqlite"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / f"txn_ctx_{uuid4().hex}.sqlite"

    try:
        executor = SqliteExecutor(connection_info=str(db_path))
        executor.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")

        with executor.transaction():
            executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[1, "commit"]))

        with executor.transaction():
            executor.execute(CompiledQuery(sql="INSERT INTO t (id, value) VALUES (?, ?)", params=[2, "rollback"]))
            raise RuntimeError("force rollback")
    except RuntimeError as exc:
        assert str(exc) == "force rollback"
        verify = SqliteExecutor(connection_info=str(db_path))
        try:
            rows = verify.fetch_all(CompiledQuery(sql="SELECT id, value FROM t ORDER BY id", params=[]))
        finally:
            verify.close()

        assert rows == [(1, "commit")]
    finally:
        if db_path.exists():
            db_path.unlink()
