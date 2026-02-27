import sqlite3
from pathlib import Path

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor


def main() -> None:
    db_dir = Path("static") / "test-sqlite"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "connection_management_example.sqlite"

    # Lifecycle control with context manager + connect timeout.
    with SqliteExecutor(
        connection_info=str(db_path),
        connect_timeout_seconds=3.0,
    ) as executor:
        executor.execute_raw("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
        executor.execute(
            CompiledQuery(
                sql="INSERT INTO users (id, name) VALUES (?, ?)",
                params=[1, "Alice"],
            )
        )
        rows = executor.fetch_all(CompiledQuery(sql="SELECT id, name FROM users", params=[]))
        print("context-managed rows:", rows)

    # Pool hooks: external acquire/release wiring.
    def acquire_connection() -> sqlite3.Connection:
        return sqlite3.connect(str(db_path))

    def release_connection(conn: sqlite3.Connection) -> None:
        conn.close()

    pooled_executor = SqliteExecutor(
        acquire_connection=acquire_connection,
        release_connection=release_connection,
        connect_timeout_seconds=3.0,
    )
    rows = pooled_executor.fetch_all(CompiledQuery(sql="SELECT id, name FROM users", params=[]))
    print("pooled rows:", rows)
    pooled_executor.close()


if __name__ == "__main__":
    main()
