import os
import sqlite3
import threading
import time
import uuid

import pytest
import psycopg

from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnNode,
    LockClauseNode,
    LiteralNode,
    OrderByClauseNode,
    ReturningClauseNode,
    SelectStatementNode,
    TableNode,
    UpdateStatementNode,
    WhereClauseNode,
)
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.errors import DeadlockError, LockTimeoutError
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.retry import RetryPolicy
from buildaquery.execution.sqlite import SqliteExecutor


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")


def _postgres_executor_or_skip() -> PostgresExecutor:
    try:
        return PostgresExecutor(connection_info=DATABASE_URL)
    except Exception as exc:
        pytest.skip(f"postgres integration unavailable: {exc}")
        raise


def _sqlite_db_path(prefix: str) -> str:
    db_dir = os.path.join("static", "test-sqlite")
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, f"{prefix}_{uuid.uuid4().hex}.sqlite")


def test_sqlite_contention_retry_eventually_succeeds() -> None:
    db_path = _sqlite_db_path("oltp_retry")
    lock_conn = sqlite3.connect(db_path, timeout=0.1, check_same_thread=False)
    runner_conn = sqlite3.connect(db_path, timeout=0.1, check_same_thread=False)
    setup_conn = sqlite3.connect(db_path, timeout=0.1, check_same_thread=False)
    executor = SqliteExecutor(connection=runner_conn)

    try:
        setup_conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
        setup_conn.execute("INSERT INTO items (id, value) VALUES (?, ?)", [1, "locked"])
        setup_conn.commit()

        lock_conn.execute("BEGIN EXCLUSIVE")
        lock_conn.execute("UPDATE items SET value = ? WHERE id = ?", ["holding", 1])

        def release_lock() -> None:
            time.sleep(0.2)
            lock_conn.rollback()

        releaser = threading.Thread(target=release_lock, daemon=True)
        releaser.start()

        executor.execute_with_retry(
            CompiledQuery(sql="INSERT INTO items (id, value) VALUES (?, ?)", params=[2, "after-lock"]),
            retry_policy=RetryPolicy(max_attempts=6, base_delay_seconds=0.05, backoff_multiplier=1.0),
        )
        releaser.join(timeout=1.0)

        rows = executor.fetch_all(CompiledQuery(sql="SELECT id FROM items ORDER BY id", params=[]))
        assert rows == [(1,), (2,)]
    finally:
        setup_conn.close()
        lock_conn.close()
        runner_conn.close()
        if os.path.exists(db_path):
            os.remove(db_path)


def test_sqlite_isolation_hides_uncommitted_changes() -> None:
    db_path = _sqlite_db_path("oltp_isolation")
    writer_conn = sqlite3.connect(db_path, timeout=0.1)
    reader_conn = sqlite3.connect(db_path, timeout=0.1)
    setup_conn = sqlite3.connect(db_path, timeout=0.1)
    writer = SqliteExecutor(connection=writer_conn)
    reader = SqliteExecutor(connection=reader_conn)

    try:
        setup_conn.execute("CREATE TABLE tx_visibility (id INTEGER PRIMARY KEY, value TEXT)")
        setup_conn.commit()

        writer.begin("IMMEDIATE")
        writer.execute_raw("INSERT INTO tx_visibility (id, value) VALUES (?, ?)", [1, "pending"])

        before_commit = reader.fetch_one(
            CompiledQuery(sql="SELECT COUNT(*) FROM tx_visibility WHERE id = ?", params=[1])
        )
        assert before_commit == (0,)

        writer.commit()

        after_commit = reader.fetch_one(
            CompiledQuery(sql="SELECT COUNT(*) FROM tx_visibility WHERE id = ?", params=[1])
        )
        assert after_commit == (1,)
    finally:
        try:
            writer.rollback()
        except Exception:
            pass
        setup_conn.close()
        writer_conn.close()
        reader_conn.close()
        if os.path.exists(db_path):
            os.remove(db_path)


def test_sqlite_optimistic_update_prevents_lost_update() -> None:
    db_path = _sqlite_db_path("oltp_lost_update")
    conn = sqlite3.connect(db_path, timeout=0.1)
    executor = SqliteExecutor(connection=conn)

    try:
        executor.execute_raw(
            "CREATE TABLE account (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL, version INTEGER NOT NULL)"
        )
        executor.execute_raw("INSERT INTO account (id, balance, version) VALUES (?, ?, ?)", [1, 100, 1])

        first_update = executor.execute(
            UpdateStatementNode(
                table=TableNode(name="account"),
                set_clauses={
                    "balance": LiteralNode(value=90),
                    "version": BinaryOperationNode(
                        left=ColumnNode(name="version"),
                        operator="+",
                        right=LiteralNode(value=1),
                    ),
                },
                where_clause=WhereClauseNode(
                    condition=BinaryOperationNode(
                        left=BinaryOperationNode(
                            left=ColumnNode(name="id"),
                            operator="=",
                            right=LiteralNode(value=1),
                        ),
                        operator="AND",
                        right=BinaryOperationNode(
                            left=ColumnNode(name="version"),
                            operator="=",
                            right=LiteralNode(value=1),
                        ),
                    )
                ),
                returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id")]),
            )
        )
        assert first_update == [(1,)]

        second_update = executor.execute(
            UpdateStatementNode(
                table=TableNode(name="account"),
                set_clauses={
                    "balance": LiteralNode(value=80),
                    "version": BinaryOperationNode(
                        left=ColumnNode(name="version"),
                        operator="+",
                        right=LiteralNode(value=1),
                    ),
                },
                where_clause=WhereClauseNode(
                    condition=BinaryOperationNode(
                        left=BinaryOperationNode(
                            left=ColumnNode(name="id"),
                            operator="=",
                            right=LiteralNode(value=1),
                        ),
                        operator="AND",
                        right=BinaryOperationNode(
                            left=ColumnNode(name="version"),
                            operator="=",
                            right=LiteralNode(value=1),
                        ),
                    )
                ),
                returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id")]),
            )
        )
        assert second_update == []

        row = executor.fetch_one(
            CompiledQuery(sql="SELECT balance, version FROM account WHERE id = ?", params=[1])
        )
        assert row == (90, 2)
    finally:
        conn.close()
        if os.path.exists(db_path):
            os.remove(db_path)


def test_postgres_for_update_nowait_and_skip_locked_behavior() -> None:
    holder = _postgres_executor_or_skip()
    contender = _postgres_executor_or_skip()
    table_name = f"oltp_locks_{uuid.uuid4().hex[:8]}"
    table = TableNode(name=table_name)

    try:
        holder.execute_raw(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, value TEXT)")
        holder.execute_raw(f"INSERT INTO {table_name} (id, value) VALUES (%s, %s)", [1, "a"])
        holder.execute_raw(f"INSERT INTO {table_name} (id, value) VALUES (%s, %s)", [2, "b"])

        holder.begin()
        holder.execute(
            SelectStatementNode(
                select_list=[ColumnNode(name="id")],
                from_table=table,
                where_clause=WhereClauseNode(
                    condition=BinaryOperationNode(
                        left=ColumnNode(name="id"),
                        operator="=",
                        right=LiteralNode(value=1),
                    )
                ),
                lock_clause=LockClauseNode(mode="UPDATE"),
            )
        )

        with pytest.raises(LockTimeoutError):
            contender.execute_with_retry(
                contender.compiler.compile(
                    SelectStatementNode(
                        select_list=[ColumnNode(name="id")],
                        from_table=table,
                        where_clause=WhereClauseNode(
                            condition=BinaryOperationNode(
                                left=ColumnNode(name="id"),
                                operator="=",
                                right=LiteralNode(value=1),
                            )
                        ),
                        lock_clause=LockClauseNode(mode="UPDATE", nowait=True),
                    )
                ),
                retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0.0),
            )

        skipped_rows = contender.execute(
            SelectStatementNode(
                select_list=[ColumnNode(name="id")],
                from_table=table,
                order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="id"), direction="ASC")],
                lock_clause=LockClauseNode(mode="UPDATE", skip_locked=True),
            )
        )
        assert skipped_rows == [(2,)]
    except psycopg.OperationalError as exc:
        pytest.skip(f"postgres integration unavailable: {exc}")
    finally:
        try:
            holder.rollback()
        except Exception:
            pass
        holder.execute_raw(f"DROP TABLE IF EXISTS {table_name}")
        holder.close()
        contender.close()


def test_postgres_deadlock_maps_to_normalized_error() -> None:
    left = _postgres_executor_or_skip()
    right = _postgres_executor_or_skip()
    table_name = f"oltp_deadlock_{uuid.uuid4().hex[:8]}"

    try:
        left.execute_raw(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, value INTEGER)")
        left.execute_raw(f"INSERT INTO {table_name} (id, value) VALUES (%s, %s)", [1, 0])
        left.execute_raw(f"INSERT INTO {table_name} (id, value) VALUES (%s, %s)", [2, 0])

        left.begin()
        right.begin()
        left.execute_raw(f"UPDATE {table_name} SET value = value + 1 WHERE id = %s", [1])
        right.execute_raw(f"UPDATE {table_name} SET value = value + 1 WHERE id = %s", [2])

        left_error: list[Exception] = []

        def left_waiting_update() -> None:
            try:
                left.execute_with_retry(
                    CompiledQuery(
                        sql=f"UPDATE {table_name} SET value = value + 1 WHERE id = %s",
                        params=[2],
                    ),
                    retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0.0),
                )
            except Exception as exc:
                left_error.append(exc)

        thread = threading.Thread(target=left_waiting_update, daemon=True)
        thread.start()
        time.sleep(0.15)

        right_error: Exception | None = None
        try:
            right.execute_with_retry(
                CompiledQuery(
                    sql=f"UPDATE {table_name} SET value = value + 1 WHERE id = %s",
                    params=[1],
                ),
                retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0.0),
            )
        except Exception as exc:
            right_error = exc

        thread.join(timeout=2.0)
        deadlock_seen = isinstance(right_error, DeadlockError) or any(
            isinstance(exc, DeadlockError) for exc in left_error
        )
        assert deadlock_seen
    except psycopg.OperationalError as exc:
        pytest.skip(f"postgres integration unavailable: {exc}")
    finally:
        try:
            left.rollback()
        except Exception:
            pass
        try:
            right.rollback()
        except Exception:
            pass
        left.execute_raw(f"DROP TABLE IF EXISTS {table_name}")
        left.close()
        right.close()
