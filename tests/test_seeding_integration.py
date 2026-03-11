from buildaquery.abstract_syntax_tree.models import ColumnNode, InsertStatementNode, LiteralNode, TableNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.seeding import SeedRunError, SeedRunner, SeedStep


def test_sqlite_seed_runner_inserts_rows(sqlite_executor) -> None:
    executor = sqlite_executor
    table_name = "seed_users"

    executor.execute_raw(
        f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
        trusted=True,
    )

    steps = [
        SeedStep(
            name="insert-one",
            action=InsertStatementNode(
                table=TableNode(name=table_name),
                columns=[ColumnNode(name="id"), ColumnNode(name="email")],
                values=[LiteralNode(1), LiteralNode("alice@example.com")],
            ),
        ),
        SeedStep(
            name="insert-two",
            action=CompiledQuery(
                sql=f"INSERT INTO {table_name} (id, email) VALUES (?, ?)",
                params=[2, "bob@example.com"],
            ),
        ),
    ]

    summary = SeedRunner(transactional=True).run(executor, steps)
    rows = executor.fetch_all(CompiledQuery(f"SELECT id, email FROM {table_name} ORDER BY id"))

    assert summary.completed_steps == 2
    assert rows == [(1, "alice@example.com"), (2, "bob@example.com")]


def test_sqlite_seed_runner_rolls_back_on_failure(sqlite_executor) -> None:
    executor = sqlite_executor
    table_name = "seed_users_rollback"

    executor.execute_raw(
        f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
        trusted=True,
    )

    steps = [
        SeedStep(
            name="insert-one",
            action=CompiledQuery(
                sql=f"INSERT INTO {table_name} (id, email) VALUES (?, ?)",
                params=[1, "dup@example.com"],
            ),
        ),
        SeedStep(
            name="insert-duplicate",
            action=CompiledQuery(
                sql=f"INSERT INTO {table_name} (id, email) VALUES (?, ?)",
                params=[2, "dup@example.com"],
            ),
        ),
    ]

    try:
        SeedRunner(transactional=True).run(executor, steps)
    except SeedRunError as error:
        assert error.step_name == "insert-duplicate"
    else:
        raise AssertionError("Expected seed run to fail.")

    rows = executor.fetch_all(CompiledQuery(f"SELECT id, email FROM {table_name}"))
    assert rows == []
