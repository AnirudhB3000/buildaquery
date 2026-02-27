from buildaquery.abstract_syntax_tree.models import (
    ColumnNode,
    InsertStatementNode,
    LiteralNode,
    TableNode,
)
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.postgres import PostgresExecutor


def main() -> None:
    executor = PostgresExecutor(
        connection_info="postgresql://postgres:password@127.0.0.1:5433/buildaquery_test"
    )

    table = TableNode(name="batch_demo")
    executor.execute_raw("DROP TABLE IF EXISTS batch_demo")
    executor.execute_raw("CREATE TABLE batch_demo (id INTEGER PRIMARY KEY, value TEXT)")

    # AST-driven multi-row insert.
    executor.execute(
        InsertStatementNode(
            table=table,
            columns=[ColumnNode(name="id"), ColumnNode(name="value")],
            rows=[
                [LiteralNode(value=1), LiteralNode(value="alpha")],
                [LiteralNode(value=2), LiteralNode(value="beta")],
            ],
        )
    )

    # Driver-level executemany API.
    executor.execute_many(
        "INSERT INTO batch_demo (id, value) VALUES (%s, %s)",
        [[3, "gamma"], [4, "delta"]],
    )

    rows = executor.execute(CompiledQuery(sql="SELECT id, value FROM batch_demo ORDER BY id", params=[]))
    print(rows)


if __name__ == "__main__":
    main()
