"""
Transaction example with savepoint control using PostgresExecutor.

Run:
    poetry run python examples/sample_transactions.py
"""

from buildaquery.abstract_syntax_tree.models import (
    ColumnDefinitionNode,
    ColumnNode,
    CreateStatementNode,
    DropStatementNode,
    InsertStatementNode,
    LiteralNode,
    SelectStatementNode,
    StarNode,
    TableNode,
)
from buildaquery.execution.postgres import PostgresExecutor


# ==================================================
# Config
# ==================================================

CONNECTION_URL = "postgresql://postgres:password@127.0.0.1:5432/buildaquery"


def main() -> None:
    executor = PostgresExecutor(connection_info=CONNECTION_URL)
    users_table = TableNode(name="transaction_demo_users")

    # Clean setup
    executor.execute(DropStatementNode(table=users_table, if_exists=True))
    executor.execute(
        CreateStatementNode(
            table=users_table,
            columns=[
                ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                ColumnDefinitionNode(name="name", data_type="TEXT", not_null=True),
            ],
        )
    )

    # Transaction with savepoint
    executor.begin()
    executor.execute(
        InsertStatementNode(
            table=users_table,
            columns=[ColumnNode(name="id"), ColumnNode(name="name")],
            values=[LiteralNode(value=1), LiteralNode(value="Alice")],
        )
    )
    executor.savepoint("after_alice")
    executor.execute(
        InsertStatementNode(
            table=users_table,
            columns=[ColumnNode(name="id"), ColumnNode(name="name")],
            values=[LiteralNode(value=2), LiteralNode(value="Bob")],
        )
    )
    executor.rollback_to_savepoint("after_alice")
    executor.release_savepoint("after_alice")
    executor.commit()

    rows = executor.execute(SelectStatementNode(select_list=[StarNode()], from_table=users_table))
    print(rows)

    # Cleanup
    executor.execute(DropStatementNode(table=users_table, if_exists=True))


if __name__ == "__main__":
    main()
