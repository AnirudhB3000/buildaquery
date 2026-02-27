from buildaquery.abstract_syntax_tree.models import (
    ColumnNode,
    ConflictTargetNode,
    InsertStatementNode,
    LiteralNode,
    SelectStatementNode,
    StarNode,
    TableNode,
    UpsertClauseNode,
)
from buildaquery.execution.postgres import PostgresExecutor


def main() -> None:
    executor = PostgresExecutor(connection_info="postgresql://postgres:password@127.0.0.1:5432/buildaquery")
    users = TableNode(name="users")

    insert_first = InsertStatementNode(
        table=users,
        columns=[ColumnNode(name="id"), ColumnNode(name="email")],
        values=[LiteralNode(value=1), LiteralNode(value="alice@example.com")],
    )
    executor.execute(insert_first)

    upsert_update = InsertStatementNode(
        table=users,
        columns=[ColumnNode(name="id"), ColumnNode(name="email")],
        values=[LiteralNode(value=1), LiteralNode(value="alice@new.example")],
        upsert_clause=UpsertClauseNode(
            conflict_target=ConflictTargetNode(columns=[ColumnNode(name="id")]),
            update_columns=["email"],
        ),
    )
    executor.execute(upsert_update)

    rows = executor.execute(
        SelectStatementNode(
            select_list=[StarNode()],
            from_table=users,
        )
    )
    print(rows)


if __name__ == "__main__":
    main()
