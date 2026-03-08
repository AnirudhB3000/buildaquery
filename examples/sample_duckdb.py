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
from buildaquery.execution.duckdb import DuckDbExecutor


def main() -> None:
    executor = DuckDbExecutor(connection_info="static/test-duckdb/sample.duckdb")

    users = TableNode(name="users")
    executor.execute(
        CreateStatementNode(
            table=users,
            columns=[
                ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
                ColumnDefinitionNode(name="name", data_type="VARCHAR"),
                ColumnDefinitionNode(name="age", data_type="INTEGER"),
            ],
        )
    )

    executor.execute(
        InsertStatementNode(
            table=users,
            columns=[ColumnNode(name="id"), ColumnNode(name="name"), ColumnNode(name="age")],
            values=[LiteralNode(value=1), LiteralNode(value="Alice"), LiteralNode(value=30)],
        )
    )

    rows = executor.execute(
        SelectStatementNode(
            select_list=[StarNode()],
            from_table=users,
        )
    )
    print(rows)

    executor.execute(DropStatementNode(table=users, if_exists=True))
    executor.close()


if __name__ == "__main__":
    main()
