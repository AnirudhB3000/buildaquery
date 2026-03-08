from buildaquery.abstract_syntax_tree.models import (
    ColumnNode,
    InsertStatementNode,
    LiteralNode,
    SelectStatementNode,
    TableNode,
    WhereClauseNode,
    BinaryOperationNode,
)
from buildaquery.compiler.clickhouse.clickhouse_compiler import ClickHouseCompiler


def main() -> None:
    compiler = ClickHouseCompiler()
    events = TableNode(name="events")

    insert_query = InsertStatementNode(
        table=events,
        columns=[ColumnNode(name="id"), ColumnNode(name="value")],
        values=[LiteralNode(value=1), LiteralNode(value="created")],
    )
    compiled_insert = compiler.compile(insert_query)
    print(compiled_insert.sql)
    print(compiled_insert.params)

    select_query = SelectStatementNode(
        select_list=[ColumnNode(name="id"), ColumnNode(name="value")],
        from_table=events,
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=1),
            )
        ),
    )
    compiled_select = compiler.compile(select_query)
    print(compiled_select.sql)
    print(compiled_select.params)


if __name__ == "__main__":
    main()
