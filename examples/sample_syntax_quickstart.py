from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnNode,
    LiteralNode,
    SelectStatementNode,
    TableNode,
    WhereClauseNode,
)
from buildaquery.compiler import PostgresCompiler, SqliteCompiler


def main() -> None:
    # Syntax-only example: build AST and compile to SQL for multiple dialects.
    query = SelectStatementNode(
        select_list=[ColumnNode(name="id"), ColumnNode(name="email")],
        from_table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="active"),
                operator="=",
                right=LiteralNode(value=True),
            )
        ),
    )

    sqlite_compiled = SqliteCompiler().compile(query)
    postgres_compiled = PostgresCompiler().compile(query)

    print("SQLite SQL:", sqlite_compiled.sql)
    print("SQLite params:", sqlite_compiled.params)
    print("PostgreSQL SQL:", postgres_compiled.sql)
    print("PostgreSQL params:", postgres_compiled.params)


if __name__ == "__main__":
    main()
