from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnDefinitionNode,
    ColumnNode,
    CreateStatementNode,
    DeleteStatementNode,
    InsertStatementNode,
    LiteralNode,
    ReturningClauseNode,
    TableNode,
    UpdateStatementNode,
    WhereClauseNode,
)
from buildaquery.execution.postgres import PostgresExecutor


# ==================================================
# Connection Setup
# ==================================================

executor = PostgresExecutor(connection_info="postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")
users = TableNode(name="users_returning_demo")


# ==================================================
# Setup
# ==================================================

executor.execute(
    CreateStatementNode(
        table=users,
        columns=[
            ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
            ColumnDefinitionNode(name="email", data_type="TEXT", not_null=True),
        ],
    )
)


# ==================================================
# INSERT ... RETURNING
# ==================================================

insert_rows = executor.execute(
    InsertStatementNode(
        table=users,
        columns=[ColumnNode(name="id"), ColumnNode(name="email")],
        values=[LiteralNode(value=1), LiteralNode(value="first@example.com")],
        returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id"), ColumnNode(name="email")]),
    )
)
print("insert returning:", insert_rows)


# ==================================================
# UPDATE ... RETURNING
# ==================================================

update_rows = executor.execute(
    UpdateStatementNode(
        table=users,
        set_clauses={"email": LiteralNode(value="updated@example.com")},
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=1),
            )
        ),
        returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id"), ColumnNode(name="email")]),
    )
)
print("update returning:", update_rows)


# ==================================================
# DELETE ... RETURNING
# ==================================================

delete_rows = executor.execute(
    DeleteStatementNode(
        table=users,
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=1),
            )
        ),
        returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id"), ColumnNode(name="email")]),
    )
)
print("delete returning:", delete_rows)


# ==================================================
# Cleanup
# ==================================================

executor.execute_raw("DROP TABLE IF EXISTS users_returning_demo")
