from buildaquery.abstract_syntax_tree.models import (
    AddColumnActionNode,
    AddConstraintActionNode,
    AlterTableStatementNode,
    BinaryOperationNode,
    CheckConstraintNode,
    ColumnDefinitionNode,
    ColumnNode,
    CreateIndexStatementNode,
    CreateStatementNode,
    DropColumnActionNode,
    DropIndexStatementNode,
    ForeignKeyConstraintNode,
    PrimaryKeyConstraintNode,
    TableNode,
    UniqueConstraintNode,
)
from buildaquery.execution.postgres import PostgresExecutor


# ==================================================
# Connection setup
# ==================================================

executor = PostgresExecutor(connection_info="postgresql://postgres:password@127.0.0.1:5433/buildaquery_test")


# ==================================================
# Base tables
# ==================================================

users_table = TableNode(name="ddl_users")
accounts_table = TableNode(name="ddl_accounts")

executor.execute(
    CreateStatementNode(
        table=users_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
            ColumnDefinitionNode(name="email", data_type="TEXT", not_null=True),
        ],
    )
)

executor.execute(
    CreateStatementNode(
        table=accounts_table,
        columns=[
            ColumnDefinitionNode(name="account_id", data_type="INTEGER", not_null=True),
            ColumnDefinitionNode(name="user_id", data_type="INTEGER", not_null=True),
            ColumnDefinitionNode(name="balance", data_type="INTEGER", not_null=True),
        ],
        constraints=[
            PrimaryKeyConstraintNode(columns=[ColumnNode(name="account_id"), ColumnNode(name="user_id")]),
            UniqueConstraintNode(name="uq_accounts_user_account", columns=[ColumnNode(name="user_id"), ColumnNode(name="account_id")]),
            ForeignKeyConstraintNode(
                name="fk_accounts_user",
                columns=[ColumnNode(name="user_id")],
                reference_table=users_table,
                reference_columns=[ColumnNode(name="id")],
                on_delete="CASCADE",
            ),
            CheckConstraintNode(
                name="ck_accounts_balance_rule",
                condition=BinaryOperationNode(
                    left=ColumnNode(name="balance"),
                    operator=">",
                    right=ColumnNode(name="user_id"),
                ),
            ),
        ],
    )
)


# ==================================================
# Index lifecycle
# ==================================================

index_name = "idx_accounts_user_id"
executor.execute(
    CreateIndexStatementNode(
        name=index_name,
        table=accounts_table,
        columns=[ColumnNode(name="user_id")],
    )
)

executor.execute(DropIndexStatementNode(name=index_name, if_exists=True))


# ==================================================
# Alter table lifecycle
# ==================================================

executor.execute(
    AlterTableStatementNode(
        table=accounts_table,
        actions=[
            AddColumnActionNode(column=ColumnDefinitionNode(name="status", data_type="TEXT")),
            AddConstraintActionNode(
                constraint=UniqueConstraintNode(
                    name="uq_accounts_status_user",
                    columns=[ColumnNode(name="status"), ColumnNode(name="user_id")],
                )
            ),
        ],
    )
)

executor.execute(
    AlterTableStatementNode(
        table=accounts_table,
        actions=[DropColumnActionNode(column_name="status", if_exists=True)],
    )
)


# ==================================================
# Cleanup
# ==================================================

executor.execute_raw("DROP TABLE IF EXISTS ddl_accounts CASCADE")
executor.execute_raw("DROP TABLE IF EXISTS ddl_users CASCADE")
