from buildaquery.execution.sqlite import SqliteExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

def main():
    # Define database path
    db_path = "static/test-sqlite/db.sqlite"

    # Create executor
    executor = SqliteExecutor(connection_info=db_path)

    # Define table
    users_table = TableNode(name="sample_users")

    print("Creating table 'sample_users'...")

    # Create table statement
    create_stmt = CreateStatementNode(
        table=users_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
            ColumnDefinitionNode(name="name", data_type="TEXT", not_null=True),
            ColumnDefinitionNode(name="age", data_type="INTEGER")
        ]
    )

    # Execute create table
    executor.execute(create_stmt)
    print("Table created successfully!")

    print("Inserting sample data...")

    # Insert some users
    users_data = [
        ("Alice", 30),
        ("Bob", 25),
        ("Charlie", 35)
    ]

    for name, age in users_data:
        insert_stmt = InsertStatementNode(
            table=users_table,
            columns=[ColumnNode(name="name"), ColumnNode(name="age")],
            values=[LiteralNode(value=name), LiteralNode(value=age)]
        )
        executor.execute(insert_stmt)

    print(f"Inserted {len(users_data)} users successfully!")

    print("Selecting all users...")

    # Select all users
    select_stmt = SelectStatementNode(
        select_list=[StarNode()],  # SELECT *
        from_table=users_table
    )

    results = executor.execute(select_stmt)

    print("Users in database:")
    for row in results:
        print(f"ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")

    print("Dropping table 'sample_users'...")

    # Drop table statement
    drop_stmt = DropStatementNode(
        table=users_table,
        if_exists=True
    )

    # Execute drop table
    executor.execute(drop_stmt)
    print("Table dropped successfully!")

if __name__ == "__main__":
    main()
