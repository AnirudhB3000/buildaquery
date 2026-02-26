from dotenv import load_dotenv
import os
from buildaquery.execution.cockroachdb import CockroachExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Build connection string from environment variables
    db_host = os.getenv("COCKROACH_HOST", "127.0.0.1")
    db_port = os.getenv("COCKROACH_PORT", "26257")
    db_name = os.getenv("COCKROACH_DB", "buildaquery")
    db_user = os.getenv("COCKROACH_USER", "root")

    connection_string = f"postgresql://{db_user}@{db_host}:{db_port}/{db_name}?sslmode=disable"

    # Create executor
    executor = CockroachExecutor(connection_info=connection_string)

    # Define table
    users_table = TableNode(name="sample_users")

    print("Creating table 'sample_users'...")

    # Create table statement
    create_stmt = CreateStatementNode(
        table=users_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="INT", primary_key=True),
            ColumnDefinitionNode(name="name", data_type="STRING", not_null=True),
            ColumnDefinitionNode(name="age", data_type="INT")
        ]
    )

    # Execute create table
    executor.execute(create_stmt)
    print("Table created successfully!")

    print("Inserting sample data...")

    # Insert some users
    users_data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, "Charlie", 35)
    ]

    for user_id, name, age in users_data:
        insert_stmt = InsertStatementNode(
            table=users_table,
            columns=[ColumnNode(name="id"), ColumnNode(name="name"), ColumnNode(name="age")],
            values=[LiteralNode(value=user_id), LiteralNode(value=name), LiteralNode(value=age)]
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

    drop_stmt = DropStatementNode(
        table=users_table,
        if_exists=True,
        cascade=True
    )

    executor.execute(drop_stmt)
    print("Table dropped successfully!")

if __name__ == "__main__":
    main()
