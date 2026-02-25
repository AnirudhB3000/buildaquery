from dotenv import load_dotenv
import os
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.abstract_syntax_tree.models import (
    CreateStatementNode, TableNode, ColumnDefinitionNode,
    InsertStatementNode, ColumnNode, LiteralNode,
    SelectStatementNode, StarNode, DropStatementNode
)

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Build connection string from environment variables
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Create executor
    executor = PostgresExecutor(connection_info=connection_string)

    # Define table
    users_table = TableNode(name="sample_users")

    print("Creating table 'sample_users'...")

    # Create table statement
    create_stmt = CreateStatementNode(
        table=users_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="SERIAL", primary_key=True),
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