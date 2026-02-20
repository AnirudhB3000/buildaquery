from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode,
    ColumnNode,
    TableNode,
    TopClauseNode,
    BinaryOperationNode,
    LiteralNode,
    WhereClauseNode,
)
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler

def main():
    """
    Example usage of the query builder and Postgres compiler.
    """
    # 1. Build an AST: SELECT * FROM users WHERE age > 25 (TOP 5 ordered by created_at DESC)
    query = SelectStatementNode(
        select_list=[ColumnNode(name="*")],
        from_table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="age"),
                operator=">",
                right=LiteralNode(value=25)
            )
        ),
        top_clause=TopClauseNode(
            count=5, 
            on_expression=ColumnNode(name="created_at"), 
            direction="DESC"
        ),
    )

    # 2. Compile the AST
    compiler = PostgresCompiler()
    compiled = compiler.compile(query)

    # 3. Display the results
    print("--- AST Representation ---")
    print(query)
    print("\n--- Compiled PostgreSQL ---")
    print(f"SQL: {compiled.sql}")
    print(f"Params: {compiled.params}")

if __name__ == "__main__":
    main()
