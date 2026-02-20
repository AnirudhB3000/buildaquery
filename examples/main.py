from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode,
    ColumnNode,
    TableNode,
    TopClauseNode,
)

def main():
    """
    Example usage of the query builder.
    """
    query = SelectStatementNode(
        select_list=[ColumnNode(name="*")],
        from_table=TableNode(name="users"),
        top_clause=TopClauseNode(count=10),
    )
    print(query)

if __name__ == "__main__":
    main()
