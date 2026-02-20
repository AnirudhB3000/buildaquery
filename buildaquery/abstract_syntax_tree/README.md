# Abstract Syntax Tree (AST)

The `abstract_syntax_tree` module defines the data structures used to represent SQL queries as an Abstract Syntax Tree. This representation is agnostic of any specific SQL dialect and serves as the intermediate format that the query builder constructs and the compiler translates into SQL.

## Core Concepts

The AST is built using a hierarchy of nodes, all inheriting from the base `ASTNode` class.

### Node Categories

-   **`ExpressionNode`**: Represents values, column references, and operations (e.g., `LiteralNode`, `ColumnNode`, `BinaryOperationNode`, `FunctionCallNode`).
-   **`StatementNode`**: Represents complete SQL statements. Currently, `SelectStatementNode` is the primary implementation.
-   **`FromClauseNode`**: Represents entities that can appear in a `FROM` clause, such as `TableNode` or `JoinClauseNode`.

## Key Components

-   **`SelectStatementNode`**: The central node for `SELECT` queries. It encapsulates the select list, `FROM` table/joins, `WHERE` conditions, `GROUP BY`, `HAVING`, `ORDER BY`, and pagination (`LIMIT`, `OFFSET`, `TOP`).
-   **`TopClauseNode`**: A specialized node for limiting results, particularly for dialects that support the `TOP` syntax. It is mutually exclusive with standard `LIMIT`/`OFFSET`.
-   **`JoinClauseNode`**: Represents various types of table joins (INNER, LEFT, etc.) with associated join conditions.
-   **Operations**: Support for both binary (e.g., `+`, `-`, `AND`, `OR`) and unary (e.g., `NOT`, `-`) operations.

## Usage

Nodes are typically instantiated and composed to build a tree representing a query. For example:

```python
from buildaquery.abstract_syntax_tree.models import SelectStatementNode, ColumnNode, TableNode

# SELECT * FROM users
query = SelectStatementNode(
    select_list=[ColumnNode(name="*")],
    from_table=TableNode(name="users")
)
```

This tree can then be processed by visitors (see the `traversal` module) for compilation or analysis.
