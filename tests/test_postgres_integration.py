import pytest
from datetime import date
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, LiteralNode,
    BinaryOperationNode, WhereClauseNode, StarNode, TopClauseNode,
    OrderByClauseNode, GroupByClauseNode, HavingClauseNode, CastNode,
    DeleteStatementNode, InsertStatementNode, UpdateStatementNode,
    ColumnDefinitionNode, CreateStatementNode, DropStatementNode,
    JoinClauseNode, UnionNode, IntersectNode, ExceptNode,
    InNode, BetweenNode, CaseExpressionNode, WhenThenNode,
    SubqueryNode, CTENode, OverClauseNode, FunctionCallNode, AliasNode
)

# ==============================================================================
# 1. Basic Lifecycle (CRUD)
# ==============================================================================

def test_basic_crud_lifecycle(executor, create_table):
    # 1. Define & Create Table
    users_table = TableNode(name="integration_users")
    create_stmt = CreateStatementNode(
        table=users_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="SERIAL", primary_key=True),
            ColumnDefinitionNode(name="name", data_type="TEXT", not_null=True),
            ColumnDefinitionNode(name="age", data_type="INTEGER")
        ]
    )
    create_table(create_stmt)

    # 2. Insert Data
    insert_stmt = InsertStatementNode(
        table=users_table,
        columns=[ColumnNode(name="name"), ColumnNode(name="age")],
        values=[LiteralNode(value="Alice"), LiteralNode(value=30)]
    )
    executor.execute(insert_stmt)

    insert_stmt_2 = InsertStatementNode(
        table=users_table,
        columns=[ColumnNode(name="name"), ColumnNode(name="age")],
        values=[LiteralNode(value="Bob"), LiteralNode(value=25)]
    )
    executor.execute(insert_stmt_2)

    # 3. Select Data
    select_stmt = SelectStatementNode(
        select_list=[ColumnNode(name="name"), ColumnNode(name="age")],
        from_table=users_table,
        order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="age"))] # Ascending by default
    )
    results = executor.execute(select_stmt)
    assert len(results) == 2
    assert results[0] == ("Bob", 25)
    assert results[1] == ("Alice", 30)

    # 4. Update Data
    update_stmt = UpdateStatementNode(
        table=users_table,
        set_clauses={"age": LiteralNode(value=31)},
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="name"),
                operator="=",
                right=LiteralNode(value="Alice")
            )
        )
    )
    executor.execute(update_stmt)

    # Verify Update
    verify_stmt = SelectStatementNode(
        select_list=[ColumnNode(name="age")],
        from_table=users_table,
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="name"),
                operator="=",
                right=LiteralNode(value="Alice")
            )
        )
    )
    updated_result = executor.execute(verify_stmt)
    assert updated_result[0][0] == 31

    # 5. Delete Data
    delete_stmt = DeleteStatementNode(
        table=users_table,
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="name"),
                operator="=",
                right=LiteralNode(value="Bob")
            )
        )
    )
    executor.execute(delete_stmt)

    # Verify Delete
    count_stmt = SelectStatementNode(
        select_list=[FunctionCallNode(name="COUNT", args=[StarNode()])],
        from_table=users_table
    )
    count_result = executor.execute(count_stmt)
    assert count_result[0][0] == 1


# ==============================================================================
# 2. Advanced Filtering (IN, BETWEEN, CASE)
# ==============================================================================

def test_advanced_filtering(executor, create_table):
    products_table = TableNode(name="integration_products")
    create_stmt = CreateStatementNode(
        table=products_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="SERIAL", primary_key=True),
            ColumnDefinitionNode(name="name", data_type="TEXT"),
            ColumnDefinitionNode(name="price", data_type="INTEGER"),
            ColumnDefinitionNode(name="category", data_type="TEXT")
        ]
    )
    create_table(create_stmt)

    # Insert sample data
    data = [
        ("Laptop", 1000, "Electronics"),
        ("Mouse", 20, "Electronics"),
        ("Chair", 150, "Furniture"),
        ("Desk", 300, "Furniture"),
        ("Apple", 1, "Food")
    ]
    for name, price, category in data:
        executor.execute(InsertStatementNode(
            table=products_table,
            columns=[ColumnNode(name="name"), ColumnNode(name="price"), ColumnNode(name="category")],
            values=[LiteralNode(value=name), LiteralNode(value=price), LiteralNode(value=category)]
        ))

    # Test IN
    in_stmt = SelectStatementNode(
        select_list=[ColumnNode(name="name")],
        from_table=products_table,
        where_clause=WhereClauseNode(
            condition=InNode(
                expression=ColumnNode(name="category"),
                values=[LiteralNode(value="Electronics"), LiteralNode(value="Food")]
            )
        ),
        order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="name"))]
    )
    in_results = executor.execute(in_stmt)
    assert len(in_results) == 3
    assert in_results[0][0] == "Apple"

    # Test BETWEEN
    between_stmt = SelectStatementNode(
        select_list=[ColumnNode(name="name")],
        from_table=products_table,
        where_clause=WhereClauseNode(
            condition=BetweenNode(
                expression=ColumnNode(name="price"),
                low=LiteralNode(value=100),
                high=LiteralNode(value=500)
            )
        )
    )
    between_results = executor.execute(between_stmt)
    assert len(between_results) == 2 # Chair, Desk (Laptop is 1000, Mouse is 20, Apple is 1)
    names = sorted([r[0] for r in between_results])
    assert names == ["Chair", "Desk"]

    # Test CASE
    case_stmt = SelectStatementNode(
        select_list=[
            ColumnNode(name="name"),
            CaseExpressionNode(
                cases=[
                    WhenThenNode(
                        condition=BinaryOperationNode(left=ColumnNode(name="price"), operator=">", right=LiteralNode(value=500)),
                        result=LiteralNode(value="Expensive")
                    )
                ],
                else_result=LiteralNode(value="Affordable")
            )
        ],
        from_table=products_table,
        order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="price"), direction="DESC")]
    )
    case_results = executor.execute(case_stmt)
    assert case_results[0] == ("Laptop", "Expensive")
    assert case_results[1] == ("Desk", "Affordable")


# ==============================================================================
# 3. Joins & Aggregations
# ==============================================================================

def test_joins_and_aggregations(executor, create_table):
    # Setup Departments
    depts_table = TableNode(name="integration_depts")
    create_table(CreateStatementNode(
        table=depts_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="SERIAL", primary_key=True),
            ColumnDefinitionNode(name="name", data_type="TEXT")
        ]
    ))
    executor.execute(InsertStatementNode(table=depts_table, columns=[ColumnNode(name="name")], values=[LiteralNode(value="HR")])) # ID 1
    executor.execute(InsertStatementNode(table=depts_table, columns=[ColumnNode(name="name")], values=[LiteralNode(value="Engineering")])) # ID 2

    # Setup Employees
    emps_table = TableNode(name="integration_emps")
    create_table(CreateStatementNode(
        table=emps_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="SERIAL", primary_key=True),
            ColumnDefinitionNode(name="name", data_type="TEXT"),
            ColumnDefinitionNode(name="dept_id", data_type="INTEGER"),
            ColumnDefinitionNode(name="salary", data_type="INTEGER")
        ]
    ))
    
    # Insert Employees
    emp_data = [
        ("Alice", 1, 50000), # HR
        ("Bob", 2, 80000),   # Eng
        ("Charlie", 2, 90000), # Eng
        ("Dave", None, 40000) # No Dept
    ]
    for name, dept_id, salary in emp_data:
        executor.execute(InsertStatementNode(
            table=emps_table,
            columns=[ColumnNode(name="name"), ColumnNode(name="dept_id"), ColumnNode(name="salary")],
            values=[LiteralNode(value=name), LiteralNode(value=dept_id), LiteralNode(value=salary)]
        ))

    # Test INNER JOIN
    join_stmt = SelectStatementNode(
        select_list=[ColumnNode(name="name", table="e"), ColumnNode(name="name", table="d")],
        from_table=JoinClauseNode(
            left=TableNode(name="integration_emps", alias="e"), # Assuming alias support in TableNode/execution or raw name
            right=TableNode(name="integration_depts", alias="d"), 
            join_type="INNER",
            on_condition=BinaryOperationNode(
                left=ColumnNode(name="dept_id", table="e"),
                operator="=",
                right=ColumnNode(name="id", table="d")
            )
        ),
        order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="name", table="e"))]
    )
    # Note: TableNode aliases are not strictly in models.py yet based on previous read, 
    # but the compiler might handle it if I add alias to TableNode or use AliasNode.
    # Looking at models.py, TableNode doesn't have alias. 
    # Workaround: Use names directly or implement AliasNode wrapper if supported for tables.
    # For now, let's use full table names to be safe.
    
    join_stmt_safe = SelectStatementNode(
        select_list=[ColumnNode(name="name", table="integration_emps"), ColumnNode(name="name", table="integration_depts")],
        from_table=JoinClauseNode(
            left=TableNode(name="integration_emps"),
            right=TableNode(name="integration_depts"),
            join_type="INNER",
            on_condition=BinaryOperationNode(
                left=ColumnNode(name="dept_id", table="integration_emps"),
                operator="=",
                right=ColumnNode(name="id", table="integration_depts")
            )
        ),
        order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="name", table="integration_emps"))]
    )
    
    results = executor.execute(join_stmt_safe)
    assert len(results) == 3 # Alice, Bob, Charlie (Dave has no dept)
    assert results[0] == ("Alice", "HR")

    # Test GROUP BY & HAVING
    agg_stmt = SelectStatementNode(
        select_list=[
            ColumnNode(name="name", table="integration_depts"),
            FunctionCallNode(name="AVG", args=[ColumnNode(name="salary", table="integration_emps")])
        ],
        from_table=JoinClauseNode(
            left=TableNode(name="integration_emps"),
            right=TableNode(name="integration_depts"),
            join_type="INNER",
            on_condition=BinaryOperationNode(
                left=ColumnNode(name="dept_id", table="integration_emps"),
                operator="=",
                right=ColumnNode(name="id", table="integration_depts")
            )
        ),
        group_by=GroupByClauseNode(expressions=[ColumnNode(name="name", table="integration_depts")]),
        having_clause=HavingClauseNode(
            condition=BinaryOperationNode(
                left=FunctionCallNode(name="AVG", args=[ColumnNode(name="salary", table="integration_emps")]),
                operator=">",
                right=LiteralNode(value=60000)
            )
        )
    )
    agg_results = executor.execute(agg_stmt)
    assert len(agg_results) == 1
    assert agg_results[0][0] == "Engineering" # Avg is 85000


# ==============================================================================
# 4. CTEs & Window Functions
# ==============================================================================

def test_cte_and_window_functions(executor, create_table):
    sales_table = TableNode(name="integration_sales")
    create_table(CreateStatementNode(
        table=sales_table,
        columns=[
            ColumnDefinitionNode(name="id", data_type="SERIAL", primary_key=True),
            ColumnDefinitionNode(name="agent", data_type="TEXT"),
            ColumnDefinitionNode(name="amount", data_type="INTEGER")
        ]
    ))

    data = [("A", 100), ("A", 200), ("B", 300), ("B", 100), ("C", 500)]
    for agent, amount in data:
        executor.execute(InsertStatementNode(
            table=sales_table,
            columns=[ColumnNode(name="agent"), ColumnNode(name="amount")],
            values=[LiteralNode(value=agent), LiteralNode(value=amount)]
        ))

    # Test CTE
    # WITH agent_totals AS (SELECT agent, SUM(amount) as total FROM sales GROUP BY agent)
    # SELECT * FROM agent_totals WHERE total > 300
    
    cte_inner = SelectStatementNode(
        select_list=[
            ColumnNode(name="agent"),
            AliasNode(
                expression=FunctionCallNode(name="SUM", args=[ColumnNode(name="amount")]),
                name="total_amount"
            )
        ],
        from_table=sales_table,
        group_by=GroupByClauseNode(expressions=[ColumnNode(name="agent")])
    )
    
    cte_node = CTENode(name="agent_totals", subquery=cte_inner)
    
    cte_query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="agent_totals"),
        ctes=[cte_node],
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="total_amount"), 
                operator=">",
                right=LiteralNode(value=350)
            )
        )
    )
    
    # We need to be careful with column names in CTEs without explicit aliases.
    # Postgres names aggregate columns like 'sum', 'count', etc.
    cte_results = executor.execute(cte_query)
    assert len(cte_results) == 2 # B (400), C (500) -> Wait, A(300), B(400), C(500). > 350 means B and C.
    
    # Test Window Function
    # SELECT agent, amount, RANK() OVER (PARTITION BY agent ORDER BY amount DESC)
    window_stmt = SelectStatementNode(
        select_list=[
            ColumnNode(name="agent"),
            ColumnNode(name="amount"),
            FunctionCallNode(
                name="RANK",
                args=[],
                over=OverClauseNode(
                    partition_by=[ColumnNode(name="agent")],
                    order_by=[OrderByClauseNode(expression=ColumnNode(name="amount"), direction="DESC")]
                )
            )
        ],
        from_table=sales_table,
        order_by_clause=[ColumnNode(name="agent"), ColumnNode(name="amount")]
    )
    
    window_results = executor.execute(window_stmt)
    # Agent A: 200 (rank 1), 100 (rank 2)
    assert window_results[0] == ("A", 100, 2)
    assert window_results[1] == ("A", 200, 1)


# ==============================================================================
# 5. Set Operations & Subqueries
# ==============================================================================

def test_set_operations_and_subqueries(executor, create_table):
    t1 = TableNode(name="set_t1")
    t2 = TableNode(name="set_t2")
    
    create_table(CreateStatementNode(table=t1, columns=[ColumnDefinitionNode(name="id", data_type="INTEGER")]))
    create_table(CreateStatementNode(table=t2, columns=[ColumnDefinitionNode(name="id", data_type="INTEGER")]))
    
    for i in [1, 2, 3]:
        executor.execute(InsertStatementNode(table=t1, columns=[ColumnNode(name="id")], values=[LiteralNode(value=i)]))
    for i in [3, 4, 5]:
        executor.execute(InsertStatementNode(table=t2, columns=[ColumnNode(name="id")], values=[LiteralNode(value=i)]))

    # UNION
    union_stmt = UnionNode(
        left=SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=t1),
        right=SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=t2)
    )
    # Wrap in SELECT * FROM (...) because execution usually expects a SelectStatementNode, 
    # but compiler handles UnionNode directly as a statement.
    # If the executor expects a query that returns rows, UnionNode should work.
    
    union_results = executor.execute(union_stmt)
    assert len(union_results) == 5 # 1, 2, 3, 4, 5 (3 is distinct)
    
    # INTERSECT
    intersect_stmt = IntersectNode(
        left=SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=t1),
        right=SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=t2)
    )
    intersect_results = executor.execute(intersect_stmt)
    assert len(intersect_results) == 1
    assert intersect_results[0][0] == 3

    # EXCEPT
    except_stmt = ExceptNode(
        left=SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=t1),
        right=SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=t2)
    )
    except_results = executor.execute(except_stmt)
    assert len(except_results) == 2 # 1, 2 (3 is in t2)

    # Subquery in WHERE
    # SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)
    sub_stmt = SelectStatementNode(
        select_list=[ColumnNode(name="id")],
        from_table=t1,
        where_clause=WhereClauseNode(
            condition=InNode(
                expression=ColumnNode(name="id"),
                values=[SubqueryNode(statement=SelectStatementNode(
                    select_list=[ColumnNode(name="id")],
                    from_table=t2
                ))]
            )
        )
    )
    sub_results = executor.execute(sub_stmt)
    assert len(sub_results) == 1
    assert sub_results[0][0] == 3

    # Subquery in FROM
    sub_from_stmt = SelectStatementNode(
        select_list=[ColumnNode(name="id")],
        from_table=SubqueryNode(
            statement=SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=t1),
            alias="sub"
        )
    )
    sub_from_results = executor.execute(sub_from_stmt)
    assert len(sub_from_results) == 3

