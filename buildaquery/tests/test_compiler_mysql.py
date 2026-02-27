import pytest
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
from buildaquery.abstract_syntax_tree.models import (
    SelectStatementNode, ColumnNode, TableNode, LiteralNode,
    BinaryOperationNode, WhereClauseNode, StarNode, TopClauseNode,
    OrderByClauseNode, GroupByClauseNode, HavingClauseNode, CastNode,
    DeleteStatementNode, UnionNode, IntersectNode, ExceptNode,
    InNode, BetweenNode, InsertStatementNode, UpdateStatementNode,
    CaseExpressionNode, WhenThenNode, SubqueryNode, CTENode,
    OverClauseNode, FunctionCallNode, ColumnDefinitionNode,
    CreateStatementNode, DropStatementNode, LockClauseNode
)

@pytest.fixture
def compiler():
    return MySqlCompiler()

def test_compile_simple_select(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users")
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM users"
    assert compiled.params == []

def test_compile_select_distinct(compiler):
    query = SelectStatementNode(
        select_list=[ColumnNode(name="city")],
        from_table=TableNode(name="users"),
        distinct=True
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT DISTINCT city FROM users"
    assert compiled.params == []

def test_compile_qualified_names(compiler):
    query = SelectStatementNode(
        select_list=[ColumnNode(name="id", table="u"), ColumnNode(name="name", table="u")],
        from_table=TableNode(name="users", schema="app")
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT u.id, u.name FROM app.users"

def test_compile_cast(compiler):
    query = SelectStatementNode(
        select_list=[CastNode(expression=ColumnNode(name="age"), data_type="CHAR")],
        from_table=TableNode(name="users")
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT CAST(age AS CHAR) FROM users"

def test_compile_delete(compiler):
    query = DeleteStatementNode(
        table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=1)
            )
        )
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "DELETE FROM users WHERE (id = %s)"
    assert compiled.params == [1]

def test_compile_union(compiler):
    select1 = SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=TableNode(name="t1"))
    select2 = SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=TableNode(name="t2"))

    union_query = UnionNode(left=select1, right=select2)
    compiled = compiler.compile(union_query)
    assert compiled.sql == "SELECT id FROM t1 UNION SELECT id FROM t2"

    union_all_query = UnionNode(left=select1, right=select2, all=True)
    compiled = compiler.compile(union_all_query)
    assert compiled.sql == "SELECT id FROM t1 UNION ALL SELECT id FROM t2"

def test_compile_intersect_except_errors(compiler):
    select1 = SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=TableNode(name="t1"))
    select2 = SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=TableNode(name="t2"))

    with pytest.raises(ValueError, match="MySQL does not support INTERSECT"):
        compiler.compile(IntersectNode(left=select1, right=select2))

    with pytest.raises(ValueError, match="MySQL does not support EXCEPT"):
        compiler.compile(ExceptNode(left=select1, right=select2))

def test_compile_in_between(compiler):
    in_query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=InNode(
                expression=ColumnNode(name="id"),
                values=[LiteralNode(value=1), LiteralNode(value=2), LiteralNode(value=3)]
            )
        )
    )
    compiled = compiler.compile(in_query)
    assert compiled.sql == "SELECT * FROM users WHERE (id IN (%s, %s, %s))"
    assert compiled.params == [1, 2, 3]

    between_query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="products"),
        where_clause=WhereClauseNode(
            condition=BetweenNode(
                expression=ColumnNode(name="price"),
                low=LiteralNode(value=10),
                high=LiteralNode(value=50)
            )
        )
    )
    compiled = compiler.compile(between_query)
    assert compiled.sql == "SELECT * FROM products WHERE (price BETWEEN %s AND %s)"
    assert compiled.params == [10, 50]

def test_compile_insert(compiler):
    query = InsertStatementNode(
        table=TableNode(name="users"),
        columns=[ColumnNode(name="name"), ColumnNode(name="age")],
        values=[LiteralNode(value="Alice"), LiteralNode(value=30)]
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "INSERT INTO users (name, age) VALUES (%s, %s)"
    assert compiled.params == ["Alice", 30]

def test_compile_update(compiler):
    query = UpdateStatementNode(
        table=TableNode(name="users"),
        set_clauses={"age": LiteralNode(value=31), "status": LiteralNode(value="active")},
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="name"),
                operator="=",
                right=LiteralNode(value="Alice")
            )
        )
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "UPDATE users SET age = %s, status = %s WHERE (name = %s)"
    assert compiled.params == [31, "active", "Alice"]

def test_compile_case_expression(compiler):
    query = SelectStatementNode(
        select_list=[
            CaseExpressionNode(
                cases=[
                    WhenThenNode(
                        condition=BinaryOperationNode(left=ColumnNode(name="score"), operator=">", right=LiteralNode(value=90)),
                        result=LiteralNode(value="A")
                    ),
                    WhenThenNode(
                        condition=BinaryOperationNode(left=ColumnNode(name="score"), operator=">", right=LiteralNode(value=80)),
                        result=LiteralNode(value="B")
                    )
                ],
                else_result=LiteralNode(value="C")
            )
        ],
        from_table=TableNode(name="students")
    )
    compiled = compiler.compile(query)
    expected_sql = "SELECT CASE WHEN (score > %s) THEN %s WHEN (score > %s) THEN %s ELSE %s END FROM students"
    assert compiled.sql == expected_sql
    assert compiled.params == [90, "A", 80, "B", "C"]

def test_compile_subquery(compiler):
    inner_select = SelectStatementNode(select_list=[ColumnNode(name="id")], from_table=TableNode(name="users"))
    subquery = SubqueryNode(statement=inner_select, alias="u")

    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=subquery
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM (SELECT id FROM users) AS u"

    query_in = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="orders"),
        where_clause=WhereClauseNode(
            condition=InNode(
                expression=ColumnNode(name="user_id"),
                values=[SubqueryNode(statement=inner_select)]
            )
        )
    )
    compiled_in = compiler.compile(query_in)
    assert compiled_in.sql == "SELECT * FROM orders WHERE (user_id IN ((SELECT id FROM users)))"

def test_compile_cte(compiler):
    inner_select = SelectStatementNode(select_list=[StarNode()], from_table=TableNode(name="users"))
    cte = CTENode(name="user_subset", subquery=inner_select)

    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="user_subset"),
        ctes=[cte]
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "WITH user_subset AS (SELECT * FROM users) SELECT * FROM user_subset"

def test_compile_window_function(compiler):
    query = SelectStatementNode(
        select_list=[
            ColumnNode(name="name"),
            FunctionCallNode(
                name="SUM",
                args=[ColumnNode(name="salary")],
                over=OverClauseNode(
                    partition_by=[ColumnNode(name="dept")],
                    order_by=[OrderByClauseNode(expression=ColumnNode(name="id"))]
                )
            )
        ],
        from_table=TableNode(name="employees")
    )
    compiled = compiler.compile(query)
    expected_sql = "SELECT name, SUM(salary) OVER (PARTITION BY dept ORDER BY id ASC) FROM employees"
    assert compiled.sql == expected_sql

def test_compile_ddl(compiler):
    create_query = CreateStatementNode(
        table=TableNode(name="users"),
        columns=[
            ColumnDefinitionNode(name="id", data_type="INTEGER", primary_key=True),
            ColumnDefinitionNode(name="name", data_type="VARCHAR(255)", not_null=True),
            ColumnDefinitionNode(name="age", data_type="INTEGER", default=LiteralNode(value=18))
        ],
        if_not_exists=True
    )
    compiled_create = compiler.compile(create_query)
    expected_create = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, age INTEGER DEFAULT %s)"
    assert compiled_create.sql == expected_create
    assert compiled_create.params == [18]

    drop_query = DropStatementNode(
        table=TableNode(name="users"),
        if_exists=True,
        cascade=False
    )
    compiled_drop = compiler.compile(drop_query)
    assert compiled_drop.sql == "DROP TABLE IF EXISTS users"

def test_compile_drop_cascade_error(compiler):
    drop_query = DropStatementNode(
        table=TableNode(name="users"),
        if_exists=True,
        cascade=True
    )
    with pytest.raises(ValueError, match="MySQL does not support CASCADE in DROP TABLE"):
        compiler.compile(drop_query)

def test_compile_where_with_params(compiler):
    query = SelectStatementNode(
        select_list=[ColumnNode(name="name")],
        from_table=TableNode(name="users"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="age"),
                operator=">",
                right=LiteralNode(value=25)
            )
        )
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT name FROM users WHERE (age > %s)"
    assert compiled.params == [25]

def test_compile_multiple_params(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="products"),
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=BinaryOperationNode(
                    left=ColumnNode(name="price"),
                    operator=">",
                    right=LiteralNode(value=100)
                ),
                operator="AND",
                right=BinaryOperationNode(
                    left=ColumnNode(name="category"),
                    operator="=",
                    right=LiteralNode(value="electronics")
                )
            )
        )
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM products WHERE ((price > %s) AND (category = %s))"
    assert compiled.params == [100, "electronics"]

def test_compile_order_by(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        order_by_clause=[OrderByClauseNode(expression=ColumnNode(name="id"), direction="DESC")]
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM users ORDER BY id DESC"

def test_compile_top_translation(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        top_clause=TopClauseNode(count=10, on_expression=ColumnNode(name="score"), direction="DESC")
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM users ORDER BY score DESC LIMIT 10"

def test_compile_top_vs_limit_error(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        top_clause=TopClauseNode(count=10),
        limit=5
    )
    with pytest.raises(ValueError, match="TOP clause is mutually exclusive with LIMIT and OFFSET"):
        compiler.compile(query)

def test_compile_group_by_having(compiler):
    query = SelectStatementNode(
        select_list=[ColumnNode(name="dept"), ColumnNode(name="COUNT(*)")],
        from_table=TableNode(name="employees"),
        group_by=GroupByClauseNode(expressions=[ColumnNode(name="dept")]),
        having_clause=HavingClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="COUNT(*)"),
                operator=">",
                right=LiteralNode(value=5)
            )
        )
    )
    compiled = compiler.compile(query)
    assert "GROUP BY dept" in compiled.sql
    assert "HAVING (COUNT(*) > %s)" in compiled.sql
    assert compiled.params == [5]

def test_compile_select_with_lock_clause(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="jobs"),
        lock_clause=LockClauseNode(mode="SHARE", skip_locked=True),
    )
    compiled = compiler.compile(query)
    assert compiled.sql == "SELECT * FROM jobs FOR SHARE SKIP LOCKED"

def test_compile_lock_clause_invalid_mode_error(compiler):
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="jobs"),
        lock_clause=LockClauseNode(mode="INVALID"),
    )
    with pytest.raises(ValueError, match="MySQL lock mode must be 'UPDATE' or 'SHARE'"):
        compiler.compile(query)
