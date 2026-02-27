import pytest

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
    DropConstraintActionNode,
    DropIndexStatementNode,
    ForeignKeyConstraintNode,
    PrimaryKeyConstraintNode,
    TableNode,
    UniqueConstraintNode,
)
from buildaquery.compiler.cockroachdb.cockroachdb_compiler import CockroachDbCompiler
from buildaquery.compiler.mariadb.mariadb_compiler import MariaDbCompiler
from buildaquery.compiler.mssql.mssql_compiler import MsSqlCompiler
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
from buildaquery.compiler.oracle.oracle_compiler import OracleCompiler
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler


COMPILERS = [
    PostgresCompiler,
    SqliteCompiler,
    MySqlCompiler,
    MariaDbCompiler,
    CockroachDbCompiler,
    OracleCompiler,
    MsSqlCompiler,
]


def _create_with_constraints() -> CreateStatementNode:
    return CreateStatementNode(
        table=TableNode(name="orders"),
        columns=[
            ColumnDefinitionNode(name="order_id", data_type="INTEGER", not_null=True),
            ColumnDefinitionNode(name="tenant_id", data_type="INTEGER", not_null=True),
            ColumnDefinitionNode(name="customer_id", data_type="INTEGER", not_null=True),
            ColumnDefinitionNode(name="qty", data_type="INTEGER", not_null=True),
        ],
        constraints=[
            PrimaryKeyConstraintNode(name="pk_orders", columns=[ColumnNode(name="order_id"), ColumnNode(name="tenant_id")]),
            UniqueConstraintNode(name="uq_orders_customer", columns=[ColumnNode(name="tenant_id"), ColumnNode(name="customer_id")]),
            ForeignKeyConstraintNode(
                name="fk_orders_customer",
                columns=[ColumnNode(name="customer_id")],
                reference_table=TableNode(name="customers"),
                reference_columns=[ColumnNode(name="id")],
                on_delete="CASCADE",
            ),
            CheckConstraintNode(
                name="ck_orders_qty",
                condition=BinaryOperationNode(
                    left=ColumnNode(name="qty"),
                    operator=">",
                    right=ColumnNode(name="tenant_id"),
                ),
            ),
        ],
        if_not_exists=True,
    )


@pytest.mark.parametrize("compiler_type", COMPILERS)
def test_create_table_with_table_constraints_compiles(compiler_type) -> None:
    compiler = compiler_type()
    node = _create_with_constraints()
    if compiler_type is OracleCompiler:
        node.if_not_exists = False
    compiled = compiler.compile(node)
    assert "CREATE TABLE" in compiled.sql
    assert "PRIMARY KEY (order_id, tenant_id)" in compiled.sql
    assert "UNIQUE (tenant_id, customer_id)" in compiled.sql
    assert "FOREIGN KEY (customer_id) REFERENCES customers (id)" in compiled.sql
    assert "CHECK ((qty > tenant_id))" in compiled.sql
    assert compiled.params == []


@pytest.mark.parametrize("compiler_type", [PostgresCompiler, SqliteCompiler, CockroachDbCompiler])
def test_create_drop_index_simple_dialects(compiler_type) -> None:
    compiler = compiler_type()
    create_compiled = compiler.compile(
        CreateIndexStatementNode(
            name="idx_orders_customer",
            table=TableNode(name="orders"),
            columns=[ColumnNode(name="customer_id"), ColumnNode(name="tenant_id")],
            unique=False,
            if_not_exists=True,
        )
    )
    assert "CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id, tenant_id)" == create_compiled.sql

    if compiler_type is SqliteCompiler:
        with pytest.raises(ValueError, match="CASCADE"):
            compiler.compile(DropIndexStatementNode(name="idx_orders_customer", if_exists=True, cascade=True))
    else:
        drop_compiled = compiler.compile(
            DropIndexStatementNode(name="idx_orders_customer", if_exists=True, cascade=True)
        )
        assert drop_compiled.sql.startswith("DROP INDEX IF EXISTS idx_orders_customer")


def test_create_drop_index_mysql_requires_table() -> None:
    compiler = MySqlCompiler()
    create_compiled = compiler.compile(
        CreateIndexStatementNode(
            name="idx_orders_customer",
            table=TableNode(name="orders"),
            columns=[ColumnNode(name="customer_id")],
        )
    )
    assert create_compiled.sql == "CREATE INDEX idx_orders_customer ON orders (customer_id)"

    drop_compiled = compiler.compile(
        DropIndexStatementNode(name="idx_orders_customer", table=TableNode(name="orders"))
    )
    assert drop_compiled.sql == "DROP INDEX idx_orders_customer ON orders"


def test_create_drop_index_mssql_requires_table() -> None:
    compiler = MsSqlCompiler()
    create_compiled = compiler.compile(
        CreateIndexStatementNode(
            name="idx_orders_customer",
            table=TableNode(name="orders"),
            columns=[ColumnNode(name="customer_id")],
            unique=True,
        )
    )
    assert create_compiled.sql == "CREATE UNIQUE INDEX idx_orders_customer ON orders (customer_id)"

    drop_compiled = compiler.compile(
        DropIndexStatementNode(name="idx_orders_customer", table=TableNode(name="orders"), if_exists=True)
    )
    assert drop_compiled.sql == "DROP INDEX IF EXISTS idx_orders_customer ON orders"


def test_create_drop_index_oracle() -> None:
    compiler = OracleCompiler()
    create_compiled = compiler.compile(
        CreateIndexStatementNode(
            name="idx_orders_customer",
            table=TableNode(name="orders"),
            columns=[ColumnNode(name="customer_id")],
        )
    )
    assert create_compiled.sql == "CREATE INDEX idx_orders_customer ON orders (customer_id)"

    drop_compiled = compiler.compile(DropIndexStatementNode(name="idx_orders_customer"))
    assert drop_compiled.sql == "DROP INDEX idx_orders_customer"


@pytest.mark.parametrize("compiler_type", [PostgresCompiler, CockroachDbCompiler, MySqlCompiler, MariaDbCompiler, MsSqlCompiler])
def test_alter_table_actions_compile_for_multi_action_dialects(compiler_type) -> None:
    compiler = compiler_type()
    compiled = compiler.compile(
        AlterTableStatementNode(
            table=TableNode(name="orders"),
            actions=[
                AddColumnActionNode(column=ColumnDefinitionNode(name="status", data_type="TEXT")),
                AddConstraintActionNode(
                    constraint=UniqueConstraintNode(name="uq_orders_status", columns=[ColumnNode(name="status")])
                ),
                DropColumnActionNode(column_name="status"),
            ],
        )
    )
    assert compiled.sql.startswith("ALTER TABLE orders ")
    assert "ADD" in compiled.sql
    assert "DROP COLUMN status" in compiled.sql


def test_alter_table_sqlite_single_action_only() -> None:
    compiler = SqliteCompiler()
    with pytest.raises(ValueError, match="single action"):
        compiler.compile(
            AlterTableStatementNode(
                table=TableNode(name="orders"),
                actions=[
                    AddColumnActionNode(column=ColumnDefinitionNode(name="status", data_type="TEXT")),
                    DropColumnActionNode(column_name="status"),
                ],
            )
        )

    with pytest.raises(ValueError, match="ADD CONSTRAINT"):
        compiler.compile(
            AlterTableStatementNode(
                table=TableNode(name="orders"),
                actions=[
                    AddConstraintActionNode(
                        constraint=UniqueConstraintNode(name="uq_orders_status", columns=[ColumnNode(name="status")])
                    )
                ],
            )
        )


def test_alter_table_drop_constraint_support_variants() -> None:
    postgres = PostgresCompiler()
    postgres_compiled = postgres.compile(
        AlterTableStatementNode(
            table=TableNode(name="orders"),
            actions=[DropConstraintActionNode(constraint_name="uq_orders_status", if_exists=True, cascade=True)],
        )
    )
    assert postgres_compiled.sql == "ALTER TABLE orders DROP CONSTRAINT IF EXISTS uq_orders_status CASCADE"

    mysql = MySqlCompiler()
    with pytest.raises(ValueError, match="DROP CONSTRAINT"):
        mysql.compile(
            AlterTableStatementNode(
                table=TableNode(name="orders"),
                actions=[DropConstraintActionNode(constraint_name="uq_orders_status")],
            )
        )
