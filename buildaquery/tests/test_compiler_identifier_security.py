import pytest

from buildaquery.abstract_syntax_tree.models import (
    AliasNode,
    CTENode,
    ColumnNode,
    SelectStatementNode,
    StarNode,
    SubqueryNode,
    TableNode,
)
from buildaquery.compiler.clickhouse.clickhouse_compiler import ClickHouseCompiler
from buildaquery.compiler.cockroachdb.cockroachdb_compiler import CockroachDbCompiler
from buildaquery.compiler.duckdb.duckdb_compiler import DuckDbCompiler
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
    DuckDbCompiler,
    ClickHouseCompiler,
]


@pytest.mark.parametrize("compiler_type", COMPILERS)
def test_rejects_unsafe_table_identifier(compiler_type) -> None:
    compiler = compiler_type()
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users; DROP TABLE audit_log;--"),
    )
    with pytest.raises(ValueError, match="Unsafe"):
        compiler.compile(query)


@pytest.mark.parametrize("compiler_type", COMPILERS)
def test_rejects_unsafe_column_identifier(compiler_type) -> None:
    compiler = compiler_type()
    query = SelectStatementNode(
        select_list=[ColumnNode(name="name; DELETE FROM users;--")],
        from_table=TableNode(name="users"),
    )
    with pytest.raises(ValueError, match="Unsafe"):
        compiler.compile(query)


@pytest.mark.parametrize("compiler_type", COMPILERS)
def test_rejects_unsafe_alias_identifier(compiler_type) -> None:
    compiler = compiler_type()
    inner = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
    )
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=SubqueryNode(statement=inner, alias="u; DROP SCHEMA public;--"),
    )
    with pytest.raises(ValueError, match="Unsafe"):
        compiler.compile(query)


@pytest.mark.parametrize("compiler_type", COMPILERS)
def test_rejects_unsafe_cte_identifier(compiler_type) -> None:
    compiler = compiler_type()
    cte_stmt = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
    )
    query = SelectStatementNode(
        select_list=[StarNode()],
        from_table=TableNode(name="users"),
        ctes=[CTENode(name="c1; DROP VIEW v;--", subquery=cte_stmt)],
    )
    with pytest.raises(ValueError, match="Unsafe"):
        compiler.compile(query)


@pytest.mark.parametrize("compiler_type", COMPILERS)
def test_allows_count_star_column_expression(compiler_type) -> None:
    compiler = compiler_type()
    query = SelectStatementNode(
        select_list=[ColumnNode(name="COUNT(*)")],
        from_table=TableNode(name="users"),
    )
    compiled = compiler.compile(query)
    assert "COUNT(*)" in compiled.sql


@pytest.mark.parametrize("compiler_type", COMPILERS)
def test_allows_regular_identifier_alias(compiler_type) -> None:
    compiler = compiler_type()
    query = SelectStatementNode(
        select_list=[AliasNode(expression=ColumnNode(name="id"), name="item_id")],
        from_table=TableNode(name="users", alias="u"),
    )
    compiled = compiler.compile(query)
    assert "item_id" in compiled.sql
