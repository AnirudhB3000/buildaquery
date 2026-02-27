import pytest

from buildaquery.abstract_syntax_tree.models import (
    ColumnNode,
    ConflictTargetNode,
    InsertStatementNode,
    LiteralNode,
    TableNode,
    UpsertClauseNode,
)
from buildaquery.compiler.cockroachdb.cockroachdb_compiler import CockroachDbCompiler
from buildaquery.compiler.mariadb.mariadb_compiler import MariaDbCompiler
from buildaquery.compiler.mssql.mssql_compiler import MsSqlCompiler
from buildaquery.compiler.mysql.mysql_compiler import MySqlCompiler
from buildaquery.compiler.oracle.oracle_compiler import OracleCompiler
from buildaquery.compiler.postgres.postgres_compiler import PostgresCompiler
from buildaquery.compiler.sqlite.sqlite_compiler import SqliteCompiler


@pytest.mark.parametrize(
    ("compiler", "expected_sql", "expected_params"),
    [
        (
            PostgresCompiler(),
            "INSERT INTO users (id, name) VALUES (%s, %s), (%s, %s)",
            [1, "a", 2, "b"],
        ),
        (
            SqliteCompiler(),
            "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)",
            [1, "a", 2, "b"],
        ),
        (
            MySqlCompiler(),
            "INSERT INTO users (id, name) VALUES (%s, %s), (%s, %s)",
            [1, "a", 2, "b"],
        ),
        (
            MariaDbCompiler(),
            "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)",
            [1, "a", 2, "b"],
        ),
        (
            CockroachDbCompiler(),
            "INSERT INTO users (id, name) VALUES (%s, CAST(%s AS STRING)), (%s, CAST(%s AS STRING))",
            [1, "a", 2, "b"],
        ),
        (
            MsSqlCompiler(),
            "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)",
            [1, "a", 2, "b"],
        ),
    ],
)
def test_compile_multi_row_insert_common_dialects(compiler, expected_sql, expected_params):
    query = InsertStatementNode(
        table=TableNode(name="users"),
        columns=[ColumnNode(name="id"), ColumnNode(name="name")],
        rows=[
            [LiteralNode(value=1), LiteralNode(value="a")],
            [LiteralNode(value=2), LiteralNode(value="b")],
        ],
    )
    compiled = compiler.compile(query)
    assert compiled.sql == expected_sql
    assert compiled.params == expected_params


def test_compile_multi_row_insert_oracle():
    compiler = OracleCompiler()
    query = InsertStatementNode(
        table=TableNode(name="users"),
        columns=[ColumnNode(name="id"), ColumnNode(name="name")],
        rows=[
            [LiteralNode(value=1), LiteralNode(value="a")],
            [LiteralNode(value=2), LiteralNode(value="b")],
        ],
    )
    compiled = compiler.compile(query)
    assert (
        compiled.sql
        == "INSERT ALL INTO users (id, name) VALUES (:1, :2) INTO users (id, name) VALUES (:3, :4) SELECT 1 FROM dual"
    )
    assert compiled.params == [1, "a", 2, "b"]


def test_compile_insert_requires_exactly_one_of_values_or_rows():
    compiler = PostgresCompiler()
    with pytest.raises(ValueError, match="exactly one of values or rows"):
        compiler.compile(
            InsertStatementNode(
                table=TableNode(name="users"),
                columns=[ColumnNode(name="id")],
                values=[LiteralNode(value=1)],
                rows=[[LiteralNode(value=2)]],
            )
        )

    with pytest.raises(ValueError, match="exactly one of values or rows"):
        compiler.compile(
            InsertStatementNode(
                table=TableNode(name="users"),
                columns=[ColumnNode(name="id")],
            )
        )


def test_compile_insert_rows_requires_consistent_row_width():
    compiler = PostgresCompiler()
    with pytest.raises(ValueError, match="same number of values"):
        compiler.compile(
            InsertStatementNode(
                table=TableNode(name="users"),
                columns=[ColumnNode(name="id"), ColumnNode(name="name")],
                rows=[
                    [LiteralNode(value=1), LiteralNode(value="a")],
                    [LiteralNode(value=2)],
                ],
            )
        )


def test_compile_insert_rows_requires_column_match():
    compiler = PostgresCompiler()
    with pytest.raises(ValueError, match="same length"):
        compiler.compile(
            InsertStatementNode(
                table=TableNode(name="users"),
                columns=[ColumnNode(name="id"), ColumnNode(name="name")],
                rows=[[LiteralNode(value=1)]],
            )
        )


def test_compile_mssql_merge_upsert_rejects_rows_payload():
    compiler = MsSqlCompiler()
    with pytest.raises(ValueError, match="does not support multi-row rows payload"):
        compiler.compile(
            InsertStatementNode(
                table=TableNode(name="users"),
                columns=[ColumnNode(name="id"), ColumnNode(name="name")],
                rows=[[LiteralNode(value=1), LiteralNode(value="a")]],
                upsert_clause=UpsertClauseNode(
                    conflict_target=ConflictTargetNode(columns=[ColumnNode(name="id")]),
                    update_columns=["name"],
                ),
            )
        )


def test_compile_oracle_merge_upsert_rejects_rows_payload():
    compiler = OracleCompiler()
    with pytest.raises(ValueError, match="does not support multi-row rows payload"):
        compiler.compile(
            InsertStatementNode(
                table=TableNode(name="users"),
                columns=[ColumnNode(name="id"), ColumnNode(name="name")],
                rows=[[LiteralNode(value=1), LiteralNode(value="a")]],
                upsert_clause=UpsertClauseNode(
                    conflict_target=ConflictTargetNode(columns=[ColumnNode(name="id")]),
                    update_columns=["name"],
                ),
            )
        )
