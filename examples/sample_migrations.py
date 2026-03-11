from buildaquery.abstract_syntax_tree.models import ColumnNode, InsertStatementNode, LiteralNode, TableNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.migrations import MigrationRunner, MigrationStep

# Syntax-only example: define migrations without connecting to a live database here.
users = TableNode(name="users")
runner = MigrationRunner()

migrations = [
    MigrationStep(
        version=1,
        name="create-users",
        up=CompiledQuery(
            sql="CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"
        ),
        down=CompiledQuery(sql="DROP TABLE users"),
    ),
    MigrationStep(
        version=2,
        name="seed-admin",
        up=InsertStatementNode(
            table=users,
            columns=[ColumnNode(name="id"), ColumnNode(name="email")],
            values=[LiteralNode(1), LiteralNode("admin@example.com")],
        ),
        down=CompiledQuery(
            sql="DELETE FROM users WHERE id = :id",
            params={"id": 1},
        ),
    ),
]

# apply_summary = runner.apply(executor, migrations)
# rollback_summary = runner.rollback_last(executor, migrations)
