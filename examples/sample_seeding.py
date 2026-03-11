from buildaquery.abstract_syntax_tree.models import ColumnNode, InsertStatementNode, LiteralNode, TableNode
from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.seeding import SeedRunner, SeedStep


steps = [
    SeedStep(
        name="insert-admin-user",
        action=InsertStatementNode(
            table=TableNode(name="users"),
            columns=[ColumnNode(name="id"), ColumnNode(name="email")],
            values=[LiteralNode(1), LiteralNode("admin@example.com")],
        ),
    ),
    SeedStep(
        name="insert-auditor-user",
        action=CompiledQuery(
            sql="INSERT INTO users (id, email) VALUES (?, ?)",
            params=[2, "auditor@example.com"],
        ),
    ),
    SeedStep(
        name="refresh-summary-table",
        action=lambda executor: executor.execute_raw(
            "INSERT INTO audit_log (event_name) VALUES (?)",
            ["seed.completed"],
            trusted=True,
        ),
    ),
]

runner = SeedRunner(transactional=True)

# Example usage:
# summary = runner.run(executor, steps)
# print(summary.completed_steps)
