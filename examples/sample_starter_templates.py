from buildaquery.abstract_syntax_tree.models import (
    BinaryOperationNode,
    ColumnNode,
    ConflictTargetNode,
    InsertStatementNode,
    LiteralNode,
    ReturningClauseNode,
    SelectStatementNode,
    TableNode,
    UpdateStatementNode,
    UpsertClauseNode,
    WhereClauseNode,
)
from buildaquery.compiler import PostgresCompiler
from buildaquery.execution import (
    InMemoryMetricsAdapter,
    InMemoryTracingAdapter,
    ObservabilitySettings,
    RetryPolicy,
    compose_event_observers,
    make_json_event_logger,
)
import logging


def print_template(name: str, sql: str, params: list[object]) -> None:
    print(f"\n{name}")
    print(sql)
    print(params)


def main() -> None:
    compiler = PostgresCompiler()
    users = TableNode(name="users")

    create_user = InsertStatementNode(
        table=users,
        columns=[ColumnNode(name="email"), ColumnNode(name="active")],
        values=[LiteralNode(value="alice@example.com"), LiteralNode(value=True)],
        returning_clause=ReturningClauseNode(expressions=[ColumnNode(name="id")]),
    )
    print_template(
        "CRUD: create",
        compiler.to_sql(create_user).to_sql(),
        compiler.to_sql(create_user).params,
    )

    read_user = SelectStatementNode(
        select_list=[ColumnNode(name="id"), ColumnNode(name="email")],
        from_table=users,
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="active"),
                operator="=",
                right=LiteralNode(value=True),
            )
        ),
    )
    print_template(
        "CRUD: read",
        compiler.to_sql(read_user).to_sql(),
        compiler.to_sql(read_user).params,
    )

    update_user = UpdateStatementNode(
        table=users,
        set_clauses={"email": LiteralNode(value="alice+new@example.com")},
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=1),
            )
        ),
    )
    print_template(
        "CRUD: update",
        compiler.to_sql(update_user).to_sql(),
        compiler.to_sql(update_user).params,
    )

    upsert_user = InsertStatementNode(
        table=users,
        columns=[ColumnNode(name="id"), ColumnNode(name="email")],
        values=[LiteralNode(value=1), LiteralNode(value="alice@example.com")],
        upsert_clause=UpsertClauseNode(
            conflict_target=ConflictTargetNode(columns=[ColumnNode(name="id")]),
            update_columns=["email"],
        ),
    )
    print_template(
        "Upsert",
        compiler.to_sql(upsert_user).to_sql(),
        compiler.to_sql(upsert_user).params,
    )

    transfer_debit = UpdateStatementNode(
        table=TableNode(name="accounts"),
        set_clauses={"balance": LiteralNode(value=-50)},
        where_clause=WhereClauseNode(
            condition=BinaryOperationNode(
                left=ColumnNode(name="id"),
                operator="=",
                right=LiteralNode(value=1),
            )
        ),
    )
    print_template(
        "Transaction step",
        compiler.to_sql(transfer_debit).to_sql(),
        compiler.to_sql(transfer_debit).params,
    )
    print("Transaction template:")
    print("executor.begin()")
    print("executor.execute(debit_stmt)")
    print("executor.execute(credit_stmt)")
    print("executor.commit()")

    retry_policy = RetryPolicy(max_attempts=3, base_delay_seconds=0.05)
    print("Retry template:")
    print(retry_policy)
    print("executor.execute_with_retry(stmt, retry_policy=retry_policy)")

    logger = logging.getLogger("buildaquery.starters")
    metrics = InMemoryMetricsAdapter()
    tracing = InMemoryTracingAdapter()
    observability = ObservabilitySettings(
        event_observer=compose_event_observers(
            make_json_event_logger(logger=logger),
            metrics,
            tracing,
        ),
        metadata={"service": "starter-template"},
    )
    print("Observability template:")
    print(observability)


if __name__ == "__main__":
    main()
